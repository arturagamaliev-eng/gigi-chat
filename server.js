const path = require('path');
const http = require('http');
const crypto = require('crypto');
const express = require('express');
const { Server } = require('socket.io');
const { Pool } = require('pg');

const PORT = process.env.PORT || 3000;
const MAX_TEXT = 2000;
const NICK_RE = /^[\p{L}\p{N}_]{3,20}$/u;
const PASS_MIN = 4;
const PASS_MAX = 100;
const MAX_IMAGE_CHARS = 2000000;              // base64 size, about 1.5 MB
const CHAT_MEDIA_MS = 7 * 24 * 3600 * 1000;   // chat photos are kept 7 days
const STORY_MS = 24 * 3600 * 1000;            // stories are kept 24 hours
const AVATAR_MS = 365 * 24 * 3600 * 1000;     // profile photo stays until removed
const keyOf = s => s.toLowerCase();
const MEDIA_RE = /^\/media\/([a-f0-9]{32})$/;
const mediaIdOf = url => { const m = MEDIA_RE.exec(String(url || '')); return m ? m[1] : null; };
const isOurMedia = (url, kind) => kind === 'image' && typeof url === 'string' && mediaIdOf(url) !== null;
const adminOk = code => Boolean(process.env.ADMIN_CODE) && String(code || '') === process.env.ADMIN_CODE;
const EMOJIS = ['👍', '❤️', '😂', '😮', '😢', '🔥'];
const SELL_PERCENT = 0.8; // a gift sold back to the market returns 80% of its price

// Sends a gift as a chat message. The text holds a snapshot, so the card still works later.
async function sendGiftMessage(from, to, item) {
  const text = JSON.stringify({ id: item.id, title: item.title, description: item.description, price: item.price, image: item.image });
  const now = Date.now();
  const row = await db.one(
    "INSERT INTO messages (from_nick, to_nick, text, kind, created_at) VALUES ($1, $2, $3, 'gift', $4) RETURNING id",
    [from, to, text, now]);
  io.to('u:' + to).to('u:' + from).emit('message', { id: row.id, from, to, text, created_at: now, read_at: null, kind: 'gift' });
}

const isMember = async (id, nick) => Boolean(await db.one('SELECT 1 AS x FROM chat_members WHERE chat_id = $1 AND nick = $2', [id, nick]));
const membersOf = async id => (await db.all('SELECT nick FROM chat_members WHERE chat_id = $1', [id])).map(r => r.nick);
const toGMsg = r => ({ id: r.id, chat: r.chat_id, from: r.from_nick, text: r.text, kind: r.kind || 'text', created_at: r.created_at });

async function chatSummary(c, me) {
  const members = await membersOf(c.id);
  const last = await db.one('SELECT id, chat_id, from_nick, text, kind, created_at FROM gmessages WHERE chat_id = $1 ORDER BY id DESC LIMIT 1', [c.id]);
  return {
    id: c.id, kind: c.kind, title: c.title, owner: c.owner,
    members: members.length, joined: members.includes(me), last: last ? toGMsg(last) : null,
  };
}

// Returns Map: target id -> { emoji: [nicknames] }
async function reactionsFor(type, ids) {
  const out = new Map();
  if (!ids.length) return out;
  const rows = await db.all(
    'SELECT target_id, emoji, nick FROM reactions WHERE target_type = $1 AND target_id = ANY($2::int[])',
    [type, ids]);
  for (const r of rows) {
    if (!out.has(r.target_id)) out.set(r.target_id, {});
    const o = out.get(r.target_id);
    if (!o[r.emoji]) o[r.emoji] = [];
    o[r.emoji].push(r.nick);
  }
  return out;
}

if (!process.env.DATABASE_URL) {
  console.error('DATABASE_URL is not set. Add it in Render → Environment.');
  process.exit(1);
}
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 5 });
pool.on('error', err => console.error('db pool error:', err.message));

const db = {
  one: async (sql, p = []) => (await pool.query(sql, p)).rows[0],
  all: async (sql, p = []) => (await pool.query(sql, p)).rows,
  run: (sql, p = []) => pool.query(sql, p),
};

async function tx(fn) {
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    const result = await fn(c);
    await c.query('COMMIT');
    return result;
  } catch (e) {
    await c.query('ROLLBACK');
    throw e;
  } finally {
    c.release();
  }
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS users (
  nickname TEXT PRIMARY KEY,
  key TEXT UNIQUE NOT NULL,
  pass_salt TEXT NOT NULL,
  pass_hash TEXT NOT NULL,
  avatar TEXT,
  created_at DOUBLE PRECISION NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
  token TEXT PRIMARY KEY,
  nickname TEXT NOT NULL,
  created_at DOUBLE PRECISION NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
  id SERIAL PRIMARY KEY,
  from_nick TEXT NOT NULL,
  to_nick TEXT NOT NULL,
  text TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT 'text',
  created_at DOUBLE PRECISION NOT NULL,
  read_at DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_msg_from ON messages(from_nick, id);
CREATE INDEX IF NOT EXISTS idx_msg_to ON messages(to_nick, id);
CREATE TABLE IF NOT EXISTS stories (
  id SERIAL PRIMARY KEY,
  text TEXT NOT NULL,
  created_at DOUBLE PRECISION NOT NULL
);
`;

const EXTRA_SCHEMA = `
ALTER TABLE stories ADD COLUMN IF NOT EXISTS media TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen DOUBLE PRECISION;
ALTER TABLE users ADD COLUMN IF NOT EXISTS bio TEXT;
CREATE TABLE IF NOT EXISTS chats (
  id SERIAL PRIMARY KEY,
  kind TEXT NOT NULL,
  title TEXT NOT NULL,
  owner TEXT NOT NULL,
  created_at DOUBLE PRECISION NOT NULL
);
CREATE TABLE IF NOT EXISTS chat_members (
  chat_id INTEGER NOT NULL,
  nick TEXT NOT NULL,
  PRIMARY KEY (chat_id, nick)
);
CREATE TABLE IF NOT EXISTS gmessages (
  id SERIAL PRIMARY KEY,
  chat_id INTEGER NOT NULL,
  from_nick TEXT NOT NULL,
  text TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT 'text',
  created_at DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gmsg_chat ON gmessages(chat_id, id);
CREATE TABLE IF NOT EXISTS gift_items (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  price INTEGER NOT NULL,
  image TEXT NOT NULL,
  owner TEXT,
  on_shelf BOOLEAN NOT NULL DEFAULT false,
  created_at DOUBLE PRECISION NOT NULL
);
ALTER TABLE users ADD COLUMN IF NOT EXISTS stars INTEGER NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS reactions (
  target_type TEXT NOT NULL,
  target_id INTEGER NOT NULL,
  nick TEXT NOT NULL,
  emoji TEXT NOT NULL,
  PRIMARY KEY (target_type, target_id, nick, emoji)
);
CREATE TABLE IF NOT EXISTS media (
  id TEXT PRIMARY KEY,
  data TEXT NOT NULL,
  mime TEXT NOT NULL,
  expires_at DOUBLE PRECISION NOT NULL
);
`;

const findUser = nick => db.one('SELECT nickname, key, pass_salt, pass_hash FROM users WHERE key = $1', [keyOf(nick)]);
const toMsg = r => ({ id: r.id, from: r.from_nick, to: r.to_nick, text: r.text, created_at: r.created_at, read_at: r.read_at ?? null, kind: r.kind || 'text' });

function hashPassword(password, salt = crypto.randomBytes(16).toString('hex')) {
  return { salt, hash: crypto.scryptSync(password, salt, 64).toString('hex') };
}

function checkPassword(password, salt, hash) {
  const a = Buffer.from(crypto.scryptSync(password, salt, 64).toString('hex'), 'hex');
  const b = Buffer.from(hash, 'hex');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

async function createSession(nick) {
  const token = crypto.randomBytes(24).toString('hex');
  await db.run('INSERT INTO sessions (token, nickname, created_at) VALUES ($1, $2, $3)', [token, nick, Date.now()]);
  return { ok: true, nick, token };
}

async function authenticate(payload) {
  if (payload?.token) {
    const s = await db.one('SELECT nickname FROM sessions WHERE token = $1', [String(payload.token)]);
    return s ? { ok: true, nick: s.nickname } : { ok: false, error: 'bad_session' };
  }
  const nick = typeof payload?.nick === 'string' ? payload.nick.trim() : '';
  const password = typeof payload?.password === 'string' ? payload.password : '';
  if (!NICK_RE.test(nick) || keyOf(nick) === 'gigi_chat') return { ok: false, error: 'invalid_nick' };
  if (password.length < PASS_MIN || password.length > PASS_MAX) return { ok: false, error: 'invalid_password' };

  const user = await findUser(nick);
  if (!user) {
    const { salt, hash } = hashPassword(password);
    await db.run('INSERT INTO users (nickname, key, pass_salt, pass_hash, created_at) VALUES ($1, $2, $3, $4, $5)',
      [nick, keyOf(nick), salt, hash, Date.now()]);
    return createSession(nick);
  }
  if (!checkPassword(password, user.pass_salt, user.pass_hash)) return { ok: false, error: 'nick_taken' };
  return createSession(user.nickname);
}

// Removes expired photos, old messages' photos and old stories.
async function cleanup() {
  const now = Date.now();
  await db.run("UPDATE messages SET kind = 'gone', text = '' WHERE kind = 'image' AND created_at < $1", [now - CHAT_MEDIA_MS]);
  await db.run("UPDATE gmessages SET kind = 'gone', text = '' WHERE kind = 'image' AND created_at < $1", [now - CHAT_MEDIA_MS]);
  await db.run('DELETE FROM stories WHERE created_at < $1', [now - STORY_MS]);
  await db.run('DELETE FROM media WHERE expires_at < $1', [now]);
  await db.run("DELETE FROM reactions WHERE target_type = 'story' AND target_id NOT IN (SELECT id FROM stories)");
}

async function recentStories() {
  const rows = await db.all('SELECT id, text, media, created_at FROM stories WHERE created_at > $1 ORDER BY id DESC LIMIT 20',
    [Date.now() - STORY_MS]);
  const reacts = await reactionsFor('story', rows.map(r => r.id));
  return rows.map(r => ({ ...r, reactions: reacts.get(r.id) || {} }));
}

async function removeUser(nick) {
  const urls = (await db.all("SELECT text FROM messages WHERE (from_nick = $1 OR to_nick = $1) AND kind = 'image'", [nick]))
    .map(r => r.text);
  const u = await db.one('SELECT avatar FROM users WHERE nickname = $1', [nick]);
  if (u?.avatar) urls.push(u.avatar);
  await tx(async c => {
    await c.query('DELETE FROM messages WHERE from_nick = $1 OR to_nick = $1', [nick]);
    await c.query("DELETE FROM reactions WHERE nick = $1 OR (target_type = 'msg' AND target_id NOT IN (SELECT id FROM messages))", [nick]);
    await c.query('DELETE FROM sessions WHERE nickname = $1', [nick]);
    await c.query("DELETE FROM reactions WHERE target_type = 'gmsg' AND target_id IN (SELECT id FROM gmessages WHERE chat_id IN (SELECT id FROM chats WHERE owner = $1) OR from_nick = $1)", [nick]);
    await c.query('DELETE FROM gmessages WHERE chat_id IN (SELECT id FROM chats WHERE owner = $1) OR from_nick = $1', [nick]);
    await c.query('DELETE FROM chat_members WHERE nick = $1 OR chat_id IN (SELECT id FROM chats WHERE owner = $1)', [nick]);
    await c.query('DELETE FROM chats WHERE owner = $1', [nick]);
    await c.query('UPDATE gift_items SET owner = NULL, on_shelf = false WHERE owner = $1', [nick]);
    await c.query('DELETE FROM users WHERE nickname = $1', [nick]);
  });
  for (const url of urls) {
    const id = mediaIdOf(url);
    if (id) await db.run('DELETE FROM media WHERE id = $1', [id]);
  }
}

// ---- HTTP ----

const route = fn => (req, res) => fn(req, res).catch(e => {
  console.error('route error:', e.message);
  res.status(500).json({ error: 'server' });
});

const app = express();
app.use(express.json({ limit: '25mb' }));
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/users', route(async (req, res) => {
  const term = String(req.query.q || '').replace(/[^\p{L}\p{N}_]/gu, '');
  if (!term) return res.json([]);
  const rows = await db.all('SELECT nickname FROM users WHERE key LIKE $1 ORDER BY nickname LIMIT 20', [`%${keyOf(term)}%`]);
  res.json(rows.map(r => r.nickname));
}));

app.get('/api/chats', route(async (req, res) => {
  const me = String(req.query.me || '');
  if (!NICK_RE.test(me)) return res.status(400).json({ error: 'bad_nick' });
  const rows = await db.all(`
    SELECT partner, MAX(id) AS last_id FROM (
      SELECT to_nick AS partner, id FROM messages WHERE from_nick = $1
      UNION ALL
      SELECT from_nick AS partner, id FROM messages WHERE to_nick = $1
    ) t GROUP BY partner ORDER BY last_id DESC LIMIT 100`, [me]);
  const out = [];
  for (const r of rows) {
    const last = await db.one('SELECT * FROM messages WHERE id = $1', [r.last_id]);
    out.push({ nick: r.partner, last: toMsg(last) });
  }
  res.json(out);
}));

app.get('/api/history', route(async (req, res) => {
  const a = String(req.query.me || '');
  const b = String(req.query.with || '');
  if (!NICK_RE.test(a) || !NICK_RE.test(b)) return res.status(400).json({ error: 'bad_nick' });
  const rows = await db.all(`
    SELECT * FROM messages
    WHERE (from_nick = $1 AND to_nick = $2) OR (from_nick = $2 AND to_nick = $1)
    ORDER BY id DESC LIMIT 200`, [a, b]);
  const reacts = await reactionsFor('msg', rows.map(r => r.id));
  res.json(rows.reverse().map(r => ({ ...toMsg(r), reactions: reacts.get(r.id) || {} })));
}));

app.post('/api/upload', route(async (req, res) => {
  const s = await db.one('SELECT nickname FROM sessions WHERE token = $1', [String(req.body?.token || '')]);
  if (!s) return res.status(401).json({ error: 'bad_session' });
  const m = /^data:(image\/(?:jpeg|png|webp));base64,([A-Za-z0-9+/=]+)$/.exec(String(req.body?.data || ''));
  if (!m) return res.status(400).json({ error: 'bad_type' });
  if (m[2].length > MAX_IMAGE_CHARS) return res.status(413).json({ error: 'too_big' });
  const id = crypto.randomBytes(16).toString('hex');
  await db.run('INSERT INTO media (id, data, mime, expires_at) VALUES ($1, $2, $3, $4)',
    [id, m[2], m[1], Date.now() + CHAT_MEDIA_MS]);
  res.json({ ok: true, url: '/media/' + id, kind: 'image' });
}));

app.get('/media/:id', route(async (req, res) => {
  if (!/^[a-f0-9]{32}$/.test(req.params.id)) return res.status(404).end();
  const row = await db.one('SELECT data, mime, expires_at FROM media WHERE id = $1', [req.params.id]);
  if (!row || row.expires_at < Date.now()) return res.status(404).end();
  res.set('Content-Type', row.mime);
  res.set('Cache-Control', 'private, max-age=86400');
  res.send(Buffer.from(row.data, 'base64'));
}));

// ---- Realtime ----

const server = http.createServer(app);
const io = new Server(server);

const online = new Map();      // nickname -> Set of socket ids
const socketNick = new Map();  // socket id -> nickname

const onlineList = () => [...online.keys()];
const broadcastPresence = () => io.emit('presence', onlineList());

function detach(socket) {
  const nick = socketNick.get(socket.id);
  if (!nick) return;
  socketNick.delete(socket.id);
  socket.leave('u:' + nick);
  const set = online.get(nick);
  if (set) {
    set.delete(socket.id);
    if (set.size === 0) {
      online.delete(nick);
      db.run('UPDATE users SET last_seen = $1 WHERE nickname = $2', [Date.now(), nick]).catch(() => {});
    }
  }
  broadcastPresence();
}

async function avatarMap() {
  const rows = await db.all('SELECT nickname, avatar FROM users WHERE avatar IS NOT NULL');
  return Object.fromEntries(rows.map(r => [r.nickname, r.avatar]));
}

io.on('connection', async socket => {
  socket.emit('presence', onlineList());
  try { socket.emit('avatars', await avatarMap()); } catch (e) { console.error('avatars:', e.message); }
  try { socket.emit('stories', await recentStories()); } catch (e) { console.error('stories:', e.message); }

  // Wraps each handler so a database error never crashes the server.
  const on = (ev, fn) => socket.on(ev, async (...args) => {
    try { await fn(...args); }
    catch (e) {
      console.error(ev + ':', e.message);
      const ack = args.find(a => typeof a === 'function');
      ack?.({ ok: false, error: 'server' });
    }
  });

  on('login', async (payload, ack) => {
    const res = await authenticate(payload);
    if (!res.ok) return ack?.(res);
    detach(socket);
    socketNick.set(socket.id, res.nick);
    if (!online.has(res.nick)) online.set(res.nick, new Set());
    online.get(res.nick).add(socket.id);
    socket.join('u:' + res.nick);
    broadcastPresence();
    ack?.(res);
  });

  on('message', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const kind = payload?.kind === 'image' ? 'image' : 'text';
    const text = typeof payload?.text === 'string' ? payload.text.trim() : '';
    if (kind === 'image' && !isOurMedia(text, 'image')) return ack?.({ ok: false, error: 'invalid' });
    if (!text || (kind === 'text' && text.length > MAX_TEXT)) return ack?.({ ok: false, error: 'invalid' });

    const to = (await findUser(String(payload?.to || '')))?.nickname;
    if (!to) return ack?.({ ok: false, error: 'no_user' });

    const now = Date.now();
    const row = await db.one(
      'INSERT INTO messages (from_nick, to_nick, text, kind, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING id',
      [me, to, text, kind, now]);
    if (kind === 'image') await db.run('UPDATE media SET expires_at = $1 WHERE id = $2', [now + CHAT_MEDIA_MS, mediaIdOf(text)]);
    const msg = { id: row.id, from: me, to, text, created_at: now, read_at: null, kind };
    io.to('u:' + to).to('u:' + me).emit('message', msg);
    ack?.({ ok: true, msg });
  });

  on('read', async payload => {
    const me = socketNick.get(socket.id);
    if (!me) return;
    const partner = (await findUser(String(payload?.with || '')))?.nickname;
    if (!partner || partner === me) return;
    const now = Date.now();
    const r = await db.run('UPDATE messages SET read_at = $1 WHERE from_nick = $2 AND to_nick = $3 AND read_at IS NULL', [now, partner, me]);
    if (r.rowCount > 0) io.to('u:' + partner).emit('read', { by: me, at: now });
  });

  on('msg_delete', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const row = Number.isInteger(id) ? await db.one('SELECT id, from_nick, to_nick, kind, text FROM messages WHERE id = $1', [id]) : null;
    if (!row || row.from_nick !== me) return ack?.({ ok: false, error: 'forbidden' });
    await db.run('DELETE FROM messages WHERE id = $1', [id]);
    await db.run("DELETE FROM reactions WHERE target_type = 'msg' AND target_id = $1", [id]);
    if (row.kind === 'image') {
      const mid = mediaIdOf(row.text);
      if (mid) await db.run('DELETE FROM media WHERE id = $1', [mid]);
    }
    io.to('u:' + row.to_nick).to('u:' + me).emit('msg_deleted', { id, from: me, to: row.to_nick });
    ack?.({ ok: true });
  });

  // Profile photo: send a photo URL to set it, or an empty value to remove it.
  on('set_avatar', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const old = (await db.one('SELECT avatar FROM users WHERE nickname = $1', [me]))?.avatar;
    const oldId = mediaIdOf(old);
    const raw = payload?.url;
    if (raw === null || raw === '' || raw === undefined) {
      await db.run('UPDATE users SET avatar = NULL WHERE nickname = $1', [me]);
      if (oldId) await db.run('DELETE FROM media WHERE id = $1', [oldId]);
      io.emit('avatar', { nick: me, url: null });
      return ack?.({ ok: true });
    }
    const url = String(raw);
    if (!isOurMedia(url, 'image')) return ack?.({ ok: false, error: 'invalid' });
    await db.run('UPDATE users SET avatar = $1 WHERE nickname = $2', [url, me]);
    await db.run('UPDATE media SET expires_at = $1 WHERE id = $2', [Date.now() + AVATAR_MS, mediaIdOf(url)]);
    if (oldId && old !== url) await db.run('DELETE FROM media WHERE id = $1', [oldId]);
    io.emit('avatar', { nick: me, url });
    ack?.({ ok: true });
  });

  on('set_bio', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const bio = typeof payload?.bio === 'string' ? payload.bio.trim().slice(0, 200) : '';
    await db.run('UPDATE users SET bio = $1 WHERE nickname = $2', [bio || null, me]);
    ack?.({ ok: true });
  });

  // Public profile of any user: online status, last seen, photo and about text.
  on('user_info', async (payload, ack) => {
    const u = await findUser(String(payload?.nick || ''));
    if (!u) return ack?.({ ok: false, error: 'no_user' });
    const row = await db.one('SELECT nickname, avatar, bio, last_seen FROM users WHERE nickname = $1', [u.nickname]);
    ack?.({ ok: true, info: {
      nick: row.nickname,
      online: online.has(row.nickname),
      lastSeen: row.last_seen,
      avatar: row.avatar,
      bio: row.bio,
      shelf: await db.all('SELECT id, title, image FROM gift_items WHERE owner = $1 AND on_shelf = true', [row.nickname]),
    } });
  });

  // Admin only: every account, including ones with no chats yet.
  on('admin_list', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const rows = await db.all('SELECT nickname, avatar, bio, last_seen, created_at FROM users ORDER BY nickname');
    ack?.({ ok: true, users: rows.map(r => ({
      nick: r.nickname,
      online: online.has(r.nickname),
      lastSeen: r.last_seen,
      created: r.created_at,
    })) });
  });

  // Admin only: deletes every account and every message. Stories are kept.
  on('admin_delete_all', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    await tx(async c => {
      await c.query('DELETE FROM messages');
      await c.query("DELETE FROM reactions WHERE target_type = 'msg'");
      await c.query('DELETE FROM sessions');
      await c.query("DELETE FROM reactions WHERE target_type = 'gmsg'");
      await c.query('DELETE FROM gmessages');
      await c.query('DELETE FROM chat_members');
      await c.query('DELETE FROM chats');
      await c.query('DELETE FROM gift_items WHERE owner IS NOT NULL');
      await c.query('DELETE FROM users');
      await c.query('DELETE FROM media WHERE id NOT IN (SELECT substr(media, 8, 32) FROM stories WHERE media IS NOT NULL)');
    });
    online.clear();
    socketNick.clear();
    io.emit('all_deleted');
    ack?.({ ok: true });
  });

  on('admin_delete', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const target = (await findUser(String(payload?.nick || '')))?.nickname;
    if (!target) return ack?.({ ok: false, error: 'no_user' });
    await removeUser(target);
    const sockets = [...(online.get(target) || [])];
    online.delete(target);
    for (const id of sockets) {
      socketNick.delete(id);
      const s = io.sockets.sockets.get(id);
      if (s) { s.leave('u:' + target); s.emit('account_deleted'); }
    }
    io.emit('user_deleted', { nick: target });
    broadcastPresence();
    ack?.({ ok: true });
  });

  on('rename', async (payload, ack) => {
    const oldNick = socketNick.get(socket.id);
    if (!oldNick) return ack?.({ ok: false, error: 'not_logged_in' });
    const newNick = typeof payload?.nick === 'string' ? payload.nick.trim() : '';
    if (!NICK_RE.test(newNick) || keyOf(newNick) === 'gigi_chat') return ack?.({ ok: false, error: 'invalid_nick' });
    if (newNick === oldNick) return ack?.({ ok: true, nick: oldNick });
    const taken = await findUser(newNick);
    if (taken && taken.nickname !== oldNick) return ack?.({ ok: false, error: 'nick_taken' });

    await tx(async c => {
      await c.query('UPDATE users SET nickname = $1, key = $2 WHERE nickname = $3', [newNick, keyOf(newNick), oldNick]);
      await c.query('UPDATE messages SET from_nick = $1 WHERE from_nick = $2', [newNick, oldNick]);
      await c.query('UPDATE messages SET to_nick = $1 WHERE to_nick = $2', [newNick, oldNick]);
      await c.query('UPDATE sessions SET nickname = $1 WHERE nickname = $2', [newNick, oldNick]);
    });

    const sockets = online.get(oldNick) || new Set();
    online.delete(oldNick);
    online.set(newNick, sockets);
    for (const id of sockets) {
      socketNick.set(id, newNick);
      const s = io.sockets.sockets.get(id);
      if (s) { s.leave('u:' + oldNick); s.join('u:' + newNick); }
    }
    io.emit('renamed', { from: oldNick, to: newNick });
    broadcastPresence();
    ack?.({ ok: true, nick: newNick });
  });

  on('delete', async (payload, ack) => {
    const nick = socketNick.get(socket.id);
    if (!nick) return ack?.({ ok: false, error: 'not_logged_in' });
    await removeUser(nick);
    const sockets = [...(online.get(nick) || [])];
    online.delete(nick);
    for (const id of sockets) {
      socketNick.delete(id);
      const s = io.sockets.sockets.get(id);
      if (s) { s.leave('u:' + nick); s.emit('account_deleted'); }
    }
    io.emit('user_deleted', { nick });
    broadcastPresence();
    ack?.({ ok: true });
  });

  on('story_post', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const text = typeof payload?.text === 'string' ? payload.text.trim() : '';
    const media = typeof payload?.media === 'string' && payload.media ? payload.media : null;
    if (media && !isOurMedia(media, 'image')) return ack?.({ ok: false, error: 'invalid' });
    if ((!text && !media) || text.length > 500) return ack?.({ ok: false, error: 'invalid' });
    const now = Date.now();
    const row = await db.one('INSERT INTO stories (text, media, created_at) VALUES ($1, $2, $3) RETURNING id', [text, media, now]);
    if (media) await db.run('UPDATE media SET expires_at = $1 WHERE id = $2', [now + STORY_MS, mediaIdOf(media)]);
    io.emit('story', { id: row.id, text, media, created_at: now });
    ack?.({ ok: true });
  });

  on('story_delete', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const id = Number(payload?.id);
    const row = Number.isInteger(id) ? await db.one('SELECT id, media FROM stories WHERE id = $1', [id]) : null;
    if (!row) return ack?.({ ok: false, error: 'no_story' });
    await db.run('DELETE FROM stories WHERE id = $1', [id]);
    await db.run("DELETE FROM reactions WHERE target_type = 'story' AND target_id = $1", [id]);
    const mid = mediaIdOf(row.media);
    if (mid) await db.run('DELETE FROM media WHERE id = $1', [mid]);
    io.emit('story_deleted', { id });
    ack?.({ ok: true });
  });

  on('group_create', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const kind = ['group', 'channel'].includes(payload?.kind) ? payload.kind : null;
    const title = typeof payload?.title === 'string' ? payload.title.trim() : '';
    if (!kind || !title || title.length > 40) return ack?.({ ok: false, error: 'invalid' });
    const row = await db.one('INSERT INTO chats (kind, title, owner, created_at) VALUES ($1, $2, $3, $4) RETURNING id',
      [kind, title, me, Date.now()]);
    await db.run('INSERT INTO chat_members (chat_id, nick) VALUES ($1, $2)', [row.id, me]);
    ack?.({ ok: true, id: row.id });
  });

  // Public list of every group and channel; "joined" tells whether this user is a member.
  on('group_list', async (payload, ack) => {
    const me = socketNick.get(socket.id) || '';
    const rows = await db.all('SELECT id, kind, title, owner, created_at FROM chats ORDER BY id DESC LIMIT 200');
    const out = [];
    for (const c of rows) out.push(await chatSummary(c, me));
    ack?.({ ok: true, chats: out });
  });

  on('group_join', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const c = Number.isInteger(id) ? await db.one('SELECT id FROM chats WHERE id = $1', [id]) : null;
    if (!c) return ack?.({ ok: false, error: 'no_chat' });
    await db.run('INSERT INTO chat_members (chat_id, nick) VALUES ($1, $2) ON CONFLICT DO NOTHING', [id, me]);
    ack?.({ ok: true });
  });

  on('group_leave', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const c = Number.isInteger(id) ? await db.one('SELECT id, owner FROM chats WHERE id = $1', [id]) : null;
    if (!c) return ack?.({ ok: false, error: 'no_chat' });
    if (c.owner === me) return ack?.({ ok: false, error: 'owner' });
    await db.run('DELETE FROM chat_members WHERE chat_id = $1 AND nick = $2', [id, me]);
    ack?.({ ok: true });
  });

  on('group_history', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    const id = Number(payload?.id);
    if (!me || !Number.isInteger(id) || !(await isMember(id, me))) return ack?.({ ok: false, error: 'forbidden' });
    const rows = await db.all('SELECT id, chat_id, from_nick, text, kind, created_at FROM gmessages WHERE chat_id = $1 ORDER BY id DESC LIMIT 200', [id]);
    const reacts = await reactionsFor('gmsg', rows.map(r => r.id));
    ack?.({ ok: true, messages: rows.reverse().map(r => ({ ...toGMsg(r), reactions: reacts.get(r.id) || {} })) });
  });

  on('group_send', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const c = Number.isInteger(id) ? await db.one('SELECT id, kind, owner FROM chats WHERE id = $1', [id]) : null;
    if (!c || !(await isMember(id, me))) return ack?.({ ok: false, error: 'forbidden' });
    if (c.kind === 'channel' && c.owner !== me) return ack?.({ ok: false, error: 'read_only' });
    const kind = payload?.kind === 'image' ? 'image' : 'text';
    const text = typeof payload?.text === 'string' ? payload.text.trim() : '';
    if (kind === 'image' && !isOurMedia(text, 'image')) return ack?.({ ok: false, error: 'invalid' });
    if (!text || (kind === 'text' && text.length > MAX_TEXT)) return ack?.({ ok: false, error: 'invalid' });
    const now = Date.now();
    const row = await db.one('INSERT INTO gmessages (chat_id, from_nick, text, kind, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING id',
      [id, me, text, kind, now]);
    if (kind === 'image') await db.run('UPDATE media SET expires_at = $1 WHERE id = $2', [now + CHAT_MEDIA_MS, mediaIdOf(text)]);
    const msg = { id: row.id, chat: id, from: me, text, kind, created_at: now, reactions: {} };
    for (const m of await membersOf(id)) io.to('u:' + m).emit('gmessage', { chat: id, msg });
    ack?.({ ok: true, msg });
  });

  // A group message can be deleted by its sender or by the group's owner.
  on('gmsg_delete', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const row = Number.isInteger(id) ? await db.one('SELECT id, chat_id, from_nick, kind, text FROM gmessages WHERE id = $1', [id]) : null;
    if (!row) return ack?.({ ok: false, error: 'no_message' });
    const owner = (await db.one('SELECT owner FROM chats WHERE id = $1', [row.chat_id]))?.owner;
    if (row.from_nick !== me && owner !== me) return ack?.({ ok: false, error: 'forbidden' });
    const members = await membersOf(row.chat_id);
    await db.run('DELETE FROM gmessages WHERE id = $1', [id]);
    await db.run("DELETE FROM reactions WHERE target_type = 'gmsg' AND target_id = $1", [id]);
    if (row.kind === 'image') {
      const mid = mediaIdOf(row.text);
      if (mid) await db.run('DELETE FROM media WHERE id = $1', [mid]);
    }
    for (const m of members) io.to('u:' + m).emit('gmsg_deleted', { chat: row.chat_id, id });
    ack?.({ ok: true });
  });

  // A group or channel can be deleted by its owner, or by anyone with the admin code.
  on('group_delete', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const c = Number.isInteger(id) ? await db.one('SELECT id, owner FROM chats WHERE id = $1', [id]) : null;
    if (!c) return ack?.({ ok: false, error: 'no_chat' });
    if (c.owner !== me && !adminOk(payload?.code)) return ack?.({ ok: false, error: 'forbidden' });
    const imgs = (await db.all("SELECT text FROM gmessages WHERE chat_id = $1 AND kind = 'image'", [id])).map(r => r.text);
    await tx(async cl => {
      await cl.query("DELETE FROM reactions WHERE target_type = 'gmsg' AND target_id IN (SELECT id FROM gmessages WHERE chat_id = $1)", [id]);
      await cl.query('DELETE FROM gmessages WHERE chat_id = $1', [id]);
      await cl.query('DELETE FROM chat_members WHERE chat_id = $1', [id]);
      await cl.query('DELETE FROM chats WHERE id = $1', [id]);
    });
    for (const u of imgs) {
      const mid = mediaIdOf(u);
      if (mid) await db.run('DELETE FROM media WHERE id = $1', [mid]);
    }
    io.emit('group_deleted', { id });
    ack?.({ ok: true });
  });

  // ---- Stars and gifts ----

  on('market_list', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    const items = await db.all('SELECT id, title, description, price, image FROM gift_items WHERE owner IS NULL ORDER BY price DESC, id DESC LIMIT 200');
    ack?.({ ok: true, items });
  });

  on('gifts_mine', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const u = await db.one('SELECT stars FROM users WHERE nickname = $1', [me]);
    const items = await db.all('SELECT id, title, description, price, image, on_shelf FROM gift_items WHERE owner = $1 ORDER BY id DESC', [me]);
    ack?.({ ok: true, stars: u?.stars || 0, items });
  });

  on('gift_buy', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const item = Number.isInteger(id) ? await db.one('SELECT id, price FROM gift_items WHERE id = $1 AND owner IS NULL', [id]) : null;
    if (!item) return ack?.({ ok: false, error: 'not_for_sale' });
    let error = null;
    try {
      await tx(async c => {
        const pay = await c.query('UPDATE users SET stars = stars - $1 WHERE nickname = $2 AND stars >= $1', [item.price, me]);
        if (pay.rowCount === 0) throw Object.assign(new Error('no_stars'), { code: 'no_stars' });
        const got = await c.query('UPDATE gift_items SET owner = $1 WHERE id = $2 AND owner IS NULL', [me, id]);
        if (got.rowCount === 0) throw Object.assign(new Error('sold_out'), { code: 'sold_out' });
      });
    } catch (e) {
      error = e.code || 'server';
    }
    if (error) return ack?.({ ok: false, error });
    ack?.({ ok: true });
  });

  on('gift_sell', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const item = Number.isInteger(id) ? await db.one('SELECT id, price, owner FROM gift_items WHERE id = $1', [id]) : null;
    if (!item || item.owner !== me) return ack?.({ ok: false, error: 'forbidden' });
    const amount = Math.floor(item.price * SELL_PERCENT);
    await tx(async c => {
      await c.query('UPDATE gift_items SET owner = NULL, on_shelf = false WHERE id = $1', [id]);
      await c.query('UPDATE users SET stars = stars + $1 WHERE nickname = $2', [amount, me]);
    });
    ack?.({ ok: true, amount });
  });

  on('gift_shelf', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const item = Number.isInteger(id) ? await db.one('SELECT id, owner FROM gift_items WHERE id = $1', [id]) : null;
    if (!item || item.owner !== me) return ack?.({ ok: false, error: 'forbidden' });
    await db.run('UPDATE gift_items SET on_shelf = $1 WHERE id = $2', [payload?.on === true, id]);
    ack?.({ ok: true });
  });

  on('gift_send', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const to = (await findUser(String(payload?.to || '')))?.nickname;
    if (!to) return ack?.({ ok: false, error: 'no_user' });
    if (to === me) return ack?.({ ok: false, error: 'self' });
    const id = Number(payload?.id);
    const item = Number.isInteger(id) ? await db.one('SELECT id, title, description, price, image, owner FROM gift_items WHERE id = $1', [id]) : null;
    if (!item || item.owner !== me) return ack?.({ ok: false, error: 'forbidden' });
    await db.run('UPDATE gift_items SET owner = $1, on_shelf = false WHERE id = $2', [to, id]);
    await sendGiftMessage(me, to, item);
    ack?.({ ok: true });
  });

  // Admin only: give stars to any user, including yourself.
  on('admin_stars', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const amount = Number(payload?.amount);
    if (!Number.isInteger(amount) || amount < 1 || amount > 1000000000) return ack?.({ ok: false, error: 'invalid' });
    const target = (await findUser(String(payload?.nick || '')))?.nickname;
    if (!target) return ack?.({ ok: false, error: 'no_user' });
    await db.run('UPDATE users SET stars = stars + $1 WHERE nickname = $2', [amount, target]);
    ack?.({ ok: true });
  });

  // Admin only: create a gift for the market.
  on('admin_gift_create', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const title = typeof payload?.title === 'string' ? payload.title.trim() : '';
    const description = typeof payload?.description === 'string' ? payload.description.trim().slice(0, 200) : '';
    const price = Number(payload?.price);
    const image = String(payload?.image || '');
    if (!title || title.length > 40 || !Number.isInteger(price) || price < 1 || price > 1000000 || !isOurMedia(image, 'image')) {
      return ack?.({ ok: false, error: 'invalid' });
    }
    await db.run('INSERT INTO gift_items (title, description, price, image, created_at) VALUES ($1, $2, $3, $4, $5)',
      [title, description, price, image, Date.now()]);
    await db.run('UPDATE media SET expires_at = $1 WHERE id = $2', [Date.now() + AVATAR_MS, mediaIdOf(image)]);
    ack?.({ ok: true });
  });

  // Admin only: give a gift to a user as a chat message from the admin account.
  on('admin_gift_give', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const id = Number(payload?.id);
    const item = Number.isInteger(id) ? await db.one('SELECT id, title, description, price, image FROM gift_items WHERE id = $1', [id]) : null;
    if (!item) return ack?.({ ok: false, error: 'no_gift' });
    const target = (await findUser(String(payload?.nick || '')))?.nickname;
    if (!target) return ack?.({ ok: false, error: 'no_user' });
    await db.run('UPDATE gift_items SET owner = $1, on_shelf = false WHERE id = $2', [target, id]);
    await sendGiftMessage(me, target, item);
    ack?.({ ok: true });
  });

  // Admin only: give copies of a market gift to every user, sent from the gigi_chat account.
  on('admin_gift_broadcast', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const id = Number(payload?.id);
    const count = Number(payload?.count);
    const item = Number.isInteger(id) ? await db.one('SELECT id, title, description, price, image FROM gift_items WHERE id = $1', [id]) : null;
    if (!item) return ack?.({ ok: false, error: 'no_gift' });
    if (!Number.isInteger(count) || count < 1 || count > 100) return ack?.({ ok: false, error: 'invalid' });
    const users = await db.all('SELECT nickname FROM users');
    const text = JSON.stringify({ id: item.id, title: item.title, description: item.description, price: item.price, image: item.image, count });
    for (const u of users) {
      await db.run(
        'INSERT INTO gift_items (title, description, price, image, owner, created_at) SELECT $1::text, $2::text, $3::int, $4::text, $5::text, $6::double precision FROM generate_series(1, $7::int)',
        [item.title, item.description, item.price, item.image, u.nickname, Date.now(), count]);
      const now = Date.now();
      const row = await db.one(
        "INSERT INTO messages (from_nick, to_nick, text, kind, created_at) VALUES ('gigi_chat', $1, $2, 'gift', $3) RETURNING id",
        [u.nickname, text, now]);
      io.to('u:' + u.nickname).emit('message', { id: row.id, from: 'gigi_chat', to: u.nickname, text, created_at: now, read_at: null, kind: 'gift' });
    }
    ack?.({ ok: true, users: users.length });
  });

  // Admin only: delete a gift from the market (gift cards already in chats stay).
  on('admin_gift_delete', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const id = Number(payload?.id);
    if (!Number.isInteger(id)) return ack?.({ ok: false, error: 'no_gift' });
    await db.run('DELETE FROM gift_items WHERE id = $1', [id]);
    ack?.({ ok: true });
  });

  // Toggles one reaction. Messages: only the two people in the chat see it. Stories: everyone.
  on('react', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const type = ['story', 'msg', 'gmsg'].includes(payload?.type) ? payload.type : null;
    const id = Number(payload?.id);
    const emoji = String(payload?.emoji || '');
    if (!type || !Number.isInteger(id) || !EMOJIS.includes(emoji)) return ack?.({ ok: false, error: 'invalid' });

    let people = null;
    if (type === 'gmsg') {
      const gm = await db.one('SELECT chat_id FROM gmessages WHERE id = $1', [id]);
      if (!gm || !(await isMember(gm.chat_id, me))) return ack?.({ ok: false, error: 'forbidden' });
      people = await membersOf(gm.chat_id);
    } else if (type === 'msg') {
      const m = await db.one('SELECT from_nick, to_nick FROM messages WHERE id = $1', [id]);
      if (!m || (m.from_nick !== me && m.to_nick !== me)) return ack?.({ ok: false, error: 'forbidden' });
      people = [m.from_nick, m.to_nick];
    } else {
      const s = await db.one('SELECT id FROM stories WHERE id = $1', [id]);
      if (!s) return ack?.({ ok: false, error: 'no_story' });
    }

    const has = await db.one(
      'SELECT 1 AS x FROM reactions WHERE target_type = $1 AND target_id = $2 AND nick = $3 AND emoji = $4',
      [type, id, me, emoji]);
    if (has) {
      await db.run('DELETE FROM reactions WHERE target_type = $1 AND target_id = $2 AND nick = $3 AND emoji = $4',
        [type, id, me, emoji]);
    } else {
      await db.run('INSERT INTO reactions (target_type, target_id, nick, emoji) VALUES ($1, $2, $3, $4)',
        [type, id, me, emoji]);
    }

    const reactions = (await reactionsFor(type, [id])).get(id) || {};
    const out = { type, id, reactions };
    if (people) {
      for (const p of new Set(people)) io.to('u:' + p).emit('reaction', out);
    } else {
      io.emit('reaction', out);
    }
    ack?.({ ok: true });
  });

  socket.on('disconnect', () => detach(socket));
});

async function main() {
  await pool.query(SCHEMA);
  await pool.query(EXTRA_SCHEMA);
  await cleanup().catch(e => console.error('cleanup:', e.message));
  setInterval(() => cleanup().catch(e => console.error('cleanup:', e.message)), 10 * 60 * 1000);
  server.listen(PORT, () => console.log(`gigi chat running on port ${PORT}`));
}

main().catch(e => {
  console.error('startup failed:', e.message);
  process.exit(1);
});
