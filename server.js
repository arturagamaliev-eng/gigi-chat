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
  if (!NICK_RE.test(nick)) return { ok: false, error: 'invalid_nick' };
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
  await db.run('DELETE FROM stories WHERE created_at < $1', [now - STORY_MS]);
  await db.run('DELETE FROM media WHERE expires_at < $1', [now]);
}

async function recentStories() {
  return db.all('SELECT id, text, media, created_at FROM stories WHERE created_at > $1 ORDER BY id DESC LIMIT 20',
    [Date.now() - STORY_MS]);
}

async function removeUser(nick) {
  const urls = (await db.all("SELECT text FROM messages WHERE (from_nick = $1 OR to_nick = $1) AND kind = 'image'", [nick]))
    .map(r => r.text);
  const u = await db.one('SELECT avatar FROM users WHERE nickname = $1', [nick]);
  if (u?.avatar) urls.push(u.avatar);
  await tx(async c => {
    await c.query('DELETE FROM messages WHERE from_nick = $1 OR to_nick = $1', [nick]);
    await c.query('DELETE FROM sessions WHERE nickname = $1', [nick]);
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
  res.json(rows.reverse().map(toMsg));
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
      await c.query('DELETE FROM sessions');
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
    if (!NICK_RE.test(newNick)) return ack?.({ ok: false, error: 'invalid_nick' });
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
    const mid = mediaIdOf(row.media);
    if (mid) await db.run('DELETE FROM media WHERE id = $1', [mid]);
    io.emit('story_deleted', { id });
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
