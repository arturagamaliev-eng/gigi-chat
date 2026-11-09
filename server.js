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
const MAX_MEDIA_CHARS = 20000000;              // base64 size, supports files up to about 15 MB
const CHAT_MEDIA_MS = 7 * 24 * 3600 * 1000;   // chat photos are kept 7 days
const STORY_MS = 24 * 3600 * 1000;            // stories are kept 24 hours
const AVATAR_MS = 365 * 24 * 3600 * 1000;     // profile photo stays until removed
const keyOf = s => s.toLowerCase();
const MEDIA_RE = /^\/media\/([a-f0-9]{32})$/;
const mediaIdOf = url => { const m = MEDIA_RE.exec(String(url || '')); return m ? m[1] : null; };
const MEDIA_KINDS = new Set(['image', 'video', 'audio', 'file']);
const isOurMedia = (url, kind) => MEDIA_KINDS.has(kind) && typeof url === 'string' && mediaIdOf(url) !== null;
let adminSecretCode = process.env.ADMIN_CODE || '';
const adminOk = code => Boolean(adminSecretCode) && String(code || '') === adminSecretCode;
const EMOJIS = ['👍', '❤️', '😂', '😮', '😢', '🔥'];
const SELL_PERCENT = 0.8; // a gift sold back to the market returns 80% of its price
const PREMIUM_PRICE = { 1: 1000, 12: 5000 }; // stars per month / per year
const DAY_MS = 24 * 3600 * 1000;

let appBg = null;          // background photo URL, or null for the default
let writesStopped = false; // true while nobody can write messages
let visualRules = {}; // administrator-managed visual overrides

async function loadSettings() {
  for (const r of await db.all('SELECT k, v FROM app_settings')) {
    if (r.k === 'bg') appBg = r.v || null;
    if (r.k === 'stopped') writesStopped = r.v === '1';
    if (r.k === 'admin_code' && r.v) adminSecretCode = r.v;
    if (r.k === 'visual_rules') { try { visualRules = JSON.parse(r.v || '{}') || {}; } catch { visualRules = {}; } }
  }
}

async function saveSetting(k, v) {
  await db.run('INSERT INTO app_settings (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = excluded.v', [k, v]);
}

const configPayload = () => ({ bg: appBg, stopped: writesStopped, visualRules });
const handleOk = h => /^[a-z0-9_]{3,20}$/.test(h);
const normHandle = s => String(s || '').trim().replace(/^@/, '').toLowerCase();
const isPremium = async nick => Boolean(await db.one('SELECT 1 AS x FROM users WHERE nickname = $1 AND premium_until > $2', [nick, Date.now()]));
const isBlockedBetween = async (a, other) => {
  const b = (await findUser(String(other || '')))?.nickname;
  if (!b) return false;
  return Boolean(await db.one('SELECT 1 AS x FROM blocks WHERE (blocker = $1 AND blocked = $2) OR (blocker = $2 AND blocked = $1)', [a, b]));
};

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
const toGMsg = r => ({ id: r.id, chat: r.chat_id, from: r.from_nick, text: r.text, kind: r.kind || 'text', created_at: r.created_at, pinned: Boolean(r.pinned) });

async function chatSummary(c, me) {
  const members = await membersOf(c.id);
  const last = await db.one('SELECT id, chat_id, from_nick, text, kind, created_at FROM gmessages WHERE chat_id = $1 ORDER BY id DESC LIMIT 1', [c.id]);
  return {
    id: c.id, kind: c.kind, title: c.title, owner: c.owner, avatar: c.avatar || null,
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
// The sslmode part is removed from the URL, because the SSL options are set below.
const dbUrl = process.env.DATABASE_URL.replace(/([?&])sslmode=[^&]*&?/, '$1').replace(/[?&]$/, '');
const pool = new Pool({ connectionString: dbUrl, ssl: { rejectUnauthorized: false }, max: 5 });
pool.on('error', err => console.error('db pool error:', err.message));
process.on('unhandledRejection', e => console.error('unhandled:', e && e.message));

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
  read_at DOUBLE PRECISION,
  reply_to INTEGER,
  edited_at DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_msg_from ON messages(from_nick, id);
CREATE INDEX IF NOT EXISTS idx_msg_to ON messages(to_nick, id);
ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to INTEGER;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS edited_at DOUBLE PRECISION;
CREATE TABLE IF NOT EXISTS admin_audit (id SERIAL PRIMARY KEY, actor TEXT NOT NULL, action TEXT NOT NULL, target TEXT, details TEXT, created_at DOUBLE PRECISION NOT NULL);
CREATE TABLE IF NOT EXISTS stories (
  id SERIAL PRIMARY KEY,
  text TEXT NOT NULL,
  created_at DOUBLE PRECISION NOT NULL
);
`;

const EXTRA_SCHEMA = `
ALTER TABLE stories ADD COLUMN IF NOT EXISTS media TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS user_id BIGSERIAL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen DOUBLE PRECISION;
ALTER TABLE users ADD COLUMN IF NOT EXISTS bio TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS status_text TEXT;
CREATE TABLE IF NOT EXISTS chats (
  id SERIAL PRIMARY KEY,
  kind TEXT NOT NULL,
  title TEXT NOT NULL,
  owner TEXT NOT NULL,
  created_at DOUBLE PRECISION NOT NULL,
  avatar TEXT
);
ALTER TABLE chats ADD COLUMN IF NOT EXISTS avatar TEXT;
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
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_invite_day TEXT;
ALTER TABLE stories ADD COLUMN IF NOT EXISTS author TEXT;
CREATE TABLE IF NOT EXISTS app_settings (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS complaints (
  id SERIAL PRIMARY KEY,
  from_nick TEXT NOT NULL,
  about_nick TEXT NOT NULL,
  text TEXT NOT NULL,
  created_at DOUBLE PRECISION NOT NULL
);
ALTER TABLE gmessages ADD COLUMN IF NOT EXISTS pinned BOOLEAN NOT NULL DEFAULT false;
CREATE TABLE IF NOT EXISTS story_views (
  story_id INTEGER NOT NULL,
  nick TEXT NOT NULL,
  PRIMARY KEY (story_id, nick)
);
ALTER TABLE users ADD COLUMN IF NOT EXISTS handle TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS premium_until DOUBLE PRECISION;
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_handle ON users(lower(handle));
CREATE TABLE IF NOT EXISTS blocks (
  blocker TEXT NOT NULL,
  blocked TEXT NOT NULL,
  PRIMARY KEY (blocker, blocked)
);
CREATE TABLE IF NOT EXISTS hidden_conversations (
  owner TEXT NOT NULL,
  partner TEXT NOT NULL,
  created_at DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (owner, partner)
);
ALTER TABLE users ADD COLUMN IF NOT EXISTS invite_day TEXT;
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
CREATE TABLE IF NOT EXISTS poll_votes (message_id INTEGER NOT NULL, nick TEXT NOT NULL, option_index INTEGER NOT NULL, PRIMARY KEY(message_id, nick));
CREATE TABLE IF NOT EXISTS group_roles (chat_id INTEGER NOT NULL, nick TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'member', PRIMARY KEY(chat_id, nick));
`;

const findUser = nick => db.one('SELECT nickname, key, pass_salt, pass_hash FROM users WHERE key = $1', [keyOf(nick)]);
const toMsg = r => ({ id: r.id, from: r.from_nick, to: r.to_nick, text: r.text, created_at: r.created_at, read_at: r.read_at ?? null, kind: r.kind || 'text', reply_to: r.reply_to ?? null, edited_at: r.edited_at ?? null });
const auditAdmin = (actor, action, target, details='') => db.run('INSERT INTO admin_audit (actor, action, target, details, created_at) VALUES ($1,$2,$3,$4,$5)', [actor, action, target || null, details, Date.now()]);

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
  await db.run('DELETE FROM story_views WHERE story_id NOT IN (SELECT id FROM stories)');
}

async function partnersOf(nick) {
  return (await db.all(
    'SELECT DISTINCT CASE WHEN from_nick = $1 THEN to_nick ELSE from_nick END AS p FROM messages WHERE from_nick = $1 OR to_nick = $1',
    [nick])).map(r => r.p);
}

// gigi chat stories (author is null) are for everyone. A user's own stories are for that
// user and for the people they have a chat with.
async function recentStories(me) {
  const rows = await db.all(
    `SELECT id, text, media, author, created_at, (SELECT COUNT(*)::int FROM story_views v WHERE v.story_id = stories.id) AS views FROM stories
     WHERE created_at > $1 AND (author IS NULL OR author = $2 OR author IN (
       SELECT CASE WHEN from_nick = $2 THEN to_nick ELSE from_nick END FROM messages WHERE from_nick = $2 OR to_nick = $2))
     ORDER BY id DESC LIMIT 20`,
    [Date.now() - STORY_MS, me || '']);
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
    await c.query('DELETE FROM blocks WHERE blocker = $1 OR blocked = $1', [nick]);
    await c.query('DELETE FROM stories WHERE author = $1', [nick]);
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
    ) t WHERE NOT EXISTS (SELECT 1 FROM hidden_conversations h WHERE h.owner = $1 AND h.partner = t.partner) GROUP BY partner ORDER BY last_id DESC LIMIT 100`, [me]);
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
    WHERE ((from_nick = $1 AND to_nick = $2) OR (from_nick = $2 AND to_nick = $1))
      AND NOT EXISTS (SELECT 1 FROM hidden_conversations h WHERE h.owner = $1 AND h.partner = $2)
    ORDER BY id DESC LIMIT 200`, [a, b]);
  const reacts = await reactionsFor('msg', rows.map(r => r.id));
  res.json(rows.reverse().map(r => ({ ...toMsg(r), reactions: reacts.get(r.id) || {} })));
}));

app.post('/api/upload', route(async (req, res) => {
  const s = await db.one('SELECT nickname FROM sessions WHERE token = $1', [String(req.body?.token || '')]);
  if (!s) return res.status(401).json({ error: 'bad_session' });
  const m = /^data:([a-zA-Z0-9.+-]+\/[a-zA-Z0-9.+-]+);base64,([A-Za-z0-9+/=]+)$/.exec(String(req.body?.data || ''));
  if (!m) return res.status(400).json({ error: 'bad_type' });
  const mime = m[1].toLowerCase();
  const allowed = /^(image\/(jpeg|png|webp|gif)|video\/(mp4|webm|quicktime)|audio\/(webm|ogg|mpeg|mp4|wav|aac)|application\/(pdf|zip|vnd\.openxmlformats-officedocument\.wordprocessingml\.document|vnd\.openxmlformats-officedocument\.spreadsheetml\.sheet|msword|vnd\.ms-excel)|text\/(plain|csv))$/;
  if (!allowed.test(mime)) return res.status(400).json({ error: 'bad_type' });
  if (m[2].length > MAX_MEDIA_CHARS) return res.status(413).json({ error: 'too_big' });
  const id = crypto.randomBytes(16).toString('hex');
  await db.run('INSERT INTO media (id, data, mime, expires_at) VALUES ($1, $2, $3, $4)',
    [id, m[2], mime, Date.now() + CHAT_MEDIA_MS]);
  const kind = mime.startsWith('image/') ? 'image' : mime.startsWith('video/') ? 'video' : mime.startsWith('audio/') ? 'audio' : 'file';
  res.json({ ok: true, url: '/media/' + id, kind, mime });
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
// Premium users are shown as offline to everyone.
async function broadcastPresence() {
  try {
    const list = onlineList();
    let hidden = new Set();
    if (list.length) {
      const rows = await db.all('SELECT nickname FROM users WHERE nickname = ANY($1::text[]) AND premium_until > $2', [list, Date.now()]);
      hidden = new Set(rows.map(r => r.nickname));
    }
    io.emit('presence', list.filter(n => !hidden.has(n)));
  } catch (e) {
    console.error('presence:', e.message);
  }
}

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
  socket.emit('app_config', configPayload());
  try { socket.emit('stories', await recentStories('')); } catch (e) { console.error('stories:', e.message); }

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
    socket.emit('stories', await recentStories(res.nick));
    ack?.(res);
  });

  on('change_password', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const current = typeof payload?.currentPassword === 'string' ? payload.currentPassword : '';
    const next = typeof payload?.newPassword === 'string' ? payload.newPassword : '';
    if (next.length < PASS_MIN || next.length > PASS_MAX) return ack?.({ ok: false, error: 'invalid_password' });
    const user = await findUser(me);
    if (!user || !checkPassword(current, user.pass_salt, user.pass_hash)) return ack?.({ ok: false, error: 'wrong_password' });
    const { salt, hash } = hashPassword(next);
    await db.run('UPDATE users SET pass_salt = $1, pass_hash = $2 WHERE key = $3', [salt, hash, keyOf(me)]);
    await db.run('DELETE FROM sessions WHERE nickname = $1 AND token <> $2', [me, String(payload?.token || '')]);
    ack?.({ ok: true });
  });

  on('admin_change_code', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.currentCode)) return ack?.({ ok: false, error: 'bad_code' });
    const next = typeof payload?.newCode === 'string' ? payload.newCode.trim() : '';
    if (next.length < 6 || next.length > 100) return ack?.({ ok: false, error: 'invalid_code' });
    await saveSetting('admin_code', next);
    adminSecretCode = next;
    await auditAdmin(me, 'change_admin_code', me, 'Адміністратор змінив секретний код');
    ack?.({ ok: true });
  });

  on('admin_visual_save', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const incoming = payload?.rules;
    if (!incoming || typeof incoming !== 'object' || Array.isArray(incoming) || Object.keys(incoming).length > 300) return ack?.({ ok: false, error: 'invalid_rules' });
    const clean = {};
    for (const [selector, rule] of Object.entries(incoming)) {
      if (typeof selector !== 'string' || selector.length > 1000 || !selector.startsWith('body > ') || !rule || typeof rule !== 'object') continue;
      const out = {};
      if (typeof rule.text === 'string' && rule.text.length <= 200) out.text = rule.text;
      for (const key of ['color', 'backgroundColor']) {
        const v = rule[key];
        if (typeof v === 'string' && /^(#[0-9a-fA-F]{3,8}|rgba?\([\d\s.,%]+\)|hsla?\([\d\s.,%]+\)|[a-zA-Z]{3,20})$/.test(v)) out[key] = v;
      }
      for (const key of ['fontSize', 'borderRadius']) {
        const n = Number(rule[key]);
        if (Number.isFinite(n)) out[key] = Math.max(key === 'fontSize' ? 8 : 0, Math.min(key === 'fontSize' ? 72 : 50, n)) + 'px';
      }
      for (const key of ['translateX', 'translateY']) {
        const n = Number(rule[key]);
        if (Number.isFinite(n)) out[key] = Math.max(-500, Math.min(500, n)) + 'px';
      }
      if (typeof rule.hidden === 'boolean') out.hidden = rule.hidden;
      if (Object.keys(out).length) clean[selector] = out;
    }
    visualRules = clean;
    await saveSetting('visual_rules', JSON.stringify(visualRules));
    await auditAdmin(me, 'visual_editor_save', 'site', `Збережено правил: ${Object.keys(clean).length}`);
    io.emit('app_config', configPayload());
    ack?.({ ok: true, count: Object.keys(clean).length });
  });

  on('message', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    if (await isBlockedBetween(me, payload?.to)) return ack?.({ ok: false, error: 'blocked' });
    if (writesStopped) return ack?.({ ok: false, error: 'stopped' });
    const kind = MEDIA_KINDS.has(payload?.kind) ? payload.kind : 'text';
    const text = typeof payload?.text === 'string' ? payload.text.trim() : '';
    if (MEDIA_KINDS.has(kind) && !isOurMedia(text, kind)) return ack?.({ ok: false, error: 'invalid' });
    if (!text || (kind === 'text' && text.length > MAX_TEXT)) return ack?.({ ok: false, error: 'invalid' });

    const to = (await findUser(String(payload?.to || '')))?.nickname;
    if (!to) return ack?.({ ok: false, error: 'no_user' });

    const now = Date.now();
    const row = await db.one(
      'INSERT INTO messages (from_nick, to_nick, text, kind, created_at, reply_to) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id',
      [me, to, text, kind, now, Number.isInteger(Number(payload?.reply_to)) ? Number(payload.reply_to) : null]);
    if (MEDIA_KINDS.has(kind)) await db.run('UPDATE media SET expires_at = $1 WHERE id = $2', [now + CHAT_MEDIA_MS, mediaIdOf(text)]);
    const msg = { id: row.id, from: me, to, text, created_at: now, read_at: null, kind, reply_to: Number.isInteger(Number(payload?.reply_to)) ? Number(payload.reply_to) : null, edited_at: null };
    io.to('u:' + to).to('u:' + me).emit('message', msg);
    ack?.({ ok: true, msg });
  });

  on('read', async payload => {
    const me = socketNick.get(socket.id);
    if (!me) return;
    if (await isPremium(me)) return; // premium users hide their read status
    const partner = (await findUser(String(payload?.with || '')))?.nickname;
    if (!partner || partner === me) return;
    const now = Date.now();
    const r = await db.run('UPDATE messages SET read_at = $1 WHERE from_nick = $2 AND to_nick = $3 AND read_at IS NULL', [now, partner, me]);
    if (r.rowCount > 0) io.to('u:' + partner).emit('read', { by: me, at: now });
  });

  on('msg_edit', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id), text = typeof payload?.text === 'string' ? payload.text.trim() : '';
    if (!Number.isInteger(id) || !text || text.length > MAX_TEXT) return ack?.({ ok: false, error: 'invalid' });
    const row = await db.one("SELECT id, from_nick, to_nick, kind FROM messages WHERE id = $1", [id]);
    if (!row || row.from_nick !== me || row.kind !== 'text') return ack?.({ ok: false, error: 'forbidden' });
    const now = Date.now();
    await db.run('UPDATE messages SET text = $1, edited_at = $2 WHERE id = $3', [text, now, id]);
    const msg = { id, from: me, to: row.to_nick, text, edited_at: now };
    io.to('u:' + row.to_nick).to('u:' + me).emit('msg_edited', msg);
    ack?.({ ok: true, msg });
  });

  on('admin_audit_list', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const rows = await db.all('SELECT actor, action, target, details, created_at FROM admin_audit ORDER BY id DESC LIMIT 200');
    ack?.({ ok: true, items: rows });
  });

  on('msg_delete', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const row = Number.isInteger(id) ? await db.one('SELECT id, from_nick, to_nick, kind, text FROM messages WHERE id = $1', [id]) : null;
    const premiumDelete = Boolean(row) && row.to_nick === me && await isPremium(me);
    if (!row || (row.from_nick !== me && !premiumDelete)) return ack?.({ ok: false, error: 'forbidden' });
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
    const row = await db.one('SELECT user_id, nickname, avatar, bio, status_text, last_seen, premium_until FROM users WHERE nickname = $1', [u.nickname]);
    if (row.premium_until > Date.now()) {
      return ack?.({ ok: true, info: {
        nick: row.nickname, id: row.user_id, hidden: true, online: false, lastSeen: null,
        avatar: row.avatar, bio: row.bio, statusText: row.status_text || '',
        shelf: await db.all('SELECT id, title, image FROM gift_items WHERE owner = $1 AND on_shelf = true', [row.nickname]),
      } });
    }
    ack?.({ ok: true, info: {
      nick: row.nickname,
      id: row.user_id,
      online: online.has(row.nickname),
      lastSeen: row.last_seen,
      avatar: row.avatar,
      bio: row.bio, statusText: row.status_text || '',
      shelf: await db.all('SELECT id, title, image FROM gift_items WHERE owner = $1 AND on_shelf = true', [row.nickname]),
    } });
  });

  on('set_profile_status', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const status = typeof payload?.status === 'string' ? payload.status.trim().slice(0, 80) : '';
    await db.run('UPDATE users SET status_text = $1 WHERE nickname = $2', [status || null, me]);
    io.emit('profile_status', { nick: me, statusText: status });
    ack?.({ ok: true, statusText: status });
  });

  // Public user lookup by numeric account ID; exposes only profile identity fields.
  on('user_lookup_id', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    const rawId = String(payload?.id || '').trim();
    if (!/^\d{1,20}$/.test(rawId)) return ack?.({ ok: false, error: 'invalid_id' });
    const user = await db.one('SELECT nickname, avatar, bio FROM users WHERE user_id = $1', [rawId]);
    if (!user) return ack?.({ ok: false, error: 'not_found' });
    ack?.({ ok: true, user: { nick: user.nickname, avatar: user.avatar || null, bio: user.bio || '' } });
  });

  // Admin only: every account, including ones with no chats yet.
  on('admin_stats', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const users = await db.one('SELECT COUNT(*)::int AS n FROM users');
    const messages = await db.one('SELECT COUNT(*)::int AS n FROM messages');
    const groups = await db.one("SELECT COUNT(*)::int AS n FROM chats WHERE kind = 'group'");
    const channels = await db.one("SELECT COUNT(*)::int AS n FROM chats WHERE kind = 'channel'");
    const complaints = await db.one('SELECT COUNT(*)::int AS n FROM complaints');
    ack?.({ ok: true, users: users.n, messages: messages.n, groups: groups.n, channels: channels.n, complaints: complaints.n, online: online.size });
  });

  on('admin_groups', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const rows = await db.all('SELECT c.id, c.kind, c.title, c.owner, c.avatar, c.created_at, COUNT(m.nick)::int AS members FROM chats c LEFT JOIN chat_members m ON m.chat_id = c.id GROUP BY c.id ORDER BY c.created_at DESC LIMIT 300');
    ack?.({ ok: true, items: rows });
  });

  on('admin_list', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const q = String(payload?.q || '').trim().slice(0, 80);
    const rows = await db.all(
      `SELECT user_id, nickname, avatar, bio, last_seen, created_at FROM users
       WHERE ($1 = '' OR nickname ILIKE $2 OR user_id::text = $1)
       ORDER BY nickname`,
      [q, `%${q.replace(/[\\%_]/g, '\\$&')}%`]);
    ack?.({ ok: true, users: rows.map(r => ({
      id: r.user_id, nick: r.nickname, online: online.has(r.nickname),
      lastSeen: r.last_seen, created: r.created_at,
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


  on('admin_clear_histories', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const counts = await db.one(
      'SELECT (SELECT COUNT(*)::int FROM messages) AS private_count, (SELECT COUNT(*)::int FROM gmessages) AS group_count');
    await tx(async c => {
      await c.query("DELETE FROM reactions WHERE target_type IN ('msg', 'gmsg')");
      await c.query('DELETE FROM poll_votes');
      await c.query('DELETE FROM messages');
      await c.query('DELETE FROM gmessages');
    });
    await auditAdmin(me, 'clear_all_histories', 'all_chats',
      `private=${counts.private_count}; group=${counts.group_count}`);
    io.emit('histories_cleared');
    ack?.({ ok: true, private: counts.private_count, group: counts.group_count });
  });

  on('admin_add_member', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const id = Number(payload?.chatId);
    const target = (await findUser(String(payload?.nick || '')))?.nickname;
    const chat = Number.isInteger(id) ? await db.one('SELECT id, title FROM chats WHERE id = $1', [id]) : null;
    if (!chat) return ack?.({ ok: false, error: 'no_chat' });
    if (!target) return ack?.({ ok: false, error: 'no_user' });
    const existed = await isMember(id, target);
    await db.run('INSERT INTO chat_members (chat_id, nick) VALUES ($1, $2) ON CONFLICT DO NOTHING', [id, target]);
    await db.run("INSERT INTO group_roles (chat_id, nick, role) VALUES ($1, $2, 'member') ON CONFLICT DO NOTHING", [id, target]);
    if (!existed) await auditAdmin(me, 'add_group_member', target, `chat=${id}; title=${chat.title}`);
    ack?.({ ok: true, already: existed });
  });

  on('group_set_avatar', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const chat = Number.isInteger(id) ? await db.one('SELECT id, owner, title, avatar FROM chats WHERE id = $1', [id]) : null;
    if (!chat) return ack?.({ ok: false, error: 'no_chat' });
    const isAdmin = adminOk(payload?.code);
    if (chat.owner !== me && !isAdmin) return ack?.({ ok: false, error: 'forbidden' });
    const raw = payload?.url;
    if (raw && !isOurMedia(String(raw), 'image')) return ack?.({ ok: false, error: 'invalid' });
    const oldId = mediaIdOf(chat.avatar);
    const url = raw ? String(raw) : null;
    await db.run('UPDATE chats SET avatar = $1 WHERE id = $2', [url, id]);
    if (url) await db.run('UPDATE media SET expires_at = $1 WHERE id = $2', [Date.now() + AVATAR_MS, mediaIdOf(url)]);
    if (oldId && chat.avatar !== url) await db.run('DELETE FROM media WHERE id = $1', [oldId]);
    await auditAdmin(me, 'change_group_avatar', String(id), `${chat.title}; via=${isAdmin ? 'admin' : 'owner'}`);
    io.emit('group_avatar', { id, url });
    ack?.({ ok: true, avatar: url });
  });

  on('admin_delete', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const target = (await findUser(String(payload?.nick || '')))?.nickname;
    if (!target) return ack?.({ ok: false, error: 'no_user' });
    await auditAdmin(me, 'delete_account', target);
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
    await db.run("INSERT INTO group_roles (chat_id, nick, role) VALUES ($1, $2, 'owner') ON CONFLICT DO NOTHING", [row.id, me]);
    ack?.({ ok: true, id: row.id });
  });

  // Public list of every group and channel; "joined" tells whether this user is a member.
  on('group_list', async (payload, ack) => {
    const me = socketNick.get(socket.id) || '';
    const rows = await db.all('SELECT id, kind, title, owner, avatar, created_at FROM chats ORDER BY id DESC LIMIT 200');
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
    await db.run("INSERT INTO group_roles (chat_id, nick, role) VALUES ($1, $2, 'member') ON CONFLICT DO NOTHING", [id, me]);
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
    await db.run('DELETE FROM group_roles WHERE chat_id = $1 AND nick = $2', [id, me]);
    ack?.({ ok: true });
  });

  on('group_history', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    const id = Number(payload?.id);
    if (!me || !Number.isInteger(id) || !(await isMember(id, me))) return ack?.({ ok: false, error: 'forbidden' });
    const rows = await db.all('SELECT id, chat_id, from_nick, text, kind, created_at, pinned FROM gmessages WHERE chat_id = $1 ORDER BY id DESC LIMIT 200', [id]);
    const reacts = await reactionsFor('gmsg', rows.map(r => r.id));
    const messages = [];
    for (const r of rows.reverse()) {
      const m = { ...toGMsg(r), reactions: reacts.get(r.id) || {} };
      if (m.kind === 'poll') {
        const votes = await db.all('SELECT option_index, COUNT(*)::int AS n FROM poll_votes WHERE message_id = $1 GROUP BY option_index', [m.id]);
        const mine = await db.one('SELECT option_index FROM poll_votes WHERE message_id = $1 AND nick = $2', [m.id, me]);
        m.pollResults = votes.reduce((a, v) => ({ ...a, [v.option_index]: v.n }), {});
        m.myVote = mine ? mine.option_index : null;
      }
      messages.push(m);
    }
    ack?.({ ok: true, messages });
  });

  on('group_send', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const c = Number.isInteger(id) ? await db.one('SELECT id, kind, owner FROM chats WHERE id = $1', [id]) : null;
    if (!c || !(await isMember(id, me))) return ack?.({ ok: false, error: 'forbidden' });
    if (writesStopped) return ack?.({ ok: false, error: 'stopped' });
    if (c.kind === 'channel' && c.owner !== me) return ack?.({ ok: false, error: 'read_only' });
    const kind = MEDIA_KINDS.has(payload?.kind) ? payload.kind : 'text';
    const text = typeof payload?.text === 'string' ? payload.text.trim() : '';
    if (MEDIA_KINDS.has(kind) && !isOurMedia(text, kind)) return ack?.({ ok: false, error: 'invalid' });
    if (!text || (kind === 'text' && text.length > MAX_TEXT)) return ack?.({ ok: false, error: 'invalid' });
    const now = Date.now();
    const row = await db.one('INSERT INTO gmessages (chat_id, from_nick, text, kind, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING id',
      [id, me, text, kind, now]);
    if (MEDIA_KINDS.has(kind)) await db.run('UPDATE media SET expires_at = $1 WHERE id = $2', [now + CHAT_MEDIA_MS, mediaIdOf(text)]);
    const msg = { id: row.id, chat: id, from: me, text, kind, created_at: now, reactions: {} };
    for (const m of await membersOf(id)) io.to('u:' + m).emit('gmessage', { chat: id, msg });
    ack?.({ ok: true, msg });
  });

  // Create a group poll. Each member may select one option; a new vote replaces the old one.
  on('group_poll_create', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    const id = Number(payload?.id);
    const question = typeof payload?.question === 'string' ? payload.question.trim().slice(0, 180) : '';
    const options = Array.isArray(payload?.options) ? payload.options.map(x => String(x).trim().slice(0, 100)).filter(Boolean).slice(0, 8) : [];
    const c = Number.isInteger(id) ? await db.one('SELECT id, kind, owner FROM chats WHERE id = $1', [id]) : null;
    if (!me || !c || !(await isMember(id, me))) return ack?.({ ok: false, error: 'forbidden' });
    if (writesStopped || (c.kind === 'channel' && c.owner !== me)) return ack?.({ ok: false, error: 'read_only' });
    if (!question || options.length < 2 || new Set(options.map(x => x.toLowerCase())).size < 2) return ack?.({ ok: false, error: 'invalid_poll' });
    const text = JSON.stringify({ question, options });
    const now = Date.now();
    const row = await db.one("INSERT INTO gmessages (chat_id, from_nick, text, kind, created_at) VALUES ($1, $2, $3, 'poll', $4) RETURNING id", [id, me, text, now]);
    const msg = { id: row.id, chat: id, from: me, text, kind: 'poll', created_at: now, reactions: {}, pollResults: {}, myVote: null };
    for (const m of await membersOf(id)) io.to('u:' + m).emit('gmessage', { chat: id, msg });
    ack?.({ ok: true, msg });
  });

  on('poll_vote', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    const id = Number(payload?.id), option = Number(payload?.option);
    const poll = Number.isInteger(id) ? await db.one("SELECT id, chat_id, text FROM gmessages WHERE id = $1 AND kind = 'poll'", [id]) : null;
    if (!me || !poll || !(await isMember(poll.chat_id, me))) return ack?.({ ok: false, error: 'forbidden' });
    let data; try { data = JSON.parse(poll.text); } catch { return ack?.({ ok: false, error: 'invalid_poll' }); }
    if (!Number.isInteger(option) || option < 0 || option >= data.options.length) return ack?.({ ok: false, error: 'invalid_option' });
    await db.run('INSERT INTO poll_votes (message_id, nick, option_index) VALUES ($1, $2, $3) ON CONFLICT (message_id, nick) DO UPDATE SET option_index = excluded.option_index', [id, me, option]);
    const votes = await db.all('SELECT option_index, COUNT(*)::int AS n FROM poll_votes WHERE message_id = $1 GROUP BY option_index', [id]);
    const pollResults = votes.reduce((a, v) => ({ ...a, [v.option_index]: v.n }), {});
    for (const member of await membersOf(poll.chat_id)) io.to('u:' + member).emit('poll_updated', { chat: poll.chat_id, id, pollResults, myVote: member === me ? option : undefined });
    ack?.({ ok: true, pollResults, myVote: option });
  });

  // Owners manage members and moderators; platform admins can also remove/promote using the admin code.
  on('group_moderate', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    const id = Number(payload?.id), target = String(payload?.nick || '').trim(), action = String(payload?.action || '');
    const c = Number.isInteger(id) ? await db.one('SELECT id, owner FROM chats WHERE id = $1', [id]) : null;
    if (!me || !c || !target || !(await isMember(id, target))) return ack?.({ ok: false, error: 'no_member' });
    const admin = adminOk(payload?.code);
    if (c.owner !== me && !admin) return ack?.({ ok: false, error: 'forbidden' });
    if (target === c.owner) return ack?.({ ok: false, error: 'owner_protected' });
    if (action === 'remove') {
      await db.run('DELETE FROM chat_members WHERE chat_id = $1 AND nick = $2', [id, target]);
      await db.run('DELETE FROM group_roles WHERE chat_id = $1 AND nick = $2', [id, target]);
      await db.run('DELETE FROM poll_votes WHERE message_id IN (SELECT id FROM gmessages WHERE chat_id = $1) AND nick = $2', [id, target]);
      io.to('u:' + target).emit('group_removed', { id });
    } else if (action === 'moderator' || action === 'member') {
      await db.run('INSERT INTO group_roles (chat_id, nick, role) VALUES ($1, $2, $3) ON CONFLICT (chat_id, nick) DO UPDATE SET role = excluded.role', [id, target, action]);
    } else return ack?.({ ok: false, error: 'invalid_action' });
    ack?.({ ok: true });
  });

  on('group_moderators', async (payload, ack) => {
    const me = socketNick.get(socket.id), id = Number(payload?.id);
    const c = Number.isInteger(id) ? await db.one('SELECT id, owner FROM chats WHERE id = $1', [id]) : null;
    if (!me || !c || !(await isMember(id, me)) || (c.owner !== me && !adminOk(payload?.code))) return ack?.({ ok: false, error: 'forbidden' });
    const members = await db.all("SELECT m.nick, COALESCE(r.role, CASE WHEN m.nick = $2 THEN 'owner' ELSE 'member' END) AS role FROM chat_members m LEFT JOIN group_roles r ON r.chat_id = m.chat_id AND r.nick = m.nick WHERE m.chat_id = $1 ORDER BY CASE WHEN m.nick = $2 THEN 0 ELSE 1 END, m.nick", [id, c.owner]);
    ack?.({ ok: true, members });
  });

  // A group message can be deleted by its sender or by the group's owner.
  on('gmsg_delete', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const row = Number.isInteger(id) ? await db.one('SELECT id, chat_id, from_nick, kind, text FROM gmessages WHERE id = $1', [id]) : null;
    if (!row) return ack?.({ ok: false, error: 'no_message' });
    const owner = (await db.one('SELECT owner FROM chats WHERE id = $1', [row.chat_id]))?.owner;
    const role = (await db.one('SELECT role FROM group_roles WHERE chat_id = $1 AND nick = $2', [row.chat_id, me]))?.role;
    if (row.from_nick !== me && owner !== me && role !== 'moderator' && !adminOk(payload?.code)) return ack?.({ ok: false, error: 'forbidden' });
    const members = await membersOf(row.chat_id);
    await db.run('DELETE FROM poll_votes WHERE message_id = $1', [id]);
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
    if (c.owner !== me) await auditAdmin(me, 'delete_group', String(id), c.title);
    const imgs = (await db.all("SELECT text FROM gmessages WHERE chat_id = $1 AND kind = 'image'", [id])).map(r => r.text);
    await tx(async cl => {
      await cl.query('DELETE FROM poll_votes WHERE message_id IN (SELECT id FROM gmessages WHERE chat_id = $1)', [id]);
      await cl.query('DELETE FROM group_roles WHERE chat_id = $1', [id]);
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

  // Invite reward: 15 stars, once per day per account.
  on('invite_reward', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const today = new Date().toISOString().slice(0, 10);
    const row = await db.one('SELECT invite_day FROM users WHERE nickname = $1', [me]);
    if (row?.invite_day === today) return ack?.({ ok: false, error: 'already' });
    const u = await db.one('UPDATE users SET stars = stars + 15, invite_day = $2 WHERE nickname = $1 RETURNING stars', [me, today]);
    ack?.({ ok: true, stars: u.stars });
  });

  // A user may hide a conversation only from their own account, or permanently delete it for both participants.
  on('conversation_remove', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const partner = (await findUser(String(payload?.nick || '')))?.nickname;
    if (!partner || partner === me) return ack?.({ ok: false, error: 'no_user' });
    const mode = payload?.mode;
    if (mode === 'self') {
      await db.run('INSERT INTO hidden_conversations (owner, partner, created_at) VALUES ($1, $2, $3) ON CONFLICT (owner, partner) DO UPDATE SET created_at = EXCLUDED.created_at', [me, partner, Date.now()]);
      ack?.({ ok: true, mode: 'self' });
      return;
    }
    if (mode === 'both') {
      await tx(async c => {
        const ids = (await c.query('SELECT id FROM messages WHERE (from_nick = $1 AND to_nick = $2) OR (from_nick = $2 AND to_nick = $1)', [me, partner])).rows.map(r => r.id);
        if (ids.length) {
          await c.query("DELETE FROM reactions WHERE target_type = 'msg' AND target_id = ANY($1::int[])", [ids]);
          await c.query('DELETE FROM poll_votes WHERE message_id = ANY($1::int[])', [ids]);
        }
        await c.query('DELETE FROM messages WHERE (from_nick = $1 AND to_nick = $2) OR (from_nick = $2 AND to_nick = $1)', [me, partner]);
        await c.query('DELETE FROM hidden_conversations WHERE (owner = $1 AND partner = $2) OR (owner = $2 AND partner = $1)', [me, partner]);
      });
      io.to('u:' + me).to('u:' + partner).emit('conversation_removed_both', { a: me, b: partner });
      ack?.({ ok: true, mode: 'both' });
      return;
    }
    ack?.({ ok: false, error: 'invalid_mode' });
  });

  on('block', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const target = (await findUser(String(payload?.nick || '')))?.nickname;
    if (!target || target === me) return ack?.({ ok: false, error: 'no_user' });
    await db.run('INSERT INTO blocks (blocker, blocked) VALUES ($1, $2) ON CONFLICT DO NOTHING', [me, target]);
    ack?.({ ok: true });
  });

  on('unblock', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    await db.run('DELETE FROM blocks WHERE blocker = $1 AND blocked = $2', [me, String(payload?.nick || '')]);
    ack?.({ ok: true });
  });

  on('blocked_list', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const rows = await db.all('SELECT blocked FROM blocks WHERE blocker = $1 ORDER BY blocked', [me]);
    ack?.({ ok: true, nicks: rows.map(r => r.blocked) });
  });

  on('me_info', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const u = await db.one('SELECT stars, handle, premium_until FROM users WHERE nickname = $1', [me]);
    ack?.({ ok: true, stars: u.stars, handle: u.handle, premium_until: u.premium_until || 0 });
  });

  on('set_handle', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const handle = normHandle(payload?.handle);
    if (!handleOk(handle)) return ack?.({ ok: false, error: 'invalid_handle' });
    const taken = await db.one('SELECT 1 AS x FROM users WHERE lower(handle) = $1 AND nickname <> $2', [handle, me]);
    if (taken) return ack?.({ ok: false, error: 'handle_taken' });
    await db.run('UPDATE users SET handle = $1 WHERE nickname = $2', [handle, me]);
    ack?.({ ok: true, handle });
  });

  // Premium: 1000 stars for a month, 5000 stars for a year. Time is added to the current period.
  on('premium_buy', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const months = Number(payload?.months);
    const cost = PREMIUM_PRICE[months];
    if (!cost) return ack?.({ ok: false, error: 'invalid' });
    let error = null;
    let until = 0;
    try {
      await tx(async c => {
        const pay = await c.query('UPDATE users SET stars = stars - $1 WHERE nickname = $2 AND stars >= $1', [cost, me]);
        if (pay.rowCount === 0) throw Object.assign(new Error('no_stars'), { code: 'no_stars' });
        const u = await c.query('SELECT premium_until FROM users WHERE nickname = $1', [me]);
        const base = Math.max(Date.now(), u.rows[0].premium_until || 0);
        until = base + (months === 12 ? 365 : 30) * DAY_MS;
        await c.query('UPDATE users SET premium_until = $1 WHERE nickname = $2', [until, me]);
      });
    } catch (e) {
      error = e.code || 'server';
    }
    if (error) return ack?.({ ok: false, error });
    broadcastPresence();
    ack?.({ ok: true, premium_until: until });
  });

  // Admin only: set the background photo for everyone (empty url = default background).
  on('admin_set_bg', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const url = String(payload?.url || '');
    if (url && !isOurMedia(url, 'image')) return ack?.({ ok: false, error: 'invalid' });
    appBg = url || null;
    await saveSetting('bg', url);
    if (url) await db.run('UPDATE media SET expires_at = $1 WHERE id = $2', [Date.now() + AVATAR_MS, mediaIdOf(url)]);
    io.emit('app_config', configPayload());
    ack?.({ ok: true });
  });

  // Admin only: stop or allow writing for everyone.
  on('admin_stop', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    writesStopped = payload?.on === true;
    await saveSetting('stopped', writesStopped ? '1' : '0');
    io.emit('app_config', configPayload());
    ack?.({ ok: true });
  });

  // Admin only: one message from gigi chat to every user.
  on('admin_notify', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const text = typeof payload?.text === 'string' ? payload.text.trim().slice(0, 500) : '';
    if (!text) return ack?.({ ok: false, error: 'invalid' });
    const users = await db.all('SELECT nickname FROM users');
    for (const u of users) {
      const now = Date.now();
      const row = await db.one(
        "INSERT INTO messages (from_nick, to_nick, text, kind, created_at) VALUES ('gigi_chat', $1, $2, 'text', $3) RETURNING id",
        [u.nickname, text, now]);
      io.to('u:' + u.nickname).emit('message', { id: row.id, from: 'gigi_chat', to: u.nickname, text, created_at: now, read_at: null, kind: 'text' });
    }
    ack?.({ ok: true, users: users.length });
  });

  // Anyone can complain about a user; the complaint goes to the admin's inbox.
  on('complaint', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const about = (await findUser(String(payload?.nick || '')))?.nickname;
    const text = typeof payload?.text === 'string' ? payload.text.trim().slice(0, 500) : '';
    if (!about || !text || about === me) return ack?.({ ok: false, error: 'invalid' });
    await db.run('INSERT INTO complaints (from_nick, about_nick, text, created_at) VALUES ($1, $2, $3, $4)',
      [me, about, text, Date.now()]);
    ack?.({ ok: true });
  });

  on('admin_complaints', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const rows = await db.all('SELECT id, from_nick, about_nick, text, created_at FROM complaints ORDER BY id DESC LIMIT 200');
    ack?.({ ok: true, items: rows.map(r => ({ id: r.id, from: r.from_nick, about: r.about_nick, text: r.text, created: r.created_at })) });
  });

  on('admin_complaint_delete', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const id = Number(payload?.id);
    if (Number.isInteger(id)) await db.run('DELETE FROM complaints WHERE id = $1', [id]);
    ack?.({ ok: true });
  });

  // Admin review is limited to the private conversation directly related to a complaint.
  // Every successful access is recorded in the admin audit log.
  on('admin_complaint_conversation', async (payload, ack) => {
    const actor = socketNick.get(socket.id);
    if (!actor) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const id = Number(payload?.complaintId);
    if (!Number.isInteger(id) || id <= 0) return ack?.({ ok: false, error: 'invalid' });
    const complaint = await db.one('SELECT id, from_nick, about_nick FROM complaints WHERE id = $1', [id]);
    if (!complaint) return ack?.({ ok: false, error: 'no_complaint' });
    const rows = await db.all(
      `SELECT id, from_nick, to_nick, text, kind, created_at, read_at
       FROM messages
       WHERE (from_nick = $1 AND to_nick = $2) OR (from_nick = $2 AND to_nick = $1)
       ORDER BY id ASC LIMIT 1000`,
      [complaint.from_nick, complaint.about_nick]);
    await auditAdmin(actor, 'review_complaint_conversation', `${complaint.from_nick} ↔ ${complaint.about_nick}`, `complaint_id=${id}; messages=${rows.length}`);
    ack?.({ ok: true, complaint: { id, from: complaint.from_nick, about: complaint.about_nick }, messages: rows.map(r => ({
      id: r.id, from: r.from_nick, to: r.to_nick, text: r.text, kind: r.kind,
      created: r.created_at, read: r.read_at,
    })) });
  });

  // A user posts a story: it is shown to that user and to everyone they have a chat with.
  on('user_story_post', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const text = typeof payload?.text === 'string' ? payload.text.trim() : '';
    const media = typeof payload?.media === 'string' && payload.media ? payload.media : null;
    if (media && !isOurMedia(media, 'image')) return ack?.({ ok: false, error: 'invalid' });
    if ((!text && !media) || text.length > 500) return ack?.({ ok: false, error: 'invalid' });
    const now = Date.now();
    const row = await db.one('INSERT INTO stories (text, media, author, created_at) VALUES ($1, $2, $3, $4) RETURNING id',
      [text, media, me, now]);
    if (media) await db.run('UPDATE media SET expires_at = $1 WHERE id = $2', [now + STORY_MS, mediaIdOf(media)]);
    const story = { id: row.id, text, media, author: me, created_at: now, reactions: {} };
    for (const p of [me, ...(await partnersOf(me))]) io.to('u:' + p).emit('story', story);
    ack?.({ ok: true });
  });

  // The author can delete their own story.
  on('user_story_delete', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const row = Number.isInteger(id) ? await db.one('SELECT id, media, author FROM stories WHERE id = $1', [id]) : null;
    if (!row || row.author !== me) return ack?.({ ok: false, error: 'forbidden' });
    await db.run('DELETE FROM stories WHERE id = $1', [id]);
    await db.run("DELETE FROM reactions WHERE target_type = 'story' AND target_id = $1", [id]);
    const mid = mediaIdOf(row.media);
    if (mid) await db.run('DELETE FROM media WHERE id = $1', [mid]);
    io.emit('story_deleted', { id });
    ack?.({ ok: true });
  });

  // Search: text of your private messages and of messages in the groups you belong to.
  on('search_messages', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const q = typeof payload?.q === 'string' ? payload.q.trim().slice(0, 100) : '';
    if (!q) return ack?.({ ok: true, results: [] });
    const like = `%${q.replace(/[\\%_]/g, '\\$&')}%`;
    const pm = await db.all(
      "SELECT id, from_nick, to_nick, text, created_at FROM messages WHERE kind = 'text' AND (from_nick = $1 OR to_nick = $1) AND text ILIKE $2 ORDER BY id DESC LIMIT 30",
      [me, like]);
    const gm = await db.all(
      `SELECT g.id, g.chat_id, g.from_nick, g.text, g.created_at, c.title FROM gmessages g
       JOIN chats c ON c.id = g.chat_id
       JOIN chat_members m ON m.chat_id = g.chat_id AND m.nick = $1
       WHERE g.kind = 'text' AND g.text ILIKE $2 ORDER BY g.id DESC LIMIT 30`,
      [me, like]);
    const results = [
      ...pm.map(r => ({ scope: 'pm', partner: r.from_nick === me ? r.to_nick : r.from_nick, id: r.id, from: r.from_nick, text: r.text, created_at: r.created_at })),
      ...gm.map(r => ({ scope: 'group', chat: r.chat_id, title: r.title, id: r.id, from: r.from_nick, text: r.text, created_at: r.created_at })),
    ].sort((a, b) => b.created_at - a.created_at).slice(0, 50);
    ack?.({ ok: true, results });
  });

  // The owner of a group or channel pins one message (or unpins it).
  on('gmsg_pin', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const on = payload?.on === true;
    const row = Number.isInteger(id) ? await db.one('SELECT id, chat_id FROM gmessages WHERE id = $1', [id]) : null;
    if (!row) return ack?.({ ok: false, error: 'no_message' });
    const owner = (await db.one('SELECT owner FROM chats WHERE id = $1', [row.chat_id]))?.owner;
    if (owner !== me) return ack?.({ ok: false, error: 'forbidden' });
    await db.run('UPDATE gmessages SET pinned = false WHERE chat_id = $1', [row.chat_id]);
    if (on) await db.run('UPDATE gmessages SET pinned = true WHERE id = $1', [id]);
    for (const m of await membersOf(row.chat_id)) {
      io.to('u:' + m).emit('gpinned', { chat: row.chat_id, id: on ? id : null });
    }
    ack?.({ ok: true });
  });

  // Viewing someone else's story is counted once per person; the author gets the new count.
  on('story_view', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const id = Number(payload?.id);
    const row = Number.isInteger(id) ? await db.one('SELECT id, author FROM stories WHERE id = $1', [id]) : null;
    if (!row) return ack?.({ ok: false, error: 'no_story' });
    if (row.author && row.author !== me) {
      await db.run('INSERT INTO story_views (story_id, nick) VALUES ($1, $2) ON CONFLICT DO NOTHING', [id, me]);
      const v = await db.one('SELECT COUNT(*)::int AS n FROM story_views WHERE story_id = $1', [id]);
      io.to('u:' + row.author).emit('story_views', { id, views: v.n });
    }
    ack?.({ ok: true });
  });

  // 15 stars once per day for inviting a friend. The day is counted in UTC.
  on('invite_reward', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const today = new Date().toISOString().slice(0, 10);
    const row = await db.one(
      'UPDATE users SET stars = stars + 15, last_invite_day = $1 WHERE nickname = $2 AND (last_invite_day IS NULL OR last_invite_day <> $1) RETURNING stars',
      [today, me]);
    if (!row) return ack?.({ ok: false, error: 'already' });
    ack?.({ ok: true, stars: row.stars });
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

  // Admin only: reset a user's star balance or revoke their Premium.
  on('admin_revoke_perk', async (payload, ack) => {
    const actor = socketNick.get(socket.id);
    if (!actor) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const target = (await findUser(String(payload?.nick || '')))?.nickname;
    if (!target) return ack?.({ ok: false, error: 'no_user' });
    const mode = String(payload?.mode || '');
    if (mode === 'stars') {
      const result = await db.run('UPDATE users SET stars = 0 WHERE nickname = $1', [target]);
      await auditAdmin(actor, 'admin_remove_all_stars', target, 'Баланс зірок встановлено на 0');
      return ack?.({ ok: true, mode, target });
    }
    if (mode === 'premium') {
      await db.run('UPDATE users SET premium_until = NULL WHERE nickname = $1', [target]);
      await auditAdmin(actor, 'admin_revoke_premium', target, 'Premium вимкнено адміністратором');
      return ack?.({ ok: true, mode, target });
    }
    ack?.({ ok: false, error: 'invalid' });
  });

  // Admin only: create a gift for the market.
  on('admin_gift_create', async (payload, ack) => {
    if (!socketNick.get(socket.id)) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const title = typeof payload?.title === 'string' ? payload.title.trim() : '';
    const description = typeof payload?.description === 'string' ? payload.description.trim().slice(0, 200) : '';
    const price = Number(payload?.price);
    const count = Number(payload?.count);
    if (!Number.isInteger(count) || count < 1 || count > 100) return ack?.({ ok: false, error: 'invalid' });
    const image = String(payload?.image || '');
    if (!title || title.length > 40 || !Number.isInteger(price) || price < 1 || price > 1000000 || !isOurMedia(image, 'image')) {
      return ack?.({ ok: false, error: 'invalid' });
    }
    await db.run(
      'INSERT INTO gift_items (title, description, price, image, created_at) SELECT $1::text, $2::text, $3::int, $4::text, $5::double precision FROM generate_series(1, $6::int)',
      [title, description, price, image, Date.now(), count]);
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

  // Admin only: permanently remove every gift item from the market and users' inventories.
  on('admin_gifts_delete_all', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    if (!adminOk(payload?.code)) return ack?.({ ok: false, error: 'bad_code' });
    const result = await db.run('DELETE FROM gift_items');
    const deleted = Number(result?.rowCount ?? result?.changes ?? 0);
    await auditAdmin(me, 'delete_all_gifts', 'all_gifts', `deleted=${deleted}`);
    ack?.({ ok: true, deleted });
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
  await loadSettings();
  await cleanup().catch(e => console.error('cleanup:', e.message));
  setInterval(() => cleanup().catch(e => console.error('cleanup:', e.message)), 10 * 60 * 1000);
  server.listen(PORT, () => console.log(`gigi chat running on port ${PORT}`));
}

main().catch(e => {
  console.error('startup failed:', e.message);
  process.exit(1);
});
