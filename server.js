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
const MAX_UPLOAD = 15 * 1024 * 1024;
const keyOf = s => s.toLowerCase();

const CLOUD = process.env.CLOUDINARY_CLOUD_NAME || '';
const CKEY = process.env.CLOUDINARY_API_KEY || '';
const CSECRET = process.env.CLOUDINARY_API_SECRET || '';
const MEDIA_ON = false; // photos and videos are not stored
const STORAGE_OK = MEDIA_ON && Boolean(CLOUD && CKEY && CSECRET);
const MEDIA = {
  'image/jpeg': ['image', 'jpg'], 'image/png': ['image', 'png'], 'image/webp': ['image', 'webp'], 'image/gif': ['image', 'gif'],
  'video/mp4': ['video', 'mp4'], 'video/webm': ['video', 'webm'],
};

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

// ---- Cloudinary: photos and videos ----

const sign = params => {
  const str = Object.keys(params).sort().map(k => `${k}=${params[k]}`).join('&') + CSECRET;
  return crypto.createHash('sha1').update(str).digest('hex');
};

async function cloudUpload(dataUrl, resourceType, publicId) {
  const timestamp = Math.floor(Date.now() / 1000);
  const form = new FormData();
  form.append('file', dataUrl);
  form.append('api_key', CKEY);
  form.append('timestamp', String(timestamp));
  form.append('public_id', publicId);
  form.append('signature', sign({ public_id: publicId, timestamp }));
  const r = await fetch(`https://api.cloudinary.com/v1_1/${CLOUD}/${resourceType}/upload`, { method: 'POST', body: form });
  const j = await r.json();
  if (!r.ok || !j.secure_url) throw new Error('cloudinary upload failed: ' + (j.error?.message || r.status));
  return j.secure_url;
}

async function cloudDestroy(url) {
  const m = /^https:\/\/res\.cloudinary\.com\/([^/]+)\/(image|video)\/upload\/(?:v\d+\/)?(.+)\.[a-z0-9]+$/i.exec(String(url));
  if (!m || m[1] !== CLOUD) return;
  const timestamp = Math.floor(Date.now() / 1000);
  const form = new FormData();
  form.append('public_id', m[3]);
  form.append('api_key', CKEY);
  form.append('timestamp', String(timestamp));
  form.append('signature', sign({ public_id: m[3], timestamp }));
  await fetch(`https://api.cloudinary.com/v1_1/${CLOUD}/${m[2]}/destroy`, { method: 'POST', body: form }).catch(() => {});
}

const isOurMedia = (url, kind) =>
  STORAGE_OK && typeof url === 'string' && url.length < 500 &&
  url.startsWith(`https://res.cloudinary.com/${CLOUD}/${kind}/upload/`);

async function removeUser(nick) {
  const files = (await db.all("SELECT text FROM messages WHERE (from_nick = $1 OR to_nick = $1) AND kind <> 'text'", [nick]))
    .map(r => r.text);
  const u = await db.one('SELECT avatar FROM users WHERE nickname = $1', [nick]);
  if (u?.avatar) files.push(u.avatar);
  await tx(async c => {
    await c.query('DELETE FROM messages WHERE from_nick = $1 OR to_nick = $1', [nick]);
    await c.query('DELETE FROM sessions WHERE nickname = $1', [nick]);
    await c.query('DELETE FROM users WHERE nickname = $1', [nick]);
  });
  for (const f of files) await cloudDestroy(f);
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
  if (!STORAGE_OK) return res.status(503).json({ error: 'storage_off' });
  const m = /^data:([\w/+.-]+);base64,(.+)$/.exec(String(req.body?.data || ''));
  const spec = m && MEDIA[m[1]];
  if (!spec) return res.status(400).json({ error: 'bad_type' });
  if (Math.floor(m[2].length * 3 / 4) > MAX_UPLOAD) return res.status(413).json({ error: 'too_big' });
  const url = await cloudUpload(req.body.data, spec[0], 'gigi/' + crypto.randomBytes(16).toString('hex'));
  res.json({ ok: true, url, kind: spec[0] });
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
    if (set.size === 0) online.delete(nick);
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
    const kind = ['image', 'video'].includes(payload?.kind) ? payload.kind : 'text';
    const text = typeof payload?.text === 'string' ? payload.text.trim() : '';
    if (kind !== 'text' && !isOurMedia(text, kind)) return ack?.({ ok: false, error: 'invalid' });
    if (!text || (kind === 'text' && text.length > MAX_TEXT)) return ack?.({ ok: false, error: 'invalid' });

    const to = (await findUser(String(payload?.to || '')))?.nickname;
    if (!to) return ack?.({ ok: false, error: 'no_user' });

    const now = Date.now();
    const row = await db.one(
      'INSERT INTO messages (from_nick, to_nick, text, kind, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING id',
      [me, to, text, kind, now]);
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
    if (row.kind !== 'text') await cloudDestroy(row.text);
    io.to('u:' + row.to_nick).to('u:' + me).emit('msg_deleted', { id, from: me, to: row.to_nick });
    ack?.({ ok: true });
  });

  on('set_avatar', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const url = String(payload?.url || '');
    if (!isOurMedia(url, 'image')) return ack?.({ ok: false, error: 'invalid' });
    const old = (await db.one('SELECT avatar FROM users WHERE nickname = $1', [me]))?.avatar;
    await db.run('UPDATE users SET avatar = $1 WHERE nickname = $2', [url, me]);
    if (old && old !== url) await cloudDestroy(old);
    io.emit('avatar', { nick: me, url });
    ack?.({ ok: true });
  });

  // Only the owner of the server knows ADMIN_CODE (set in Render's Environment settings).
  on('admin_delete', async (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const code = process.env.ADMIN_CODE;
    if (!code || String(payload?.code || '') !== code) return ack?.({ ok: false, error: 'bad_code' });
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

  socket.on('disconnect', () => detach(socket));
});

async function main() {
  await pool.query(SCHEMA);
  server.listen(PORT, () => console.log(`gigi chat running on port ${PORT}`));
}

main().catch(e => {
  console.error('startup failed:', e.message);
  process.exit(1);
});
