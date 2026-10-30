const path = require('path');
const http = require('http');
const crypto = require('crypto');
const express = require('express');
const { Server } = require('socket.io');
const Database = require('better-sqlite3');
const webpush = require('web-push');

const PORT = process.env.PORT || 3000;
const MAX_TEXT = 2000;
const NICK_RE = /^[\p{L}\p{N}_]{3,20}$/u;
const PASS_MIN = 4;
const PASS_MAX = 100;
const RING_MS = 40000;
const keyOf = s => s.toLowerCase();

const db = new Database(process.env.DB_FILE || path.join(__dirname, 'gigi.db'));
db.pragma('journal_mode = WAL');
db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    nickname TEXT PRIMARY KEY,
    key TEXT,
    pass_salt TEXT NOT NULL,
    pass_hash TEXT NOT NULL,
    created_at INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    nickname TEXT NOT NULL,
    created_at INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_nick TEXT NOT NULL,
    to_nick TEXT NOT NULL,
    text TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    read_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS settings (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS push_subs (
    endpoint TEXT PRIMARY KEY,
    nickname TEXT NOT NULL,
    sub TEXT NOT NULL
  );
`);

const columnsOf = table => db.prepare(`PRAGMA table_info(${table})`).all().map(c => c.name);
if (!columnsOf('users').includes('key')) db.exec('ALTER TABLE users ADD COLUMN key TEXT');
if (!columnsOf('messages').includes('read_at')) db.exec('ALTER TABLE messages ADD COLUMN read_at INTEGER');
db.exec(`
  CREATE UNIQUE INDEX IF NOT EXISTS idx_users_key ON users(key);
  CREATE INDEX IF NOT EXISTS idx_msg_from ON messages(from_nick, id);
  CREATE INDEX IF NOT EXISTS idx_msg_to ON messages(to_nick, id);
`);
for (const u of db.prepare('SELECT nickname FROM users WHERE key IS NULL').all()) {
  try {
    db.prepare('UPDATE users SET key = ? WHERE nickname = ?').run(keyOf(u.nickname), u.nickname);
  } catch {
    db.prepare('UPDATE users SET key = ? WHERE nickname = ?').run('dup:' + u.nickname, u.nickname);
  }
}

const q = {
  getUser: db.prepare('SELECT nickname, key, pass_salt, pass_hash FROM users WHERE key = ?'),
  insertUser: db.prepare('INSERT INTO users (nickname, key, pass_salt, pass_hash, created_at) VALUES (?, ?, ?, ?, ?)'),
  renameUser: db.prepare('UPDATE users SET nickname = ?, key = ? WHERE nickname = ?'),
  deleteUser: db.prepare('DELETE FROM users WHERE nickname = ?'),
  insertSession: db.prepare('INSERT INTO sessions (token, nickname, created_at) VALUES (?, ?, ?)'),
  getSession: db.prepare('SELECT nickname FROM sessions WHERE token = ?'),
  renameSessions: db.prepare('UPDATE sessions SET nickname = ? WHERE nickname = ?'),
  deleteSessions: db.prepare('DELETE FROM sessions WHERE nickname = ?'),
  searchUsers: db.prepare('SELECT nickname FROM users WHERE key LIKE ? ORDER BY nickname LIMIT 20'),
  insertMsg: db.prepare('INSERT INTO messages (from_nick, to_nick, text, created_at) VALUES (?, ?, ?, ?)'),
  lastMsg: db.prepare('SELECT * FROM messages WHERE id = ?'),
  renameFrom: db.prepare('UPDATE messages SET from_nick = ? WHERE from_nick = ?'),
  renameTo: db.prepare('UPDATE messages SET to_nick = ? WHERE to_nick = ?'),
  deleteMsgs: db.prepare('DELETE FROM messages WHERE from_nick = ? OR to_nick = ?'),
  markRead: db.prepare('UPDATE messages SET read_at = ? WHERE from_nick = ? AND to_nick = ? AND read_at IS NULL'),
  pushFor: db.prepare('SELECT endpoint, sub FROM push_subs WHERE nickname = ?'),
  upsertPush: db.prepare(`INSERT INTO push_subs (endpoint, nickname, sub) VALUES (?, ?, ?)
    ON CONFLICT(endpoint) DO UPDATE SET nickname = excluded.nickname, sub = excluded.sub`),
  pushDelete: db.prepare('DELETE FROM push_subs WHERE endpoint = ?'),
  renamePush: db.prepare('UPDATE push_subs SET nickname = ? WHERE nickname = ?'),
  deletePushUser: db.prepare('DELETE FROM push_subs WHERE nickname = ?'),
  chats: db.prepare(`
    SELECT partner, MAX(id) AS last_id FROM (
      SELECT to_nick AS partner, id FROM messages WHERE from_nick = @me
      UNION ALL
      SELECT from_nick AS partner, id FROM messages WHERE to_nick = @me
    ) GROUP BY partner ORDER BY last_id DESC LIMIT 100`),
  history: db.prepare(`
    SELECT * FROM messages
    WHERE (from_nick = @a AND to_nick = @b) OR (from_nick = @b AND to_nick = @a)
    ORDER BY id DESC LIMIT 200`),
};

const PUSH_TEXT = {
  en: { message: '{name}: new message', call: '{name} is calling you' },
  uk: { message: 'Нове повідомлення від {name}', call: '{name} дзвонить вам' },
  ru: { message: 'Новое сообщение от {name}', call: '{name} звонит вам' },
};

const VAPID = (() => {
  const read = k => db.prepare('SELECT v FROM settings WHERE k = ?').get(k)?.v;
  let pub = read('vapid_pub');
  let priv = read('vapid_priv');
  if (!pub || !priv) {
    const keys = webpush.generateVAPIDKeys();
    pub = keys.publicKey;
    priv = keys.privateKey;
    const put = db.prepare('INSERT OR REPLACE INTO settings (k, v) VALUES (?, ?)');
    put.run('vapid_pub', pub);
    put.run('vapid_priv', priv);
  }
  return { pub, priv };
})();
webpush.setVapidDetails('mailto:gigi-chat@example.com', VAPID.pub, VAPID.priv);

const findUser = nick => q.getUser.get(keyOf(nick));
const toMsg = r => ({ id: r.id, from: r.from_nick, to: r.to_nick, text: r.text, created_at: r.created_at, read_at: r.read_at ?? null });

const renameAll = db.transaction((oldNick, newNick) => {
  q.renameUser.run(newNick, keyOf(newNick), oldNick);
  q.renameFrom.run(newNick, oldNick);
  q.renameTo.run(newNick, oldNick);
  q.renameSessions.run(newNick, oldNick);
  q.renamePush.run(newNick, oldNick);
});

const deleteAll = db.transaction(nick => {
  q.deleteMsgs.run(nick, nick);
  q.deleteSessions.run(nick);
  q.deletePushUser.run(nick);
  q.deleteUser.run(nick);
});

function hashPassword(password, salt = crypto.randomBytes(16).toString('hex')) {
  return { salt, hash: crypto.scryptSync(password, salt, 64).toString('hex') };
}

function checkPassword(password, salt, hash) {
  const a = Buffer.from(crypto.scryptSync(password, salt, 64).toString('hex'), 'hex');
  const b = Buffer.from(hash, 'hex');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function createSession(nick) {
  const token = crypto.randomBytes(24).toString('hex');
  q.insertSession.run(token, nick, Date.now());
  return { ok: true, nick, token };
}

function authenticate(payload) {
  if (payload?.token) {
    const s = q.getSession.get(String(payload.token));
    return s ? { ok: true, nick: s.nickname } : { ok: false, error: 'bad_session' };
  }
  const nick = typeof payload?.nick === 'string' ? payload.nick.trim() : '';
  const password = typeof payload?.password === 'string' ? payload.password : '';
  if (!NICK_RE.test(nick)) return { ok: false, error: 'invalid_nick' };
  if (password.length < PASS_MIN || password.length > PASS_MAX) return { ok: false, error: 'invalid_password' };

  const user = findUser(nick);
  if (!user) {
    const { salt, hash } = hashPassword(password);
    q.insertUser.run(nick, keyOf(nick), salt, hash, Date.now());
    return createSession(nick);
  }
  if (!checkPassword(password, user.pass_salt, user.pass_hash)) return { ok: false, error: 'nick_taken' };
  return createSession(user.nickname);
}

async function pushTo(nick, type, from) {
  for (const row of q.pushFor.all(nick)) {
    let saved;
    try { saved = JSON.parse(row.sub); } catch { continue; }
    const texts = PUSH_TEXT[saved.lang] || PUSH_TEXT.en;
    const body = texts[type].replace('{name}', from);
    try {
      await webpush.sendNotification(saved.subscription, JSON.stringify({ type, title: 'gigi chat', body }), { TTL: 60 });
    } catch (err) {
      if (err.statusCode === 404 || err.statusCode === 410) q.pushDelete.run(row.endpoint);
    }
  }
}

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/users', (req, res) => {
  const term = String(req.query.q || '').replace(/[^\p{L}\p{N}_]/gu, '');
  if (!term) return res.json([]);
  res.json(q.searchUsers.all(`%${keyOf(term)}%`).map(r => r.nickname));
});

app.get('/api/chats', (req, res) => {
  const me = String(req.query.me || '');
  if (!NICK_RE.test(me)) return res.status(400).json({ error: 'bad_nick' });
  res.json(q.chats.all({ me }).map(r => ({
    nick: r.partner,
    last: toMsg(q.lastMsg.get(r.last_id)),
  })));
});

app.get('/api/history', (req, res) => {
  const a = String(req.query.me || '');
  const b = String(req.query.with || '');
  if (!NICK_RE.test(a) || !NICK_RE.test(b)) return res.status(400).json({ error: 'bad_nick' });
  res.json(q.history.all({ a, b }).reverse().map(toMsg));
});

app.get('/api/push/key', (req, res) => res.json({ key: VAPID.pub }));

app.post('/api/push/subscribe', (req, res) => {
  const s = q.getSession.get(String(req.body?.token || ''));
  const sub = req.body?.subscription;
  if (!s || !sub?.endpoint) return res.status(400).json({ ok: false });
  const lang = ['en', 'uk', 'ru'].includes(req.body?.lang) ? req.body.lang : 'en';
  q.upsertPush.run(sub.endpoint, s.nickname, JSON.stringify({ subscription: sub, lang }));
  res.json({ ok: true });
});

app.post('/api/push/unsubscribe', (req, res) => {
  q.pushDelete.run(String(req.body?.endpoint || ''));
  res.json({ ok: true });
});

const server = http.createServer(app);
const io = new Server(server);

const online = new Map();      // nickname -> Set of socket ids
const socketNick = new Map();  // socket id -> nickname
const calls = new Map();       // nickname -> { peer, role, kind, active, timer }

const onlineList = () => [...online.keys()];
const broadcastPresence = () => io.emit('presence', onlineList());

function endCall(nick, reason) {
  const c = calls.get(nick);
  if (!c) return;
  clearTimeout(c.timer);
  calls.delete(nick);
  const p = calls.get(c.peer);
  if (p && p.peer === nick) {
    clearTimeout(p.timer);
    calls.delete(c.peer);
  }
  io.to('u:' + c.peer).emit('call:ended', { reason });
  io.to('u:' + nick).emit('call:ended', { reason });
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
      if (calls.get(nick)?.active) endCall(nick, 'disconnected');
    }
  }
  broadcastPresence();
}

io.on('connection', socket => {
  socket.emit('presence', onlineList());

  socket.on('login', (payload, ack) => {
    const res = authenticate(payload);
    if (!res.ok) return ack?.(res);

    detach(socket);
    socketNick.set(socket.id, res.nick);
    if (!online.has(res.nick)) online.set(res.nick, new Set());
    online.get(res.nick).add(socket.id);
    socket.join('u:' + res.nick);

    broadcastPresence();
    ack?.(res);

    // Restore a call that was ringing while the app was closed.
    const c = calls.get(res.nick);
    if (c?.role === 'callee' && !c.active) socket.emit('call:incoming', { from: c.peer, kind: c.kind });
  });

  socket.on('message', (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });

    const text = typeof payload?.text === 'string' ? payload.text.trim() : '';
    if (!text || text.length > MAX_TEXT) return ack?.({ ok: false, error: 'invalid' });

    const to = findUser(String(payload?.to || ''))?.nickname;
    if (!to) return ack?.({ ok: false, error: 'no_user' });

    const now = Date.now();
    const info = q.insertMsg.run(me, to, text, now);
    const msg = { id: Number(info.lastInsertRowid), from: me, to, text, created_at: now, read_at: null };

    io.to('u:' + to).to('u:' + me).emit('message', msg);
    if (!online.has(to)) pushTo(to, 'message', me).catch(() => {});
    ack?.({ ok: true, msg });
  });

  socket.on('read', payload => {
    const me = socketNick.get(socket.id);
    if (!me) return;
    const partner = findUser(String(payload?.with || ''))?.nickname;
    if (!partner || partner === me) return;
    const now = Date.now();
    const info = q.markRead.run(now, partner, me);
    if (info.changes > 0) io.to('u:' + partner).emit('read', { by: me, at: now });
  });

  socket.on('rename', (payload, ack) => {
    const oldNick = socketNick.get(socket.id);
    if (!oldNick) return ack?.({ ok: false, error: 'not_logged_in' });
    if (calls.has(oldNick)) return ack?.({ ok: false, error: 'in_call' });

    const newNick = typeof payload?.nick === 'string' ? payload.nick.trim() : '';
    if (!NICK_RE.test(newNick)) return ack?.({ ok: false, error: 'invalid_nick' });
    if (newNick === oldNick) return ack?.({ ok: true, nick: oldNick });

    const taken = findUser(newNick);
    if (taken && taken.nickname !== oldNick) return ack?.({ ok: false, error: 'nick_taken' });

    renameAll(oldNick, newNick);

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

  socket.on('delete', (payload, ack) => {
    const nick = socketNick.get(socket.id);
    if (!nick) return ack?.({ ok: false, error: 'not_logged_in' });

    if (calls.has(nick)) endCall(nick, 'ended');
    deleteAll(nick);

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

  socket.on('call:invite', (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });
    const peer = findUser(String(payload?.to || ''))?.nickname;
    if (!peer || peer === me) return ack?.({ ok: false, error: 'no_user' });
    if (calls.has(me) || calls.has(peer)) return ack?.({ ok: false, error: 'busy' });

    const kind = payload?.kind === 'video' ? 'video' : 'audio';
    const timer = setTimeout(() => endCall(me, 'missed'), RING_MS);
    calls.set(me, { peer, role: 'caller', kind, active: false, timer });
    calls.set(peer, { peer: me, role: 'callee', kind, active: false, timer: null });

    if (online.has(peer)) io.to('u:' + peer).emit('call:incoming', { from: me, kind });
    else pushTo(peer, 'call', me).catch(() => {});
    ack?.({ ok: true });
  });

  socket.on('call:accept', () => {
    const me = socketNick.get(socket.id);
    const c = me && calls.get(me);
    if (!c || c.role !== 'callee' || c.active) return;
    const caller = calls.get(c.peer);
    if (!caller) return;
    clearTimeout(caller.timer);
    caller.timer = null;
    caller.active = true;
    c.active = true;
    io.to('u:' + c.peer).emit('call:accepted', { from: me });
  });

  socket.on('call:decline', () => {
    const me = socketNick.get(socket.id);
    if (me) endCall(me, 'declined');
  });

  socket.on('call:end', () => {
    const me = socketNick.get(socket.id);
    if (me) endCall(me, 'ended');
  });

  socket.on('call:signal', (payload) => {
    const me = socketNick.get(socket.id);
    const c = me && calls.get(me);
    if (!c || !c.active || !payload?.data) return;
    io.to('u:' + c.peer).emit('call:signal', { data: payload.data });
  });

  socket.on('disconnect', () => detach(socket));
});

server.listen(PORT, () => console.log(`gigi chat running on http://localhost:${PORT}`));
