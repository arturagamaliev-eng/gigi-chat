const path = require('path');
const http = require('http');
const crypto = require('crypto');
const express = require('express');
const { Server } = require('socket.io');
const Database = require('better-sqlite3');

const PORT = process.env.PORT || 3000;
const MAX_TEXT = 2000;
const NICK_RE = /^[\p{L}\p{N}_]{3,20}$/u;
const PASS_MIN = 4;
const PASS_MAX = 100;

const db = new Database(process.env.DB_FILE || path.join(__dirname, 'gigi.db'));
db.pragma('journal_mode = WAL');
db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    nickname TEXT PRIMARY KEY,
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
    created_at INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_msg_from ON messages(from_nick, id);
  CREATE INDEX IF NOT EXISTS idx_msg_to ON messages(to_nick, id);
`);

const q = {
  getUser: db.prepare('SELECT nickname, pass_salt, pass_hash FROM users WHERE nickname = ? COLLATE NOCASE'),
  insertUser: db.prepare('INSERT INTO users (nickname, pass_salt, pass_hash, created_at) VALUES (?, ?, ?, ?)'),
  insertSession: db.prepare('INSERT INTO sessions (token, nickname, created_at) VALUES (?, ?, ?)'),
  getSession: db.prepare('SELECT nickname FROM sessions WHERE token = ?'),
  searchUsers: db.prepare('SELECT nickname FROM users WHERE nickname LIKE ? ORDER BY nickname LIMIT 20'),
  insertMsg: db.prepare('INSERT INTO messages (from_nick, to_nick, text, created_at) VALUES (?, ?, ?, ?)'),
  lastMsg: db.prepare('SELECT * FROM messages WHERE id = ?'),
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

const toMsg = r => ({ id: r.id, from: r.from_nick, to: r.to_nick, text: r.text, created_at: r.created_at });

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

  const user = q.getUser.get(nick);
  if (!user) {
    const { salt, hash } = hashPassword(password);
    q.insertUser.run(nick, salt, hash, Date.now());
    return createSession(nick);
  }
  if (!checkPassword(password, user.pass_salt, user.pass_hash)) return { ok: false, error: 'nick_taken' };
  return createSession(user.nickname);
}

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/users', (req, res) => {
  const term = String(req.query.q || '').replace(/[^\p{L}\p{N}_]/gu, '');
  if (!term) return res.json([]);
  res.json(q.searchUsers.all(`%${term}%`).map(r => r.nickname));
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

const server = http.createServer(app);
const io = new Server(server);

const online = new Map();
const socketNick = new Map();

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
  });

  socket.on('message', (payload, ack) => {
    const me = socketNick.get(socket.id);
    if (!me) return ack?.({ ok: false, error: 'not_logged_in' });

    const text = typeof payload?.text === 'string' ? payload.text.trim() : '';
    if (!text || text.length > MAX_TEXT) return ack?.({ ok: false, error: 'invalid' });

    const to = q.getUser.get(String(payload?.to || ''))?.nickname;
    if (!to) return ack?.({ ok: false, error: 'no_user' });

    const now = Date.now();
    const info = q.insertMsg.run(me, to, text, now);
    const msg = { id: Number(info.lastInsertRowid), from: me, to, text, created_at: now };

    io.to('u:' + to).to('u:' + me).emit('message', msg);
    ack?.({ ok: true, msg });
  });

  socket.on('disconnect', () => detach(socket));
});

server.listen(PORT, () => console.log(`gigi chat running on http://localhost:${PORT}`));
