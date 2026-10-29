const path = require('path');
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');
const Database = require('better-sqlite3');

const PORT = process.env.PORT || 3000;
const MAX_TEXT = 2000;
const NICK_RE = /^[\p{L}\p{N}_]{3,20}$/u;

const db = new Database(process.env.DB_FILE || path.join(__dirname, 'gigi.db'));
db.pragma('journal_mode = WAL');
db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    nickname TEXT PRIMARY KEY,
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
  insertUser: db.prepare('INSERT OR IGNORE INTO users (nickname, created_at) VALUES (?, ?)'),
  getUser: db.prepare('SELECT nickname FROM users WHERE nickname = ? COLLATE NOCASE'),
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

io.on('connection', socket => {
  socket.emit('presence', onlineList());

  socket.on('login', (nick, ack) => {
    const wanted = typeof nick === 'string' ? nick.trim() : '';
    if (!NICK_RE.test(wanted)) return ack?.({ ok: false, error: 'invalid_nick' });

    q.insertUser.run(wanted, Date.now());
    const canonical = q.getUser.get(wanted).nickname;

    detach(socket);
    socketNick.set(socket.id, canonical);
    if (!online.has(canonical)) online.set(canonical, new Set());
    online.get(canonical).add(socket.id);
    socket.join('u:' + canonical);

    broadcastPresence();
    ack?.({ ok: true, nick: canonical });
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
