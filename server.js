const path = require('path');
const http = require('http');
const crypto = require('crypto');
const express = require('express');
const { Server } = require('socket.io');
const { Pool } = require('pg');

const PORT = process.env.PORT || 3000;
const NICK_RE = /^[a-zA-Z0-9_]{3,20}$/;
const keyOf = s => s.toLowerCase();

if (!process.env.DATABASE_URL) {
  console.error('DATABASE_URL не знайдено!');
  process.exit(1);
}

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

const db = {
  one: async (sql, p = []) => (await pool.query(sql, p)).rows[0],
  all: async (sql, p = []) => (await pool.query(sql, p)).rows,
  run: (sql, p = []) => pool.query(sql, p),
};

const SCHEMA = `
CREATE TABLE IF NOT EXISTS users (
  nickname TEXT PRIMARY KEY,
  key TEXT UNIQUE NOT NULL,
  pass_salt TEXT NOT NULL,
  pass_hash TEXT NOT NULL,
  avatar TEXT,
  bio TEXT,
  stars INTEGER DEFAULT 0,
  premium_until DOUBLE PRECISION DEFAULT 0,
  created_at DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS blocks (
  blocker TEXT NOT NULL,
  blocked TEXT NOT NULL,
  PRIMARY KEY (blocker, blocked)
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

CREATE TABLE IF NOT EXISTS stories (
  id SERIAL PRIMARY KEY,
  author TEXT NOT NULL,
  text TEXT NOT NULL,
  media TEXT,
  created_at DOUBLE PRECISION NOT NULL
);
`;

function hashPassword(password, salt = crypto.randomBytes(16).toString('hex')) {
  return { salt, hash: crypto.scryptSync(password, salt, 64).toString('hex') };
}

function checkPassword(password, salt, hash) {
  const a = Buffer.from(crypto.scryptSync(password, salt, 64).toString('hex'), 'hex');
  const b = Buffer.from(hash, 'hex');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/users', async (req, res) => {
  const q = String(req.query.q || '').toLowerCase();
  if (!q) return res.json([]);
  const rows = await db.all('SELECT nickname FROM users WHERE key LIKE $1 LIMIT 20', [`%${q}%`]);
  res.json(rows.map(r => r.nickname));
});

app.get('/api/chats', async (req, res) => {
  const me = String(req.query.me || '');
  const rows = await db.all(`
    SELECT partner, MAX(id) AS last_id FROM (
      SELECT to_nick AS partner, id FROM messages WHERE from_nick = $1
      UNION ALL
      SELECT from_nick AS partner, id FROM messages WHERE to_nick = $1
    ) t GROUP BY partner ORDER BY last_id DESC`, [me]);
  const out = [];
  for (const r of rows) {
    const last = await db.one('SELECT * FROM messages WHERE id = $1', [r.last_id]);
    out.push({ nick: r.partner, last });
  }
  res.json(out);
});

app.get('/api/history', async (req, res) => {
  const { me, with: partner } = req.query;
  const rows = await db.all(`
    SELECT * FROM messages 
    WHERE (from_nick = $1 AND to_nick = $2) OR (from_nick = $2 AND to_nick = $1) 
    ORDER BY id ASC LIMIT 200`, [me, partner]);
  res.json(rows);
});

const server = http.createServer(app);
const io = new Server(server);

const online = new Map();
const socketNick = new Map();

io.on('connection', socket => {

  socket.on('login', async (payload, ack) => {
    let nick = payload?.nick?.trim();
    const pass = payload?.password;
    if (payload?.token) {
      const s = await db.one('SELECT nickname FROM sessions WHERE token = $1', [payload.token]);
      if (s) nick = s.nickname;
    }
    if (!nick || !NICK_RE.test(nick)) return ack?.({ ok: false, error: 'invalid' });

    let user = await db.one('SELECT * FROM users WHERE key = $1', [keyOf(nick)]);
    if (!user && pass) {
      const { salt, hash } = hashPassword(pass);
      await db.run('INSERT INTO users (nickname, key, pass_salt, pass_hash, created_at) VALUES ($1, $2, $3, $4, $5)',
        [nick, keyOf(nick), salt, hash, Date.now()]);
      user = { nickname: nick };
    } else if (user && pass && !checkPassword(pass, user.pass_salt, user.pass_hash)) {
      return ack?.({ ok: false, error: 'wrong_pass' });
    }

    const token = crypto.randomBytes(24).toString('hex');
    await db.run('INSERT INTO sessions (token, nickname, created_at) VALUES ($1, $2, $3)', [token, user.nickname, Date.now()]);
    
    socketNick.set(socket.id, user.nickname);
    online.set(user.nickname, socket.id);
    socket.join('u:' + user.nickname);

    const blocks = await db.all('SELECT blocked FROM blocks WHERE blocker = $1', [user.nickname]);
    socket.emit('blocks_list', blocks.map(b => b.blocked));
    
    const isPrem = user.premium_until > Date.now();
    socket.emit('premium_status', { active: isPrem });

    io.emit('presence', [...online.keys()]);
    ack?.({ ok: true, nick: user.nickname, token });
  });

  socket.on('message', async ({ to, text, kind = 'text' }) => {
    const me = socketNick.get(socket.id);
    if (!me) return;

    // Перевірка на блокування
    const blocked = await db.one('SELECT 1 FROM blocks WHERE (blocker = $1 AND blocked = $2) OR (blocker = $2 AND blocked = $1)', [to, me]);
    if (blocked) return; // Повідомлення тихо відкидається

    const now = Date.now();
    const row = await db.one('INSERT INTO messages (from_nick, to_nick, text, kind, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING id',
      [me, to, text, kind, now]);
    
    const msg = { id: row.id, from: me, to, text, kind, created_at: now };
    io.to('u:' + to).to('u:' + me).emit('message', msg);
  });

  socket.on('msg_delete', async ({ id }) => {
    const me = socketNick.get(socket.id);
    const m = await db.one('SELECT * FROM messages WHERE id = $1', [id]);
    if (!m) return;
    
    const u = await db.one('SELECT premium_until FROM users WHERE nickname = $1', [me]);
    const isPrem = u?.premium_until > Date.now();

    // Видаляти може власник АБО юзер з Преміумом
    if (m.from_nick === me || isPrem) {
      await db.run('DELETE FROM messages WHERE id = $1', [id]);
      io.to('u:' + m.from_nick).to('u:' + m.to_nick).emit('msg_deleted', { id, from: m.from_nick, to: m.to_nick });
    }
  });

  socket.on('block_user', async ({ nick }, ack) => {
    const me = socketNick.get(socket.id);
    await db.run('INSERT INTO blocks (blocker, blocked) VALUES ($1, $2) ON CONFLICT DO NOTHING', [me, nick]);
    const blocks = await db.all('SELECT blocked FROM blocks WHERE blocker = $1', [me]);
    socket.emit('blocks_list', blocks.map(b => b.blocked));
    ack?.({ ok: true });
  });

  socket.on('unblock_user', async ({ nick }, ack) => {
    const me = socketNick.get(socket.id);
    await db.run('DELETE FROM blocks WHERE blocker = $1 AND blocked = $2', [me, nick]);
    const blocks = await db.all('SELECT blocked FROM blocks WHERE blocker = $1', [me]);
    socket.emit('blocks_list', blocks.map(b => b.blocked));
    ack?.({ ok: true });
  });

  socket.on('buy_premium', async ({ cost, days }, ack) => {
    const me = socketNick.get(socket.id);
    const u = await db.one('SELECT stars, premium_until FROM users WHERE nickname = $1', [me]);
    if (u.stars < cost) return ack?.({ ok: false, error: 'no_stars' });

    const base = Math.max(Date.now(), u.premium_until || 0);
    const newUntil = base + (days * 24 * 3600 * 1000);

    await db.run('UPDATE users SET stars = stars - $1, premium_until = $2 WHERE nickname = $3', [cost, newUntil, me]);
    socket.emit('premium_status', { active: true });
    ack?.({ ok: true });
  });

  socket.on('story_post', async ({ text, media }) => {
    const me = socketNick.get(socket.id);
    const u = await db.one('SELECT premium_until FROM users WHERE nickname = $1', [me]);
    if (u?.premium_until <= Date.now()) return; // Тільки для Преміум

    const row = await db.one('INSERT INTO stories (author, text, media, created_at) VALUES ($1, $2, $3, $4) RETURNING id',
      [me, text, media, Date.now()]);
    
    // Відправляємо історію тільки тим, у кого є чат з автором
    const contacts = await db.all(`
      SELECT DISTINCT partner FROM (
        SELECT to_nick AS partner FROM messages WHERE from_nick = $1
        UNION
        SELECT from_nick AS partner FROM messages WHERE to_nick = $1
      ) t`, [me]);

    for (const c of contacts) {
      io.to('u:' + c.partner).emit('story', { id: row.id, author: me, text, media });
    }
  });

  socket.on('user_info', async ({ nick }, ack) => {
    const u = await db.one('SELECT nickname, stars, premium_until FROM users WHERE nickname = $1', [nick]);
    ack?.({ ok: true, info: u });
  });

  socket.on('disconnect', () => {
    const me = socketNick.get(socket.id);
    if (me) {
      online.delete(me);
      socketNick.delete(socket.id);
      io.emit('presence', [...online.keys()]);
    }
  });
});

db.run(SCHEMA).then(() => {
  server.listen(PORT, () => console.log('gigi chat працює на порту ' + PORT));
});
