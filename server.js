const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 1e6 });
const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, 'messages.json');
let messages = [];
let nextId = 1;
const scheduled = new Map();

try {
  if (fs.existsSync(DATA_FILE)) {
    messages = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    nextId = messages.reduce((n, m) => Math.max(n, Number(m.id) + 1), 1);
  }
} catch (e) { console.error('Не вдалося прочитати messages.json:', e.message); }

function save() {
  const tmp = DATA_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(messages, null, 2));
  fs.renameSync(tmp, DATA_FILE);
}
function cleanupExpired() {
  const now = Date.now();
  const before = messages.length;
  messages = messages.filter(m => !m.expiresAt || m.expiresAt > now);
  if (messages.length !== before) { save(); io.emit('history', messages); }
}
setInterval(cleanupExpired, 15000).unref();

app.use(express.static(__dirname));
app.get('/health', (_req, res) => res.json({ ok: true, messages: messages.length }));

io.on('connection', socket => {
  socket.data.username = 'Гість';
  socket.emit('history', messages.filter(m => !m.expiresAt || m.expiresAt > Date.now()));

  socket.on('join', data => {
    const name = String(data?.username || '').trim().slice(0, 32);
    if (!name) return socket.emit('error_msg', 'Вкажи нікнейм.');
    socket.data.username = name;
    socket.emit('history', messages.filter(m => !m.expiresAt || m.expiresAt > Date.now()));
  });

  socket.on('get_history', () => {
    cleanupExpired();
    socket.emit('history', messages.filter(m => !m.expiresAt || m.expiresAt > Date.now()));
  });

  socket.on('send', data => {
    const text = String(data?.text || '').trim().slice(0, 4000);
    if (!text) return;
    const kind = ['text', 'sticker', 'gif'].includes(data?.kind) ? data.kind : 'text';
    const expiresIn = Math.max(0, Math.min(Number(data?.expiresIn) || 0, 7 * 24 * 60 * 60 * 1000));
    const scheduledAt = Number(data?.scheduledAt) || 0;
    const publish = () => {
      const now = Date.now();
      const msg = { id: nextId++, username: socket.data.username, text, kind,
        createdAt: now, expiresAt: expiresIn ? now + expiresIn : null, edited: false };
      messages.push(msg);
      if (messages.length > 5000) messages = messages.slice(-5000);
      save();
      io.emit('message', msg);
    };
    if (scheduledAt > Date.now()) {
      const delay = Math.min(scheduledAt - Date.now(), 7 * 24 * 60 * 60 * 1000);
      const key = `${socket.id}:${Date.now()}:${Math.random()}`;
      const timer = setTimeout(() => { scheduled.delete(key); publish(); }, delay);
      scheduled.set(key, timer);
      socket.emit('error_msg', 'Повідомлення заплановано. Сервер має залишатися запущеним до часу надсилання.');
    } else publish();
  });

  socket.on('edit', data => {
    const id = Number(data?.id);
    const text = String(data?.text || '').trim().slice(0, 4000);
    const msg = messages.find(m => m.id === id);
    if (!msg || msg.username !== socket.data.username || msg.kind === 'gif' || !text) return;
    msg.text = text; msg.edited = true; save(); io.emit('edited', msg);
  });

  socket.on('disconnect', () => {
    for (const [key, timer] of scheduled) {
      if (key.startsWith(socket.id + ':')) { clearTimeout(timer); scheduled.delete(key); }
    }
  });
});

server.listen(PORT, () => console.log(`Gigi Chat запущено на порті ${PORT}`));
