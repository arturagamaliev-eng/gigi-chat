const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);
const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, 'gigi-data.json');

let state = { rooms: [{ id: 'general', name: 'Загальний чат', kind: 'group', theme: 'light' }], messages: {} };
const users = new Map();
try {
  if (fs.existsSync(DATA_FILE)) {
    const saved = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    if (saved && Array.isArray(saved.rooms) && saved.messages) state = saved;
  }
} catch (e) { console.error('Не вдалося прочитати базу даних:', e.message); }

function persist() {
  try { fs.writeFileSync(DATA_FILE, JSON.stringify(state, null, 2)); }
  catch (e) { console.error('Не вдалося зберегти дані:', e.message); }
}
function ensureRoom(id) {
  if (!state.rooms.some(r => r.id === id)) return false;
  if (!state.messages[id]) state.messages[id] = [];
  return true;
}
function cleanExpired(roomId) {
  const now = Date.now();
  for (const id of Object.keys(state.messages)) {
    const before = state.messages[id].length;
    state.messages[id] = state.messages[id].filter(m => !m.expiresAt || m.expiresAt > now);
    if (state.messages[id].length !== before) io.to(id).emit('room-history', { room: id, messages: state.messages[id] });
  }
}
app.use(express.static(__dirname));
app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/health', (_req, res) => res.json({ ok: true, app: 'Gigi Chat' }));

io.on('connection', socket => {
  users.set(socket.id, { name: 'Гість' });
  socket.emit('rooms', state.rooms);
  socket.on('set-name', name => {
    users.get(socket.id).name = String(name || 'Гість').trim().slice(0, 24) || 'Гість';
  });
  socket.on('get-rooms', () => socket.emit('rooms', state.rooms));
  socket.on('join-room', roomId => {
    if (!ensureRoom(roomId)) return;
    for (const room of state.rooms) socket.leave(room.id);
    socket.join(roomId);
    cleanExpired(roomId);
    socket.emit('room-history', { room: roomId, messages: state.messages[roomId] || [] });
  });
  socket.on('create-room', room => {
    if (!room || typeof room.name !== 'string') return;
    const name = room.name.trim().slice(0, 40);
    if (!name) return;
    const id = 'room-' + Date.now() + '-' + Math.random().toString(36).slice(2, 7);
    const safeRoom = { id, name, kind: room.kind === 'direct' ? 'direct' : 'group', theme: 'light' };
    state.rooms.push(safeRoom);
    state.messages[id] = [];
    persist();
    io.emit('room-created', safeRoom);
    socket.emit('rooms', state.rooms);
  });
  socket.on('update-room', room => {
    const existing = state.rooms.find(r => r.id === room?.id);
    if (existing) {
      existing.theme = ['light', 'dark', 'ocean', 'pink'].includes(room.theme) ? room.theme : 'light';
      existing.expire = !!room.expire;
      persist();
    }
  });
  socket.on('send-message', payload => {
    if (!payload || !ensureRoom(payload.room)) return;
    const user = users.get(socket.id)?.name || 'Гість';
    const text = String(payload.text || '').slice(0, 5000);
    const url = typeof payload.url === 'string' && /^https?:\/\//i.test(payload.url) ? payload.url.slice(0, 2000) : '';
    if (!text.trim() && !url) return;
    const room = state.rooms.find(r => r.id === payload.room);
    const message = {
      id: 'm-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8),
      room: payload.room, user, text, type: ['gif', 'poll'].includes(payload.type) ? payload.type : 'text',
      url: url || undefined, time: Date.now(),
      replyText: String(payload.replyText || '').slice(0, 1000),
      replyUser: String(payload.replyUser || '').slice(0, 24),
      expiresAt: room?.expire ? Date.now() + 60 * 60 * 1000 : undefined
    };
    state.messages[payload.room].push(message);
    if (state.messages[payload.room].length > 2000) state.messages[payload.room] = state.messages[payload.room].slice(-2000);
    persist();
    io.to(payload.room).emit('new-message', message);
  });
  socket.on('edit-message', data => {
    if (!data || !ensureRoom(data.room)) return;
    const list = state.messages[data.room] || [];
    const m = list.find(x => x.id === data.id);
    const user = users.get(socket.id)?.name || 'Гість';
    if (!m || m.user !== user) { socket.emit('message-error', 'Можна змінювати лише власні повідомлення.'); return; }
    const text = String(data.text || '').trim().slice(0, 5000);
    if (!text) return;
    m.text = text; m.edited = true; m.editedAt = Date.now();
    persist();
    io.to(data.room).emit('message-edited', m);
  });
  socket.on('disconnect', () => users.delete(socket.id));
});
setInterval(() => {
  let changed = false; const now = Date.now();
  for (const id of Object.keys(state.messages)) {
    const old = state.messages[id].length;
    state.messages[id] = state.messages[id].filter(m => !m.expiresAt || m.expiresAt > now);
    if (old !== state.messages[id].length) { changed = true; io.to(id).emit('room-history', { room: id, messages: state.messages[id] }); }
  }
  if (changed) persist();
}, 30_000);

server.listen(PORT, '0.0.0.0', () => console.log(`Gigi Chat запущено: http://localhost:${PORT}`));
