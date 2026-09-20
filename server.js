const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    const file = path.join(__dirname, 'index.html');
    if (!fs.existsSync(file)) {
      res.writeHead(404);
      return res.end('index.html not found');
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return fs.createReadStream(file).pipe(res);
  }
  res.writeHead(404);
  res.end('Not found');
});

const wss = new WebSocket.Server({ server });
const rooms = {};

function getRoomId(ws) {
  for (const [id, room] of Object.entries(rooms)) {
    if (room.clients.has(ws)) return id;
  }
  return null;
}

function broadcast(roomId, data, excludeWs = null) {
  const room = rooms[roomId];
  if (!room) return;
  const msg = JSON.stringify(data);
  room.clients.forEach(client => {
    if (client !== excludeWs && client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  });
}

function broadcastAll(roomId, data) {
  const room = rooms[roomId];
  if (!room) return;
  const msg = JSON.stringify(data);
  room.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  });
}

wss.on('connection', (ws) => {
  ws.info = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case 'join': {
        const roomId = (msg.room || 'default').slice(0, 30);
        const name = (msg.name || 'بیننده').slice(0, 30);
        if (!rooms[roomId]) rooms[roomId] = { clients: new Set(), host: null };
        const room = rooms[roomId];
        room.clients.add(ws);
        ws.info = { name, roomId };
        const isHost = !room.host;
        if (isHost) room.host = ws;
        ws.send(JSON.stringify({ type: 'joined', isHost, roomId, name }));
        broadcast(roomId, { type: 'chat', system: true, text: `${name} وارد اتاق شد 👋`, time: Date.now() }, ws);
        const members = [];
        room.clients.forEach(c => { if (c.info) members.push({ name: c.info.name, isHost: c === room.host }); });
        broadcastAll(roomId, { type: 'room_info', count: members.length, members });
        break;
      }
      case 'chat': {
        const roomId = getRoomId(ws);
        if (!roomId || !ws.info) return;
        broadcast(roomId, { type: 'chat', name: ws.info.name, text: (msg.text || '').slice(0, 500), time: Date.now() }, ws);
        break;
      }
      case 'play':
      case 'pause':
      case 'seek': {
        const roomId = getRoomId(ws);
        if (!roomId) return;
        const room = rooms[roomId];
        if (room.host && room.host !== ws) return;
        broadcast(roomId, { type: msg.type, time: msg.time || 0, by: ws.info?.name }, ws);
        break;
      }
      case 'ping':
        ws.send(JSON.stringify({ type: 'pong' }));
        break;
    }
  });

  ws.on('close', () => {
    const roomId = getRoomId(ws);
    if (!roomId) return;
    const room = rooms[roomId];
    room.clients.delete(ws);
    if (room.host === ws) {
      const next = [...room.clients][0];
      room.host = next || null;
      if (next) next.send(JSON.stringify({ type: 'promoted', message: 'شما میزبان شدید ⭐' }));
    }
    if (ws.info) broadcast(roomId, { type: 'chat', system: true, text: `${ws.info.name} اتاق رو ترک کرد`, time: Date.now() });
    if (room.clients.size === 0) delete rooms[roomId];
    else {
      const members = [];
      room.clients.forEach(c => { if (c.info) members.push({ name: c.info.name, isHost: c === room.host }); });
      broadcastAll(roomId, { type: 'room_info', count: members.length, members });
    }
  });

  ws.on('error', () => {});
});

server.listen(PORT, () => console.log(`✅ SyncWatch on port ${PORT}`));
