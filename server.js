// Poker lobby relay — پیام‌ها را بین میزبان و بازیکنان از طریق WebSocket جابه‌جا می‌کند
// (بدون WebRTC / STUN / TURN)
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 10000;
const rooms = new Map(); // code -> { host: ws|null, clients: Map(id -> ws), timer }

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain', 'access-control-allow-origin': '*' });
  res.end('ok');
});
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 1 << 20 });

const send = (ws, o) => { try { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); } catch (e) {} };

wss.on('connection', (ws, req) => {
  const q = new URL(req.url, 'http://x').searchParams;
  const code = String(q.get('room') || '').toUpperCase().slice(0, 12);
  const role = q.get('role');
  const id = String(q.get('id') || '').slice(0, 40);
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  if (!code || (role !== 'host' && role !== 'client')) { send(ws, { r: 'err', e: 'bad' }); return ws.close(); }

  if (role === 'host') {
    let room = rooms.get(code);
    if (room && room.host && room.host.readyState === 1) { send(ws, { r: 'err', e: 'taken' }); return ws.close(); }
    if (!room) { room = { host: null, clients: new Map(), timer: null }; rooms.set(code, room); }
    clearTimeout(room.timer);
    room.host = ws;
    send(ws, { r: 'ok' });
    for (const cid of room.clients.keys()) send(ws, { r: 'join', id: cid }); // اتصال مجدد میزبان
    ws.on('message', raw => {
      let m; try { m = JSON.parse(raw); } catch (e) { return; }
      if (m.r === 'm') send(room.clients.get(m.id), { r: 'm', d: m.d });
    });
    ws.on('close', () => {
      if (room.host !== ws) return;
      room.host = null;
      room.timer = setTimeout(() => { // ۶۰ ثانیه مهلت برای برگشتن میزبان
        for (const c of room.clients.values()) { send(c, { r: 'err', e: 'hostgone' }); try { c.close(); } catch (e) {} }
        rooms.delete(code);
      }, 60000);
    });
  } else {
    const room = rooms.get(code);
    if (!room || !id) { send(ws, { r: 'err', e: 'nolobby' }); return ws.close(); }
    const old = room.clients.get(id); if (old && old !== ws) { try { old.close(); } catch (e) {} }
    room.clients.set(id, ws);
    send(ws, { r: 'ok' });
    send(room.host, { r: 'join', id });
    ws.on('message', raw => {
      let m; try { m = JSON.parse(raw); } catch (e) { return; }
      if (m.r === 'm') send(room.host, { r: 'm', id, d: m.d });
    });
    ws.on('close', () => {
      if (room.clients.get(id) === ws) { room.clients.delete(id); send(room.host, { r: 'leave', id }); }
    });
  }
});

// جلوگیری از قطع شدن اتصال‌های بیکار توسط پروکسی Render
setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; try { ws.ping(); } catch (e) {} });
}, 25000);

server.listen(PORT, () => console.log('relay on', PORT));
