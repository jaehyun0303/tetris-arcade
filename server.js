'use strict';
// 공학아카데미 테트리스 아케이드 서버 (외부 라이브러리 없이 Node.js만으로 동작)
// 실행: node server.js  →  http://<이 컴퓨터 IP>:3000
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), os = require('os');
const PORT = +process.env.PORT || 3000;
const PUB = path.join(__dirname, 'public');
const MIME = {'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css','.mp3':'audio/mpeg','.ogg':'audio/ogg','.wav':'audio/wav','.png':'image/png','.jpg':'image/jpeg','.svg':'image/svg+xml','.json':'application/json'};

const server = http.createServer((req, res) => {
  let p;
  try { p = decodeURIComponent(req.url.split('?')[0]); } catch (e) { res.writeHead(400); return res.end(); }
  if (p === '/') p = '/index.html';
  const f = path.normalize(path.join(PUB, p));
  if (!f.startsWith(PUB)) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (e, d) => {
    if (e) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, {'Content-Type': MIME[path.extname(f)] || 'application/octet-stream'});
    res.end(d);
  });
});

/* ---------- 최소 WebSocket 구현 ---------- */
const clients = new Set();
class Client {
  constructor(socket) {
    this.socket = socket; this.buf = Buffer.alloc(0); this.frag = [];
    this.name = '손님'; this.room = null; this.ready = false; this.alive = true;
    socket.on('data', d => { this.buf = Buffer.concat([this.buf, d]); this.parse(); });
    socket.on('close', () => this.close());
    socket.on('error', () => this.close());
  }
  parse() {
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = !!(b[0] & 0x80), op = b[0] & 0x0f, masked = !!(b[1] & 0x80);
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > 65536) { this.socket.destroy(); return; }
      const need = off + (masked ? 4 : 0) + len; if (b.length < need) return;
      let payload = b.subarray(off + (masked ? 4 : 0), need);
      if (masked) { const mk = b.subarray(off, off + 4); payload = Buffer.from(payload); for (let i = 0; i < payload.length; i++) payload[i] ^= mk[i & 3]; }
      this.buf = b.subarray(need);
      if (op === 8) { this.socket.end(); return; }
      if (op === 9) { this.write(10, payload); continue; }
      if (op === 10) continue;
      if (op === 0 || op === 1 || op === 2) {
        if (op !== 0) this.frag = [];
        this.frag.push(payload);
        if (fin) { const text = Buffer.concat(this.frag).toString('utf8'); this.frag = []; this.onMessage(text); }
      }
    }
  }
  write(op, payload) {
    if (!this.alive) return;
    const n = payload.length; let h;
    if (n < 126) h = Buffer.from([0x80 | op, n]);
    else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
    else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
    try { this.socket.write(Buffer.concat([h, payload])); } catch (e) {}
  }
  send(o) { this.write(1, Buffer.from(JSON.stringify(o))); }
  close() {
    if (!this.alive) return;
    leaveRoom(this); this.alive = false; clients.delete(this); try { this.socket.destroy(); } catch (e) {}
  }
  onMessage(text) {
    let m; try { m = JSON.parse(text); } catch (e) { return; }
    if (m && typeof m.t === 'string') handle(this, m);
  }
}
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  const c = new Client(socket); clients.add(c);
  broadcastRooms(); c.send({t: 'scores', list: topScores(), rank: -1});
});
setInterval(() => { for (const c of clients) c.write(9, Buffer.alloc(0)); }, 25000);

/* ---------- 개인전 랭킹 (scores.json에 저장) ---------- */
const SCORE_FILE = path.join(__dirname, 'scores.json');
let scores = [];
try { scores = JSON.parse(fs.readFileSync(SCORE_FILE, 'utf8')); } catch (e) {}
const saveScores = () => fs.writeFile(SCORE_FILE, JSON.stringify(scores), () => {});
const topScores = () => scores.slice(0, 20);

/* ---------- 방(매치) 관리 ---------- */
const rooms = new Map(); let nextId = 1;
const MODES = ['1v1', '2v2'];
const clean = (s, d) => (typeof s === 'string' ? s.replace(/[<>]/g, '').trim().slice(0, 16) : '') || d;
const roomInfo = r => ({id: r.id, name: r.name, mode: r.mode, state: r.state, members: r.members.map(m => ({name: m.name, ready: m.ready}))});
function broadcastRooms() {
  const list = [...rooms.values()].filter(r => r.state === 'waiting' && r.members.length < 2 && !r.quick)
    .map(r => ({id: r.id, name: r.name, mode: r.mode, host: r.members[0].name}));
  for (const c of clients) c.send({t: 'rooms', list});
}
function pushRoom(r) { r.members.forEach((m, i) => m.send({t: 'room', room: roomInfo(r), me: i})); }
function opp(c) { const r = c.room; return r ? r.members.find(m => m !== c) : null; }
function tryStart(r) {
  if (r.state !== 'waiting' || r.members.length < 2 || !r.members.every(m => m.ready)) return;
  r.state = 'playing';
  const seed = crypto.randomBytes(4).readUInt32BE(0);
  const names = r.members.map(m => m.name);
  r.members.forEach((m, i) => m.send({t: 'start', seed, you: i, names, mode: r.mode}));
  pushRoom(r); broadcastRooms();
}
function endRoom(r, loser) {
  if (r.state !== 'playing') return;
  r.state = 'waiting';
  r.members.forEach(m => { m.ready = !!r.quick ? false : false; m.send({t: 'over', won: m !== loser, reason: loser && !loser.alive ? 'opp_left' : 'ko'}); });
  pushRoom(r); broadcastRooms();
}
function leaveRoom(c) {
  const r = c.room; if (!r) return;
  c.room = null; c.ready = false;
  const wasPlaying = r.state === 'playing';
  r.members = r.members.filter(m => m !== c);
  if (!r.members.length) rooms.delete(r.id);
  else {
    if (wasPlaying) { r.state = 'waiting'; r.members[0].ready = false; r.members[0].send({t: 'over', won: true, reason: 'opp_left'}); }
    pushRoom(r);
  }
  c.send({t: 'room', room: null});
  broadcastRooms();
}
function joinRoom(c, r, auto) {
  r.members.push(c); c.room = r; c.ready = !!auto;
  pushRoom(r); broadcastRooms(); tryStart(r);
}
function handle(c, m) {
  switch (m.t) {
    case 'hello': c.name = clean(m.name, '손님'); break;
    case 'create': {
      if (c.room || !MODES.includes(m.mode)) return;
      const r = {id: nextId++, name: clean(m.name, c.name + '의 방'), mode: m.mode, state: 'waiting', members: [], quick: false};
      rooms.set(r.id, r); joinRoom(c, r, false); break;
    }
    case 'join': {
      const r = rooms.get(+m.id);
      if (c.room) return;
      if (!r || r.state !== 'waiting' || r.members.length >= 2) return c.send({t: 'err', msg: '참가할 수 없는 방이에요.'});
      joinRoom(c, r, false); break;
    }
    case 'quick': {
      if (c.room || !MODES.includes(m.mode)) return;
      let r = [...rooms.values()].find(x => x.mode === m.mode && x.state === 'waiting' && x.members.length === 1);
      if (r) { r.members[0].ready = true; joinRoom(c, r, true); }
      else { r = {id: nextId++, name: '빠른 매칭', mode: m.mode, state: 'waiting', members: [], quick: true}; rooms.set(r.id, r); joinRoom(c, r, true); }
      break;
    }
    case 'leave': leaveRoom(c); break;
    case 'ready': if (c.room && c.room.state === 'waiting') { c.ready = !!m.v; pushRoom(c.room); tryStart(c.room); } break;
    case 'state': { const o = opp(c); if (o && c.room.state === 'playing' && typeof m.s === 'object') o.send({t: 'state', s: m.s}); break; }
    case 'attack': { const o = opp(c); const n = Math.max(1, Math.min(12, m.n | 0)); if (o && c.room.state === 'playing') o.send({t: 'attack', n}); break; }
    case 'dead': if (c.room) endRoom(c.room, c); break;
    case 'list': broadcastRooms(); break;
    case 'score': {
      const sc = m.score | 0, ln = m.lines | 0;
      if (sc <= 0 || sc > 5e6 || ln < 0 || ln > 5000) return;
      const e = {name: clean(m.name, '손님'), score: sc, lines: ln, date: Date.now()};
      scores.push(e); scores.sort((a, b) => b.score - a.score); scores = scores.slice(0, 100); saveScores();
      c.send({t: 'scores', list: topScores(), rank: scores.indexOf(e)});
      for (const o of clients) if (o !== c) o.send({t: 'scores', list: topScores(), rank: -1});
      break;
    }
  }
}

server.listen(PORT, () => {
  console.log('\n공학아카데미 테트리스 아케이드 서버 실행 중!');
  console.log('  이 컴퓨터:   http://localhost:' + PORT);
  for (const list of Object.values(os.networkInterfaces()))
    for (const i of list) if (i.family === 'IPv4' && !i.internal) console.log('  다른 컴퓨터: http://' + i.address + ':' + PORT);
  console.log('\n(종료: Ctrl + C)\n');
});
