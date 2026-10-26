'use strict';
// Servidor de movimentos em tempo real (Node.js + WebSocket) para o Render (ou Google Cloud Run).
// - Os clientes enviam a posição por WebSocket; o servidor repassa a todos da sala (10x/seg).
// - O Firebase fica só para persistir: a última posição de cada jogador é gravada em
//   rooms/<sala>/lastpos/<uid> (a cada 3s, ao desconectar e ao desligar a instância).
// - O Cloud Run derruba conexões após o timeout (máx. 60 min): o cliente reconecta sozinho
//   e o servidor devolve a última posição no "welcome".
const http = require('http');
const { WebSocketServer } = require('ws');
const admin = require('firebase-admin');

// Credencial: no Render, cole o JSON da conta de serviço na variável FIREBASE_SERVICE_ACCOUNT.
// (No Google Cloud Run essa variável pode ficar vazia: usa a conta de serviço do próprio serviço.)
let sa = (process.env.FIREBASE_SERVICE_ACCOUNT || '').trim();
if (!sa) { // alternativa: Secret File do Render (Environment > Secret Files > serviceAccount.json)
  for (const f of ['/etc/secrets/serviceAccount.json', process.env.FIREBASE_SERVICE_ACCOUNT_FILE]) {
    try { if (f) { sa = require('fs').readFileSync(f, 'utf8').trim(); console.log('Credencial lida do arquivo', f); break; } } catch (e) {}
  }
}
const dbUrl = (process.env.FIREBASE_DB_URL || '').trim();
let cred;
if (sa) {
  let j; try { j = JSON.parse(sa); } catch (e) { console.error('ERRO: FIREBASE_SERVICE_ACCOUNT não é um JSON válido (cole o arquivo inteiro, de { até }).'); process.exit(1); }
  cred = admin.credential.cert(j);
  console.log('Credencial: conta de serviço do projeto', j.project_id);
} else if (process.env.RENDER) {
  console.error('ERRO: credencial não encontrada. Crie a variável FIREBASE_SERVICE_ACCOUNT (Environment) ou o Secret File serviceAccount.json no Render.');
  process.exit(1);
} else {
  cred = admin.credential.applicationDefault();
  console.log('Credencial: applicationDefault (Google Cloud)');
}
if (!dbUrl) { console.error('ERRO: variável FIREBASE_DB_URL não encontrada.'); process.exit(1); }
admin.initializeApp({ credential: cred, databaseURL: dbUrl });
const db = admin.database();
const TS = admin.database.ServerValue.TIMESTAMP;

const PORT = process.env.PORT || 8080;
const ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const TICK_MS = 100, FLUSH_MS = 3000, HB_MS = 15000, PING_MS = 25000, MAX_MSG_PER_SEC = 40, LIM = 5000;

const rooms = new Map(); // sala -> { clients: Map<uid, cl>, moved: Set<uid>, dirty: Set<uid> }
const log = (...a) => console.log(new Date().toISOString(), ...a);
const send = (ws, s) => { if (ws.readyState === 1) ws.send(s); };
const arr = p => [p.x, p.y, p.z, p.yaw, p.w, p.c];
const rec = cl => Object.assign({}, cl.pos, { m: cl.m, ts: TS });

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  res.writeHead(404); res.end();
});
const wss = new WebSocketServer({
  server, maxPayload: 1024,
  verifyClient: i => {
    const ok = !ORIGINS.length || ORIGINS.includes(i.origin);
    if (!ok) log('origem bloqueada:', i.origin, '(permitidas:', ORIGINS.join(', ') + ')');
    return ok;
  }
});

wss.on('connection', (ws, req) => {
  log('conexão ws aberta, origem:', req.headers.origin || '-');
  const cl = { ws, uid: null, room: null, m: '0', pos: null, joining: false, n: 0, w: Date.now() };
  ws.isAlive = true;
  const joinTimer = setTimeout(() => { if (!cl.room) ws.close(4001, 'join timeout'); }, 10000);
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', async raw => {
    ws.isAlive = true;
    const now = Date.now();
    if (now - cl.w > 1000) { cl.w = now; cl.n = 0; }
    if (++cl.n > MAX_MSG_PER_SEC) return; // anti-flood
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (m.t === 'join' && !cl.joining) {
      cl.joining = true;
      try { await join(cl, m); } catch (e) { log('join falhou:', e.message); ws.close(4003, 'auth'); }
    } else if (m.t === 'pos' && cl.room) onPos(cl, m);
  });
  ws.on('close', () => { clearTimeout(joinTimer); leave(cl); });
  ws.on('error', () => {});
});

async function join(cl, m) {
  const room = String(m.room || '');
  if (!/^[\w-]{1,64}$/.test(room)) throw new Error('sala inválida');
  const uid = (await admin.auth().verifyIdToken(String(m.token || ''))).uid;
  const [pl, lp] = await Promise.all([
    db.ref('rooms/' + room + '/players/' + uid).once('value'),
    db.ref('rooms/' + room + '/lastpos/' + uid).once('value')
  ]);
  if (!pl.exists()) throw new Error('jogador não está na sala');
  if (cl.ws.readyState !== 1) return;

  let r = rooms.get(room);
  if (!r) rooms.set(room, r = { clients: new Map(), moved: new Set(), dirty: new Set() });
  const old = r.clients.get(uid);
  // posição mais recente: memória (reconexão rápida) > Firebase (reinício/ instância nova)
  const last = old && old.pos ? Object.assign({}, old.pos, { m: old.m }) : lp.val();
  if (old && old !== cl) old.ws.close(4004, 'replaced'); // mesma conta em outra aba/conexão

  cl.uid = uid; cl.room = room; cl.m = String(m.matchId == null ? 0 : m.matchId);
  if (last && last.m != null && String(last.m) === cl.m) cl.pos = { x: last.x, y: last.y, z: last.z, yaw: last.yaw, w: last.w || '', c: last.c ? 1 : 0 };
  r.clients.set(uid, cl);

  const peers = {};
  for (const [id, o] of r.clients) if (id !== uid && o.pos) peers[id] = arr(o.pos);
  send(cl.ws, JSON.stringify({ t: 'welcome', last: last || null, peers, st: Date.now() }));
  if (cl.pos) { r.moved.add(uid); r.dirty.add(uid); }
  log('join', room, uid, last ? '(com posição salva)' : '(nova)');
}

function onPos(cl, m) {
  const x = +m.x, y = +m.y, z = +m.z, yaw = +m.yaw;
  if (![x, y, z, yaw].every(Number.isFinite) || Math.abs(x) > LIM || Math.abs(y) > LIM || Math.abs(z) > LIM) return;
  cl.pos = { x, y, z, yaw, w: typeof m.w === 'string' ? m.w.slice(0, 16) : '', c: m.c ? 1 : 0 };
  const r = rooms.get(cl.room); if (!r) return;
  r.moved.add(cl.uid); r.dirty.add(cl.uid);
}

function leave(cl) {
  const r = cl.room && rooms.get(cl.room);
  if (!r || r.clients.get(cl.uid) !== cl) return; // conexão já substituída por outra
  r.clients.delete(cl.uid); r.moved.delete(cl.uid); r.dirty.delete(cl.uid);
  if (cl.pos) db.ref('rooms/' + cl.room + '/lastpos/' + cl.uid).set(rec(cl)).catch(e => log('save', e.message));
  const msg = JSON.stringify({ t: 'left', id: cl.uid });
  for (const o of r.clients.values()) send(o.ws, msg);
  if (!r.clients.size) rooms.delete(cl.room);
  log('leave', cl.room, cl.uid);
}

// difusão 10x/seg: um único pacote por sala com quem se mexeu
setInterval(() => {
  for (const r of rooms.values()) {
    if (!r.moved.size) continue;
    const p = {};
    for (const uid of r.moved) { const cl = r.clients.get(uid); if (cl && cl.pos) p[uid] = arr(cl.pos); }
    r.moved.clear();
    const msg = JSON.stringify({ t: 'snap', p });
    for (const cl of r.clients.values()) send(cl.ws, msg);
  }
}, TICK_MS);

// persistência no Firebase: 1 gravação por sala a cada 3s (só quem mudou)
function flushRoom(name, r) {
  if (!r.dirty.size) return Promise.resolve();
  const u = {};
  for (const uid of r.dirty) { const cl = r.clients.get(uid); if (cl && cl.pos) u[uid] = rec(cl); }
  r.dirty.clear();
  return Object.keys(u).length ? db.ref('rooms/' + name + '/lastpos').update(u).catch(e => log('flush', e.message)) : Promise.resolve();
}
setInterval(() => { for (const [n, r] of rooms) flushRoom(n, r); }, FLUSH_MS);

// keep-alive (evita conexões mortas e mantém proxies abertos)
setInterval(() => {
  for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; ws.ping(); }
}, PING_MS);
setInterval(() => { const s = JSON.stringify({ t: 'hb' }); for (const ws of wss.clients) send(ws, s); }, HB_MS);

// Cloud Run manda SIGTERM ao trocar/desligar a instância: salva tudo e avisa os clientes para reconectarem
process.on('SIGTERM', async () => {
  log('SIGTERM: salvando posições');
  await Promise.allSettled([...rooms].map(([n, r]) => { for (const [uid, cl] of r.clients) if (cl.pos) r.dirty.add(uid); return flushRoom(n, r); }));
  for (const ws of wss.clients) ws.close(1012, 'restart');
  setTimeout(() => process.exit(0), 1500);
});
process.on('unhandledRejection', e => log('unhandledRejection', e && e.message));

server.listen(PORT, () => log('rt ouvindo na porta', PORT));
