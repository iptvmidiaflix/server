'use strict';
// Servidor de movimentos em tempo real (Node.js + WebSocket) para o Render (ou Google Cloud Run).
// - Clientes enviam a posição por WebSocket; o servidor repassa a todos da sala (10x/seg).
// - O Firebase só persiste: última posição em rooms/<sala>/lastpos/<uid> (a cada 3s, ao sair e ao desligar).
// - Se a conexão cair (deploy, 60 min do Cloud Run, rede), o cliente reconecta e recebe a última posição.
// - O servidor SEMPRE sobe (não derruba o deploy): se faltar credencial, avisa nos Logs e recusa os joins.
const http = require('http');
const fs = require('fs');
const { WebSocketServer } = require('ws');
const admin = require('firebase-admin');

const PORT = process.env.PORT || 8080;
const ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const TICK_MS = 50, FLUSH_MS = 3000, HB_MS = 15000, PING_MS = 25000, MAX_MSG_PER_SEC = 80, LIM = 5000;
const log = (...a) => console.log(new Date().toISOString(), ...a);

// ---------- credencial do Firebase ----------
// Aceita: (1) FIREBASE_SERVICE_ACCOUNT = JSON inteiro; (2) Secret File /etc/secrets/serviceAccount.json;
// (3) FIREBASE_PROJECT_ID + FIREBASE_CLIENT_EMAIL + FIREBASE_PRIVATE_KEY; (4) credencial do Google Cloud.
function parseCred(raw) {
  raw = String(raw || '').trim();
  if (!raw) return null;
  try { return JSON.parse(raw); } catch (e) { /* JSON quebrado por quebras de linha: tenta extrair os campos */ }
  const f = k => { const m = raw.match(new RegExp('"' + k + '"\\s*:\\s*"([\\s\\S]*?)"\\s*[,}]')); return m ? m[1] : ''; };
  const j = { project_id: f('project_id'), client_email: f('client_email'), private_key: f('private_key') };
  return j.project_id && j.client_email && j.private_key ? j : null;
}
function loadCred() {
  let raw = process.env.FIREBASE_SERVICE_ACCOUNT;
  if (!raw || !raw.trim()) {
    for (const f of ['/etc/secrets/serviceAccount.json', process.env.FIREBASE_SERVICE_ACCOUNT_FILE]) {
      try { if (f && fs.existsSync(f)) { raw = fs.readFileSync(f, 'utf8'); log('Credencial lida do arquivo', f); break; } } catch (e) {}
    }
  }
  let j = parseCred(raw);
  if (!j && process.env.FIREBASE_PROJECT_ID && process.env.FIREBASE_CLIENT_EMAIL && process.env.FIREBASE_PRIVATE_KEY) {
    j = { project_id: process.env.FIREBASE_PROJECT_ID, client_email: process.env.FIREBASE_CLIENT_EMAIL, private_key: process.env.FIREBASE_PRIVATE_KEY };
  }
  if (j) {
    j.private_key = String(j.private_key).replace(/\\n/g, '\n');
    return { cert: admin.credential.cert({ projectId: j.project_id, clientEmail: j.client_email, privateKey: j.private_key }), project: j.project_id };
  }
  if (raw && raw.trim()) {
    const t = raw.trim(), has = k => t.includes('"' + k + '"') ? 'sim' : 'NÃO';
    throw new Error('FIREBASE_SERVICE_ACCOUNT inválido: cole o arquivo .json inteiro (de { até }). Diagnóstico: tamanho=' + t.length +
      ' caracteres (o normal é ~2300), começa com "{"=' + (t[0] === '{' ? 'sim' : 'NÃO') + ', termina com "}"=' + (t[t.length - 1] === '}' ? 'sim' : 'NÃO') +
      ', type=' + has('type') + ', project_id=' + has('project_id') + ', client_email=' + has('client_email') + ', private_key=' + has('private_key') +
      (has('apiKey') === 'sim' ? ' | ATENÇÃO: isso parece o config do SITE (apiKey), não a chave de serviço!' : ''));
  }
  if (process.env.RENDER) throw new Error('credencial não encontrada: crie a variável FIREBASE_SERVICE_ACCOUNT (Environment) ou o Secret File serviceAccount.json.');
  return { cert: admin.credential.applicationDefault(), project: '(Google Cloud)' };
}

let db = null, TS = null, initError = 'iniciando...';
function initFirebase() {
  try {
    const dbUrl = (process.env.FIREBASE_DB_URL || '').trim();
    if (!dbUrl) throw new Error('variável FIREBASE_DB_URL não encontrada (ex.: https://SEU-PROJETO-default-rtdb.firebaseio.com).');
    const c = loadCred();
    admin.initializeApp({ credential: c.cert, databaseURL: dbUrl });
    db = admin.database(); TS = admin.database.ServerValue.TIMESTAMP; initError = null;
    log('Firebase OK. Projeto:', c.project, '| banco:', dbUrl);
  } catch (e) { initError = e.message; log('ERRO DE CONFIGURAÇÃO:', e.message); }
}
initFirebase();
if (initError) setInterval(() => log('ERRO DE CONFIGURAÇÃO (o servidor está no ar, mas ninguém consegue entrar):', initError), 30000);

// ---------- estado ----------
const rooms = new Map(); // sala -> { clients: Map<uid, cl>, moved: Set<uid>, dirty: Set<uid> }
const send = (ws, s) => { if (ws.readyState === 1) ws.send(s); };
const arr = p => [p.x, p.y, p.z, p.yaw, p.w, p.c];
const rec = cl => Object.assign({}, cl.pos, { m: cl.m, ts: TS });

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); return res.end(initError ? 'ok (sem firebase: ' + initError + ')' : 'ok'); }
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
    else if (m.t === 'ping') send(ws, JSON.stringify({ t: 'pong', c: +m.c || 0 }));
  });
  ws.on('close', () => { clearTimeout(joinTimer); leave(cl); });
  ws.on('error', () => {});
});

async function join(cl, m) {
  if (initError) throw new Error('servidor sem configuração do Firebase: ' + initError);
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
  // posição mais recente: memória (reconexão rápida) > Firebase (reinício / instância nova)
  const last = old && old.pos ? Object.assign({}, old.pos, { m: old.m }) : lp.val();
  if (old && old !== cl) old.ws.close(4004, 'replaced'); // mesma conta em outra aba

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
  if (cl.pos && db) db.ref('rooms/' + cl.room + '/lastpos/' + cl.uid).set(rec(cl)).catch(e => log('save', e.message));
  const msg = JSON.stringify({ t: 'left', id: cl.uid });
  for (const o of r.clients.values()) send(o.ws, msg);
  if (!r.clients.size) rooms.delete(cl.room);
  log('leave', cl.room, cl.uid);
}

// difusão 20x/seg: um pacote por sala com quem se mexeu
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
  if (!db || !r.dirty.size) return Promise.resolve();
  const u = {};
  for (const uid of r.dirty) { const cl = r.clients.get(uid); if (cl && cl.pos) u[uid] = rec(cl); }
  r.dirty.clear();
  return Object.keys(u).length ? db.ref('rooms/' + name + '/lastpos').update(u).catch(e => log('flush', e.message)) : Promise.resolve();
}
setInterval(() => { for (const [n, r] of rooms) flushRoom(n, r); }, FLUSH_MS);

// keep-alive
setInterval(() => {
  for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; ws.ping(); }
}, PING_MS);
setInterval(() => { const s = JSON.stringify({ t: 'hb' }); for (const ws of wss.clients) send(ws, s); }, HB_MS);

// SIGTERM (deploy/reinício): salva tudo e avisa os clientes para reconectarem
process.on('SIGTERM', async () => {
  log('SIGTERM: salvando posições');
  await Promise.allSettled([...rooms].map(([n, r]) => { for (const [uid, cl] of r.clients) if (cl.pos) r.dirty.add(uid); return flushRoom(n, r); }));
  for (const ws of wss.clients) ws.close(1012, 'restart');
  setTimeout(() => process.exit(0), 1500);
});
process.on('unhandledRejection', e => log('unhandledRejection', e && e.message));
process.on('uncaughtException', e => log('uncaughtException', e && e.message));

server.listen(PORT, '0.0.0.0', () => log('rt ouvindo na porta', PORT));
