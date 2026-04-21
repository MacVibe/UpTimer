const WebSocket = require("ws");
const { TextEncoder } = require("util");
const http = require("http");

const fetch =
  globalThis.fetch ||
  ((...args) =>
    import("node-fetch").then(({ default: f }) => f(...args)));

const MODE_URL =
  "https://drive.google.com/uc?export=download&id=1Igt8Zf9xJ8VonOygxPb6KMb2qVQ2TD6g";
const WS_URL = "wss://ip-207-148-8-148.cavegame.io";

const encoder = new TextEncoder();

let CURRENT_MODE = 2;
let TARGET_BOT_COUNT = 50;
let SERVER_ONLINE = true;

const HEARTBEAT_INTERVAL = 5000;
const TEAM_INTERVAL = 2000;

const MAX_BUFFER = 4096;
const KILL_BUFFER = MAX_BUFFER * 10;

const TEAM_CREATE_PACKET = Uint8Array.from([
  49, 33, 47, 116, 101, 97, 109, 32, 99, 114, 101, 97, 116, 101, 32, 84,
  101, 115, 116, 101, 114, 115, 32, 103, 51, 56, 57, 56, 101, 110, 97, 107,
  108, 49, 48,
]);

const TEAM_JOIN_PACKET = Uint8Array.from([
  49, 31, 47, 116, 101, 97, 109, 32, 106, 111, 105, 110, 32, 84, 101, 115,
  116, 101, 114, 115, 32, 103, 51, 56, 57, 56, 101, 110, 97, 107, 108, 49, 48,
]);

const TEAM_JOINED_PACKET = Uint8Array.from([
  24, 0, 0, 12, 84, 101, 97, 109, 32, 106, 111, 105, 110, 101, 100, 33, 4,
  103, 111, 111, 100,
]);

const CHAT_JOIN_PACKET = Uint8Array.from([49, 10, 47, 116, 101, 97, 109, 32, 99, 104, 97, 116]);
const INFINITE_PACKET = Uint8Array.from([49, 120, 0]);

const HEARTBEATS = [
  Uint8Array.from([34, 0, 0, 0, 0, 0, 64, 128, 0, 192, 195, 166, 192, 0]),
  Uint8Array.from([34, 0, 0, 0, 0, 0, 194, 143, 255, 252, 67, 177, 63, 255]),
];

const bots = new Set();

let connectingSockets = 0;
let totalQueuedMessages = 0;
let lastActivity = Date.now();
let inactivityStart = null;

const INACTIVITY_THRESHOLD = 15000;

function safeSend(ws, data, force = false) {
  try {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    totalQueuedMessages += ws.bufferedAmount || 0;
    if (ws.bufferedAmount > KILL_BUFFER) return "OVERFLOW";
    if (!force && ws.bufferedAmount > MAX_BUFFER) return false;
    ws.send(data);
    return true;
  } catch {
    return false;
  }
}

function buildPacket(...bytes) {
  const randomNum = Math.floor(Math.random() * 10000);
  const randomBytes = Array.from(String(randomNum)).map((c) =>
    c.charCodeAt(0)
  );
  return Uint8Array.from([...bytes, ...randomBytes]);
}

function buildIntroPacket() {
  return buildPacket(31, 1, 13, 240, 159, 148, 146, 13, 240, 159, 148, 145);
}

function isExactTeamJoined(data) {
  const bytes = new Uint8Array(data);
  if (bytes.length !== TEAM_JOINED_PACKET.length) return false;
  for (let i = 0; i < bytes.length; i++) {
    if (bytes[i] !== TEAM_JOINED_PACKET[i]) return false;
  }
  return true;
}

function clearBotIntervals(bot) {
  for (const i of bot.intervals) clearInterval(i);
  bot.intervals.length = 0;
}

function destroyBot(bot) {
  if (!bot || bot.destroyed) return;
  bot.destroyed = true;
  if (bot.connecting) {
    bot.connecting = false;
    connectingSockets = Math.max(0, connectingSockets - 1);
  }
  clearBotIntervals(bot);
  try {
    bot.ws?.removeAllListeners();
    bot.ws?.terminate();
  } catch {}
  bots.delete(bot);
}

function attachBotHandlers(bot) {
  const ws = bot.ws;

  ws.on("open", () => {
    if (bot.connecting) {
      bot.connecting = false;
      connectingSockets = Math.max(0, connectingSockets - 1);
    }

    lastActivity = Date.now();
    SERVER_ONLINE = true;

    clearBotIntervals(bot);

    bot.hbIndex = bot.hbIndex || 0;

    safeSend(ws, Uint8Array.from([48]));
    safeSend(ws, buildIntroPacket());
    safeSend(ws, TEAM_CREATE_PACKET, true);

    bot.intervals.push(
      setInterval(() => {
        if (bot.destroyed) return;
        const packet = HEARTBEATS[bot.hbIndex % 2];
        bot.hbIndex++;
        const res = safeSend(ws, packet, true);
        if (res === "OVERFLOW") destroyBot(bot);
      }, HEARTBEAT_INTERVAL)
    );

    const joinInterval = setInterval(() => {
      if (bot.destroyed) return;
      if (!bot.joined && ws.readyState === WebSocket.OPEN) {
        const res = safeSend(ws, TEAM_JOIN_PACKET, true);
        if (res === "OVERFLOW") destroyBot(bot);
      } else {
        clearInterval(joinInterval);
      }
    }, TEAM_INTERVAL);

    bot.intervals.push(joinInterval);

    bot.intervals.push(
      setInterval(() => {
        if (bot.destroyed || !bot.joined) return;
        if (CURRENT_MODE !== 1) return;
        const res = safeSend(ws, INFINITE_PACKET, true);
        if (res === "OVERFLOW") destroyBot(bot);
      }, 10)
    );
  });

  ws.on("message", (data) => {
    lastActivity = Date.now();
    if (!bot.joined && isExactTeamJoined(data)) {
      bot.joined = true;
      safeSend(ws, CHAT_JOIN_PACKET, true);
    }
  });

  ws.on("close", () => destroyBot(bot));
  ws.on("error", () => destroyBot(bot));
}

function createBot() {
  connectingSockets++;
  const bot = {
    ws: new WebSocket(WS_URL),
    joined: false,
    destroyed: false,
    intervals: [],
    hbIndex: 0,
    connecting: true,
  };
  attachBotHandlers(bot);
  bots.add(bot);
}

function ensureBotCount() {
  if (!SERVER_ONLINE) return;

  const connected = bots.size - connectingSockets;

  if (connected >= TARGET_BOT_COUNT) {
    for (const bot of [...bots]) if (bot.connecting) destroyBot(bot);
    return;
  }

  let total = bots.size;

  while (total < TARGET_BOT_COUNT) {
    createBot();
    total++;
  }

  if (total > TARGET_BOT_COUNT) {
    let excess = total - TARGET_BOT_COUNT;

    for (const bot of [...bots]) {
      if (excess <= 0) break;
      if (bot.connecting) {
        destroyBot(bot);
        excess--;
      }
    }

    for (const bot of [...bots]) {
      if (excess <= 0) break;
      destroyBot(bot);
      excess--;
    }
  }
}

function applyConfig(newMode, newAmount) {
  if (newMode === CURRENT_MODE && newAmount === TARGET_BOT_COUNT) return;
  CURRENT_MODE = newMode;
  TARGET_BOT_COUNT = newAmount;
  ensureBotCount();
}

function parseConfig(text) {
  const lines = text.replace(/\r/g, "").split("\n").map(l => l.trim().toLowerCase());
  let mode = CURRENT_MODE;
  let amount = TARGET_BOT_COUNT;

  for (const line of lines) {
    if (line.startsWith("mode:")) {
      const val = parseInt(line.split(":")[1]?.trim());
      if (val === 1 || val === 2) mode = val;
    }
    if (line.startsWith("amount:")) {
      const val = parseInt(line.split(":")[1]?.trim());
      if (!isNaN(val) && val > 0) amount = val;
    }
  }

  return { mode, amount: Math.min(amount, 500) };
}

async function fetchInitialConfig() {
  try {
    const res = await fetch(MODE_URL + "&t=" + Date.now());
    const txt = await res.text();
    const { mode, amount } = parseConfig(txt);
    CURRENT_MODE = mode;
    TARGET_BOT_COUNT = amount;
  } catch {}
}

async function pollConfigFile() {
  try {
    const res = await fetch(MODE_URL + "&t=" + Date.now());
    const txt = await res.text();
    const { mode, amount } = parseConfig(txt);
    applyConfig(mode, amount);
  } catch {}
}

async function init() {
  await fetchInitialConfig();
  ensureBotCount();
  setInterval(pollConfigFile, 3000);
  setInterval(ensureBotCount, 30);
}

init();

process.on("uncaughtException", () => {});
process.on("unhandledRejection", () => {});

setInterval(() => {
  const now = Date.now();
  const inactive = now - lastActivity > INACTIVITY_THRESHOLD;
  if (inactive) {
    if (!inactivityStart) inactivityStart = now;
  } else {
    inactivityStart = null;
  }
}, 1000);

const PORT = process.env.PORT || 3000;

http.createServer((req, res) => {
  if (req.url === "/stats") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      connected: bots.size - connectingSockets,
      connecting: connectingSockets,
      total: bots.size,
      queuedMessages: totalQueuedMessages,
      uptime: process.uptime(),
      inactiveFor: inactivityStart ? Date.now() - inactivityStart : 0
    }));
    return;
  }

  res.writeHead(200, { "Content-Type": "text/html" });
  res.end(`<!DOCTYPE html>
<html>
<head>
<title>UpTimer</title>
<link rel="icon" type="image/png" href="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAMAAACdt4HsAAAAjVBMVEWwWECwWUCxWUCyWUCyWUGyWkCyWkGzWUGzWkCzWkG0WkG0W0G1W0G1W0K2W0K2XEK3XEK3XEO4XEO4XUO5XEO5XUO6XUO6XUS6XkS7XkS8XkS8X0S8X0W9X0S9X0W+X0W/X0W/YEXAYEXAYEbBYEbBYUXBYUbCYUbDYUbDYUfDYkbDYkfEYUfEYkfFYkeohyysAAADQ0lEQVRYw51X22KbMAx1nGVr2rWjWUjBHjhgiunF/P/nDWwLfCMk8RvGOsjS0ZFA+VWrlLCY9wZdB1BNADQOUMvyIkA9AZAIQNn2Uopp67ALAVqw77MIgFCvCrNzxHjrn8qbzgB85TEP1KtWb7zhcb0FPmQ5LSve1jGAXMH3egPrdcyvXAqADfaVft4aAHy4BSAXkN0d2ONN4h8lFwBg7bG1gkSw0Jxx5OT/YNmnIZUaxyHWdEP6kRvW42TvRZHqJM7XOPc6r0i4B08mAn4Mv9Rp6zAxxEDSO3lSeUjixWDzYgIIimAbsR++KLxKbAEg4Fa+e44mjDexAkXyI79vFQaA5veuwfq7rdYEhZJljWAeE0kdEZVP2UtRrdbCFBVRFW4Rq3s6AMf9EgCojgMSCOmgFw9xgGySPck9BhceU3dRgGIGmGNR9q6QZlovdjGAWbltxpKC8XZ+THWp/IkBiMleXAi6kszf0SvMDtQXmbHFT9Egzs1LFpe59RhPI4+GYEkJIwBFBUGwYkarrreK5bQmqiUfVQ54V9adS8Nk0qmKLnbnQSqN08w41MGblyH+J6BXV623d2inELsxgds5Xv0qANTGWVNooyj0MEEbtqD3RdowN6jJ1K8YVNhIUzTPHKkvpb2b1kfomIUwu2OloLlYPYoBucUZnn/+gLmBcOUXVaIKeRo/sD15utsyZ0TwCaYB6vmKGDmH2AoLRwlBH632ACmAl9uEeVBM5LS0/Y3KTklm8eAJ4+z27mATKXm94pvVZ3nPpKpjVqjyau4GAL3IlgCykkQSPq8uEG0HgHJoCLUUbpfjxBmYeVyR+sk7ReG+40zZkRZMqAH4zqIedDDMzy2qH/xorHrqIiM/cif62hqftGYwq70a4W3+5fnfQ/rmx0D2oiKWDhg1s0p6lH5eZjASb/ZuFs6BwKuMn61eU1NzfYKhQ0V4ILz/IynCqiQbaJFomS/h/5HXIvFrFMBq88u1pSU2jQKw76u6dHpIXiALafK8t2dzUnIRMG65nLU3h7Bum+JKPdjcoWUOgBp7HlfO1S1nJY3UKTJNb7cC8AXkrEOAZ7zBv1Y8yKBLyfcQIEtXxxCLXfy+v3c6Afhz8391Qq4gKHLTTQAAAABJRU5ErkJggg==">
<style>
body{margin:0;background:#111;font-family:Segoe UI,Tahoma,sans-serif}
#panel{position:fixed;top:20px;left:20px;width:240px;background:#222;border-radius:12px;color:#fff;overflow:hidden}
#header{background:#0072ff;padding:8px;font-weight:bold;text-align:center;cursor:move;user-select:none}
#content{padding:10px}
.stat{margin-bottom:6px;padding:6px;background:rgba(255,255,255,0.05);border-radius:6px;font-size:13px}
</style>
</head>
<body>
<div id="panel">
<div id="header">
  <img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAMAAACdt4HsAAAAjVBMVEWwWECwWUCxWUCyWUCyWUGyWkCyWkGzWUGzWkCzWkG0WkG0W0G1W0G1W0K2W0K2XEK3XEK3XEO4XEO4XUO5XEO5XUO6XUO6XUS6XkS7XkS8XkS8X0S8X0W9X0S9X0W+X0W/X0W/YEXAYEXAYEbBYEbBYUXBYUbCYUbDYUbDYUfDYkbDYkfEYUfEYkfFYkeohyysAAADQ0lEQVRYw51X22KbMAx1nGVr2rWjWUjBHjhgiunF/P/nDWwLfCMk8RvGOsjS0ZFA+VWrlLCY9wZdB1BNADQOUMvyIkA9AZAIQNn2Uopp67ALAVqw77MIgFCvCrNzxHjrn8qbzgB85TEP1KtWb7zhcb0FPmQ5LSve1jGAXMH3egPrdcyvXAqADfaVft4aAHy4BSAXkN0d2ONN4h8lFwBg7bG1gkSw0Jxx5OT/YNmnIZUaxyHWdEP6kRvW42TvRZHqJM7XOPc6r0i4B08mAn4Mv9Rp6zAxxEDSO3lSeUjixWDzYgIIimAbsR++KLxKbAEg4Fa+e44mjDexAkXyI79vFQaA5veuwfq7rdYEhZJljWAeE0kdEZVP2UtRrdbCFBVRFW4Rq3s6AMf9EgCojgMSCOmgFw9xgGySPck9BhceU3dRgGIGmGNR9q6QZlovdjGAWbltxpKC8XZ+THWp/IkBiMleXAi6kszf0SvMDtQXmbHFT9Egzs1LFpe59RhPI4+GYEkJIwBFBUGwYkarrreK5bQmqiUfVQ54V9adS8Nk0qmKLnbnQSqN08w41MGblyH+J6BXV623d2inELsxgds5Xv0qANTGWVNooyj0MEEbtqD3RdowN6jJ1K8YVNhIUzTPHKkvpb2b1kfomIUwu2OloLlYPYoBucUZnn/+gLmBcOUXVaIKeRo/sD15utsyZ0TwCaYB6vmKGDmH2AoLRwlBH632ACmAl9uEeVBM5LS0/Y3KTklm8eAJ4+z27mATKXm94pvVZ3nPpKpjVqjyau4GAL3IlgCykkQSPq8uEG0HgHJoCLUUbpfjxBmYeVyR+sk7ReG+40zZkRZMqAH4zqIedDDMzy2qH/xorHrqIiM/cif62hqftGYwq70a4W3+5fnfQ/rmx0D2oiKWDhg1s0p6lH5eZjASb/ZuFs6BwKuMn61eU1NzfYKhQ0V4ILz/IynCqiQbaJFomS/h/5HXIvFrFMBq88u1pSU2jQKw76u6dHpIXiALafK8t2dzUnIRMG65nLU3h7Bum+JKPdjcoWUOgBp7HlfO1S1nJY3UKTJNb7cC8AXkrEOAZ7zBv1Y8yKBLyfcQIEtXxxCLXfy+v3c6Afhz8391Qq4gKHLTTQAAAABJRU5ErkJggg==" 
  style="height:18px;vertical-align:middle;margin-right:6px;">
  Script Status
</div>
<div id="content">
<div class="stat" id="uptime"></div>
<div class="stat" id="connected"></div>
<div class="stat" id="connecting"></div>
<div class="stat" id="queued"></div>
<div class="stat" id="inactive"></div>
</div>
</div>

<script>
const panel = document.getElementById("panel");
const header = document.getElementById("header");

let dragging = false;
let offsetX = 0;
let offsetY = 0;

const savedX = localStorage.getItem("panelX");
const savedY = localStorage.getItem("panelY");

if (savedX && savedY) {
    panel.style.left = savedX + "px";
    panel.style.top = savedY + "px";
}

header.onmousedown = (e) => {
    dragging = true;
    offsetX = e.clientX - panel.offsetLeft;
    offsetY = e.clientY - panel.offsetTop;
    e.preventDefault();
};

window.onmousemove = (e) => {
    if (!dragging) return;
    panel.style.left = (e.clientX - offsetX) + "px";
    panel.style.top = (e.clientY - offsetY) + "px";
};

window.onmouseup = () => {
    dragging = false;
    localStorage.setItem("panelX", panel.offsetLeft);
    localStorage.setItem("panelY", panel.offsetTop);
};

async function update(){
try{
const r=await fetch('/stats');
const d=await r.json();
uptime.textContent="UpTime: "+Math.floor(d.uptime)+"s";
connected.textContent="Connected: "+d.connected;
connecting.textContent="Connecting: "+d.connecting;
queued.textContent="Queued Messages: "+d.queuedMessages;
inactive.textContent="Inactive: "+Math.floor(d.inactiveFor/1000)+"s";
}catch{}
}
setInterval(update,1000);
update();
</script>

</body>
</html>`);
}).listen(PORT);

setInterval(() => {
  totalQueuedMessages = 0;
}, 1000);
