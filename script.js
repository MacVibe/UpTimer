const WebSocket = require("ws");
const { TextEncoder } = require("util");
const http = require("http");

const MODE_URL = "https://drive.google.com/uc?export=download&id=1Igt8Zf9xJ8VonOygxPb6KMb2qVQ2TD6g";
const WS_URL = "wss://ip-207-148-8-148.cavegame.io";
const encoder = new TextEncoder();

let CURRENT_MODE = 2;
let TARGET_BOT_COUNT = 50;
let SERVER_ONLINE = true;

const HEARTBEAT_INTERVAL = 5000;
const TEAM_INTERVAL = 2000;

const MAX_BUFFER = 1024;
const KILL_BUFFER = MAX_BUFFER * 10;

const bots = new Set();

let connectingSockets = 0;
let totalQueuedMessages = 0;
let lastActivity = Date.now();
let inactivityStart = null;

const INACTIVITY_THRESHOLD = 15000;

function safeSend(ws, data, force = false) {
    if (ws.readyState !== WebSocket.OPEN) return false;
    totalQueuedMessages += ws.bufferedAmount;
    if (ws.bufferedAmount > KILL_BUFFER) return "OVERFLOW";
    if (!force && ws.bufferedAmount > MAX_BUFFER) return false;
    ws.send(data);
    return true;
}

function createBot() {
    connectingSockets++;

    const bot = {
        ws: new WebSocket(WS_URL),
        destroyed: false,
        connecting: true
    };

    bot.ws.on("open", () => {
        bot.connecting = false;
        connectingSockets = Math.max(0, connectingSockets - 1);
        lastActivity = Date.now();
    });

    bot.ws.on("message", () => {
        lastActivity = Date.now();
    });

    bot.ws.on("close", () => destroyBot(bot));
    bot.ws.on("error", () => destroyBot(bot));

    bots.add(bot);
}

function destroyBot(bot) {
    if (bot.destroyed) return;
    bot.destroyed = true;

    if (bot.connecting) {
        bot.connecting = false;
        connectingSockets = Math.max(0, connectingSockets - 1);
    }

    try {
        bot.ws.terminate();
    } catch {}

    bots.delete(bot);
}

function ensureBotCount() {
    const connected = bots.size - connectingSockets;

    if (connected < TARGET_BOT_COUNT) {
        createBot();
    }
}

async function fetchInitialConfig() {
    try {
        const res = await fetch(MODE_URL + "&t=" + Date.now());
        const txt = await res.text();

        const mode = txt.includes("mode:1") ? 1 : 2;
        const amount = parseInt(txt.match(/amount:(\\d+)/)?.[1] || TARGET_BOT_COUNT);

        CURRENT_MODE = mode;
        TARGET_BOT_COUNT = Math.min(amount, 500);
    } catch {}
}

async function init() {
    await fetchInitialConfig();
    setInterval(ensureBotCount, 50);
}

init();

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
    res.end(`
<html>
<head>
<style>
body {
    margin: 0;
    background: #111;
    font-family: 'Segoe UI', Tahoma, sans-serif;
}

#panel {
    position: fixed;
    top: 20px;
    left: 20px;
    width: 240px;
    background: linear-gradient(145deg, #1f1f1f, #282828);
    border-radius: 12px;
    box-shadow: 0 4px 20px rgba(0,0,0,0.6);
    color: #f5f5f5;
    overflow: hidden;
    user-select: none;
}

#header {
    background: linear-gradient(to right, #00c6ff, #0072ff);
    padding: 8px;
    font-weight: bold;
    text-align: center;
    font-size: 14px;
    cursor: move;
}

#content {
    padding: 10px;
}

.stat {
    margin-bottom: 6px;
    padding: 6px;
    background: rgba(255,255,255,0.05);
    border-radius: 6px;
    font-size: 13px;
}
</style>
</head>

<body>

<div id="panel">
    <div id="header">Script Status</div>
    <div id="content">
        <div class="stat" id="uptime">UpTime: 0</div>
        <div class="stat" id="connected">Connected: 0</div>
        <div class="stat" id="connecting">Connecting: 0</div>
        <div class="stat" id="queued">Queued Messages: 0</div>
        <div class="stat" id="inactive">Inactive: 0</div>
    </div>
</div>

<script>
const panel = document.getElementById('panel');
const header = document.getElementById('header');

let dragging = false, offsetX = 0, offsetY = 0;

const savedX = localStorage.getItem("panelX");
const savedY = localStorage.getItem("panelY");

if (savedX && savedY) {
    panel.style.left = savedX + "px";
    panel.style.top = savedY + "px";
}

header.onmousedown = e => {
    dragging = true;
    offsetX = e.clientX - panel.offsetLeft;
    offsetY = e.clientY - panel.offsetTop;
};

document.onmousemove = e => {
    if (!dragging) return;
    panel.style.left = (e.clientX - offsetX) + 'px';
    panel.style.top = (e.clientY - offsetY) + 'px';
};

document.onmouseup = () => {
    dragging = false;
    localStorage.setItem("panelX", panel.offsetLeft);
    localStorage.setItem("panelY", panel.offsetTop);
};

async function update() {
    try {
        const res = await fetch('/stats');
        const data = await res.json();

        document.getElementById('uptime').textContent =
            "UpTime: " + Math.floor(data.uptime) + "s";

        document.getElementById('connected').textContent =
            "Connected: " + data.connected;

        document.getElementById('connecting').textContent =
            "Connecting: " + data.connecting;

        document.getElementById('queued').textContent =
            "Queued Messages: " + data.queuedMessages;

        document.getElementById('inactive').textContent =
            "Inactive: " + Math.floor(data.inactiveFor / 1000) + "s";

    } catch {}
}

setInterval(update, 1000);
update();
</script>

</body>
</html>
`);
}).listen(PORT);

setInterval(() => {
    totalQueuedMessages = 0;
}, 1000);
