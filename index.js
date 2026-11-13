const makeWASocket = require('@whiskeysockets/baileys').default;
const {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
} = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');
const express = require('express');
const pino = require('pino');
const fs = require('fs');
const path = require('path');
const multer = require('multer');

const PORT = process.env.PORT || 25029;
const HOST = '0.0.0.0';
const SESSIONS_BASE_DIR = 'sessions_data';

const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'raja123';

if (!fs.existsSync(SESSIONS_BASE_DIR)) {
  fs.mkdirSync(SESSIONS_BASE_DIR, { recursive: true });
}

const MIN_DELAY_SECONDS = 3;
const DEFAULT_DELAY_SECONDS = 10;
const SEND_RETRY = 1;
const RETRY_WAIT_MS = 2000;
const WATCHDOG_INTERVAL_MS = 30000;

const sessions = {};
let serverStartTime = Date.now();

function formatUptime(ms) {
  const s = Math.floor(ms / 1000);
  const h = String(Math.floor(s / 3600)).padStart(2, '0');
  const m = String(Math.floor((s % 3600) / 60)).padStart(2, '0');
  const sec = String(s % 60).padStart(2, '0');
  return `${h}:${m}:${sec}`;
}

class WhatsAppSession {
  constructor(name) {
    this.name = name;
    this.authDir = path.join(SESSIONS_BASE_DIR, name);
    this.sock = null;
    this.pairingCode = null;
    this.isPaired = false;
    this.currentPhone = null;
    this.pairingRequested = false;
    this.isConnecting = false;
    this.lastError = null;
    this.connectedAt = null;
    this.groupsCache = {};

    this.bulkState = {
      running: false,
      stopFlag: false,
      sent: 0,
      failed: 0,
      tasks: 0,
      total: 0,
      remaining: 0,
      cycle: 0,
      msgIndex: 0,
      targetIndex: 0,
      totalTargets: 0,
      currentMessage: '',
      currentTarget: '',
      targets: [],
      messages: [],
      delayMs: DEFAULT_DELAY_SECONDS * 1000,
      logs: [],
      logId: 0,
      startedAt: null,
      workerAlive: false,
      lastBeat: 0,
    };
  }

  pushLog(type, msg) {
    this.bulkState.logs.push({ id: ++this.bulkState.logId, ts: Date.now(), type, msg });
    if (this.bulkState.logs.length > 500) this.bulkState.logs.splice(0, this.bulkState.logs.length - 500);
    const icons = { ok: '✅', err: '❌', warn: '⚠️', info: 'ℹ️' };
    console.log(`[${this.name}] ${icons[type] || '•'} ${msg}`);
  }

  isSocketReady() {
    return !!(this.sock && this.isPaired);
  }

  getGroupsFromStore() {
    const out = [];
    try {
      if (this.sock && this.sock.store && this.sock.store.groupMetadata) {
        const map = this.sock.store.groupMetadata;
        map.forEach((v) => {
          out.push({
            id: v.id,
            name: v.subject || '',
            size: v.participants ? v.participants.length : 0,
          });
        });
      }
    } catch (_) {}
    return out;
  }

  async safeSend(jid, text) {
    let lastErr = null;
    for (let attempt = 0; attempt <= SEND_RETRY; attempt++) {
      try {
        if (!this.isSocketReady()) throw new Error('socket not ready');
        await this.sock.sendMessage(jid, { text });
        return { ok: true };
      } catch (e) {
        lastErr = e;
        if (attempt < SEND_RETRY) {
          this.pushLog('warn', `Retry ${attempt + 1}/${SEND_RETRY} → ${jid}: ${e.message}`);
          await new Promise((r) => setTimeout(r, RETRY_WAIT_MS));
        }
      }
    }
    return { ok: false, error: lastErr ? lastErr.message : 'unknown' };
  }

  async runWorker() {
    if (this.bulkState.workerAlive) return;
    this.bulkState.workerAlive = true;
    this.pushLog('info', 'Worker started — loop active');

    try {
      while (!this.bulkState.stopFlag) {
        if (!this.isSocketReady()) {
          this.bulkState.lastBeat = Date.now();
          await new Promise((r) => setTimeout(r, 3000));
          continue;
        }

        for (let mi = 0; mi < this.bulkState.messages.length && !this.bulkState.stopFlag; mi++) {
          const msg = this.bulkState.messages[mi];
          this.bulkState.msgIndex = mi;
          this.bulkState.currentMessage = msg;

          for (let ti = 0; ti < this.bulkState.targets.length && !this.bulkState.stopFlag; ti++) {
            const t = this.bulkState.targets[ti];
            this.bulkState.targetIndex = ti;
            this.bulkState.currentTarget = t.label;
            this.bulkState.lastBeat = Date.now();

            if (!this.isSocketReady()) {
              this.pushLog('warn', 'Socket temporarily unavailable — waiting to resume');
              break;
            }

            try {
              const res = await this.safeSend(t.jid, msg);
              if (res.ok) {
                this.bulkState.sent++;
                this.pushLog('ok', `Cycle ${this.bulkState.cycle + 1} | Msg ${mi + 1}/${this.bulkState.messages.length} → ${t.label}`);
              } else {
                this.bulkState.failed++;
                this.pushLog('err', `Msg ${mi + 1} → ${t.label}: ${res.error}`);
              }
            } catch (e) {
              this.bulkState.failed++;
              this.pushLog('err', `Unexpected send error → ${t.label}: ${e.message}`);
            }

            this.bulkState.remaining = this.bulkState.messages.length - (mi + 1);

            if (!this.bulkState.stopFlag) {
              await new Promise((r) => setTimeout(r, this.bulkState.delayMs));
            }
          }

          if (!this.isSocketReady() && !this.bulkState.stopFlag) break;
        }

        if (!this.bulkState.stopFlag) {
          this.bulkState.cycle++;
          this.bulkState.remaining = this.bulkState.messages.length;
          this.bulkState.msgIndex = 0;
          this.bulkState.targetIndex = 0;
          this.pushLog('info', `Cycle ${this.bulkState.cycle} completed — restarting from Message 1`);
        }
      }
    } catch (loopErr) {
      this.pushLog('err', 'Worker crashed: ' + loopErr.message);
      if (!this.bulkState.stopFlag) {
        this.pushLog('warn', 'Worker auto-restarting in 3s...');
        this.bulkState.workerAlive = false;
        this.bulkState.running = true;
        setTimeout(() => { this.runWorker(); }, 3000);
        return;
      }
    } finally {
      if (this.bulkState.stopFlag) {
        this.bulkState.running = false;
        this.bulkState.stopFlag = false;
        this.bulkState.workerAlive = false;
        this.bulkState.currentMessage = '';
        this.bulkState.currentTarget = '';
        this.pushLog('info', `Task stopped — Sent: ${this.bulkState.sent}, Failed: ${this.bulkState.failed}, Cycles: ${this.bulkState.cycle}`);
      } else {
        this.bulkState.workerAlive = false;
      }
    }
  }

  async connect(phone) {
    if (!phone) throw new Error('Phone number required');
    if (this.isConnecting) throw new Error('Connection already in progress');
    if (this.isPaired) throw new Error('Already paired');

    this.isConnecting = true;
    this.currentPhone = phone;
    this.pairingCode = null;
    this.lastError = null;

    try {
      const { state, saveCreds } = await useMultiFileAuthState(this.authDir);
      const { version } = await fetchLatestBaileysVersion();

      // FIXED: Added proper mobile pairing socket configuration to bypass "Couldn't link device" error
      this.sock = makeWASocket({
        version,
        auth: state,
        printQRInTerminal: false,
        logger: pino({ level: 'silent' }),
        browser: ['Ubuntu', 'Chrome', '20.0.04'],
        mobile: false,
        syncFullHistory: false,
        markOnlineOnConnect: true,
      });

      this.sock.ev.on('creds.update', saveCreds);

      this.sock.ev.on('groups.upsert', (groups) => {
        groups.forEach((g) => {
          this.groupsCache[g.id] = {
            id: g.id,
            name: g.subject || '',
            size: g.participants ? g.participants.length : 0,
          };
        });
      });

      this.sock.ev.on('groups.update', (updates) => {
        updates.forEach((u) => {
          if (this.groupsCache[u.id]) {
            if (u.subject) this.groupsCache[u.id].name = u.subject;
          } else if (u.id) {
            this.groupsCache[u.id] = { id: u.id, name: u.subject || '', size: 0 };
          }
        });
      });

      this.sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;

        if (connection === 'connecting' && !this.sock.authState.creds.registered && !this.pairingRequested) {
          this.pairingRequested = true;
          try {
            // Wait 4 seconds for complete socket registration before requesting pairing code
            await new Promise((r) => setTimeout(r, 4000));
            this.pairingCode = await this.sock.requestPairingCode(phone);
            this.pushLog('info', `Pairing code for ${phone}: ${this.pairingCode}`);
            this.lastError = null;
          } catch (err) {
            this.pushLog('err', 'Pairing code error: ' + err.message);
            this.lastError = err.message;
            this.pairingCode = null;
            this.pairingRequested = false;
            this.isConnecting = false;
          }
        }

        if (connection === 'open') {
          this.isPaired = true;
          this.pairingCode = null;
          this.pairingRequested = false;
          this.isConnecting = false;
          this.connectedAt = new Date().toISOString();
          this.lastError = null;
          this.pushLog('ok', `WhatsApp connected (${phone})`);

          setTimeout(() => {
            try {
              const groups = this.getGroupsFromStore();
              groups.forEach((g) => { this.groupsCache[g.id] = g; });
              this.pushLog('info', `Loaded ${groups.length} groups into cache`);
            } catch (e) {}
          }, 3000);

          if (this.bulkState.running && !this.bulkState.workerAlive) {
            this.pushLog('info', 'Reconnect detected — resuming bulk worker');
            this.runWorker();
          }
        }

        if (connection === 'close') {
          const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
          const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
          this.pushLog('warn', `Connection closed (code ${statusCode})`);

          this.isPaired = false;
          this.isConnecting = false;

          if (shouldReconnect && this.currentPhone) {
            this.pairingRequested = false;
            setTimeout(() => this.connect(this.currentPhone).catch(() => {}), 3000);
          } else if (statusCode === DisconnectReason.loggedOut) {
            this.pairingCode = null;
            this.pairingRequested = false;
            this.currentPhone = null;
            if (this.bulkState.running) {
              this.bulkState.stopFlag = true;
              this.pushLog('warn', 'Logged out — stopping bulk worker');
            }
          }
        }
      });
    } catch (err) {
      this.isConnecting = false;
      this.lastError = err.message;
      throw err;
    }
  }

  async logout() {
    if (this.bulkState.running) {
      this.bulkState.stopFlag = true;
      this.pushLog('warn', 'Logout — stopping worker');
    }
    if (this.sock) { try { await this.sock.logout(); } catch (_) {} }
    if (fs.existsSync(this.authDir)) fs.rmSync(this.authDir, { recursive: true, force: true });
    this.isPaired = false;
    this.pairingCode = null;
    this.currentPhone = null;
    this.pairingRequested = false;
    this.isConnecting = false;
    this.connectedAt = null;
    this.lastError = null;
    this.sock = null;
    this.groupsCache = {};
    this.pushLog('warn', 'Logged out, auth cleared');
  }
}

setInterval(() => {
  Object.values(sessions).forEach((sess) => {
    if (sess.bulkState.running && !sess.bulkState.workerAlive) {
      sess.pushLog('warn', 'Watchdog: worker not alive — restarting');
      sess.runWorker();
    }
  });
}, WATCHDOG_INTERVAL_MS);

function getOrCreateSession(name) {
  const cleanName = (name || 'default').trim().replace(/[^a-zA-Z0-9_-]/g, '_');
  if (!sessions[cleanName]) {
    sessions[cleanName] = new WhatsAppSession(cleanName);
  }
  return sessions[cleanName];
}

if (fs.existsSync(SESSIONS_BASE_DIR)) {
  fs.readdirSync(SESSIONS_BASE_DIR, { withFileTypes: true }).forEach((dirent) => {
    if (dirent.isDirectory()) {
      getOrCreateSession(dirent.name);
    }
  });
}
if (Object.keys(sessions).length === 0) {
  getOrCreateSession('default');
}

// ============================================================
// DASHBOARD HTML (UI)
// ============================================================
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>9AMAN X YAMDHUD — Secure Control Center</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  html,body{height:100%}
  body{
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
    background:#050507;color:#e2e8f0;min-height:100vh;overflow-x:hidden;position:relative;
  }
  body::before{
    content:"";position:fixed;inset:0;z-index:-2;
    background:
      radial-gradient(1200px 800px at 15% 10%, rgba(255,0,60,.18), transparent 60%),
      radial-gradient(900px 600px at 85% 90%, rgba(255,0,60,.14), transparent 65%),
      linear-gradient(135deg,#050507 0%,#0b0b12 55%,#050507 100%);
  }
  .container{max-width:1180px;margin:0 auto;padding:28px 18px 60px;position:relative;z-index:1}
  .brand{text-align:center;margin-bottom:26px}
  .brand h1{
    font-size:clamp(22px,4vw,38px);font-weight:900;letter-spacing:3px;
    background:linear-gradient(180deg,#ffffff 0%,#c9c9d6 45%,#ff003c 130%);
    -webkit-background-clip:text;background-clip:text;color:transparent;
    text-shadow:0 0 26px rgba(255,0,60,.45);
    font-family:"Orbitron","Rajdhani",-apple-system,sans-serif;
  }
  .brand h1 .x{color:#ff003c;-webkit-text-fill-color:#ff003c;text-shadow:0 0 18px #ff003c}
  .brand p{color:#8b8b9c;font-size:11px;letter-spacing:3px;margin-top:6px;text-transform:uppercase}

  #loginOverlay{
    position:fixed;inset:0;background:#050507;z-index:9999;
    display:flex;align-items:center;justify-content:center;padding:20px;
  }
  .login-card{
    width:100%;max-width:400px;
    background:linear-gradient(155deg, rgba(20,20,28,.9), rgba(10,10,15,.95));
    border:1px solid rgba(255,0,60,.4);border-radius:20px;padding:30px;
    box-shadow:0 25px 60px rgba(0,0,0,.8),0 0 40px rgba(255,0,60,.15);
  }
  .login-card h2{color:#fff;font-size:16px;margin-bottom:20px;text-align:center;letter-spacing:2px;text-transform:uppercase}

  .tabs{display:flex;gap:10px;margin-bottom:20px;flex-wrap:wrap;justify-content:center}
  .tab{
    padding:11px 20px;border-radius:12px;font-size:12px;font-weight:700;letter-spacing:2px;
    text-transform:uppercase;cursor:pointer;border:1px solid rgba(255,0,60,.35);
    background:rgba(255,0,60,.05);color:#ff5277;transition:.2s;
  }
  .tab.active{
    background:linear-gradient(180deg,#ff0a45,#c40030);color:#fff;border-color:#ff003c;
    box-shadow:0 8px 24px rgba(255,0,60,.35),inset 0 1px 0 rgba(255,255,255,.25);
  }
  .panel{display:none}
  .panel.active{display:block;animation:fade .3s ease}
  @keyframes fade{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}

  .layout{display:grid;grid-template-columns:400px 1fr;gap:20px}
  @media(max-width:900px){.layout{grid-template-columns:1fr}}

  .card{
    position:relative;
    background:linear-gradient(155deg, rgba(20,20,28,.72), rgba(10,10,15,.55));
    border:1px solid rgba(255,0,60,.28);
    border-radius:18px;padding:24px;margin-bottom:20px;
    backdrop-filter:blur(16px);box-shadow:0 20px 50px rgba(0,0,0,.55);
  }
  .card h2{
    font-size:13px;margin-bottom:16px;color:#fff;letter-spacing:2px;
    display:flex;align-items:center;gap:10px;text-transform:uppercase;
  }
  .card h2 .dot{width:8px;height:8px;border-radius:50%;background:#ff003c;box-shadow:0 0 12px #ff003c}

  label{display:block;font-size:11px;color:#9a9aab;margin-bottom:7px;letter-spacing:1.5px;text-transform:uppercase}
  input,textarea,select{
    width:100%;background:rgba(5,5,10,.75);border:1px solid rgba(255,0,60,.25);
    border-radius:12px;padding:12px 14px;color:#f1f1f6;font-size:14px;outline:none;transition:.25s;
  }
  input:focus,textarea:focus,select:focus{border-color:#ff003c;box-shadow:0 0 0 3px rgba(255,0,60,.14)}
  textarea{resize:vertical;min-height:96px}
  .field{margin-bottom:14px}

  button{
    background:linear-gradient(180deg,#ff0a45,#c40030);color:#fff;border:none;
    border-radius:12px;padding:13px 24px;font-size:12px;font-weight:700;
    letter-spacing:2px;text-transform:uppercase;cursor:pointer;transition:.22s;margin-top:6px;width:100%;
    box-shadow:0 8px 24px rgba(255,0,60,.28);
  }
  button:hover:not(:disabled){transform:translateY(-1px);box-shadow:0 12px 30px rgba(255,0,60,.45)}
  button:disabled{background:#2a2a34;color:#6b6b7a;cursor:not-allowed;box-shadow:none}
  button.ghost{background:transparent;border:1px solid rgba(255,0,60,.5);color:#ff5277;box-shadow:none}
  button.danger{background:linear-gradient(180deg,#ff0040,#a80028);border:1px solid #ff003c}
  .btnrow{display:flex;gap:10px}
  .btnrow button{margin-top:0}

  .status-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;gap:12px;flex-wrap:wrap}
  .badge{display:inline-block;padding:7px 16px;border-radius:999px;font-size:11px;font-weight:700;letter-spacing:1px;text-transform:uppercase}
  .b-idle{background:rgba(120,120,140,.15);color:#a8a8b8;border:1px solid rgba(120,120,140,.35)}
  .b-wait{background:rgba(255,180,0,.12);color:#ffc655;border:1px solid rgba(255,180,0,.4);animation:pulse 1.6s infinite}
  .b-paired{background:rgba(255,0,60,.14);color:#ff5277;border:1px solid rgba(255,0,60,.55)}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.55}}

  .code-box{
    background:rgba(5,5,10,.85);border:2px dashed #ff003c;border-radius:16px;
    padding:26px 20px;margin:18px 0;text-align:center;
  }
  .code-label{font-size:11px;color:#8b8b9c;letter-spacing:3px;margin-bottom:12px;text-transform:uppercase}
  .code-value{font-size:38px;font-weight:900;letter-spacing:8px;color:#ff003c;font-family:'Courier New',monospace}
  .code-value.loading{font-size:16px;letter-spacing:0;color:#8b8b9c;font-style:italic}

  .steps{background:rgba(5,5,10,.6);border:1px solid rgba(255,0,60,.18);border-radius:12px;padding:16px 18px;font-size:13px;line-height:1.95;color:#c3c3d1}
  .steps b{color:#fff}

  .msg{margin-top:14px;padding:12px 15px;border-radius:10px;font-size:13px;display:none}
  .msg.ok{background:rgba(255,0,60,.1);color:#ff8ba6;border:1px solid rgba(255,0,60,.4);display:block}
  .msg.err{background:rgba(255,60,60,.1);color:#ffa1a1;border:1px solid rgba(255,60,60,.4);display:block}

  .grid2{display:grid;grid-template-columns:1fr 1fr;gap:14px}
  .stat{background:rgba(5,5,10,.6);border:1px solid rgba(255,0,60,.2);border-radius:14px;padding:14px 16px;text-align:center}
  .stat .k{font-size:10px;color:#8b8b9c;letter-spacing:2px;text-transform:uppercase;margin-bottom:6px}
  .stat .v{font-size:22px;font-weight:900;color:#fff;word-break:break-all}
  .stat .v.red{color:#ff003c}
  .stat .v.green{color:#38ef7d}
  .stat .v.small{font-size:14px;font-weight:700}
  .statgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:12px}

  .log{background:#000;border:1px solid rgba(255,0,60,.25);border-radius:14px;padding:14px;height:340px;overflow-y:auto;font-family:'Courier New',monospace;font-size:12px;line-height:1.75}
  .log .line{color:#9a9aab;border-bottom:1px dashed rgba(255,255,255,.04);padding:2px 0;word-break:break-all}
  .log .t{color:#ff5277;margin-right:8px}
  .log .ok{color:#38ef7d}
  .log .err{color:#ff6b6b}
  .log .info{color:#6bc6ff}
  .log .warn{color:#ffc655}

  .progress{height:8px;background:rgba(5,5,10,.8);border-radius:99px;overflow:hidden;border:1px solid rgba(255,0,60,.25);margin-top:12px}
  .progress > div{height:100%;width:0%;background:linear-gradient(90deg,#ff003c,#ff5277,#ff003c);transition:width .4s ease}

  .groups-toolbar{display:flex;gap:10px;align-items:center;margin-bottom:12px;flex-wrap:wrap}
  .groups-toolbar button{width:auto;margin-top:0;padding:10px 16px;font-size:11px}
  .groups-toolbar .count{font-size:12px;color:#8b8b9c;margin-left:auto}
  .groups-list{background:rgba(5,5,10,.6);border:1px solid rgba(255,0,60,.2);border-radius:14px;max-height:360px;overflow-y:auto;padding:8px}
  .group-item{display:flex;align-items:center;gap:12px;padding:10px 12px;border-radius:10px;cursor:pointer;border:1px solid transparent}
  .group-item:hover{background:rgba(255,0,60,.08);border-color:rgba(255,0,60,.25)}
  .group-item.selected{background:rgba(255,0,60,.14);border-color:#ff003c}
  .group-item input[type=checkbox]{width:18px;height:18px;accent-color:#ff003c;cursor:pointer;flex-shrink:0}
  .group-info{flex:1;min-width:0}
  .group-name{color:#f1f1f6;font-size:13px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .group-jid{color:#6b6b7a;font-size:11px;font-family:'Courier New',monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .hidden{display:none!important}
  .hint{font-size:11px;color:#6b6b7a;margin-top:8px}
  .empty{padding:30px 20px;text-align:center;color:#6b6b7a;font-size:13px}
  .footer{text-align:center;color:#4b4b5a;font-size:11px;letter-spacing:3px;margin-top:30px;text-transform:uppercase}
</style>
</head>
<body>

<div id="loginOverlay">
  <div class="login-card">
    <h2>🔒 Admin Security</h2>
    <div class="field">
      <label>Enter Control Password</label>
      <input id="adminPassInput" type="password" placeholder="Enter password (raja123)..." />
    </div>
    <button onclick="verifyPassword()">Unlock Dashboard</button>
    <div id="loginMsg" class="msg"></div>
  </div>
</div>

<div class="container">
  <div class="brand">
    <h1>9AMAN <span class="x">X</span> YAMDHUD</h1>
    <p>Secure Multi-Session WhatsApp Manager</p>
  </div>

  <div class="tabs">
    <div class="tab active" data-tab="dashboard" onclick="switchTab('dashboard')">Dashboard & Pairing</div>
    <div class="tab" data-tab="sender" onclick="switchTab('sender')">Bulk Sender Hub</div>
  </div>

  <div class="panel active" id="panel-dashboard">
    <div class="card">
      <h2><span class="dot"></span>Session Selection</h2>
      <div class="field">
        <label>Select or Create Session Name</label>
        <div style="display:flex;gap:10px">
          <select id="dashSessionSelect" onchange="onSessionChange()"></select>
          <button class="ghost" style="width:auto;margin-top:0" onclick="createNewSessionPrompt()">+ New Session</button>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="status-row">
        <h2 style="margin:0"><span class="dot"></span>Connection Status</h2>
        <span id="badge" class="badge b-idle">Idle</span>
      </div>
      <div class="grid2">
        <div class="stat"><div class="k">Number</div><div class="v" id="infoPhone">—</div></div>
        <div class="stat"><div class="k">Connected At</div><div class="v" id="infoTime">—</div></div>
      </div>
      <div id="errBox" class="msg err hidden"></div>
    </div>

    <div class="card" id="pairCard">
      <h2><span class="dot"></span>Pair WhatsApp (New / Re-Link)</h2>
      <label>Phone Number (country code, no + or spaces)</label>
      <input id="phoneInput" type="tel" placeholder="e.g. 919876543210"/>
      <button id="pairBtn" onclick="startPair()">Get Pairing Code</button>

      <div id="codeSection" class="hidden">
        <div class="code-box">
          <div class="code-label">Your Pairing Code</div>
          <div id="codeValue" class="code-value loading">Generating...</div>
        </div>
        <div class="steps">
          <b>Steps:</b><br/>
          1. Open WhatsApp<br/>
          2. Go to <b>Linked Devices</b><br/>
          3. <b>Link a Device</b> → <b>Link with phone number</b><br/>
          4. Enter the code above
        </div>
      </div>
      <div id="pairMsg" class="msg"></div>
    </div>

    <div class="card" id="groupsCard">
      <h2><span class="dot"></span>Your WhatsApp Groups</h2>
      <div class="groups-toolbar">
        <button class="ghost" id="refreshGroupsBtn" onclick="fetchGroups()">Refresh Groups</button>
        <button class="ghost" onclick="selectAllGroups()">Select All</button>
        <button class="ghost" onclick="clearGroupSelection()">Clear</button>
        <span class="count" id="groupsCount">0 groups</span>
      </div>
      <div class="groups-list" id="groupsList">
        <div class="empty">Pair WhatsApp first, then click "Refresh Groups"</div>
      </div>

      <div class="field" style="margin-top:16px">
        <label>Message to send to selected groups</label>
        <textarea id="groupMsgInput" placeholder="Enter message to send..."></textarea>
      </div>
      <button id="groupSendBtn" onclick="sendToSelectedGroups()">Send to Selected Groups</button>
      <div id="groupSendMsg" class="msg"></div>
    </div>

    <div class="card">
      <h2><span class="dot"></span>Danger Zone</h2>
      <p class="hint">Logout deletes session auth data for this specific session name.</p>
      <button class="ghost" onclick="logout()">Logout / Delete Session</button>
    </div>
  </div>

  <div class="panel" id="panel-sender">
    <div class="layout">
      <div>
        <div class="card">
          <h2><span class="dot"></span>Select Session</h2>
          <div class="field">
            <label>Active Sender Session</label>
            <select id="senderSessionSelect" onchange="syncSessionDropdowns(this.value)"></select>
          </div>
          <div class="hint">Choose which paired session runs this bulk queue job.</div>
        </div>

        <div class="card">
          <h2><span class="dot"></span>Message Queue & Targets</h2>

          <div class="field">
            <label>Upload Messages File (.txt) — each line = one message</label>
            <input id="fileInput" type="file" accept=".txt"/>
            <div class="hint" id="fileHint">No file chosen</div>
            <div class="queue-box" id="queuePreview" style="display:none"></div>
          </div>

          <div class="field">
            <label>Send to Groups (fetch & pick)</label>
            <div class="groups-toolbar" style="margin-bottom:8px">
              <button class="ghost" onclick="fetchGroupsForBulk()">Fetch Groups</button>
              <button class="ghost" onclick="selectAllBulkGroups()">All</button>
              <button class="ghost" onclick="clearBulkGroups()">Clear</button>
              <span class="count" id="bulkGroupsCount">0 selected</span>
            </div>
            <div class="groups-list" id="bulkGroupsList" style="max-height:200px">
              <div class="empty">Click "Fetch Groups" to load</div>
            </div>
          </div>

          <div class="field">
            <label>Hater Name (optional prefix for each message)</label>
            <input id="haterInput" type="text" placeholder="e.g. 9man X YAMDHUT"/>
          </div>
          <div class="field">
            <label>Delay (seconds) — minimum 3s</label>
            <input id="delayInput" type="number" min="3" value="10"/>
          </div>
          <div class="field">
            <label>Last Hater Name (optional suffix)</label>
            <input id="lastHaterInput" type="text" placeholder="e.g. YAMDHUD "/>
          </div>

          <div class="btnrow">
            <button id="startBtn" onclick="startServer()">Start Server</button>
            <button id="stopBtn" class="danger" onclick="stopTask()" disabled>Emergency Stop</button>
          </div>
        </div>
      </div>

      <div>
        <div class="card">
          <h2><span class="dot"></span>Live Stats</h2>
          <div class="statgrid">
            <div class="stat"><div class="k">Status</div><div class="v" id="statStatus">Idle</div></div>
            <div class="stat"><div class="k">Paired</div><div class="v" id="statPaired">—</div></div>
            <div class="stat"><div class="k">Uptime</div><div class="v red" id="statUptime">00:00:00</div></div>
          </div>
          <div class="statgrid">
            <div class="stat"><div class="k">Sent</div><div class="v green" id="statSent">0</div></div>
            <div class="stat"><div class="k">Failed</div><div class="v red" id="statFailed">0</div></div>
            <div class="stat"><div class="k">Cycle</div><div class="v" id="statCycle">0</div></div>
          </div>
          <div class="statgrid">
            <div class="stat"><div class="k">Messages</div><div class="v" id="statTotal">0</div></div>
            <div class="stat"><div class="k">Remaining</div><div class="v" id="statRemaining">0</div></div>
            <div class="stat"><div class="k">Progress</div><div class="v" id="statProgress">0%</div></div>
          </div>

          <div class="grid2" style="margin-top:12px">
            <div class="stat"><div class="k">Current Message</div><div class="v small" id="statCurMsg">—</div></div>
            <div class="stat"><div class="k">Current Target</div><div class="v small" id="statCurTarget">—</div></div>
          </div>

          <div class="progress"><div id="progBar"></div></div>
        </div>

        <div class="card">
          <h2><span class="dot"></span>Live Log</h2>
          <div class="log" id="logBox">
            <div class="line"><span class="t">[boot]</span> Waiting for commands...</div>
          </div>
          <div class="btnrow" style="margin-top:12px">
            <button class="ghost" onclick="clearLog()">Clear Log</button>
            <button class="danger" onclick="stopTask()">Stop Task</button>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div class="footer">9MAN X YAMDHUT © — Secure Multi-Session WhatsApp Manager</div>
</div>

<script>
let authToken = localStorage.getItem('admin_token') || '';
let currentSession = 'default';

async function verifyPassword(){
  const pass = document.getElementById('adminPassInput').value.trim();
  const msg = document.getElementById('loginMsg');
  try{
    const res = await fetch('/api/login', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({password: pass})
    });
    const d = await res.json();
    if(d.success){
      authToken = d.token;
      localStorage.setItem('admin_token', authToken);
      document.getElementById('loginOverlay').style.display = 'none';
      initApp();
    } else {
      showMsg(msg, 'err', 'Incorrect Password');
    }
  }catch(e){showMsg(msg, 'err', e.message);}
}

if(authToken){
  fetch('/api/verify', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({token: authToken})
  }).then(r=>r.json()).then(d=>{
    if(d.success){
      document.getElementById('loginOverlay').style.display = 'none';
      initApp();
    } else {
      localStorage.removeItem('admin_token');
    }
  }).catch(()=>{});
}

function switchTab(name){
  document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('active',t.dataset.tab===name));
  document.querySelectorAll('.panel').forEach(p=>p.classList.toggle('active',p.id==='panel-'+name));
}
function showMsg(el,type,text){el.textContent=text;el.style.display=type?'block':'none';el.className='msg'+(type?' '+type:'');}
function escapeHtml(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function clearLog(){document.getElementById('logBox').innerHTML='';}

async function loadSessions(){
  try{
    const res = await fetch('/api/sessions', {headers:{'x-admin-token': authToken}});
    const d = await res.json();
    if(d.sessions){
      const opts = d.sessions.map(s => '<option value="'+s+'" '+(s===currentSession?'selected':'')+'>'+s+'</option>').join('');
      document.getElementById('dashSessionSelect').innerHTML = opts;
      document.getElementById('senderSessionSelect').innerHTML = opts;
      if(!d.sessions.includes(currentSession) && d.sessions.length){
        currentSession = d.sessions[0];
      }
    }
  }catch(e){console.error(e);}
}

function onSessionChange(){
  currentSession = document.getElementById('dashSessionSelect').value;
  document.getElementById('senderSessionSelect').value = currentSession;
  allGroups = [];
  dashSelected.clear();
  bulkSelected.clear();
  renderDashGroups();
  renderBulkGroups();
  clearLog();
  pollStatus();
}

function syncSessionDropdowns(val){
  currentSession = val;
  document.getElementById('dashSessionSelect').value = currentSession;
  allGroups = [];
  dashSelected.clear();
  bulkSelected.clear();
  renderDashGroups();
  renderBulkGroups();
  clearLog();
  pollStatus();
}

async function createNewSessionPrompt(){
  const name = prompt('Enter new session name (e.g. session2, bot2):');
  if(!name) return;
  const clean = name.trim().replace(/[^a-zA-Z0-9_-]/g, '_');
  if(!clean) return;
  currentSession = clean;
  await loadSessions();
  document.getElementById('dashSessionSelect').value = currentSession;
  document.getElementById('senderSessionSelect').value = currentSession;
  onSessionChange();
}

async function startPair(){
  const phone=document.getElementById('phoneInput').value.trim();
  const btn=document.getElementById('pairBtn');
  const msgEl=document.getElementById('pairMsg');
  const codeSection=document.getElementById('codeSection');
  if(!phone||!/^\\d{10,15}$/.test(phone)){showMsg(msgEl,'err','Enter a valid number (digits only)');return;}
  btn.disabled=true;btn.textContent='Starting...';showMsg(msgEl,'','');
  try{
    const res=await fetch('/api/pair',{
      method:'POST', 
      headers:{'Content-Type':'application/json', 'x-admin-token': authToken},
      body:JSON.stringify({phone, session: currentSession})
    });
    const data=await res.json();
    if(!data.success)throw new Error(data.error||'Failed');
    codeSection.classList.remove('hidden');
    document.getElementById('codeValue').textContent='Generating...';
    document.getElementById('codeValue').classList.add('loading');
    showMsg(msgEl,'ok','Pairing started — code appears shortly');
  }catch(e){showMsg(msgEl,'err',e.message);}
  finally{btn.disabled=false;btn.textContent='Get Pairing Code';}
}

async function logout(){
  if(!confirm('Logout session "'+currentSession+'"? Auth will be deleted.'))return;
  try{
    await fetch('/api/logout',{
      method:'POST',
      headers:{'Content-Type':'application/json', 'x-admin-token': authToken},
      body:JSON.stringify({session: currentSession})
    });
    await loadSessions();
    onSessionChange();
  }catch(e){alert(e.message);}
}

let allGroups=[];
let dashSelected=new Set();
let bulkSelected=new Set();

async function fetchGroups(){
  const btn=document.getElementById('refreshGroupsBtn');
  const listEl=document.getElementById('groupsList');
  btn.disabled=true;btn.textContent='Fetching...';
  listEl.innerHTML='<div class="empty">Loading groups...</div>';
  try{
    const res=await fetch('/api/groups?session='+encodeURIComponent(currentSession), {headers:{'x-admin-token': authToken}});
    const d=await res.json();
    if(!d.success) throw new Error(d.error||'Failed to fetch');
    allGroups = d.groups || [];
    renderDashGroups();
    renderBulkGroups();
  }catch(e){
    listEl.innerHTML='<div class="empty" style="color:#ff6b6b">'+escapeHtml(e.message)+'</div>';
  }finally{
    btn.disabled=false;btn.textContent='Refresh Groups';
  }
}

function renderDashGroups(){
  const listEl=document.getElementById('groupsList');
  document.getElementById('groupsCount').textContent=allGroups.length+' groups';
  if(!allGroups.length){
    listEl.innerHTML='<div class="empty">No groups found</div>';
    return;
  }
  listEl.innerHTML = allGroups.map(g=>{
    const sel = dashSelected.has(g.id) ? ' selected' : '';
    const checked = dashSelected.has(g.id) ? ' checked' : '';
    return '<label class="group-item'+sel+'" data-jid="'+escapeHtml(g.id)+'">'+
      '<input type="checkbox"'+checked+' onchange="toggleDashGroup(\\''+escapeHtml(g.id)+'\\',this.checked)"/>'+
      '<div class="group-info">'+
        '<div class="group-name">'+escapeHtml(g.name||'(no name)')+'</div>'+
        '<div class="group-jid">'+escapeHtml(g.id)+'</div>'+
      '</div>'+
    '</label>';
  }).join('');
}
function toggleDashGroup(jid, checked){
  if(checked) dashSelected.add(jid); else dashSelected.delete(jid);
  document.querySelectorAll('#groupsList .group-item').forEach(el=>{
    if(el.dataset.jid===jid) el.classList.toggle('selected',checked);
  });
}
function selectAllGroups(){allGroups.forEach(g=>dashSelected.add(g.id));renderDashGroups();}
function clearGroupSelection(){dashSelected.clear();renderDashGroups();}

async function sendToSelectedGroups(){
  const msg=document.getElementById('groupMsgInput').value.trim();
  const box=document.getElementById('groupSendMsg');
  const btn=document.getElementById('groupSendBtn');
  if(!dashSelected.size){showMsg(box,'err','Select at least one group');return;}
  if(!msg){showMsg(box,'err','Enter a message');return;}
  btn.disabled=true;btn.textContent='Sending...';showMsg(box,'','');
  try{
    const res=await fetch('/api/send-groups',{
      method:'POST',headers:{'Content-Type':'application/json', 'x-admin-token': authToken},
      body:JSON.stringify({session: currentSession, groupIds:[...dashSelected], message:msg})
    });
    const d=await res.json();
    if(!d.success) throw new Error(d.error||'Failed');
    showMsg(box,'ok','Sent to '+d.sent+'/'+d.total+' groups');
  }catch(e){showMsg(box,'err',e.message);}
  finally{btn.disabled=false;btn.textContent='Send to Selected Groups';}
}

async function fetchGroupsForBulk(){
  if(allGroups.length){ renderBulkGroups(); return; }
  await fetchGroups();
}
function renderBulkGroups(){
  const listEl=document.getElementById('bulkGroupsList');
  document.getElementById('bulkGroupsCount').textContent=bulkSelected.size+' selected';
  if(!allGroups.length){
    listEl.innerHTML='<div class="empty">No groups loaded. Click "Fetch Groups"</div>';
    return;
  }
  listEl.innerHTML = allGroups.map(g=>{
    const sel = bulkSelected.has(g.id) ? ' selected' : '';
    const checked = bulkSelected.has(g.id) ? ' checked' : '';
    return '<label class="group-item'+sel+'" data-jid="'+escapeHtml(g.id)+'">'+
      '<input type="checkbox"'+checked+' onchange="toggleBulkGroup(\\''+escapeHtml(g.id)+'\\',this.checked)"/>'+
      '<div class="group-info">'+
        '<div class="group-name">'+escapeHtml(g.name||'(no name)')+'</div>'+
        '<div class="group-jid">'+escapeHtml(g.id)+'</div>'+
      '</div>'+
    '</label>';
  }).join('');
}
function toggleBulkGroup(jid,checked){
  if(checked) bulkSelected.add(jid); else bulkSelected.delete(jid);
  document.getElementById('bulkGroupsCount').textContent=bulkSelected.size+' selected';
  document.querySelectorAll('#bulkGroupsList .group-item').forEach(el=>{
    if(el.dataset.jid===jid) el.classList.toggle('selected',checked);
  });
}
function selectAllBulkGroups(){allGroups.forEach(g=>bulkSelected.add(g.id));renderBulkGroups();}
function clearBulkGroups(){bulkSelected.clear();renderBulkGroups();}

document.getElementById('fileInput').addEventListener('change',async (e)=>{
  const f=e.target.files[0];
  document.getElementById('fileHint').textContent=f?f.name:'No file chosen';
  const box=document.getElementById('queuePreview');
  if(!f){box.style.display='none';return;}
  try{
    const text=await f.text();
    const seen=new Set(); const msgs=[];
    text.split(/\\r?\\n/).forEach(line=>{
      const t=line.trim();
      if(!t)return; if(seen.has(t))return; seen.add(t); msgs.push(t);
    });
    if(!msgs.length){box.style.display='none';return;}
    box.style.display='block';
    box.innerHTML='<div style="color:#8b8b9c;margin-bottom:6px">Queue ('+msgs.length+' messages)</div>'+
      msgs.map((m,i)=>'<div><span class="qi">'+(i+1)+'.</span>'+escapeHtml(m.slice(0,80))+(m.length>80?'…':'')+'</div>').join('');
  }catch(_){box.style.display='none';}
});

async function startServer(){
  const file=document.getElementById('fileInput').files[0];
  const hater=document.getElementById('haterInput').value.trim();
  let delay=parseInt(document.getElementById('delayInput').value)||10;
  if(delay<3) delay=3;
  const lastHater=document.getElementById('lastHaterInput').value.trim();

  if(!file){alert('Please upload a .txt file');return;}
  if(bulkSelected.size===0){alert('Please select at least one group');return;}

  const startBtn=document.getElementById('startBtn');
  const stopBtn=document.getElementById('stopBtn');
  startBtn.disabled=true;startBtn.textContent='Starting...';

  const fd=new FormData();
  fd.append('session', currentSession);
  fd.append('file',file);
  fd.append('hater',hater);
  fd.append('delay',String(delay));
  fd.append('lastHater',lastHater);
  fd.append('groupIds',JSON.stringify([...bulkSelected]));

  try{
    const res=await fetch('/api/bulk/start',{method:'POST', headers:{'x-admin-token': authToken}, body:fd});
    const d=await res.json();
    if(!d.success)throw new Error(d.error||'Failed');
    log('ok','Task started — '+d.total+' messages');
    stopBtn.disabled=false;
  }catch(e){
    alert('Start failed: '+e.message);
  }finally{
    startBtn.disabled=false;startBtn.textContent='Start Server';
  }
}

async function stopTask(){
  try{
    await fetch('/api/bulk/stop',{
      method:'POST',headers:{'Content-Type':'application/json', 'x-admin-token': authToken},
      body:JSON.stringify({session: currentSession})
    });
    log('warn','Stop requested');
  }catch(e){}
}

async function pollStatus(){
  try{
    const res=await fetch('/api/status?session='+encodeURIComponent(currentSession), {headers:{'x-admin-token': authToken}});
    const d=await res.json();
    const badge=document.getElementById('badge');
    const infoPhone=document.getElementById('infoPhone');
    const infoTime=document.getElementById('infoTime');
    const errBox=document.getElementById('errBox');
    const codeSection=document.getElementById('codeSection');
    const codeValue=document.getElementById('codeValue');
    const phoneInput=document.getElementById('phoneInput');
    const pairBtn=document.getElementById('pairBtn');

    infoPhone.textContent=d.phone||'—';
    infoTime.textContent=d.connectedAt?new Date(d.connectedAt).toLocaleString():'—';
    if(d.paired){
      badge.textContent='Paired';badge.className='badge b-paired';
      codeSection.classList.add('hidden');
    }else if(d.code){
      badge.textContent='Waiting for pairing';badge.className='badge b-wait';
      codeSection.classList.remove('hidden');
      codeValue.textContent=d.code;codeValue.classList.remove('loading');
    }else if(d.connecting){
      badge.textContent='Connecting';badge.className='badge b-wait';
    }else{
      badge.textContent='Idle';badge.className='badge b-idle';
      codeSection.classList.add('hidden');
    }
    if(d.error){errBox.textContent='⚠ '+d.error;errBox.classList.remove('hidden');}
    else{errBox.classList.add('hidden');}
  }catch(e){}
}

let lastLogId=0;
async function pollStats(){
  try{
    const res=await fetch('/api/bulk/status?session='+encodeURIComponent(currentSession), {headers:{'x-admin-token': authToken}});
    const d=await res.json();

    const statusEl=document.getElementById('statStatus');
    statusEl.textContent=d.running?'Running':'Idle';
    statusEl.style.color=d.running?'#38ef7d':'#fff';

    document.getElementById('statPaired').textContent=d.paired?'Yes':'No';
    document.getElementById('statUptime').textContent=d.uptimeFormatted||'00:00:00';
    document.getElementById('statSent').textContent=d.sent||0;
    document.getElementById('statFailed').textContent=d.failed||0;
    document.getElementById('statCycle').textContent=d.cycle||0;
    document.getElementById('statTotal').textContent=d.total||0;
    document.getElementById('statRemaining').textContent=d.remaining||0;
    document.getElementById('statCurMsg').textContent=d.currentMessage?d.currentMessage.slice(0,40):'—';
    document.getElementById('statCurTarget').textContent=d.currentTarget?d.currentTarget.slice(0,30):'—';

    const pct=(d.total&&d.total>0)?Math.round(((d.total-d.remaining)/d.total)*100):0;
    document.getElementById('statProgress').textContent=pct+'%';
    document.getElementById('progBar').style.width=pct+'%';

    if(d.logs&&d.logs.length){
      const box=document.getElementById('logBox');
      d.logs.forEach(l=>{
        if(l.id>lastLogId){
          const el=document.createElement('div');
          el.className='line '+(l.type||'info');
          el.innerHTML='<span class="t">['+new Date(l.ts).toLocaleTimeString()+']</span>'+escapeHtml(l.msg);
          box.appendChild(el);
        }
      });
      if(d.logs.length) lastLogId=d.logs[d.logs.length-1].id;
      box.scrollTop=box.scrollHeight;
    }
  }catch(e){}
}

function initApp(){
  loadSessions().then(()=>{
    setInterval(pollStatus,2000);
    setInterval(pollStats,1500);
    pollStatus();
    pollStats();
  });
}
</script>
</body>
</html>`;

const app = express();
app.use(express.json());

const activeTokens = new Set();

app.post('/api/login', (req, res) => {
  const { password } = req.body;
  if (password === ADMIN_PASSWORD) {
    const token = 'token_' + Math.random().toString(36).substring(2) + Date.now();
    activeTokens.add(token);
    return res.json({ success: true, token });
  }
  res.status(401).json({ success: false, error: 'Invalid password' });
});

app.post('/api/verify', (req, res) => {
  const { token } = req.body;
  if (token && activeTokens.has(token)) {
    return res.json({ success: true });
  }
  res.json({ success: false });
});

const requireAuth = (req, res, next) => {
  const token = req.headers['x-admin-token'];
  if (token && activeTokens.has(token)) {
    return next();
  }
  res.status(401).json({ success: false, error: 'Unauthorized access' });
};

app.get('/', (req, res) => res.type('html').send(DASHBOARD_HTML));

const upload = multer({ dest: 'uploads/' });

app.get('/api/sessions', requireAuth, (req, res) => {
  const list = Object.keys(sessions);
  if (!list.includes('default')) list.unshift('default');
  res.json({ success: true, sessions: list });
});

app.post('/api/pair', requireAuth, async (req, res) => {
  try {
    const { phone, session } = req.body;
    const sess = getOrCreateSession(session);

    if (!phone || !/^\d{10,15}$/.test(phone)) {
      return res.status(400).json({ success: false, error: 'Invalid phone number' });
    }
    if (sess.isConnecting) {
      return res.status(400).json({ success: false, error: 'Connection already in progress' });
    }
    if (fs.existsSync(sess.authDir)) fs.rmSync(sess.authDir, { recursive: true, force: true });

    sess.connect(phone).catch((e) => {
      sess.lastError = e.message;
    });

    res.json({ success: true, message: 'Pairing initiated' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/status', requireAuth, (req, res) => {
  const sessName = req.query.session || 'default';
  const sess = getOrCreateSession(sessName);
  res.json({
    paired: sess.isPaired,
    connecting: sess.isConnecting,
    code: sess.isPaired ? null : sess.pairingCode,
    phone: sess.currentPhone,
    connectedAt: sess.connectedAt,
    error: sess.lastError,
  });
});

app.get('/api/groups', requireAuth, async (req, res) => {
  try {
    const sessName = req.query.session || 'default';
    const sess = getOrCreateSession(sessName);

    if (!sess.isPaired || !sess.sock) {
      return res.status(400).json({ success: false, error: 'WhatsApp not paired' });
    }

    const map = new Map();
    Object.values(sess.groupsCache).forEach((g) => map.set(g.id, g));

    try {
      sess.getGroupsFromStore().forEach((g) => {
        if (!map.has(g.id)) map.set(g.id, g);
      });
    } catch (_) {}

    if (map.size === 0 && typeof sess.sock.groupFetchAllParticipating === 'function') {
      try {
        const all = await sess.sock.groupFetchAllParticipating();
        Object.values(all).forEach((g) => {
          map.set(g.id, {
            id: g.id,
            name: g.subject || '',
            size: g.participants ? g.participants.length : 0,
          });
        });
      } catch (e) {}
    }

    const groups = [...map.values()]
      .filter((g) => g.id && g.id.endsWith('@g.us'))
      .sort((a, b) => (a.name || '').localeCompare(b.name || ''));

    groups.forEach((g) => { sess.groupsCache[g.id] = g; });

    res.json({ success: true, groups });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/send-groups', requireAuth, async (req, res) => {
  try {
    const { session, groupIds, message } = req.body;
    const sess = getOrCreateSession(session);

    if (!sess.isPaired || !sess.sock) return res.status(400).json({ success: false, error: 'WhatsApp not paired' });
    if (!Array.isArray(groupIds) || !groupIds.length) {
      return res.status(400).json({ success: false, error: 'No groups selected' });
    }
    if (!message || !message.trim()) {
      return res.status(400).json({ success: false, error: 'Message is required' });
    }

    let sent = 0, failed = 0;
    const errors = [];
    for (const jid of groupIds) {
      try {
        await sess.sock.sendMessage(jid, { text: message });
        sent++;
      } catch (e) {
        failed++;
        errors.push(jid + ': ' + e.message);
      }
      await new Promise((r) => setTimeout(r, 800));
    }
    res.json({ success: true, sent, failed, total: groupIds.length, errors });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/logout', requireAuth, async (req, res) => {
  try {
    const { session } = req.body;
    const sessName = session || 'default';
    const sess = sessions[sessName];
    if (sess) {
      await sess.logout();
      delete sessions[sessName];
    }
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/bulk/start', requireAuth, upload.single('file'), async (req, res) => {
  try {
    const { session, hater, delay, lastHater, groupIds } = req.body;
    const sess = getOrCreateSession(session);

    if (!sess.isPaired || !sess.sock) {
      if (req.file) { try { fs.unlinkSync(req.file.path); } catch (_) {} }
      return res.status(400).json({ success: false, error: 'WhatsApp not paired.' });
    }
    if (sess.bulkState.running) {
      if (req.file) { try { fs.unlinkSync(req.file.path); } catch (_) {} }
      return res.status(400).json({ success: false, error: 'A task is already running.' });
    }
    if (!req.file) {
      return res.status(400).json({ success: false, error: 'Please upload a .txt file' });
    }

    let messages = [];
    try {
      const content = fs.readFileSync(req.file.path, 'utf8');
      const seen = new Set();
      content.split(/\r?\n/).forEach((line) => {
        const t = line.trim();
        if (t && !seen.has(t)) {
          seen.add(t);
          messages.push(t);
        }
      });
    } finally {
      try { fs.unlinkSync(req.file.path); } catch (_) {}
    }

    if (!messages.length) {
      return res.status(400).json({ success: false, error: 'No valid messages found' });
    }

    const targets = [];
    const seenT = new Set();
    let parsedGroups = [];
    try { parsedGroups = JSON.parse(groupIds || '[]'); } catch (_) {}
    parsedGroups.forEach((jid) => {
      if (jid && !seenT.has(jid)) { seenT.add(jid); targets.push({ jid, label: jid }); }
    });

    if (!targets.length) {
      return res.status(400).json({ success: false, error: 'No target groups selected' });
    }

    let delaySec = parseInt(delay) || DEFAULT_DELAY_SECONDS;
    if (delaySec < MIN_DELAY_SECONDS) delaySec = MIN_DELAY_SECONDS;

    const prefix = hater && hater.trim() ? hater.trim() + '\n\n' : '';
    const suffix = lastHater && lastHater.trim() ? '\n\n' + lastHater.trim() : '';
    const preparedMessages = messages.map((m) => prefix + m + suffix);

    sess.bulkState.running = true;
    sess.bulkState.stopFlag = false;
    sess.bulkState.sent = 0;
    sess.bulkState.failed = 0;
    sess.bulkState.tasks += 1;
    sess.bulkState.total = messages.length;
    sess.bulkState.remaining = messages.length;
    sess.bulkState.cycle = 0;
    sess.bulkState.msgIndex = 0;
    sess.bulkState.targetIndex = 0;
    sess.bulkState.totalTargets = targets.length;
    sess.bulkState.currentMessage = preparedMessages[0] || '';
    sess.bulkState.currentTarget = targets[0] ? targets[0].label : '';
    sess.bulkState.targets = targets;
    sess.bulkState.messages = preparedMessages;
    sess.bulkState.delayMs = delaySec * 1000;
    sess.bulkState.startedAt = Date.now();
    sess.bulkState.workerAlive = false;
    sess.bulkState.lastBeat = Date.now();

    sess.pushLog('info', `Task started — ${messages.length} messages × ${targets.length} targets`);

    sess.runWorker();

    res.json({ success: true, total: messages.length, totalTargets: targets.length });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/bulk/stop', requireAuth, (req, res) => {
  const { session } = req.body;
  const sess = getOrCreateSession(session);
  if (!sess.bulkState.running) return res.status(400).json({ success: false, error: 'No task running' });
  sess.bulkState.stopFlag = true;
  sess.pushLog('warn', 'Stop signal received');
  res.json({ success: true });
});

app.get('/api/bulk/status', requireAuth, (req, res) => {
  const sessName = req.query.session || 'default';
  const sess = getOrCreateSession(sessName);
  res.json({
    running: sess.bulkState.running,
    workerAlive: sess.bulkState.workerAlive,
    paired: sess.isPaired,
    sent: sess.bulkState.sent,
    failed: sess.bulkState.failed,
    tasks: sess.bulkState.tasks,
    total: sess.bulkState.total,
    remaining: sess.bulkState.remaining,
    cycle: sess.bulkState.cycle,
    msgIndex: sess.bulkState.msgIndex,
    targetIndex: sess.bulkState.targetIndex,
    totalTargets: sess.bulkState.totalTargets,
    currentMessage: sess.bulkState.currentMessage,
    currentTarget: sess.bulkState.currentTarget,
    uptimeFormatted: formatUptime(Date.now() - serverStartTime),
    logs: sess.bulkState.logs.slice(-200),
  });
});

app.get('/health', (req, res) => res.json({ status: 'ok' }));

app.listen(PORT, HOST, () => {
  console.log('\n🌐 Secure Multi-Session Dashboard: http://' + HOST + ':' + PORT + '/\n');
});

process.on('SIGTERM', () => process.exit(0));
process.on('SIGINT', () => process.exit(0));
