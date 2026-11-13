const makeWASocket = require('@whiskeysockets/baileys').default;
const { useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion } = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');
const express = require('express');
const pino = require('pino');
const fs = require('fs');
const path = require('path');
const multer = require('multer');

const uploadsDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir, { recursive: true });

const PORT = process.env.PORT || 25029;
const HOST = '0.0.0.0';
const AUTH_BASE = 'auth_info_baileys';
const OWNER_NAME = 'RK RAJA XWD';

const MIN_DELAY_SECONDS = 3;
const DEFAULT_DELAY_SECONDS = 10;
const SEND_RETRY = 1;
const RETRY_WAIT_MS = 2000;
const WATCHDOG_INTERVAL_MS = 30000;

const sessions = new Map();
const bulkStates = new Map();

function getAuthDir(clientId, phone) {
  const safe = clientId.replace(/[^a-zA-Z0-9]/g, '').slice(0, 10);
  return `${AUTH_BASE}_${safe}_${phone.slice(-4)}`;
}

function newSessionState(id, clientId, phone) {
  return {
    id, clientId, sock: null, pairingCode: null, isPaired: false, phone: null,
    isConnecting: false, lastError: null, connectedAt: null, groupsCache: {}, 
    authDir: getAuthDir(clientId, phone),
  };
}

function ensureSession(id, clientId, phone) {
  if (!sessions.has(id)) sessions.set(id, newSessionState(id, clientId, phone));
  return sessions.get(id);
}

function getBulkState(clientId) {
  if (!bulkStates.has(clientId)) {
    bulkStates.set(clientId, {
      running: false, stopFlag: false, sessionId: null, sent: 0, failed: 0,
      tasks: 0, total: 0, remaining: 0, cycle: 0, msgIndex: 0, targetIndex: 0,
      totalTargets: 0, currentMessage: '', currentTarget: '', targets: [],
      messages: [], delayMs: DEFAULT_DELAY_SECONDS * 1000, logs: [], logId: 0,
      startedAt: null, workerAlive: false, lastBeat: 0,
    });
  }
  return bulkStates.get(clientId);
}

let serverStartTime = Date.now();

function pushLog(bulkState, type, msg) {
  bulkState.logs.push({ id: ++bulkState.logId, ts: Date.now(), type, msg });
  if (bulkState.logs.length > 500) bulkState.logs.splice(0, bulkState.logs.length - 500);
  const icons = { ok: '✅', err: '❌', warn: '⚠️', info: 'ℹ️' };
  console.log(`${icons[type] || '•'} ${msg}`);
}

function formatUptime(ms) {
  const s = Math.floor(ms / 1000);
  return `${String(Math.floor(s / 3600)).padStart(2, '0')}:${String(Math.floor((s % 3600) / 60)).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`;
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function getGroupsFromStore(s) {
  const out = [];
  try {
    if (s?.sock?.store?.groupMetadata) {
      s.sock.store.groupMetadata.forEach((v) => {
        out.push({ id: v.id, name: v.subject || '', size: v.participants?.length || 0 });
      });
    }
  } catch (_) {}
  return out;
}

function isSocketReady(clientId) {
  const bState = getBulkState(clientId);
  const s = sessions.get(bState.sessionId);
  return !!(s && s.sock && s.isPaired && s.clientId === clientId);
}

function parseMessagesFile(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  const seen = new Set();
  const out = [];
  content.split(/\r?\n/).forEach((line) => {
    const t = line.trim();
    if (!t || seen.has(t)) return;
    seen.add(t);
    out.push(t);
  });
  return out;
}

async function safeSend(clientId, jid, text) {
  const bState = getBulkState(clientId);
  const s = sessions.get(bState.sessionId);
  if (!s || !s.sock) throw new Error('session not ready');
  let lastErr = null;
  for (let attempt = 0; attempt <= SEND_RETRY; attempt++) {
    try {
      if (!isSocketReady(clientId)) throw new Error('socket not ready');
      await s.sock.sendMessage(jid, { text });
      return { ok: true };
    } catch (e) {
      lastErr = e;
      if (attempt < SEND_RETRY) {
        pushLog(bState, 'warn', `Retry ${attempt + 1}/${SEND_RETRY} → ${jid}: ${e.message}`);
        await sleep(RETRY_WAIT_MS);
      }
    }
  }
  return { ok: false, error: lastErr ? lastErr.message : 'unknown' };
}

async function runWorker(clientId) {
  const bState = getBulkState(clientId);
  if (bState.workerAlive) return;
  bState.workerAlive = true;
  pushLog(bState, 'info', `Worker started [${bState.sessionId}]`);

  try {
    while (!bState.stopFlag) {
      if (!isSocketReady(clientId)) { bState.lastBeat = Date.now(); await sleep(3000); continue; }
      for (let mi = 0; mi < bState.messages.length && !bState.stopFlag; mi++) {
        const msg = bState.messages[mi];
        bState.msgIndex = mi;
        bState.currentMessage = msg;
        for (let ti = 0; ti < bState.targets.length && !bState.stopFlag; ti++) {
          const t = bState.targets[ti];
          bState.targetIndex = ti;
          bState.currentTarget = t.label;
          bState.lastBeat = Date.now();
          if (!isSocketReady(clientId)) { pushLog(bState, 'warn', 'Socket dropped'); break; }
          try {
            const res = await safeSend(clientId, t.jid, msg);
            if (res.ok) { bState.sent++; pushLog(bState, 'ok', `Msg ${mi + 1} → ${t.label}`); }
            else { bState.failed++; pushLog(bState, 'err', `Msg ${mi + 1} → ${t.label}: ${res.error}`); }
          } catch (e) { bState.failed++; pushLog(bState, 'err', `Send error → ${t.label}: ${e.message}`); }
          bState.remaining = bState.messages.length - (mi + 1);
          if (!bState.stopFlag) await sleep(bState.delayMs);
        }
        if (!isSocketReady(clientId) && !bState.stopFlag) break;
      }
      if (!bState.stopFlag) {
        bState.cycle++;
        bState.remaining = bState.messages.length;
        pushLog(bState, 'info', `Cycle ${bState.cycle} completed`);
      }
    }
  } catch (loopErr) {
    pushLog(bState, 'err', 'Worker crashed: ' + loopErr.message);
    if (!bState.stopFlag) {
      bState.workerAlive = false;
      bState.running = true;
      setTimeout(() => runWorker(clientId), 3000);
      return;
    }
  } finally {
    if (bState.stopFlag) {
      bState.running = false; bState.stopFlag = false; bState.workerAlive = false;
      pushLog(bState, 'info', `Task stopped`);
    } else bState.workerAlive = false;
  }
}

setInterval(() => {
  for (const [clientId, bState] of bulkStates.entries()) {
    if (bState.running && !bState.workerAlive) {
      pushLog(bState, 'warn', 'Watchdog: restarting worker');
      runWorker(clientId);
    }
  }
}, WATCHDOG_INTERVAL_MS);

function attachListeners(sock, s) {
  sock.ev.on('creds.update', () => {});
  
  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect } = update;
    if (connection === 'open') {
      s.isPaired = true;
      s.pairingCode = null;
      s.isConnecting = false;
      s.connectedAt = new Date().toISOString();
      s.lastError = null;
      const bState = getBulkState(s.clientId);
      pushLog(bState, 'ok', `[${s.id}] Connected successfully!`);
      
      setTimeout(async () => {
        try {
          if (typeof sock.groupFetchAllParticipating === 'function') {
            const all = await sock.groupFetchAllParticipating();
            Object.values(all).forEach((g) => {
              s.groupsCache[g.id] = { id: g.id, name: g.subject || '', size: g.participants?.length || 0 };
            });
          }
        } catch (_) {}
      }, 3000);
      
      if (bState.running && bState.sessionId === s.id && !bState.workerAlive) {
        runWorker(s.clientId);
      }
    }
    
    if (connection === 'close') {
      const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
      s.isPaired = false;
      s.isConnecting = false;
      
      if (shouldReconnect && s.phone) {
        setTimeout(() => connectSession(s.id, s.phone, s.clientId), 3000);
      } else if (statusCode === DisconnectReason.loggedOut) {
        s.pairingCode = null;
        s.phone = null;
      }
    }
  });
}

async function connectSession(id, phone, clientId) {
  const s = ensureSession(id, clientId, phone);
  s.isConnecting = true;
  s.phone = phone;
  s.lastError = null;

  const { state, saveCreds } = await useMultiFileAuthState(s.authDir);
  let version;
  try { version = (await fetchLatestBaileysVersion()).version; } 
  catch (e) { version = [2, 3000, 1015901307]; }

  s.sock = makeWASocket({
    version, auth: state, printQRInTerminal: false,
    logger: pino({ level: 'silent' }),
    browser: ['Ubuntu', 'Chrome', '20.0.04'],
  });

  s.sock.ev.on('creds.update', saveCreds);
  attachListeners(s.sock, s);
  return s.sock;
}

// --- HTML UI (FULL, NOTHING CUT) ---
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>🎀 ${OWNER_NAME} — Cute Control Center 💖</title>
<link href="https://fonts.googleapis.com/css2?family=Quicksand:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  html,body{height:100%}
  body{
    font-family:'Quicksand',sans-serif; color:#5a4a5a;min-height:100vh;
    background:radial-gradient(900px 700px at 10% 5%, rgba(255,182,213,.35), transparent 60%), radial-gradient(900px 700px at 90% 95%, rgba(196,181,253,.35), transparent 60%), linear-gradient(135deg,#fff5fa 0%,#f7efff 50%,#eef7ff 100%);
    background-attachment:fixed;
  }
  .sticker-col{position:fixed;top:0;bottom:0;width:80px;display:flex;flex-direction:column;justify-content:space-around;align-items:center;pointer-events:none;z-index:5;padding:30px 0}
  .sticker-col.left{left:0} .sticker-col.right{right:0}
  .sticker-col span{font-size:34px;animation:bob 3.2s ease-in-out infinite;filter:drop-shadow(0 6px 10px rgba(255,105,180,.45))}
  @keyframes bob{0%,100%{transform:translateY(0) rotate(-8deg)}50%{transform:translateY(-22px) rotate(10deg)}}
  @media(max-width:800px){.sticker-col{display:none}}
  .container{max-width:1180px;margin:0 auto;padding:32px 100px 60px;position:relative;z-index:10}
  @media(max-width:800px){.container{padding:20px 16px}}
  .brand{text-align:center;margin-bottom:28px}
  .brand h1{font-size:clamp(22px,4vw,36px);font-weight:700;background:linear-gradient(120deg,#ff6b9d,#c084fc,#ff6b9d);background-size:300% 300%;-webkit-background-clip:text;color:transparent;animation:gradientShift 6s ease infinite}
  @keyframes gradientShift{0%,100%{background-position:0% 50%}50%{background-position:100% 50%}}
  .brand p{color:#a78bfa;font-size:12px;letter-spacing:4px;margin-top:8px;text-transform:uppercase;font-weight:600}
  .tabs{display:flex;gap:10px;margin-bottom:24px;justify-content:center;flex-wrap:wrap}
  .tab{padding:12px 24px;border-radius:99px;font-size:13px;font-weight:700;cursor:pointer;border:2px solid #ffc4d9;background:rgba(255,255,255,.7);color:#ff6b9d;transition:.25s}
  .tab.active{background:linear-gradient(135deg,#ff6b9d,#c084fc);color:#fff;border-color:transparent;box-shadow:0 8px 20px rgba(255,107,157,.4)}
  .panel{display:none} .panel.active{display:block;animation:fade .35s}
  @keyframes fade{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
  .card{background:rgba(255,255,255,.82);border:2px solid rgba(255,196,217,.5);border-radius:24px;padding:24px;margin-bottom:22px;backdrop-filter:blur(14px);box-shadow:0 10px 40px rgba(255,107,157,.13)}
  .card h2{font-size:14px;margin-bottom:18px;color:#7c3aed;display:flex;align-items:center;gap:10px;text-transform:uppercase;font-weight:700}
  .dot{width:10px;height:10px;border-radius:50%;background:linear-gradient(135deg,#ff6b9d,#c084fc)}
  label{display:block;font-size:11px;color:#a78bfa;margin-bottom:8px;text-transform:uppercase;font-weight:700}
  input,textarea,select{width:100%;background:#fff;border:2px solid #ffc4d9;border-radius:16px;padding:12px 16px;font-family:inherit;font-weight:500;outline:none;transition:.22s;color:#5a4a5a}
  input:focus,textarea:focus,select:focus{border-color:#ff6b9d;box-shadow:0 0 0 4px rgba(255,107,157,.15)}
  .field{margin-bottom:16px}
  button{background:linear-gradient(135deg,#ff6b9d,#c084fc);color:#fff;border:none;border-radius:99px;padding:13px 24px;font-size:12px;font-weight:700;cursor:pointer;transition:.22s;width:100%;box-shadow:0 6px 18px rgba(255,107,157,.35)}
  button:disabled{background:#ccc;box-shadow:none}
  button.ghost{background:#fff;border:2px solid #ffc4d9;color:#ff6b9d;box-shadow:none}
  button.danger{background:linear-gradient(135deg,#ff6b9d,#ff4d6d);border:none}
  .pair-code-box{background:linear-gradient(135deg,#fff5fa,#f7efff);border:2px dashed #ff6b9d;border-radius:16px;padding:20px;text-align:center;margin-top:15px;box-shadow:inset 0 0 30px rgba(255,107,157,.08)}
  .code-value{font-size:38px;font-weight:700;letter-spacing:6px;background:linear-gradient(135deg,#ff6b9d,#c084fc);-webkit-background-clip:text;color:transparent;font-family:monospace}
  .msg{padding:12px 16px;border-radius:14px;font-size:13px;display:none;font-weight:600;margin-top:12px}
  .msg.ok{background:rgba(134,239,172,.2);color:#15803d;border:2px solid rgba(134,239,172,.5);display:block}
  .msg.err{background:rgba(255,107,157,.15);color:#be185d;border:2px solid rgba(255,107,157,.5);display:block}
  .hidden{display:none!important}
  .layout{display:grid;grid-template-columns:420px 1fr;gap:22px}
  @media(max-width:900px){.layout{grid-template-columns:1fr}}
  .statgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:12px}
  .stat{background:#fff;border:2px solid #ffc4d9;border-radius:16px;padding:14px;text-align:center}
  .stat .k{font-size:10px;color:#a78bfa;font-weight:700;margin-bottom:6px;text-transform:uppercase}
  .stat .v{font-size:20px;font-weight:700;color:#5a4a5a}
  .stat .v.pink{color:#ff6b9d} .stat .v.green{color:#16a34a} .stat .v.purple{color:#8b5cf6}
  .log{background:#faf5ff;border:2px solid #c4b5fd;border-radius:16px;padding:14px;height:250px;overflow-y:auto;font-family:monospace;font-size:12px;line-height:1.8}
  .log .line{color:#6b7280;border-bottom:1px dashed #c4b5fd;padding:3px 0}
  .log .t{color:#ff6b9d;margin-right:8px;font-weight:700}
  .btnrow{display:flex;gap:10px;margin-top:10px} .btnrow button{margin-top:0}
  .sessions-list { margin-top: 15px; }
  .session-item { background: #fff; border: 2px solid #ffc4d9; border-radius: 12px; padding: 15px; margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center; gap: 10px; flex-wrap: wrap; }
  .session-actions { display: flex; gap: 6px; }
  .btn-mini { padding: 6px 12px; font-size: 10px; width: auto; margin: 0; }
  .groups-list{background:#fff;border:2px solid #ffc4d9;border-radius:14px;max-height:200px;overflow-y:auto;padding:8px;margin-top:10px}
  .group-item{display:flex;align-items:center;gap:10px;padding:8px 10px;border-radius:8px;cursor:pointer;transition:.18s;border:1px solid transparent;margin-bottom:4px;font-size:12px}
  .group-item:hover{background:rgba(255,196,217,.2)}
  .group-item input[type=checkbox]{width:16px;height:16px;accent-color:#ff6b9d;cursor:pointer}
  .progress{height:8px;background:rgba(255,196,217,.3);border-radius:99px;overflow:hidden;margin-top:12px;border:1px solid rgba(255,196,217,.4)}
  .progress > div{height:100%;width:0%;background:linear-gradient(90deg,#ff6b9d,#c084fc);transition:width .4s;border-radius:99px}
</style>
</head>
<body>
<div class="sticker-col left"><span>🌸</span><span>💖</span><span>✨</span><span>🦋</span><span>🌷</span></div>
<div class="sticker-col right"><span>💕</span><span>🌟</span><span>🎀</span><span>🍓</span><span>🌈</span></div>

<div class="container">
  <div class="brand">
    <h1>🎀 ${OWNER_NAME} 💖</h1>
    <p>✨ WhatsApp Cute Control Center ✨</p>
  </div>

  <div class="tabs">
    <div class="tab active" data-tab="dashboard" onclick="switchTab('dashboard')">💫 Dashboard</div>
    <div class="tab" data-tab="sender" onclick="switchTab('sender')">🌸 Bulk Sender</div>
  </div>

  <div class="panel active" id="panel-dashboard">
    <div class="card">
      <h2><span class="dot"></span>🔐 WhatsApp Login Karein</h2>
      <div class="field">
        <label>Phone Number (Country code ke sath, bina + ke)</label>
        <input id="phoneInput" type="tel" placeholder="e.g. 918368312643"/>
      </div>
      <button id="pairBtn" onclick="startPair()">🔗 Get Pairing Code</button>
      <div id="pairMsg" class="msg"></div>
      
      <div id="livePairDashboard" class="hidden">
        <div class="pair-code-box">
          <div style="font-size:11px;color:#a78bfa;font-weight:700;margin-bottom:10px;">✨ AAPKA PAIRING CODE ✨</div>
          <div class="code-value" id="displayCode">------</div>
        </div>
        <div style="background:#fff;border:2px solid #ffc4d9;border-radius:14px;padding:12px;margin-top:12px;font-size:12px;color:#6b7280;line-height:1.8;">
          <b>Steps:</b><br>
          1. WhatsApp kholiye 📱<br>
          2. <b>Linked Devices</b> par jayein<br>
          3. <b>Link a Device</b> → <b>Link with phone number</b><br>
          4. Upar wala code enter karein 💕
        </div>
      </div>
    </div>

    <div class="card">
      <h2><span class="dot"></span>💫 Aapke Saare Sessions</h2>
      <div id="sessionsList" class="sessions-list">
        <div style="text-align:center;color:#a78bfa;">Loading...</div>
      </div>
    </div>

    <div class="card">
      <h2><span class="dot"></span>💌 Send to Groups</h2>
      <div class="field">
        <label>Select Session</label>
        <select id="groupsSessionSelect" onchange="onGroupsSessionChange()">
          <option value="">-- choose a paired session --</option>
        </select>
      </div>
      <div class="btnrow">
        <button class="ghost" onclick="fetchGroups()">🔄 Refresh Groups</button>
        <button class="ghost" onclick="selectAllGroups()">💖 Select All</button>
        <button class="ghost" onclick="clearGroupSelection()">🗑️ Clear</button>
      </div>
      <div style="text-align:right;font-size:12px;color:#a78bfa;font-weight:700;margin-top:8px;" id="groupsCount">0 groups</div>
      <div class="groups-list" id="groupsList">
        <div style="text-align:center;color:#a78bfa;padding:20px;">Select a session first 🦋</div>
      </div>
      <div class="field" style="margin-top:16px">
        <label>Message to send</label>
        <textarea id="groupMsgInput" style="min-height:80px;" placeholder="Type your cute message... 💌"></textarea>
      </div>
      <button onclick="sendToSelectedGroups()">💌 Send to Selected Groups</button>
      <div id="groupSendMsg" class="msg"></div>
    </div>
  </div>

  <div class="panel" id="panel-sender">
    <div class="layout">
      <div>
        <div class="card">
          <h2><span class="dot"></span>✨ Session</h2>
          <select id="sessionSelect"><option value="">-- select paired session --</option></select>
        </div>
        <div class="card">
          <h2><span class="dot"></span>💌 Queue & Targets</h2>
          <div class="field">
            <label>Upload Messages (.txt)</label>
            <input id="fileInput" type="file" accept=".txt"/>
            <div style="font-size:11px;color:#a78bfa;margin-top:6px;" id="fileHint">No file chosen</div>
          </div>
          <div class="field">
            <label>Groups</label>
            <div class="btnrow">
              <button class="ghost" onclick="fetchGroupsForBulk()">🔄 Fetch Groups</button>
              <button class="ghost" onclick="selectAllBulkGroups()">💖 All</button>
              <button class="ghost" onclick="clearBulkGroups()">🗑️ Clear</button>
            </div>
            <div style="text-align:right;font-size:12px;color:#a78bfa;font-weight:700;margin-top:8px;" id="bulkGroupsCount">0 selected</div>
            <div class="groups-list" id="bulkGroupsList">
              <div style="text-align:center;color:#a78bfa;padding:20px;">Choose session & click Fetch 🌸</div>
            </div>
          </div>
          <div class="field">
            <label>Hater Name (prefix)</label>
            <input id="haterInput" type="text" value="${OWNER_NAME}"/>
          </div>
          <div class="field">
            <label>Delay (seconds) — min 3</label>
            <input id="delayInput" type="number" value="10" min="3"/>
          </div>
          <div class="field">
            <label>Last Hater Name (suffix)</label>
            <input id="lastHaterInput" type="text" placeholder="optional suffix"/>
          </div>
          <div class="btnrow">
            <button id="startBtn" onclick="startServer()">🚀 Start</button>
            <button id="stopBtn" class="danger" onclick="stopTask()" disabled>🛑 Stop</button>
          </div>
        </div>
      </div>
      <div>
        <div class="card">
          <h2><span class="dot"></span>📊 Live Stats</h2>
          <div class="statgrid">
            <div class="stat"><div class="k">Status</div><div class="v" id="statStatus">Idle</div></div>
            <div class="stat"><div class="k">Session</div><div class="v" id="statSession" style="font-size:12px;">—</div></div>
            <div class="stat"><div class="k">Uptime</div><div class="v pink" id="statUptime">00:00:00</div></div>
          </div>
          <div class="statgrid">
            <div class="stat"><div class="k">Sent</div><div class="v green" id="statSent">0</div></div>
            <div class="stat"><div class="k">Failed</div><div class="v pink" id="statFailed">0</div></div>
            <div class="stat"><div class="k">Cycle</div><div class="v" id="statCycle">0</div></div>
          </div>
          <div class="statgrid">
            <div class="stat"><div class="k">Messages</div><div class="v" id="statTotal">0</div></div>
            <div class="stat"><div class="k">Remaining</div><div class="v" id="statRemaining">0</div></div>
            <div class="stat"><div class="k">Progress</div><div class="v purple" id="statProgress">0%</div></div>
          </div>
          <div class="statgrid">
            <div class="stat"><div class="k">Current Msg</div><div class="v" id="statCurMsg" style="font-size:11px;">—</div></div>
            <div class="stat"><div class="k">Target</div><div class="v" id="statCurTarget" style="font-size:11px;">—</div></div>
          </div>
          <div class="progress"><div id="progBar"></div></div>
        </div>
        <div class="card">
          <h2><span class="dot"></span>📜 Live Log</h2>
          <div class="log" id="logBox"><div class="line"><span class="t">[boot]</span> Waiting...</div></div>
          <div class="btnrow" style="margin-top:12px">
            <button class="ghost" onclick="clearLog()">🧹 Clear Log</button>
            <button class="danger" onclick="stopTask()">🛑 Stop Task</button>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>

<script>
var CLIENT_ID = localStorage.getItem('x_client_id');
if (!CLIENT_ID) {
  CLIENT_ID = 'user_' + Math.random().toString(36).substring(2, 15);
  localStorage.setItem('x_client_id', CLIENT_ID);
}
function apiFetch(url, options) {
  options = options || {};
  options.headers = options.headers || {};
  options.headers['x-client-id'] = CLIENT_ID;
  return fetch(url, options);
}

function switchTab(name){
  document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('active',t.dataset.tab===name));
  document.querySelectorAll('.panel').forEach(p=>p.classList.toggle('active',p.id==='panel-'+name));
}
function showMsg(el,type,text){el.textContent=text;el.style.display=type?'block':'none';el.className='msg'+(type?' '+type:'');}
function escapeHtml(s){return String(s).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
function clearLog(){document.getElementById('logBox').innerHTML='';}
function log(type,msg){
  var box=document.getElementById('logBox');
  var el=document.createElement('div');
  el.className='line '+type;
  el.innerHTML='<span class="t">['+new Date().toLocaleTimeString()+']</span>'+escapeHtml(msg);
  box.appendChild(el);
  box.scrollTop=box.scrollHeight;
  while(box.children.length>500) box.removeChild(box.firstChild);
}

var currentSessions=[];
async function loadSessions(){
  try{
    var res=await apiFetch('/api/sessions');
    var d=await res.json();
    currentSessions=d.sessions||[];
    
    var paired=currentSessions.filter(s=>s.paired);
    var opts='<option value="">-- select paired session --</option>'+paired.map(s=>'<option value="'+s.id+'">💚 '+s.phone+'</option>').join('');
    var sel1=document.getElementById('sessionSelect');
    var sel2=document.getElementById('groupsSessionSelect');
    var v1=sel1.value, v2=sel2.value;
    if(sel1.innerHTML!==opts){ sel1.innerHTML=opts; if(paired.some(s=>s.id===v1)) sel1.value=v1; }
    if(sel2.innerHTML!==opts){ sel2.innerHTML=opts; if(paired.some(s=>s.id===v2)) sel2.value=v2; }

    var list=document.getElementById('sessionsList');
    if(!currentSessions.length){
      list.innerHTML='<div style="text-align:center;color:#a78bfa;padding:20px;">No sessions found. Login first.</div>';
      return;
    }
    list.innerHTML=currentSessions.map(s=>{
      var badge = s.paired ? '<span style="background:#dcfce7;color:#16a34a;padding:4px 10px;border-radius:99px;font-size:11px;font-weight:700;">PAIRED</span>' : '<span style="background:#fef08a;color:#ca8a04;padding:4px 10px;border-radius:99px;font-size:11px;font-weight:700;">WAITING</span>';
      var logoutBtn = s.paired ? '<button class="ghost btn-mini" onclick="logoutSession(\''+s.id+'\')">Logout 🚪</button>' : '';
      var deleteBtn = '<button class="danger btn-mini" onclick="deleteSession(\''+s.id+'\')">Delete 🗑️</button>';
      return '<div class="session-item"><div><b>📱 '+(s.phone||'—')+'</b><br><small style="color:#a78bfa;">'+s.id+'</small></div><div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">'+badge+'<div class="session-actions">'+logoutBtn+deleteBtn+'</div></div></div>';
    }).join('');
  }catch(e){}
}

async function logoutSession(id){
  if(!confirm('Logout this session?')) return;
  try{
    var res = await apiFetch('/api/sessions/'+id+'/logout', {method:'POST'});
    var d = await res.json();
    if(!d.success) throw new Error(d.error);
    loadSessions();
  }catch(e){ alert(e.message); }
}

async function deleteSession(id){
  if(!confirm('Delete this session completely?')) return;
  try{
    var res = await apiFetch('/api/sessions/'+id, {method:'DELETE'});
    var d = await res.json();
    if(!d.success) throw new Error(d.error);
    loadSessions();
  }catch(e){ alert(e.message); }
}

async function startPair(){
  var phone=document.getElementById('phoneInput').value.trim();
  var btn=document.getElementById('pairBtn');
  var msgEl=document.getElementById('pairMsg');
  var dash=document.getElementById('livePairDashboard');
  var codeVal=document.getElementById('displayCode');

  if(!phone || !/^\\d{10,15}$/.test(phone)){ showMsg(msgEl,'err','Sahi number dalein (Country code ke sath)'); return; }
  
  btn.disabled=true; btn.textContent='Generating Code...';
  showMsg(msgEl,'',''); dash.classList.add('hidden');
  
  try{
    var res=await apiFetch('/api/pair',{
      method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({phone:phone})
    });
    var d=await res.json();
    
    if(!d.success) throw new Error(d.error||'Failed');
    
    dash.classList.remove('hidden');
    codeVal.textContent=d.code;
    showMsg(msgEl,'ok','Code generate ho gaya! Ab WhatsApp me enter karein.');
    document.getElementById('phoneInput').value = '';
    loadSessions();
    
  }catch(e){
    showMsg(msgEl,'err',e.message);
  }finally{
    btn.disabled=false; btn.textContent='🔗 Get Pairing Code';
  }
}

// ===== DASHBOARD GROUPS =====
var allGroups=[];
var dashSelected=new Set();
function onGroupsSessionChange(){
  dashSelected.clear(); allGroups=[];
  document.getElementById('groupsList').innerHTML='<div style="text-align:center;color:#a78bfa;padding:20px;">Click Refresh to load groups 🌸</div>';
  document.getElementById('groupsCount').textContent='0 groups';
}
async function fetchGroups(){
  var sid=document.getElementById('groupsSessionSelect').value;
  if(!sid){alert('Select session first');return;}
  document.getElementById('groupsList').innerHTML='<div style="text-align:center;color:#a78bfa;padding:20px;">Loading... ⏳</div>';
  try{
    var res=await apiFetch('/api/sessions/'+sid+'/groups');
    var d=await res.json();
    if(!d.success) throw new Error(d.error);
    allGroups=d.groups||[];
    dashSelected.clear();
    renderDashGroups();
    renderBulkGroups();
    log('info','Fetched '+allGroups.length+' groups');
  }catch(e){document.getElementById('groupsList').innerHTML='<div style="text-align:center;color:#dc2626;padding:20px;">❌ '+escapeHtml(e.message)+'</div>';}
}
function renderDashGroups(){
  document.getElementById('groupsCount').textContent=allGroups.length+' groups';
  var list=document.getElementById('groupsList');
  if(!allGroups.length){list.innerHTML='<div style="text-align:center;color:#a78bfa;padding:20px;">No groups found 😢</div>';return;}
  list.innerHTML=allGroups.map(g=>{
    var sel=dashSelected.has(g.id)?' selected':'';
    var chk=dashSelected.has(g.id)?' checked':'';
    return '<label class="group-item'+sel+'" data-jid="'+escapeHtml(g.id)+'"><input type="checkbox"'+chk+' onchange="toggleDashGroup(\''+escapeHtml(g.id)+'\',this.checked)"/> '+escapeHtml(g.name||'no name')+'</label>';
  }).join('');
}
function toggleDashGroup(jid,chk){
  if(chk) dashSelected.add(jid); else dashSelected.delete(jid);
  document.querySelectorAll('#groupsList .group-item').forEach(el=>{if(el.dataset.jid===jid) el.classList.toggle('selected',chk);});
}
function selectAllGroups(){allGroups.forEach(g=>dashSelected.add(g.id));renderDashGroups();}
function clearGroupSelection(){dashSelected.clear();renderDashGroups();}
async function sendToSelectedGroups(){
  var sid=document.getElementById('groupsSessionSelect').value;
  var msg=document.getElementById('groupMsgInput').value.trim();
  var box=document.getElementById('groupSendMsg');
  if(!sid){showMsg(box,'err','Select session');return;}
  if(!dashSelected.size){showMsg(box,'err','Select groups');return;}
  if(!msg){showMsg(box,'err','Enter message');return;}
  showMsg(box,'','');
  try{
    var res=await apiFetch('/api/sessions/'+sid+'/send-groups',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({groupIds:Array.from(dashSelected),message:msg})});
    var d=await res.json();
    if(!d.success) throw new Error(d.error);
    showMsg(box,'ok','💌 Sent to '+d.sent+'/'+d.total+' groups');
    log('ok','Group send: '+d.sent+'/'+d.total);
  }catch(e){showMsg(box,'err','❌ '+e.message);}
}

// ===== BULK =====
var bulkSelected=new Set();
async function fetchGroupsForBulk(){
  var sid=document.getElementById('sessionSelect').value;
  if(!sid){alert('Select session first');return;}
  try{
    var res=await apiFetch('/api/sessions/'+sid+'/groups');
    var d=await res.json();
    if(!d.success) throw new Error(d.error);
    allGroups=d.groups||[];
    bulkSelected.clear();
    renderBulkGroups();
    log('info','Loaded '+allGroups.length+' groups for bulk');
  }catch(e){alert(e.message);}
}
function renderBulkGroups(){
  document.getElementById('bulkGroupsCount').textContent=bulkSelected.size+' selected';
  var list=document.getElementById('bulkGroupsList');
  if(!allGroups.length){list.innerHTML='<div style="text-align:center;color:#a78bfa;padding:20px;">Choose session & click Fetch 🌸</div>';return;}
  list.innerHTML=allGroups.map(g=>{
    var sel=bulkSelected.has(g.id)?' selected':'';
    var chk=bulkSelected.has(g.id)?' checked':'';
    return '<label class="group-item'+sel+'" data-jid="'+escapeHtml(g.id)+'"><input type="checkbox"'+chk+' onchange="toggleBulkGroup(\''+escapeHtml(g.id)+'\',this.checked)"/> '+escapeHtml(g.name||'no name')+'</label>';
  }).join('');
}
function toggleBulkGroup(jid,chk){
  if(chk) bulkSelected.add(jid); else bulkSelected.delete(jid);
  document.getElementById('bulkGroupsCount').textContent=bulkSelected.size+' selected';
  document.querySelectorAll('#bulkGroupsList .group-item').forEach(el=>{if(el.dataset.jid===jid) el.classList.toggle('selected',chk);});
}
function selectAllBulkGroups(){allGroups.forEach(g=>bulkSelected.add(g.id));renderBulkGroups();}
function clearBulkGroups(){bulkSelected.clear();renderBulkGroups();}

document.getElementById('fileInput').addEventListener('change',async function(e){
  var f=e.target.files[0];
  document.getElementById('fileHint').textContent=f?f.name:'No file chosen';
});

async function startServer(){
  var sid=document.getElementById('sessionSelect').value;
  var file=document.getElementById('fileInput').files[0];
  var hater=document.getElementById('haterInput').value.trim();
  var delay=parseInt(document.getElementById('delayInput').value)||10;
  if(delay<3) delay=3;
  var lastHater=document.getElementById('lastHaterInput').value.trim();

  if(!sid){alert('Choose session');return;}
  if(!file){alert('Upload .txt');return;}
  if(!bulkSelected.size){alert('Select groups');return;}

  var btn=document.getElementById('startBtn');
  btn.disabled=true; btn.textContent='🚀 Starting...';
  var fd=new FormData();
  fd.append('file',file);
  fd.append('hater',hater);
  fd.append('delay',String(delay));
  fd.append('lastHater',lastHater);
  fd.append('groupIds',JSON.stringify(Array.from(bulkSelected)));
  fd.append('sessionId',sid);
  log('info','Starting bulk task...');
  try{
    var res=await apiFetch('/api/bulk/start',{method:'POST',body:fd});
    var d=await res.json();
    if(!d.success) throw new Error(d.error);
    log('ok','Task started — '+d.total+' msgs');
  }catch(e){log('err','Start failed: '+e.message);alert(e.message);}
  finally{btn.disabled=false; btn.textContent='🚀 Start';}
}

async function stopTask(){
  try{
    await apiFetch('/api/bulk/stop',{method:'POST'});
    log('warn','Stop requested');
  }catch(e){}
}

// ===== POLLING =====
var lastLogId=0;
setInterval(loadSessions, 3000);
setInterval(async ()=>{
  try{
    var res=await apiFetch('/api/bulk/status');
    var d=await res.json();
    document.getElementById('statStatus').textContent=d.running?'Running':'Idle';
    document.getElementById('statStatus').style.color=d.running?'#16a34a':'#5a4a5a';
    document.getElementById('statSession').textContent=d.sessionId||'—';
    document.getElementById('statUptime').textContent=d.uptimeFormatted||'00:00:00';
    document.getElementById('statSent').textContent=d.sent||0;
    document.getElementById('statFailed').textContent=d.failed||0;
    document.getElementById('statCycle').textContent=d.cycle||0;
    document.getElementById('statTotal').textContent=d.total||0;
    document.getElementById('statRemaining').textContent=d.remaining||0;
    document.getElementById('statCurMsg').textContent=d.currentMessage?d.currentMessage.slice(0,30):'—';
    document.getElementById('statCurTarget').textContent=d.currentTarget?d.currentTarget.slice(0,25):'—';
    var pct=d.total>0?Math.round(((d.total-d.remaining)/d.total)*100):0;
    document.getElementById('statProgress').textContent=pct+'%';
    document.getElementById('progBar').style.width=pct+'%';
    document.getElementById('startBtn').disabled=d.running;
    document.getElementById('stopBtn').disabled=!d.running;
    if(d.logs&&d.logs.length){
      var box=document.getElementById('logBox');
      d.logs.forEach(l=>{
        if(l.id>lastLogId){
          var el=document.createElement('div');
          el.className='line '+(l.type||'info');
          el.innerHTML='<span class="t">['+new Date(l.ts).toLocaleTimeString()+']</span>'+escapeHtml(l.msg);
          box.appendChild(el);
        }
      });
      lastLogId=d.logs[d.logs.length-1].id;
      box.scrollTop=box.scrollHeight;
      while(box.children.length>500) box.removeChild(box.firstChild);
    }
  }catch(e){}
}, 2000);
loadSessions();
</script>
</body>
</html>`;

// ============ EXPRESS APP ============
const app = express();
app.use(express.json());
app.get('/', (req, res) => res.type('html').send(DASHBOARD_HTML));
const upload = multer({ dest: uploadsDir });

function getClientId(req) {
  return req.headers['x-client-id'] || null;
}

app.post('/api/pair', async (req, res) => {
  try {
    const clientId = getClientId(req);
    if (!clientId) return res.status(401).json({ success: false, error: 'Client ID missing' });

    let { phone } = req.body;
    if (!phone) return res.status(400).json({ success: false, error: 'Phone required' });
    phone = phone.replace(/\D/g, '');
    if (phone.length === 10) phone = '91' + phone;

    for (let [id, s] of sessions.entries()) {
      if (s.clientId === clientId && s.phone === phone && s.isPaired) {
        return res.status(400).json({ success: false, error: 'Already paired!' });
      }
    }

    const id = 'session_' + phone.slice(-4) + '_' + Date.now().toString().slice(-4);
    const s = ensureSession(id, clientId, phone);
    
    if (fs.existsSync(s.authDir)) fs.rmSync(s.authDir, { recursive: true, force: true });

    const sock = await connectSession(id, phone, clientId);
    await sleep(2000);
    
    const code = await sock.requestPairingCode(phone);
    
    s.pairingCode = code;
    s.isConnecting = false;
    
    res.json({ success: true, id, code });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/sessions', (req, res) => {
  const clientId = getClientId(req);
  if (!clientId) return res.status(401).json({ success: false, error: 'Client ID missing' });
  
  const list = [...sessions.values()]
    .filter(s => s.clientId === clientId)
    .map((s) => ({
      id: s.id, paired: s.isPaired, phone: s.phone, code: s.pairingCode, error: s.lastError,
    }));
  res.json({ success: true, sessions: list });
});

app.post('/api/sessions/:id/logout', async (req, res) => {
  try {
    const clientId = getClientId(req);
    const s = sessions.get(req.params.id);
    
    if (!s) return res.status(404).json({ success: false, error: 'Session not found' });
    if (s.clientId !== clientId) return res.status(403).json({ success: false, error: 'Unauthorized' });
    
    const bState = getBulkState(clientId);
    if (bState.running && bState.sessionId === s.id) {
      bState.stopFlag = true;
    }
    
    if (s.sock) { try { await s.sock.logout(); } catch (_) {} }
    if (fs.existsSync(s.authDir)) fs.rmSync(s.authDir, { recursive: true, force: true });
    
    sessions.delete(s.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.delete('/api/sessions/:id', async (req, res) => {
  try {
    const clientId = getClientId(req);
    const s = sessions.get(req.params.id);
    
    if (!s) return res.status(404).json({ success: false, error: 'Session not found' });
    if (s.clientId !== clientId) return res.status(403).json({ success: false, error: 'Unauthorized' });
    
    const bState = getBulkState(clientId);
    if (bState.running && bState.sessionId === s.id) {
      bState.stopFlag = true;
    }
    
    if (s.sock) { try { await s.sock.logout(); } catch (_) {} }
    if (fs.existsSync(s.authDir)) fs.rmSync(s.authDir, { recursive: true, force: true });
    
    sessions.delete(s.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/sessions/:id/groups', async (req, res) => {
  const clientId = getClientId(req);
  const s = sessions.get(req.params.id);
  
  if (!s || !s.isPaired) return res.status(400).json({ success: false, error: 'Not paired' });
  if (s.clientId !== clientId) return res.status(403).json({ success: false, error: 'Unauthorized' });
  
  try {
    const map = new Map();
    Object.values(s.groupsCache).forEach(g => map.set(g.id, g));
    try { getGroupsFromStore(s).forEach(g => { if (!map.has(g.id)) map.set(g.id, g); }); } catch (_) {}
    if (map.size === 0 && typeof s.sock.groupFetchAllParticipating === 'function') {
      try {
        const all = await s.sock.groupFetchAllParticipating();
        Object.values(all).forEach((g) => { map.set(g.id, { id: g.id, name: g.subject || '', size: g.participants?.length || 0 }); });
      } catch (e) { console.error('groupFetch failed:', e.message); }
    }
    const groups = [...map.values()].filter((g) => g.id.endsWith('@g.us')).sort((a, b) => (a.name).localeCompare(b.name));
    groups.forEach((g) => { s.groupsCache[g.id] = g; });
    res.json({ success: true, groups });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.post('/api/sessions/:id/send-groups', async (req, res) => {
  const clientId = getClientId(req);
  const s = sessions.get(req.params.id);
  
  if (!s || !s.isPaired || !s.sock) return res.status(400).json({ success: false, error: 'Not paired' });
  if (s.clientId !== clientId) return res.status(403).json({ success: false, error: 'Unauthorized' });
  
  const { groupIds, message } = req.body;
  if (!Array.isArray(groupIds) || !groupIds.length) return res.status(400).json({ success: false, error: 'No groups' });
  if (!message || !message.trim()) return res.status(400).json({ success: false, error: 'Message required' });
  
  let sent = 0, failed = 0; const errors = [];
  for (const jid of groupIds) {
    try { await s.sock.sendMessage(jid, { text: message }); sent++; }
    catch (e) { failed++; errors.push(jid + ': ' + e.message); }
    await sleep(800);
  }
  res.json({ success: true, sent, failed, total: groupIds.length, errors });
});

app.post('/api/bulk/start', upload.single('file'), async (req, res) => {
  try {
    const clientId = getClientId(req);
    const { hater, delay, lastHater, groupIds, sessionId } = req.body;
    
    const s = sessions.get(sessionId);
    if (!s || !s.isPaired) {
      if (req.file) try { fs.unlinkSync(req.file.path); } catch (_) {}
      return res.status(400).json({ success: false, error: 'Not paired' });
    }
    if (s.clientId !== clientId) {
      if (req.file) try { fs.unlinkSync(req.file.path); } catch (_) {}
      return res.status(403).json({ success: false, error: 'Unauthorized' });
    }
    
    const bState = getBulkState(clientId);
    if (bState.running) {
      if (req.file) try { fs.unlinkSync(req.file.path); } catch (_) {}
      return res.status(400).json({ success: false, error: 'Task already running' });
    }
    if (!req.file) return res.status(400).json({ success: false, error: 'Upload .txt file' });
    
    let messages = parseMessagesFile(req.file.path);
    try { fs.unlinkSync(req.file.path); } catch (_) {}
    if (!messages.length) return res.status(400).json({ success: false, error: 'No valid messages' });

    const targets = [];
    const seen = new Set();
    let parsedGroups = [];
    try { parsedGroups = JSON.parse(groupIds || '[]'); } catch (_) {}
    parsedGroups.forEach(jid => { if (jid && !seen.has(jid)) { seen.add(jid); targets.push({ jid, label: jid }); } });
    if (!targets.length) return res.status(400).json({ success: false, error: 'Select groups' });

    let delaySec = parseInt(delay) || DEFAULT_DELAY_SECONDS;
    if (delaySec < MIN_DELAY_SECONDS) delaySec = MIN_DELAY_SECONDS;

    const prefix = hater && hater.trim() ? hater.trim() + '\n\n' : '';
    const suffix = lastHater && lastHater.trim() ? '\n\n' + lastHater.trim() : '';
    const preparedMessages = messages.map(m => prefix + m + suffix);

    bState.running = true; bState.stopFlag = false; bState.sessionId = sessionId;
    bState.sent = 0; bState.failed = 0; bState.tasks += 1;
    bState.total = messages.length; bState.remaining = messages.length;
    bState.cycle = 0; bState.msgIndex = 0; bState.targetIndex = 0;
    bState.totalTargets = targets.length;
    bState.currentMessage = preparedMessages[0] || '';
    bState.currentTarget = targets[0]?.label || '';
    bState.targets = targets;
    bState.messages = preparedMessages;
    bState.delayMs = delaySec * 1000;
    bState.startedAt = Date.now();
    bState.workerAlive = false;
    bState.lastBeat = Date.now();
    
    pushLog(bState, 'info', `Task #${bState.tasks} — ${messages.length} msgs × ${targets.length} targets, delay ${delaySec}s`);
    runWorker(clientId);
    res.json({ success: true, total: messages.length, totalTargets: targets.length });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.post('/api/bulk/stop', (req, res) => {
  const clientId = getClientId(req);
  if (!clientId) return res.status(401).json({ success: false, error: 'Client ID missing' });
  
  const bState = getBulkState(clientId);
  if (!bState.running) return res.status(400).json({ success: false, error: 'No task' });
  
  bState.stopFlag = true;
  pushLog(bState, 'warn', 'Stop signal received');
  res.json({ success: true });
});

app.get('/api/bulk/status', (req, res) => {
  const clientId = getClientId(req);
  if (!clientId) return res.status(401).json({ success: false, error: 'Client ID missing' });
  
  const bState = getBulkState(clientId);
  res.json({
    running: bState.running, sessionId: bState.sessionId,
    sent: bState.sent, failed: bState.failed, tasks: bState.tasks,
    total: bState.total, remaining: bState.remaining, cycle: bState.cycle,
    msgIndex: bState.msgIndex, targetIndex: bState.targetIndex,
    totalTargets: bState.totalTargets,
    currentMessage: bState.currentMessage, currentTarget: bState.currentTarget,
    uptimeFormatted: formatUptime(Date.now() - serverStartTime),
    logs: bState.logs.slice(-200),
  });
});

app.listen(PORT, HOST, () => {
  console.log(`\n🌸 Server started on ${PORT}\n`);
});
