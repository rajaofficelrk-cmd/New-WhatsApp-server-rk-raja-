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

// Render par uploads folder auto create hoga
const uploadsDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}

const PORT = process.env.PORT || 25029;
const HOST = '0.0.0.0';
const AUTH_BASE = 'auth_info_baileys';
const OWNER_NAME = 'RK RAJA';

const MIN_DELAY_SECONDS = 3;
const DEFAULT_DELAY_SECONDS = 10;
const SEND_RETRY = 1;
const RETRY_WAIT_MS = 2000;
const WATCHDOG_INTERVAL_MS = 30000;

// ============ SESSIONS ============
const sessions = new Map(); // id -> state

function getAuthDir(id) {
  return id === 'default' ? AUTH_BASE : `${AUTH_BASE}_${id}`;
}

function newSessionState(id) {
  return {
    id,
    sock: null,
    pairingCode: null,
    isPaired: false,
    phone: null,
    pairingRequested: false,
    isConnecting: false,
    lastError: null,
    connectedAt: null,
    groupsCache: {},
    authDir: getAuthDir(id),
  };
}

function ensureSession(id) {
  if (!sessions.has(id)) sessions.set(id, newSessionState(id));
  return sessions.get(id);
}

// ============ BULK STATE ============
const bulkState = {
  running: false,
  stopFlag: false,
  sessionId: null,
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

let serverStartTime = Date.now();

// ============ UTILS ============
function pushLog(type, msg) {
  bulkState.logs.push({ id: ++bulkState.logId, ts: Date.now(), type, msg });
  if (bulkState.logs.length > 500) bulkState.logs.splice(0, bulkState.logs.length - 500);
  const icons = { ok: '✅', err: '❌', warn: '⚠️', info: 'ℹ️' };
  console.log(`${icons[type] || '•'} ${msg}`);
}

function formatUptime(ms) {
  const s = Math.floor(ms / 1000);
  const h = String(Math.floor(s / 3600)).padStart(2, '0');
  const m = String(Math.floor((s % 3600) / 60)).padStart(2, '0');
  const sec = String(s % 60).padStart(2, '0');
  return `${h}:${m}:${sec}`;
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function getGroupsFromStore(s) {
  const out = [];
  try {
    if (s && s.sock && s.sock.store && s.sock.store.groupMetadata) {
      s.sock.store.groupMetadata.forEach((v) => {
        out.push({ id: v.id, name: v.subject || '', size: v.participants ? v.participants.length : 0 });
      });
    }
  } catch (_) {}
  return out;
}

function isSocketReady() {
  const s = sessions.get(bulkState.sessionId);
  return !!(s && s.sock && s.isPaired);
}

function parseMessagesFile(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  const seen = new Set();
  const out = [];
  content.split(/\r?\n/).forEach((line) => {
    const t = line.trim();
    if (!t) return;
    if (seen.has(t)) return;
    seen.add(t);
    out.push(t);
  });
  return out;
}

// ============ SAFE SEND ============
async function safeSend(jid, text) {
  const s = sessions.get(bulkState.sessionId);
  if (!s || !s.sock) throw new Error('session not ready');
  let lastErr = null;
  for (let attempt = 0; attempt <= SEND_RETRY; attempt++) {
    try {
      if (!isSocketReady()) throw new Error('socket not ready');
      await s.sock.sendMessage(jid, { text });
      return { ok: true };
    } catch (e) {
      lastErr = e;
      if (attempt < SEND_RETRY) {
        pushLog('warn', `Retry ${attempt + 1}/${SEND_RETRY} → ${jid}: ${e.message}`);
        await sleep(RETRY_WAIT_MS);
      }
    }
  }
  return { ok: false, error: lastErr ? lastErr.message : 'unknown' };
}

// ============ WORKER ============
async function runWorker() {
  if (bulkState.workerAlive) return;
  bulkState.workerAlive = true;
  pushLog('info', `Worker started [${bulkState.sessionId}] — 24/7 loop active`);

  try {
    while (!bulkState.stopFlag) {
      if (!isSocketReady()) {
        bulkState.lastBeat = Date.now();
        await sleep(3000);
        continue;
      }

      for (let mi = 0; mi < bulkState.messages.length && !bulkState.stopFlag; mi++) {
        const msg = bulkState.messages[mi];
        bulkState.msgIndex = mi;
        bulkState.currentMessage = msg;

        for (let ti = 0; ti < bulkState.targets.length && !bulkState.stopFlag; ti++) {
          const t = bulkState.targets[ti];
          bulkState.targetIndex = ti;
          bulkState.currentTarget = t.label;
          bulkState.lastBeat = Date.now();

          if (!isSocketReady()) {
            pushLog('warn', 'Socket temporarily unavailable — waiting to resume');
            break;
          }

          try {
            const res = await safeSend(t.jid, msg);
            if (res.ok) {
              bulkState.sent++;
              pushLog('ok', `Cycle ${bulkState.cycle + 1} | Msg ${mi + 1}/${bulkState.messages.length} → ${t.label}`);
            } else {
              bulkState.failed++;
              pushLog('err', `Msg ${mi + 1} → ${t.label}: ${res.error}`);
            }
          } catch (e) {
            bulkState.failed++;
            pushLog('err', `Unexpected send error → ${t.label}: ${e.message}`);
          }

          bulkState.remaining = bulkState.messages.length - (mi + 1);

          if (!bulkState.stopFlag) await sleep(bulkState.delayMs);
        }

        if (!isSocketReady() && !bulkState.stopFlag) break;
      }

      if (!bulkState.stopFlag) {
        bulkState.cycle++;
        bulkState.remaining = bulkState.messages.length;
        bulkState.msgIndex = 0;
        bulkState.targetIndex = 0;
        pushLog('info', `Cycle ${bulkState.cycle} completed — restarting from Message 1`);
      }
    }
  } catch (loopErr) {
    pushLog('err', 'Worker crashed: ' + loopErr.message);
    if (!bulkState.stopFlag) {
      pushLog('warn', 'Worker auto-restarting in 3s...');
      bulkState.workerAlive = false;
      bulkState.running = true;
      setTimeout(() => { runWorker(); }, 3000);
      return;
    }
  } finally {
    if (bulkState.stopFlag) {
      bulkState.running = false;
      bulkState.stopFlag = false;
      bulkState.workerAlive = false;
      bulkState.currentMessage = '';
      bulkState.currentTarget = '';
      pushLog('info', `Task stopped — Sent: ${bulkState.sent}, Failed: ${bulkState.failed}, Cycles: ${bulkState.cycle}`);
    } else {
      bulkState.workerAlive = false;
    }
  }
}

setInterval(() => {
  if (bulkState.running && !bulkState.workerAlive) {
    pushLog('warn', 'Watchdog: worker not alive — restarting');
    runWorker();
  }
}, WATCHDOG_INTERVAL_MS);

// ============ CONNECT PER SESSION ============
async function connectSession(sessionId, phone) {
  const s = ensureSession(sessionId);
  if (!phone) throw new Error('Phone number required');
  if (s.isConnecting) throw new Error('Connection already in progress for ' + sessionId);
  if (s.isPaired) throw new Error('Session already paired');

  s.isConnecting = true;
  s.phone = phone;
  s.pairingCode = null;
  s.pairingRequested = false;
  s.lastError = null;

  try {
    const { state, saveCreds } = await useMultiFileAuthState(s.authDir);
    const { version } = await fetchLatestBaileysVersion();

    s.sock = makeWASocket({
      version,
      auth: state,
      printQRInTerminal: false,
      logger: pino({ level: 'silent' }),
      browser: ['Ubuntu', 'Chrome', '20.0.04'],
      mobile: false,
      syncFullHistory: false,
    });

    s.sock.ev.on('creds.update', saveCreds);

    s.sock.ev.on('groups.upsert', (groups) => {
      groups.forEach((g) => {
        s.groupsCache[g.id] = {
          id: g.id, name: g.subject || '', size: g.participants ? g.participants.length : 0,
        };
      });
    });

    s.sock.ev.on('groups.update', (updates) => {
      updates.forEach((u) => {
        if (s.groupsCache[u.id]) {
          if (u.subject) s.groupsCache[u.id].name = u.subject;
        } else if (u.id) {
          s.groupsCache[u.id] = { id: u.id, name: u.subject || '', size: 0 };
        }
      });
    });

    s.sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect } = update;

      if (connection === 'connecting' && !s.sock.authState.creds.registered && !s.pairingRequested) {
        s.pairingRequested = true;
        try {
          await sleep(1000);
          s.pairingCode = await s.sock.requestPairingCode(phone);
          console.log(`\n📱 [${sessionId}] PAIRING CODE for ${phone}: ${s.pairingCode}\n`);
          pushLog('info', `[${sessionId}] Pairing code for ${phone}: ${s.pairingCode}`);
          s.lastError = null;
        } catch (err) {
          s.lastError = err.message;
          s.pairingCode = null;
          s.pairingRequested = false;
          s.isConnecting = false;
        }
      }

      if (connection === 'open') {
        s.isPaired = true;
        s.pairingCode = null;
        s.pairingRequested = false;
        s.isConnecting = false;
        s.connectedAt = new Date().toISOString();
        s.lastError = null;
        pushLog('ok', `[${sessionId}] Connected (${phone})`);

        setTimeout(async () => {
          try {
            if (typeof s.sock.groupFetchAllParticipating === 'function') {
              const all = await s.sock.groupFetchAllParticipating();
              Object.values(all).forEach((g) => {
                s.groupsCache[g.id] = {
                  id: g.id, name: g.subject || '', size: g.participants ? g.participants.length : 0,
                };
              });
            }
            const store = getGroupsFromStore(s);
            store.forEach((g) => { s.groupsCache[g.id] = g; });
            pushLog('info', `[${sessionId}] Loaded ${Object.keys(s.groupsCache).length} groups`);
          } catch (e) {
            console.error('Group cache load error:', e.message);
          }
        }, 3000);

        if (bulkState.running && bulkState.sessionId === sessionId && !bulkState.workerAlive) {
          pushLog('info', `[${sessionId}] Reconnect — resuming bulk worker`);
          runWorker();
        }
      }

      if (connection === 'close') {
        const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        pushLog('warn', `[${sessionId}] Connection closed (code ${statusCode})`);
        s.isPaired = false;
        s.isConnecting = false;

        if (bulkState.running && bulkState.sessionId === sessionId) {
          pushLog('warn', `[${sessionId}] Socket dropped — worker will auto-resume`);
        }

        if (shouldReconnect && s.phone) {
          s.pairingRequested = false;
          setTimeout(() => connectSession(sessionId, s.phone).catch(console.error), 3000);
        } else if (statusCode === DisconnectReason.loggedOut) {
          s.pairingCode = null;
          s.pairingRequested = false;
          s.phone = null;
          if (bulkState.running && bulkState.sessionId === sessionId) {
            bulkState.stopFlag = true;
            pushLog('warn', `[${sessionId}] Logged out — stopping bulk worker`);
          }
        }
      }
    });
  } catch (err) {
    s.isConnecting = false;
    s.lastError = err.message;
    throw err;
  }
}

// ============================================================
// EMBEDDED HTML — CUTE THEME
// ============================================================
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>🎀 ${OWNER_NAME} — Cute Control Center 💖</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Quicksand:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  html,body{height:100%}
  body{
    font-family:'Quicksand',-apple-system,BlinkMacSystemFont,sans-serif;
    color:#5a4a5a;min-height:100vh;overflow-x:hidden;position:relative;
    background:
      radial-gradient(900px 700px at 10% 5%, rgba(255,182,213,.35), transparent 60%),
      radial-gradient(900px 700px at 90% 95%, rgba(196,181,253,.35), transparent 60%),
      linear-gradient(135deg,#fff5fa 0%,#f7efff 50%,#eef7ff 100%);
    background-attachment:fixed;
  }
  .bubble{position:fixed;border-radius:50%;pointer-events:none;z-index:0;opacity:.55;filter:blur(2px);animation:floaty 8s ease-in-out infinite}
  .bubble.b1{width:180px;height:180px;background:radial-gradient(circle,#ffc4d9,transparent 70%);top:8%;left:5%}
  .bubble.b2{width:220px;height:220px;background:radial-gradient(circle,#c4b5fd,transparent 70%);bottom:10%;right:6%;animation-delay:2s}
  .bubble.b3{width:140px;height:140px;background:radial-gradient(circle,#a5f3fc,transparent 70%);top:50%;left:8%;animation-delay:4s}
  @keyframes floaty{0%,100%{transform:translateY(0)}50%{transform:translateY(-30px)}}

  .sticker-col{
    position:fixed;top:0;bottom:0;width:80px;
    display:flex;flex-direction:column;justify-content:space-around;
    align-items:center;pointer-events:none;z-index:5;padding:30px 0;
  }
  .sticker-col.left{left:0}
  .sticker-col.right{right:0}
  .sticker-col span{
    font-size:34px;display:block;text-align:center;
    filter:drop-shadow(0 6px 10px rgba(255,105,180,.45));
    animation:bob 3.2s ease-in-out infinite;
  }
  .sticker-col span:nth-child(2){animation-delay:.4s;animation-duration:3.6s}
  .sticker-col span:nth-child(3){animation-delay:.8s;animation-duration:2.8s}
  .sticker-col span:nth-child(4){animation-delay:1.2s;animation-duration:4s}
  .sticker-col span:nth-child(5){animation-delay:1.6s;animation-duration:3.4s}
  @keyframes bob{
    0%,100%{transform:translateY(0) rotate(-8deg) scale(1)}
    25%{transform:translateY(-12px) rotate(4deg) scale(1.08)}
    50%{transform:translateY(-22px) rotate(10deg) scale(1.15)}
    75%{transform:translateY(-10px) rotate(-4deg) scale(1.05)}
  }
  @media(max-width:1100px){.sticker-col{width:50px}.sticker-col span{font-size:24px}}
  @media(max-width:800px){.sticker-col{display:none}}

  .container{max-width:1180px;margin:0 auto;padding:32px 100px 60px;position:relative;z-index:10}
  @media(max-width:1100px){.container{padding:24px 60px 50px}}
  @media(max-width:800px){.container{padding:20px 16px 50px}}

  .brand{text-align:center;margin-bottom:28px}
  .brand h1{
    font-size:clamp(22px,4vw,36px);font-weight:700;letter-spacing:2px;
    background:linear-gradient(120deg,#ff6b9d,#c084fc,#f9a8d4,#ff6b9d);
    background-size:300% 300%;
    -webkit-background-clip:text;background-clip:text;color:transparent;
    animation:gradientShift 6s ease infinite;
    filter:drop-shadow(0 2px 6px rgba(255,107,157,.3));
  }
  @keyframes gradientShift{0%,100%{background-position:0% 50%}50%{background-position:100% 50%}}
  .brand p{color:#a78bfa;font-size:12px;letter-spacing:4px;margin-top:8px;text-transform:uppercase;font-weight:600}

  .tabs{display:flex;gap:10px;margin-bottom:24px;flex-wrap:wrap;justify-content:center}
  .tab{
    padding:12px 24px;border-radius:99px;font-size:13px;font-weight:700;letter-spacing:1px;
    cursor:pointer;border:2px solid #ffc4d9;background:rgba(255,255,255,.7);
    color:#ff6b9d;transition:.25s;backdrop-filter:blur(8px);
  }
  .tab.active{
    background:linear-gradient(135deg,#ff6b9d,#c084fc);color:#fff;border-color:transparent;
    box-shadow:0 8px 20px rgba(255,107,157,.4);transform:translateY(-1px);
  }
  .tab:hover:not(.active){background:rgba(255,196,217,.4);transform:translateY(-1px)}

  .panel{display:none}
  .panel.active{display:block;animation:fade .35s ease}
  @keyframes fade{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}

  .card{
    background:rgba(255,255,255,.82);border:2px solid rgba(255,196,217,.5);
    border-radius:24px;padding:24px;margin-bottom:22px;
    backdrop-filter:blur(14px) saturate(150%);
    box-shadow:0 10px 40px rgba(255,107,157,.13),0 2px 8px rgba(196,181,253,.1);
    position:relative;
  }
  .card h2{
    font-size:14px;margin-bottom:18px;color:#7c3aed;letter-spacing:1.5px;
    display:flex;align-items:center;gap:10px;text-transform:uppercase;font-weight:700;
  }
  .card h2 .dot{
    width:10px;height:10px;border-radius:50%;
    background:linear-gradient(135deg,#ff6b9d,#c084fc);
    box-shadow:0 0 12px rgba(255,107,157,.6);
  }

  .layout{display:grid;grid-template-columns:420px 1fr;gap:22px}
  @media(max-width:900px){.layout{grid-template-columns:1fr}}

  label{display:block;font-size:11px;color:#a78bfa;margin-bottom:8px;letter-spacing:1px;text-transform:uppercase;font-weight:700}
  input,textarea,select{
    width:100%;background:rgba(255,255,255,.9);border:2px solid #ffc4d9;
    border-radius:16px;padding:12px 16px;color:#5a4a5a;font-size:14px;
    font-family:inherit;font-weight:500;outline:none;transition:.22s;
  }
  input:focus,textarea:focus,select:focus{
    border-color:#ff6b9d;box-shadow:0 0 0 4px rgba(255,107,157,.15);background:#fff;
  }
  input:disabled{color:#c4b5fd;cursor:not-allowed;background:#faf5ff}
  textarea{resize:vertical;min-height:96px}
  .field{margin-bottom:16px}

  button{
    background:linear-gradient(135deg,#ff6b9d,#c084fc);color:#fff;border:none;
    border-radius:99px;padding:13px 24px;font-size:12px;font-weight:700;
    letter-spacing:1.5px;text-transform:uppercase;cursor:pointer;
    transition:.22s;margin-top:6px;width:100%;font-family:inherit;
    box-shadow:0 6px 18px rgba(255,107,157,.35);
  }
  button:hover:not(:disabled){transform:translateY(-2px);box-shadow:0 10px 24px rgba(255,107,157,.5)}
  button:disabled{background:linear-gradient(135deg,#ddd,#ccc);color:#fff;cursor:not-allowed;box-shadow:none;opacity:.7}
  button.ghost{background:rgba(255,255,255,.9);border:2px solid #ffc4d9;color:#ff6b9d;box-shadow:none}
  button.ghost:hover:not(:disabled){background:rgba(255,196,217,.3);box-shadow:0 4px 12px rgba(255,107,157,.2)}
  button.danger{background:linear-gradient(135deg,#ff6b9d,#ff4d6d);border:none}
  .btnrow{display:flex;gap:10px}
  .btnrow button{margin-top:0}

  .badge{
    display:inline-block;padding:7px 16px;border-radius:99px;font-size:11px;
    font-weight:700;letter-spacing:1px;text-transform:uppercase;
  }
  .badge-idle{background:rgba(196,181,253,.2);color:#8b5cf6;border:2px solid rgba(196,181,253,.4)}
  .badge-wait{background:rgba(253,224,71,.2);color:#ca8a04;border:2px solid rgba(253,224,71,.5);animation:pulse 1.8s infinite}
  .badge-paired{background:rgba(134,239,172,.25);color:#16a34a;border:2px solid rgba(134,239,172,.6);box-shadow:0 0 14px rgba(134,239,172,.3)}
  .badge-run{background:linear-gradient(135deg,#ff6b9d,#c084fc);color:#fff;border:2px solid transparent;animation:pulse 1.8s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.6}}

  /* Session cards */
  .session-card{
    background:linear-gradient(135deg,rgba(255,255,255,.95),rgba(250,245,255,.95));
    border:2px solid #ffc4d9;border-radius:20px;padding:18px;margin-bottom:14px;
    box-shadow:0 6px 20px rgba(255,107,157,.12);transition:.25s;
  }
  .session-card:hover{box-shadow:0 10px 30px rgba(255,107,157,.2);transform:translateY(-2px)}
  .session-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;gap:10px}
  .session-name{font-size:16px;font-weight:700;color:#7c3aed;letter-spacing:.5px}
  .session-body{display:flex;flex-direction:column;gap:10px}
  .sess-info{font-size:13px;color:#6b7280;line-height:1.9}
  .pair-code-box{
    background:linear-gradient(135deg,#fff5fa,#f7efff);
    border:2px dashed #ff6b9d;border-radius:16px;
    padding:20px;text-align:center;
    box-shadow:inset 0 0 30px rgba(255,107,157,.08);
  }
  .code-label{font-size:11px;color:#a78bfa;letter-spacing:2px;margin-bottom:10px;text-transform:uppercase;font-weight:700}
  .code-value{
    font-size:32px;font-weight:700;letter-spacing:6px;
    background:linear-gradient(135deg,#ff6b9d,#c084fc);
    -webkit-background-clip:text;background-clip:text;color:transparent;
    font-family:'Courier New',monospace;
  }
  .steps-cute{
    background:rgba(255,245,250,.8);border:2px solid rgba(255,196,217,.6);
    border-radius:14px;padding:12px 16px;font-size:12px;line-height:2;color:#6b7280;
  }
  .connecting-msg{color:#a78bfa;font-size:13px;font-weight:600;text-align:center;padding:14px}
  .phone-input{margin-bottom:0}
  .btn-row{display:flex;gap:8px}
  .btn-row button{flex:1;padding:10px 16px;font-size:11px;margin-top:0}

  .groups-toolbar{display:flex;gap:8px;align-items:center;margin-bottom:12px;flex-wrap:wrap}
  .groups-toolbar button{width:auto;margin-top:0;padding:9px 16px;font-size:11px}
  .groups-toolbar .count{font-size:12px;color:#a78bfa;margin-left:auto;font-weight:700}

  .groups-list{
    background:rgba(255,255,255,.7);border:2px solid rgba(255,196,217,.4);
    border-radius:16px;max-height:340px;overflow-y:auto;padding:8px;
  }
  .groups-list::-webkit-scrollbar{width:8px}
  .groups-list::-webkit-scrollbar-track{background:transparent}
  .groups-list::-webkit-scrollbar-thumb{background:linear-gradient(#ff6b9d,#c084fc);border-radius:8px}
  .group-item{
    display:flex;align-items:center;gap:12px;padding:10px 12px;border-radius:12px;
    cursor:pointer;transition:.18s;border:2px solid transparent;margin-bottom:4px;
  }
  .group-item:hover{background:rgba(255,196,217,.2);border-color:rgba(255,196,217,.4)}
  .group-item.selected{background:rgba(255,196,217,.35);border-color:#ff6b9d}
  .group-item input[type=checkbox]{width:18px;height:18px;accent-color:#ff6b9d;cursor:pointer;flex-shrink:0}
  .group-info{flex:1;min-width:0}
  .group-name{color:#5a4a5a;font-size:13px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .group-jid{color:#a78bfa;font-size:10px;font-family:'Courier New',monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-top:2px}
  .group-meta{color:#ff6b9d;font-size:10px;font-weight:700;letter-spacing:.5px;flex-shrink:0}

  .empty{padding:30px 20px;text-align:center;color:#c4b5fd;font-size:13px;font-weight:500}
  .hint{font-size:11px;color:#c4b5fd;margin-top:8px;font-weight:500}

  .msg{padding:12px 16px;border-radius:14px;font-size:13px;display:none;font-weight:600;margin-top:12px}
  .msg.ok{background:rgba(134,239,172,.2);color:#15803d;border:2px solid rgba(134,239,172,.5);display:block}
  .msg.err{background:rgba(255,107,157,.15);color:#be185d;border:2px solid rgba(255,107,157,.5);display:block}

  .statgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:12px}
  @media(max-width:520px){.statgrid{grid-template-columns:1fr 1fr}}
  .stat{
    background:linear-gradient(135deg,rgba(255,255,255,.9),rgba(250,245,255,.9));
    border:2px solid rgba(255,196,217,.4);border-radius:16px;padding:14px;text-align:center;
    transition:.2s;
  }
  .stat:hover{border-color:#ff6b9d;transform:translateY(-2px)}
  .stat .k{font-size:10px;color:#a78bfa;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:6px;font-weight:700}
  .stat .v{font-size:20px;font-weight:700;color:#5a4a5a;word-break:break-all}
  .stat .v.pink{color:#ff6b9d}
  .stat .v.purple{color:#8b5cf6}
  .stat .v.green{color:#16a34a}
  .stat .v.small{font-size:13px;font-weight:600}

  .log{
    background:linear-gradient(135deg,#faf5ff,#fff5fa);
    border:2px solid rgba(196,181,253,.5);border-radius:16px;
    padding:14px;height:340px;overflow-y:auto;
    font-family:'Courier New',monospace;font-size:12px;line-height:1.8;
    box-shadow:inset 0 2px 20px rgba(196,181,253,.1);
  }
  .log::-webkit-scrollbar{width:8px}
  .log::-webkit-scrollbar-thumb{background:linear-gradient(#ff6b9d,#c084fc);border-radius:8px}
  .log .line{color:#6b7280;border-bottom:1px dashed rgba(196,181,253,.3);padding:3px 0;word-break:break-all}
  .log .t{color:#ff6b9d;margin-right:8px;font-weight:700}
  .log .ok{color:#16a34a}
  .log .err{color:#dc2626}
  .log .info{color:#7c3aed}
  .log .warn{color:#ca8a04}

  .progress{height:10px;background:rgba(255,196,217,.3);border-radius:99px;overflow:hidden;margin-top:14px;border:2px solid rgba(255,196,217,.4)}
  .progress > div{height:100%;width:0%;background:linear-gradient(90deg,#ff6b9d,#c084fc,#ff6b9d);background-size:200% 100%;transition:width .4s;animation:shine 2s linear infinite;border-radius:99px}
  @keyframes shine{0%{background-position:0 0}100%{background-position:200% 0}}

  .footer{text-align:center;color:#c4b5fd;font-size:11px;letter-spacing:3px;margin-top:40px;text-transform:uppercase;font-weight:700}

  .queue-box{
    background:rgba(255,255,255,.7);border:2px solid rgba(196,181,253,.4);
    border-radius:14px;padding:12px 14px;margin-top:10px;font-size:12px;
    color:#6b7280;max-height:140px;overflow-y:auto;font-family:'Courier New',monospace;
    line-height:1.7;
  }
  .queue-box .qi{color:#7c3aed;margin-right:8px;font-weight:700}

  .hidden{display:none!important}
</style>
</head>
<body>
<div class="bubble b1"></div>
<div class="bubble b2"></div>
<div class="bubble b3"></div>

<div class="sticker-col left">
  <span>🌸</span><span>💖</span><span>✨</span><span>🦋</span><span>🌷</span>
</div>
<div class="sticker-col right">
  <span>💕</span><span>🌟</span><span>🎀</span><span>🍓</span><span>🌈</span>
</div>

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
    <div class="card" id="pairCard">
      <h2><span class="dot"></span>🔐 Naya WhatsApp Login Karein</h2>
      <div class="field">
        <label>Phone Number (Country code ke sath, bina + ke)</label>
        <input id="phoneInput" type="tel" placeholder="e.g. 919876543210"/>
      </div>
      <button id="pairBtn" onclick="startPair()">🔗 Get Pairing Code</button>
      
      <!-- CODE YAHAN NICHE DIKHEGA -->
      <div id="codeDisplayArea" class="hidden" style="margin-top: 18px;">
        <div class="pair-code-box">
          <div class="code-label">✨ Aapka Pairing Code ✨</div>
          <div class="code-value" id="displayCode">------</div>
        </div>
        <div class="steps-cute" style="margin-top: 12px;">
          <b>Steps:</b><br>
          1. WhatsApp kholiye 📱<br>
          2. <b>Linked Devices</b> par jayein<br>
          3. <b>Link a Device</b> → <b>Link with phone number</b><br>
          4. Upar wala code enter karein 💕
        </div>
      </div>
      
      <div id="pairMsg" class="msg"></div>
    </div>

    <div class="card">
      <h2><span class="dot"></span>💫 Aapke Saare Sessions</h2>
      <div id="sessionsList">
        <div class="empty">Loading sessions...</div>
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

      <div class="groups-toolbar">
        <button class="ghost" onclick="fetchGroups()">🔄 Refresh Groups</button>
        <button class="ghost" onclick="selectAllGroups()">💖 Select All</button>
        <button class="ghost" onclick="clearGroupSelection()">🗑️ Clear</button>
        <span class="count" id="groupsCount">0 groups</span>
      </div>

      <div class="groups-list" id="groupsList">
        <div class="empty">Select a session first 🦋</div>
      </div>

      <div class="field" style="margin-top:16px">
        <label>Message to send</label>
        <textarea id="groupMsgInput" placeholder="Type your cute message... 💌"></textarea>
      </div>
      <button onclick="sendToSelectedGroups()">💌 Send to Selected Groups</button>
      <div id="groupSendMsg" class="msg"></div>
    </div>
  </div>

  <div class="panel" id="panel-sender">
    <div class="layout">
      <div>
        <div class="card">
          <h2><span class="dot"></span>✨ Choose Session</h2>
          <div class="field">
            <label>Which WhatsApp number to use?</label>
            <select id="sessionSelect">
              <option value="">-- select paired session --</option>
            </select>
          </div>
          <div class="hint">💡 Pair sessions from the Dashboard tab</div>
        </div>

        <div class="card">
          <h2><span class="dot"></span>💌 Queue & Targets</h2>

          <div class="field">
            <label>Upload Messages (.txt) — each line = one message</label>
            <input id="fileInput" type="file" accept=".txt"/>
            <div class="hint" id="fileHint">No file chosen</div>
            <div class="queue-box" id="queuePreview" style="display:none"></div>
          </div>

          <div class="field">
            <label>Select Groups</label>
            <div class="groups-toolbar" style="margin-bottom:8px">
              <button class="ghost" onclick="fetchGroupsForBulk()">🔄 Fetch Groups</button>
              <button class="ghost" onclick="selectAllBulkGroups()">💖 All</button>
              <button class="ghost" onclick="clearBulkGroups()">🗑️ Clear</button>
              <span class="count" id="bulkGroupsCount">0 selected</span>
            </div>
            <div class="groups-list" id="bulkGroupsList" style="max-height:200px">
              <div class="empty">Choose a session & click Fetch 🌸</div>
            </div>
          </div>

          <div class="field">
            <label>Hater Name (prefix)</label>
            <input id="haterInput" type="text" value="${OWNER_NAME}" placeholder="${OWNER_NAME}"/>
          </div>
          <div class="field">
            <label>Delay (seconds) — min 3s</label>
            <input id="delayInput" type="number" min="3" value="10"/>
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
            <div class="stat"><div class="k">Session</div><div class="v small purple" id="statSession">—</div></div>
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
            <div class="stat"><div class="k">Current Msg</div><div class="v small" id="statCurMsg">—</div></div>
            <div class="stat"><div class="k">Current Target</div><div class="v small" id="statCurTarget">—</div></div>
          </div>
          <div class="progress"><div id="progBar"></div></div>
        </div>

        <div class="card">
          <h2><span class="dot"></span>📜 Live Log</h2>
          <div class="log" id="logBox">
            <div class="line"><span class="t">[boot]</span> Waiting for commands... 💫</div>
          </div>
          <div class="btnrow" style="margin-top:12px">
            <button class="ghost" onclick="clearLog()">🧹 Clear</button>
            <button class="danger" onclick="stopTask()">🛑 Stop</button>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div class="footer">💖 ${OWNER_NAME} © — Made with love 🎀</div>
</div>

<script>
function switchTab(name){
  document.querySelectorAll('.tab').forEach(function(t){t.classList.toggle('active',t.dataset.tab===name)});
  document.querySelectorAll('.panel').forEach(function(p){p.classList.toggle('active',p.id==='panel-'+name)});
}
function showMsg(el,type,text){el.textContent=text;el.style.display=type?'block':'none';el.className='msg'+(type?' '+type:'');}
function escapeHtml(s){return String(s).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
function clearLog(){document.getElementById('logBox').innerHTML='';}
function log(type,msg){
  var box=document.getElementById('logBox');
  var t=new Date().toLocaleTimeString();
  var el=document.createElement('div');
  el.className='line '+type;
  el.innerHTML='<span class="t">['+t+']</span>'+escapeHtml(msg);
  box.appendChild(el);
  box.scrollTop=box.scrollHeight;
  while(box.children.length>500) box.removeChild(box.firstChild);
}

// ============ SESSIONS ============
var currentSessions=[];
var lastSessionsJson='';

async function loadSessions(){
  try{
    var res=await fetch('/api/sessions');
    var d=await res.json();
    var json=JSON.stringify(d.sessions);
    if(json===lastSessionsJson) return;
    lastSessionsJson=json;
    currentSessions=d.sessions||[];
    renderSessions();
    updateSessionDropdowns();
  }catch(e){console.error(e);}
}

function renderSessions(){
  var list=document.getElementById('sessionsList');
  if(!currentSessions.length){
    list.innerHTML='<div class="empty">No sessions yet 🌸 Upar number dalein</div>';
    return;
  }
  list.innerHTML=currentSessions.map(renderSessionCard).join('');
}

function renderSessionCard(s){
  var status, badgeClass;
  if(s.paired){status='Paired 💚';badgeClass='badge-paired';}
  else if(s.code){status='Waiting 💛';badgeClass='badge-wait';}
  else if(s.connecting){status='Connecting 💜';badgeClass='badge-wait';}
  else{status='Idle';badgeClass='badge-idle';}

  var body='';
  if(s.paired){
    body='<div class="sess-info">'+
      '<div>📱 '+escapeHtml(s.phone||'—')+'</div>'+
      '<div>🕐 '+(s.connectedAt?new Date(s.connectedAt).toLocaleString():'—')+'</div>'+
      '</div>'+
      '<div class="btn-row">'+
      '<button class="ghost" onclick="logoutSession(\''+s.id+'\')">🚪 Logout</button>'+
      '</div>';
  }else if(s.code){
    body='<div class="pair-code-box">'+
      '<div class="code-label">✨ Your Pairing Code ✨</div>'+
      '<div class="code-value">'+escapeHtml(s.code)+'</div>'+
      '</div>'+
      '<div class="steps-cute">1. Open WhatsApp 📱<br>2. Linked Devices → Link a Device<br>3. Enter the code above 💕</div>'+
      '<div class="btn-row">'+
      '<button class="ghost" onclick="deleteSession(\''+s.id+'\')">🗑️ Cancel</button>'+
      '</div>';
  }else if(s.connecting){
    body='<div class="connecting-msg">⏳ Generating pairing code... 💫</div>';
  }else{
    body='<div class="connecting-msg">Session ready. Logout to reset.</div>'+
      '<div class="btn-row">'+
      '<button class="ghost" onclick="deleteSession(\''+s.id+'\')">🗑️ Delete</button>'+
      '</div>';
  }

  if(s.error){
    body+='<div class="msg err" style="display:block">⚠️ '+escapeHtml(s.error)+'</div>';
  }

  return '<div class="session-card">'+
    '<div class="session-head">'+
    '<div class="session-name">💫 '+escapeHtml(s.id)+'</div>'+
    '<span class="badge '+badgeClass+'">'+status+'</span>'+
    '</div>'+
    '<div class="session-body">'+body+'</div>'+
    '</div>';
}

async function startPair(){
  var phone = document.getElementById('phoneInput').value.trim();
  var btn = document.getElementById('pairBtn');
  var msgEl = document.getElementById('pairMsg');
  var codeArea = document.getElementById('codeDisplayArea');
  var codeVal = document.getElementById('displayCode');

  if(!phone || !/^\\d{10,15}$/.test(phone)) {
    showMsg(msgEl, 'err', 'Sahi number dalein (Country code ke sath, jaise 91...)');
    return;
  }
  
  btn.disabled = true; 
  btn.textContent = 'Generating Code...';
  showMsg(msgEl, '', '');
  codeArea.classList.add('hidden');
  codeVal.textContent = '------';
  
  try {
    var res = await fetch('/api/pair-new', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({phone:phone})
    });
    var d = await res.json();
    if(!d.success) throw new Error(d.error||'Failed');
    
    showMsg(msgEl, 'ok', 'Code generate ho raha hai, kripya pratiksha karein...');
    codeArea.classList.remove('hidden');
    codeVal.textContent = 'WAIT...';

    // Poll for the code specifically for this new session
    let attempts = 0;
    let interval = setInterval(async () => {
      attempts++;
      if(attempts > 30) { // 30 seconds timeout
        clearInterval(interval);
        codeVal.textContent = 'TIMEOUT';
        showMsg(msgEl, 'err', 'Code generate nahi hua. Network ya number check karein.');
        btn.disabled = false;
        btn.textContent = '🔗 Get Pairing Code';
        return;
      }
      
      var sRes = await fetch('/api/sessions');
      var sData = await sRes.json();
      var mySession = sData.sessions.find(s => s.id === d.id);
      
      if (mySession && mySession.code) {
        clearInterval(interval);
        codeVal.textContent = mySession.code;
        showMsg(msgEl, 'ok', 'WhatsApp me code enter karein!');
        btn.disabled = false;
        btn.textContent = '🔗 Get Pairing Code';
      } else if (mySession && mySession.paired) {
        clearInterval(interval);
        codeArea.classList.add('hidden');
        showMsg(msgEl, 'ok', 'Successfully Linked! 🎉');
        btn.disabled = false;
        btn.textContent = '🔗 Get Pairing Code';
      }
    }, 1000);

  } catch(e) {
    showMsg(msgEl, 'err', '❌ ' + e.message);
    btn.disabled = false;
    btn.textContent = '🔗 Get Pairing Code';
  }
}

async function logoutSession(id){
  if(!confirm('Logout session "'+id+'"? 🚪')) return;
  try{
    await fetch('/api/sessions/'+encodeURIComponent(id)+'/logout',{method:'POST'});
    lastSessionsJson='';
    await loadSessions();
    log('warn','Logged out: '+id);
  }catch(e){alert('❌ '+e.message);}
}

async function deleteSession(id){
  if(!confirm('Delete session "'+id+'"? 🗑️')) return;
  try{
    var res=await fetch('/api/sessions/'+encodeURIComponent(id),{method:'DELETE'});
    var d=await res.json();
    if(!d.success){alert('❌ '+d.error);return;}
    lastSessionsJson='';
    await loadSessions();
    log('warn','Deleted: '+id);
  }catch(e){alert('❌ '+e.message);}
}

function updateSessionDropdowns(){
  var paired=currentSessions.filter(function(s){return s.paired;});
  var opts='<option value="">-- select paired session --</option>'+
    paired.map(function(s){return '<option value="'+escapeHtml(s.id)+'">💚 '+escapeHtml(s.id)+' ('+escapeHtml(s.phone||'')+')</option>';}).join('');
  var a=document.getElementById('groupsSessionSelect');
  var b=document.getElementById('sessionSelect');
  var av=a.value, bv=b.value;
  a.innerHTML=opts; b.innerHTML=opts;
  if(paired.some(function(s){return s.id===av;})) a.value=av;
  if(paired.some(function(s){return s.id===bv;})) b.value=bv;
}

// ============ GROUPS (Dashboard) ============
var allGroups=[];
var dashSelected=new Set();

function onGroupsSessionChange(){
  dashSelected.clear();
  allGroups=[];
  document.getElementById('groupsList').innerHTML='<div class="empty">Click Refresh to load groups 🌸</div>';
  document.getElementById('groupsCount').textContent='0 groups';
}

async function fetchGroups(){
  var sessionId=document.getElementById('groupsSessionSelect').value;
  if(!sessionId){alert('❌ Select a session first');return;}
  var listEl=document.getElementById('groupsList');
  listEl.innerHTML='<div class="empty">Loading... ⏳</div>';
  try{
    var res=await fetch('/api/sessions/'+encodeURIComponent(sessionId)+'/groups');
    var d=await res.json();
    if(!d.success) throw new Error(d.error||'Failed');
    allGroups=d.groups||[];
    dashSelected.clear();
    renderDashGroups();
    renderBulkGroups();
    log('info','Fetched '+allGroups.length+' groups from '+sessionId);
  }catch(e){
    listEl.innerHTML='<div class="empty" style="color:#dc2626">❌ '+escapeHtml(e.message)+'</div>';
    log('err','Fetch groups: '+e.message);
  }
}

function renderDashGroups(){
  var listEl=document.getElementById('groupsList');
  document.getElementById('groupsCount').textContent=allGroups.length+' groups';
  if(!allGroups.length){
    listEl.innerHTML='<div class="empty">No groups found 😢</div>';
    return;
  }
  listEl.innerHTML=allGroups.map(function(g){
    var sel=dashSelected.has(g.id)?' selected':'';
    var checked=dashSelected.has(g.id)?' checked':'';
    return '<label class="group-item'+sel+'" data-jid="'+escapeHtml(g.id)+'">'+
      '<input type="checkbox"'+checked+' onchange="toggleDashGroup(\''+escapeHtml(g.id)+'\',this.checked)"/>'+
      '<div class="group-info">'+
      '<div class="group-name">'+escapeHtml(g.name||'(no name)')+'</div>'+
      '<div class="group-jid">'+escapeHtml(g.id)+'</div>'+
      '</div>'+
      '<div class="group-meta">'+(g.size?g.size+' 👥':'')+'</div>'+
      '</label>';
  }).join('');
}

function toggleDashGroup(jid,checked){
  if(checked) dashSelected.add(jid); else dashSelected.delete(jid);
  document.querySelectorAll('#groupsList .group-item').forEach(function(el){
    if(el.dataset.jid===jid) el.classList.toggle('selected',checked);
  });
}
function selectAllGroups(){allGroups.forEach(function(g){dashSelected.add(g.id)});renderDashGroups();}
function clearGroupSelection(){dashSelected.clear();renderDashGroups();}

async function sendToSelectedGroups(){
  var sessionId=document.getElementById('groupsSessionSelect').value;
  var msg=document.getElementById('groupMsgInput').value.trim();
  var box=document.getElementById('groupSendMsg');
  if(!sessionId){showMsg(box,'err','❌ Select a session');return;}
  if(!dashSelected.size){showMsg(box,'err','❌ Select at least one group');return;}
  if(!msg){showMsg(box,'err','❌ Enter a message');return;}
  showMsg(box,'','');
  try{
    var res=await fetch('/api/sessions/'+encodeURIComponent(sessionId)+'/send-groups',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({groupIds:Array.from(dashSelected),message:msg})
    });
    var d=await res.json();
    if(!d.success) throw new Error(d.error||'Failed');
    showMsg(box,'ok','💌 Sent to '+d.sent+'/'+d.total+' groups'+(d.failed?' ('+d.failed+' failed)':''));
    log('ok','Group send: '+d.sent+'/'+d.total);
  }catch(e){showMsg(box,'err','❌ '+e.message);log('err','Group send: '+e.message);}
}

// ============ BULK ============
var bulkSelected=new Set();

async function fetchGroupsForBulk(){
  var sessionId=document.getElementById('sessionSelect').value;
  if(!sessionId){alert('❌ Select a session first');return;}
  try{
    var res=await fetch('/api/sessions/'+encodeURIComponent(sessionId)+'/groups');
    var d=await res.json();
    if(!d.success) throw new Error(d.error||'Failed');
    allGroups=d.groups||[];
    bulkSelected.clear();
    renderBulkGroups();
    log('info','Loaded '+allGroups.length+' groups for bulk');
  }catch(e){alert('❌ '+e.message);log('err','Fetch groups: '+e.message);}
}

function renderBulkGroups(){
  var listEl=document.getElementById('bulkGroupsList');
  document.getElementById('bulkGroupsCount').textContent=bulkSelected.size+' selected';
  if(!allGroups.length){
    listEl.innerHTML='<div class="empty">Choose session & click Fetch 🌸</div>';
    return;
  }
  listEl.innerHTML=allGroups.map(function(g){
    var sel=bulkSelected.has(g.id)?' selected':'';
    var checked=bulkSelected.has(g.id)?' checked':'';
    return '<label class="group-item'+sel+'" data-jid="'+escapeHtml(g.id)+'">'+
      '<input type="checkbox"'+checked+' onchange="toggleBulkGroup(\''+escapeHtml(g.id)+'\',this.checked)"/>'+
      '<div class="group-info">'+
      '<div class="group-name">'+escapeHtml(g.name||'(no name)')+'</div>'+
      '<div class="group-jid">'+escapeHtml(g.id)+'</div>'+
      '</div>'+
      '<div class="group-meta">'+(g.size?g.size+' 👥':'')+'</div>'+
      '</label>';
  }).join('');
}
function toggleBulkGroup(jid,checked){
  if(checked) bulkSelected.add(jid); else bulkSelected.delete(jid);
  document.getElementById('bulkGroupsCount').textContent=bulkSelected.size+' selected';
  document.querySelectorAll('#bulkGroupsList .group-item').forEach(function(el){
    if(el.dataset.jid===jid) el.classList.toggle('selected',checked);
  });
}
function selectAllBulkGroups(){allGroups.forEach(function(g){bulkSelected.add(g.id)});renderBulkGroups();}
function clearBulkGroups(){bulkSelected.clear();renderBulkGroups();}

document.getElementById('fileInput').addEventListener('change',async function(e){
  var f=e.target.files[0];
  document.getElementById('fileHint').textContent=f?f.name:'No file chosen';
  var box=document.getElementById('queuePreview');
  if(!f){box.style.display='none';return;}
  try{
    var text=await f.text();
    var seen=new Set(); var msgs=[];
    text.split(/\\r?\\n/).forEach(function(line){
      var t=line.trim();
      if(!t)return; if(seen.has(t))return; seen.add(t); msgs.push(t);
    });
    if(!msgs.length){box.style.display='none';return;}
    box.style.display='block';
    box.innerHTML='<div style="color:#a78bfa;margin-bottom:6px;font-weight:700">💌 Queue ('+msgs.length+' messages)</div>'+
      msgs.map(function(m,i){return '<div><span class="qi">'+(i+1)+'.</span>'+escapeHtml(m.slice(0,80))+(m.length>80?'…':'')+'</div>';}).join('');
  }catch(_){box.style.display='none';}
});

async function startServer(){
  var sessionId=document.getElementById('sessionSelect').value;
  var file=document.getElementById('fileInput').files[0];
  var hater=document.getElementById('haterInput').value.trim();
  var delay=parseInt(document.getElementById('delayInput').value)||10;
  if(delay<3) delay=3;
  var lastHater=document.getElementById('lastHaterInput').value.trim();

  if(!sessionId){alert('❌ Choose a session first');return;}
  if(!file){alert('❌ Upload a .txt file');return;}
  if(bulkSelected.size===0){alert('❌ Select at least one group');return;}

  var startBtn=document.getElementById('startBtn');
  startBtn.disabled=true;startBtn.textContent='🚀 Starting...';

  var fd=new FormData();
  fd.append('file',file);
  fd.append('hater',hater);
  fd.append('delay',String(delay));
  fd.append('lastHater',lastHater);
  fd.append('groupIds',JSON.stringify(Array.from(bulkSelected)));
  fd.append('sessionId',sessionId);

  log('info','Starting bulk task on '+sessionId+'...');
  try{
    var res=await fetch('/api/bulk/start',{method:'POST',body:fd});
    var d=await res.json();
    if(!d.success)throw new Error(d.error||'Failed');
    log('ok','Task started — '+d.total+' messages × '+d.totalTargets+' targets');
  }catch(e){
    log('err','Start failed: '+e.message);
    alert('❌ '+e.message);
  }finally{
    startBtn.disabled=false;startBtn.textContent='🚀 Start';
  }
}

async function stopTask(){
  try{
    var res=await fetch('/api/bulk/stop',{method:'POST'});
    var d=await res.json();
    if(d.success) log('warn','Stop requested 💫');
    else log('err',d.error||'No task running');
  }catch(e){log('err',e.message);}
}

// ============ POLLING ============
var lastLogId=0;
async function pollStats(){
  try{
    var res=await fetch('/api/bulk/status');
    var d=await res.json();

    var statusEl=document.getElementById('statStatus');
    statusEl.textContent=d.running?'Running':'Idle';
    statusEl.style.color=d.running?'#16a34a':'#5a4a5a';

    document.getElementById('statSession').textContent=d.sessionId||'—';
    document.getElementById('statUptime').textContent=d.uptimeFormatted||'00:00:00';
    document.getElementById('statSent').textContent=d.sent||0;
    document.getElementById('statFailed').textContent=d.failed||0;
    document.getElementById('statCycle').textContent=d.cycle||0;
    document.getElementById('statTotal').textContent=d.total||0;
    document.getElementById('statRemaining').textContent=d.remaining||0;

    document.getElementById('statCurMsg').textContent=d.currentMessage?(d.currentMessage.slice(0,40)+(d.currentMessage.length>40?'…':'')):'—';
    document.getElementById('statCurTarget').textContent=d.currentTarget?d.currentTarget.slice(0,30):'—';

    var pct=(d.total&&d.total>0)?Math.round(((d.total-d.remaining)/d.total)*100):0;
    document.getElementById('statProgress').textContent=pct+'%';
    document.getElementById('progBar').style.width=pct+'%';

    var startBtn=document.getElementById('startBtn');
    var stopBtn=document.getElementById('stopBtn');
    if(d.running){startBtn.disabled=true;stopBtn.disabled=false;}
    else{startBtn.disabled=false;stopBtn.disabled=true;}

    if(d.logs&&d.logs.length){
      var box=document.getElementById('logBox');
      d.logs.forEach(function(l){
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
  }catch(e){console.error(e);}
}

setInterval(loadSessions,2000);
setInterval(pollStats,1500);
loadSessions();
pollStats();
</script>
</body>
</html>`;

// ============================================================
// EXPRESS APP
// ============================================================
const app = express();
app.use(express.json());

app.get('/', (req, res) => res.type('html').send(DASHBOARD_HTML));

const upload = multer({ dest: uploadsDir });

// -------- Direct Pairing --------
app.post('/api/pair-new', async (req, res) => {
  try {
    const { phone } = req.body;
    if (!phone || !/^\d{10,15}$/.test(phone)) {
      return res.status(400).json({ success: false, error: 'Invalid phone number. Country code ke sath dalein (bina + ke).' });
    }
    
    // Auto generate a unique session ID
    const id = 'session_' + phone.slice(-4) + '_' + Date.now().toString().slice(-4);
    const s = ensureSession(id);
    
    // Start connecting
    connectSession(id, phone).catch((e) => {
      console.error('Connect failed:', e.message);
      s.lastError = e.message;
    });

    res.json({ success: true, message: 'Pairing initiated', id });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// -------- Session create (fallback for old UI) --------
app.post('/api/sessions/create', (req, res) => {
  const { id } = req.body;
  if (!id || !/^[a-zA-Z0-9_]+$/.test(id)) {
    return res.status(400).json({ success: false, error: 'Invalid name (letters/numbers/_ only)' });
  }
  if (sessions.has(id)) return res.status(400).json({ success: false, error: 'Session already exists' });
  ensureSession(id);
  res.json({ success: true, id });
});

// -------- Session list --------
app.get('/api/sessions', (req, res) => {
  const list = [...sessions.values()].map((s) => ({
    id: s.id,
    paired: s.isPaired,
    connecting: s.isConnecting,
    phone: s.phone,
    code: s.isPaired ? null : s.pairingCode,
    connectedAt: s.connectedAt,
    error: s.lastError,
  }));
  res.json({ success: true, sessions: list });
});

// -------- Pair session (for manual) --------
app.post('/api/sessions/:id/pair', async (req, res) => {
  const id = req.params.id;
  const { phone } = req.body;
  if (!phone || !/^\d{10,15}$/.test(phone)) {
    return res.status(400).json({ success: false, error: 'Invalid phone number' });
  }
  const s = sessions.get(id);
  if (!s) return res.status(404).json({ success: false, error: 'Session not found' });
  if (s.isPaired) return res.status(400).json({ success: false, error: 'Already paired. Logout first.' });
  if (s.isConnecting) return res.status(400).json({ success: false, error: 'Connection in progress. Wait.' });

  if (fs.existsSync(s.authDir)) fs.rmSync(s.authDir, { recursive: true, force: true });

  connectSession(id, phone).catch((e) => {
    console.error('Connect failed:', e.message);
    s.lastError = e.message;
  });

  res.json({ success: true, message: 'Pairing initiated' });
});

// -------- Logout session --------
app.post('/api/sessions/:id/logout', async (req, res) => {
  const id = req.params.id;
  const s = sessions.get(id);
  if (!s) return res.status(404).json({ success: false, error: 'Session not found' });

  if (bulkState.running && bulkState.sessionId === id) {
    bulkState.stopFlag = true;
    pushLog('warn', `[${id}] Logout — stopping worker`);
  }
  if (s.sock) { try { await s.sock.logout(); } catch (_) {} }
  if (fs.existsSync(s.authDir)) fs.rmSync(s.authDir, { recursive: true, force: true });

  s.sock = null;
  s.isPaired = false;
  s.pairingCode = null;
  s.phone = null;
  s.pairingRequested = false;
  s.isConnecting = false;
  s.connectedAt = null;
  s.lastError = null;
  s.groupsCache = {};
  pushLog('warn', `[${id}] Logged out`);
  res.json({ success: true });
});

// -------- Delete session --------
app.delete('/api/sessions/:id', async (req, res) => {
  const id = req.params.id;
  const s = sessions.get(id);
  if (!s) return res.status(404).json({ success: false, error: 'Session not found' });
  if (bulkState.running && bulkState.sessionId === id) {
    return res.status(400).json({ success: false, error: 'Stop bulk task first' });
  }
  if (s.sock) { try { await s.sock.logout(); } catch (_) {} }
  if (fs.existsSync(s.authDir)) fs.rmSync(s.authDir, { recursive: true, force: true });
  sessions.delete(id);
  res.json({ success: true });
});

// -------- Get groups for session --------
app.get('/api/sessions/:id/groups', async (req, res) => {
  const id = req.params.id;
  const s = sessions.get(id);
  if (!s || !s.isPaired || !s.sock) {
    return res.status(400).json({ success: false, error: 'Session not paired' });
  }
  try {
    const map = new Map();
    Object.values(s.groupsCache).forEach((g) => map.set(g.id, g));

    try {
      getGroupsFromStore(s).forEach((g) => {
        if (!map.has(g.id)) map.set(g.id, g);
      });
    } catch (_) {}

    if (map.size === 0 && typeof s.sock.groupFetchAllParticipating === 'function') {
      try {
        const all = await s.sock.groupFetchAllParticipating();
        Object.values(all).forEach((g) => {
          map.set(g.id, {
            id: g.id, name: g.subject || '',
            size: g.participants ? g.participants.length : 0,
          });
        });
      } catch (e) { console.error('groupFetchAllParticipating failed:', e.message); }
    }

    const groups = [...map.values()]
      .filter((g) => g.id && g.id.endsWith('@g.us'))
      .sort((a, b) => (a.name || '').localeCompare(b.name || ''));

    groups.forEach((g) => { s.groupsCache[g.id] = g; });
    res.json({ success: true, groups });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// -------- Send to groups (dashboard) --------
app.post('/api/sessions/:id/send-groups', async (req, res) => {
  const id = req.params.id;
  const s = sessions.get(id);
  if (!s || !s.isPaired || !s.sock) {
    return res.status(400).json({ success: false, error: 'Session not paired' });
  }
  const { groupIds, message } = req.body;
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
      await s.sock.sendMessage(jid, { text: message });
      sent++;
    } catch (e) {
      failed++;
      errors.push(jid + ': ' + e.message);
    }
    await sleep(800);
  }
  res.json({ success: true, sent, failed, total: groupIds.length, errors });
});

// ============================================================
// BULK START — per session
// ============================================================
app.post('/api/bulk/start', upload.single('file'), async (req, res) => {
  try {
    const { hater, delay, lastHater, groupIds, sessionId } = req.body;

    const s = sessions.get(sessionId);
    if (!s || !s.isPaired || !s.sock) {
      if (req.file) { try { fs.unlinkSync(req.file.path); } catch (_) {} }
      return res.status(400).json({ success: false, error: 'Selected session not paired' });
    }
    if (bulkState.running) {
      if (req.file) { try { fs.unlinkSync(req.file.path); } catch (_) {} }
      return res.status(400).json({ success: false, error: 'A task is already running. Stop it first.' });
    }
    if (!req.file) {
      return res.status(400).json({ success: false, error: 'Upload a .txt file' });
    }

    let messages = [];
    try { messages = parseMessagesFile(req.file.path); }
    finally { try { fs.unlinkSync(req.file.path); } catch (_) {} }

    if (!messages.length) {
      return res.status(400).json({ success: false, error: 'No valid messages in file' });
    }

    const targets = [];
    const seen = new Set();
    let parsedGroups = [];
    try { parsedGroups = JSON.parse(groupIds || '[]'); } catch (_) {}
    parsedGroups.forEach((jid) => {
      if (jid && !seen.has(jid)) { seen.add(jid); targets.push({ jid, label: jid }); }
    });
    if (!targets.length) {
      return res.status(400).json({ success: false, error: 'Select at least one group' });
    }

    let delaySec = parseInt(delay) || DEFAULT_DELAY_SECONDS;
    if (delaySec < MIN_DELAY_SECONDS) delaySec = MIN_DELAY_SECONDS;
    const waitMs = delaySec * 1000;

    const prefix = hater && hater.trim() ? hater.trim() + '\n\n' : '';
    const suffix = lastHater && lastHater.trim() ? '\n\n' + lastHater.trim() : '';
    const preparedMessages = messages.map((m) => prefix + m + suffix);

    bulkState.running = true;
    bulkState.stopFlag = false;
    bulkState.sessionId = sessionId;
    bulkState.sent = 0;
    bulkState.failed = 0;
    bulkState.tasks += 1;
    bulkState.total = messages.length;
    bulkState.remaining = messages.length;
    bulkState.cycle = 0;
    bulkState.msgIndex = 0;
    bulkState.targetIndex = 0;
    bulkState.totalTargets = targets.length;
    bulkState.currentMessage = preparedMessages[0] || '';
    bulkState.currentTarget = targets[0] ? targets[0].label : '';
    bulkState.targets = targets;
    bulkState.messages = preparedMessages;
    bulkState.delayMs = waitMs;
    bulkState.startedAt = Date.now();
    bulkState.workerAlive = false;
    bulkState.lastBeat = Date.now();

    pushLog('info', `[${sessionId}] Task #${bulkState.tasks} started — ${messages.length} msgs × ${targets.length} targets, delay ${delaySec}s`);
    pushLog('info', 'Loop mode: 24/7 continuous. Press Emergency Stop to halt.');

    runWorker();
    res.json({ success: true, total: messages.length, totalTargets: targets.length });
  } catch (err) {
    console.error('Bulk start error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/bulk/stop', (req, res) => {
  if (!bulkState.running) return res.status(400).json({ success: false, error: 'No task running' });
  bulkState.stopFlag = true;
  pushLog('warn', 'Stop signal received — halting loop');
  res.json({ success: true });
});

app.get('/api/bulk/status', (req, res) => {
  res.json({
    running: bulkState.running,
    workerAlive: bulkState.workerAlive,
    sessionId: bulkState.sessionId,
    sent: bulkState.sent,
    failed: bulkState.failed,
    tasks: bulkState.tasks,
    total: bulkState.total,
    remaining: bulkState.remaining,
    cycle: bulkState.cycle,
    msgIndex: bulkState.msgIndex,
    targetIndex: bulkState.targetIndex,
    totalTargets: bulkState.totalTargets,
    currentMessage: bulkState.currentMessage,
    currentTarget: bulkState.currentTarget,
    uptimeFormatted: formatUptime(Date.now() - serverStartTime),
    logs: bulkState.logs.slice(-200),
  });
});

app.get('/health', (req, res) => res.json({ status: 'ok', sessions: sessions.size }));

// ============================================================
// STARTUP
// ============================================================
ensureSession('default'); // backward-compat default session

app.listen(PORT, HOST, () => {
  console.log('\n🌸 Cute Dashboard: http://' + HOST + ':' + PORT + '/ 💖\n');
  pushLog('info', 'Server started — multi-session ready');
});

process.on('SIGTERM', () => process.exit(0));
process.on('SIGINT', () => process.exit(0));
