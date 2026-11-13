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
const AUTH_BASE = 'auth_info_baileys';

const MIN_DELAY_SECONDS = 3;
const DEFAULT_DELAY_SECONDS = 10;
const SEND_RETRY = 1;
const RETRY_WAIT_MS = 2000;
const WATCHDOG_INTERVAL_MS = 30000;

// MULTIPLE SESSIONS SUPPORT
const sessions = new Map();
const bulkStates = new Map();

function getAuthDir(clientId, phone) {
  const safe = clientId.replace(/[^a-zA-Z0-9]/g, '').slice(0, 10);
  return `${AUTH_BASE}_${safe}_${phone.slice(-4)}`;
}

function newSessionState(id, clientId, phone) {
  return {
    id, clientId, sock: null, pairingCode: null, isPaired: false, phone,
    isConnecting: false, pairingRequested: false, lastError: null, 
    connectedAt: null, groupsCache: {}, authDir: getAuthDir(clientId, phone)
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

function pushLog(bState, type, msg) {
  bState.logs.push({ id: ++bState.logId, ts: Date.now(), type, msg });
  if (bState.logs.length > 500) bState.logs.splice(0, bState.logs.length - 500);
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

function getGroupsFromStore(s) {
  const out = [];
  try {
    if (s && s.sock && s.sock.store && s.sock.store.groupMetadata) {
      const map = s.sock.store.groupMetadata;
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

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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
    if (!t) return;
    if (seen.has(t)) return;
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
  pushLog(bState, 'info', 'Worker started — 24/7 loop active');

  try {
    while (!bState.stopFlag) {
      if (!isSocketReady(clientId)) {
        bState.lastBeat = Date.now();
        await sleep(3000);
        continue;
      }

      for (let mi = 0; mi < bState.messages.length && !bState.stopFlag; mi++) {
        const msg = bState.messages[mi];
        bState.msgIndex = mi;
        bState.currentMessage = msg;

        for (let ti = 0; ti < bState.targets.length && !bState.stopFlag; ti++) {
          const t = bState.targets[ti];
          bState.targetIndex = ti;
          bState.currentTarget = t.label;
          bState.lastBeat = Date.now();

          if (!isSocketReady(clientId)) {
            pushLog(bState, 'warn', 'Socket temporarily unavailable — waiting to resume');
            break;
          }

          try {
            const res = await safeSend(clientId, t.jid, msg);
            if (res.ok) {
              bState.sent++;
              pushLog(bState, 'ok', `Cycle ${bState.cycle + 1} | Msg ${mi + 1}/${bState.messages.length} → ${t.label}`);
            } else {
              bState.failed++;
              pushLog(bState, 'err', `Msg ${mi + 1} → ${t.label}: ${res.error}`);
            }
          } catch (e) {
            bState.failed++;
            pushLog(bState, 'err', `Unexpected send error → ${t.label}: ${e.message}`);
          }

          bState.remaining = bState.messages.length - (mi + 1);

          if (!bState.stopFlag) {
            await sleep(bState.delayMs);
          }
        }

        if (!isSocketReady(clientId) && !bState.stopFlag) break;
      }

      if (!bState.stopFlag) {
        bState.cycle++;
        bState.remaining = bState.messages.length;
        bState.msgIndex = 0;
        bState.targetIndex = 0;
        pushLog(bState, 'info', `Cycle ${bState.cycle} completed — restarting from Message 1`);
      }
    }
  } catch (loopErr) {
    pushLog(bState, 'err', 'Worker crashed: ' + loopErr.message);
    if (!bState.stopFlag) {
      pushLog(bState, 'warn', 'Worker auto-restarting in 3s...');
      bState.workerAlive = false;
      bState.running = true;
      setTimeout(() => { runWorker(clientId); }, 3000);
      return;
    }
  } finally {
    if (bState.stopFlag) {
      bState.running = false;
      bState.stopFlag = false;
      bState.workerAlive = false;
      bState.currentMessage = '';
      bState.currentTarget = '';
      pushLog(bState, 'info', `Task stopped — Sent: ${bState.sent}, Failed: ${bState.failed}, Cycles: ${bState.cycle}`);
    } else {
      bState.workerAlive = false;
    }
  }
}

setInterval(() => {
  for (const [clientId, bState] of bulkStates.entries()) {
    if (bState.running && !bState.workerAlive) {
      pushLog(bState, 'warn', 'Watchdog: worker not alive — restarting');
      runWorker(clientId);
    }
  }
}, WATCHDOG_INTERVAL_MS);

async function connectSession(id, phone, clientId) {
  const s = ensureSession(id, clientId, phone);
  if (s.isPaired) throw new Error('Already paired');
  if (s.isConnecting) throw new Error('Connection already in progress');

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
        s.groupsCache[g.id] = { id: g.id, name: g.subject || '', size: g.participants ? g.participants.length : 0 };
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
      const bState = getBulkState(clientId);

      if (connection === 'connecting' && !s.sock.authState.creds.registered && !s.pairingRequested) {
        s.pairingRequested = true;
        try {
          await sleep(1000);
          s.pairingCode = await s.sock.requestPairingCode(phone);
          console.log(`\n📱 PAIRING CODE for ${phone}: ${s.pairingCode}\n`);
          pushLog(bState, 'info', `Pairing code for ${phone}: ${s.pairingCode}`);
          s.lastError = null;
        } catch (err) {
          console.error('❌ Pairing code error:', err.message);
          s.lastError = err.message;
          s.pairingCode = null;
          s.pairingRequested = false;
          s.isConnecting = false;
        }
      }

      if (connection === 'open') {
        console.log('✅ WhatsApp connected!');
        s.isPaired = true;
        s.pairingCode = null;
        s.pairingRequested = false;
        s.isConnecting = false;
        s.connectedAt = new Date().toISOString();
        s.lastError = null;
        pushLog(bState, 'ok', `WhatsApp connected (${phone})`);

        setTimeout(() => {
          try {
            const groups = getGroupsFromStore(s);
            groups.forEach((g) => { s.groupsCache[g.id] = g; });
            pushLog(bState, 'info', `Loaded ${groups.length} groups into cache`);
          } catch (e) {}
        }, 3000);

        if (bState.running && bState.sessionId === s.id && !bState.workerAlive) {
          pushLog(bState, 'info', 'Reconnect detected — resuming bulk worker');
          runWorker(clientId);
        }
      }

      if (connection === 'close') {
        const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        pushLog(bState, 'warn', `Connection closed (code ${statusCode})`);

        s.isPaired = false;
        s.isConnecting = false;

        if (bState.running && bState.sessionId === s.id) {
          pushLog(bState, 'warn', 'Socket dropped — worker will auto-resume after reconnect');
        }

        if (shouldReconnect && s.phone) {
          s.pairingRequested = false;
          setTimeout(() => connectSession(s.id, s.phone, clientId).catch(console.error), 3000);
        } else if (statusCode === DisconnectReason.loggedOut) {
          s.pairingCode = null;
          s.pairingRequested = false;
          s.phone = null;
          if (bState.running && bState.sessionId === s.id) {
            bState.stopFlag = true;
            pushLog(bState, 'warn', 'Logged out — stopping bulk worker');
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

const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>RK RAJA XWD 🎀💖 — Control Center</title>
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
  body::after{
    content:"";position:fixed;inset:0;z-index:-1;pointer-events:none;
    background-image:
      linear-gradient(115deg, transparent 40%, rgba(255,0,60,.06) 41%, transparent 42%),
      linear-gradient(115deg, transparent 60%, rgba(255,0,60,.05) 61%, transparent 62%),
      repeating-linear-gradient(115deg, rgba(255,255,255,.015) 0 1px, transparent 1px 60px);
    mask-image:radial-gradient(circle at 50% 40%, #000 30%, transparent 90%);
  }
  .streaks{position:fixed;inset:0;z-index:-1;pointer-events:none;overflow:hidden}
  .streaks span{
    position:absolute;height:1px;width:220px;left:-30%;
    background:linear-gradient(90deg,transparent,#ff003c,transparent);
    filter:drop-shadow(0 0 6px #ff003c);opacity:.55;
    animation:streak 7s linear infinite;
  }
  .streaks span:nth-child(2){top:25%;animation-delay:1.5s;animation-duration:9s}
  .streaks span:nth-child(3){top:55%;animation-delay:3s;animation-duration:8s}
  .streaks span:nth-child(4){top:78%;animation-delay:4.5s;animation-duration:10s}
  @keyframes streak{from{transform:translateX(0) rotate(-12deg)}to{transform:translateX(160vw) rotate(-12deg)}}

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
  .tab:hover:not(.active){background:rgba(255,0,60,.12)}

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
    backdrop-filter:blur(16px) saturate(140%);
    -webkit-backdrop-filter:blur(16px) saturate(140%);
    box-shadow:
      0 20px 50px rgba(0,0,0,.55),
      inset 0 1px 0 rgba(255,255,255,.05),
      0 0 32px rgba(255,0,60,.06);
  }
  .card::before{
    content:"";position:absolute;inset:-1px;border-radius:18px;padding:1px;
    background:linear-gradient(135deg, rgba(255,0,60,.55), transparent 40%, transparent 60%, rgba(255,0,60,.35));
    -webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);
    -webkit-mask-composite:xor;mask-composite:exclude;pointer-events:none;opacity:.7;
  }
  .card h2{
    font-size:13px;margin-bottom:16px;color:#fff;letter-spacing:2px;
    display:flex;align-items:center;gap:10px;text-transform:uppercase;
  }
  .card h2 .dot{width:8px;height:8px;border-radius:50%;background:#ff003c;box-shadow:0 0 12px #ff003c}

  label{display:block;font-size:11px;color:#9a9aab;margin-bottom:7px;letter-spacing:1.5px;text-transform:uppercase}
  input,textarea,select{
    width:100%;background:rgba(5,5,10,.75);border:1px solid rgba(255,0,60,.25);
    border-radius:12px;padding:12px 14px;color:#f1f1f6;font-size:14px;
    font-family:inherit;outline:none;transition:.25s;
  }
  input:focus,textarea:focus,select:focus{
    border-color:#ff003c;box-shadow:0 0 0 3px rgba(255,0,60,.14),0 0 22px rgba(255,0,60,.25);
  }
  input:disabled{color:#5c5c6d;cursor:not-allowed}
  textarea{resize:vertical;min-height:96px}
  .field{margin-bottom:14px}

  button{
    background:linear-gradient(180deg,#ff0a45,#c40030);color:#fff;border:none;
    border-radius:12px;padding:13px 24px;font-size:12px;font-weight:700;
    letter-spacing:2px;text-transform:uppercase;cursor:pointer;
    transition:.22s;margin-top:6px;width:100%;
    box-shadow:0 8px 24px rgba(255,0,60,.28),inset 0 1px 0 rgba(255,255,255,.25);
  }
  button:hover:not(:disabled){transform:translateY(-1px);box-shadow:0 12px 30px rgba(255,0,60,.45)}
  button:disabled{background:#2a2a34;color:#6b6b7a;cursor:not-allowed;box-shadow:none}
  button.ghost{background:transparent;border:1px solid rgba(255,0,60,.5);color:#ff5277;box-shadow:none}
  button.ghost:hover:not(:disabled){background:rgba(255,0,60,.1)}
  button.danger{background:linear-gradient(180deg,#ff0040,#a80028);border:1px solid #ff003c}
  .btnrow{display:flex;gap:10px}
  .btnrow button{margin-top:0}

  .status-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;gap:12px;flex-wrap:wrap}
  .badge{display:inline-block;padding:7px 16px;border-radius:999px;font-size:11px;font-weight:700;letter-spacing:1px;text-transform:uppercase}
  .b-idle{background:rgba(120,120,140,.15);color:#a8a8b8;border:1px solid rgba(120,120,140,.35)}
  .b-wait{background:rgba(255,180,0,.12);color:#ffc655;border:1px solid rgba(255,180,0,.4);animation:pulse 1.6s infinite}
  .b-paired{background:rgba(255,0,60,.14);color:#ff5277;border:1px solid rgba(255,0,60,.55);box-shadow:0 0 18px rgba(255,0,60,.25)}
  .b-run{background:rgba(56,239,125,.14);color:#38ef7d;border:1px solid rgba(56,239,125,.55);box-shadow:0 0 18px rgba(56,239,125,.3);animation:pulse 1.6s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.55}}

  .code-box{
    background:rgba(5,5,10,.85);border:2px dashed #ff003c;border-radius:16px;
    padding:26px 20px;margin:18px 0;text-align:center;
    box-shadow:inset 0 0 40px rgba(255,0,60,.12),0 0 30px rgba(255,0,60,.15);
  }
  .code-label{font-size:11px;color:#8b8b9c;letter-spacing:3px;margin-bottom:12px;text-transform:uppercase}
  .code-value{font-size:38px;font-weight:900;letter-spacing:8px;color:#ff003c;
    font-family:'Courier New',monospace;text-shadow:0 0 24px rgba(255,0,60,.7)}
  .code-value.loading{font-size:16px;letter-spacing:0;color:#8b8b9c;font-style:italic;text-shadow:none}

  .steps{background:rgba(5,5,10,.6);border:1px solid rgba(255,0,60,.18);border-radius:12px;
    padding:16px 18px;font-size:13px;line-height:1.95;color:#c3c3d1}
  .steps b{color:#fff}

  .msg{margin-top:14px;padding:12px 15px;border-radius:10px;font-size:13px;display:none;letter-spacing:.4px}
  .msg.ok{background:rgba(255,0,60,.1);color:#ff8ba6;border:1px solid rgba(255,0,60,.4);display:block}
  .msg.err{background:rgba(255,60,60,.1);color:#ffa1a1;border:1px solid rgba(255,60,60,.4);display:block}

  .grid2{display:grid;grid-template-columns:1fr 1fr;gap:14px}
  @media(max-width:520px){.grid2{grid-template-columns:1fr}}
  .stat{background:rgba(5,5,10,.6);border:1px solid rgba(255,0,60,.2);
    border-radius:14px;padding:14px 16px;text-align:center}
  .stat .k{font-size:10px;color:#8b8b9c;letter-spacing:2px;text-transform:uppercase;margin-bottom:6px}
  .stat .v{font-size:22px;font-weight:900;color:#fff;text-shadow:0 0 14px rgba(255,0,60,.35);word-break:break-all}
  .stat .v.red{color:#ff003c}
  .stat .v.green{color:#38ef7d;text-shadow:0 0 14px rgba(56,239,125,.4)}
  .stat .v.small{font-size:14px;font-weight:700}
  .statgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:12px}
  @media(max-width:520px){.statgrid{grid-template-columns:1fr 1fr}}

  .log{background:#000;border:1px solid rgba(255,0,60,.25);border-radius:14px;
    padding:14px;height:340px;overflow-y:auto;font-family:'Courier New',monospace;
    font-size:12px;line-height:1.75;box-shadow:inset 0 0 40px rgba(255,0,60,.08)}
  .log::-webkit-scrollbar{width:8px}
  .log::-webkit-scrollbar-track{background:#0a0a0f}
  .log::-webkit-scrollbar-thumb{background:#ff003c;border-radius:8px}
  .log .line{color:#9a9aab;border-bottom:1px dashed rgba(255,255,255,.04);padding:2px 0;word-break:break-all}
  .log .t{color:#ff5277;margin-right:8px}
  .log .ok{color:#38ef7d}
  .log .err{color:#ff6b6b}
  .log .info{color:#6bc6ff}
  .log .warn{color:#ffc655}

  .progress{height:8px;background:rgba(5,5,10,.8);border-radius:99px;overflow:hidden;
    border:1px solid rgba(255,0,60,.25);margin-top:12px}
  .progress > div{height:100%;width:0%;
    background:linear-gradient(90deg,#ff003c,#ff5277,#ff003c);
    background-size:200% 100%;transition:width .4s ease;animation:shine 2s linear infinite}
  @keyframes shine{0%{background-position:0 0}100%{background-position:200% 0}}

  .groups-toolbar{display:flex;gap:10px;align-items:center;margin-bottom:12px;flex-wrap:wrap}
  .groups-toolbar button{width:auto;margin-top:0;padding:10px 16px;font-size:11px}
  .groups-toolbar .count{font-size:12px;color:#8b8b9c;margin-left:auto;letter-spacing:1px}
  .groups-list{
    background:rgba(5,5,10,.6);border:1px solid rgba(255,0,60,.2);border-radius:14px;
    max-height:360px;overflow-y:auto;padding:8px;
  }
  .groups-list::-webkit-scrollbar{width:8px}
  .groups-list::-webkit-scrollbar-track{background:#0a0a0f}
  .groups-list::-webkit-scrollbar-thumb{background:#ff003c;border-radius:8px}
  .group-item{
    display:flex;align-items:center;gap:12px;padding:10px 12px;border-radius:10px;
    cursor:pointer;transition:.15s;border:1px solid transparent;
  }
  .group-item:hover{background:rgba(255,0,60,.08);border-color:rgba(255,0,60,.25)}
  .group-item.selected{background:rgba(255,0,60,.14);border-color:#ff003c}
  .group-item input[type=checkbox]{
    width:18px;height:18px;accent-color:#ff003c;cursor:pointer;flex-shrink:0;
  }
  .group-info{flex:1;min-width:0}
  .group-name{color:#f1f1f6;font-size:13px;font-weight:600;
    white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .group-jid{color:#6b6b7a;font-size:11px;font-family:'Courier New',monospace;
    white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-top:2px}
  .group-meta{color:#ff5277;font-size:10px;letter-spacing:1px;text-transform:uppercase;flex-shrink:0}

  .hidden{display:none!important}
  .hint{font-size:11px;color:#6b6b7a;margin-top:8px;letter-spacing:.5px}
  .empty{padding:30px 20px;text-align:center;color:#6b6b7a;font-size:13px}
  .footer{text-align:center;color:#4b4b5a;font-size:11px;letter-spacing:3px;margin-top:30px;text-transform:uppercase}

  .queue-box{
    background:rgba(5,5,10,.7);border:1px solid rgba(255,0,60,.25);border-radius:12px;
    padding:12px 14px;margin-top:8px;font-size:12px;color:#c3c3d1;
    max-height:140px;overflow-y:auto;font-family:'Courier New',monospace;line-height:1.6;
  }
  .queue-box .qi{color:#6bc6ff;margin-right:8px}
</style>
</head>
<body>
<div class="streaks"><span></span><span></span><span></span><span></span></div>

<div class="container">
  <div class="brand">
    <h1>RK RAJA <span class="x">X</span>WD 🎀💖</h1>
    <p>WhatsApp Control Center</p>
  </div>

  <div class="tabs">
    <div class="tab active" data-tab="dashboard" onclick="switchTab('dashboard')">Dashboard</div>
    <div class="tab" data-tab="sender" onclick="switchTab('sender')">Bulk Sender</div>
  </div>

  <div class="panel active" id="panel-dashboard">
    <div class="card">
      <div class="status-row">
        <h2 style="margin:0"><span class="dot"></span>Your Sessions</h2>
      </div>
      <div id="sessionsList" class="grid2" style="grid-template-columns:1fr; gap:12px;">
        <div class="empty" style="grid-column:1/-1;">Loading sessions...</div>
      </div>
    </div>

    <div class="card" id="pairCard">
      <h2><span class="dot"></span>Pair WhatsApp</h2>
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
      
      <div class="field">
        <label>Select Session</label>
        <select id="groupsSessionSelect" onchange="onGroupsSessionChange()">
          <option value="">-- Select Session --</option>
        </select>
      </div>

      <div class="groups-toolbar">
        <button class="ghost" id="refreshGroupsBtn" onclick="fetchGroups()">Refresh Groups</button>
        <button class="ghost" onclick="selectAllGroups()">Select All</button>
        <button class="ghost" onclick="clearGroupSelection()">Clear</button>
        <span class="count" id="groupsCount">0 groups</span>
      </div>
      <div class="groups-list" id="groupsList">
        <div class="empty">Select session & click "Refresh Groups"</div>
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
      <p class="hint">Logout deletes auth — you'll need to pair again.</p>
      <button class="ghost" onclick="logoutAll()">Logout All / Reset</button>
    </div>
  </div>

  <div class="panel" id="panel-sender">
    <div class="layout">
      <div>
        <div class="card">
          <h2><span class="dot"></span>Select Session</h2>
          <div class="field">
            <label>Session</label>
            <select id="sessionSelect">
              <option value="">-- Select Session --</option>
            </select>
          </div>
          <div class="hint" id="sessionHint">Select a paired session for bulk sending.</div>
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

  <div class="footer">RK RAJA XWD © — Secure Session</div>
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
function escapeHtml(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function clearLog(){document.getElementById('logBox').innerHTML='';}
function log(type,msg){
  const box=document.getElementById('logBox');
  const t=new Date().toLocaleTimeString();
  const el=document.createElement('div');
  el.className='line '+type;
  el.innerHTML='<span class="t">['+t+']</span>'+escapeHtml(msg);
  box.appendChild(el);
  box.scrollTop=box.scrollHeight;
  while(box.children.length>500) box.removeChild(box.firstChild);
}

async function startPair(){
  const phone=document.getElementById('phoneInput').value.trim();
  const btn=document.getElementById('pairBtn');
  const msgEl=document.getElementById('pairMsg');
  const codeSection=document.getElementById('codeSection');
  if(!phone||!/^\\d{10,15}$/.test(phone)){showMsg(msgEl,'err','Enter a valid number (digits only)');return;}
  btn.disabled=true;btn.textContent='Starting...';showMsg(msgEl,'','');
  try{
    const res=await apiFetch('/api/pair',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({phone})});
    const data=await res.json();
    if(!data.success)throw new Error(data.error||'Failed');
    codeSection.classList.remove('hidden');
    document.getElementById('codeValue').textContent='Generating...';
    document.getElementById('codeValue').classList.add('loading');
    showMsg(msgEl,'ok','Pairing started — code appears shortly');
    pollStatus();
  }catch(e){showMsg(msgEl,'err',e.message);}
  finally{btn.disabled=false;btn.textContent='Get Pairing Code';}
}

async function logoutAll(){
  if(!confirm('Logout all sessions? Auth will be deleted.'))return;
  try{await apiFetch('/api/logout-all',{method:'POST'});location.reload();}catch(e){alert(e.message);}
}
async function logoutSession(id){
  if(!confirm('Logout this session?'))return;
  try{await apiFetch('/api/sessions/'+id+'/logout',{method:'POST'});pollStatus();}catch(e){alert(e.message);}
}
async function deleteSession(id){
  if(!confirm('Delete this session completely?'))return;
  try{await apiFetch('/api/sessions/'+id,{method:'DELETE'});pollStatus();}catch(e){alert(e.message);}
}

let allGroups=[];
let dashSelected=new Set();
let bulkSelected=new Set();

function onGroupsSessionChange(){
  dashSelected.clear(); allGroups=[];
  document.getElementById('groupsList').innerHTML='<div class="empty">Click Refresh to load groups</div>';
  document.getElementById('groupsCount').textContent='0 groups';
}

async function fetchGroups(){
  const sessionId = document.getElementById('groupsSessionSelect').value;
  if(!sessionId){alert('Select a session first');return;}
  const btn=document.getElementById('refreshGroupsBtn');
  const listEl=document.getElementById('groupsList');
  btn.disabled=true;btn.textContent='Fetching...';
  listEl.innerHTML='<div class="empty">Loading groups...</div>';
  try{
    const res=await apiFetch('/api/sessions/'+sessionId+'/groups');
    const d=await res.json();
    if(!d.success) throw new Error(d.error||'Failed to fetch');
    allGroups = d.groups || [];
    renderDashGroups();
    renderBulkGroups();
    log('info', 'Fetched '+allGroups.length+' groups');
  }catch(e){
    listEl.innerHTML='<div class="empty" style="color:#ff6b6b">'+escapeHtml(e.message)+'</div>';
    log('err','Fetch groups: '+e.message);
  }finally{
    btn.disabled=false;btn.textContent='Refresh Groups';
  }
}
function renderDashGroups(){
  const listEl=document.getElementById('groupsList');
  document.getElementById('groupsCount').textContent=allGroups.length+' groups';
  if(!allGroups.length){
    listEl.innerHTML='<div class="empty">No groups found for this WhatsApp account</div>';
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
      '<div class="group-meta">'+(g.size?g.size+' members':'')+'</div>'+
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
  const sessionId = document.getElementById('groupsSessionSelect').value;
  if(!sessionId){showMsg(document.getElementById('groupSendMsg'),'err','Select a session first');return;}
  const msg=document.getElementById('groupMsgInput').value.trim();
  const box=document.getElementById('groupSendMsg');
  const btn=document.getElementById('groupSendBtn');
  if(!dashSelected.size){showMsg(box,'err','Select at least one group');return;}
  if(!msg){showMsg(box,'err','Enter a message');return;}
  btn.disabled=true;btn.textContent='Sending...';showMsg(box,'','');
  try{
    const res=await apiFetch('/api/sessions/'+sessionId+'/send-groups',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({groupIds:[...dashSelected], message:msg})
    });
    const d=await res.json();
    if(!d.success) throw new Error(d.error||'Failed');
    showMsg(box,'ok','Sent to '+d.sent+'/'+d.total+' groups'+(d.failed?' ('+d.failed+' failed)':''));
    log('ok','Group send: '+d.sent+'/'+d.total+' success');
    if(d.errors&&d.errors.length) d.errors.forEach(e=>log('err',e));
  }catch(e){showMsg(box,'err',e.message);log('err','Group send: '+e.message);}
  finally{btn.disabled=false;btn.textContent='Send to Selected Groups';}
}

async function fetchGroupsForBulk(){
  if(allGroups.length){ renderBulkGroups(); return; }
  const sessionId = document.getElementById('sessionSelect').value;
  if(!sessionId){alert('Select a session first');return;}
  try{
    const res=await apiFetch('/api/sessions/'+sessionId+'/groups');
    const d=await res.json();
    if(!d.success) throw new Error(d.error||'Failed to fetch');
    allGroups = d.groups || [];
    renderBulkGroups();
    log('info', 'Fetched '+allGroups.length+' groups');
  }catch(e){
    alert(e.message);
    log('err','Fetch groups: '+e.message);
  }
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
      '<div class="group-meta">'+(g.size?g.size+' members':'')+'</div>'+
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
  const sessionId=document.getElementById('sessionSelect').value;
  if(!sessionId){alert('Select a session first');return;}
  const file=document.getElementById('fileInput').files[0];
  const hater=document.getElementById('haterInput').value.trim();
  let delay=parseInt(document.getElementById('delayInput').value)||10;
  if(delay<3) delay=3;
  const lastHater=document.getElementById('lastHaterInput').value.trim();

  if(!file){alert('Please upload a .txt file — each line will be a separate message');return;}
  if(bulkSelected.size===0){alert('Please select at least one group as target');return;}

  const startBtn=document.getElementById('startBtn');
  const stopBtn=document.getElementById('stopBtn');
  startBtn.disabled=true;startBtn.textContent='Starting...';

  const fd=new FormData();
  fd.append('file',file);
  fd.append('hater',hater);
  fd.append('delay',String(delay));
  fd.append('lastHater',lastHater);
  fd.append('groupIds',JSON.stringify([...bulkSelected]));
  fd.append('sessionId',sessionId);

  log('info','Starting bulk task...');
  try{
    const res=await apiFetch('/api/bulk/start',{method:'POST',body:fd});
    const d=await res.json();
    if(!d.success)throw new Error(d.error||'Failed');
    log('ok','Task started — '+d.total+' messages in queue, '+d.totalTargets+' targets');
    stopBtn.disabled=false;
  }catch(e){
    log('err','Start failed: '+e.message);
    alert('Start failed: '+e.message);
  }finally{
    startBtn.disabled=false;startBtn.textContent='Start Server';
  }
}
async function stopTask(){
  try{
    const res=await apiFetch('/api/bulk/stop',{method:'POST'});
    const d=await res.json();
    if(d.success) log('warn','Stop requested — task will halt immediately');
    else log('err',d.error||'No task running');
  }catch(e){log('err',e.message);}
}

let currentSessions = [];
async function pollStatus(){
  try{
    const res=await apiFetch('/api/sessions');
    const d=await res.json();
    currentSessions = d.sessions || [];

    // Render Sessions List
    const listEl=document.getElementById('sessionsList');
    if(!currentSessions.length){
      listEl.innerHTML = '<div class="empty" style="grid-column:1/-1;">No sessions found. Add one below.</div>';
    } else {
      listEl.innerHTML = currentSessions.map(s=>{
        const badge = s.paired 
          ? '<span class="badge b-paired" style="font-size:9px;padding:4px 10px;">Paired</span>' 
          : (s.code ? '<span class="badge b-wait" style="font-size:9px;padding:4px 10px;">Waiting</span>' : '<span class="badge b-idle" style="font-size:9px;padding:4px 10px;">Connecting</span>');
        
        const logoutBtn = s.paired ? `<button class="ghost" style="width:auto; padding:6px 12px; font-size:10px; margin:0;" onclick="logoutSession('${s.id}')">Logout</button>` : '';
        const deleteBtn = `<button class="danger" style="width:auto; padding:6px 12px; font-size:10px; margin:0;" onclick="deleteSession('${s.id}')">Delete</button>`;

        return `
          <div class="stat" style="display:flex; justify-content:space-between; align-items:center; text-align:left;">
            <div>
              <div class="v small" style="font-size:14px; margin-bottom:4px; text-shadow:none;">${s.phone || s.id}</div>
              <div style="font-size:11px; color:#8b8b9c;">${s.code ? 'Code: '+s.code : (s.connectedAt ? new Date(s.connectedAt).toLocaleString() : 'Connecting...')}</div>
            </div>
            <div style="display:flex; flex-direction:column; gap:6px; align-items:flex-end;">
              ${badge}
              <div style="display:flex; gap:6px;">
                ${logoutBtn}
                ${deleteBtn}
              </div>
            </div>
          </div>
        `;
      }).join('');
    }

    // Update Dropdowns
    const paired = currentSessions.filter(s => s.paired);
    const opts = '<option value="">-- Select Session --</option>' + paired.map(s => `<option value="${s.id}">${s.phone}</option>`).join('');
    
    const sel1 = document.getElementById('sessionSelect');
    const sel2 = document.getElementById('groupsSessionSelect');
    const v1 = sel1.value, v2 = sel2.value;
    if (sel1.innerHTML !== opts) { sel1.innerHTML = opts; if (paired.some(s => s.id === v1)) sel1.value = v1; }
    if (sel2.innerHTML !== opts) { sel2.innerHTML = opts; if (paired.some(s => s.id === v2)) sel2.value = v2; }

    // Pairing UI Update
    const anyConnecting = currentSessions.find(s => s.connecting || s.code);
    if (anyConnecting) {
      document.getElementById('pairCard').classList.remove('hidden');
      document.getElementById('phoneInput').disabled = true;
      document.getElementById('pairBtn').disabled = true;
      if (anyConnecting.code) {
        document.getElementById('codeSection').classList.remove('hidden');
        document.getElementById('codeValue').textContent = anyConnecting.code;
        document.getElementById('codeValue').classList.remove('loading');
      } else {
        document.getElementById('codeSection').classList.remove('hidden');
        document.getElementById('codeValue').textContent = 'Generating...';
        document.getElementById('codeValue').classList.add('loading');
      }
    } else {
      document.getElementById('phoneInput').disabled = false;
      document.getElementById('pairBtn').disabled = false;
      document.getElementById('codeSection').classList.add('hidden');
    }
  }catch(e){console.error(e);}
}

let lastLogId=0;
async function pollStats(){
  try{
    const res=await apiFetch('/api/bulk/status');
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

    document.getElementById('statCurMsg').textContent=d.currentMessage?(d.currentMessage.slice(0,40)+(d.currentMessage.length>40?'…':'')):'—';
    document.getElementById('statCurTarget').textContent=d.currentTarget?d.currentTarget.slice(0,30):'—';

    const pct=(d.total&&d.total>0)?Math.round(((d.total-d.remaining)/d.total)*100):0;
    document.getElementById('statProgress').textContent=pct+'%';
    document.getElementById('progBar').style.width=pct+'%';

    const startBtn=document.getElementById('startBtn');
    const stopBtn=document.getElementById('stopBtn');
    if(d.running){startBtn.disabled=true;stopBtn.disabled=false;}
    else{startBtn.disabled=false;stopBtn.disabled=true;}

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
      lastLogId=d.logs[d.logs.length-1].id;
      box.scrollTop=box.scrollHeight;
      while(box.children.length>500) box.removeChild(box.firstChild);
    }
  }catch(e){console.error(e);}
}

setInterval(pollStatus,2000);
setInterval(pollStats,1500);
pollStatus();
pollStats();
</script>
</body>
</html>`;

const app = express();
app.use(express.json());

app.get('/', (req, res) => res.type('html').send(DASHBOARD_HTML));

const upload = multer({ dest: 'uploads/' });

function getClientId(req) {
  return req.headers['x-client-id'] || 'default_client';
}

app.post('/api/pair', async (req, res) => {
  try {
    const clientId = getClientId(req);
    const { phone } = req.body;
    if (!phone || !/^\d{10,15}$/.test(phone)) {
      return res.status(400).json({ success: false, error: 'Invalid phone number' });
    }
    
    // Check if this user already has this number paired
    for (let s of sessions.values()) {
      if (s.clientId === clientId && s.phone === phone && s.isPaired) {
        return res.status(400).json({ success: false, error: 'Already paired!' });
      }
    }

    const id = 'session_' + phone.slice(-4) + '_' + Date.now().toString().slice(-4);
    ensureSession(id, clientId, phone);
    
    connectSession(id, phone, clientId).catch((e) => {
      console.error('Connect failed:', e.message);
      const s = sessions.get(id);
      if (s) s.lastError = e.message;
    });

    res.json({ success: true, message: 'Pairing initiated', id });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/sessions', (req, res) => {
  const clientId = getClientId(req);
  const list = [...sessions.values()]
    .filter(s => s.clientId === clientId)
    .map(s => ({
      id: s.id,
      paired: s.isPaired,
      connecting: s.isConnecting,
      code: s.isPaired ? null : s.pairingCode,
      phone: s.phone,
      connectedAt: s.connectedAt,
      error: s.lastError,
    }));
  res.json({ success: true, sessions: list });
});

app.post('/api/logout-all', async (req, res) => {
  try {
    const clientId = getClientId(req);
    const bState = getBulkState(clientId);
    if (bState.running) bState.stopFlag = true;

    for (let [id, s] of sessions.entries()) {
      if (s.clientId === clientId) {
        if (s.sock) { try { await s.sock.logout(); } catch (_) {} }
        if (fs.existsSync(s.authDir)) fs.rmSync(s.authDir, { recursive: true, force: true });
        sessions.delete(id);
      }
    }
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/sessions/:id/logout', async (req, res) => {
  try {
    const clientId = getClientId(req);
    const s = sessions.get(req.params.id);
    
    if (!s || s.clientId !== clientId) return res.status(404).json({ success: false, error: 'Session not found' });
    
    const bState = getBulkState(clientId);
    if (bState.running && bState.sessionId === s.id) bState.stopFlag = true;
    
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
    
    if (!s || s.clientId !== clientId) return res.status(404).json({ success: false, error: 'Session not found' });
    
    const bState = getBulkState(clientId);
    if (bState.running && bState.sessionId === s.id) bState.stopFlag = true;

    if (s.sock) { try { await s.sock.logout(); } catch (_) {} }
    if (fs.existsSync(s.authDir)) fs.rmSync(s.authDir, { recursive: true, force: true });
    
    sessions.delete(s.id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/sessions/:id/groups', async (req, res) => {
  try {
    const clientId = getClientId(req);
    const s = sessions.get(req.params.id);
    
    if (!s || !s.isPaired || !s.sock) return res.status(400).json({ success: false, error: 'WhatsApp not paired yet' });
    if (s.clientId !== clientId) return res.status(403).json({ success: false, error: 'Unauthorized' });

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

    groups.forEach((g) => { s.groupsCache[g.id] = g; });

    res.json({ success: true, groups });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/sessions/:id/send-groups', async (req, res) => {
  try {
    const clientId = getClientId(req);
    const s = sessions.get(req.params.id);
    
    if (!s || !s.isPaired || !s.sock) return res.status(400).json({ success: false, error: 'WhatsApp not paired yet' });
    if (s.clientId !== clientId) return res.status(403).json({ success: false, error: 'Unauthorized' });

    const { groupIds, message } = req.body;
    if (!Array.isArray(groupIds) || !groupIds.length) return res.status(400).json({ success: false, error: 'No groups selected' });
    if (!message || !message.trim()) return res.status(400).json({ success: false, error: 'Message is required' });

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
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/bulk/start', upload.single('file'), async (req, res) => {
  try {
    const clientId = getClientId(req);
    const { hater, delay, lastHater, groupIds, sessionId } = req.body;
    
    const s = sessions.get(sessionId);
    if (!s || !s.isPaired || !s.sock) {
      if (req.file) { try { fs.unlinkSync(req.file.path); } catch (_) {} }
      return res.status(400).json({ success: false, error: 'WhatsApp not paired yet.' });
    }
    if (s.clientId !== clientId) {
      if (req.file) { try { fs.unlinkSync(req.file.path); } catch (_) {} }
      return res.status(403).json({ success: false, error: 'Unauthorized' });
    }

    const bState = getBulkState(clientId);
    if (bState.running) {
      if (req.file) { try { fs.unlinkSync(req.file.path); } catch (_) {} }
      return res.status(400).json({ success: false, error: 'A task is already running. Please STOP it first.' });
    }
    if (!req.file) {
      return res.status(400).json({ success: false, error: 'Please upload a .txt file (each line = one message)' });
    }

    let messages = [];
    try {
      messages = parseMessagesFile(req.file.path);
    } finally {
      try { fs.unlinkSync(req.file.path); } catch (_) {}
    }

    if (!messages.length) {
      return res.status(400).json({ success: false, error: 'No valid messages found (all empty/duplicate)' });
    }

    const targets = [];
    const seen = new Set();
    let parsedGroups = [];
    try { parsedGroups = JSON.parse(groupIds || '[]'); } catch (_) {}
    parsedGroups.forEach((jid) => {
      if (jid && !seen.has(jid)) { seen.add(jid); targets.push({ jid, label: jid }); }
    });

    if (!targets.length) {
      return res.status(400).json({ success: false, error: 'No targets — select at least one group' });
    }

    let delaySec = parseInt(delay) || DEFAULT_DELAY_SECONDS;
    if (delaySec < MIN_DELAY_SECONDS) delaySec = MIN_DELAY_SECONDS;
    const waitMs = delaySec * 1000;

    const prefix = hater && hater.trim() ? hater.trim() + '\n\n' : '';
    const suffix = lastHater && lastHater.trim() ? '\n\n' + lastHater.trim() : '';
    const preparedMessages = messages.map((m) => prefix + m + suffix);

    bState.running = true;
    bState.stopFlag = false;
    bState.sessionId = sessionId;
    bState.sent = 0;
    bState.failed = 0;
    bState.tasks += 1;
    bState.total = messages.length;
    bState.remaining = messages.length;
    bState.cycle = 0;
    bState.msgIndex = 0;
    bState.targetIndex = 0;
    bState.totalTargets = targets.length;
    bState.currentMessage = preparedMessages[0] || '';
    bState.currentTarget = targets[0] ? targets[0].label : '';
    bState.targets = targets;
    bState.messages = preparedMessages;
    bState.delayMs = waitMs;
    bState.startedAt = Date.now();
    bState.workerAlive = false;
    bState.lastBeat = Date.now();

    pushLog(bState, 'info', `Task #${bState.tasks} started — ${messages.length} messages × ${targets.length} targets, delay ${delaySec}s`);
    pushLog(bState, 'info', 'Loop mode: 24/7 continuous. Press Emergency Stop to halt.');

    runWorker(clientId);
