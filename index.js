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
const multer = require('multer');

const PORT = process.env.PORT || 25029;
const HOST = '0.0.0.0';
const AUTH_DIR = 'auth_info_baileys';

const MIN_DELAY_SECONDS = 3;
const DEFAULT_DELAY_SECONDS = 10;
const SEND_RETRY = 2;
const RETRY_WAIT_MS = 2000;
const WATCHDOG_INTERVAL_MS = 10000; // Har 10 second me check karega aur loop ko zinda rakhega

let sock = null;
let pairingCode = null;
let isPaired = false;
let currentPhone = null;
let pairingRequested = false;
let isConnecting = false;
let lastError = null;
let connectedAt = null;
let serverStartTime = Date.now();

let groupsCache = {};

const bulkState = {
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

function getGroupsFromStore() {
  const out = [];
  try {
    if (sock && sock.store && sock.store.groupMetadata) {
      const map = sock.store.groupMetadata;
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

function isSocketReady() {
  return !!(sock && isPaired);
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

async function safeSend(jid, text) {
  let lastErr = null;
  for (let attempt = 0; attempt <= SEND_RETRY; attempt++) {
    try {
      if (!isSocketReady()) throw new Error('socket not ready');
      await sock.sendMessage(jid, { text });
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

// ===== 24/7 UNSTOPPABLE BACKGROUND WORKER LOOP =====
async function runWorker() {
  if (bulkState.workerAlive) return;
  bulkState.workerAlive = true;
  pushLog('info', 'Background Live Worker started — 24/7 continuous mode active');

  try {
    while (!bulkState.stopFlag) {
      bulkState.lastBeat = Date.now();

      // Agar socket ready nahi hai, toh task band nahi hoga balki wait karega jab tak reconnect na ho jaye
      if (!isSocketReady()) {
        await sleep(3000);
        continue;
      }

      if (!bulkState.messages.length || !bulkState.targets.length) {
        await sleep(2000);
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
            pushLog('warn', 'Socket temporary drop detected — pausing briefly until reconnection...');
            while (!isSocketReady() && !bulkState.stopFlag) {
              await sleep(3000);
            }
          }

          if (bulkState.stopFlag) break;

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

          if (!bulkState.stopFlag) {
            await sleep(bulkState.delayMs);
          }
        }
        if (bulkState.stopFlag) break;
      }

      if (!bulkState.stopFlag) {
        bulkState.cycle++;
        bulkState.remaining = bulkState.messages.length;
        bulkState.msgIndex = 0;
        bulkState.targetIndex = 0;
        pushLog('info', `Cycle ${bulkState.cycle} finished successfully — looping back to Message 1 automatically.`);
      }
    }
  } catch (loopErr) {
    pushLog('err', 'Worker unexpected exception: ' + loopErr.message);
  } finally {
    bulkState.workerAlive = false;
    if (bulkState.stopFlag) {
      bulkState.running = false;
      bulkState.stopFlag = false;
      bulkState.currentMessage = '';
      bulkState.currentTarget = '';
      pushLog('warn', `Task explicitly stopped by user. Total Sent: ${bulkState.sent}, Failed: ${bulkState.failed}`);
    } else if (bulkState.running) {
      // Agar user ne stop nahi kiya aur koi error se loop ruka, toh auto-restart karega turant
      pushLog('warn', 'Worker heartbeat restarting loop instantly...');
      setTimeout(() => runWorker(), 1000);
    }
  }
}

// Watchdog jo ensure karega ki jab tak running flag true hai, background worker hamesha zinda rahe
setInterval(() => {
  if (bulkState.running && (!bulkState.workerAlive || Date.now() - bulkState.lastBeat > 20000)) {
    pushLog('warn', 'Watchdog triggered: reviving sleeping background worker...');
    bulkState.workerAlive = false;
    runWorker();
  }
}, WATCHDOG_INTERVAL_MS);

async function connectToWhatsApp(phone) {
  if (!phone) throw new Error('Phone number required');
  if (isConnecting) throw new Error('Connection already in progress');
  if (isPaired) throw new Error('Already paired');

  isConnecting = true;
  currentPhone = phone;
  pairingCode = null;
  lastError = null;

  try {
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
    const { version } = await fetchLatestBaileysVersion();

    sock = makeWASocket({
      version,
      auth: state,
      printQRInTerminal: false,
      logger: pino({ level: 'silent' }),
      browser: ['Ubuntu', 'Chrome', '20.0.04'],
      mobile: false,
      syncFullHistory: false,
      markOnlineOnConnect: true,
      connectTimeoutMs: 60000,
      defaultQueryTimeoutMs: 60000,
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('groups.upsert', (groups) => {
      groups.forEach((g) => {
        groupsCache[g.id] = {
          id: g.id,
          name: g.subject || '',
          size: g.participants ? g.participants.length : 0,
        };
      });
    });

    sock.ev.on('groups.update', (updates) => {
      updates.forEach((u) => {
        if (groupsCache[u.id]) {
          if (u.subject) groupsCache[u.id].name = u.subject;
        } else if (u.id) {
          groupsCache[u.id] = { id: u.id, name: u.subject || '', size: 0 };
        }
      });
    });

    sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect } = update;

      if (connection === 'connecting' && !sock.authState.creds.registered && !pairingRequested) {
        pairingRequested = true;
        try {
          await sleep(4000);
          pairingCode = await sock.requestPairingCode(phone);
          console.log(`\n📱 PAIRING CODE for ${phone}: ${pairingCode}\n`);
          pushLog('info', `Pairing code generated: ${pairingCode}`);
          lastError = null;
        } catch (err) {
          console.error('❌ Pairing code error:', err.message);
          lastError = err.message;
          pairingCode = null;
          pairingRequested = false;
          isConnecting = false;
        }
      }

      if (connection === 'open') {
        console.log('✅ WhatsApp connected successfully!');
        isPaired = true;
        pairingCode = null;
        pairingRequested = false;
        isConnecting = false;
        connectedAt = new Date().toISOString();
        lastError = null;
        pushLog('ok', `WhatsApp connected (${phone})`);

        setTimeout(() => {
          try {
            const groups = getGroupsFromStore();
            groups.forEach((g) => { groupsCache[g.id] = g; });
            pushLog('info', `Loaded ${groups.length} groups into cache`);
          } catch (e) {}
        }, 3000);

        if (bulkState.running && !bulkState.workerAlive) {
          pushLog('info', 'Socket reopened — resuming background loop');
          runWorker();
        }
      }

      if (connection === 'close') {
        const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        pushLog('warn', `Connection closed (code ${statusCode})`);

        isPaired = false;
        isConnecting = false;

        if (shouldReconnect && currentPhone) {
          pairingRequested = false;
          setTimeout(() => connectToWhatsApp(currentPhone).catch(() => {}), 3000);
        } else if (statusCode === DisconnectReason.loggedOut) {
          pairingCode = null;
          pairingRequested = false;
          currentPhone = null;
          if (bulkState.running) {
            bulkState.stopFlag = true;
          }
        }
      }
    });
  } catch (err) {
    isConnecting = false;
    lastError = err.message;
    throw err;
  }
}

// ===== NEW LIVE ANIMATED CYBERPUNK THEME HTML =====
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>RK RAJA XWD — Live Cyberpunk Control Center</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@500;700;900&family=Rajdhani:wght@500;600;700&display=swap');
  
  *{box-sizing:border-box;margin:0;padding:0}
  html,body{height:100%}
  body{
    font-family:'Rajdhani',sans-serif;
    background:#030305;color:#e2e8f0;min-height:100vh;overflow-x:hidden;position:relative;
  }

  /* Live Animated Neon Background Theme */
  .live-bg{
    position:fixed;inset:0;z-index:-3;pointer-events:none;background:#030305;
    overflow:hidden;
  }
  .live-bg::before{
    content:"";position:absolute;top:-50%;left:-50%;width:200%;height:200%;
    background:radial-gradient(circle at 50% 50%, rgba(255,0,60,0.12) 0%, rgba(13,2,33,0.8) 45%, transparent 70%);
    animation:rotateGlow 15s linear infinite;
  }
  @keyframes rotateGlow{0%{transform:rotate(0deg)}100%{transform:rotate(360deg)}}

  .cyber-grid{
    position:fixed;inset:0;z-index:-2;pointer-events:none;
    background-image:linear-gradient(rgba(255,0,60,0.04) 1px, transparent 1px),
                     linear-gradient(90deg, rgba(255,0,60,0.04) 1px, transparent 1px);
    background-size:40px 40px;
    animation:gridMove 20s linear infinite;
  }
  @keyframes gridMove{0%{background-position:0 0}100%{background-position:40px 40px}}

  .floating-orbs{position:fixed;inset:0;z-index:-1;pointer-events:none;overflow:hidden}
  .orb{
    position:absolute;border-radius:50%;filter:blur(80px);opacity:0.35;
    animation:orbFloat 10s ease-in-out infinite alternate;
  }
  .orb:nth-child(1){top:10%;left:15%;width:300px;height:300px;background:#ff003c;animation-delay:0s}
  .orb:nth-child(2){bottom:10%;right:15%;width:350px;height:350px;background:#7928ca;animation-delay:3s}
  @keyframes orbFloat{0%{transform:translate(0,0) scale(1)}100%{transform:translate(40px,50px) scale(1.15)}}

  .container{max-width:1180px;margin:0 auto;padding:28px 18px 60px;position:relative;z-index:1}

  .brand{text-align:center;margin-bottom:26px}
  .brand h1{
    font-size:clamp(24px,4.5vw,42px);font-weight:900;letter-spacing:4px;
    background:linear-gradient(180deg,#ffffff 0%,#e0e0f0 40%,#ff003c 120%);
    -webkit-background-clip:text;background-clip:text;color:transparent;
    text-shadow:0 0 30px rgba(255,0,60,0.5);
    font-family:'Orbitron',sans-serif;
  }
  .brand h1 .x{color:#ff003c;-webkit-text-fill-color:#ff003c;text-shadow:0 0 20px #ff003c}
  .brand p{color:#a0a0b5;font-size:12px;letter-spacing:4px;margin-top:6px;text-transform:uppercase;font-weight:600}

  .tabs{display:flex;gap:12px;margin-bottom:24px;flex-wrap:wrap;justify-content:center}
  .tab{
    padding:12px 24px;border-radius:12px;font-size:13px;font-weight:700;letter-spacing:2px;
    text-transform:uppercase;cursor:pointer;border:1px solid rgba(255,0,60,0.4);
    background:rgba(255,0,60,0.06);color:#ff5277;transition:all 0.3s ease;
    font-family:'Orbitron',sans-serif;backdrop-filter:blur(10px);
  }
  .tab.active{
    background:linear-gradient(135deg,#ff0a45,#b3002d);color:#fff;border-color:#ff003c;
    box-shadow:0 0 25px rgba(255,0,60,0.6), inset 0 1px 0 rgba(255,255,255,0.3);
  }
  .tab:hover:not(.active){background:rgba(255,0,60,0.15);color:#fff;box-shadow:0 0 15px rgba(255,0,60,0.3)}

  .panel{display:none}
  .panel.active{display:block;animation:fadeIn 0.4s cubic-bezier(0.16,1,0.3,1)}
  @keyframes fadeIn{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:translateY(0)}}

  .layout{display:grid;grid-template-columns:400px 1fr;gap:20px}
  @media(max-width:900px){.layout{grid-template-columns:1fr}}

  .card{
    position:relative;
    background:linear-gradient(145deg, rgba(18,18,26,0.8), rgba(8,8,14,0.85));
    border:1px solid rgba(255,0,60,0.3);
    border-radius:20px;padding:24px;margin-bottom:20px;
    backdrop-filter:blur(20px);box-shadow:0 20px 50px rgba(0,0,0,0.7), 0 0 30px rgba(255,0,60,0.08);
  }
  .card::after{
    content:"";position:absolute;inset:0;border-radius:20px;pointer-events:none;
    border:1px solid rgba(255,255,255,0.05);
  }
  .card h2{
    font-size:14px;margin-bottom:18px;color:#fff;letter-spacing:2px;
    display:flex;align-items:center;gap:10px;text-transform:uppercase;
    font-family:'Orbitron',sans-serif;
  }
  .card h2 .dot{width:9px;height:9px;border-radius:50%;background:#ff003c;box-shadow:0 0 15px #ff003c, 0 0 5px #fff}

  label{display:block;font-size:11px;color:#a0a0b5;margin-bottom:8px;letter-spacing:1.5px;text-transform:uppercase;font-weight:700}
  input,textarea,select{
    width:100%;background:rgba(3,3,6,0.85);border:1px solid rgba(255,0,60,0.3);
    border-radius:12px;padding:12px 16px;color:#f1f1f6;font-size:14px;
    font-family:inherit;outline:none;transition:all 0.3s ease;
  }
  input:focus,textarea:focus,select:focus{
    border-color:#ff003c;box-shadow:0 0 0 3px rgba(255,0,60,0.2), 0 0 25px rgba(255,0,60,0.3);
  }
  input:disabled{color:#5c5c6d;cursor:not-allowed}
  textarea{resize:vertical;min-height:96px}
  .field{margin-bottom:16px}

  button{
    background:linear-gradient(135deg,#ff0a45,#b3002d);color:#fff;border:none;
    border-radius:12px;padding:14px 24px;font-size:12px;font-weight:700;
    letter-spacing:2px;text-transform:uppercase;cursor:pointer;
    transition:all 0.3s ease;margin-top:6px;width:100%;
    box-shadow:0 8px 25px rgba(255,0,60,0.35), inset 0 1px 0 rgba(255,255,255,0.3);
    font-family:'Orbitron',sans-serif;
  }
  button:hover:not(:disabled){transform:translateY(-2px);box-shadow:0 12px 35px rgba(255,0,60,0.55), 0 0 20px rgba(255,0,60,0.4)}
  button:disabled{background:#1c1c24;color:#555566;cursor:not-allowed;box-shadow:none}
  button.ghost{background:transparent;border:1px solid rgba(255,0,60,0.5);color:#ff5277;box-shadow:none}
  button.ghost:hover:not(:disabled){background:rgba(255,0,60,0.12);color:#fff}
  button.danger{background:linear-gradient(135deg,#ff0040,#800020);border:1px solid #ff003c;box-shadow:0 8px 25px rgba(255,0,60,0.4)}
  .btnrow{display:flex;gap:12px}
  .btnrow button{margin-top:0}

  .status-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;gap:12px;flex-wrap:wrap}
  .badge{display:inline-block;padding:8px 18px;border-radius:999px;font-size:11px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;font-family:'Orbitron',sans-serif;}
  .b-idle{background:rgba(120,120,140,0.15);color:#a8a8b8;border:1px solid rgba(120,120,140,0.35)}
  .b-wait{background:rgba(255,180,0,0.15);color:#ffc655;border:1px solid rgba(255,180,0,0.45);animation:pulse 1.6s infinite}
  .b-paired{background:rgba(255,0,60,0.15);color:#ff5277;border:1px solid rgba(255,0,60,0.6);box-shadow:0 0 20px rgba(255,0,60,0.3)}
  .b-run{background:rgba(56,239,125,0.15);color:#38ef7d;border:1px solid rgba(56,239,125,0.6);box-shadow:0 0 20px rgba(56,239,125,0.35);animation:pulse 1.6s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:0.5}}

  .code-box{
    background:rgba(3,3,6,0.9);border:2px dashed #ff003c;border-radius:16px;
    padding:28px 20px;margin:20px 0;text-align:center;
    box-shadow:inset 0 0 35px rgba(255,0,60,0.15), 0 0 25px rgba(255,0,60,0.2);
  }
  .code-label{font-size:11px;color:#a0a0b5;letter-spacing:3px;margin-bottom:12px;text-transform:uppercase;font-weight:700}
  .code-value{font-size:36px;font-weight:900;letter-spacing:8px;color:#ff003c;
    font-family:'Orbitron',monospace;text-shadow:0 0 25px rgba(255,0,60,0.8)}
  .code-value.loading{font-size:16px;letter-spacing:1px;color:#a0a0b5;font-style:italic;text-shadow:none}

  .steps{background:rgba(3,3,6,0.6);border:1px solid rgba(255,0,60,0.25);border-radius:14px;
    padding:18px;font-size:13px;line-height:2;color:#d0d0e0}
  .steps b{color:#fff}

  .msg{margin-top:14px;padding:12px 16px;border-radius:12px;font-size:13px;display:none}
  .msg.ok{background:rgba(255,0,60,0.12);color:#ff8ba6;border:1px solid rgba(255,0,60,0.4);display:block}
  .msg.err{background:rgba(255,60,60,0.12);color:#ffa1a1;border:1px solid rgba(255,60,60,0.4);display:block}

  .grid2{display:grid;grid-template-columns:1fr 1fr;gap:14px}
  @media(max-width:520px){.grid2{grid-template-columns:1fr}}
  .stat{background:rgba(3,3,6,0.65);border:1px solid rgba(255,0,60,0.25);
    border-radius:16px;padding:16px;text-align:center}
  .stat .k{font-size:10px;color:#a0a0b5;letter-spacing:2px;text-transform:uppercase;margin-bottom:6px;font-weight:700}
  .stat .v{font-size:24px;font-weight:900;color:#fff;text-shadow:0 0 15px rgba(255,0,60,0.4);word-break:break-all;font-family:'Orbitron',sans-serif;}
  .stat .v.red{color:#ff003c}
  .stat .v.green{color:#38ef7d;text-shadow:0 0 15px rgba(56,239,125,0.5)}
  .stat .v.small{font-size:14px;font-weight:700}
  .statgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:14px}
  @media(max-width:520px){.statgrid{grid-template-columns:1fr 1fr}}

  .log{background:#010103;border:1px solid rgba(255,0,60,0.3);border-radius:16px;
    padding:16px;height:350px;overflow-y:auto;font-family:'Courier New',monospace;
    font-size:12px;line-height:1.8;box-shadow:inset 0 0 40px rgba(255,0,60,0.08)}
  .log::-webkit-scrollbar{width:8px}
  .log::-webkit-scrollbar-track{background:#050508}
  .log::-webkit-scrollbar-thumb{background:#ff003c;border-radius:8px}
  .log .line{color:#a0a0b5;border-bottom:1px dashed rgba(255,255,255,0.04);padding:3px 0;word-break:break-all}
  .log .t{color:#ff5277;margin-right:8px;font-weight:bold}
  .log .ok{color:#38ef7d}
  .log .err{color:#ff6b6b}
  .log .info{color:#6bc6ff}
  .log .warn{color:#ffc655}

  .progress{height:10px;background:rgba(3,3,6,0.9);border-radius:99px;overflow:hidden;
    border:1px solid rgba(255,0,60,0.3);margin-top:14px}
  .progress > div{height:100%;width:0%;
    background:linear-gradient(90deg,#ff003c,#ff5277,#ff003c);
    background-size:200% 100%;transition:width 0.4s ease;animation:shine 2s linear infinite}
  @keyframes shine{0%{background-position:0 0}100%{background-position:200% 0}}

  .groups-toolbar{display:flex;gap:10px;align-items:center;margin-bottom:12px;flex-wrap:wrap}
  .groups-toolbar button{width:auto;margin-top:0;padding:10px 16px;font-size:11px}
  .groups-toolbar .count{font-size:12px;color:#a0a0b5;margin-left:auto;letter-spacing:1px;font-weight:700}
  .groups-list{
    background:rgba(3,3,6,0.7);border:1px solid rgba(255,0,60,0.25);border-radius:16px;
    max-height:360px;overflow-y:auto;padding:8px;
  }
  .groups-list::-webkit-scrollbar{width:8px}
  .groups-list::-webkit-scrollbar-track{background:#050508}
  .groups-list::-webkit-scrollbar-thumb{background:#ff003c;border-radius:8px}
  .group-item{
    display:flex;align-items:center;gap:14px;padding:12px 14px;border-radius:12px;
    cursor:pointer;transition:all 0.2s ease;border:1px solid transparent;
  }
  .group-item:hover{background:rgba(255,0,60,0.1);border-color:rgba(255,0,60,0.3)}
  .group-item.selected{background:rgba(255,0,60,0.18);border-color:#ff003c;box-shadow:0 0 15px rgba(255,0,60,0.15)}
  .group-item input[type=checkbox]{
    width:20px;height:20px;accent-color:#ff003c;cursor:pointer;flex-shrink:0;
  }
  .group-info{flex:1;min-width:0}
  .group-name{color:#f1f1f6;font-size:13px;font-weight:700;
    white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .group-jid{color:#808095;font-size:11px;font-family:'Courier New',monospace;
    white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-top:2px}
  .group-meta{color:#ff5277;font-size:10px;letter-spacing:1px;text-transform:uppercase;flex-shrink:0;font-weight:700}

  .hidden{display:none!important}
  .hint{font-size:11px;color:#808095;margin-top:8px;letter-spacing:0.5px}
  .empty{padding:35px 20px;text-align:center;color:#808095;font-size:13px}
  .footer{text-align:center;color:#555568;font-size:11px;letter-spacing:3px;margin-top:35px;text-transform:uppercase;font-weight:700}

  .queue-box{
    background:rgba(3,3,6,0.85);border:1px solid rgba(255,0,60,0.3);border-radius:14px;
    padding:14px;margin-top:10px;font-size:12px;color:#d0d0e0;
    max-height:145px;overflow-y:auto;font-family:'Courier New',monospace;line-height:1.7;
  }
  .queue-box .qi{color:#6bc6ff;margin-right:8px;font-weight:bold}
</style>
</head>
<body>

<div class="live-bg"></div>
<div class="cyber-grid"></div>
<div class="floating-orbs"><div class="orb"></div><div class="orb"></div></div>

<div class="container">
  <div class="brand">
    <h1>RK <span class="x">X</span> RAJA</h1>
    <p>Live Cyberpunk Control Center</p>
  </div>

  <div class="tabs">
    <div class="tab active" data-tab="dashboard" onclick="switchTab('dashboard')">Dashboard</div>
    <div class="tab" data-tab="sender" onclick="switchTab('sender')">Bulk Sender Hub</div>
  </div>

  <div class="panel active" id="panel-dashboard">
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
      <div class="groups-toolbar">
        <button class="ghost" id="refreshGroupsBtn" onclick="fetchGroups()">Refresh Groups</button>
        <button class="ghost" onclick="selectAllGroups()">Select All</button>
        <button class="ghost" onclick="clearGroupSelection()">Clear</button>
        <span class="count" id="groupsCount">0 groups</span>
      </div>
      <div class="groups-list" id="groupsList">
        <div class="empty">Pair WhatsApp first, then click "Refresh Groups"</div>
      </div>

      <div class="field" style="margin-top:18px">
        <label>Message to send to selected groups</label>
        <textarea id="groupMsgInput" placeholder="Enter instant message to broadcast..."></textarea>
      </div>
      <button id="groupSendBtn" onclick="sendToSelectedGroups()">Send to Selected Groups</button>
      <div id="groupSendMsg" class="msg"></div>
    </div>

    <div class="card">
      <h2><span class="dot"></span>Danger Zone</h2>
      <p class="hint">Logout deletes session auth — requires re-pairing.</p>
      <button class="ghost" onclick="logout()">Logout / Reset Session</button>
    </div>
  </div>

  <div class="panel" id="panel-sender">
    <div class="layout">
      <div>
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
            <button id="startBtn" onclick="startServer()">Start 24/7 Loop</button>
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

          <div class="grid2" style="margin-top:14px">
            <div class="stat"><div class="k">Current Message</div><div class="v small" id="statCurMsg">—</div></div>
            <div class="stat"><div class="k">Current Target</div><div class="v small" id="statCurTarget">—</div></div>
          </div>

          <div class="progress"><div id="progBar"></div></div>
        </div>

        <div class="card">
          <h2><span class="dot"></span>Live Cyber Log</h2>
          <div class="log" id="logBox">
            <div class="line"><span class="t">[boot]</span> Cyberpunk system initialized. Ready...</div>
          </div>
          <div class="btnrow" style="margin-top:14px">
            <button class="ghost" onclick="clearLog()">Clear Log</button>
            <button class="danger" onclick="stopTask()">Stop Task</button>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div class="footer">RK RAJA XWD 💀 © — Live Cyberpunk Edition</div>
</div>

<script>
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
    const res=await fetch('/api/pair',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({phone})});
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
  if(!confirm('Logout? Auth will be deleted.'))return;
  try{await fetch('/api/logout',{method:'POST'});location.reload();}catch(e){alert(e.message);}
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
    const res=await fetch('/api/groups');
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
  const msg=document.getElementById('groupMsgInput').value.trim();
  const box=document.getElementById('groupSendMsg');
  const btn=document.getElementById('groupSendBtn');
  if(!dashSelected.size){showMsg(box,'err','Select at least one group');return;}
  if(!msg){showMsg(box,'err','Enter a message');return;}
  btn.disabled=true;btn.textContent='Sending...';showMsg(box,'','');
  try{
    const res=await fetch('/api/send-groups',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({groupIds:[...dashSelected], message:msg})
    });
    const d=await res.json();
    if(!d.success) throw new Error(d.error||'Failed');
    showMsg(box,'ok','Sent to '+d.sent+'/'+d.total+' groups');
    log('ok','Group send: '+d.sent+'/'+d.total+' success');
  }catch(e){showMsg(box,'err',e.message);log('err','Group send: '+e.message);}
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
    box.innerHTML='<div style="color:#a0a0b5;margin-bottom:6px">Queue ('+msgs.length+' messages)</div>'+
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
  fd.append('file',file);
  fd.append('hater',hater);
  fd.append('delay',String(delay));
  fd.append('lastHater',lastHater);
  fd.append('groupIds',JSON.stringify([...bulkSelected]));

  log('info','Starting 24/7 continuous task...');
  try{
    const res=await fetch('/api/bulk/start',{method:'POST',body:fd});
    const d=await res.json();
    if(!d.success)throw new Error(d.error||'Failed');
    log('ok','Task running 24/7 successfully — '+d.total+' messages');
    stopBtn.disabled=false;
  }catch(e){
    log('err','Start failed: '+e.message);
    alert('Start failed: '+e.message);
  }finally{
    startBtn.disabled=false;startBtn.textContent='Start 24/7 Loop';
  }
}
async function stopTask(){
  try{
    const res=await fetch('/api/bulk/stop',{method:'POST'});
    const d=await res.json();
    if(d.success) log('warn','Emergency stop executed successfully');
    else log('err',d.error||'No task running');
  }catch(e){log('err',e.message);}
}
async function pollStatus(){
  try{
    const res=await fetch('/api/status');
    const d=await res.json();
    const badge=document.getElementById('badge');
    const infoPhone=document.getElementById('infoPhone');
    const infoTime=document.getElementById('infoTime');
    const errBox=document.getElementById('errBox');
    const codeSection=document.getElementById('codeSection');
    const codeValue=document.getElementById('codeValue');
    const pairCard=document.getElementById('pairCard');
    const phoneInput=document.getElementById('phoneInput');
    const pairBtn=document.getElementById('pairBtn');
    infoPhone.textContent=d.phone||'—';
    infoTime.textContent=d.connectedAt?new Date(d.connectedAt).toLocaleString():'—';
    if(d.paired){
      badge.textContent='Paired';badge.className='badge b-paired';
      codeSection.classList.add('hidden');pairCard.classList.add('hidden');
    }else if(d.code){
      badge.textContent='Waiting for pairing';badge.className='badge b-wait';
      codeSection.classList.remove('hidden');
      codeValue.textContent=d.code;codeValue.classList.remove('loading');
      pairCard.classList.remove('hidden');phoneInput.disabled=true;pairBtn.disabled=true;
    }else if(d.connecting){
      badge.textContent='Connecting';badge.className='badge b-wait';
      pairCard.classList.remove('hidden');phoneInput.disabled=true;pairBtn.disabled=true;
    }else{
      badge.textContent='Idle';badge.className='badge b-idle';
      phoneInput.disabled=false;pairBtn.disabled=false;
    }
    if(d.error){errBox.textContent='⚠ '+d.error;errBox.classList.remove('hidden');}
    else{errBox.classList.add('hidden');}
  }catch(e){}
}
let lastLogId=0;
async function pollStats(){
  try{
    const res=await fetch('/api/bulk/status');
    const d=await res.json();
    const statusEl=document.getElementById('statStatus');
    statusEl.textContent=d.running?'Running 24/7':'Idle';
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
  }catch(e){}
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

app.post('/api/pair', async (req, res) => {
  try {
    const { phone } = req.body;
    if (!phone || !/^\d{10,15}$/.test(phone)) {
      return res.status(400).json({ success: false, error: 'Invalid phone number' });
    }
    if (isPaired) return res.status(400).json({ success: false, error: 'Already paired. Logout first.' });
    if (isConnecting) {
      return res.status(400).json({ success: false, error: 'Connection in progress.' });
    }
    if (fs.existsSync(AUTH_DIR)) fs.rmSync(AUTH_DIR, { recursive: true, force: true });

    connectToWhatsApp(phone).catch((e) => {
      lastError = e.message;
    });

    res.json({ success: true, message: 'Pairing initiated' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/status', (req, res) => {
  res.json({
    paired: isPaired,
    connecting: isConnecting,
    code: isPaired ? null : pairingCode,
    phone: currentPhone,
    connectedAt,
    error: lastError,
  });
});

app.get('/api/groups', async (req, res) => {
  try {
    if (!isPaired || !sock) {
      return res.status(400).json({ success: false, error: 'WhatsApp not paired yet' });
    }

    const map = new Map();
    Object.values(groupsCache).forEach((g) => map.set(g.id, g));

    try {
      getGroupsFromStore().forEach((g) => {
        if (!map.has(g.id)) map.set(g.id, g);
      });
    } catch (_) {}

    if (map.size === 0 && typeof sock.groupFetchAllParticipating === 'function') {
      try {
        const all = await sock.groupFetchAllParticipating();
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

    groups.forEach((g) => { groupsCache[g.id] = g; });

    res.json({ success: true, groups });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/send-groups', async (req, res) => {
  try {
    if (!isPaired || !sock) return res.status(400).json({ success: false, error: 'WhatsApp not paired yet' });
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
        await sock.sendMessage(jid, { text: message });
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

app.post('/api/logout', async (req, res) => {
  try {
    if (bulkState.running) {
      bulkState.stopFlag = true;
      pushLog('warn', 'Logout — stopping background worker');
    }
    if (sock) { try { await sock.logout(); } catch (_) {} }
    if (fs.existsSync(AUTH_DIR)) fs.rmSync(AUTH_DIR, { recursive: true, force: true });
    isPaired = false; pairingCode = null; currentPhone = null;
    pairingRequested = false; isConnecting = false;
    connectedAt = null; lastError = null; sock = null;
    groupsCache = {};
    pushLog('warn', 'Logged out, auth cleared');
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/bulk/start', upload.single('file'), async (req, res) => {
  try {
    if (!isPaired || !sock) {
      if (req.file) { try { fs.unlinkSync(req.file.path); } catch (_) {} }
      return res.status(400).json({ success: false, error: 'WhatsApp not paired yet.' });
    }
    if (bulkState.running) {
      if (req.file) { try { fs.unlinkSync(req.file.path); } catch (_) {} }
      return res.status(400).json({ success: false, error: 'A task is already running.' });
    }
    if (!req.file) {
      return res.status(400).json({ success: false, error: 'Please upload a .txt file' });
    }

    const { hater, delay, lastHater, groupIds } = req.body;

    let messages = [];
    try {
      messages = parseMessagesFile(req.file.path);
    } finally {
      try { fs.unlinkSync(req.file.path); } catch (_) {}
    }

    if (!messages.length) {
      return res.status(400).json({ success: false, error: 'No valid messages found' });
    }

    const targets = [];
    const seen = new Set();
    let parsedGroups = [];
    try { parsedGroups = JSON.parse(groupIds || '[]'); } catch (_) {}
    parsedGroups.forEach((jid) => {
      if (jid && !seen.has(jid)) { seen.add(jid); targets.push({ jid, label: jid }); }
    });

    if (!targets.length) {
      return res.status(400).json({ success: false, error: 'No targets selected' });
    }

    let delaySec = parseInt(delay) || DEFAULT_DELAY_SECONDS;
    if (delaySec < MIN_DELAY_SECONDS) delaySec = MIN_DELAY_SECONDS;
    const waitMs = delaySec * 1000;

    const prefix = hater && hater.trim() ? hater.trim() + '\n\n' : '';
    const suffix = lastHater && lastHater.trim() ? '\n\n' + lastHater.trim() : '';
    const preparedMessages = messages.map((m) => prefix + m + suffix);

    bulkState.running = true;
    bulkState.stopFlag = false;
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

    pushLog('info', `24/7 Task launched — ${messages.length} messages × ${targets.length} targets`);

    runWorker();

    res.json({ success: true, total: messages.length, totalTargets: targets.length });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/bulk/stop', (req, res) => {
  if (!bulkState.running) return res.status(400).json({ success: false, error: 'No task running' });
  bulkState.stopFlag = true;
  pushLog('warn', 'Emergency stop signal received from user');
  res.json({ success: true });
});

app.get('/api/bulk/status', (req, res) => {
  res.json({
    running: bulkState.running,
    workerAlive: bulkState.workerAlive,
    paired: isPaired,
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

app.get('/health', (req, res) => res.json({ status: 'ok', paired: isPaired }));

app.listen(PORT, HOST, () => {
  console.log('\n🌐 Live Cyberpunk Dashboard: http://' + HOST + ':' + PORT + '/\n');
  pushLog('info', 'Cyberpunk server started successfully');
});

process.on('SIGTERM', () => process.exit(0));
process.on('SIGINT', () => process.exit(0));
