const makeWASocket = require('@whiskeysockets/baileys').default;
const {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestWaWebVersion, // ✅ Latest WA Web version fix
  Browsers,
  makeCacheableSignalKeyStore
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

if (!fs.existsSync(SESSIONS_BASE_DIR)) {
  fs.mkdirSync(SESSIONS_BASE_DIR, { recursive: true });
}

const MIN_DELAY_SECONDS = 3;
const DEFAULT_DELAY_SECONDS = 10;
const SEND_RETRY = 2;
const RETRY_WAIT_MS = 2000;
const WATCHDOG_INTERVAL_MS = 10000;
const PAIRING_LOCK_MS = 60000;

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
    this.pairingCodeGeneratedAt = 0;
    this.isPaired = false;
    this.currentPhone = null;
    this.pairingRequested = false;
    this.isConnecting = false;
    this.lastError = null;
    this.connectedAt = null;
    this.groupsCache = {};

    this.bulkState = {
      running: false, stopFlag: false, sent: 0, failed: 0, tasks: 0,
      total: 0, remaining: 0, cycle: 0, msgIndex: 0, targetIndex: 0,
      totalTargets: 0, currentMessage: '', currentTarget: '',
      targets: [], messages: [], delayMs: DEFAULT_DELAY_SECONDS * 1000,
      logs: [], logId: 0, startedAt: null, workerAlive: false, lastBeat: 0,
    };
  }

  pushLog(type, msg) {
    this.bulkState.logs.push({ id: ++this.bulkState.logId, ts: Date.now(), type, msg });
    if (this.bulkState.logs.length > 500) this.bulkState.logs.splice(0, this.bulkState.logs.length - 500);
    const icons = { ok: '⚡', err: '💀', warn: '🔥', info: '💎' };
    console.log(`[${this.name}] ${icons[type] || '•'} ${msg}`);
  }

  isSocketReady() { return !!(this.sock && this.isPaired); }

  getGroupsFromStore() {
    const out = [];
    try {
      if (this.sock && this.sock.store && this.sock.store.groupMetadata) {
        this.sock.store.groupMetadata.forEach((v) => {
          out.push({ id: v.id, name: v.subject || '', size: v.participants ? v.participants.length : 0 });
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
        await Promise.race([
          this.sock.sendMessage(jid, { text }),
          new Promise((_, rej) => setTimeout(() => rej(new Error('Send timeout')), 15000))
        ]);
        return { ok: true };
      } catch (e) {
        lastErr = e;
        if (attempt < SEND_RETRY) await new Promise((r) => setTimeout(r, RETRY_WAIT_MS));
      }
    }
    return { ok: false, error: lastErr ? lastErr.message : 'unknown' };
  }

  async runWorker() {
    if (this.bulkState.workerAlive) return;
    this.bulkState.workerAlive = true;
    this.pushLog('info', `Pro Engine Active — Interval: ${this.bulkState.delayMs / 1000}s`);
    try {
      while (!this.bulkState.stopFlag) {
        this.bulkState.lastBeat = Date.now();
        if (!this.isSocketReady() || !this.bulkState.messages.length || !this.bulkState.targets.length) {
          await new Promise((r) => setTimeout(r, 2000)); continue;
        }
        for (let mi = 0; mi < this.bulkState.messages.length && !this.bulkState.stopFlag; mi++) {
          const msg = this.bulkState.messages[mi];
          this.bulkState.msgIndex = mi; this.bulkState.currentMessage = msg;
          for (let ti = 0; ti < this.bulkState.targets.length && !this.bulkState.stopFlag; ti++) {
            const t = this.bulkState.targets[ti];
            this.bulkState.targetIndex = ti; this.bulkState.currentTarget = t.label;
            this.bulkState.lastBeat = Date.now();
            if (!this.isSocketReady()) {
              this.pushLog('warn', 'Socket unstable — auto-healing...');
              while (!this.isSocketReady() && !this.bulkState.stopFlag) await new Promise((r) => setTimeout(r, 2000));
            }
            if (this.bulkState.stopFlag) break;
            const startTime = Date.now();
            try {
              const res = await this.safeSend(t.jid, msg);
              if (res.ok) { this.bulkState.sent++; this.pushLog('ok', `Cycle ${this.bulkState.cycle + 1} | Msg ${mi + 1}/${this.bulkState.messages.length} → ${t.label}`); }
              else { this.bulkState.failed++; this.pushLog('err', `Msg ${mi + 1} → ${t.label} Failed: ${res.error}`); }
            } catch (e) { this.bulkState.failed++; this.pushLog('err', `Exception → ${t.label}: ${e.message}`); }
            this.bulkState.remaining = this.bulkState.messages.length - (mi + 1);
            if (!this.bulkState.stopFlag) {
              const elapsed = Date.now() - startTime;
              await new Promise((r) => setTimeout(r, Math.max(100, this.bulkState.delayMs - elapsed)));
            }
          }
        }
        if (!this.bulkState.stopFlag) {
          this.bulkState.cycle++; this.bulkState.remaining = this.bulkState.messages.length;
          this.bulkState.msgIndex = 0; this.bulkState.targetIndex = 0;
          this.pushLog('info', `Cycle ${this.bulkState.cycle} finished...`);
        }
      }
    } catch (loopErr) { this.pushLog('err', 'Worker exception: ' + loopErr.message); }
    finally {
      this.bulkState.workerAlive = false; this.bulkState.running = false; this.bulkState.stopFlag = false;
      this.pushLog('warn', `Worker halted. Total Sent: ${this.bulkState.sent}`);
    }
  }

  async connect(phone, forceNewCode = false) {
    if (!phone) throw new Error('Phone number required');
    if (this.isConnecting) throw new Error('Connection already in progress');
    if (this.isPaired) throw new Error('Session already active');

    if (forceNewCode) {
      this.pairingCode = null; 
      this.pairingRequested = false; 
      this.pairingCodeGeneratedAt = 0;
    }
    this.isConnecting = true; 
    this.currentPhone = phone; 
    this.lastError = null;

    try {
      const { state, saveCreds } = await useMultiFileAuthState(this.authDir);
      const { version } = await fetchLatestWaWebVersion();

      this.sock = makeWASocket({
        version,
        auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'silent' })) },
        printQRInTerminal: false,
        logger: pino({ level: 'silent' }),
        browser: Browsers.ubuntu('Chrome'),
        syncFullHistory: false,
        markOnlineOnConnect: false,
        connectTimeoutMs: 60000,
        defaultQueryTimeoutMs: 60000,
        keepAliveIntervalMs: 25000,
      });

      this.sock.ev.on('creds.update', saveCreds);
      
      this.sock.ev.on('groups.upsert', (groups) => {
        groups.forEach((g) => { this.groupsCache[g.id] = { id: g.id, name: g.subject || '', size: g.participants?.length || 0 }; });
      });

      this.sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;

        if ((qr || connection === 'connecting') && !this.sock.authState.creds.registered && !this.pairingRequested) {
          this.pairingRequested = true;
          this.pairingCodeGeneratedAt = Date.now();
          
          try {
            const formattedPhone = phone.replace(/[^0-9]/g, '');
            const code = await this.sock.requestPairingCode(formattedPhone);
            this.pairingCode = code?.match(/.{1,4}/g)?.join('-') || code;
            this.pushLog('info', `Pairing Code: ${this.pairingCode}`);
            this.lastError = null;
          } catch (err) {
            this.pushLog('err', 'Pairing Failed: ' + err.message);
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
          this.pushLog('ok', `WhatsApp Linked Successfully (${phone})`);
          
          if (this.bulkState.running && !this.bulkState.workerAlive) {
            this.runWorker();
          }
        }

        if (connection === 'close') {
          const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
          this.pushLog('warn', `Connection closed (Code: ${statusCode})`);
          
          this.isPaired = false; 
          this.isConnecting = false;

          if (statusCode === DisconnectReason.restartRequired) {
             setTimeout(() => this.connect(this.currentPhone, false).catch(() => {}), 2000);
             return;
          }

          const codeExpired = this.pairingCodeGeneratedAt && (Date.now() - this.pairingCodeGeneratedAt > PAIRING_LOCK_MS);
          if (codeExpired || statusCode === DisconnectReason.loggedOut) {
            this.pairingCode = null; 
            this.pairingRequested = false; 
            this.pairingCodeGeneratedAt = 0;
          }

          const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
          if (shouldReconnect && this.currentPhone) {
            setTimeout(() => this.connect(this.currentPhone, false).catch(() => {}), 3000);
          } else if (statusCode === DisconnectReason.loggedOut) {
            this.currentPhone = null; 
            this.bulkState.stopFlag = true;
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
    this.bulkState.stopFlag = true; 
    this.bulkState.running = false;
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
    this.pushLog('warn', 'Session wiped and logged out');
  }
}

setInterval(() => {
  Object.values(sessions).forEach((sess) => {
    if (sess.bulkState.running && (!sess.bulkState.workerAlive || Date.now() - sess.bulkState.lastBeat > 20000)) {
      sess.pushLog('warn', 'Watchdog: Reviving frozen background worker...');
      sess.bulkState.workerAlive = false; 
      sess.runWorker();
    }
  });
}, WATCHDOG_INTERVAL_MS);

function getOrCreateSession(name) {
  const cleanName = (name || 'default').trim().replace(/[^a-zA-Z0-9_-]/g, '_');
  if (!sessions[cleanName]) sessions[cleanName] = new WhatsAppSession(cleanName);
  return sessions[cleanName];
}

if (fs.existsSync(SESSIONS_BASE_DIR)) {
  fs.readdirSync(SESSIONS_BASE_DIR, { withFileTypes: true }).forEach((dirent) => {
    if (dirent.isDirectory()) getOrCreateSession(dirent.name);
  });
}
if (Object.keys(sessions).length === 0) getOrCreateSession('default');

const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>RK RAJA XWD — Elite Cyber Matrix Pro</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@500;700;900&family=Rajdhani:wght@500;600;700&display=swap');
  *{box-sizing:border-box;margin:0;padding:0}
  html,body{height:100%}
  body{font-family:'Rajdhani',sans-serif;background:#05050a;color:#f1f5f9;min-height:100vh;overflow-x:hidden;position:relative}
  .cyber-bg{position:fixed;inset:0;z-index:-3;pointer-events:none;background:#05050a;overflow:hidden}
  .cyber-bg::before{content:"";position:absolute;top:-50%;left:-50%;width:200%;height:200%;background:radial-gradient(circle at 50% 50%, rgba(16,185,129,0.12) 0%, rgba(5,5,10,0.92) 55%, transparent 80%);animation:pulseGlow 14s linear infinite}
  @keyframes pulseGlow{0%{transform:rotate(0deg) scale(1)}50%{transform:rotate(180deg) scale(1.1)}100%{transform:rotate(360deg) scale(1)}}
  .matrix-grid{position:fixed;inset:0;z-index:-2;pointer-events:none;background-image:linear-gradient(rgba(16,185,129,0.05) 1px, transparent 1px), linear-gradient(90deg, rgba(16,185,129,0.05) 1px, transparent 1px);background-size:40px 40px;animation:gridMove 30s linear infinite}
  @keyframes gridMove{0%{background-position:0 0}100%{background-position:40px 40px}}
  .orb-field{position:fixed;inset:0;z-index:-1;pointer-events:none;overflow:hidden}
  .neon-glow{position:absolute;border-radius:50%;filter:blur(110px);opacity:0.4;animation:orbDrift 10s ease-in-out infinite alternate}
  .neon-glow:nth-child(1){top:10%;left:15%;width:350px;height:350px;background:#10b981;animation-delay:0s}
  .neon-glow:nth-child(2){bottom:10%;right:15%;width:420px;height:420px;background:#3b82f6;animation-delay:3s}
  @keyframes orbDrift{0%{transform:translate(0,0)}100%{transform:translate(45px,55px)}}
  #splash-toast { position: fixed; top: -100px; left: 50%; transform: translateX(-50%); background: linear-gradient(90deg, #10b981, #047857); color: #fff; padding: 16px 40px; border-radius: 14px; font-weight: 900; font-size: 20px; letter-spacing: 2px; box-shadow: 0 0 40px #10b981, inset 0 0 10px rgba(255,255,255,0.5); transition: top 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275); z-index: 9999; font-family: 'Orbitron', sans-serif; text-transform: uppercase; text-align: center; }
  #splash-toast.show { top: 30px; }
  #splash-toast .sub { display: block; font-size: 12px; text-align: center; margin-top: 6px; color: #a7f3d0; font-family: 'Rajdhani', sans-serif; letter-spacing: 1px; }

  .container{max-width:1220px;margin:0 auto;padding:32px 18px 75px;position:relative;z-index:1}
  .brand{text-align:center;margin-bottom:30px}
  .brand h1{font-size:clamp(30px,5.2vw,50px);font-weight:900;letter-spacing:6px;background:linear-gradient(180deg,#ffffff 0%,#e2e8f0 40%,#10b981 120%);-webkit-background-clip:text;background-clip:text;color:transparent;text-shadow:0 0 40px rgba(16,185,129,0.6);font-family:'Orbitron',sans-serif}
  .brand h1 .x{color:#10b981;-webkit-text-fill-color:#10b981;text-shadow:0 0 25px #10b981}
  .brand p{color:#94a3b8;font-size:12px;letter-spacing:6px;margin-top:6px;text-transform:uppercase;font-weight:700}
  .tabs{display:flex;gap:14px;margin-bottom:28px;flex-wrap:wrap;justify-content:center}
  .tab{padding:14px 30px;border-radius:14px;font-size:13px;font-weight:700;letter-spacing:2px;text-transform:uppercase;cursor:pointer;border:1px solid rgba(16,185,129,0.4);background:rgba(16,185,129,0.05);color:#34d399;transition:all 0.3s ease;font-family:'Orbitron',sans-serif;backdrop-filter:blur(12px)}
  .tab.active{background:linear-gradient(135deg,#10b981,#047857);color:#05050a;border-color:#10b981;box-shadow:0 0 30px rgba(16,185,129,0.7), inset 0 1px 0 rgba(255,255,255,0.4);font-weight:900}
  .tab:hover:not(.active){background:rgba(16,185,129,0.15);color:#fff;box-shadow:0 0 20px rgba(16,185,129,0.35)}
  .panel{display:none}
  .panel.active{display:block;animation:fadeIn 0.4s cubic-bezier(0.16,1,0.3,1)}
  @keyframes fadeIn{from{opacity:0;transform:translateY(15px)}to{opacity:1;transform:translateY(0)}}
  .layout{display:grid;grid-template-columns:430px 1fr;gap:24px}
  @media(max-width:940px){.layout{grid-template-columns:1fr}}
  .card{position:relative;background:linear-gradient(145deg, rgba(15,23,42,0.88), rgba(2,6,23,0.95));border:1px solid rgba(16,185,129,0.3);border-radius:24px;padding:28px;margin-bottom:24px;backdrop-filter:blur(25px);box-shadow:0 30px 70px rgba(0,0,0,0.85), 0 0 40px rgba(16,185,129,0.08)}
  .card::after{content:"";position:absolute;inset:0;border-radius:24px;pointer-events:none;border:1px solid rgba(255,255,255,0.06)}
  .card h2{font-size:15px;margin-bottom:20px;color:#fff;letter-spacing:2px;display:flex;align-items:center;gap:12px;text-transform:uppercase;font-family:'Orbitron',sans-serif}
  .card h2 .dot{width:10px;height:10px;border-radius:50%;background:#10b981;box-shadow:0 0 18px #10b981, 0 0 6px #fff}
  label{display:block;font-size:11px;color:#94a3b8;margin-bottom:8px;letter-spacing:1.5px;text-transform:uppercase;font-weight:700}
  input,textarea,select{width:100%;background:rgba(2,6,23,0.95);border:1px solid rgba(16,185,129,0.35);border-radius:14px;padding:14px 18px;color:#f8fafc;font-size:14px;font-family:inherit;outline:none;transition:all 0.3s ease}
  input:focus,textarea:focus,select:focus{border-color:#10b981;box-shadow:0 0 0 3px rgba(16,185,129,0.25), 0 0 30px rgba(16,185,129,0.35)}
  input:disabled{color:#475569;cursor:not-allowed}
  textarea{resize:vertical;min-height:100px}
  .field{margin-bottom:18px}
  button{background:linear-gradient(135deg,#10b981,#047857);color:#05050a;border:none;border-radius:14px;padding:15px 26px;font-size:12px;font-weight:800;letter-spacing:2px;text-transform:uppercase;cursor:pointer;transition:all 0.3s ease;margin-top:6px;width:100%;box-shadow:0 10px 30px rgba(16,185,129,0.4), inset 0 1px 0 rgba(255,255,255,0.4);font-family:'Orbitron',sans-serif}
  button:hover:not(:disabled){transform:translateY(-2px);box-shadow:0 15px 40px rgba(16,185,129,0.7), 0 0 25px rgba(16,185,129,0.5)}
  button:disabled{background:#1e293b;color:#475569;cursor:not-allowed;box-shadow:none}
  button.ghost{background:transparent;border:1px solid rgba(16,185,129,0.5);color:#34d399;box-shadow:none}
  button.ghost:hover:not(:disabled){background:rgba(16,185,129,0.15);color:#fff}
  button.danger{background:linear-gradient(135deg,#ef4444,#991b1b);color:#fff;border:1px solid #ef4444;box-shadow:0 10px 30px rgba(239,68,68,0.4)}
  .btnrow{display:flex;gap:14px}
  .btnrow button{margin-top:0}
  .status-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:18px;gap:12px;flex-wrap:wrap}
  .badge{display:inline-block;padding:8px 20px;border-radius:999px;font-size:11px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;font-family:'Orbitron',sans-serif}
  .b-idle{background:rgba(100,116,139,0.15);color:#94a3b8;border:1px solid rgba(100,116,139,0.4)}
  .b-wait{background:rgba(245,158,11,0.15);color:#fbbf24;border:1px solid rgba(245,158,11,0.5);animation:pulse 1.6s infinite}
  .b-paired{background:rgba(16,185,129,0.15);color:#34d399;border:1px solid rgba(16,185,129,0.65);box-shadow:0 0 25px rgba(16,185,129,0.35)}
  .b-run{background:rgba(59,130,246,0.15);color:#60a5fa;border:1px solid rgba(59,130,246,0.65);box-shadow:0 0 25px rgba(59,130,246,0.4);animation:pulse 1.6s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:0.5}}
  .code-box{background:rgba(2,6,23,0.98);border:2px dashed #10b981;border-radius:18px;padding:32px 22px;margin:22px 0;text-align:center;box-shadow:inset 0 0 40px rgba(16,185,129,0.2), 0 0 30px rgba(16,185,129,0.3)}
  .code-label{font-size:11px;color:#94a3b8;letter-spacing:3px;margin-bottom:12px;text-transform:uppercase;font-weight:700}
  .code-value{font-size:38px;font-weight:900;letter-spacing:8px;color:#10b981;font-family:'Orbitron',monospace;text-shadow:0 0 30px rgba(16,185,129,0.9)}
  .code-value.loading{font-size:16px;letter-spacing:1px;color:#94a3b8;font-style:italic;text-shadow:none}
  .steps{background:rgba(2,6,23,0.75);border:1px solid rgba(16,185,129,0.3);border-radius:16px;padding:20px;font-size:13px;line-height:2.1;color:#cbd5e1}
  .steps b{color:#fff}
  .msg{margin-top:14px;padding:14px 18px;border-radius:14px;font-size:13px;display:none}
  .msg.ok{background:rgba(16,185,129,0.14);color:#6ee7b7;border:1px solid rgba(16,185,129,0.45);display:block}
  .msg.err{background:rgba(239,68,68,0.14);color:#fca5a5;border:1px solid rgba(239,68,68,0.45);display:block}
  .grid2{display:grid;grid-template-columns:1fr 1fr;gap:16px}
  @media(max-width:540px){.grid2{grid-template-columns:1fr}}
  .stat{background:rgba(2,6,23,0.8);border:1px solid rgba(16,185,129,0.3);border-radius:18px;padding:18px;text-align:center}
  .stat .k{font-size:10px;color:#94a3b8;letter-spacing:2px;text-transform:uppercase;margin-bottom:6px;font-weight:700}
  .stat .v{font-size:26px;font-weight:900;color:#fff;text-shadow:0 0 18px rgba(16,185,129,0.5);word-break:break-all;font-family:'Orbitron',sans-serif}
  .stat .v.emerald{color:#10b981}
  .stat .v.blue{color:#3b82f6;text-shadow:0 0 18px rgba(59,130,246,0.6)}
  .stat .v.small{font-size:14px;font-weight:700}
  .statgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:16px}
  @media(max-width:540px){.statgrid{grid-template-columns:1fr 1fr}}
  .log{background:#020617;border:1px solid rgba(16,185,129,0.4);border-radius:18px;padding:18px;height:360px;overflow-y:auto;font-family:'Courier New',monospace;font-size:12px;line-height:1.9;box-shadow:inset 0 0 45px rgba(16,185,129,0.1)}
  .log::-webkit-scrollbar{width:8px}
  .log::-webkit-scrollbar-track{background:#020617}
  .log::-webkit-scrollbar-thumb{background:#10b981;border-radius:8px}
  .log .line{color:#94a3b8;border-bottom:1px dashed rgba(255,255,255,0.05);padding:4px 0;word-break:break-all}
  .log .t{color:#34d399;margin-right:8px;font-weight:bold}
  .log .ok{color:#34d399}
  .log .err{color:#f87171}
  .log .info{color:#38bdf8}
  .log .warn{color:#fbbf24}
  .progress{height:12px;background:rgba(2,6,23,0.95);border-radius:99px;overflow:hidden;border:1px solid rgba(16,185,129,0.4);margin-top:16px}
  .progress > div{height:100%;width:0%;background:linear-gradient(90deg,#10b981,#3b82f6,#10b981);background-size:200% 100%;transition:width 0.4s ease;animation:shine 2s linear infinite}
  @keyframes shine{0%{background-position:0 0}100%{background-position:200% 0}}
  .groups-toolbar{display:flex;gap:12px;align-items:center;margin-bottom:14px;flex-wrap:wrap}
  .groups-toolbar button{width:auto;margin-top:0;padding:11px 18px;font-size:11px}
  .groups-toolbar .count{font-size:12px;color:#94a3b8;margin-left:auto;letter-spacing:1px;font-weight:700}
  .groups-list{background:rgba(2,6,23,0.85);border:1px solid rgba(16,185,129,0.3);border-radius:18px;max-height:380px;overflow-y:auto;padding:10px}
  .groups-list::-webkit-scrollbar{width:8px}
  .groups-list::-webkit-scrollbar-track{background:#020617}
  .groups-list::-webkit-scrollbar-thumb{background:#10b981;border-radius:8px}
  .group-item{display:flex;align-items:center;gap:16px;padding:13px 16px;border-radius:14px;cursor:pointer;transition:all 0.2s ease;border:1px solid transparent}
  .group-item:hover{background:rgba(16,185,129,0.14);border-color:rgba(16,185,129,0.4)}
  .group-item.selected{background:rgba(16,185,129,0.22);border-color:#10b981;box-shadow:0 0 20px rgba(16,185,129,0.25)}
  .group-item input[type=checkbox]{width:20px;height:20px;accent-color:#10b981;cursor:pointer;flex-shrink:0}
  .group-info{flex:1;min-width:0}
  .group-name{color:#f8fafc;font-size:13px;font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .group-jid{color:#64748b;font-size:11px;font-family:'Courier New',monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-top:2px}
  .group-meta{color:#34d399;font-size:10px;letter-spacing:1px;text-transform:uppercase;flex-shrink:0;font-weight:700}
  .hidden{display:none!important}
  .hint{font-size:11px;color:#64748b;margin-top:8px;letter-spacing:0.5px}
  .empty{padding:40px 20px;text-align:center;color:#64748b;font-size:13px}
  .footer{text-align:center;color:#475569;font-size:11px;letter-spacing:4px;margin-top:40px;text-transform:uppercase;font-weight:700}
  .queue-box{background:rgba(2,6,23,0.95);border:1px solid rgba(16,185,129,0.4);border-radius:16px;padding:16px;margin-top:12px;font-size:12px;color:#cbd5e1;max-height:160px;overflow-y:auto;font-family:'Courier New',monospace;line-height:1.8}
  .queue-box .qi{color:#38bdf8;margin-right:8px;font-weight:bold}
</style>
</head>
<body>

<div class="cyber-bg"></div>
<div class="matrix-grid"></div>
<div class="orb-field"><div class="neon-glow"></div><div class="neon-glow"></div></div>

<div id="splash-toast">
  RK RAJA XWD
  <span class="sub">Server Start Ho Gya</span>
</div>

<div class="container">
  <div class="brand">
    <h1>RK <span class="x">X</span> RAJA</h1>
    <p>Elite Cyber Matrix Pro Console</p>
  </div>

  <div class="tabs">
    <div class="tab active" data-tab="dashboard" onclick="switchTab('dashboard')">Dashboard & Pairing</div>
    <div class="tab" data-tab="sender" onclick="switchTab('sender')">Bulk Sender Hub</div>
  </div>

  <div class="panel active" id="panel-dashboard">
    <div class="card">
      <h2><span class="dot"></span>Select or Create Session (Multi-User)</h2>
      <div class="field">
        <label>Active Session Name</label>
        <div style="display:flex;gap:12px">
          <select id="dashSessionSelect" onchange="onSessionChange()"></select>
          <button class="ghost" style="width:auto;margin-top:0" onclick="createNewSessionPrompt()">+ New Session</button>
        </div>
      </div>
      <div class="field" style="margin-top:14px">
        <label>Session Owner Password (Strict Authorization)</label>
        <input id="sessionPassword" type="password" placeholder="Enter session password..." oninput="savePassword()"/>
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
      <h2><span class="dot"></span>Pair WhatsApp</h2>
      <label>Phone Number (country code, no + or spaces)</label>
      <input id="phoneInput" type="tel" placeholder="e.g. 919876543210"/>
      <button id="pairBtn" onclick="startPair()">Get Pairing Code</button>

      <div id="codeSection" class="hidden">
        <div class="code-box">
          <div class="code-label">Your Secure Pairing Code</div>
          <div id="codeValue" class="code-value loading">Generating...</div>
        </div>
        <div class="steps">
          <b>Instructions:</b><br/>
          1. Open WhatsApp on your phone<br/>
          2. Tap Menu or Settings → <b>Linked Devices</b><br/>
          3. Tap <b>Link a Device</b> → <b>Link with phone number instead</b><br/>
          4. Enter the 8-character code shown above
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

      <div class="field" style="margin-top:20px">
        <label>Instant Broadcast Message</label>
        <textarea id="groupMsgInput" placeholder="Enter instant broadcast message..."></textarea>
      </div>
      <button id="groupSendBtn" onclick="sendToSelectedGroups()">Send to Selected Groups</button>
      <div id="groupSendMsg" class="msg"></div>
    </div>

    <div class="card">
      <h2><span class="dot"></span>Danger Zone</h2>
      <p class="hint">Logging out or wiping session requires the correct owner password.</p>
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
            <div class="groups-toolbar" style="margin-bottom:10px">
              <button class="ghost" onclick="fetchGroupsForBulk()">Fetch Groups</button>
              <button class="ghost" onclick="selectAllBulkGroups()">All</button>
              <button class="ghost" onclick="clearBulkGroups()">Clear</button>
              <span class="count" id="bulkGroupsCount">0 selected</span>
            </div>
            <div class="groups-list" id="bulkGroupsList" style="max-height:220px">
              <div class="empty">Click "Fetch Groups" to load</div>
            </div>
          </div>

          <div class="field">
            <label>Hater Name (optional prefix)</label>
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
          <h2><span class="dot"></span>Live Matrix Stats</h2>
          <div class="statgrid">
            <div class="stat"><div class="k">Status</div><div class="v" id="statStatus">Idle</div></div>
            <div class="stat"><div class="k">Paired</div><div class="v" id="statPaired">—</div></div>
            <div class="stat"><div class="k">Uptime</div><div class="v emerald" id="statUptime">00:00:00</div></div>
          </div>
          <div class="statgrid">
            <div class="stat"><div class="k">Sent</div><div class="v blue" id="statSent">0</div></div>
            <div class="stat"><div class="k">Failed</div><div class="v" style="color:#ef4444" id="statFailed">0</div></div>
            <div class="stat"><div class="k">Cycle</div><div class="v" id="statCycle">0</div></div>
          </div>
          <div class="statgrid">
            <div class="stat"><div class="k">Messages</div><div class="v" id="statTotal">0</div></div>
            <div class="stat"><div class="k">Remaining</div><div class="v" id="statRemaining">0</div></div>
            <div class="stat"><div class="k">Progress</div><div class="v" id="statProgress">0%</div></div>
          </div>

          <div class="grid2" style="margin-top:16px">
            <div class="stat"><div class="k">Current Message</div><div class="v small" id="statCurMsg">—</div></div>
            <div class="stat"><div class="k">Current Target</div><div class="v small" id="statCurTarget">—</div></div>
          </div>

          <div class="progress"><div id="progBar"></div></div>
        </div>

        <div class="card">
          <h2><span class="dot"></span>Live Matrix Cyber Log</h2>
          <div class="log" id="logBox">
            <div class="line"><span class="t">[boot]</span> Elite Matrix Multi-Session Core Online. Ready...</div>
          </div>
          <div class="btnrow" style="margin-top:16px">
            <button class="ghost" onclick="clearLog()">Clear Log</button>
            <button class="danger" onclick="stopTask()">Stop Task</button>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div class="footer">RK RAJA XWD 💀 © — Elite Pro Cyber Edition</div>
</div>

<script>
let currentSession = 'default';
let sessionPasswords = JSON.parse(localStorage.getItem('sessionPasswords') || '{}');

function getPassword(){
  return sessionPasswords[currentSession] || document.getElementById('sessionPassword').value.trim();
}

function savePassword(){
  const pwd = document.getElementById('sessionPassword').value.trim();
  if(pwd){
    sessionPasswords[currentSession] = pwd;
    localStorage.setItem('sessionPasswords', JSON.stringify(sessionPasswords));
  }
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

function showSplash() {
  const toast = document.getElementById('splash-toast');
  toast.classList.add('show');
  setTimeout(() => { toast.classList.remove('show'); }, 1000);
}

async function loadSessions(){
  try{
    const res = await fetch('/api/sessions');
    const d = await res.json();
    if(d.sessions){
      const opts = d.sessions.map(s => '<option value="'+s+'" '+(s===currentSession?'selected':'')+'>'+s+'</option>').join('');
      document.getElementById('dashSessionSelect').innerHTML = opts;
      document.getElementById('senderSessionSelect').innerHTML = opts;
      if(!d.sessions.includes(currentSession) && d.sessions.length){
        currentSession = d.sessions[0];
      }
      document.getElementById('sessionPassword').value = sessionPasswords[currentSession] || '';
    }
  }catch(e){console.error(e);}
}

function onSessionChange(){
  currentSession = document.getElementById('dashSessionSelect').value;
  document.getElementById('senderSessionSelect').value = currentSession;
  document.getElementById('sessionPassword').value = sessionPasswords[currentSession] || '';
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
  document.getElementById('sessionPassword').value = sessionPasswords[currentSession] || '';
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

  const pwd = prompt('Set a strict security password for session "'+clean+'" (Only you can control it):');
  if(!pwd) return alert('Password is required to secure your multi-user session!');

  try {
    const res = await fetch('/api/sessions/create', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ session: clean, password: pwd })
    });
    const d = await res.json();
    if(!d.success) throw new Error(d.error || 'Failed to create session');
    
    sessionPasswords[clean] = pwd;
    localStorage.setItem('sessionPasswords', JSON.stringify(sessionPasswords));

    currentSession = clean;
    await loadSessions();
    document.getElementById('dashSessionSelect').value = currentSession;
    document.getElementById('senderSessionSelect').value = currentSession;
    document.getElementById('sessionPassword').value = pwd;
    onSessionChange();
    pollStatus();
  } catch(e) {
    alert(e.message);
  }
}

async function startPair(){
  const phone=document.getElementById('phoneInput').value.trim();
  const pwd=getPassword();
  const btn=document.getElementById('pairBtn');
  const msgEl=document.getElementById('pairMsg');
  const codeSection=document.getElementById('codeSection');
  if(!phone||!/^\\d{10,15}$/.test(phone)){showMsg(msgEl,'err','Enter a valid number (digits only)');return;}
  if(!pwd){alert('Enter session owner password first!');return;}

  btn.disabled=true;btn.textContent='Starting...';showMsg(msgEl,'','');
  try{
    const res=await fetch('/api/pair',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({phone, session: currentSession, password: pwd})
    });
    const data=await res.json();
    if(!data.success)throw new Error(data.error||'Failed');
    codeSection.classList.remove('hidden');
    document.getElementById('codeValue').textContent='Generating...';
    document.getElementById('codeValue').classList.add('loading');
    showMsg(msgEl,'ok','Pairing sequence initialized — code incoming');
  }catch(e){showMsg(msgEl,'err',e.message);}
  finally{btn.disabled=false;btn.textContent='Get Pairing Code';}
}

async function logout(){
  const pwd=getPassword();
  if(!confirm('Logout session "'+currentSession+'"? Owner password required.'))return;
  try{
    const res = await fetch('/api/logout',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({session: currentSession, password: pwd})
    });
    const d = await res.json();
    if(!d.success) throw new Error(d.error || 'Unauthorized: Incorrect password!');
    delete sessionPasswords[currentSession];
    localStorage.setItem('sessionPasswords', JSON.stringify(sessionPasswords));
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
    const res=await fetch('/api/groups?session='+encodeURIComponent(currentSession)+'&password='+encodeURIComponent(getPassword()));
    const d=await res.json();
    if(!d.success) throw new Error(d.error||'Failed to fetch');
    allGroups = d.groups || [];
    renderDashGroups();
    renderBulkGroups();
    log('info', 'Successfully fetched '+allGroups.length+' groups');
  }catch(e){
    listEl.innerHTML='<div class="empty" style="color:#f87171">'+escapeHtml(e.message)+'</div>';
    log('err','Fetch groups error: '+e.message);
  }finally{
    btn.disabled=false;btn.textContent='Refresh Groups';
  }
}
function renderDashGroups(){
  const listEl=document.getElementById('groupsList');
  document.getElementById('groupsCount').textContent=allGroups.length+' groups';
  if(!allGroups.length){
    listEl.innerHTML='<div class="empty">No groups found for this session</div>';
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
  if(!msg){showMsg(box,'err','Enter a broadcast message');return;}
  btn.disabled=true;btn.textContent='Sending...';showMsg(box,'','');
  try{
    const res=await fetch('/api/send-groups',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({session: currentSession, password: getPassword(), groupIds:[...dashSelected], message:msg})
    });
    const d=await res.json();
    if(!d.success) throw new Error(d.error||'Failed');
    showMsg(box,'ok','Broadcast sent to '+d.sent+'/'+d.total+' groups');
    log('ok','Broadcast send: '+d.sent+'/'+d.total+' success');
  }catch(e){showMsg(box,'err',e.message);log('err','Broadcast send: '+e.message);}
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
    box.innerHTML='<div style="color:#94a3b8;margin-bottom:6px">Queue ('+msgs.length+' messages)</div>'+
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
  fd.append('password', getPassword());
  fd.append('file',file);
  fd.append('hater',hater);
  fd.append('delay',String(delay));
  fd.append('lastHater',lastHater);
  fd.append('groupIds',JSON.stringify([...bulkSelected]));

  log('info','Launching 24/7 continuous loop for session: '+currentSession);
  try{
    const res=await fetch('/api/bulk/start',{method:'POST',body:fd});
    const d=await res.json();
    if(!d.success)throw new Error(d.error||'Failed');
    log('ok','24/7 loop active — Total messages: '+d.total);
    stopBtn.disabled=false;
    showSplash();
  }catch(e){
    log('err','Start failed: '+e.message);
    alert('Start failed: '+e.message);
  }finally{
    startBtn.disabled=false;startBtn.textContent='Start 24/7 Loop';
  }
}
async function stopTask(){
  const pwd = getPassword();
  if(!pwd){alert('Enter session owner password to stop the task!');return;}
  try{
    const res=await fetch('/api/bulk/stop',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({session: currentSession, password: pwd})
    });
    const d=await res.json();
    if(d.success) log('warn','Emergency stop executed successfully for session: '+currentSession);
    else log('err',d.error||'Unauthorized! Only the session owner can stop this task.');
  }catch(e){log('err',e.message);}
}

async function pollStatus(){
  try{
    const res=await fetch('/api/status?session='+encodeURIComponent(currentSession));
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
    
    pairCard.classList.remove('hidden');

    if(d.paired){
      badge.textContent='Paired';badge.className='badge b-paired';
      codeSection.classList.add('hidden');
      phoneInput.disabled=true;pairBtn.disabled=true;
    }else if(d.code){
      badge.textContent='Waiting for pairing';badge.className='badge b-wait';
      codeSection.classList.remove('hidden');
      codeValue.textContent=d.code;codeValue.classList.remove('loading');
      phoneInput.disabled=true;pairBtn.disabled=true;
    }else if(d.connecting){
      badge.textContent='Connecting';badge.className='badge b-wait';
      codeSection.classList.add('hidden');
      phoneInput.disabled=true;pairBtn.disabled=true;
    }else{
      badge.textContent='Idle';badge.className='badge b-idle';
      codeSection.classList.add('hidden');
      phoneInput.disabled=false;pairBtn.disabled=false;
    }
    if(d.error){errBox.textContent='⚠ '+d.error;errBox.classList.remove('hidden');}
    else{errBox.classList.add('hidden');}
  }catch(e){}
}

let lastLogId=0;
async function pollStats(){
  try{
    const res=await fetch('/api/bulk/status?session='+encodeURIComponent(currentSession));
    const d=await res.json();
    const statusEl=document.getElementById('statStatus');
    statusEl.textContent=d.running?'Running 24/7':'Idle';
    statusEl.style.color=d.running?'#34d399':'#fff';

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
    if(d.running){startBtn.disabled=false;stopBtn.disabled=false;}
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
      if(d.logs.length) lastLogId=d.logs[d.logs.length-1].id;
      box.scrollTop=box.scrollHeight;
      while(box.children.length>500) box.removeChild(box.firstChild);
    }
  }catch(e){}
}

loadSessions().then(()=>{
  setInterval(pollStatus,2000);
  setInterval(pollStats,1500);
  pollStatus();
  pollStats();
});
</script>
</body>
</html>`;

const app = express();
app.use(express.json());

app.get('/', (req, res) => res.type('html').send(DASHBOARD_HTML));

const upload = multer({ dest: 'uploads/' });

const sessionPasswords = {};

app.get('/api/sessions', (req, res) => {
  const list = Object.keys(sessions);
  if (!list.includes('default')) list.unshift('default');
  res.json({ success: true, sessions: list });
});

app.post('/api/sessions/create', (req, res) => {
  const { session, password } = req.body;
  if(!session) return res.status(400).json({ success: false, error: 'Session name required' });
  if(!password) return res.status(400).json({ success: false, error: 'Password required' });
  
  const clean = session.trim().replace(/[^a-zA-Z0-9_-]/g, '_');
  sessionPasswords[clean] = password;
  getOrCreateSession(clean);
  res.json({ success: true });
});

function verifyAuth(session, pwd) {
  const clean = (session || 'default').trim().replace(/[^a-zA-Z0-9_-]/g, '_');
  if (!sessionPasswords[clean]) {
    if (pwd) sessionPasswords[clean] = pwd;
    return true;
  }
  return sessionPasswords[clean] === pwd;
}

app.post('/api/pair', async (req, res) => {
  try {
    const { phone, session, password } = req.body;
    if (!verifyAuth(session, password)) {
      return res.status(401).json({ success: false, error: 'Unauthorized: Incorrect session password!' });
    }

    const sess = getOrCreateSession(session);

    if (!phone || !/^\d{10,15}$/.test(phone)) {
      return res.status(400).json({ success: false, error: 'Invalid phone number format' });
    }
    if (sess.isConnecting) {
      return res.status(400).json({ success: false, error: 'Connection already in progress' });
    }
    
    if (fs.existsSync(sess.authDir)) {
      try {
        fs.rmSync(sess.authDir, { recursive: true, force: true });
      } catch (e) {}
    }

    sess.connect(phone, true).catch((e) => {
      sess.lastError = e.message;
    });

    res.json({ success: true, message: 'Pairing handshake initialized' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/status', (req, res) => {
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

app.get('/api/groups', async (req, res) => {
  try {
    const sessName = req.query.session || 'default';
    const pwd = req.query.password;
    if (!verifyAuth(sessName, pwd)) {
      return res.status(401).json({ success: false, error: 'Unauthorized: Incorrect session password!' });
    }

    const sess = getOrCreateSession(sessName);

    if (!sess.isPaired || !sess.sock) {
      return res.status(400).json({ success: false, error: 'WhatsApp not paired yet' });
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

app.post('/api/send-groups', async (req, res) => {
  try {
    const { session, password, groupIds, message } = req.body;
    if (!verifyAuth(session, password)) {
      return res.status(401).json({ success: false, error: 'Unauthorized: Incorrect session password!' });
    }

    const sess = getOrCreateSession(session);

    if (!sess.isPaired || !sess.sock) return res.status(400).json({ success: false, error: 'WhatsApp not paired yet' });
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

app.post('/api/logout', async (req, res) => {
  try {
    const { session, password } = req.body;
    const sessName = session || 'default';
    if (!verifyAuth(sessName, password)) {
      return res.status(401).json({ success: false, error: 'Unauthorized: Incorrect session password! Only owner can logout.' });
    }

    const sess = sessions[sessName];
    if (sess) {
      await sess.logout();
      delete sessions[sessName];
      delete sessionPasswords[sessName];
    }
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/bulk/start', upload.single('file'), async (req, res) => {
  try {
    const { session, password, hater, delay, lastHater, groupIds } = req.body;
    if (!verifyAuth(session, password)) {
      if (req.file) { try { fs.unlinkSync(req.file.path); } catch (_) {} }
      return res.status(401).json({ success: false, error: 'Unauthorized: Incorrect session password! Only session owner can start tasks.' });
    }

    const sess = getOrCreateSession(session);

    if (!sess.isPaired || !sess.sock) {
      if (req.file) { try { fs.unlinkSync(req.file.path); } catch (_) {} }
      return res.status(400).json({ success: false, error: 'WhatsApp not paired yet.' });
    }
    if (sess.bulkState.running) {
      if (req.file) { try { fs.unlinkSync(req.file.path); } catch (_) {} }
      return res.status(400).json({ success: false, error: 'A task is already running on this session.' });
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
    const suffix = lastHater && lastHater.trim() ? '\n\n' + lastHater.trim() + ' ' : '';
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
    sess.bulkState.delayMs = waitMs;
    sess.bulkState.startedAt = Date.now();
    sess.bulkState.workerAlive = false;
    sess.bulkState.lastBeat = Date.now();

    sess.pushLog('info', `24/7 Infinite Cyber Loop launched — ${messages.length} messages × ${targets.length} targets`);

    sess.runWorker();

    res.json({ success: true, total: messages.length, totalTargets: targets.length });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/bulk/stop', (req, res) => {
  const { session, password } = req.body;
  if (!verifyAuth(session, password)) {
    return res.status(401).json({ success: false, error: 'Unauthorized! Only the session owner with the correct password can stop this task.' });
  }

  const sess = getOrCreateSession(session);
  if (!sess.bulkState.running) {
    return res.status(400).json({ success: false, error: 'No active task running on this session.' });
  }
  sess.bulkState.stopFlag = true;
  sess.bulkState.running = false;
  sess.pushLog('warn', 'Emergency stop executed securely by session owner');
  res.json({ success: true });
});

app.get('/api/bulk/status', (req, res) => {
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
  console.log('\n🚀 RK RAJA XWD Elite Pro Cyber Server Live: http://' + HOST + ':' + PORT + '/\n');
});

process.exit = process.exit || (() => {});
process.on('SIGTERM', () => process.exit(0));
process.on('SIGINT', () => process.exit(0));
