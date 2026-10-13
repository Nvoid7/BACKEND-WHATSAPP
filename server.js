/* DevConnect WA server v1.0 — Baileys + Socket.IO namespace /wa (protokol sama dengan js/wa.js).
   1 username app = 1 sesi WhatsApp. Login pakai kode pairing (tanpa QR). Harus jalan terus (Node biasa, BUKAN Cloudflare Worker). */
import { createServer } from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { Server } from 'socket.io';
import pino from 'pino';
import makeWASocket, { Browsers, useMultiFileAuthState, fetchLatestBaileysVersion, DisconnectReason, jidNormalizedUser, isJidGroup, isJidBroadcast, isJidStatusBroadcast } from '@whiskeysockets/baileys';

const { APP_KEY = '', PORT = '3100', DATA_DIR = './data', CORS_ORIGIN = '*' } = process.env;
const log = pino({ level: 'warn' });
const ID = /^[A-Za-z0-9_.-]{2,40}$/;
fs.mkdirSync(DATA_DIR, { recursive: true });

const http = createServer((q, r) => { if (q.url === '/health') { r.writeHead(200, { 'content-type': 'application/json' }); r.end('{"ok":true}'); } else { r.writeHead(404); r.end(); } });
const io = new Server(http, { cors: { origin: CORS_ORIGIN === '*' ? true : CORS_ORIGIN.split(',') }, maxHttpBufferSize: 20e6 });
const nsp = io.of('/wa');
const same = (a, b) => { a = Buffer.from(String(a)); b = Buffer.from(String(b)); return a.length === b.length && crypto.timingSafeEqual(a, b); };

/* ---------- 1 sesi per user ---------- */
const sessions = new Map();           // u -> Session
const dirOf = (u) => path.join(DATA_DIR, u);
const MAXM = 80;                      // pesan disimpan per chat

class Session {
  constructor(u) {
    this.u = u; this.dir = dirOf(u); this.sk = ''; this.sock = null; this.st = { st: 'none' };
    this.chats = new Map(); this.msgs = new Map(); this.names = new Map(); this.avc = new Map();
    this.pairing = null; this.stopped = false; this.saveT = null; this.retry = 0;
    this.load();
  }
  load() {
    try {
      const j = JSON.parse(fs.readFileSync(path.join(this.dir, 'store.json'), 'utf8'));
      this.sk = j.sk || '';
      (j.chats || []).forEach(c => this.chats.set(c.id, c));
      Object.entries(j.msgs || {}).forEach(([k, v]) => this.msgs.set(k, v));
      Object.entries(j.names || {}).forEach(([k, v]) => this.names.set(k, v));
    } catch { /* baru */ }
    if (fs.existsSync(path.join(this.dir, 'auth', 'creds.json'))) { try { if (JSON.parse(fs.readFileSync(path.join(this.dir, 'auth', 'creds.json'), 'utf8')).registered) this.st = { st: 'connecting' }; } catch { /* abaikan */ } }
  }
  save() {
    clearTimeout(this.saveT);
    this.saveT = setTimeout(() => {
      try {
        fs.mkdirSync(this.dir, { recursive: true });
        const o = { sk: this.sk, chats: [...this.chats.values()], msgs: Object.fromEntries(this.msgs), names: Object.fromEntries(this.names) };
        fs.writeFileSync(path.join(this.dir, 'store.tmp'), JSON.stringify(o)); fs.renameSync(path.join(this.dir, 'store.tmp'), path.join(this.dir, 'store.json'));
      } catch (e) { log.warn(e); }
    }, 1500);
  }
  emit(ev, d) { nsp.to('u:' + this.u).emit(ev, d); }
  setSt(s) { this.st = s; this.emit('st', s); }
  async start() {
    if (this.sock || this.stopped) return;
    const { state, saveCreds } = await useMultiFileAuthState(path.join(this.dir, 'auth'));
    let version; try { version = (await fetchLatestBaileysVersion()).version; } catch { /* pakai bawaan */ }
    const sock = this.sock = makeWASocket({ version, auth: state, logger: pino({ level: 'silent' }), printQRInTerminal: false, browser: Browsers.ubuntu('Chrome'), syncFullHistory: false, markOnlineOnConnect: false, generateHighQualityLinkPreview: false });
    sock.ev.on('creds.update', saveCreds);
    sock.ev.on('connection.update', (u) => this.onConn(sock, u));
    sock.ev.on('messaging-history.set', (h) => { (h.contacts || []).forEach(c => this.name(c)); (h.chats || []).forEach(c => this.upChat(c.id, { name: c.name, ts: (Number(c.conversationTimestamp) || 0) * 1000, un: c.unreadCount | 0 })); (h.messages || []).forEach(m => this.addMsg(m, true)); this.pushAll(); });
    sock.ev.on('contacts.upsert', (l) => { l.forEach(c => this.name(c)); this.pushAll(); });
    sock.ev.on('contacts.update', (l) => { l.forEach(c => this.name(c)); });
    sock.ev.on('chats.upsert', (l) => { l.forEach(c => this.upChat(c.id, { name: c.name, ts: (Number(c.conversationTimestamp) || 0) * 1000, un: c.unreadCount | 0 })); this.pushAll(); });
    sock.ev.on('messages.upsert', ({ messages, type }) => { messages.forEach(m => this.addMsg(m, false, type === 'notify')); });
    sock.ev.on('messages.update', (l) => l.forEach(u => { const jid = u.key && jidNormalizedUser(u.key.remoteJid || ''); const s = this.mapSt(u.update && u.update.status); if (!jid || s == null) return; const arr = this.msgs.get(jid) || []; const m = arr.find(x => x.id === u.key.id); if (m && s > (m.s | 0)) { m.s = s; this.emit('mst', { id: jid, mid: m.id, s }); const c = this.chats.get(jid); if (c && c.lm) { c.ls = s; this.emit('chat', c); } this.save(); } }));
    sock.ev.on('presence.update', ({ id, presences }) => { const on = Object.values(presences || {}).some(p => p.lastKnownPresence === 'composing' || p.lastKnownPresence === 'recording'); this.emit('typ', { id: jidNormalizedUser(id), on: on ? 1 : 0 }); });
  }
  async onConn(sock, u) {
    if (sock !== this.sock) return;
    const { connection, lastDisconnect } = u;
    if (connection) console.log('[wa]', this.u, 'conn', connection);
    if (u.qr && this.pairing && this.pairWait && !this.pairWait.asked) {            // minta kode saat handshake siap (cara resmi Baileys)
      this.pairWait.asked = true; console.log('[wa]', this.u, 'minta kode');
      sock.requestPairingCode(this.pairing).then((c) => { console.log('[wa]', this.u, 'kode dibuat'); this.pairWait && this.pairWait.done({ code: c.length === 8 ? c.slice(0, 4) + '-' + c.slice(4) : c }); })
        .catch((e) => { console.log('[wa]', this.u, 'gagal minta kode:', e && e.message); this.pairWait && this.pairWait.done({ e: 'Gagal meminta kode: ' + (e && e.message || 'coba lagi') }); });
    }
    if (connection === 'open') { this.retry = 0; this.pairing = null; this.setSt({ st: 'open' }); try { await sock.sendPresenceUpdate('unavailable'); } catch { /* abaikan */ } }
    if (connection === 'connecting' && this.st.st === 'none' && !this.pairing) this.setSt({ st: 'connecting' });
    if (connection === 'close') {
      this.sock = null;
      const code = lastDisconnect && lastDisconnect.error && lastDisconnect.error.output && lastDisconnect.error.output.statusCode;
      console.log('[wa]', this.u, 'close', code, lastDisconnect && lastDisconnect.error && lastDisconnect.error.message, this.pairing ? '(pairing)' : '');
      if (this.stopped) return;
      if (code === DisconnectReason.loggedOut) return this.wipe('Sesi diputus dari WhatsApp');
      if (this.pairing && code === DisconnectReason.restartRequired) { this.start(); return; }   // normal setelah kode dimasukkan
      if (this.pairing && this.st.st === 'pairing') { this.pairing = null; this.setSt({ st: 'none', err: 'Kode kedaluwarsa, minta lagi' }); try { fs.rmSync(path.join(this.dir, 'auth'), { recursive: true, force: true }); } catch { /* */ } return; }
      this.setSt({ st: 'connecting' });
      const wait = Math.min(30000, 1500 * 2 ** Math.min(this.retry++, 5)); setTimeout(() => this.start(), wait);
    }
  }
  async pair(phone) {
    phone = String(phone || '').replace(/\D/g, '');
    if (phone.startsWith('0')) phone = '62' + phone.slice(1);   // 08xx -> 628xx
    if (phone.startsWith('62') && phone[2] === '0') phone = '62' + phone.slice(3);
    if (phone.length < 8 || phone.length > 15) return { e: 'Nomor tidak valid (pakai kode negara, tanpa +)' };
    if (this.st.st === 'open') return { e: 'Sudah tertaut' };
    await this.drop(); try { fs.rmSync(path.join(this.dir, 'auth'), { recursive: true, force: true }); } catch { /* */ }
    this.stopped = false; this.pairing = phone;
    const res = await new Promise((resolve) => {
      const t = setTimeout(() => done({ e: 'Server WhatsApp tidak merespons, coba lagi' }), 25000);
      const done = (r) => { clearTimeout(t); this.pairWait = null; resolve(r); };
      this.pairWait = { done, asked: false };
      this.start().catch((e) => done({ e: 'Gagal memulai: ' + (e && e.message) }));
    });
    if (res.e) { this.pairing = null; await this.drop(); return res; }
    this.setSt({ st: 'pairing', code: res.code });
    return { code: res.code };
  }
  async drop() { const s = this.sock; this.sock = null; if (s) { try { s.ev.removeAllListeners(); s.end(undefined); } catch { /* */ } } }
  async wipe(err) {
    this.stopped = true; this.pairing = null; await this.drop();
    this.chats.clear(); this.msgs.clear(); this.names.clear(); this.avc.clear();
    try { fs.rmSync(this.dir, { recursive: true, force: true }); } catch { /* */ }
    this.sk = ''; this.stopped = false; this.setSt(err ? { st: 'none', err } : { st: 'none' });
  }
  async logout() { try { if (this.sock && this.st.st === 'open') await this.sock.logout(); } catch { /* */ } await this.wipe(); }

  /* ---------- data ---------- */
  mapSt(s) { if (s == null) return null; if (s >= 4) return 3; if (s === 3) return 2; if (s === 2) return 1; return 0; }
  name(c) { const id = c && c.id && jidNormalizedUser(c.id); const n = c && (c.name || c.notify || c.verifiedName); if (id && n) { this.names.set(id, n); const ch = this.chats.get(id); if (ch && ch.name !== n) { ch.name = n; } } }
  disp(id) { return this.names.get(id) || (isJidGroup(id) ? 'Grup' : '+' + id.split('@')[0]); }
  upChat(id, p) {
    id = jidNormalizedUser(id); if (!id || isJidBroadcast(id) || isJidStatusBroadcast(id) || id.endsWith('@newsletter')) return null;
    let c = this.chats.get(id); if (!c) { c = { id, name: '', g: isJidGroup(id) ? 1 : 0, ts: 0, last: '', lm: 0, ls: 0, un: 0, pin: 0 }; this.chats.set(id, c); }
    if (p.name) { c.name = p.name; this.names.set(id, p.name); }
    if (p.ts && p.ts > c.ts) c.ts = p.ts;
    if (typeof p.un === 'number') c.un = p.un;
    if (!c.name) c.name = this.disp(id);
    return c;
  }
  parse(m) {
    const k = m.key || {}, msg = m.message || {}; let inner = msg.ephemeralMessage ? msg.ephemeralMessage.message : msg; if (inner && inner.viewOnceMessage) inner = inner.viewOnceMessage.message; if (!inner) return null;
    let t = 'text', x = '', l = '';
    if (inner.conversation) x = inner.conversation;
    else if (inner.extendedTextMessage) x = inner.extendedTextMessage.text || '';
    else if (inner.imageMessage) { t = 'media'; l = '📷 Foto'; x = inner.imageMessage.caption || ''; }
    else if (inner.ptvMessage) { t = 'media'; l = '⭕ Video note'; }
    else if (inner.videoMessage) { t = 'media'; l = '🎥 Video'; x = inner.videoMessage.caption || ''; }
    else if (inner.audioMessage) { t = 'media'; l = inner.audioMessage.ptt ? '🎤 Pesan suara' : '🎵 Audio'; }
    else if (inner.documentMessage) { t = 'media'; l = '📄 ' + (inner.documentMessage.fileName || 'Dokumen'); x = inner.documentMessage.caption || ''; }
    else if (inner.stickerMessage) { t = 'media'; l = 'Stiker'; }
    else if (inner.contactMessage) { t = 'media'; l = '👤 Kontak'; }
    else if (inner.locationMessage) { t = 'media'; l = '📍 Lokasi'; }
    else if (inner.reactionMessage || inner.protocolMessage || inner.senderKeyDistributionMessage) return null;
    else { t = 'media'; l = 'Pesan tidak didukung'; }
    return { id: k.id, me: k.fromMe ? 1 : 0, t, x, l, ts: (Number(m.messageTimestamp) || Math.floor(Date.now() / 1000)) * 1000, s: this.mapSt(m.status) | 0, w: k.fromMe ? '' : (m.pushName || '') };
  }
  addMsg(raw, history, live) {
    const jid = raw.key && raw.key.remoteJid && jidNormalizedUser(raw.key.remoteJid); if (!jid) return;
    const pm = this.parse(raw); if (!pm) return;
    if (raw.pushName && !raw.key.fromMe && !isJidGroup(jid)) { this.names.set(jid, this.names.get(jid) || raw.pushName); }
    const c = this.upChat(jid, {}); if (!c) return;
    const arr = this.msgs.get(jid) || []; if (arr.some(x => x.id === pm.id)) return;
    arr.push(pm); arr.sort((a, b) => a.ts - b.ts); while (arr.length > MAXM) arr.shift(); this.msgs.set(jid, arr);
    if (pm.ts >= c.ts) { c.ts = pm.ts; c.last = pm.l || pm.x; c.lm = pm.me; c.ls = pm.s; }
    if (!history && !pm.me && live !== false) c.un = (c.un | 0) + 1;
    if (!history) { this.emit('msg', { id: jid, m: pm }); this.emit('chat', c); }
    this.save();
  }
  pushAll() { this.emit('chats', this.list()); this.save(); }
  list() { return [...this.chats.values()].sort((a, b) => ((b.pin | 0) - (a.pin | 0)) || (b.ts - a.ts)).slice(0, 300); }
  async sendVnote(id, data) {
    if (!this.sock || this.st.st !== 'open') return { e: 'WhatsApp belum tersambung' };
    const buf = Buffer.isBuffer(data) ? data : (data instanceof ArrayBuffer ? Buffer.from(data) : null);
    if (!buf || buf.length < 1000) return { e: 'File video tidak valid' };
    if (buf.length > 15 * 1024 * 1024) return { e: 'Video terlalu besar (maks 15 MB)' };
    if (buf.slice(4, 8).toString('latin1') !== 'ftyp') return { e: 'Bukan file MP4 yang valid' };
    try {
      const jid = jidNormalizedUser(id); const r = await this.sock.sendMessage(jid, { video: buf, ptv: true, mimetype: 'video/mp4' });
      this.addMsg(r, false); const m = (this.msgs.get(jid) || []).find(x => x.id === r.key.id);
      return { ok: 1, m: m || { id: r.key.id, me: 1, t: 'media', x: '', l: '⭕ Video note', ts: Date.now(), s: 1, w: '' } };
    } catch (e) { console.log('[wa]', this.u, 'vnote gagal:', e && e.message); return { e: 'Gagal mengirim video note' }; }
  }
  async send(id, text) {
    if (!this.sock || this.st.st !== 'open') return { e: 'WhatsApp belum tersambung' };
    text = String(text || '').slice(0, 4000); if (!text.trim()) return { e: 'Pesan kosong' };
    try {
      const jid = jidNormalizedUser(id); const r = await this.sock.sendMessage(jid, { text });
      this.addMsg(r, false); const arr = this.msgs.get(jid) || []; const m = arr.find(x => x.id === r.key.id);
      return { ok: 1, m: m || { id: r.key.id, me: 1, t: 'text', x: text, l: '', ts: Date.now(), s: 1, w: '' } };
    } catch (e) { return { e: 'Gagal mengirim' }; }
  }
}
const get = (u) => { let s = sessions.get(u); if (!s) { s = new Session(u); sessions.set(u, s); } return s; };

/* ---------- Socket.IO /wa ---------- */
nsp.use((sock, next) => {
  const { key = '', u = '', sk = '' } = sock.handshake.auth || {};
  if (APP_KEY && !same(key, APP_KEY)) return next(new Error('key'));
  if (!ID.test(u) || !/^[A-Za-z0-9]{32,64}$/.test(sk)) return next(new Error('bad'));
  const s = get(u);
  if (s.sk && !same(s.sk, sk)) return next(new Error('locked'));    // sesi sudah dipegang perangkat lain
  if (!s.sk) { s.sk = sk; s.save(); }
  sock.data.u = u; next();
});
nsp.on('connection', (sock) => {
  const s = get(sock.data.u); sock.join('u:' + s.u);
  if (s.st.st === 'connecting' && !s.sock) s.start();
  const ok = (cb) => typeof cb === 'function' ? cb : () => {};
  sock.on('state', (_, cb) => ok(cb)(s.st));
  sock.on('pair', async (d, cb) => ok(cb)(await s.pair(d && d.phone)));
  sock.on('logout', async (_, cb) => { await s.logout(); ok(cb)({ ok: 1 }); });
  sock.on('chats', (_, cb) => ok(cb)(s.list()));
  sock.on('msgs', async (d, cb) => {
    const id = d && String(d.id || ''); if (!id) return ok(cb)({});
    const c = s.chats.get(id); if (c && c.un) { c.un = 0; s.emit('chat', c); s.save(); }
    try { const arr = s.msgs.get(id) || []; const last = arr[arr.length - 1]; if (s.sock && s.st.st === 'open' && last && !last.me) await s.sock.readMessages([{ remoteJid: id, id: last.id, fromMe: false, participant: undefined }]); } catch { /* abaikan */ }
    ok(cb)({ list: s.msgs.get(id) || [], name: c ? c.name : s.disp(id), g: c ? c.g : 0 });
  });
  sock.on('vnote', async (d, cb) => ok(cb)(await s.sendVnote(d && d.id, d && d.data)));
  sock.on('send', async (d, cb) => ok(cb)(await s.send(d && d.id, d && d.text)));
  sock.on('typing', async (d) => { try { if (s.sock && s.st.st === 'open' && d && d.id) await s.sock.sendPresenceUpdate(d.on ? 'composing' : 'paused', d.id); } catch { /* */ } });
  sock.on('newchat', async (d, cb) => {
    const n = String(d && d.phone || '').replace(/\D/g, ''); if (n.length < 8 || n.length > 15) return ok(cb)({ e: 'Nomor tidak valid' });
    try { if (!s.sock || s.st.st !== 'open') return ok(cb)({ e: 'WhatsApp belum tersambung' }); const r = await s.sock.onWhatsApp(n); if (!r || !r[0] || !r[0].exists) return ok(cb)({ e: 'Nomor tidak terdaftar di WhatsApp' });
      const id = jidNormalizedUser(r[0].jid); const c = s.upChat(id, {}); s.emit('chat', c); ok(cb)({ id, name: c.name });
    } catch { ok(cb)({ e: 'Gagal memeriksa nomor' }); }
  });
  sock.on('avatar', async (d, cb) => {
    const id = d && String(d.id || ''); if (!id || !s.sock || s.st.st !== 'open') return ok(cb)({});
    if (s.avc.has(id)) return ok(cb)({ url: s.avc.get(id) });
    try { const url = await s.sock.profilePictureUrl(id, 'preview'); s.avc.set(id, url || ''); ok(cb)({ url: url || '' }); } catch { s.avc.set(id, ''); ok(cb)({}); }
  });
});

/* ---------- pulihkan sesi tersimpan saat server restart ---------- */
for (const u of fs.readdirSync(DATA_DIR)) { if (ID.test(u) && fs.existsSync(path.join(DATA_DIR, u, 'auth', 'creds.json'))) { const s = get(u); if (s.st.st === 'connecting') s.start().catch(e => log.warn(e)); } }
process.on('unhandledRejection', (e) => log.warn(e));
http.listen(+PORT, () => console.log('DevConnect WA server :' + PORT));
       
