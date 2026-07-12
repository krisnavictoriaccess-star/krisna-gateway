require('dotenv').config();
const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient({});
const { default: makeWASocket, useMultiFileAuthState, DisconnectReason } = require('@whiskeysockets/baileys');
const pino = require('pino');
const fs = require('fs');
const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const axios = require('axios');
const rateLimit = require('express-rate-limit');
const morgan = require('morgan');

const app = express();
app.set('trust proxy', 1);

// Limit 100 requests per IP per minute
const apiLimiter = rateLimit({
    windowMs: 1 * 60 * 1000,
    max: 100,
    message: { status: false, message: 'Terlalu banyak request. Silakan coba lagi nanti.' }
});
app.use(apiLimiter);

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ==========================================
//        TERMINAL BEAUTIFIER (LOGGER)
// ==========================================
const COLORS = {
    reset: "\x1b[0m", blue: "\x1b[34m", green: "\x1b[32m", yellow: "\x1b[33m",
    red: "\x1b[31m", cyan: "\x1b[36m", magenta: "\x1b[35m", white: "\x1b[37m", gray: "\x1b[90m"
};

const originalLog = console.log;
const originalError = console.error;

function formatMessage(msgStr) {
    if (msgStr.includes('[WEBHOOK]')) return { color: COLORS.magenta, icon: '🪝', module: 'WEBHOOK', text: msgStr.replace(/\[.*?\]\s*/, '') };
    if (msgStr.includes('[WEBHOOK FAILED]') || msgStr.includes('[WEBHOOK-REPLY ERROR]')) return { color: COLORS.red, icon: '❌', module: 'WEBHOOK', text: msgStr.replace(/\[.*?\]\s*/, '') };
    if (msgStr.includes('[🤖 WEBHOOK-REPLY]')) return { color: COLORS.magenta, icon: '🤖', module: 'WEBHOOK', text: msgStr.replace(/\[.*?\]\s*/, '') };
    
    if (msgStr.includes('[🔄 SESSION]') || msgStr.includes('[✅ SESSION]') || msgStr.includes('[🗑️ SESSION]') || msgStr.includes('[SESSION]')) {
        let icon = msgStr.includes('✅') ? '✅' : msgStr.includes('🗑️') ? '🗑️' : '🔄';
        return { color: COLORS.green, icon: icon, module: 'SESSION', text: msgStr.replace(/\[.*?\]\s*/, '') };
    }
    
    if (msgStr.includes('[QUEUE WORKER]')) return { color: COLORS.blue, icon: '📤', module: 'QUEUE', text: msgStr.replace(/\[.*?\]\s*/, '') };
    if (msgStr.includes('[QUEUE WORKER ERROR]') || msgStr.includes('Gagal kirim')) return { color: COLORS.red, icon: '❌', module: 'QUEUE', text: msgStr.replace(/\[.*?\]\s*/, '') };
    
    if (msgStr.includes('[🤖 AUTO-REPLY]')) return { color: COLORS.cyan, icon: '🤖', module: 'AUTO-REPLY', text: msgStr.replace(/\[.*?\]\s*/, '') };
    if (msgStr.includes('[🧹 CLEANUP]')) return { color: COLORS.yellow, icon: '🧹', module: 'CLEANUP', text: msgStr.replace(/\[.*?\]\s*/, '') };
    if (msgStr.includes('[⚙️ BOOT]')) return { color: COLORS.white, icon: '⚙️', module: 'SYSTEM', text: msgStr.replace(/\[.*?\]\s*/, '') };
    if (msgStr.includes('[DATABASE]')) return { color: COLORS.yellow, icon: '💾', module: 'DATABASE', text: msgStr.replace(/\[.*?\]\s*/, '') };
    
    return { color: COLORS.white, icon: '💬', module: 'SYSTEM', text: msgStr };
}

console.log = function(...args) {
    const time = new Date().toLocaleTimeString('id-ID', { hour12: false });
    const msgStr = args.map(a => typeof a === 'object' ? JSON.stringify(a) : a).join(' ');
    const f = formatMessage(msgStr);
    originalLog(`${COLORS.gray}[${time}]${COLORS.reset} ${f.color}${f.icon} [${f.module}]${COLORS.reset} ${f.text}`);
};

console.error = function(...args) {
    const time = new Date().toLocaleTimeString('id-ID', { hour12: false });
    const msgStr = args.map(a => typeof a === 'object' ? JSON.stringify(a) : a).join(' ');
    const f = formatMessage(msgStr);
    originalError(`${COLORS.gray}[${time}]${COLORS.reset} ${COLORS.red}❌ [ERROR]${COLORS.reset} ${f.text}`);
};

// Custom Morgan untuk API Logs
app.use(morgan((tokens, req, res) => {
    const time = new Date().toLocaleTimeString('id-ID', { hour12: false });
    const method = tokens.method(req, res);
    const url = tokens.url(req, res);
    const status = tokens.status(req, res);
    const responseTime = tokens['response-time'](req, res);
    
    let statusColor = status >= 500 ? COLORS.red : status >= 400 ? COLORS.yellow : COLORS.green;
    
    return `${COLORS.gray}[${time}]${COLORS.reset} ${COLORS.cyan}🌐 [API]${COLORS.reset} ${method} ${url} ${statusColor}${status}${COLORS.reset} - ${responseTime} ms`;
}));

// Middleware tambahan untuk mencegah req.body undefined jika client lupa set Content-Type
app.use((req, res, next) => {
    if (!req.body) req.body = {};
    next();
});

const PORT = process.env.PORT || 8000;
const MASTER_SECRET_KEY = process.env.MASTER_SECRET_KEY || 'krisna_owner_secret';

const activeSessions = {};
const activeStores = {}; // Store untuk menampung data sinkronisasi WA (Contact, dll)
const userDeviceIndex = {}; // Untuk sistem Rotator (Round-Robin)
const deviceProcessing = {}; // Lock status per-device untuk antrean


// --- DAFTAR LIMIT PAKET & EXPIRATION (SaaS Tiers) ---
const PAKET_CONFIG = {
    'Free': { limit_pesan: 1000, max_devices: 1, expiry_days: 3 },
    'Lite': { limit_pesan: 10000, max_devices: 1, expiry_days: 30 },
    'Pro': { limit_pesan: 50000, max_devices: 3, expiry_days: 30 },
    'Premium': { limit_pesan: 500000, max_devices: 10, expiry_days: 30 }
};

const PAKET_RANK = { 'Free': 1, 'Lite': 2, 'Pro': 3, 'Premium': 4 };

// --- WEBHOOK SENDER ---
async function sendWebhook(url, payload) {
    if (!url) return null;
    try {
        const response = await axios.post(url, payload, { timeout: 5000 });
        console.log(`[WEBHOOK] Berhasil mengirim event ke ${url}`);
        return response.data;
    } catch (error) {
        console.error(`[WEBHOOK FAILED] Ke ${url}: ${error.message}`);
        return null;
    }
}

// --- FUNGSI MENGHITUNG DEVICE AKTIF ---
async function countOwnedDevices(apiKey) {
    return await prisma.device.count({ where: { api_key_id: apiKey } });
}

// --- CORE FUNCTION: INISIALISASI SESI WHATSAPP DINAMIS ---
async function initWhatsAppSession(sessionId) {
    if (activeSessions[sessionId]) return activeSessions[sessionId];

    const sessionFolder = `./sessions/session-${sessionId}`;
    const storeFile = `./sessions/store-${sessionId}.json`;
    
    // Inisialisasi Custom Store (Untuk Contacts)
    const store = { contacts: {} };
    if (fs.existsSync(storeFile)) {
        try { store.contacts = JSON.parse(fs.readFileSync(storeFile, 'utf-8')); } catch(e) {}
    }
    // Auto-save store setiap 10 detik
    const storeInterval = setInterval(() => {
        try { fs.writeFileSync(storeFile, JSON.stringify(store.contacts)); } catch (e) {}
    }, 10_000);

    const { state, saveCreds } = await useMultiFileAuthState(sessionFolder);
    
    const sock = makeWASocket({
        auth: state,
        logger: pino({ level: 'silent' }),
        printQRInTerminal: false,
        browser: ['Ubuntu', 'Chrome', '20.0.04'],
        syncFullHistory: false,
        
        // --- PATCH ANTI-STUCK (KEEPALIVE) ---
        keepAliveIntervalMs: 10000,         // Ping server WA setiap 10 detik agar sesi tidak ditendang (stale)
        connectTimeoutMs: 60000,            // Timeout jika WA server tidak merespon
        defaultQueryTimeoutMs: 60000,       // Batas waktu proses query WA
        retryRequestDelayMs: 5000,          // Delay jika request gagal
        markOnlineOnConnect: true           // Memaksa status menjadi Online saat terkoneksi
    });

    activeStores[sessionId] = store;

    // Sinkronisasi Kontak Custom
    sock.ev.on('contacts.upsert', (contacts) => {
        for (const contact of contacts) {
            store.contacts[contact.id] = Object.assign(store.contacts[contact.id] || {}, contact);
        }
    });
    
    sock.ev.on('messaging-history.set', ({ contacts }) => {
        if (contacts) {
            for (const contact of contacts) {
                store.contacts[contact.id] = Object.assign(store.contacts[contact.id] || {}, contact);
            }
        }
    });

    sock.ev.on('creds.update', saveCreds);

    const getDeviceData = async () => {
        try {
            return await prisma.device.findUnique({ where: { nomor_device: sessionId }, include: { apiKey: true } });
        } catch (e) {
            console.error('[DATABASE ERROR] Gagal mengambil data device:', e.message);
            return null;
        }
    };

    // Event: Koneksi Update
    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;
        if (connection === 'close') {
            const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log(`[🔄 SESSION] Sesi ${sessionId} terputus. Reconnect: ${shouldReconnect}`);
            
            await prisma.device.update({ where: { nomor_device: sessionId }, data: { status: 'disconnected' } }).catch(() => {});

            if (shouldReconnect) {
                delete activeSessions[sessionId];
                delete activeStores[sessionId];
                clearInterval(storeInterval);
                initWhatsAppSession(sessionId);
            } else {
                console.log(`[🗑️ SESSION] Sesi ${sessionId} Logout. Menghapus data...`);
                
                // Trigger Webhook Disconnected
                const device = await getDeviceData();
                if (device?.apiKey?.webhook_url) {
                    sendWebhook(device.apiKey.webhook_url, { event: 'device.disconnected', device: sessionId });
                }

                clearInterval(storeInterval);
                try { fs.rmSync(sessionFolder, { recursive: true, force: true }); } catch (e) {}
                try { fs.unlinkSync(storeFile); } catch (e) {}
                await prisma.device.delete({ where: { nomor_device: sessionId } }).catch(() => {});
                delete activeSessions[sessionId];
                delete activeStores[sessionId];
            }
        } else if (connection === 'open') {
            console.log(`[✅ SESSION] Sesi ${sessionId} TERHUBUNG & SIAP!`);
            await prisma.device.update({ where: { nomor_device: sessionId }, data: { status: 'connected' } }).catch(() => {});
        }
    });

    // Event: Pesan Masuk (Auto-Responder & Webhook)
    sock.ev.on('messages.upsert', async ({ messages, type }) => {
        if (type !== 'notify') return;
        const msg = messages[0];
        if (msg.key.fromMe) return;

        const senderJid = msg.key.remoteJid;
        if (senderJid.includes('@broadcast')) return; // Abaikan status WA

        // Ambil teks pesan (dari berbagai jenis format WA)
        const textMessage = msg.message?.conversation || 
                            msg.message?.extendedTextMessage?.text || 
                            msg.message?.imageMessage?.caption || "";

        const device = await getDeviceData();
        if (!device) return;

        // 1. AUTO-RESPONDER & INBOX LOGIC
        if (textMessage) {
            // Save to Inbox
            try {
                await prisma.messageInbox.create({
                    data: { nomor_device: sessionId, sender_jid: senderJid, message: textMessage }
                });
                console.log(`[DATABASE] Menyimpan pesan masuk dari ${senderJid}`);
            } catch(e) {}

            let autoReplies = [];
            try {
                autoReplies = await prisma.autoReply.findMany({ where: { nomor_device: device.nomor_device } });
            } catch(e) {
                console.error('[DATABASE ERROR] Gagal load auto-reply:', e.message);
            }
            
            for (const reply of autoReplies) {
                let isMatch = false;
                if (reply.match_type === 'exact' && textMessage.trim().toLowerCase() === reply.keyword.toLowerCase()) {
                    isMatch = true;
                } else if (reply.match_type === 'contains' && textMessage.toLowerCase().includes(reply.keyword.toLowerCase())) {
                    isMatch = true;
                }

                if (isMatch) {
                    try {
                        await sock.sendMessage(senderJid, { text: reply.response });
                        console.log(`[🤖 AUTO-REPLY] Membalas ke ${senderJid} untuk keyword: ${reply.keyword}`);
                    } catch (e) {
                        console.error(`[AUTO-REPLY ERROR]`, e.message);
                    }
                    break; // Hanya balas 1 kata kunci pertama yang cocok
                }
            }
        }

        // 2. WEBHOOK TRIGGER & SYNCHRONOUS REPLY
        if (device.apiKey?.webhook_url) {
            const webhookResponse = await sendWebhook(device.apiKey.webhook_url, {
                event: 'message.received',
                device: sessionId,
                data: msg
            });
            
            if (webhookResponse && webhookResponse.reply) {
                try {
                    await sock.sendMessage(senderJid, { text: webhookResponse.reply });
                    console.log(`[🤖 WEBHOOK-REPLY] Membalas instan ke ${senderJid}`);
                } catch (e) {
                    console.error(`[WEBHOOK-REPLY ERROR]`, e.message);
                }
            }
        }
        
        // 3. AUTO-READ (CENTANG BIRU)
        if (!msg.key.fromMe) {
            try { await sock.readMessages([msg.key]); } catch(e) {}
        }
    });

    // Event: Status Pesan Berubah
    sock.ev.on('messages.update', async (updates) => {
        const device = await getDeviceData();
        if (device?.apiKey?.webhook_url) {
            for (const update of updates) {
                if (update.update.status) {
                    sendWebhook(device.apiKey.webhook_url, {
                        event: 'message.status',
                        device: sessionId,
                        messageId: update.key.id,
                        status: update.update.status
                    });
                }
            }
        }
    });

    activeSessions[sessionId] = sock;
    return sock;
}

// --- DATABASE POLLING WORKER (PERSISTENT QUEUE) ---
async function processQueue() {
    try {
        // Ambil SEMUA device yang aktif dan sedang tidak terkunci (nganggur)
        const activeDeviceNumbers = Object.keys(activeSessions).filter(num => !deviceProcessing[num]);

        if (activeDeviceNumbers.length === 0) {
            setTimeout(processQueue, 2000);
            return;
        }

        // Cari pesan pertama untuk masing-masing device yang idle secara bersamaan
        for (const sender_device of activeDeviceNumbers) {
            const pendingMsg = await prisma.messageQueue.findFirst({
                where: { 
                    status: 'pending',
                    send_at: { lte: new Date() },
                    sender_device: sender_device
                },
                orderBy: { createdAt: 'asc' }
            });

            if (pendingMsg) {
                // Kunci device ini agar tidak memproses pesan lain sebelum delay selesai
                deviceProcessing[sender_device] = true;
                
                // Jalankan proses pengiriman secara asinkron (background)
                (async () => {
                    try {
                        await prisma.messageQueue.update({ where: { id: pendingMsg.id }, data: { status: 'processing' } });
                        const sock = activeSessions[sender_device];
                        
                        if (sock) {
                            const payload = JSON.parse(pendingMsg.payload);
                            
                            // SPINTAX ENGINE (Anti-Spam Randomizer)
                            if (payload.text) {
                                const spintaxRegex = /\{([^{}]*)\}/g;
                                while (spintaxRegex.test(payload.text)) {
                                    payload.text = payload.text.replace(spintaxRegex, (match, contents) => {
                                        const choices = contents.split('|');
                                        return choices[Math.floor(Math.random() * choices.length)];
                                    });
                                }
                            }
                            
                            await sock.sendMessage(pendingMsg.recipient_jid, payload);
                            await prisma.messageQueue.update({ where: { id: pendingMsg.id }, data: { status: 'sent' } });
                            
                            await prisma.apiKey.update({ where: { key: pendingMsg.api_key_id }, data: { terpakai_bulan_ini: { increment: 1 } } });
                            console.log(`[QUEUE WORKER] Sukses kirim pesan ke ${pendingMsg.recipient_jid} via ${sender_device}`);
                        } else {
                            await prisma.messageQueue.update({ where: { id: pendingMsg.id }, data: { status: 'failed', error_message: 'Sesi pengirim tidak aktif.' } });
                        }
                    } catch (err) {
                        try {
                            await prisma.messageQueue.update({ where: { id: pendingMsg.id }, data: { status: 'failed', error_message: err.message } });
                        } catch(dbErr) { console.error('[DATABASE ERROR] Gagal update status queue:', dbErr.message); }
                        console.error(`[QUEUE WORKER] Gagal kirim ke ${pendingMsg.recipient_jid} via ${sender_device}: ${err.message}`);
                    }

                    // RANDOM DELAY KHUSUS UNTUK DEVICE INI (4 - 10 Detik)
                    const delay = Math.floor(Math.random() * (10000 - 4000 + 1) + 4000);
                    setTimeout(() => {
                        deviceProcessing[sender_device] = false; // Buka kunci setelah delay selesai
                    }, delay);
                })();
            }
        }
    } catch (error) {
        console.error('[QUEUE WORKER ERROR]', error.message);
    }
    
    // Polling cepat (2 detik) untuk mencari pesan baru bagi device yang sedang nganggur
    setTimeout(processQueue, 2000);
}


// --- LOAD SESSIONS ON BOOT ---
async function loadSavedSessions() {
    // FIX STUCK QUEUE: Reset antrean yang nyangkut karena server crash
    await prisma.messageQueue.updateMany({
        where: { status: 'processing' },
        data: { status: 'pending' }
    });

    if (!fs.existsSync('./sessions')) fs.mkdirSync('./sessions');
    const devices = await prisma.device.findMany();
    for (const dev of devices) {
        console.log(`[⚙️ BOOT] Memulihkan: ${dev.nomor_device}`);
        initWhatsAppSession(dev.nomor_device);
    }
}

// --- MIDDLEWARES ---
const validateMasterKey = (req, res, next) => {
    const masterKey = req.headers['x-master-key'] || req.query.masterkey || req.body.masterkey;
    if (masterKey !== MASTER_SECRET_KEY) return res.status(401).json({ status: false, message: 'Akses ditolak. Master Key salah.' });
    next();
};

const validateApiKey = async (req, res, next) => {
    const apiKey = req.headers['x-api-key'] || req.query.apikey || req.body.apikey;
    if (!apiKey) return res.status(401).json({ status: false, message: 'API Key dibutuhkan.' });

    const keyData = await prisma.apiKey.findUnique({ where: { key: apiKey } });
    if (!keyData || keyData.status !== 'active') return res.status(403).json({ status: false, message: 'API Key tidak valid atau tidak aktif.' });

    if (new Date() > new Date(keyData.expired_at)) {
        await prisma.apiKey.update({ where: { key: apiKey }, data: { status: 'expired' } });
        return res.status(403).json({ status: false, message: 'API Key telah kedaluwarsa. Silakan perpanjang.' });
    }

    req.apiKeyData = keyData;
    next();
};

const checkQuotaMiddleware = async (req, res, next) => {
    const user = req.apiKeyData;
    if (user.terpakai_bulan_ini >= user.limit_pesan && user.paket !== 'Premium') {
        return res.status(402).json({ status: false, message: 'Kuota pesan Anda habis. Silakan upgrade.' });
    }
    next();
};

const validateDeviceOwnership = async (req, res, next) => {
    const user = req.apiKeyData;
    let rawSender = req.headers['sender_id'] || req.query.sender_id || req.body.sender_id;
    let sender_id = rawSender ? String(rawSender).replace(/[^0-9]/g, '') : '';
    
    let isOwner;
    if (sender_id) {
        isOwner = await prisma.device.findFirst({ where: { nomor_device: sender_id, api_key_id: user.key } });
    }
    
    // Fallback: Jika sender_id tidak ada atau tidak valid, jalankan logika Rotator (Round-Robin)
    if (!isOwner) {
        const userDevices = await prisma.device.findMany({ 
            where: { api_key_id: user.key, status: 'connected' },
            orderBy: { id: 'asc' }
        });
        
        if (userDevices.length > 0) {
            if (userDeviceIndex[user.key] === undefined) userDeviceIndex[user.key] = 0;
            
            let currentIndex = userDeviceIndex[user.key];
            if (currentIndex >= userDevices.length) currentIndex = 0;
            
            sender_id = userDevices[currentIndex].nomor_device;
            isOwner = true; // Rotator berhasil memilih device
            
            userDeviceIndex[user.key] = currentIndex + 1; // Increment giliran
        } else {
            // Jika tidak ada yang connected, fallback ke semua device (meskipun disconnected, agar masuk queue)
            const allUserDevices = await prisma.device.findMany({ where: { api_key_id: user.key } });
            if (allUserDevices.length > 0) {
                 isOwner = true;
                 sender_id = allUserDevices[0].nomor_device;
            }
        }
    }

    if (!isOwner) return res.status(403).json({ status: false, message: 'Nomor pengirim bukan milik Anda atau Anda belum mendaftarkan device.' });
    
    req.cleanSender = sender_id;
    next();
}

// ==========================================
//          UPTIMEROBOT PING ENDPOINT
// ==========================================
app.get('/ping', (req, res) => {
    res.status(200).json({ status: true, message: 'WA Gateway Enterprise is online.', timestamp: new Date() });
});

// ==========================================
//          ENDPOINT MANAJEMEN API KEY
// ==========================================
// Admin: Melihat semua API Key
app.get('/api-key/list', validateMasterKey, async (req, res) => {
    try {
        const keys = await prisma.apiKey.findMany({ orderBy: { key: 'desc' } });
        return res.status(200).json({ status: true, data: keys });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

app.post('/api-key/generate', validateMasterKey, async (req, res) => {
    try {
        const { paket = 'Free' } = req.body;
        if (!PAKET_CONFIG[paket]) return res.status(400).json({ status: false, message: 'Paket tidak valid.' });

        const config = PAKET_CONFIG[paket];
        const newApiKey = 'KEY-' + crypto.randomBytes(16).toString('hex').toUpperCase();
        
        const now = new Date();
        const expiredAt = new Date(now);
        expiredAt.setDate(now.getDate() + config.expiry_days);

        const keyData = await prisma.apiKey.create({
            data: {
                key: newApiKey, paket: paket, limit_pesan: config.limit_pesan, max_devices: config.max_devices,
                terpakai_bulan_ini: 0, last_reset_month: now.toISOString().substring(0, 7), expired_at: expiredAt
            }
        });
        return res.status(201).json({ status: true, message: `API Key ${paket} berhasil dibuat.`, data: keyData });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

app.post('/api-key/upgrade', validateMasterKey, async (req, res) => {
    try {
        const { target_api_key, nama_paket } = req.body;
        if (!target_api_key || !nama_paket) return res.status(400).json({ status: false, message: 'Parameter tidak lengkap.' });
        if (!PAKET_CONFIG[nama_paket]) return res.status(400).json({ status: false, message: 'Nama paket salah!' });

        const currentUser = await prisma.apiKey.findUnique({ where: { key: target_api_key } });
        if (!currentUser) return res.status(404).json({ status: false, message: 'API Key tidak ditemukan.' });

        if (PAKET_RANK[nama_paket] < PAKET_RANK[currentUser.paket]) {
            return res.status(400).json({ status: false, message: 'Tidak bisa downgrade ke paket yang lebih rendah.' });
        }

        const config = PAKET_CONFIG[nama_paket];
        const now = new Date();
        const expiredAt = new Date(now);
        expiredAt.setDate(now.getDate() + config.expiry_days);

        const updated = await prisma.apiKey.update({
            where: { key: target_api_key },
            data: { paket: nama_paket, limit_pesan: config.limit_pesan, max_devices: config.max_devices, terpakai_bulan_ini: 0, status: 'active', expired_at: expiredAt }
        });
        return res.status(200).json({ status: true, message: `Sukses di-upgrade ke ${nama_paket}`, data: updated });
    } catch (error) { return res.status(500).json({ status: false, message: 'Gagal update.', error: error.message }); }
});

app.post('/webhook/set', validateApiKey, async (req, res) => {
    try {
        const { webhook_url } = req.body;
        const updated = await prisma.apiKey.update({ where: { key: req.apiKeyData.key }, data: { webhook_url: webhook_url || null } });
        return res.status(200).json({ status: true, message: 'URL Webhook berhasil diperbarui.', data: updated });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

// User: Cek status, kuota, paket dan masa aktif API Key saat ini
app.get('/api-key/info', validateApiKey, async (req, res) => {
    try {
        // req.apiKeyData sudah diekstrak oleh middleware validateApiKey
        // namun kita hilangkan properti yang tidak perlu jika diinginkan, atau kirim semuanya.
        return res.status(200).json({ 
            status: true, 
            data: req.apiKeyData 
        });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

// ==========================================
//           ENDPOINT DEVICE / SAAS
// ==========================================
// Admin: Melihat semua device di sistem
app.get('/device/all', validateMasterKey, async (req, res) => {
    try {
        const devices = await prisma.device.findMany({ orderBy: { id: 'desc' } });
        return res.status(200).json({ status: true, data: devices });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

// User: Melihat device miliknya
app.get('/device/list', validateApiKey, async (req, res) => {
    try {
        const devices = await prisma.device.findMany({ where: { api_key_id: req.apiKeyData.key } });
        return res.status(200).json({ status: true, data: devices });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

app.post('/device/add', validateApiKey, async (req, res) => {
    const { nomor_device } = req.body;
    if (!nomor_device) return res.status(400).json({ status: false, message: 'Parameter nomor_device wajib diisi.' });

    const cleanDevice = nomor_device.replace(/[^0-9]/g, '');
    const user = req.apiKeyData;

    try {
        const existingDevice = await prisma.device.findUnique({ where: { nomor_device: cleanDevice } });
        if (!existingDevice) {
            const currentCount = await countOwnedDevices(user.key);
            if (currentCount >= user.max_devices) return res.status(403).json({ status: false, message: `Slot device penuh (${currentCount}/${user.max_devices}).` });
            await prisma.device.create({ data: { nomor_device: cleanDevice, api_key_id: user.key } });
        } else if (existingDevice.api_key_id !== user.key) {
            return res.status(403).json({ status: false, message: 'Nomor device ini sudah dipakai oleh API Key lain.' });
        }

        const sock = await initWhatsAppSession(cleanDevice);
        if (sock.authState.creds.registered) return res.status(200).json({ status: true, message: 'Terhubung.', device_status: 'CONNECTED' });

        // Delay 3 detik agar WebSocket Baileys siap menerima pairing request
        await new Promise(resolve => setTimeout(resolve, 3000));
        
        let pairingCode = await sock.requestPairingCode(cleanDevice);
        return res.status(200).json({ status: true, message: 'Silakan masukkan kode pairing ini di WhatsApp.', pairing_code: pairingCode, device_status: 'WAITING_PAIRING' });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

app.post('/device/delete', validateApiKey, validateDeviceOwnership, async (req, res) => {
    const sock = activeSessions[req.cleanSender];
    try {
        if (sock) {
            await sock.logout(); // Memicu event connection.update "close" dengan "loggedOut"
        } else {
            // Jika tidak ada aktif di RAM, hapus paksa file dan DB
            try { fs.rmSync(`./sessions/session-${req.cleanSender}`, { recursive: true, force: true }); } catch (e) {}
            try { fs.unlinkSync(`./sessions/store-${req.cleanSender}.json`); } catch (e) {}
            await prisma.device.delete({ where: { nomor_device: req.cleanSender } }).catch(() => {});
        }
        return res.status(200).json({ status: true, message: `Device ${req.cleanSender} berhasil dihapus dan dilogout.` });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

app.get('/group/list', validateApiKey, validateDeviceOwnership, async (req, res) => {
    const sock = activeSessions[req.cleanSender];
    if (!sock) return res.status(404).json({ status: false, message: 'Sesi perangkat tidak aktif' });
    try {
        const groups = await sock.groupFetchAllParticipating();
        const groupList = Object.values(groups).map(g => ({ id: g.id, subject: g.subject, participants: g.participants.length }));
        return res.status(200).json({ status: true, data: groupList });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

app.get('/contact/list', validateApiKey, validateDeviceOwnership, async (req, res) => {
    const store = activeStores[req.cleanSender];
    if (!store) return res.status(404).json({ status: false, message: 'Store kontak untuk perangkat tidak ditemukan atau sesi belum siap.' });
    
    try {
        const contacts = Object.values(store.contacts).map(c => ({
            id: c.id,
            name: c.name || c.notify || c.verifiedName || 'Unknown'
        }));
        return res.status(200).json({ status: true, data: contacts });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

app.get('/inbox', validateApiKey, async (req, res) => {
    try {
        const user = req.apiKeyData;
        const limit = parseInt(req.query.limit) || 100;
        const page = parseInt(req.query.page) || 1;
        const skip = (page - 1) * limit;
        
        let deviceFilter = {};
        // Opsional: Filter berdasarkan 1 nomor spesifik
        if (req.query.sender_id || req.query.device) {
             const cleanDevice = String(req.query.sender_id || req.query.device).replace(/[^0-9]/g, '');
             const isOwner = await prisma.device.findFirst({ where: { nomor_device: cleanDevice, api_key_id: user.key }});
             if (!isOwner) return res.status(403).json({ status: false, message: 'Nomor pengirim bukan milik Anda.' });
             deviceFilter = { nomor_device: cleanDevice };
        } else {
             // Secara default, tarik inbox dari SELURUH nomor yang dimiliki oleh API Key ini
             const userDevices = await prisma.device.findMany({ where: { api_key_id: user.key }, select: { nomor_device: true }});
             deviceFilter = { nomor_device: { in: userDevices.map(d => d.nomor_device) } };
        }

        const messages = await prisma.messageInbox.findMany({
            where: deviceFilter,
            orderBy: { createdAt: 'desc' },
            take: limit > 500 ? 500 : limit, // Maksimal 500
            skip: skip
        });
        
        const total = await prisma.messageInbox.count({ where: deviceFilter });

        return res.status(200).json({ 
            status: true, 
            data: messages,
            pagination: { page, limit, total }
        });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

// ==========================================
//          ENDPOINT QUEUE (ANTREAN PESAN)
// ==========================================
// Admin: Melihat semua antrean (maks 500 terbaru)
app.get('/queue/all', validateMasterKey, async (req, res) => {
    try {
        const queues = await prisma.messageQueue.findMany({
            orderBy: { createdAt: 'desc' },
            take: 500
        });
        return res.status(200).json({ status: true, data: queues });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

// User: Melihat antrean miliknya (maks 100 terbaru)
app.get('/queue/my', validateApiKey, async (req, res) => {
    try {
        const queues = await prisma.messageQueue.findMany({
            where: { api_key_id: req.apiKeyData.key },
            orderBy: { createdAt: 'desc' },
            take: 100
        });
        return res.status(200).json({ status: true, data: queues });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

// ==========================================
//          AUTO-RESPONDER (CHATBOT)
// ==========================================
app.post('/auto-reply/add', validateApiKey, validateDeviceOwnership, async (req, res) => {
    const { keyword, response, match_type = 'exact' } = req.body;
    if (!keyword || !response) return res.status(400).json({ status: false, message: 'Keyword dan response wajib diisi.' });
    try {
        const reply = await prisma.autoReply.create({
            data: { keyword, response, match_type, nomor_device: req.cleanSender }
        });
        return res.status(201).json({ status: true, message: 'Auto-reply ditambahkan.', data: reply });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

app.get('/auto-reply/list', validateApiKey, validateDeviceOwnership, async (req, res) => {
    try {
        const replies = await prisma.autoReply.findMany({ where: { nomor_device: req.cleanSender } });
        return res.status(200).json({ status: true, data: replies });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

app.post('/auto-reply/delete', validateApiKey, validateDeviceOwnership, async (req, res) => {
    const { id } = req.body;
    if (!id) return res.status(400).json({ status: false, message: 'Parameter ID wajib.' });
    try {
        const reply = await prisma.autoReply.findFirst({ where: { id: parseInt(id), nomor_device: req.cleanSender } });
        if (!reply) return res.status(404).json({ status: false, message: 'Data tidak ditemukan atau bukan milik device ini.' });
        await prisma.autoReply.delete({ where: { id: parseInt(id) } });
        return res.status(200).json({ status: true, message: 'Berhasil dihapus.' });
    } catch (error) { return res.status(500).json({ status: false, error: error.message }); }
});

// ==========================================
//          ENDPOINT KIRIM PESAN & MEDIA
// ==========================================
// Helper function to insert into Database Queue
async function addToQueue(req, res, recipient_jid, payload) {
    try {
        let sendAt = new Date();
        if (req.body.send_at) { // Dukungan Penjadwalan Broadcast
            let inputDate = req.body.send_at;
            
            // Konversi dari format YYYY-MM-DD HH:mm
            if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(inputDate)) {
                inputDate = inputDate.replace(' ', 'T') + ':00+07:00';
            } 
            // Konversi dari format DD-MM-YYYY HH:mm
            else if (/^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$/.test(inputDate)) {
                const parts = inputDate.split(' ');
                const dateParts = parts[0].split('-');
                inputDate = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]}T${parts[1]}:00+07:00`;
            }

            sendAt = new Date(inputDate);
        }
        await prisma.messageQueue.create({
            data: {
                sender_device: req.cleanSender,
                recipient_jid: recipient_jid,
                payload: JSON.stringify(payload),
                send_at: sendAt,
                api_key_id: req.apiKeyData.key
            }
        });
        
        // Kalkulasi Sisa Kuota
        const user = await prisma.apiKey.findUnique({ where: { key: req.apiKeyData.key } });
        const sisaKuota = user.limit_pesan - user.terpakai_bulan_ini - 1; // -1 untuk pesan yang barusan masuk antrean
        const realKuota = sisaKuota < 0 ? 0 : sisaKuota;

        return res.status(200).json({ 
            status: true, 
            message: 'Pesan telah dimasukkan ke dalam antrean (Database Queue).',
            sisa_kuota: user.paket === 'Premium' ? 'UNLIMITED' : realKuota
        });
    } catch (error) {
        return res.status(500).json({ status: false, error: error.message });
    }
}

app.post('/kirim-pesan', validateApiKey, validateDeviceOwnership, checkQuotaMiddleware, async (req, res) => {
    const { nomor, pesan } = req.body;
    if (!nomor || !pesan) return res.status(400).json({ status: false, message: 'Parameter nomor & pesan wajib.' });
    const cleanNomor = nomor.replace(/[^0-9]/g, '');
    const jid = `${cleanNomor}@s.whatsapp.net`;
    return addToQueue(req, res, jid, { text: pesan });
});

app.post('/kirim-massal', validateApiKey, validateDeviceOwnership, checkQuotaMiddleware, async (req, res) => {
    const { pesan_list } = req.body; // Array of {nomor, pesan}
    if (!pesan_list || !Array.isArray(pesan_list)) return res.status(400).json({ status: false, message: 'Format salah. Butuh array pesan_list.' });
    
    const user = req.apiKeyData;
    if (user.paket !== 'Premium' && (user.terpakai_bulan_ini + pesan_list.length) > user.limit_pesan) {
         return res.status(402).json({ status: false, message: 'Kuota pesan Anda tidak cukup untuk broadcast massal ini.' });
    }

    try {
        const queueData = pesan_list.map(item => ({
             sender_device: req.cleanSender, // Mendukung Rotator dari middleware
             recipient_jid: `${String(item.nomor).replace(/[^0-9]/g, '')}@s.whatsapp.net`,
             payload: JSON.stringify({ text: item.pesan }),
             api_key_id: user.key
        }));
        
        await prisma.messageQueue.createMany({ data: queueData });
        return res.status(200).json({ status: true, message: `${pesan_list.length} pesan berhasil diantrekan secara massal.`});
    } catch(err) {
        return res.status(500).json({ status: false, error: err.message });
    }
});

app.post('/kirim-grup', validateApiKey, validateDeviceOwnership, checkQuotaMiddleware, async (req, res) => {
    const { group_id, pesan } = req.body; // group_id = misal 123456789@g.us
    if (!group_id || !pesan) return res.status(400).json({ status: false, message: 'Parameter group_id & pesan wajib.' });
    return addToQueue(req, res, group_id, { text: pesan });
});

app.post('/kirim-lokasi', validateApiKey, validateDeviceOwnership, checkQuotaMiddleware, async (req, res) => {
    const { nomor, lat, long } = req.body;
    if (!nomor || !lat || !long) return res.status(400).json({ status: false, message: 'Parameter nomor, lat, long wajib.' });
    const jid = `${nomor.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
    return addToQueue(req, res, jid, { location: { degreesLatitude: parseFloat(lat), degreesLongitude: parseFloat(long) } });
});

app.post('/kirim-polling', validateApiKey, validateDeviceOwnership, checkQuotaMiddleware, async (req, res) => {
    const { nomor, nama_polling, opsi, multiple_choice = false } = req.body;
    if (!nomor || !nama_polling || !opsi || !Array.isArray(opsi)) return res.status(400).json({ status: false, message: 'Parameter tidak valid. Opsi harus berupa array.' });
    const jid = `${nomor.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
    return addToQueue(req, res, jid, {
        poll: {
            name: nama_polling,
            values: opsi,
            selectableCount: multiple_choice ? 0 : 1
        }
    });
});

app.post('/kirim-media', validateApiKey, validateDeviceOwnership, checkQuotaMiddleware, async (req, res) => {
    const { nomor, url, tipe, caption = '' } = req.body; // tipe = image, video, document
    if (!nomor || !url || !tipe) return res.status(400).json({ status: false, message: 'Parameter nomor, url, dan tipe wajib.' });
    
    const jid = `${nomor.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
    let payload = {};
    if (tipe === 'image') payload = { image: { url }, caption };
    else if (tipe === 'video') payload = { video: { url }, caption };
    else if (tipe === 'document') payload = { document: { url }, mimetype: 'application/pdf', fileName: caption || 'document.pdf' };
    else return res.status(400).json({ status: false, message: 'Tipe media harus image, video, atau document.' });

    return addToQueue(req, res, jid, payload);
});

app.post('/kirim-vcard', validateApiKey, validateDeviceOwnership, checkQuotaMiddleware, async (req, res) => {
    const { nomor, nama_kontak, nomor_kontak } = req.body;
    if (!nomor || !nama_kontak || !nomor_kontak) return res.status(400).json({ status: false, message: 'Parameter wajib.' });
    
    const jid = `${nomor.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
    const cleanContact = nomor_kontak.replace(/[^0-9]/g, '');
    const vcard = 'BEGIN:VCARD\n' 
                + 'VERSION:3.0\n' 
                + `FN:${nama_kontak}\n` 
                + `TEL;type=CELL;type=VOICE;waid=${cleanContact}:+${cleanContact}\n` 
                + 'END:VCARD';

    return addToQueue(req, res, jid, {
        contacts: { displayName: nama_kontak, contacts: [{ vcard }] }
    });
});

// --- EXECUTE ON STARTUP ---
app.listen(PORT, async () => {
    console.log('\n\x1b[36m%s\x1b[0m', '██╗  ██╗██████╗ ██╗███████╗███╗   ██╗ █████╗ ');
    console.log('\x1b[36m%s\x1b[0m', '██║ ██╔╝██╔══██╗██║██╔════╝████╗  ██║██╔══██╗');
    console.log('\x1b[36m%s\x1b[0m', '█████╔╝ ██████╔╝██║███████╗██╔██╗ ██║███████║');
    console.log('\x1b[36m%s\x1b[0m', '██╔═██╗ ██╔══██╗██║╚════██║██║╚██╗██║██╔══██║');
    console.log('\x1b[36m%s\x1b[0m', '██║  ██╗██║  ██║██║███████║██║ ╚████║██║  ██║');
    console.log('\x1b[36m%s\x1b[0m', '╚═╝  ╚═╝╚═╝  ╚═╝╚═╝╚══════╝╚═╝  ╚═══╝╚═╝  ╚═╝');
    console.log('\x1b[33m%s\x1b[0m', '===================================================');
    console.log('\x1b[32m%s\x1b[0m', `🚀 KRISNA GATEWAY (API Server) Berjalan di Port: ${PORT}`);
    console.log('\x1b[33m%s\x1b[0m', '===================================================\n');
    
    // Resume worker & check connected sessions
    processQueue();
    await loadSavedSessions();
    
    // AUTO CLEANUP INBOX (Hapus pesan yang umurnya > 30 Hari)
    setInterval(async () => {
        try {
            const date30DaysAgo = new Date();
            date30DaysAgo.setDate(date30DaysAgo.getDate() - 30);
            const deleted = await prisma.messageInbox.deleteMany({
                where: { createdAt: { lt: date30DaysAgo } }
            });
            if (deleted.count > 0) console.log(`[🧹 CLEANUP] Menghapus ${deleted.count} pesan inbox lawas.`);
        } catch(e) {}
    }, 24 * 60 * 60 * 1000); // Jalan setiap 24 Jam
});

