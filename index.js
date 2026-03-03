const fs = require('fs');
const readline = require('readline');
const axios = require('axios');
const path = require('path');
const mysql = require('mysql2/promise');

// Đọc cấu hình từ file
const config = require('./config.json');
// Sử dụng đường dẫn log từ file cấu hình (CentOS)
const LOG_FILE = config.api.log_path;
const BUFFER_FILE = path.join(__dirname, 'log_buffer.ndjson');
const CACHE_FILE = path.join(__dirname, 'token_cache.json');

// CẤU HÌNH BATCHING
const BATCH_SIZE = 50;
const FLUSH_INTERVAL = 2000;
const MAX_BUFFER_SIZE = 5000;

let logBuffer = [];          
let flushTimer = null;
let isSending = false;       // Biến khóa để đảm bảo không gửi chồng chéo

// Cache Token
let tokenCache = {};
let dbPool = null;

// Regex phân tích dòng log
const LOG_REGEX = /^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3})\s+(\w+)\s+.*IP:\[([^\]]+)\]\s+(.*)/;

// ================= DATABASE & CACHE LOGIC =================

function loadCache() {
    if (fs.existsSync(CACHE_FILE)) {
        try {
            const data = fs.readFileSync(CACHE_FILE, 'utf8');
            tokenCache = JSON.parse(data);
            console.log(`[Cache] Loaded ${Object.keys(tokenCache).length} tokens from disk.`);
        } catch (e) {
            console.error('[Cache] Failed to load cache from disk:', e.message);
        }
    }
}

function saveCache() {
    try {
        fs.writeFileSync(CACHE_FILE, JSON.stringify(tokenCache, null, 2), 'utf8');
    } catch (e) {
        console.error('[Cache] Failed to save cache to disk:', e.message);
    }
}

function initDatabase() {
    if (config.database && config.database.host) {
        dbPool = mysql.createPool({
            host: config.database.host,
            user: config.database.user,
            password: config.database.password,
            database: config.database.database,
            port: config.database.port || 3306,
            waitForConnections: true,
            connectionLimit: 10,
            queueLimit: 0
        });
        console.log(`[DB] MySQL pool initialized.`);
    } else {
        console.warn(`[DB] No database configuration found.`);
    }
}

async function getTokenHid(tokenCode) {
    if (!tokenCode) return tokenCode;
    
    // 1. Kiểm tra RAM Cache
    if (tokenCache[tokenCode]) {
        return tokenCache[tokenCode];
    }

    // 2. Nếu chưa có kết nối DB thì trả về gốc
    if (!dbPool) return tokenCode;

    // 3. Query xuống Database
    try {
        const [rows] = await dbPool.execute(
            'SELECT token_hid FROM token_ms WHERE CAST(token_code AS CHAR) = ? LIMIT 1',
            [tokenCode.toString().trim()]
        );

        if (rows.length > 0 && rows[0].token_hid) {
            const tokenHid = rows[0].token_hid;
            // Lưu vào Cache RAM và ghi ra ổ cứng
            tokenCache[tokenCode] = tokenHid;
            saveCache();
            return tokenHid;
        }
    } catch (err) {
        console.error(`[DB] Error querying tokenCode ${tokenCode}:`, err.message);
    }

    // Nếu không tìm thấy trong DB, lưu lại giá trị gốc để lần sau không query lại làm chậm DB
    tokenCache[tokenCode] = tokenCode;
    saveCache();
    
    return tokenCode;
}

// ================= LOG PROCESSING LOGIC =================

function countDiskBuffer() {
    if (!fs.existsSync(BUFFER_FILE)) return 0;
    const data = fs.readFileSync(BUFFER_FILE, 'utf8');
    if (!data.trim()) return 0;
    return data.split('\n').filter(Boolean).length;
}

// Hàm gửi Batch log lên API
async function flushLogs() {
    // Nếu đang gửi dở (mạng chậm) hoặc không có log thì thôi
    if (isSending) return;

	const chunkToSend = readBatchFromDisk(BATCH_SIZE);
	if (chunkToSend.length === 0) return;

    isSending = true; // Bắt đầu khóa

    // Tạo body dạng NDJSON
    const ndjsonBody = chunkToSend.map(item => JSON.stringify(item)).join('\n');

    try {
        await axios.post(config.api.url, ndjsonBody, {
            auth: {
                username: config.api.auth.username,
                password: config.api.auth.password
            },
            headers: config.api.headers,
            timeout: 10000 // Timeout 10s để tránh treo
        });
        
        // --- GỬI THÀNH CÔNG ---
        removeBatchFromDisk(chunkToSend.length);
        
        const remaining = countDiskBuffer();
		console.log(`[TMS2] Sent ${chunkToSend.length} logs successfully. Remaining buffer: ${remaining}`);

        if (countDiskBuffer() > 0) {
            isSending = false;
            setTimeout(flushLogs, 100); 
            return;
        }

    } catch (error) {
        const errorMsg = error.response ? `Status ${error.response.status}` : error.message;
        console.error(`[RETRY] Connection failed (${errorMsg}). Keeping logs in queue. Total buffered: ${countDiskBuffer()}`);
    } finally {
        isSending = false; // Mở khóa
    }
}

function startBatchTimer() {
    if (flushTimer) clearInterval(flushTimer);
    flushTimer = setInterval(() => {
        flushLogs();
    }, FLUSH_INTERVAL);
}

function appendToDisk(payload) {
    try {
        fs.appendFileSync(
            BUFFER_FILE,
            JSON.stringify(payload) + '\n',
            'utf8'
        );
    } catch (err) {
        console.error('[ERROR] Cannot write buffer file:', err.message);
    }
}

// Hàm parse và xử lý một dòng log (Đã chuyển thành async)
async function processLine(line) {
    if (!line || !line.trim()) return;

    const match = line.match(LOG_REGEX);
    if (!match) return;

    const [_, rawTime, level, ip, content] = match;

    if (level.toUpperCase().trim() !== 'INFO') return;

    let timestamp = "0";
    try {
        const isoTime = rawTime.replace(',', '.').trim(); 
        timestamp = Math.floor(new Date(isoTime).getTime() / 1000).toString();
    } catch (e) {
        timestamp = Math.floor(Date.now() / 1000).toString();
    }

    const extract = (key) => {
        const regex = new RegExp(`${key}=\\s*([^\\s]*)`);
        const m = content.match(regex);
        return (m && m[1]) ? m[1].trim() : '';
    };

    const rawAction = extract('actionCode');
    if (!rawAction) return; 

    const action = rawAction.toLowerCase();
    const tokenCode = extract('tokenCode');

    // QUERY DATABASE / CACHE LẤY token_hid
    const username = await getTokenHid(tokenCode);

    let msg = content;
    const msgKey = "logCreatedByCertSn=";
    const msgIndex = content.indexOf(msgKey);
    if (msgIndex !== -1) {
        msg = content.substring(msgIndex + msgKey.length).trim();
    } else {
        msg = content.trim();
    }

    const payload = {
        _time: timestamp.trim(),
        _msg: msg,
        username: username, // Sử dụng username đã được query DB hoặc lấy từ cache
        ip: ip.trim(),
        domain: "tms2.smartsign.com.vn",
        mirror: "0",
        action: action,
        status: "success",
        user_agent: "", 
        location: "" 
    };
	appendToDisk(payload);
}

function readBatchFromDisk(limit) {
    if (!fs.existsSync(BUFFER_FILE)) return [];

    const data = fs.readFileSync(BUFFER_FILE, 'utf8');
    if (!data.trim()) return [];

    return data
        .split('\n')
        .filter(Boolean)
        .slice(0, limit)
        .map(line => JSON.parse(line));
}

function removeBatchFromDisk(count) {
    const data = fs.readFileSync(BUFFER_FILE, 'utf8');
    const lines = data.split('\n').filter(Boolean);

    const remaining = lines.slice(count);
    fs.writeFileSync(
        BUFFER_FILE,
        remaining.length ? remaining.join('\n') + '\n' : '',
        'utf8'
    );
}

// Sử dụng sự kiện 'line' với pause/resume để tương thích với các phiên bản Node.js cũ
function processStream(stream) {
    return new Promise((resolve, reject) => {
        const rl = readline.createInterface({
            input: stream,
            crlfDelay: Infinity
        });

        rl.on('line', async (line) => {
            // Tạm dừng đọc dòng tiếp theo để chờ xử lý xong dòng hiện tại (bao gồm query DB)
            rl.pause(); 
            try {
                await processLine(line);
            } catch (err) {
                console.error("Error processing line:", err);
            } finally {
                rl.resume(); // Tiếp tục đọc
            }
        });

        rl.on('close', resolve);
        rl.on('error', reject);
    });
}

// Hàm chính
function main() {
    console.log(`Starting Log Shipper... Watching: ${LOG_FILE}`);
    console.log(`Target API: ${config.api.url}`);
    
    loadCache();
    initDatabase();
    startBatchTimer();

    // 1. Kiểm tra và đọc nội dung hiện có nếu file tồn tại
    if (fs.existsSync(LOG_FILE)) {
        console.log("Reading existing logs...");
        const initialStream = fs.createReadStream(LOG_FILE);
        // Chạy bất đồng bộ
        processStream(initialStream).catch(e => console.error("Error processing initial log stream:", e));
    } else {
        console.warn(`File not found: ${LOG_FILE}. Waiting for file creation...`);
    }

    // 2. Theo dõi thay đổi (bao gồm cả Log Rotation)
    fs.watchFile(LOG_FILE, { interval: 1000 }, (curr, prev) => {
        if (curr.size > prev.size) {
            const newStream = fs.createReadStream(LOG_FILE, {
                start: prev.size,
                end: curr.size
            });
            processStream(newStream).catch(e => console.error("Error processing tail stream:", e));
        } 
        else if (curr.size < prev.size) {
            console.log("Log rotation detected (new file started). Reading from beginning.");
            const rotateStream = fs.createReadStream(LOG_FILE);
            processStream(rotateStream).catch(e => console.error("Error processing rotated stream:", e));
        }
    });
}

main();