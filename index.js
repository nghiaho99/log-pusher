const fs = require('fs');
const readline = require('readline');
const axios = require('axios');
const path = require('path');

// Đọc cấu hình từ file
const config = require('./config.json');
// Sử dụng đường dẫn log từ file cấu hình (CentOS)
const LOG_FILE = config.api.log_path;
const BUFFER_FILE = path.join(__dirname, 'log_buffer.ndjson');

// CẤU HÌNH BATCHING
const BATCH_SIZE = 50;
const FLUSH_INTERVAL = 2000;
const MAX_BUFFER_SIZE = 5000;

let logBuffer = [];          
let flushTimer = null;
let isSending = false;       // Biến khóa để đảm bảo không gửi chồng chéo

// Regex phân tích dòng log
const LOG_REGEX = /^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3})\s+(\w+)\s+.*IP:\[([^\]]+)\]\s+(.*)/;

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
        // Bây giờ mới xóa số log đã gửi khỏi buffer
        removeBatchFromDisk(chunkToSend.length);
        
        const remaining = countDiskBuffer();
		console.log(`[TMS2] Sent ${chunkToSend.length} logs successfully. Remaining buffer: ${remaining}`);

        
        // Nếu buffer vẫn còn nhiều (do tích tụ lâu), gọi gửi tiếp ngay lập tức không cần đợi 5s
        if (logBuffer.length > 0) {
            isSending = false;
            setTimeout(flushLogs, 100); // Nghỉ 100ms rồi gửi tiếp luôn
            return;
        }

    } catch (error) {
        // --- GỬI THẤT BẠI ---
        // Không làm gì cả, log vẫn nằm nguyên trong logBuffer.
        // Lần sau hàm này chạy lại sẽ lấy đúng đám log này gửi lại.
        
        const errorMsg = error.response ? `Status ${error.response.status}` : error.message;
        console.error(`[RETRY] Connection failed (${errorMsg}). Keeping ${chunkToSend.length} logs in queue. Total buffered: ${logBuffer.length}`);
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

// Hàm parse và xử lý một dòng log
function processLine(line) {
    // ... (Phần logic parse giữ nguyên)
    if (!line || !line.trim()) return;

    const match = line.match(LOG_REGEX);
    if (!match) return;

    const [_, rawTime, level, ip, content] = match;

    if (level.toUpperCase().trim() !== 'INFO') return;

    // ... (Logic parse timestamp giữ nguyên)
    let timestamp = "0";
    try {
        const isoTime = rawTime.replace(',', '.').trim(); 
        timestamp = Math.floor(new Date(isoTime).getTime() / 1000).toString();
    } catch (e) {
        timestamp = Math.floor(Date.now() / 1000).toString();
    }

    // Helper extract
    const extract = (key) => {
        const regex = new RegExp(`${key}=\\s*([^\\s]*)`);
        const m = content.match(regex);
        return (m && m[1]) ? m[1].trim() : '';
    };

    const rawAction = extract('actionCode');
    if (!rawAction) return; 

    const action = rawAction.toLowerCase();
    const username = extract('tokenCode');

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
        username: username,
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


// Hàm chính
function main() {
    console.log(`Starting Log Shipper... Watching: ${LOG_FILE}`);
    console.log(`Target API: ${config.api.url}`);
    
    startBatchTimer();

    // 1. Kiểm tra và đọc nội dung hiện có nếu file tồn tại
    if (fs.existsSync(LOG_FILE)) {
        console.log("Reading existing logs...");
        const initialStream = fs.createReadStream(LOG_FILE);
        const initialRl = readline.createInterface({
            input: initialStream,
            crlfDelay: Infinity
        });
        initialRl.on('line', (line) => processLine(line));
    } else {
        console.warn(`File not found: ${LOG_FILE}. Waiting for file creation...`);
    }

    // 2. Theo dõi thay đổi (bao gồm cả Log Rotation)
    // interval: 1000ms giúp giảm tải CPU trên CentOS cũ
    fs.watchFile(LOG_FILE, { interval: 1000 }, (curr, prev) => {
        if (curr.size > prev.size) {
            // Trường hợp có log mới nối thêm vào
            const newStream = fs.createReadStream(LOG_FILE, {
                start: prev.size,
                end: curr.size
            });
            const newRl = readline.createInterface({
                input: newStream,
                crlfDelay: Infinity
            });
            newRl.on('line', (line) => processLine(line));
        } 
        else if (curr.size < prev.size) {
            // TRƯỜNG HỢP LOG ROTATION: File cũ được nén/đổi tên, file mới (nhỏ hơn) được tạo ra
            console.log("Log rotation detected (new file started). Reading from beginning.");
            const rotateStream = fs.createReadStream(LOG_FILE);
            const rotateRl = readline.createInterface({
                input: rotateStream,
                crlfDelay: Infinity
            });
            rotateRl.on('line', (line) => processLine(line));
        }
    });
}

main();
