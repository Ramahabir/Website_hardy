/**
 * Plant Monitor — Frontend Server (10.39.30.102:3000)
 *
 * - Serves index.html
 * - Connects to the backend SSE stream at 10.39.30.101:3000
 * - Transforms backend field names  →  what index.html expects
 * - Exposes GET /api/history  and  GET /api/stream for the browser
 *
 * Install:  npm install express cors node-fetch
 * Run:      node server.js
 */

const express   = require("express");
const cors      = require("cors");
const path      = require("path");
const http      = require("http");

const app  = express();
const PORT = 3000;

const BACKEND_HOST = "10.39.30.101";
const BACKEND_PORT = 3000;

/* ────────────────────────────────
   Middleware
──────────────────────────────── */
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname)));   // serves index.html

/* ────────────────────────────────
   In-memory store
──────────────────────────────── */
const MAX_HISTORY = 500;
const history     = [];       // transformed readings for /api/history
let   sseClients  = new Set();

/* ────────────────────────────────
   Field transformer
   Backend shape  →  index.html shape
──────────────────────────────── */
function transform(raw) {
  return {
    node_id:     raw.node_id     ?? 1,
    temperature: raw.temp_c      ?? raw.temperature ?? 0,
    humidity:    raw.humidity    ?? 0,
    soil:        raw.soil_pct    ?? raw.soil        ?? 0,
    relay:       raw.relay       ?? false,
    rssi:        raw.rssi        ?? null,
    timestamp:   raw.timestamp   ?? new Date().toISOString(),
  };
}

/* ────────────────────────────────
   Store + broadcast a reading
──────────────────────────────── */
function handleReading(reading) {
  history.push(reading);
  if (history.length > MAX_HISTORY) history.shift();

  const payload = `data: ${JSON.stringify({ type: "reading", data: reading })}\n\n`;
  sseClients.forEach(res => {
    try { res.write(payload); } catch { sseClients.delete(res); }
  });

  console.log(
    `[${reading.timestamp}]  Node ${reading.node_id}` +
    `  T:${reading.temperature}°C  H:${reading.humidity}%  Soil:${reading.soil}%` +
    (reading.rssi !== null ? `  RSSI:${reading.rssi}` : "")
  );
}

/* ────────────────────────────────
   Connect to backend SSE stream
   Automatically reconnects on error
──────────────────────────────── */
function connectBackendSSE() {
  console.log(`[SSE→Backend] Connecting to http://${BACKEND_HOST}:${BACKEND_PORT}/api/stream ...`);

  const req = http.request(
    { host: BACKEND_HOST, port: BACKEND_PORT, path: "/api/stream" },
    res => {
      console.log(`[SSE→Backend] Connected (HTTP ${res.statusCode})`);
      let buffer = "";

      res.on("data", chunk => {
        buffer += chunk.toString();
        const lines = buffer.split("\n");
        buffer = lines.pop();                  // keep incomplete line

        for (const line of lines) {
          if (!line.startsWith("data:")) continue;
          try {
            const msg = JSON.parse(line.slice(5).trim());

            if (msg.type === "readings" && Array.isArray(msg.data)) {
              // Multi-node array from POST broadcast
              msg.data.forEach(raw => handleReading(transform(raw)));

            } else if (msg.type === "snapshot" && msg.data) {
              // Initial snapshot: { [nodeId]: latestReading }
              Object.values(msg.data).forEach(raw => handleReading(transform(raw)));

            } else if (msg.type === "reading" && msg.data) {
              // Single reading (legacy / single-node)
              handleReading(transform(msg.data));
            }
          } catch (e) {
            // ignore malformed lines
          }
        }
      });

      res.on("end", () => {
        console.warn("[SSE→Backend] Stream ended — reconnecting in 3 s...");
        setTimeout(connectBackendSSE, 3000);
      });
    }
  );

  req.on("error", err => {
    console.error(`[SSE→Backend] Error: ${err.message} — retrying in 3 s...`);
    setTimeout(connectBackendSSE, 3000);
  });

  req.end();
}

/* ────────────────────────────────
   GET /api/history
   Serves stored history to index.html on first load
──────────────────────────────── */
app.get("/api/history", (req, res) => {
  res.json({ history });
});

/* ────────────────────────────────
   GET /api/stream
   SSE endpoint for the browser (index.html)
──────────────────────────────── */
app.get("/api/stream", (req, res) => {
  res.setHeader("Content-Type",  "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection",    "keep-alive");
  res.flushHeaders();

  // Send current history as initial snapshot
  if (history.length > 0) {
    const snap = JSON.stringify({ type: "reading", data: history[history.length - 1] });
    res.write(`data: ${snap}\n\n`);
  }

  // Heartbeat every 20 s
  const heartbeat = setInterval(() => res.write(": heartbeat\n\n"), 20000);

  sseClients.add(res);
  console.log(`[SSE→Browser] Client connected  (total: ${sseClients.size})`);

  req.on("close", () => {
    clearInterval(heartbeat);
    sseClients.delete(res);
    console.log(`[SSE→Browser] Client disconnected (total: ${sseClients.size})`);
  });
});

/* ────────────────────────────────
   Catch-all → index.html
──────────────────────────────── */
app.use((req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

/* ────────────────────────────────
   Start
──────────────────────────────── */
app.listen(PORT, "0.0.0.0", () => {
  console.log(`\n Plant Monitor — Frontend Server`);
  console.log(`   Dashboard  : http://localhost:${PORT}`);
  console.log(`   History    : http://localhost:${PORT}/api/history`);
  console.log(`   Stream     : http://localhost:${PORT}/api/stream`);
  console.log(`   Backend    : http://${BACKEND_HOST}:${BACKEND_PORT}\n`);
  connectBackendSSE();
});