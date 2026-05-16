/**
 * Plant Monitor Server — Multi-node edition
 * Install:  npm install express cors
 * Run:      node server.js
 *
 * Folder structure:
 *   server.js
 *   package.json
 *   public/
 *     index.html
 *
 * Center node posts to:  POST /api/readings        (JSON array)
 * Dashboard loads from:  GET  /api/readings         (latest per node)
 *                        GET  /api/readings/:nodeId  (full history)
 * Live updates via:      GET  /api/stream           (SSE)
 */

const express = require("express");
const cors    = require("cors");
const path    = require("path");

const app  = express();
const PORT = 3000;

app.use(cors());
app.use(express.json());

// Redirect root to frontend
app.get("/", (req, res) => {
  res.redirect("http://10.39.30.102:3000");
});

// ── In-memory store ────────────────────────────────────────────────────────────
// Per-node history:  { [nodeId]: Reading[] }
// Latest per node:   { [nodeId]: Reading   }
const MAX_HISTORY = 100;
const nodeHistory = {};   // { "1": [...], "2": [...] }
const nodeLatest  = {};   // { "1": {...}, "2": {...} }
let   sseClients  = [];

// Calibration for soil ADC → percentage (adjust to your sensor)
const SOIL_DRY = 2800;   // ADC value in dry air
const SOIL_WET = 1200;   // ADC value fully submerged

function soilToPercent(raw) {
  return Math.round(
    Math.max(0, Math.min(100, (SOIL_DRY - raw) / (SOIL_DRY - SOIL_WET) * 100))
  );
}

function broadcastSSE(data) {
  const msg = `data: ${JSON.stringify(data)}\n\n`;
  sseClients = sseClients.filter(res => {
    try { res.write(msg); return true; } catch { return false; }
  });
}

// ── POST  /api/readings  — center node sends array here ───────────────────────
//
// Expected body (array — matches center_node.ino):
//   [ { node_id, temp_c, humidity, soil_raw, rssi }, ... ]
//
// Also accepts a plain object for single-node / legacy setups:
//   { temperature, humidity, soil, relay }
//
app.post("/api/readings", (req, res) => {
  // Accept both an array (multi-node) and a plain object (single-node legacy)
  const incoming = Array.isArray(req.body) ? req.body : [req.body];

  if (incoming.length === 0)
    return res.status(400).json({ error: "Empty payload." });

  const stored = [];

  for (const raw of incoming) {
    // Support both field naming conventions
    const nodeId   = String(raw.node_id   ?? raw.nodeId ?? 1);
    const tempC    = parseFloat(raw.temp_c      ?? raw.temperature);
    const humidity = parseFloat(raw.humidity);
    const soilRaw  = parseInt(raw.soil_raw  ?? raw.soil ?? 0);
    const rssi     = raw.rssi  !== undefined ? parseInt(raw.rssi)  : null;
    const relay    = raw.relay !== undefined ? Boolean(raw.relay)  : null;

    if (isNaN(tempC) || isNaN(humidity)) {
      console.warn(`[Node ${nodeId}] Skipped — missing temp/humidity.`);
      continue;
    }

    const reading = {
      node_id:   nodeId,
      temp_c:    tempC,
      humidity,
      soil_raw:  soilRaw,
      soil_pct:  soilToPercent(soilRaw),
      rssi,
      relay,
      timestamp: new Date().toISOString(),
    };

    // Store per-node
    if (!nodeHistory[nodeId]) nodeHistory[nodeId] = [];
    nodeHistory[nodeId].push(reading);
    if (nodeHistory[nodeId].length > MAX_HISTORY) nodeHistory[nodeId].shift();
    nodeLatest[nodeId] = reading;

    stored.push(reading);

    console.log(
      `[${reading.timestamp}]  Node ${nodeId}` +
      `  T:${reading.temp_c}°C  H:${reading.humidity}%` +
      `  Soil:${reading.soil_pct}%  RSSI:${rssi ?? "—"}` +
      (relay !== null ? `  Relay:${relay ? "ON" : "OFF"}` : "")
    );
  }

  broadcastSSE({ type: "readings", data: stored });
  res.json({ ok: true, stored: stored.length });
});

// ── GET  /api/readings  — latest reading for every node ───────────────────────
app.get("/api/readings", (req, res) => {
  res.json({ nodes: nodeLatest });
});

// ── GET  /api/readings/:nodeId  — full history for one node ───────────────────
app.get("/api/readings/:nodeId", (req, res) => {
  const { nodeId } = req.params;
  const history    = nodeHistory[nodeId];

  if (!history)
    return res.status(404).json({ error: `Node ${nodeId} not found.` });

  res.json({ node_id: nodeId, latest: nodeLatest[nodeId], history });
});

// ── GET  /api/stream  — SSE live updates ──────────────────────────────────────
app.get("/api/stream", (req, res) => {
  res.setHeader("Content-Type",  "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection",    "keep-alive");
  res.flushHeaders();

  // Send all current node states immediately on connect
  if (Object.keys(nodeLatest).length > 0)
    res.write(`data: ${JSON.stringify({ type: "snapshot", data: nodeLatest })}\n\n`);

  sseClients.push(res);
  req.on("close", () => { sseClients = sseClients.filter(c => c !== res); });
});

// ── Start ──────────────────────────────────────────────────────────────────────
app.listen(PORT, "0.0.0.0", () => {
  console.log("\n Plant Monitor Server  (multi-node)");
  console.log("   Dashboard    : http://localhost:" + PORT);
  console.log("   Center node  : http://<YOUR_PC_IP>:" + PORT + "/api/readings");
  console.log("   Live stream  : http://localhost:" + PORT + "/api/stream\n");
});