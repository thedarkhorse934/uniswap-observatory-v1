#!/usr/bin/env node
import { ethers } from "ethers";
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

/**
 * UNISWAP OBSERVATORY v0.11.2 (local)
 *
 * Live:
 *   export RPC_URL=https://mainnet.infura.io/v3/XXXX
 *   node index.js --poll 30 --pool-stagger-ms 400 --post-429-cooldown-ms 1500 --out obs.ndjson --latest latest.json
 *
 * Replay:
 *   node index.js --from-block <n> --to-block <n> --poll 1200 --pools pools.json --out replay.ndjson --latest latest.json
 *
 * Summarize:
 *   node index.js --summarize obs.ndjson --top 10 --csv out.csv
 *
 * Notes:
 * - Never prints RPC_URL or any key.
 * - v0.11.2 tweaks:
 *   - RPC health auto-recovers (degraded only if recent errors)
 *   - UI clarifies PX as base/quote and shows (BASE/QUOTE) next to pool name
 */

// ---------------- CLI ARGS ----------------
function parseArgs(argv) {
  const args = {
    poolsFile: null,
    poll: 30,
    json: false,
    noClear: false,
    fromBlock: null,
    toBlock: null,

    outFile: null,
    pretty: false,

    summarizeFile: null,
    top: 10,
    csvFile: null,

    latestFile: "latest.json",

    alertOnBucketChange: false,
    trendSamples: 5,

    mevBaseline: 60,

    // retry/backoff knobs
    maxRpcRetries: 10,
    rpcRetryBaseMs: 800,
    maxLogRetries: 10,
    logRetryBaseMs: 900,

    // getLogs split fallback
    logSplitAfterRetries: 6,
    logSplitMaxDepth: 8,

    // smoothness knobs
    poolStaggerMs: 0,
    post429CooldownMs: 0,

    // incremental logs
    reorgOverlapBlocks: 2,
    maxLogBufferBlocks: null, // default: computeLookbackBlocks() + padding
  };

  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];

    if (a === "--pools") args.poolsFile = argv[++i];
    else if (a === "--poll") args.poll = Number(argv[++i]);
    else if (a === "--json") args.json = true;
    else if (a === "--no-clear") args.noClear = true;
    else if (a === "--from-block") args.fromBlock = Number(argv[++i]);
    else if (a === "--to-block") args.toBlock = Number(argv[++i]);

    else if (a === "--out") args.outFile = argv[++i];
    else if (a === "--pretty") args.pretty = true;

    else if (a === "--summarize") args.summarizeFile = argv[++i];
    else if (a === "--top") args.top = Number(argv[++i] ?? "10");
    else if (a === "--csv") args.csvFile = argv[++i];

    else if (a === "--latest") args.latestFile = argv[++i];

    else if (a === "--alert-on-bucket-change") args.alertOnBucketChange = true;
    else if (a === "--trend-samples") args.trendSamples = Number(argv[++i]);

    else if (a === "--mev-baseline") args.mevBaseline = Number(argv[++i]);

    else if (a === "--max-rpc-retries") args.maxRpcRetries = Number(argv[++i]);
    else if (a === "--rpc-retry-base-ms") args.rpcRetryBaseMs = Number(argv[++i]);
    else if (a === "--max-log-retries") args.maxLogRetries = Number(argv[++i]);
    else if (a === "--log-retry-base-ms") args.logRetryBaseMs = Number(argv[++i]);

    else if (a === "--log-split-after-retries") args.logSplitAfterRetries = Number(argv[++i]);
    else if (a === "--log-split-max-depth") args.logSplitMaxDepth = Number(argv[++i]);

    else if (a === "--pool-stagger-ms") args.poolStaggerMs = Number(argv[++i]);
    else if (a === "--post-429-cooldown-ms") args.post429CooldownMs = Number(argv[++i]);

    else if (a === "--reorg-overlap-blocks") args.reorgOverlapBlocks = Number(argv[++i]);
    else if (a === "--max-log-buffer-blocks") args.maxLogBufferBlocks = Number(argv[++i]);

    else if (a === "--help" || a === "-h") {
      console.log(
        [
          "Usage:",
          "  node index.js [--pools pools.json] [--poll <seconds>] [--json] [--no-clear] [--out <file.ndjson>] [--pretty] [--latest <latest.json>]",
          "              [--alert-on-bucket-change] [--trend-samples <n>] [--mev-baseline <n>]",
          "              [--pool-stagger-ms <ms>] [--post-429-cooldown-ms <ms>]",
          "              [--reorg-overlap-blocks <n>] [--max-log-buffer-blocks <n>]",
          "",
          "  node index.js --from-block <n> --to-block <n> [--pools pools.json] [--poll <seconds>] [--out <file.ndjson>] [--latest latest.json]",
          "",
          "  node index.js --summarize <file.ndjson> [--top <n>] [--csv <out.csv>]",
          "",
          "Infura-friendly retry/backoff:",
          "  --max-rpc-retries <n>       default 10",
          "  --rpc-retry-base-ms <ms>    default 800",
          "  --max-log-retries <n>       default 10",
          "  --log-retry-base-ms <ms>    default 900",
          "",
          "getLogs range split fallback:",
          "  --log-split-after-retries <n>  default 6",
          "  --log-split-max-depth <n>      default 8",
          "",
          "Smoothness:",
          "  --pool-stagger-ms <ms>          default 0",
          "  --post-429-cooldown-ms <ms>     default 0",
          "",
          "Incremental logs:",
          "  --reorg-overlap-blocks <n>      default 2",
          "  --max-log-buffer-blocks <n>     default auto (lookback + padding)",
          "",
          "Env:",
          "  RPC_URL=<https://mainnet.infura.io/v3/...>",
        ].join("\n")
      );
      process.exit(0);
    }
  }

  if (!Number.isFinite(args.poll) || args.poll <= 0) args.poll = 30;
  if (!Number.isFinite(args.top) || args.top <= 0) args.top = 10;
  if (!Number.isFinite(args.mevBaseline) || args.mevBaseline < 10) args.mevBaseline = 60;
  if (!Number.isFinite(args.trendSamples) || args.trendSamples < 2) args.trendSamples = 5;

  if (!Number.isFinite(args.maxRpcRetries) || args.maxRpcRetries < 0) args.maxRpcRetries = 10;
  if (!Number.isFinite(args.rpcRetryBaseMs) || args.rpcRetryBaseMs < 50) args.rpcRetryBaseMs = 800;
  if (!Number.isFinite(args.maxLogRetries) || args.maxLogRetries < 0) args.maxLogRetries = 10;
  if (!Number.isFinite(args.logRetryBaseMs) || args.logRetryBaseMs < 50) args.logRetryBaseMs = 900;

  if (!Number.isFinite(args.logSplitAfterRetries) || args.logSplitAfterRetries < 0) args.logSplitAfterRetries = 6;
  if (!Number.isFinite(args.logSplitMaxDepth) || args.logSplitMaxDepth < 0) args.logSplitMaxDepth = 8;

  if (!Number.isFinite(args.poolStaggerMs) || args.poolStaggerMs < 0) args.poolStaggerMs = 0;
  if (!Number.isFinite(args.post429CooldownMs) || args.post429CooldownMs < 0) args.post429CooldownMs = 0;

  if (!Number.isFinite(args.reorgOverlapBlocks) || args.reorgOverlapBlocks < 0) args.reorgOverlapBlocks = 2;
  if (args.maxLogBufferBlocks !== null && (!Number.isFinite(args.maxLogBufferBlocks) || args.maxLogBufferBlocks < 20)) {
    args.maxLogBufferBlocks = null;
  }

  return args;
}

const CLI = parseArgs(process.argv);
const VERSION = "0.11.2";

// ---------------- Globals / Caches ----------------
const PAIR_META_CACHE = new Map();  // pairAddress -> meta
const TOKEN_META_CACHE = new Map(); // tokenAddress -> {address,symbol,decimals}

// ---------------- Helpers ----------------
function nowIso() {
  return new Date().toISOString().replace("T", " ").slice(0, 19);
}
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
function clamp(n, lo, hi) { return Math.max(lo, Math.min(hi, n)); }
function riskBucket(score) {
  if (score >= 80) return "SEVERE";
  if (score >= 50) return "HIGH";
  if (score >= 25) return "ELEVATED";
  return "OK";
}
function addDriver(drivers, points, label) {
  const sign = points >= 0 ? "+" : "-";
  drivers.push(`${sign}${Math.abs(points)}  ${label}`);
}
function bigIntReplacer(_k, v) { return typeof v === "bigint" ? v.toString() : v; }
function ensureDirForFile(filePath) {
  if (!filePath) return;
  const dir = path.dirname(filePath);
  if (dir && dir !== ".") fs.mkdirSync(dir, { recursive: true });
}
function writeJsonAtomic(filePath, obj) {
  if (!filePath) return;
  ensureDirForFile(filePath);
  const tmp = `${filePath}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(obj, bigIntReplacer, 2));
  fs.renameSync(tmp, filePath);
}
function safeNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}
function shortAt(at) {
  if (!at || typeof at !== "string") return "n/a";
  const parts = at.split(" ");
  return parts.length === 2 ? parts[1] : at;
}
function padRight(s, n) {
  const t = String(s ?? "");
  return t.length >= n ? t.slice(0, n) : t + " ".repeat(n - t.length);
}
function padLeft(s, n) {
  const t = String(s ?? "");
  return t.length >= n ? t.slice(0, n) : " ".repeat(n - t.length) + t;
}
function trunc(s, n) {
  const t = String(s ?? "");
  return t.length <= n ? t : t.slice(0, Math.max(0, n - 1)) + "…";
}
function fmtUnitsBI(bi, decimals, maxFrac = 2) {
  const s = ethers.formatUnits(bi, decimals);
  const n = Number(s);
  if (!Number.isFinite(n)) return s;
  return n.toLocaleString(undefined, { maximumFractionDigits: maxFrac });
}
function pushTopN(arr, item, n, keyFn) {
  arr.push(item);
  arr.sort((a, b) => keyFn(b) - keyFn(a));
  if (arr.length > n) arr.length = n;
}
function anyPositive(arr, key) {
  return arr.some((x) => (Number(x?.[key]) ?? 0) > 0);
}
function fmtPctSigned(x, dp = 2) {
  const n = Number(x);
  if (!Number.isFinite(n)) return "n/a";
  const sign = n > 0 ? "▲" : n < 0 ? "▼" : "⇄";
  return `${sign} ${Math.abs(n).toFixed(dp)}%`;
}
function isStableSymbol(sym) {
  const s = String(sym ?? "").toUpperCase();
  return ["USDC", "USDT", "DAI", "FRAX", "LUSD"].includes(s);
}
function isWethSymbol(sym) {
  const s = String(sym ?? "").toUpperCase();
  return ["WETH", "ETH"].includes(s);
}
function isBtcLikeSymbol(sym) {
  const s = String(sym ?? "").toUpperCase();
  return ["WBTC", "BTC"].includes(s);
}
function humanAbsFromBI(bi, dec) {
  const n = Number(ethers.formatUnits(bi < 0n ? -bi : bi, dec));
  return Number.isFinite(n) ? n : 0;
}

// ---------------- RPC metrics ----------------
const RPC_METRICS = {
  calls: 0,
  errs: 0,
  r429: 0,
  totalMs: 0,

  lastErrAt: null,
  lastErrMs: null,     // ✅ new: millis timestamp of last error
  lastOkMs: null,      // ✅ new: millis timestamp of last successful call
  lastErrLabel: null,
  lastErrMsg: null,

  lastWas429: false,
  byLabel: new Map(), // label -> {calls, errs, r429, totalMs}
};

function bumpMetric(label, { ok, ms, is429, errMsg }) {
  RPC_METRICS.calls += 1;
  RPC_METRICS.totalMs += ms;

  if (!RPC_METRICS.byLabel.has(label)) RPC_METRICS.byLabel.set(label, { calls: 0, errs: 0, r429: 0, totalMs: 0 });
  const b = RPC_METRICS.byLabel.get(label);
  b.calls += 1;
  b.totalMs += ms;

  if (ok) {
    RPC_METRICS.lastOkMs = Date.now();
  }

  if (!ok) {
    RPC_METRICS.errs += 1;
    b.errs += 1;
    RPC_METRICS.lastErrAt = nowIso();
    RPC_METRICS.lastErrMs = Date.now();
    RPC_METRICS.lastErrLabel = label;
    RPC_METRICS.lastErrMsg = errMsg ?? null;
  }
  if (is429) {
    RPC_METRICS.r429 += 1;
    b.r429 += 1;
    RPC_METRICS.lastWas429 = true;
  }
}

function rpcHealthLine() {
  // ✅ Only degraded if error happened "recently"
  const RECOVER_AFTER_MS = 10 * 60 * 1000; // 10 minutes
  const errRecent = RPC_METRICS.lastErrMs && (Date.now() - RPC_METRICS.lastErrMs) < RECOVER_AFTER_MS;

  const avg = RPC_METRICS.calls ? Math.round(RPC_METRICS.totalMs / RPC_METRICS.calls) : 0;
  const ok = errRecent ? "degraded" : "ok";

  const lastErr = RPC_METRICS.lastErrAt
    ? `${shortAt(RPC_METRICS.lastErrAt)} ${RPC_METRICS.lastErrLabel}`
    : "n/a";

  return `RPC: ${ok} | calls ${RPC_METRICS.calls} | 429s ${RPC_METRICS.r429} | errs ${RPC_METRICS.errs} | lastErr ${lastErr} | avg ${avg}ms`;
}

// ---------------- Rate limit detection ----------------
function isInfuraRateLimit(err) {
  const msg = String(err?.message ?? "").toLowerCase();
  if (msg.includes("too many requests")) return true;

  const value = err?.value;
  if (Array.isArray(value)) {
    return value.some((x) => String(x?.message ?? "").toLowerCase().includes("too many requests"));
  }

  const info = err?.info;
  if (info && typeof info === "object") {
    const payload = info?.payload;
    if (payload && typeof payload === "object") {
      const m = String(payload?.error?.message ?? "").toLowerCase();
      if (m.includes("too many requests")) return true;
    }
  }

  const m2 = String(err?.data?.message ?? "").toLowerCase();
  if (m2.includes("too many requests")) return true;

  return false;
}

// ---------------- RPC wrapper ----------------
async function rpcCall(fn, { label = "rpc", maxRetries = CLI.maxRpcRetries, baseDelayMs = CLI.rpcRetryBaseMs } = {}) {
  let attempt = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const t0 = Date.now();
    try {
      const out = await fn();
      bumpMetric(label, { ok: true, ms: Date.now() - t0, is429: false });
      return out;
    } catch (err) {
      const ms = Date.now() - t0;
      const rateLimited = isInfuraRateLimit(err);
      bumpMetric(label, { ok: false, ms, is429: rateLimited, errMsg: err?.message ?? String(err) });

      attempt++;
      if (!rateLimited || attempt > maxRetries) throw err;

      const jitter = Math.floor(Math.random() * 250);
      const delay = Math.min(15_000, baseDelayMs * 2 ** (attempt - 1)) + jitter;
      console.error(`[${nowIso()}] Rate limited (${label}) attempt ${attempt}/${maxRetries}. Sleeping ${delay}ms...`);
      await sleep(delay);
    }
  }
}

// ---------------- NDJSON writer ----------------
function createNdjsonWriter(filePath, { pretty = false } = {}) {
  if (!filePath) return null;
  const dir = path.dirname(filePath);
  if (dir && dir !== ".") fs.mkdirSync(dir, { recursive: true });
  const stream = fs.createWriteStream(filePath, { flags: "a" });

  function write(record) {
    const line = JSON.stringify(record, bigIntReplacer, pretty ? 2 : 0);
    stream.write(line + "\n");
  }
  function close() {
    return new Promise((resolve, reject) => {
      stream.end(() => resolve());
      stream.on("error", reject);
    });
  }
  return { write, close };
}

// ---------------- Rolling baseline (snapshot-based) ----------------
function median(arr) {
  if (!arr.length) return 0;
  const a = [...arr].sort((x, y) => x - y);
  const mid = Math.floor(a.length / 2);
  return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
}
function mad(arr) {
  if (!arr.length) return 0;
  const m = median(arr);
  const dev = arr.map((x) => Math.abs(x - m));
  return median(dev);
}
function zRobust(x, m, madVal) {
  const scale = 1.4826 * madVal + 1e-9;
  return (x - m) / scale;
}
function riskFromZ(z, z0 = 1.5, z1 = 4.0) {
  const t = (Math.abs(z) - z0) / (z1 - z0);
  return clamp(t, 0, 1) * 100;
}
class RollingBaseline {
  constructor(size) {
    this.size = size;
    this.buf = new Map(); // name -> { arr:number[], idx:number, count:number }
  }
  push(name, x) {
    if (!this.buf.has(name)) this.buf.set(name, { arr: new Array(this.size).fill(0), idx: 0, count: 0 });
    const b = this.buf.get(name);
    b.arr[b.idx] = x;
    b.idx = (b.idx + 1) % this.size;
    b.count++;
  }
  values(name) {
    const b = this.buf.get(name);
    if (!b) return [];
    const n = Math.min(b.count, this.size);
    if (b.count < this.size) return b.arr.slice(0, n);
    const tail = b.arr.slice(b.idx);
    const head = b.arr.slice(0, b.idx);
    return tail.concat(head);
  }
  stats(name) {
    const v = this.values(name);
    return { n: v.length, median: median(v), mad: mad(v) };
  }
}

// ---------------- Defaults / Pools config ----------------
function loadPoolsConfig(poolsFile) {
  const defaults = [
    { name: "USDC/WETH", pair: "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc" },
    { name: "WBTC/WETH", pair: "0xBb2b8038a1640196FbE3e38816F3e67Cba72D940" },
    { name: "USDT/WETH", pair: "0x0d4a11d5EEaaC28EC3F61d100daF4d40471f1852" },
    { name: "USDC/USDT", pair: "0x3041cbd36888becc7bbcbc0045e3b1f144466f5f" },
  ];

  const normalize = (arr) =>
    arr
      .map((x) => {
        const raw = String(x?.pair ?? "").trim();
        if (!raw) return null;
        try {
          const pair = ethers.getAddress(raw);
          const name = String(x?.name ?? x?.label ?? pair.slice(0, 6)).trim() || pair.slice(0, 6);
          return { name, pair };
        } catch {
          return null;
        }
      })
      .filter(Boolean);

  if (!poolsFile) return normalize(defaults);

  if (!fs.existsSync(poolsFile)) {
    console.error(`Pools file not found: ${poolsFile}. Using defaults.`);
    return normalize(defaults);
  }

  try {
    const raw = fs.readFileSync(poolsFile, "utf8");
    const obj = JSON.parse(raw);
    const arr = Array.isArray(obj) ? obj : (Array.isArray(obj?.pools) ? obj.pools : null);
    if (!arr) return normalize(defaults);
    const pools = normalize(arr);
    return pools.length ? pools : normalize(defaults);
  } catch (e) {
    console.error(`Could not parse pools file (${poolsFile}). Using defaults. Reason: ${e?.message ?? e}`);
    return normalize(defaults);
  }
}

// ---------------- ABIs ----------------
const PAIR_ABI = [
  "function token0() view returns (address)",
  "function token1() view returns (address)",
  "function getReserves() view returns (uint112 reserve0,uint112 reserve1,uint32 blockTimestampLast)",
  "event Swap(address indexed sender,uint amount0In,uint amount1In,uint amount0Out,uint amount1Out,address indexed to)",
  "event Mint(address indexed sender,uint amount0,uint amount1)",
  "event Burn(address indexed sender,uint amount0,uint amount1,address indexed to)",
  "event Sync(uint112 reserve0,uint112 reserve1)",
];

const ERC20_ABI = ["function decimals() view returns (uint8)", "function symbol() view returns (string)"];
const iface = new ethers.Interface(PAIR_ABI);

// ---------------- Per-pool constants ----------------
const BLOCKS_PER_MINUTE = 5;
const M1_BOUNCE_BLOCKS = 10;

const HEARTBEAT_MIN = 5;
const L1_WINDOW_MIN = 15;
const L1_BASELINE_MIN = 60;
const M1_WINDOW_MIN = 15;

const EXTRA_PADDING_BLOCKS = 30;

const VOL_SHORT_SAMPLES = 5;
const VOL_LONG_SAMPLES = 60;
const VOL_REGIME_ELEVATED = 2.0;
const VOL_REGIME_HIGH = 3.0;

const FLOW_NET_ELEVATED_PCT = 0.10;
const FLOW_NET_HIGH_PCT = 0.30;
const FLOW_GROSS_ELEVATED_PCT = 0.50;
const FLOW_GROSS_HIGH_PCT = 1.50;

// Event labeling thresholds
const FLOW_SHOCK_NET_PCT = 0.50;
const FLOW_SHOCK_GROSS_PCT = 1.00;

const L1_MULTIPLIER = 2.0;
const L1_MIN_QUOTE_REMOVED = 50_000;

const DEPTH_ELEVATED_IMPACT_PCT = 0.75;
const DEPTH_HIGH_IMPACT_PCT = 2.0;
const DEPTH_BINARYSEARCH_MAX_QUOTE = 5_000_000;

const UNCERTAINTY_VOL_LONG_PENALTY = 5;

// Staleness thresholds (blocks)
const STALE_BLOCKS = 25;

// ---------------- Provider / State ----------------
let provider = null;

// ---------------- Math helpers (vol) ----------------
function ln(x) { return Math.log(x); }
function stdev(values) {
  const n = values.length;
  if (n < 2) return null;
  const mean = values.reduce((a, b) => a + b, 0) / n;
  const varSum = values.reduce((acc, v) => acc + (v - mean) ** 2, 0);
  return Math.sqrt(varSum / (n - 1));
}
function calcVolFromLastNSamples(points, nSamples) {
  if (points.length < Math.max(3, nSamples)) return null;
  const slice = points.slice(-nSamples);
  const rets = [];
  for (let i = 1; i < slice.length; i++) {
    const p0 = slice[i - 1].price;
    const p1 = slice[i].price;
    if (p0 > 0 && p1 > 0) rets.push(ln(p1 / p0));
  }
  return stdev(rets);
}
function pctOfReserve(absFlow, reserve) {
  if (reserve === null || reserve === 0n) return null;
  const a = Number(absFlow.toString());
  const b = Number(reserve.toString());
  if (!Number.isFinite(a) || !Number.isFinite(b) || b === 0) return null;
  return (a / b) * 100;
}

// ---------------- L2 Depth helpers ----------------
function parseUnitsSafe(human, decimals) {
  try { return ethers.parseUnits(String(human), decimals); } catch { return null; }
}
function getAmountOutV2(amountIn, reserveIn, reserveOut, feeBps = 30) {
  const feeDen = 10_000n;
  const feeMul = feeDen - BigInt(feeBps);

  const amountInWithFee = amountIn * feeMul;
  const numerator = amountInWithFee * reserveOut;
  const denominator = reserveIn * feeDen + amountInWithFee;
  if (denominator === 0n) return 0n;
  return numerator / denominator;
}
function computeQuoteImpactPct({ quoteInUnits, reserveBase, reserveQuote, baseDecimals, quoteDecimals }) {
  if (reserveBase <= 0n || reserveQuote <= 0n) return null;
  const amountIn = parseUnitsSafe(quoteInUnits, quoteDecimals);
  if (amountIn === null || amountIn <= 0n) return null;

  const baseOut = getAmountOutV2(amountIn, reserveQuote, reserveBase, 30);
  if (baseOut <= 0n) return null;

  const quoteInHuman = Number(quoteInUnits);
  const baseOutHuman = Number(ethers.formatUnits(baseOut, baseDecimals));
  if (!Number.isFinite(baseOutHuman) || baseOutHuman <= 0) return null;
  const execPrice = quoteInHuman / baseOutHuman;

  const reserveQuoteHuman = Number(ethers.formatUnits(reserveQuote, quoteDecimals));
  const reserveBaseHuman = Number(ethers.formatUnits(reserveBase, baseDecimals));
  if (!Number.isFinite(reserveQuoteHuman) || !Number.isFinite(reserveBaseHuman) || reserveBaseHuman <= 0) return null;

  const midPrice = reserveQuoteHuman / reserveBaseHuman;
  if (!Number.isFinite(midPrice) || midPrice <= 0) return null;

  return ((execPrice - midPrice) / midPrice) * 100;
}
function approxMaxQuoteInForImpactPct({ targetImpactPct, reserveBase, reserveQuote, baseDecimals, quoteDecimals }) {
  if (reserveBase <= 0n || reserveQuote <= 0n) return null;
  let lo = 1;
  let hi = DEPTH_BINARYSEARCH_MAX_QUOTE;
  let best = null;

  for (let i = 0; i < 20; i++) {
    const mid = Math.floor((lo + hi) / 2);
    const imp = computeQuoteImpactPct({ quoteInUnits: mid, reserveBase, reserveQuote, baseDecimals, quoteDecimals });
    if (imp === null) return null;

    if (imp >= targetImpactPct) hi = mid - 1;
    else { best = mid; lo = mid + 1; }
  }
  return best;
}

// Depth probe sizes based on quote token
function depthProbeQuoteSizes(quoteSymbol, quoteDecimals) {
  const q = String(quoteSymbol ?? "").toUpperCase();

  // dollar-like
  if (isStableSymbol(q)) return [5_000, 25_000, 100_000];

  // ETH-like quote
  if (isWethSymbol(q)) return [0.5, 2, 10];

  // BTC-like quote
  if (isBtcLikeSymbol(q)) return [0.05, 0.25, 1];

  // fallback heuristic
  if (Number.isFinite(quoteDecimals) && quoteDecimals <= 10) return [10, 50, 200];

  return [1_000, 5_000, 20_000];
}

// ---------------- Pair meta helpers ----------------
function pickQuoteSide(token0, token1) {
  const s0 = String(token0.symbol || "").toUpperCase();
  const s1 = String(token1.symbol || "").toUpperCase();

  // Prefer stable as quote
  if (isStableSymbol(s0) && !isStableSymbol(s1)) return "token0";
  if (isStableSymbol(s1) && !isStableSymbol(s0)) return "token1";

  // If no stable present, prefer WETH as quote
  if (isWethSymbol(s0) && !isWethSymbol(s1)) return "token0";
  if (isWethSymbol(s1) && !isWethSymbol(s0)) return "token1";

  // Otherwise: keep old heuristics
  if (token0.decimals === 6 && token1.decimals !== 6) return "token0";
  if (token1.decimals === 6 && token0.decimals !== 6) return "token1";

  return "token0";
}

async function loadToken(addr) {
  const address = ethers.getAddress(addr);
  if (TOKEN_META_CACHE.has(address)) return TOKEN_META_CACHE.get(address);

  const c = new ethers.Contract(address, ERC20_ABI, provider);

  const [symbol, decimalsRaw] = await Promise.all([
    rpcCall(() => c.symbol(), { label: `token.symbol:${address}` }).catch(() => null),
    rpcCall(() => c.decimals(), { label: `token.decimals:${address}` }).catch(() => 18),
  ]);

  const decimals = Number(decimalsRaw);
  const meta = { address, symbol, decimals: Number.isFinite(decimals) ? decimals : 18 };
  TOKEN_META_CACHE.set(address, meta);
  return meta;
}

async function resolvePairMeta(pairAddr, nameForLabel = "pool") {
  const pair = ethers.getAddress(pairAddr);
  if (PAIR_META_CACHE.has(pair)) return PAIR_META_CACHE.get(pair);

  const pairC = new ethers.Contract(pair, PAIR_ABI, provider);

  const t0 = await rpcCall(() => pairC.token0(), { label: `pair.token0:${nameForLabel}` });
  const t1 = await rpcCall(() => pairC.token1(), { label: `pair.token1:${nameForLabel}` });

  const token0 = await loadToken(t0);
  const token1 = await loadToken(t1);

  const quoteSide = pickQuoteSide(token0, token1);
  const quoteToken = quoteSide === "token0" ? token0 : token1;
  const baseToken = quoteSide === "token0" ? token1 : token0;

  const meta = { pair, token0, token1, quoteSide, baseToken, quoteToken };
  PAIR_META_CACHE.set(pair, meta);
  return meta;
}

// ---------------- Swap/Mint/Burn direction helpers ----------------
function priceBaseInQuote(state, reserve0, reserve1) {
  const meta = state?.pairMeta;
  if (!meta) return null;
  const { quoteSide, token0, token1 } = meta;

  const R0 = Number(ethers.formatUnits(reserve0, token0.decimals));
  const R1 = Number(ethers.formatUnits(reserve1, token1.decimals));
  if (!Number.isFinite(R0) || !Number.isFinite(R1) || R0 <= 0 || R1 <= 0) return null;

  return quoteSide === "token0" ? R0 / R1 : R1 / R0;
}
function swapDirection(state, args) {
  const meta = state?.pairMeta;
  if (!meta) return 0;
  const a0in = args.amount0In;
  const a1in = args.amount1In;

  if (meta.quoteSide === "token0") {
    if (a0in > 0n && a1in === 0n) return +1;
    if (a1in > 0n && a0in === 0n) return -1;
    return 0;
  } else {
    if (a1in > 0n && a0in === 0n) return +1;
    if (a0in > 0n && a1in === 0n) return -1;
    return 0;
  }
}
function quoteFlowFromSwap(state, args) {
  const meta = state?.pairMeta;
  if (!meta) return { net: 0n, gross: 0n };

  if (meta.quoteSide === "token0") {
    const qIn = args.amount0In;
    const qOut = args.amount0Out;
    return { net: qIn - qOut, gross: qIn + qOut };
  } else {
    const qIn = args.amount1In;
    const qOut = args.amount1Out;
    return { net: qIn - qOut, gross: qIn + qOut };
  }
}
function quoteAmountFromBurn(state, args) {
  const meta = state?.pairMeta;
  if (!meta) return 0n;
  return meta.quoteSide === "token0" ? args.amount0 : args.amount1;
}
function quoteAmountFromMint(state, args) {
  const meta = state?.pairMeta;
  if (!meta) return 0n;
  return meta.quoteSide === "token0" ? args.amount0 : args.amount1;
}
function quoteReserveFromSync(state, reserve0, reserve1) {
  const meta = state?.pairMeta;
  if (!meta) return null;
  return meta.quoteSide === "token0" ? reserve0 : reserve1;
}
function baseReserveFromSync(state, reserve0, reserve1) {
  const meta = state?.pairMeta;
  if (!meta) return null;
  return meta.quoteSide === "token0" ? reserve1 : reserve0;
}

// ---------------- MEV helpers ----------------
function countSandwichLikeInBlock(swapsInBlock) {
  let candidates = 0;
  for (let j = 1; j < swapsInBlock.length - 1; j++) {
    const victim = swapsInBlock[j];
    if (victim.dir === 0) continue;

    let hasFront = false;
    for (let i = 0; i < j; i++) {
      const s = swapsInBlock[i];
      if (s.dir === victim.dir && s.txHash !== victim.txHash) { hasFront = true; break; }
    }
    if (!hasFront) continue;

    let hasBack = false;
    for (let k = j + 1; k < swapsInBlock.length; k++) {
      const s = swapsInBlock[k];
      if (s.dir === -victim.dir && s.txHash !== victim.txHash) { hasBack = true; break; }
    }
    if (!hasBack) continue;

    candidates++;
  }
  return candidates;
}
function countReversalPairsInBlock(swapsInBlock) {
  let reversals = 0;
  for (let i = 0; i < swapsInBlock.length - 1; i++) {
    const a = swapsInBlock[i];
    const b = swapsInBlock[i + 1];
    if (a.dir !== 0 && b.dir !== 0 && a.dir === -b.dir) reversals++;
  }
  return reversals;
}
function countBouncePairs(swapsSorted, bounceBlocks) {
  let bounces = 0;
  for (let i = 0; i < swapsSorted.length; i++) {
    const a = swapsSorted[i];
    if (a.dir === 0) continue;

    for (let j = i + 1; j < swapsSorted.length; j++) {
      const b = swapsSorted[j];
      if (b.bn - a.bn > bounceBlocks) break;
      if (b.dir === 0) continue;

      if (b.dir === -a.dir && b.txHash !== a.txHash) { bounces++; break; }
    }
  }
  return bounces;
}

function computeAdaptiveM1Status(state, { classicCandidates, reversalPairs, bouncePairs, blocksWithClassic }) {
  const baseline = state.mevBaseline;

  const feats = [
    { name: "m1Classic", value: classicCandidates, weight: 0.55 },
    { name: "m1Reversal", value: reversalPairs, weight: 0.225 },
    { name: "m1Bounce", value: bouncePairs, weight: 0.225 },
  ];

  for (const f of feats) baseline.push(f.name, f.value);

  const warm = baseline.stats(feats[0].name).n < Math.min(10, CLI.mevBaseline);
  if (warm) {
    const elevated = classicCandidates >= 1 || reversalPairs >= 2 || bouncePairs >= 3;
    const high = (classicCandidates >= 3 && blocksWithClassic >= 2) || reversalPairs >= 6 || bouncePairs >= 8;

    if (high) return { status: "HIGH", adaptiveScore: 85, note: "Baseline warming; fallback HIGH logic." };
    if (elevated) return { status: "ELEVATED", adaptiveScore: 55, note: "Baseline warming; fallback ELEVATED logic." };
    return { status: "OK", adaptiveScore: 15, note: "Baseline warming; fallback OK." };
  }

  let score = 0;
  const details = [];

  for (const f of feats) {
    const st = baseline.stats(f.name);
    const z = zRobust(f.value, st.median, st.mad);
    const r = riskFromZ(z, 1.5, 4.0);
    score += r * f.weight;
    details.push(`${f.name}=${f.value} (med=${st.median.toFixed(2)} z=${z.toFixed(2)})`);
  }

  const adaptiveScore = clamp(Math.round(score), 0, 100);
  let status = "OK";
  if (adaptiveScore >= 75) status = "HIGH";
  else if (adaptiveScore >= 45) status = "ELEVATED";

  return { status, adaptiveScore, note: details.join("; ") };
}

// ---------------- Snapshot engine ----------------
function buildTopics() {
  return [[
    ethers.id("Swap(address,uint256,uint256,uint256,uint256,address)"),
    ethers.id("Mint(address,uint256,uint256)"),
    ethers.id("Burn(address,uint256,uint256,address)"),
    ethers.id("Sync(uint112,uint112)")
  ]];
}
function computeLookbackBlocks() {
  const totalLookbackMin = Math.max(HEARTBEAT_MIN, L1_WINDOW_MIN + L1_BASELINE_MIN, M1_WINDOW_MIN);
  return totalLookbackMin * BLOCKS_PER_MINUTE + EXTRA_PADDING_BLOCKS;
}

function computeTrend(state, score) {
  state.trendScores.push(score);
  if (state.trendScores.length > CLI.trendSamples) state.trendScores.shift();

  if (state.trendScores.length < 2) {
    return { deltaScore: 0, trend: "FLAT", note: `(+0/${state.trendScores.length}samp)` };
  }

  const prev = state.trendScores[state.trendScores.length - 2];
  const delta = score - prev;

  let trend = "FLAT";
  if (delta >= 3) trend = "RISING";
  else if (delta <= -3) trend = "FALLING";

  return { deltaScore: delta, trend, note: `(${delta >= 0 ? "+" : ""}${delta}/${state.trendScores.length}samp)` };
}

function computeEventLabels(snapshot) {
  const ev = [];

  // FLOW_SHOCK
  const flowNet = safeNum(snapshot?.flow?.netPct);
  const flowGross = safeNum(snapshot?.flow?.grossPct);
  if ((flowNet !== null && flowNet >= FLOW_SHOCK_NET_PCT) || (flowGross !== null && flowGross >= FLOW_SHOCK_GROSS_PCT)) {
    ev.push("FLOW_SHOCK");
  }

  // MEV_SWARM
  const m1Status = snapshot?.m1?.status;
  const bounce = safeNum(snapshot?.m1?.bouncePairs) ?? 0;
  const reversal = safeNum(snapshot?.m1?.reversalPairs) ?? 0;
  const elevatedBounce = bounce >= 3;
  const elevatedReversal = reversal >= 4;
  if (m1Status === "HIGH" || (elevatedBounce && elevatedReversal)) {
    ev.push("MEV_SWARM");
  }

  // LP_REPOSITION: churn high, net exit low
  try {
    const churn = BigInt(snapshot?.l1?.churn15Quote ?? "0");
    const net = BigInt(snapshot?.l1?.netLiq15Quote ?? "0");
    const churnAbs = churn < 0n ? -churn : churn;
    const netAbs = net < 0n ? -net : net;
    if (snapshot?.l1?.status === "ELEVATED" && churnAbs > 0n && netAbs * 5n < churnAbs) {
      ev.push("LP_REPOSITION");
    }
  } catch {}

  // Logs availability labels first (truth!)
  const conf = snapshot?.logsConfidence ?? "unknown"; // ok | partial | unavailable
  if (conf === "partial") ev.push("LOGS_PARTIAL");
  else if (conf === "unavailable") ev.push("LOGS_UNAVAILABLE");

  // Quiet pool label (replaces LOG_STALE)
  const logAge = safeNum(snapshot?.logAgeBlocks);
  const hbSwaps = safeNum(snapshot?.heartbeat?.swaps) ?? 0;
  const hbMints = safeNum(snapshot?.heartbeat?.mints) ?? 0;
  const hbBurns = safeNum(snapshot?.heartbeat?.burns) ?? 0;
  const hbTotal = hbSwaps + hbMints + hbBurns;

  if (conf === "ok" && logAge !== null && logAge >= STALE_BLOCKS && hbTotal === 0) {
    ev.push("QUIET_POOL");
  }

  // STATE_STALE only when reserves are sourced from logs (otherwise not meaningful)
  const syncAge = safeNum(snapshot?.syncAgeBlocks);
  if (snapshot?.reservesFrom === "logs" && syncAge !== null && syncAge >= STALE_BLOCKS) {
    ev.push("STATE_STALE");
  }

  return ev;
}

function computeSnapshot(state, endBlock, logs, logsMeta, reservesFallback) {
  const lookbackBlocks = computeLookbackBlocks();
  const startBlock = Math.max(0, endBlock - lookbackBlocks);

  const hbCutoff = endBlock - HEARTBEAT_MIN * BLOCKS_PER_MINUTE;

  const l1Cutoff = endBlock - L1_WINDOW_MIN * BLOCKS_PER_MINUTE;
  const baselineStart = endBlock - (L1_WINDOW_MIN + L1_BASELINE_MIN) * BLOCKS_PER_MINUTE;
  const baselineEnd = l1Cutoff;

  const m1Cutoff = endBlock - M1_WINDOW_MIN * BLOCKS_PER_MINUTE;

  let swapsHb = 0, mintsHb = 0, burnsHb = 0;

  let burns15Count = 0, burns60Count = 0;
  let burns15Quote = 0n, burns60Quote = 0n;

  let mints15Count = 0, mints60Count = 0;
  let mints15Quote = 0n, mints60Quote = 0n;

  let netQuote_M1 = 0n;
  let grossQuote_M1 = 0n;

  const swapsByBlock = new Map();
  const swapsM1 = [];

  let lastSyncBlock = null;
  let reserve0 = null;
  let reserve1 = null;

  let lastEventBlock = null;
  let lastEventType = null;

  // logsConfidence: ok | partial | unavailable
  const logsConfidence = logsMeta?.confidence ?? "unavailable";

  for (const log of logs) {
    const bn = log.blockNumber;
    if (bn < startBlock || bn > endBlock) continue;

    let parsed;
    try { parsed = iface.parseLog(log); } catch { continue; }
    if (!parsed) continue;

    if (bn >= hbCutoff) {
      if (parsed.name === "Swap") swapsHb++;
      if (parsed.name === "Mint") mintsHb++;
      if (parsed.name === "Burn") burnsHb++;
    }

    if (parsed.name === "Sync") {
      lastSyncBlock = bn;
      reserve0 = parsed.args.reserve0;
      reserve1 = parsed.args.reserve1;
    }

    if (parsed.name === "Burn") {
      const qAmt = quoteAmountFromBurn(state, parsed.args);
      if (bn >= l1Cutoff) { burns15Count++; burns15Quote += qAmt; }
      if (bn >= baselineStart && bn < baselineEnd) { burns60Count++; burns60Quote += qAmt; }
    }

    if (parsed.name === "Mint") {
      const qAmt = quoteAmountFromMint(state, parsed.args);
      if (bn >= l1Cutoff) { mints15Count++; mints15Quote += qAmt; }
      if (bn >= baselineStart && bn < baselineEnd) { mints60Count++; mints60Quote += qAmt; }
    }

    if (parsed.name === "Swap" && bn >= m1Cutoff) {
      const dir = swapDirection(state, parsed.args);
      const entry = { bn, logIndex: log.logIndex, txHash: log.transactionHash, dir };

      const { net, gross } = quoteFlowFromSwap(state, parsed.args);
      netQuote_M1 += net;
      grossQuote_M1 += gross;

      swapsM1.push(entry);
      if (!swapsByBlock.has(bn)) swapsByBlock.set(bn, []);
      swapsByBlock.get(bn).push(entry);
    }

    lastEventBlock = bn;
    lastEventType = parsed.name;
  }

  // If we did not see a Sync, optionally use fallback reserves (getReserves)
  let reservesFrom = "logs";
  if ((reserve0 === null || reserve1 === null) && reservesFallback?.ok && reservesFallback?.reserve0 != null && reservesFallback?.reserve1 != null) {
    reserve0 = reservesFallback.reserve0;
    reserve1 = reservesFallback.reserve1;
    reservesFrom = "fallback_getReserves";
  } else if (reserve0 === null || reserve1 === null) {
    reservesFrom = logsConfidence === "unavailable" ? "missing_logs" : "missing_sync";
  }

  const baseReserve = (reserve0 !== null && reserve1 !== null) ? baseReserveFromSync(state, reserve0, reserve1) : null;
  const quoteReserve = (reserve0 !== null && reserve1 !== null) ? quoteReserveFromSync(state, reserve0, reserve1) : null;

  const poolPrice = (reserve0 !== null && reserve1 !== null) ? priceBaseInQuote(state, reserve0, reserve1) : null;

  if (poolPrice && Number.isFinite(poolPrice)) {
    const last = state.priceHistory[state.priceHistory.length - 1];
    if (!last || last.bn !== endBlock) {
      state.priceHistory.push({ bn: endBlock, price: poolPrice });
      if (state.priceHistory.length > state.maxPricePoints) state.priceHistory.shift();
    }
  }

  const volShort = calcVolFromLastNSamples(state.priceHistory, VOL_SHORT_SAMPLES);
  const volLong = calcVolFromLastNSamples(state.priceHistory, VOL_LONG_SAMPLES);

  let volStatus = "OK";
  let volNote = "Insufficient history yet.";
  let volRatio = null;

  if (volShort !== null && volLong !== null) {
    volRatio = volLong === 0 ? null : (volShort / volLong);
    if (volRatio !== null && volRatio >= VOL_REGIME_HIGH) { volStatus = "HIGH"; volNote = `Vol regime: short/long ≈ ${volRatio.toFixed(2)}x`; }
    else if (volRatio !== null && volRatio >= VOL_REGIME_ELEVATED) { volStatus = "ELEVATED"; volNote = `Vol regime: short/long ≈ ${volRatio.toFixed(2)}x`; }
    else volNote = `Short/Long vol ≈ ${volRatio !== null ? volRatio.toFixed(2) : "n/a"}x`;
  }

  const absNetQuote_M1 = netQuote_M1 < 0n ? -netQuote_M1 : netQuote_M1;
  const netPct = pctOfReserve(absNetQuote_M1, quoteReserve);
  const grossPct = pctOfReserve(grossQuote_M1, quoteReserve);

  let flowStatus = "OK";
  let flowNote = "No notable flow intensity.";
  if (netPct === null || grossPct === null) {
    flowStatus = "n/a";
    flowNote = "Missing reserves or insufficient data.";
  } else {
    const netHigh = netPct >= FLOW_NET_HIGH_PCT;
    const netElev = netPct >= FLOW_NET_ELEVATED_PCT;
    const grossHigh = grossPct >= FLOW_GROSS_HIGH_PCT;
    const grossElev = grossPct >= FLOW_GROSS_ELEVATED_PCT;

    if (netHigh || grossHigh) { flowStatus = "HIGH"; flowNote = `Flow stress: net≈${netPct.toFixed(3)}% gross≈${grossPct.toFixed(3)}% (last ~${M1_WINDOW_MIN}m).`; }
    else if (netElev || grossElev) { flowStatus = "ELEVATED"; flowNote = `Flow rising: net≈${netPct.toFixed(3)}% gross≈${grossPct.toFixed(3)}% (last ~${M1_WINDOW_MIN}m).`; }
    else flowNote = `Flow calm: net≈${netPct.toFixed(3)}% gross≈${grossPct.toFixed(3)}% (last ~${M1_WINDOW_MIN}m).`;
  }

  // L1 churn vs net exit
  const netLiq15Quote = burns15Quote - mints15Quote; // positive => net exit
  const churn15Quote = burns15Quote + mints15Quote;

  const netLiq60Quote = burns60Quote - mints60Quote;
  const churn60Quote = burns60Quote + mints60Quote;

  const baselinePer15Count = Math.floor(burns60Count / 4);
  const baselinePer15Quote = burns60Quote / 4n;

  const baselineNetExit15Quote = netLiq60Quote / 4n;
  const baselineChurn15Quote = churn60Quote / 4n;

  let l1Status = "OK";
  let l1Reason = "No abnormal liquidity exit detected.";
  let l1Triggered = false;

  const qDec = state?.pairMeta?.quoteToken?.decimals ?? 18;

  const netExit15Abs = netLiq15Quote < 0n ? -netLiq15Quote : netLiq15Quote;
  const netExit15Float = Number(ethers.formatUnits(netExit15Abs, qDec));
  const churn15Float = Number(ethers.formatUnits(churn15Quote, qDec));

  const baselineNetExit15Abs = baselineNetExit15Quote < 0n ? -baselineNetExit15Quote : baselineNetExit15Quote;
  const baselineNetExit15Float = Number(ethers.formatUnits(baselineNetExit15Abs, qDec));
  const baselineChurn15Float = Number(ethers.formatUnits(baselineChurn15Quote, qDec));

  const isNetExit = netLiq15Quote > 0n;
  const netExitMeetsMin = isNetExit && netExit15Float >= L1_MIN_QUOTE_REMOVED;

  if (netExitMeetsMin) {
    if (churn60Quote === 0n) {
      l1Status = "ELEVATED";
      l1Reason = "Net liquidity exit detected; baseline churn near-zero.";
    } else {
      const netExitSpike =
        baselineNetExit15Quote === 0n ? netExit15Float >= L1_MIN_QUOTE_REMOVED : netExit15Float > baselineNetExit15Float * L1_MULTIPLIER;

      const churnSpike =
        baselineChurn15Quote === 0n ? churn15Float >= L1_MIN_QUOTE_REMOVED : churn15Float > baselineChurn15Float * L1_MULTIPLIER;

      if (netExitSpike && churnSpike) {
        l1Triggered = true;
        l1Status = "HIGH";
        l1Reason = "Net liquidity exit in last 15m is significantly above baseline, with elevated churn.";
      } else if (netExitSpike) {
        l1Triggered = true;
        l1Status = "HIGH";
        l1Reason = "Net liquidity exit in last 15m is significantly above baseline.";
      } else {
        l1Status = "ELEVATED";
        l1Reason = "Net liquidity exit detected, but not a clear baseline spike yet.";
      }
    }
  } else {
    const churnHigh =
      churn15Float >= L1_MIN_QUOTE_REMOVED &&
      (baselineChurn15Float > 0 ? churn15Float > baselineChurn15Float * L1_MULTIPLIER : churn15Float >= L1_MIN_QUOTE_REMOVED);

    if (churnHigh) {
      l1Status = "ELEVATED";
      l1Reason = "High liquidity churn detected (mints + burns), but net exit is not elevated.";
    }
  }

  let burnSharePct = null;
  if (quoteReserve !== null) {
    const denom = quoteReserve + burns15Quote;
    if (denom > 0n) burnSharePct = pctOfReserve(burns15Quote, denom);
  }
  if (burnSharePct !== null && burnSharePct >= 10 && netLiq15Quote > 0n) {
    l1Triggered = true;
    l1Status = "HIGH";
    l1Reason = "Very large burn activity vs reserves (high exit stress).";
  }

  // M1 features
  let classicCandidates = 0;
  let blocksWithClassic = 0;
  let reversalPairs = 0;
  let blocksWithReversal = 0;
  let blocksWith3PlusSwaps = 0;

  for (const [bn, arr] of swapsByBlock.entries()) {
    arr.sort((a, b) => a.logIndex - b.logIndex);
    if (arr.length >= 3) blocksWith3PlusSwaps++;

    const c = countSandwichLikeInBlock(arr);
    if (c > 0) { classicCandidates += c; blocksWithClassic++; }

    const r = countReversalPairsInBlock(arr);
    if (r > 0) { reversalPairs += r; blocksWithReversal++; }
  }

  swapsM1.sort((a, b) => (a.bn - b.bn) || (a.logIndex - b.logIndex));
  const bouncePairs = countBouncePairs(swapsM1, M1_BOUNCE_BLOCKS);

  const m1Adaptive = computeAdaptiveM1Status(state, { classicCandidates, reversalPairs, bouncePairs, blocksWithClassic });

  const m1Status = m1Adaptive.status;
  const m1Triggered = m1Status !== "OK";
  const m1Reason =
    m1Status === "OK"
      ? "MEV/Bot signals within recent baseline."
      : m1Status === "ELEVATED"
      ? `MEV/Bot activity elevated vs baseline. (${m1Adaptive.note})`
      : `MEV/Bot activity high vs baseline. (${m1Adaptive.note})`;

  const syncAgeBlocks = lastSyncBlock === null ? null : Math.max(0, endBlock - lastSyncBlock);
  const logAgeBlocks = lastEventBlock === null ? null : Math.max(0, endBlock - lastEventBlock);

  // Depth
  const quoteSym = state?.pairMeta?.quoteToken?.symbol ?? "QUOTE";
  const baseSym = state?.pairMeta?.baseToken?.symbol ?? "BASE";
  const quoteDec2 = state?.pairMeta?.quoteToken?.decimals ?? 18;
  const baseDec = state?.pairMeta?.baseToken?.decimals ?? 18;

  const probeSizes = depthProbeQuoteSizes(quoteSym, quoteDec2);

  let depth = {
    status: "n/a",
    quoteSizes: probeSizes,
    impactsPct: {},
    maxInFor1Pct: null,
    note: "Depth unavailable (missing reserves/meta).",
  };

  if (baseReserve !== null && quoteReserve !== null && baseDec != null && quoteDec2 != null) {
    for (const q of probeSizes) {
      const imp = computeQuoteImpactPct({
        quoteInUnits: q,
        reserveBase: baseReserve,
        reserveQuote: quoteReserve,
        baseDecimals: baseDec,
        quoteDecimals: quoteDec2
      });
      depth.impactsPct[String(q)] = imp;
    }

    const largest = probeSizes[probeSizes.length - 1];
    const impLargest = depth.impactsPct[String(largest)];

    depth.maxInFor1Pct = isStableSymbol(quoteSym) ? approxMaxQuoteInForImpactPct({
      targetImpactPct: 1.0,
      reserveBase: baseReserve,
      reserveQuote: quoteReserve,
      baseDecimals: baseDec,
      quoteDecimals: quoteDec2
    }) : null;

    if (impLargest === null) {
      depth.status = "n/a";
      depth.note = "Depth unavailable (could not compute impact).";
    } else if (impLargest >= DEPTH_HIGH_IMPACT_PCT) {
      depth.status = "HIGH";
      depth.note = `Depth fragile: ~${impLargest.toFixed(2)}% impact for ~${String(largest)} ${quoteSym}.`;
    } else if (impLargest >= DEPTH_ELEVATED_IMPACT_PCT) {
      depth.status = "ELEVATED";
      depth.note = `Depth thinning: ~${impLargest.toFixed(2)}% impact for ~${String(largest)} ${quoteSym}.`;
    } else {
      depth.status = "OK";
      depth.note = `Depth OK: ~${impLargest.toFixed(2)}% impact for ~${String(largest)} ${quoteSym}.`;
    }
  }

  // Risk scoring
  let riskScore = 0;
  const drivers = [];

  if (volStatus === "HIGH") { riskScore += 30; addDriver(drivers, +30, `Volatility HIGH (${volNote})`); }
  else if (volStatus === "ELEVATED") { riskScore += 20; addDriver(drivers, +20, `Volatility ELEVATED (${volNote})`); }
  else addDriver(drivers, 0, "Volatility OK");

  if (flowStatus === "HIGH") { riskScore += 30; addDriver(drivers, +30, `Flow HIGH (${flowNote})`); }
  else if (flowStatus === "ELEVATED") { riskScore += 18; addDriver(drivers, +18, `Flow ELEVATED (${flowNote})`); }
  else if (flowStatus === "OK") addDriver(drivers, 0, "Flow OK");
  else addDriver(drivers, 0, "Flow n/a");

  if (l1Status === "HIGH") { riskScore += 28; addDriver(drivers, +28, "Liquidity exit HIGH (L1 net)"); }
  else if (l1Status === "ELEVATED") { riskScore += 16; addDriver(drivers, +16, "Liquidity exit ELEVATED (L1 net/churn)"); }
  else addDriver(drivers, 0, "Liquidity exit OK");

  if (m1Status === "HIGH") { riskScore += 22; addDriver(drivers, +22, `MEV/Bot HIGH (M1 adaptive=${m1Adaptive.adaptiveScore})`); }
  else if (m1Status === "ELEVATED") { riskScore += 12; addDriver(drivers, +12, `MEV/Bot ELEVATED (M1 adaptive=${m1Adaptive.adaptiveScore})`); }
  else addDriver(drivers, 0, "MEV/Bot OK");

  if (depth.status === "HIGH") { riskScore += 18; addDriver(drivers, +18, `Depth HIGH (${depth.note})`); }
  else if (depth.status === "ELEVATED") { riskScore += 10; addDriver(drivers, +10, `Depth ELEVATED (${depth.note})`); }
  else if (depth.status === "OK") addDriver(drivers, 0, "Depth OK");
  else addDriver(drivers, 0, "Depth n/a");

  if (volLong === null) { riskScore += UNCERTAINTY_VOL_LONG_PENALTY; addDriver(drivers, +UNCERTAINTY_VOL_LONG_PENALTY, "Vol long history incomplete"); }

  if (reserve0 === null || reserve1 === null || poolPrice === null) {
    riskScore += 10;
    addDriver(drivers, +10, "State incomplete (missing Sync/price)");
  }

  if (reservesFrom === "logs" && syncAgeBlocks !== null && syncAgeBlocks >= STALE_BLOCKS) {
    riskScore += 5;
    addDriver(drivers, +5, `State stale (syncAgeBlocks=${syncAgeBlocks})`);
  }

  if (logsConfidence === "unavailable") {
    riskScore += 2;
    addDriver(drivers, +2, "Logs unavailable (RPC/limits)");
  } else if (logsConfidence === "partial") {
    riskScore += 1;
    addDriver(drivers, +1, "Logs partial (split/range)");
  }

  riskScore = clamp(riskScore, 0, 100);
  const riskStatus = riskBucket(riskScore);

  const snapshot = {
    meta: {
      name: state.name,
      pair: state.pair,
      token0: state.pairMeta?.token0?.address ?? null,
      token1: state.pairMeta?.token1?.address ?? null,
      token0Symbol: state.pairMeta?.token0?.symbol ?? null,
      token1Symbol: state.pairMeta?.token1?.symbol ?? null,
      token0Decimals: state.pairMeta?.token0?.decimals ?? null,
      token1Decimals: state.pairMeta?.token1?.decimals ?? null,
      quoteSide: state.pairMeta?.quoteSide ?? null,
      baseSymbol: baseSym,
      quoteSymbol: quoteSym,
      quoteDecimals: state.pairMeta?.quoteToken?.decimals ?? null,
    },

    endBlock,
    startBlock,
    syncBlock: lastSyncBlock,
    syncAgeBlocks,

    lastEventBlock,
    lastEventType,
    logAgeBlocks,
    logsConfidence,
    reservesFrom,

    reserves: (reserve0 !== null && reserve1 !== null) ? {
      token0: reserve0.toString(),
      token1: reserve1.toString(),
      base: baseReserve !== null ? baseReserve.toString() : null,
      quote: quoteReserve !== null ? quoteReserve.toString() : null,
    } : null,

    priceBaseInQuote: poolPrice,

    heartbeat: { swaps: swapsHb, mints: mintsHb, burns: burnsHb, lastEventType, lastEventBlock },

    vol: { status: volStatus, short: volShort, long: volLong, note: volNote },

    flow: { status: flowStatus, netQuote: netQuote_M1.toString(), grossQuote: grossQuote_M1.toString(), netPct, grossPct, note: flowNote },

    l1: {
      status: l1Status,
      triggered: l1Triggered,
      reason: l1Reason,

      mints15Count,
      mints15Quote: mints15Quote.toString(),
      burns15Count,
      burns15Quote: burns15Quote.toString(),
      netLiq15Quote: netLiq15Quote.toString(),
      churn15Quote: churn15Quote.toString(),

      baselinePer15Count,
      baselinePer15Quote: baselinePer15Quote.toString(),
      burnSharePct,

      baselineNetExit15Quote: baselineNetExit15Quote.toString(),
      baselineChurn15Quote: baselineChurn15Quote.toString(),
    },

    m1: {
      status: m1Status,
      triggered: m1Triggered,
      reason: m1Reason,

      classicCandidates,
      blocksWithClassic,
      reversalPairs,
      blocksWithReversal,
      bouncePairs,
      blocksWith3PlusSwaps,

      adaptiveScore: m1Adaptive.adaptiveScore,
    },

    depth,
    risk: { score: riskScore, status: riskStatus, drivers },
  };

  snapshot.events = computeEventLabels(snapshot);

  const trendInfo = computeTrend(state, riskScore);
  snapshot.momentum = trendInfo;

  return snapshot;
}

// ---------------- Operator summary ----------------
function operatorSummary(snapshot) {
  const px = snapshot.priceBaseInQuote;
  const score = snapshot.risk?.score ?? 0;
  const bucket = snapshot.risk?.status ?? "n/a";
  const endBlock = snapshot.endBlock;

  const m = snapshot.momentum ?? { trend: "FLAT", note: "(+0/1samp)" };
  const events = Array.isArray(snapshot.events) && snapshot.events.length ? snapshot.events.join(",") : "";

  const drivers = Array.isArray(snapshot.risk?.drivers) ? snapshot.risk.drivers : [];
  const top = drivers
    .filter((d) => typeof d === "string" && d.startsWith("+") && !d.includes(" OK") && !d.includes(" n/a"))
    .slice(0, 2)
    .join(" | ");

  return `b${endBlock} px=${px !== null && px !== undefined ? Number(px).toFixed(6) : "n/a"} score=${score} ${bucket} | trend=${m.trend}${m.note}${events ? ` | events=${events}` : ""}${top ? ` | ${top}` : ""}`;
}

// ---------------- getLogs (Infura-safe with split) ----------------
function buildFilter(pairAddr, fromBlock, toBlock) {
  return { address: pairAddr, fromBlock, toBlock, topics: buildTopics() };
}

async function fetchLogsRange(pairAddr, fromBlock, toBlock, label) {
  return await rpcCall(() => provider.getLogs(buildFilter(pairAddr, fromBlock, toBlock)), { label });
}

async function fetchLogsRangeWithRetry(pairName, pairAddr, fromBlock, toBlock, {
  maxRetries = CLI.maxLogRetries,
  baseDelayMs = CLI.logRetryBaseMs
} = {}) {
  let attempt = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await fetchLogsRange(pairAddr, fromBlock, toBlock, `eth_getLogs:${pairName}`);
    } catch (err) {
      attempt++;
      const rateLimited = isInfuraRateLimit(err);
      if (!rateLimited || attempt > maxRetries) throw err;

      const jitter = Math.floor(Math.random() * 250);
      const delay = Math.min(15_000, baseDelayMs * 2 ** (attempt - 1)) + jitter;

      console.error(`[${nowIso()}] Rate limited (eth_getLogs:${pairName}) attempt ${attempt}/${maxRetries} range=${fromBlock}..${toBlock}. Sleeping ${delay}ms...`);
      await sleep(delay);
    }
  }
}

/**
 * Returns:
 *  { logs: [], confidence: "ok"|"partial"|"unavailable", note?: string }
 */
async function fetchLogsSmart(pairName, pairAddr, fromBlock, toBlock, depth = 0) {
  try {
    const logs = await fetchLogsRangeWithRetry(pairName, pairAddr, fromBlock, toBlock, {
      maxRetries: CLI.logSplitAfterRetries,
      baseDelayMs: CLI.logRetryBaseMs
    });
    return { logs, confidence: "ok" };
  } catch (err) {
    const rateLimited = isInfuraRateLimit(err);
    if (!rateLimited) {
      return { logs: [], confidence: "unavailable", note: `non-429: ${err?.message ?? String(err)}` };
    }
    if (depth >= CLI.logSplitMaxDepth) {
      return { logs: [], confidence: "unavailable", note: `splitMaxDepth reached` };
    }
    if (fromBlock >= toBlock) {
      return { logs: [], confidence: "unavailable", note: `invalid range` };
    }

    const mid = Math.floor((fromBlock + toBlock) / 2);
    if (mid === fromBlock || mid === toBlock) {
      return { logs: [], confidence: "unavailable", note: `unsplittable range` };
    }

    const left = await fetchLogsSmart(pairName, pairAddr, fromBlock, mid, depth + 1);
    const right = await fetchLogsSmart(pairName, pairAddr, mid + 1, toBlock, depth + 1);

    const merged = left.logs.concat(right.logs);
    merged.sort((a, b) => (a.blockNumber - b.blockNumber) || (a.logIndex - b.logIndex));

    if (left.confidence === "ok" && right.confidence === "ok") return { logs: merged, confidence: "ok" };
    if (merged.length > 0) return { logs: merged, confidence: "partial", note: `split partial: left=${left.confidence} right=${right.confidence}` };
    return { logs: [], confidence: "unavailable", note: `split failed` };
  }
}

// ---------------- getReserves fallback ----------------
async function fetchReservesFallback(pairName, pairAddr) {
  try {
    const c = new ethers.Contract(pairAddr, PAIR_ABI, provider);
    const res = await rpcCall(() => c.getReserves(), { label: `pair.getReserves:${pairName}` });
    return { ok: true, reserve0: res.reserve0, reserve1: res.reserve1 };
  } catch (e) {
    return { ok: false, err: e?.message ?? String(e) };
  }
}

// ---------------- Incremental log buffer ----------------
function logKey(l) {
  return `${l.blockNumber}:${l.logIndex}:${l.transactionHash ?? "0x"}`;
}
function mergeLogsIntoBuffer(state, newLogs) {
  if (!Array.isArray(newLogs) || newLogs.length === 0) return;

  const seen = state.logSeenKeys;
  for (const l of newLogs) {
    const k = logKey(l);
    if (seen.has(k)) continue;
    seen.add(k);
    state.logBuffer.push(l);
  }

  state.logBuffer.sort((a, b) => (a.blockNumber - b.blockNumber) || (a.logIndex - b.logIndex));

  const keepBlocks = CLI.maxLogBufferBlocks ?? (computeLookbackBlocks() + EXTRA_PADDING_BLOCKS);
  const minBn = Math.max(0, state.lastProcessedBlock - keepBlocks);
  if (state.logBuffer.length) {
    let cut = 0;
    while (cut < state.logBuffer.length && state.logBuffer[cut].blockNumber < minBn) {
      state.logSeenKeys.delete(logKey(state.logBuffer[cut]));
      cut++;
    }
    if (cut > 0) state.logBuffer.splice(0, cut);
  }
}

// ---------------- Render (table + global sections) ----------------
function computeGlobalThemes(pools) {
  let repo = 0, l1hot = 0, mev = 0, flowShock = 0, stateStale = 0, logsDegraded = 0, quiet = 0;

  for (const p of pools) {
    const s = p.lastSnapshot;
    if (!s) continue;

    const ev = new Set(s.events ?? []);
    if (ev.has("LP_REPOSITION")) repo++;
    if (s.l1?.status === "ELEVATED" || s.l1?.status === "HIGH") l1hot++;
    if (ev.has("MEV_SWARM")) mev++;
    if (ev.has("FLOW_SHOCK")) flowShock++;

    if (ev.has("STATE_STALE")) stateStale++;
    if (ev.has("LOGS_UNAVAILABLE") || ev.has("LOGS_PARTIAL")) logsDegraded++;
    if (ev.has("QUIET_POOL")) quiet++;
  }

  const out = [];
  if (repo || l1hot) out.push(`⚠ LIQ ROTATION: repo=${repo} l1hot=${l1hot}`);
  if (mev) out.push(`⚠ MEV SWARM: pools=${mev}`);
  if (flowShock) out.push(`⚠ FLOW SHOCK: pools=${flowShock}`);
  if (stateStale) out.push(`⚠ STATE STALE: ${stateStale}/${pools.length} pools`);
  if (logsDegraded) out.push(`⚠ LOGS DEGRADED: ${logsDegraded}/${pools.length} pools`);
  if (quiet) out.push(`ℹ QUIET POOLS: ${quiet}/${pools.length}`);
  return out;
}

function pctChange15mForPool(p, latestBlock) {
  const hist = p.priceHistory;
  if (!hist || hist.length < 2) return null;

  const targetBn = latestBlock - (15 * BLOCKS_PER_MINUTE);
  let older = null;
  for (let i = hist.length - 1; i >= 0; i--) {
    if (hist[i].bn <= targetBn) { older = hist[i]; break; }
  }
  const newest = hist[hist.length - 1];
  if (!older || !newest || older.price <= 0 || newest.price <= 0) return null;

  const pct = ((newest.price - older.price) / older.price) * 100;
  if (!Number.isFinite(pct)) return null;
  return pct;
}

function computeTopMovers(pools, latestBlock) {
  const priceMoves = [];
  for (const p of pools) {
    const s = p.lastSnapshot;
    if (!s) continue;
    const pct = pctChange15mForPool(p, latestBlock);
    if (pct === null) continue;
    if (Math.abs(pct) < 0.05) continue;
    priceMoves.push({ name: p.name, pct });
  }
  priceMoves.sort((a, b) => Math.abs(b.pct) - Math.abs(a.pct));
  const topPrice = priceMoves.slice(0, 3);

  const churnMoves = [];
  const netExitMoves = [];
  for (const p of pools) {
    const s = p.lastSnapshot;
    if (!s) continue;

    const qSym = s?.meta?.quoteSymbol ?? "QUOTE";
    const qDec = Number.isFinite(Number(s?.meta?.quoteDecimals)) ? Number(s.meta.quoteDecimals) : (p?.pairMeta?.quoteToken?.decimals ?? 18);

    try {
      const churn = BigInt(s?.l1?.churn15Quote ?? "0");
      const net = BigInt(s?.l1?.netLiq15Quote ?? "0");

      const churnAbs = churn < 0n ? -churn : churn;
      const netAbs = net < 0n ? -net : net;

      const absChurnHuman = humanAbsFromBI(churnAbs, qDec);
      const absNetHuman = humanAbsFromBI(netAbs, qDec);

      const min = isStableSymbol(qSym) ? 1 : 0.001;

      if (absChurnHuman >= min) churnMoves.push({ name: p.name, qSym, qDec, churnAbs });
      if (absNetHuman >= min) netExitMoves.push({ name: p.name, qSym, qDec, netAbs, netSign: net > 0n ? +1 : net < 0n ? -1 : 0 });
    } catch {}
  }

  churnMoves.sort((a, b) => Number(b.churnAbs) - Number(a.churnAbs));
  netExitMoves.sort((a, b) => Number(b.netAbs) - Number(a.netAbs));

  return {
    topPrice,
    topChurn: churnMoves.slice(0, 3),
    topNetExit: netExitMoves.slice(0, 3),
  };
}

function renderTopMovers(pools, latestBlock) {
  const { topPrice, topChurn, topNetExit } = computeTopMovers(pools, latestBlock);

  if (!topPrice.length) console.log("TOP price(15m): (none detected)");
  else console.log(
    "TOP price(15m): " +
      topPrice.map((x) => `${fmtPctSigned(x.pct, 2)} ${x.name}`).join(" | ")
  );

  if (!topChurn.length) console.log("TOP churn15(abs): (none detected)");
  else console.log(
    "TOP churn15(abs): " +
      topChurn.map((x) => `⇄ ${fmtUnitsBI(x.churnAbs, x.qDec, isStableSymbol(x.qSym) ? 2 : 6)} ${x.qSym} ${x.name}`).join(" | ")
  );

  if (!topNetExit.length) console.log("TOP netExit15(abs): (none detected)");
  else console.log(
    "TOP netExit15(abs): " +
      topNetExit.map((x) => {
        const arrow = x.netSign > 0 ? "▼" : x.netSign < 0 ? "▲" : "⇄";
        return `${arrow} ${fmtUnitsBI(x.netAbs, x.qDec, isStableSymbol(x.qSym) ? 2 : 6)} ${x.qSym} ${x.name}`;
      }).join(" | ")
  );
}

function renderTable({ pools, latestBlock, mode }) {
  if (!CLI.noClear) console.clear();

  console.log(`UNISWAP OBSERVATORY v${VERSION}  |  ${nowIso()}`);
  console.log(`mode=${mode}  poll=${CLI.poll}s  pools=${pools.length}  latestBlock=${latestBlock}`);
  console.log(rpcHealthLine());
  console.log("");

  const globals = computeGlobalThemes(pools);
  if (globals.length) {
    for (const g of globals) console.log(g);
    console.log("");
  }

  renderTopMovers(pools, latestBlock);
  console.log("");

  const COL_TREND = 20;

  // ✅ Header clarifies PX semantics
  const header =
    padRight("NAME", 24) + " " +
    padRight("BUCKET", 10) + " " +
    padLeft("SCORE", 6) + "  " +
    padRight("TREND", COL_TREND) + " " +
    padRight("EVENTS", 35) + " " +
    padLeft("PX(base/quote)", 16) + "  " +
    padRight("TOP DRIVER", 28);

  console.log(header);
  console.log("-".repeat(header.length));

  for (const p of pools) {
    const s = p.lastSnapshot;
    if (!s) continue;

    const bucket = s.risk?.status ?? "n/a";
    const score = s.risk?.score ?? 0;

    const m = s.momentum ?? { trend: "FLAT", note: "(+0/1samp)" };
    let trendStr = `${m.trend}${m.note ?? ""}`;
    if (trendStr.includes("(") && !trendStr.endsWith(")")) trendStr += ")";
    trendStr = trunc(trendStr, COL_TREND);

    const events = Array.isArray(s.events) && s.events.length ? trunc(s.events.join(","), 35) : "";
    const px = (s.priceBaseInQuote !== null && s.priceBaseInQuote !== undefined && Number(s.priceBaseInQuote) > 0)
      ? (Number(s.priceBaseInQuote) >= 100 ? Number(s.priceBaseInQuote).toFixed(2) : Number(s.priceBaseInQuote).toFixed(6))
      : "n/a";

    const drivers = Array.isArray(s.risk?.drivers) ? s.risk.drivers : [];
    const topDriver = drivers
      .filter((d) => typeof d === "string" && d.startsWith("+") && !d.includes(" OK") && !d.includes(" n/a"))
      .slice(0, 1)[0] ?? "";
    const td = trunc(topDriver, 28);

    // ✅ Show base/quote next to name (keeps PX interpretable)
    const baseSym = s?.meta?.baseSymbol ?? "";
    const quoteSym = s?.meta?.quoteSymbol ?? "";
    const nameShown = `${p.name} (${baseSym}/${quoteSym})`;

    const line =
      padRight(trunc(nameShown, 24), 24) + " " +
      padRight(bucket, 10) + " " +
      padLeft(score, 6) + "  " +
      padRight(trendStr, COL_TREND) + " " +
      padRight(events, 35) + " " +
      padLeft(px, 16) + "  " +
      padRight(td, 28);

    console.log(line);
  }

  console.log("");
  console.log(`FILES: latest=${CLI.latestFile}${CLI.outFile ? `  |  ndjson=${CLI.outFile}` : ""}`);
}

// ---------------- latest.json payload ----------------
function buildLatestPayload({ mode, at, latestBlock, pools }) {
  return {
    v: VERSION,
    mode,
    at,
    latestBlock,
    pools: pools.map((p) => {
      const s = p.lastSnapshot;
      return {
        name: p.name,
        pair: p.pair,
        endBlock: s?.endBlock ?? null,
        bucket: s?.risk?.status ?? null,
        score: s?.risk?.score ?? null,
        priceBaseInQuote: s?.priceBaseInQuote ?? null,
        events: s?.events ?? [],
        momentum: s?.momentum ?? null,
        reservesFrom: s?.reservesFrom ?? null,
        snapshot: s ?? null,
        op: s ? operatorSummary(s) : null,
      };
    }),
    rpc: {
      calls: RPC_METRICS.calls,
      errs: RPC_METRICS.errs,
      r429: RPC_METRICS.r429,
      avgMs: RPC_METRICS.calls ? Math.round(RPC_METRICS.totalMs / RPC_METRICS.calls) : 0,
      lastErrAt: RPC_METRICS.lastErrAt,
      lastErrLabel: RPC_METRICS.lastErrLabel,
      lastErrMsg: RPC_METRICS.lastErrMsg,
    },
  };
}

function buildNdjsonRecord({ mode, at, poolName, pair, snapshot }) {
  return { v: VERSION, mode, at, poolName, pair, snapshot };
}

// ---------------- Summarizer (+ CSV) ----------------
function csvEscape(v) {
  if (v === null || v === undefined) return "";
  const s = String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

async function summarizeNdjson(filePath, topN = 10, csvFile = null) {
  if (!filePath) throw new Error("Missing --summarize <file.ndjson>");
  if (!fs.existsSync(filePath)) throw new Error(`File not found: ${filePath}`);

  let csvStream = null;
  if (csvFile) {
    ensureDirForFile(csvFile);
    csvStream = fs.createWriteStream(csvFile, { flags: "w" });

    const header =
      [
        "at",
        "poolName",
        "pair",
        "endBlock",
        "priceBaseInQuote",
        "riskScore",
        "riskStatus",
        "trend",
        "deltaScore",
        "events",

        "syncBlock",
        "syncAgeBlocks",
        "lastEventBlock",
        "logAgeBlocks",
        "logsConfidence",
        "reservesFrom",

        "flowStatus",
        "flowNetQuote",
        "flowGrossQuote",
        "flowNetPct",
        "flowGrossPct",

        "l1Status",
        "l1NetLiq15Quote",
        "l1Churn15Quote",

        "m1Status",
        "m1BouncePairs",
        "m1ClassicCandidates",
        "m1ReversalPairs",

        "volStatus",
        "volShort",
        "volLong",
      ].join(",") + "\n";

    csvStream.write(header);
  }

  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let lines = 0;
  let first = null;
  let last = null;

  let minPrice = null;
  let maxPrice = null;

  const eventCounts = new Map();
  const topNetExit15 = [];
  const topChurn15 = [];

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;

    let obj;
    try { obj = JSON.parse(t); } catch { continue; }

    const snap = obj?.snapshot;
    if (!snap) continue;
    lines++;

    const endBlock = snap.endBlock;
    const at = obj.at ?? null;
    const poolName = obj.poolName ?? (snap?.meta?.name ?? "pool");
    const pair = obj.pair ?? snap?.meta?.pair ?? "";

    if (!first) first = { endBlock, at };
    last = { endBlock, at };

    const price = safeNum(snap.priceBaseInQuote);
    if (price !== null && price > 0) {
      minPrice = minPrice === null ? price : Math.min(minPrice, price);
      maxPrice = maxPrice === null ? price : Math.max(maxPrice, price);
    }

    const ev = Array.isArray(snap.events) ? snap.events : [];
    for (const e of ev) eventCounts.set(e, (eventCounts.get(e) ?? 0) + 1);

    try {
      const churn = BigInt(snap?.l1?.churn15Quote ?? "0");
      const net = BigInt(snap?.l1?.netLiq15Quote ?? "0");
      const churnAbs = churn < 0n ? -churn : churn;
      const netAbs = net < 0n ? -net : net;

      const qDec = snap?.meta?.quoteDecimals ?? 18;
      const qSym = snap?.meta?.quoteSymbol ?? "QUOTE";
      const dec = Number.isFinite(Number(qDec)) ? Number(qDec) : 18;

      pushTopN(
        topChurn15,
        { poolName, pair, endBlock, at, churnAbsRaw: churnAbs.toString(), churnAbsFmt: `${fmtUnitsBI(churnAbs, dec, 2)} ${qSym}`, note: snap?.l1?.reason ?? "" },
        topN,
        (x) => Number(x.churnAbsRaw)
      );

      pushTopN(
        topNetExit15,
        { poolName, pair, endBlock, at, netExitAbsRaw: netAbs.toString(), netExitAbsFmt: `${fmtUnitsBI(netAbs, dec, 2)} ${qSym}`, note: snap?.l1?.reason ?? "" },
        topN,
        (x) => Number(x.netExitAbsRaw)
      );
    } catch {}

    if (csvStream) {
      const row =
        [
          obj.at ?? "",
          poolName,
          pair,
          snap.endBlock ?? "",
          snap.priceBaseInQuote ?? "",
          snap?.risk?.score ?? "",
          snap?.risk?.status ?? "",
          snap?.momentum?.trend ?? "",
          snap?.momentum?.deltaScore ?? "",
          Array.isArray(snap.events) ? snap.events.join("|") : "",

          snap.syncBlock ?? "",
          snap.syncAgeBlocks ?? "",
          snap.lastEventBlock ?? "",
          snap.logAgeBlocks ?? "",
          snap.logsConfidence ?? "",
          snap.reservesFrom ?? "",

          snap?.flow?.status ?? "",
          snap?.flow?.netQuote ?? "",
          snap?.flow?.grossQuote ?? "",
          snap?.flow?.netPct ?? "",
          snap?.flow?.grossPct ?? "",

          snap?.l1?.status ?? "",
          snap?.l1?.netLiq15Quote ?? "",
          snap?.l1?.churn15Quote ?? "",

          snap?.m1?.status ?? "",
          snap?.m1?.bouncePairs ?? "",
          snap?.m1?.classicCandidates ?? "",
          snap?.m1?.reversalPairs ?? "",

          snap?.vol?.status ?? "",
          snap?.vol?.short ?? "",
          snap?.vol?.long ?? "",
        ].map(csvEscape).join(",") + "\n";

      csvStream.write(row);
    }
  }

  if (csvStream) {
    await new Promise((resolve, reject) => {
      csvStream.end(() => resolve());
      csvStream.on("error", reject);
    });
  }

  console.log(`=== NDJSON SUMMARY ===`);
  console.log(`file=${filePath}`);
  console.log(`lines=${lines}`);
  if (first && last) console.log(`blockRange=${first.endBlock} .. ${last.endBlock}`);
  if (minPrice !== null && maxPrice !== null) {
    console.log(`priceRange= ${minPrice.toLocaleString(undefined, { maximumFractionDigits: 6 })} .. ${maxPrice.toLocaleString(undefined, { maximumFractionDigits: 6 })}`);
  } else {
    console.log(`priceRange=n/a`);
  }
  if (csvFile) console.log(`csv=written -> ${csvFile}`);
  console.log("");

  console.log("EVENT COUNTS (snapshots with label)");
  if (!eventCounts.size) console.log("  (none)\n");
  else {
    const arr = [...eventCounts.entries()].sort((a, b) => b[1] - a[1]);
    for (const [k, v] of arr) console.log(`  ${k}: ${v}`);
    console.log("");
  }

  const printTop = (title, arr, { noneIfAllZero = false, key = null } = {}, formatter = null) => {
    console.log(title);
    if (!arr.length) { console.log("  (none)\n"); return; }
    if (noneIfAllZero && key && !anyPositive(arr, key)) { console.log("  (none detected)\n"); return; }
    for (const x of arr) console.log("  " + (formatter ? formatter(x) : JSON.stringify(x)));
    console.log("");
  };

  printTop(
    `TOP ${topN} | L1 churn15 (abs)`,
    topChurn15,
    { noneIfAllZero: true, key: "churnAbsRaw" },
    (x) => `pool=${x.poolName} endBlock=${x.endBlock} churn15Abs=${x.churnAbsFmt} at=${x.at ?? "n/a"} (${shortAt(x.at)})  note=${x.note}`
  );

  printTop(
    `TOP ${topN} | L1 netExit15 (abs)`,
    topNetExit15,
    { noneIfAllZero: true, key: "netExitAbsRaw" },
    (x) => `pool=${x.poolName} endBlock=${x.endBlock} netExitAbs=${x.netExitAbsFmt} at=${x.at ?? "n/a"} (${shortAt(x.at)})  note=${x.note}`
  );

  console.log("Done.");
}

// ---------------- Pool state init ----------------
function createPoolState({ name, pair }) {
  return {
    name,
    pair: ethers.getAddress(pair),

    pairMeta: null,

    priceHistory: [],
    maxPricePoints: 1200,

    mevBaseline: new RollingBaseline(CLI.mevBaseline),
    trendScores: [],

    lastSnapshot: null,
    lastBucket: null,

    // incremental logs
    lastProcessedBlock: 0,
    lastFetchedBlock: null,
    logBuffer: [],
    logSeenKeys: new Set(),
    lastLogsConfidence: "unavailable",
  };
}

// ---------------- LIVE MODE ----------------
async function runLive(pools, writer) {
  let lastTickHad429 = false;

  while (true) {
    const at = nowIso();
    try {
      RPC_METRICS.lastWas429 = false;

      const latestBlock = await rpcCall(() => provider.getBlockNumber(), { label: "getBlockNumber" });

      for (let i = 0; i < pools.length; i++) {
        const p = pools[i];
        p.lastProcessedBlock = latestBlock;

        const lookbackStart = Math.max(0, latestBlock - computeLookbackBlocks());

        let fromBlock;
        if (p.lastFetchedBlock === null) fromBlock = lookbackStart;
        else fromBlock = Math.max(lookbackStart, p.lastFetchedBlock - CLI.reorgOverlapBlocks);

        const logsRes = await fetchLogsSmart(p.name, p.pair, fromBlock, latestBlock);

        mergeLogsIntoBuffer(p, logsRes.logs);

        if (logsRes.confidence !== "unavailable") {
          p.lastFetchedBlock = latestBlock;
        }

        p.lastLogsConfidence = logsRes.confidence;

        const reservesFallback = await fetchReservesFallback(p.name, p.pair);

        const snap = computeSnapshot(
          p,
          latestBlock,
          p.logBuffer,
          { confidence: logsRes.confidence },
          reservesFallback
        );

        p.lastSnapshot = snap;

        if (writer) writer.write(buildNdjsonRecord({ mode: "live", at, poolName: p.name, pair: p.pair, snapshot: snap }));

        const curBucket = snap?.risk?.status ?? "n/a";
        if (CLI.alertOnBucketChange) {
          if (p.lastBucket === null) console.log(`🚨 ALERT START -> ${curBucket} | ${operatorSummary(snap)}`);
          else if (curBucket !== p.lastBucket) console.log(`🚨 ALERT ${p.lastBucket} -> ${curBucket} | ${operatorSummary(snap)}`);
        }
        p.lastBucket = curBucket;

        if (CLI.poolStaggerMs > 0 && i < pools.length - 1) {
          await sleep(CLI.poolStaggerMs);
        }
      }

      if (!CLI.json) {
        renderTable({ pools, latestBlock, mode: "live" });
        writeJsonAtomic(CLI.latestFile, buildLatestPayload({ mode: "live", at, latestBlock, pools }));
      } else {
        console.log(JSON.stringify({ at, latestBlock, pools: pools.map((p) => p.lastSnapshot) }, null, 2));
      }

      lastTickHad429 = Boolean(RPC_METRICS.lastWas429);
    } catch (err) {
      const msg = err?.message ?? String(err);
      console.error(`[${nowIso()}] Error: ${msg}`);
      console.log("Retrying next poll...");
    }

    if (lastTickHad429 && CLI.post429CooldownMs > 0) {
      await sleep(CLI.post429CooldownMs);
    }

    await sleep(CLI.poll * 1000);
  }
}

// ---------------- REPLAY MODE ----------------
async function runReplay(pools, fromBlock, toBlock, writer) {
  if (!Number.isInteger(fromBlock) || !Number.isInteger(toBlock) || fromBlock < 0 || toBlock < fromBlock) {
    console.error("Invalid replay range. Use: --from-block <n> --to-block <n>");
    process.exit(1);
  }

  const stepBlocks = Math.max(1, Math.round((CLI.poll / 60) * BLOCKS_PER_MINUTE));
  const lookbackBlocks = computeLookbackBlocks();

  for (let end = fromBlock; end <= toBlock; end += stepBlocks) {
    const at = nowIso();
    const start = Math.max(fromBlock, end - lookbackBlocks);

    try {
      for (let i = 0; i < pools.length; i++) {
        const p = pools[i];

        const logsRes = await fetchLogsSmart(p.name, p.pair, start, end);
        const reservesFallback = await fetchReservesFallback(p.name, p.pair);
        const snap = computeSnapshot(p, end, logsRes.logs, { confidence: logsRes.confidence }, reservesFallback);
        p.lastSnapshot = snap;

        if (writer) writer.write(buildNdjsonRecord({ mode: "replay", at, poolName: p.name, pair: p.pair, snapshot: snap }));

        const curBucket = snap?.risk?.status ?? "n/a";
        if (CLI.alertOnBucketChange) {
          if (p.lastBucket === null) console.log(`🚨 ALERT START -> ${curBucket} | ${operatorSummary(snap)}`);
          else if (curBucket !== p.lastBucket) console.log(`🚨 ALERT ${p.lastBucket} -> ${curBucket} | ${operatorSummary(snap)}`);
        }
        p.lastBucket = curBucket;

        if (CLI.poolStaggerMs > 0 && i < pools.length - 1) {
          await sleep(CLI.poolStaggerMs);
        }
      }

      if (!CLI.json) {
        renderTable({ pools, latestBlock: end, mode: "replay" });
        writeJsonAtomic(CLI.latestFile, buildLatestPayload({ mode: "replay", at, latestBlock: end, pools }));
        console.log(`replayProgress: endBlock=${end}/${toBlock} (step=${stepBlocks} blocks)\n`);
      }
    } catch (err) {
      const msg = err?.message ?? String(err);
      console.error(`[${nowIso()}] Replay error at endBlock=${end}: ${msg}`);
    }
  }

  console.log("Replay done.");
}

// ---------------- MAIN ----------------
(async () => {
  if (CLI.summarizeFile) {
    await summarizeNdjson(CLI.summarizeFile, CLI.top, CLI.csvFile);
    process.exit(0);
  }

  const RPC_URL = process.env.RPC_URL;
  if (!RPC_URL) {
    console.error(
      "Missing RPC_URL env var.\nExample:\n  export RPC_URL=https://mainnet.infura.io/v3/YOUR_KEY\n  node index.js\n"
    );
    process.exit(1);
  }

  provider = new ethers.JsonRpcProvider(RPC_URL);
  await rpcCall(() => provider.getNetwork(), { label: "getNetwork" });

  const poolsCfg = loadPoolsConfig(CLI.poolsFile);
  const pools = poolsCfg.map(createPoolState);

  for (const p of pools) {
    p.pairMeta = await resolvePairMeta(p.pair, p.name);
    if (CLI.poolStaggerMs > 0) await sleep(Math.min(CLI.poolStaggerMs, 500));
  }

  let writer = null;
  try {
    if (CLI.outFile) writer = createNdjsonWriter(CLI.outFile, { pretty: CLI.pretty });
  } catch (e) {
    console.error(`[${nowIso()}] Could not open NDJSON file: ${CLI.outFile}`);
    console.error(`Reason: ${e?.message ?? e}`);
    process.exit(1);
  }

  process.on("SIGINT", async () => {
    try { if (writer) await writer.close(); } catch {}
    process.exit(0);
  });

  const isReplay = CLI.fromBlock !== null || CLI.toBlock !== null;
  if (isReplay) {
    const fromB = CLI.fromBlock ?? 0;
    const toB = CLI.toBlock ?? (await rpcCall(() => provider.getBlockNumber(), { label: "getBlockNumber(toBlock)" }));
    await runReplay(pools, fromB, toB, writer);
    process.exit(0);
  } else {
    await runLive(pools, writer);
  }
})().catch((err) => {
  console.error(`[${nowIso()}] Fatal: ${err?.message ?? err}`);
  process.exit(1);
});