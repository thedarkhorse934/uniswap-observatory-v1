const REFRESH_MS = 5000;
const DATA_URL = "../latest.json";

const priceHistory = {};
const previousState = {};

const els = {
  statusText: document.getElementById("statusText"),
  lastUpdated: document.getElementById("lastUpdated"),
  latestBlock: document.getElementById("latestBlock"),
  rpcSummary: document.getElementById("rpcSummary"),
  poolGrid: document.getElementById("poolGrid"),
  poolCount: document.getElementById("poolCount"),
  messageBar: document.getElementById("messageBar"),
};

function formatNumber(value) {
  if (value === null || value === undefined) return "—";
  const num = Number(value);
  if (!Number.isFinite(num)) return String(value);
  return num.toLocaleString(undefined, { maximumFractionDigits: 6 });
}

function extractPools(data) {
  return Array.isArray(data?.pools) ? data.pools : [];
}

function getPoolPrice(pool) {
  return pool.priceBaseInQuote ?? 0;
}

function getTrend(pool) {
  return pool.momentum?.trend ?? "FLAT";
}

function getDriver(pool) {
  const drivers = pool.snapshot?.risk?.drivers ?? [];
  const meaningful = drivers.find(d => !d.startsWith("+0"));
  return meaningful ?? drivers[0] ?? "—";
}

function formatDriver(driver) {
  if (!driver || driver === "—") return "—";

  return String(driver)
    .replace(/^(\+\d+)\s*/i, "")
    .replace(/Depth HIGH\s*\(Depth fragile:\s*~/i, "Depth fragile (~")
    .replace(/Depth ELEVATED\s*\(Depth thinning:\s*~/i, "Depth thinning (~")
    .replace(/impact for ~?100000\s+(USDC|USDT|USD)\.?\)/i, "impact for $100k)")
    .replace(/impact for ~?10\s+WETH\.?\)/i, "impact for ~10 WETH)")
    .replace(/\.\)/g, ")")
    .trim();
}

function scoreClass(score) {
  if (score <= 15) return "score-low";
  if (score <= 30) return "score-mid";
  return "score-high";
}

function scoreValueClass(score) {
  if (score <= 15) return "score-value-low";
  if (score <= 30) return "score-value-mid";
  return "score-value-high";
}

function bucketClass(bucket) {
  const b = String(bucket || "").toUpperCase();
  if (b === "OK") return "badge ok";
  if (b === "ELEVATED") return "badge elevated";
  if (b === "ALERT") return "badge alert";
  return "badge neutral";
}

function trendDisplay(trend) {
  if (trend === "UP") return { text: "▲ UP", class: "trend-up" };
  if (trend === "DOWN") return { text: "▼ DOWN", class: "trend-down" };
  return { text: "■ FLAT", class: "trend-flat" };
}

function sparkline(name, price) {
  if (!priceHistory[name]) priceHistory[name] = [];
  const arr = priceHistory[name];

  arr.push(price);
  if (arr.length > 20) arr.shift();

  const min = Math.min(...arr);
  const max = Math.max(...arr);
  const blocks = "▁▂▃▄▅▆▇";

  return arr.map(v => {
    const r = (v - min) / (max - min || 1);
    return blocks[Math.floor(r * (blocks.length - 1))];
  }).join("");
}

function detectChange(name, price, score, eventsJoined) {
  const prev = previousState[name];
  previousState[name] = { price, score, eventsJoined };

  if (!prev) return "";

  if (
    prev.price !== price ||
    prev.score !== score ||
    prev.eventsJoined !== eventsJoined
  ) {
    return "card-flash";
  }

  return "";
}

function eventBadgeClass(eventName) {
  switch (eventName) {
    case "STATE_STALE":
      return "event-badge event-stale";
    case "QUIET_POOL":
      return "event-badge event-quiet";
    case "MEV_SWARM":
      return "event-badge event-danger";
    case "FLOW_SURGE":
      return "event-badge event-warn";
    case "DEPTH_FRAGMENT":
      return "event-badge event-danger";
    case "LIQUIDITY_EXIT":
      return "event-badge event-danger";
    default:
      return "event-badge";
  }
}

function eventLabel(eventName) {
  switch (eventName) {
    case "STATE_STALE":
      return "State stale";
    case "QUIET_POOL":
      return "Quiet pool";
    case "MEV_SWARM":
      return "MEV swarm";
    case "FLOW_SURGE":
      return "Flow surge";
    case "DEPTH_FRAGMENT":
      return "Depth fragile";
    case "LIQUIDITY_EXIT":
      return "Liquidity exit";
    default:
      return String(eventName || "")
        .toLowerCase()
        .replace(/_/g, " ")
        .replace(/\b\w/g, c => c.toUpperCase());
  }
}

function buildAlertSummary(pools, rpc) {
  const alertPools = pools.filter(p => (p.score ?? 0) >= 25);
  const stalePools = pools.filter(p => (p.events || []).includes("STATE_STALE"));
  const mevPools = pools.filter(p => (p.events || []).includes("MEV_SWARM"));
  const rateLimited = (rpc?.r429 ?? 0) > 0;

  const parts = [];

  if (alertPools.length) {
    parts.push(`⚠ ${alertPools.length} elevated-risk pool${alertPools.length === 1 ? "" : "s"}`);
  }
  if (stalePools.length) {
    parts.push(`⚠ ${stalePools.length} pool${stalePools.length === 1 ? "" : "s"} stale`);
  }
  if (mevPools.length) {
    parts.push(`⚠ ${mevPools.length} MEV alert${mevPools.length === 1 ? "" : "s"}`);
  }
  if (rateLimited) {
    parts.push(`RPC 429s: ${rpc.r429}`);
  }

  if (!parts.length) {
    return { text: "All monitored pools look calm.", level: "info" };
  }

  return {
    text: parts.join("  |  "),
    level: alertPools.length || mevPools.length ? "danger" : "warn"
  };
}

function buildMarketSummary(pools) {
  const wethUsdc = pools.find(p => p.name === "USDC/WETH");
  const wethUsdt = pools.find(p => p.name === "USDT/WETH");
  const wbtcWeth = pools.find(p => p.name === "WBTC/WETH");
  const stable = pools.find(p => p.name === "USDC/USDT");

  const ethPrice =
    wethUsdc?.priceBaseInQuote ??
    wethUsdt?.priceBaseInQuote ??
    null;

  const btcEth = wbtcWeth?.priceBaseInQuote ?? null;
  const btcUsd =
    Number.isFinite(btcEth) && Number.isFinite(ethPrice)
      ? btcEth * ethPrice
      : null;

  const stablePx = stable?.priceBaseInQuote ?? null;

  let stableState = "Stable peg —";
  if (Number.isFinite(stablePx)) {
    const driftPct = Math.abs((stablePx - 1) * 100);
    stableState = driftPct < 0.05 ? "Stable peg OK" : `Stable drift ${driftPct.toFixed(2)}%`;
  }

  const parts = [];

  if (Number.isFinite(ethPrice)) {
    parts.push(`ETH $${Number(ethPrice).toLocaleString(undefined, { maximumFractionDigits: 2 })}`);
  }
  if (Number.isFinite(btcUsd)) {
    parts.push(`BTC $${Number(btcUsd).toLocaleString(undefined, { maximumFractionDigits: 0 })}`);
  }
  parts.push(stableState);

  return `Markets: ${parts.join(" | ")}`;
}

function renderPools(pools) {
  pools.sort((a, b) => (b.score ?? 0) - (a.score ?? 0));

  els.poolCount.textContent = `${pools.length} pool${pools.length === 1 ? "" : "s"}`;

  els.poolGrid.innerHTML = pools.map((pool, index) => {
    const name = pool.name;
    const price = getPoolPrice(pool);
    const score = pool.score ?? 0;
    const bucket = pool.bucket ?? "—";
    const trend = trendDisplay(getTrend(pool));
    const driver = getDriver(pool);
    const driverText = formatDriver(driver);
    const events = pool.events || [];
    const spark = sparkline(name, price);
    const scoreCls = scoreClass(score);
    const scoreValCls = scoreValueClass(score);
    const flash = detectChange(name, price, score, events.join("|"));
    const pulse = score >= 25 ? "risk-pulse" : "";
    const hot = score >= 25 ? "card-hot" : score >= 16 ? "card-warm" : "";
    const badgeCls = bucketClass(bucket);
    const topRisk = index === 0 ? "top-risk" : "";

    return `
      <article class="pool-card ${scoreCls} ${flash} ${pulse} ${hot} ${topRisk}">
        <div class="pool-top">
          <h3 class="pool-name">${name}</h3>
          <span class="${badgeCls}">${bucket}</span>
        </div>

        <div class="pool-price">${formatNumber(price)}</div>

        <div class="sparkline">${spark}</div>

        <div class="pool-meta">
          <div class="meta-row">
            <span class="meta-row-label">Score</span>
            <span class="meta-row-value score-value ${scoreValCls}">${score}</span>
          </div>

          <div class="meta-row">
            <span class="meta-row-label">Trend</span>
            <span class="meta-row-value ${trend.class}">${trend.text}</span>
          </div>

          <div class="meta-row">
            <span class="meta-row-label">Top driver</span>
            <span class="meta-row-value">${driverText}</span>
          </div>

          <div class="meta-row">
            <span class="meta-row-label">Events</span>
            <span class="meta-row-value">
              ${
                events.length
                  ? events.map(e => `<span class="${eventBadgeClass(e)}">${eventLabel(e)}</span>`).join("")
                  : "None"
              }
            </span>
          </div>
        </div>
      </article>
    `;
  }).join("");
}

async function loadData() {
  try {
    const response = await fetch(`${DATA_URL}?t=${Date.now()}`, { cache: "no-store" });
    const data = await response.json();

    els.statusText.textContent = "Live";
    els.latestBlock.textContent = data.latestBlock ?? "—";

    const rpc = data.rpc || {};
    const pools = extractPools(data);

    els.rpcSummary.innerHTML = `
      <div>calls=${rpc.calls ?? "—"} | errs=${rpc.errs ?? "—"} | 429=${rpc.r429 ?? "—"} | avg=${rpc.avgMs ?? "—"}ms</div>
      <div class="market-summary">${buildMarketSummary(pools)}</div>
    `;

    els.lastUpdated.textContent = data.at ?? "—";

    const alert = buildAlertSummary(pools, rpc);

    els.messageBar.className = `message-bar ${alert.level} ${alert.level !== "info" ? "alert-pulse" : ""}`;
    els.messageBar.textContent = alert.text;

    renderPools(pools);
  } catch (err) {
    console.error("Dashboard load error:", err);
    els.statusText.textContent = "Unavailable";
    els.messageBar.className = "message-bar error";
    els.messageBar.textContent = "Could not load latest.json";
    els.poolGrid.innerHTML = `<div class="empty-state">latest.json unavailable</div>`;
  }
}

loadData();
setInterval(loadData, REFRESH_MS);