#!/usr/bin/env node
import { ethers } from "ethers";

// ---------- CONFIG ----------
const RPC_URL = process.env.RPC_URL;
if (!RPC_URL) {
  console.error(
    "Missing RPC_URL env var.\n" +
      "Example:\n" +
      "  export RPC_URL=https://mainnet.infura.io/v3/YOUR_KEY\n" +
      "  node index.js\n"
  );
  process.exit(1);
}

const PAIR = "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"; // Uniswap V2 WETH/USDC

const POLL_SECONDS = 60;

// We approximate time by blocks to stay Infura-friendly.
// Ethereum ~12s => ~5 blocks/min, ~10 blocks ≈ 120 seconds.
const BLOCKS_PER_MINUTE = 5;
const M1_BOUNCE_BLOCKS = 10; // ≈120s

// Lookback for logs (keep bounded)
const HEARTBEAT_MIN = 5;
const L1_WINDOW_MIN = 15;
const L1_BASELINE_MIN = 60;
const M1_WINDOW_MIN = 15;

// Extra padding for safety; keep small to avoid large log queries
const EXTRA_PADDING_BLOCKS = 30;

// Pair specifics
const USDC_DECIMALS = 6;
const WETH_DECIMALS = 18;

// L1 thresholds
const L1_MULTIPLIER = 2.0;
const L1_MIN_USDC_REMOVED = 50_000;
const L1_COOLDOWN_MIN = 30;

// M1 thresholds (two-tier + bounce)
const M1_COOLDOWN_MIN = 15;

// Classic (rare but strong)
const M1_CLASSIC_HIGH_CANDIDATES = 3;
const M1_CLASSIC_HIGH_BLOCKS = 2;

// Reversal (more common)
const M1_REVERSAL_ELEVATED = 2;
const M1_REVERSAL_HIGH = 6;

// Bounce (cross-block, uses block distance proxy)
const M1_BOUNCE_ELEVATED = 3;
const M1_BOUNCE_HIGH = 8;

// Minimal UniswapV2Pair ABI events
const PAIR_ABI = [
  "event Swap(address indexed sender,uint amount0In,uint amount1In,uint amount0Out,uint amount1Out,address indexed to)",
  "event Mint(address indexed sender,uint amount0,uint amount1)",
  "event Burn(address indexed sender,uint amount0,uint amount1,address indexed to)",
  "event Sync(uint112 reserve0,uint112 reserve1)",
];

const provider = new ethers.JsonRpcProvider(RPC_URL);
const iface = new ethers.Interface(PAIR_ABI);

let lastL1AlertAtMs = 0;
let lastM1AlertAtMs = 0;

function nowIso() {
  return new Date().toISOString().replace("T", " ").slice(0, 19);
}

function inCooldown(msNow, lastAlertMs, cooldownMin) {
  return msNow - lastAlertMs < cooldownMin * 60_000;
}

function formatUSDC(amountBigInt) {
  const s = ethers.formatUnits(amountBigInt, USDC_DECIMALS);
  const n = Number(s);
  if (!Number.isFinite(n)) return s;
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function formatWETH(amountBigInt) {
  const s = ethers.formatUnits(amountBigInt, WETH_DECIMALS);
  const n = Number(s);
  if (!Number.isFinite(n)) return s;
  return n.toLocaleString(undefined, { maximumFractionDigits: 6 });
}

// NOTE for this specific pair address (0xB4e16...):
// token0 = USDC, token1 = WETH
// Sync(reserve0,reserve1) => reserve0=USDC, reserve1=WETH
function priceUsdcPerWeth(reserve0USDC, reserve1WETH) {
  const usdc = Number(ethers.formatUnits(reserve0USDC, USDC_DECIMALS));
  const weth = Number(ethers.formatUnits(reserve1WETH, WETH_DECIMALS));
  if (!Number.isFinite(usdc) || !Number.isFinite(weth) || weth === 0) return null;
  return usdc / weth;
}

// Swap direction for this pair:
// amount0In > 0 => USDC -> WETH  (dir +1)
// amount1In > 0 => WETH -> USDC  (dir -1)
function swapDirection(args) {
  const a0in = args.amount0In;
  const a1in = args.amount1In;

  if (a0in > 0n && a1in === 0n) return +1;
  if (a1in > 0n && a0in === 0n) return -1;
  return 0;
}

function countSandwichLikeInBlock(swapsInBlock) {
  let candidates = 0;

  for (let j = 1; j < swapsInBlock.length - 1; j++) {
    const victim = swapsInBlock[j];
    if (victim.dir === 0) continue;

    let hasFront = false;
    for (let i = 0; i < j; i++) {
      const s = swapsInBlock[i];
      if (s.dir === victim.dir && s.txHash !== victim.txHash) {
        hasFront = true;
        break;
      }
    }
    if (!hasFront) continue;

    let hasBack = false;
    for (let k = j + 1; k < swapsInBlock.length; k++) {
      const s = swapsInBlock[k];
      if (s.dir === -victim.dir && s.txHash !== victim.txHash) {
        hasBack = true;
        break;
      }
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

// Bounce pairs across blocks: opposite direction swap within N blocks after a swap.
// We count at most 1 bounce per starting swap to avoid runaway counts.
function countBouncePairs(swapsSorted, bounceBlocks) {
  let bounces = 0;

  for (let i = 0; i < swapsSorted.length; i++) {
    const a = swapsSorted[i];
    if (a.dir === 0) continue;

    for (let j = i + 1; j < swapsSorted.length; j++) {
      const b = swapsSorted[j];
      if (b.bn - a.bn > bounceBlocks) break;
      if (b.dir === 0) continue;

      if (b.dir === -a.dir && b.txHash !== a.txHash) {
        bounces++;
        break;
      }
    }
  }

  return bounces;
}

async function mainLoop() {
  try {
    const latestBlock = await provider.getBlockNumber();

    // Compute lookback blocks (bounded)
    const totalLookbackMin = Math.max(
      HEARTBEAT_MIN,
      L1_WINDOW_MIN + L1_BASELINE_MIN,
      M1_WINDOW_MIN
    );

    const lookbackBlocks =
      totalLookbackMin * BLOCKS_PER_MINUTE + EXTRA_PADDING_BLOCKS;

    const startBlock = Math.max(0, latestBlock - lookbackBlocks);

    const logs = await provider.getLogs({
      address: PAIR,
      fromBlock: startBlock,
      toBlock: latestBlock,
      topics: [[
        ethers.id("Swap(address,uint256,uint256,uint256,uint256,address)"),
        ethers.id("Mint(address,uint256,uint256)"),
        ethers.id("Burn(address,uint256,uint256,address)"),
        ethers.id("Sync(uint112,uint112)")
      ]]
    });

    // Cutoffs (block-based approximation)
    const hbCutoff = latestBlock - HEARTBEAT_MIN * BLOCKS_PER_MINUTE;

    const l1Cutoff = latestBlock - L1_WINDOW_MIN * BLOCKS_PER_MINUTE;
    const baselineStart = latestBlock - (L1_WINDOW_MIN + L1_BASELINE_MIN) * BLOCKS_PER_MINUTE;
    const baselineEnd = l1Cutoff;

    const m1Cutoff = latestBlock - M1_WINDOW_MIN * BLOCKS_PER_MINUTE;

    // Heartbeat
    let swapsHb = 0, mintsHb = 0, burnsHb = 0;

    // L1 burn aggregates
    let burns15Count = 0;
    let burns60Count = 0;
    let burns15USDC = 0n;
    let burns60USDC = 0n;

    // M1 swap collections
    const swapsByBlock = new Map(); // bn -> [{logIndex, bn, txHash, dir}]
    const swapsM1 = []; // flat list for bounce

    // Pool state (from Sync) — we’ll keep the latest Sync in the lookback window
    let lastSyncBlock = null;
    let reserve0USDC = null; // token0 reserve (USDC for this pair)
    let reserve1WETH = null; // token1 reserve (WETH for this pair)

    let lastEventBlock = null;
    let lastEventType = null;

    for (const log of logs) {
      const parsed = iface.parseLog(log);
      if (!parsed) continue;

      const bn = log.blockNumber;

      if (bn >= hbCutoff) {
        if (parsed.name === "Swap") swapsHb++;
        if (parsed.name === "Mint") mintsHb++;
        if (parsed.name === "Burn") burnsHb++;
        if (parsed.name === "Sync") {
          // counts not part of heartbeat currently; leaving as-is (no counter)
        }
      }

      if (parsed.name === "Sync") {
        // Keep the latest Sync in the lookback (end-of-most-recent state)
        lastSyncBlock = bn;
        reserve0USDC = parsed.args.reserve0;
        reserve1WETH = parsed.args.reserve1;
      }

      if (parsed.name === "Burn") {
        const amount0 = parsed.args.amount0; // USDC

        if (bn >= l1Cutoff) {
          burns15Count++;
          burns15USDC += amount0;
        }
        if (bn >= baselineStart && bn < baselineEnd) {
          burns60Count++;
          burns60USDC += amount0;
        }
      }

      if (parsed.name === "Swap" && bn >= m1Cutoff) {
        const dir = swapDirection(parsed.args);
        const entry = { bn, logIndex: log.logIndex, txHash: log.transactionHash, dir };

        swapsM1.push(entry);
        if (!swapsByBlock.has(bn)) swapsByBlock.set(bn, []);
        swapsByBlock.get(bn).push(entry);
      }

      lastEventBlock = bn;
      lastEventType = parsed.name;
    }

    const poolPrice =
      (reserve0USDC !== null && reserve1WETH !== null)
        ? priceUsdcPerWeth(reserve0USDC, reserve1WETH)
        : null;

    // ----- L1 compute -----
    const baselinePer15Count = Math.floor(burns60Count / 4);
    const baselinePer15USDC = burns60USDC / 4n;

    let l1Status = "OK";
    let l1Reason = "No abnormal liquidity withdrawal detected.";
    let l1Triggered = false;

    const burns15USDCFloat = Number(ethers.formatUnits(burns15USDC, USDC_DECIMALS));
    const baseline15USDCFloat = Number(ethers.formatUnits(baselinePer15USDC, USDC_DECIMALS));

    if (burns15USDCFloat >= L1_MIN_USDC_REMOVED) {
      if (burns60Count === 0 && burns60USDC === 0n) {
        if (burns15Count >= 2) {
          l1Triggered = true;
          l1Status = "HIGH";
          l1Reason = "Material liquidity removals detected despite no recent baseline activity (possible liquidity flight).";
        } else {
          l1Status = "ELEVATED";
          l1Reason = "Material liquidity removal detected, but baseline is near-zero.";
        }
      } else {
        const countSpike =
          baselinePer15Count === 0
            ? burns15Count >= 2
            : burns15Count > baselinePer15Count * L1_MULTIPLIER;

        const usdcSpike =
          baselinePer15USDC === 0n
            ? burns15USDCFloat >= L1_MIN_USDC_REMOVED
            : burns15USDCFloat > baseline15USDCFloat * L1_MULTIPLIER;

        if (countSpike && usdcSpike) {
          l1Triggered = true;
          l1Status = "HIGH";
          l1Reason = "Liquidity removals in the last 15m are significantly higher than the prior 60m baseline.";
        } else if (countSpike || usdcSpike) {
          l1Status = "ELEVATED";
          l1Reason = "Liquidity removal activity is above baseline on at least one dimension (size or frequency).";
        }
      }
    }

    // ----- M1 compute (Classic + Reversal + Bounce) -----
    let classicCandidates = 0;
    let blocksWithClassic = 0;
    let reversalPairs = 0;
    let blocksWithReversal = 0;
    let blocksWith3PlusSwaps = 0;

    for (const [bn, arr] of swapsByBlock.entries()) {
      arr.sort((a, b) => a.logIndex - b.logIndex);

      if (arr.length >= 3) blocksWith3PlusSwaps++;

      const c = countSandwichLikeInBlock(arr);
      if (c > 0) {
        classicCandidates += c;
        blocksWithClassic++;
      }

      const r = countReversalPairsInBlock(arr);
      if (r > 0) {
        reversalPairs += r;
        blocksWithReversal++;
      }
    }

    swapsM1.sort((a, b) => (a.bn - b.bn) || (a.logIndex - b.logIndex));
    const bouncePairs = countBouncePairs(swapsM1, M1_BOUNCE_BLOCKS);

    let m1Status = "OK";
    let m1Reason =
      "No classic sandwich-like patterns detected; no strong same-block reversal; no significant cross-block bounce patterns.";
    let m1Triggered = false;

    const classicHigh =
      classicCandidates >= M1_CLASSIC_HIGH_CANDIDATES &&
      blocksWithClassic >= M1_CLASSIC_HIGH_BLOCKS;

    const reversalHigh = reversalPairs >= M1_REVERSAL_HIGH;
    const bounceHigh = bouncePairs >= M1_BOUNCE_HIGH;

    if (classicHigh || reversalHigh || bounceHigh) {
      m1Status = "HIGH";
      m1Triggered = true;

      if (classicHigh) {
        m1Reason = "Repeated same-block directional patterns consistent with classic sandwiching were detected.";
      } else if (reversalHigh) {
        m1Reason = "High volume of same-block direction reversals detected (strong bot/arb/backrun signal).";
      } else {
        m1Reason = `High volume of opposite-direction swaps within ~${M1_BOUNCE_BLOCKS} blocks detected (strong bot/arb/backrun signal).`;
      }
    } else {
      const elevated =
        classicCandidates >= 1 ||
        reversalPairs >= M1_REVERSAL_ELEVATED ||
        bouncePairs >= M1_BOUNCE_ELEVATED;

      if (elevated) {
        m1Status = "ELEVATED";
        m1Reason = `Some bot/MEV-like activity signals detected (classic/reversal/bounce). Bounce uses ~${M1_BOUNCE_BLOCKS} blocks window.`;
      }
    }

    // cooldowns
    const msNow = Date.now();
    const l1CanAlert = !inCooldown(msNow, lastL1AlertAtMs, L1_COOLDOWN_MIN);
    const m1CanAlert = !inCooldown(msNow, lastM1AlertAtMs, M1_COOLDOWN_MIN);

    // ----- Print -----
    console.clear();
    console.log(`Uniswap Risk CLI  |  ${nowIso()}`);
    console.log(`PAIR: WETH/USDC (Uniswap V2)`);
    console.log(`Address: ${PAIR}`);
    console.log(`Latest block: ${latestBlock} (mainnet)`);
    console.log("");

    // NEW: Pool state (reserves + price)
    console.log(`Pool state (from latest Sync in lookback):`);
    if (reserve0USDC !== null && reserve1WETH !== null) {
      console.log(`  Sync block: ${lastSyncBlock ?? "n/a"}`);
      console.log(`  Reserves: ${formatWETH(reserve1WETH)} WETH | ${formatUSDC(reserve0USDC)} USDC`);
      console.log(`  Price: 1 WETH ≈ ${poolPrice ? poolPrice.toLocaleString(undefined, { maximumFractionDigits: 2 }) : "n/a"} USDC`);
    } else {
      console.log(`  No Sync event found in lookback window (unexpected).`);
    }
    console.log("");

    console.log(`Heartbeat (~last ${HEARTBEAT_MIN}m):`);
    console.log(`  Swaps: ${swapsHb}`);
    console.log(`  Mints: ${mintsHb}`);
    console.log(`  Burns: ${burnsHb}`);
    console.log(`  Last event: ${lastEventType ?? "n/a"} @ block ${lastEventBlock ?? "n/a"}`);
    console.log("");

    console.log(`L1 — Liquidity Fragility Rising (LP removals)`);
    console.log(`  Status: ${l1Status}`);
    console.log(`  Last ${L1_WINDOW_MIN}m burns: ${burns15Count} | USDC removed: ${formatUSDC(burns15USDC)}`);
    console.log(
      `  Baseline (prev ${L1_BASELINE_MIN}m → per ${L1_WINDOW_MIN}m): ~${baselinePer15Count} | ~${baseline15USDCFloat.toLocaleString(undefined, {
        maximumFractionDigits: 2,
      })} USDC`
    );
    console.log(`  Note: ${l1Reason}`);
    console.log("");

    if (l1Triggered && l1CanAlert) {
      console.log(`⚠️  ALERT (L1 HIGH): Liquidity withdrawal spike detected. Remaining LPs face higher slippage/adverse selection.`);
      lastL1AlertAtMs = msNow;
      console.log(`    Cooldown: ${L1_COOLDOWN_MIN} minutes`);
      console.log("");
    }

    console.log(`M1 — Bot/MEV Activity (stable heuristics)`);
    console.log(`  Status: ${m1Status}`);
    console.log(`  Window: last ${M1_WINDOW_MIN}m (approx by blocks)`);
    console.log(`  Classic sandwich-like candidates: ${classicCandidates} (blocks: ${blocksWithClassic})`);
    console.log(`  Same-block reversal pairs: ${reversalPairs} (blocks: ${blocksWithReversal})`);
    console.log(`  Cross-block bounce pairs (≤~${M1_BOUNCE_BLOCKS} blocks): ${bouncePairs}`);
    console.log(`  Blocks with ≥3 swaps: ${blocksWith3PlusSwaps}`);
    console.log(`  Note: ${m1Reason}`);
    console.log("");

    if (m1Triggered && m1CanAlert) {
      console.log(`⚠️  ALERT (M1 HIGH): Strong bot/MEV activity signal detected. LP returns may be suppressed by automated extraction.`);
      lastM1AlertAtMs = msNow;
      console.log(`    Cooldown: ${M1_COOLDOWN_MIN} minutes`);
      console.log("");
    }

    console.log(`Next update in ${POLL_SECONDS}s...`);
  } catch (err) {
    const msg = err?.message ?? String(err);
    console.error(`[${nowIso()}] Error: ${msg}`);
    console.log("Retrying next poll...");
  }
}

await mainLoop();
setInterval(() => mainLoop(), POLL_SECONDS * 1000);
