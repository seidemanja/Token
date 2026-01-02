/**
 * Reward Controller (AMM Swap–Driven)
 *
 * CURRENT STATE:
 * - Swap-driven eligibility + cumulative buy tracking (IMPLEMENTED)
 * - ERC-20 Transfer + balanceOf eligibility (legacy, optional; DISABLED by default)
 *
 * Swap-driven rule:
 * - Determine which pool token is the tracked TOKEN_ADDRESS (token0 or token1).
 * - If tracked token is token0:
 *     buy => pool sends token0 to recipient => amount0 < 0 => tokenBought = -amount0
 * - If tracked token is token1:
 *     buy => pool sends token1 to recipient => amount1 < 0 => tokenBought = -amount1
 * - Accumulate tokenBought per recipient (in base units, stored as decimal strings).
 * - Mint once when cumulativeBuys[recipient] >= threshold AND cohort gate passes.
 *
 * Operational hardening added:
 * - mintFailures + cooldown to avoid spamming retries when minting fails (e.g., out of gas/ETH).
 * - optional setting to suppress throwing on mint failures (keeps backfill loop clean).
 *
 * Env vars:
 * - LEGACY_BALANCE_MINT_ENABLED=true|false (default false)
 * - SWAP_MINT_ENABLED=true|false (default true)
 * - THRESHOLD_TOKENS (interpreted in TOKEN_DECIMALS)
 * - MINT_FAILURE_COOLDOWN_MS (default 60000)
 * - THROW_ON_MINT_FAILURE=true|false (default false)
 */

require("dotenv").config();
const fs = require("fs");
const { ethers } = require("ethers");
let sqlite3 = null;
try {
  sqlite3 = require("sqlite3");
} catch {
  sqlite3 = null;
}

/* ------------------------- shared env ------------------------- */

const {
  network,
  RPC_URL,
  WSS_URL,
  TOKEN_ADDRESS,
  JSTVIP_ADDRESS,
  POOL_ADDRESS,
  POOL_TOKEN0,
  POOL_TOKEN1,
  STATE_FILE,
} = require("../env");

/* ------------------------- helpers ------------------------- */

function envBool(name, def) {
  const raw = (process.env[name] || "").trim().toLowerCase();
  if (!raw) return def;
  return raw === "true" || raw === "1" || raw === "yes" || raw === "y";
}

function envInt(name, def) {
  const raw = (process.env[name] || "").trim();
  if (!raw) return def;
  const n = parseInt(raw, 10);
  return Number.isNaN(n) ? def : n;
}

function loadState(path) {
  try {
    const s = JSON.parse(fs.readFileSync(path, "utf8"));

    // Backward-compatible migration:
    // old:    { lastProcessedBlock }
    // newer:  { lastProcessedTransferBlock, lastProcessedSwapBlock }
    // newest: + { cumulativeBuys, mintedCache, mintFailures }
    const migrated = {
      lastProcessedTransferBlock:
        typeof s.lastProcessedTransferBlock === "number"
          ? s.lastProcessedTransferBlock
          : typeof s.lastProcessedBlock === "number"
            ? s.lastProcessedBlock
            : 0,

      lastProcessedSwapBlock:
        typeof s.lastProcessedSwapBlock === "number" ? s.lastProcessedSwapBlock : 0,

      // BigInt values stored as decimal strings
      cumulativeBuys:
        typeof s.cumulativeBuys === "object" && s.cumulativeBuys ? s.cumulativeBuys : {},

      // Optional: local cache to avoid repeated hasMinted checks on hot paths
      mintedCache:
        typeof s.mintedCache === "object" && s.mintedCache ? s.mintedCache : {},

      // New: record mint failures to avoid retry spam (values are ms timestamps as strings)
      mintFailures:
        typeof s.mintFailures === "object" && s.mintFailures ? s.mintFailures : {},
    };

    return migrated;
  } catch {
    return {
      lastProcessedTransferBlock: 0,
      lastProcessedSwapBlock: 0,
      cumulativeBuys: {},
      mintedCache: {},
      mintFailures: {},
    };
  }
}

function saveState(path, state) {
  fs.writeFileSync(path, JSON.stringify(state, null, 2));
}

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function toLowerAddr(a) {
  try {
    return String(a).toLowerCase();
  } catch {
    return "";
  }
}

function parseBigintSafe(x) {
  if (typeof x === "bigint") return x;
  if (typeof x === "number") return BigInt(x);
  if (typeof x === "string" && x.trim() !== "") return BigInt(x);
  return 0n;
}

function openSqlite(path) {
  if (!sqlite3 || !path) return null;
  const db = new sqlite3.Database(path);
  db.serialize(() => {
    db.run(
      `
      CREATE TABLE IF NOT EXISTS controller_swaps (
        block_number INTEGER NOT NULL,
        tx_hash TEXT NOT NULL,
        log_index INTEGER NOT NULL,
        sender TEXT NOT NULL,
        recipient TEXT NOT NULL,
        amount0 TEXT NOT NULL,
        amount1 TEXT NOT NULL,
        sqrt_price_x96 TEXT NOT NULL,
        liquidity TEXT NOT NULL,
        tick INTEGER NOT NULL,
        token_bought_raw TEXT NOT NULL,
        PRIMARY KEY (tx_hash, log_index)
      );
      `
    );
    db.run(
      `
      CREATE TABLE IF NOT EXISTS controller_transfers (
        block_number INTEGER NOT NULL,
        tx_hash TEXT NOT NULL,
        log_index INTEGER NOT NULL,
        from_address TEXT NOT NULL,
        to_address TEXT NOT NULL,
        value_raw TEXT NOT NULL,
        PRIMARY KEY (tx_hash, log_index)
      );
      `
    );
    db.run(
      `
      CREATE TABLE IF NOT EXISTS controller_mints (
        block_number INTEGER NOT NULL,
        tx_hash TEXT NOT NULL,
        log_index INTEGER NOT NULL,
        to_address TEXT NOT NULL,
        token_id TEXT NOT NULL,
        PRIMARY KEY (tx_hash, log_index)
      );
      `
    );
  });
  return db;
}

function dbRun(db, sql, params) {
  if (!db) return Promise.resolve();
  return new Promise((resolve, reject) => {
    db.run(sql, params, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

/* ------------------------- ABIs ------------------------- */

const ERC20_ABI = [
  "event Transfer(address indexed from, address indexed to, uint256 value)",
  "function balanceOf(address) view returns (uint256)",
];

const NFT_ABI = [
  "function hasMinted(address) view returns (bool)",
  "function mint(address to) returns (uint256)",
];

const UNISWAP_V3_POOL_ABI = [
  "event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)",
  "function token0() view returns (address)",
  "function token1() view returns (address)",
];

/* ------------------------- cohort ------------------------- */

function cohortBucket(address, salt) {
  const input = `${address.toLowerCase()}:${salt}`;
  const h = ethers.keccak256(ethers.toUtf8Bytes(input));
  return parseInt(h.slice(2, 10), 16) % 100;
}

function isInEligibleCohort(address, enabled, pct, salt) {
  if (!enabled) return true;
  if (!salt) throw new Error("COHORT_SALT required when COHORT_ENABLED=true");
  if (pct <= 0) return false;
  if (pct >= 100) return true;
  return cohortBucket(address, salt) < pct;
}

/* ------------------------- main ------------------------- */

async function main() {
  console.log(`Reward controller starting (network=${network})`);

  const privateKey = process.env.PRIVATE_KEY;
  if (!privateKey) throw new Error("Missing PRIVATE_KEY");

  const legacyMintEnabled = envBool("LEGACY_BALANCE_MINT_ENABLED", false);
  const swapMintEnabled = envBool("SWAP_MINT_ENABLED", true);
  const indexTransfers = envBool("CONTROLLER_INDEX_TRANSFERS", false);

  const decimals = parseInt(process.env.TOKEN_DECIMALS || "18", 10);
  const threshold = ethers.parseUnits(process.env.THRESHOLD_TOKENS || "0", decimals);

  const confirmations = envInt("CONFIRMATIONS", 2);
  const chunkSize = envInt("CHUNK_SIZE", 1000);
  const minBackfillIntervalMs = envInt("MIN_BACKFILL_INTERVAL_MS", 2000);

  // New operational knobs
  const mintFailureCooldownMs = envInt("MINT_FAILURE_COOLDOWN_MS", 60_000);
  const throwOnMintFailure = envBool("THROW_ON_MINT_FAILURE", false);

  const cohortEnabled = envBool("COHORT_ENABLED", true);
  const cohortEligiblePercent = envInt("COHORT_ELIGIBLE_PERCENT", 50);
  const cohortSalt = (process.env.COHORT_SALT || "").trim();

  const sqlitePath = (process.env.CONTROLLER_SQLITE_PATH || "").trim();
  const sqliteDb = openSqlite(sqlitePath);
  if (sqlitePath && !sqliteDb) {
    console.warn("CONTROLLER_SQLITE_PATH set, but sqlite3 module not available.");
  }

  const pk = privateKey.startsWith("0x") ? privateKey : `0x${privateKey}`;

  const wsProvider = new ethers.WebSocketProvider(WSS_URL);
  const httpProvider = new ethers.JsonRpcProvider(RPC_URL);
  const signer = new ethers.Wallet(pk, httpProvider);

  const token = new ethers.Contract(TOKEN_ADDRESS, ERC20_ABI, httpProvider);
  const nft = new ethers.Contract(JSTVIP_ADDRESS, NFT_ABI, signer);
  const pool = new ethers.Contract(POOL_ADDRESS, UNISWAP_V3_POOL_ABI, httpProvider);

  // --- pool sanity check ---
  const onchainToken0 = (await pool.token0()).toLowerCase();
  const onchainToken1 = (await pool.token1()).toLowerCase();

  if (
    onchainToken0 !== POOL_TOKEN0.toLowerCase() ||
    onchainToken1 !== POOL_TOKEN1.toLowerCase()
  ) {
    throw new Error(
      `Pool token mismatch.
       env=(${POOL_TOKEN0}, ${POOL_TOKEN1})
       chain=(${onchainToken0}, ${onchainToken1})`
    );
  }

  // Confirm the token this controller tracks matches pool token0 or token1.
  const trackedToken = TOKEN_ADDRESS.toLowerCase();
  const tokenIs0 = trackedToken === onchainToken0;
  const tokenIs1 = trackedToken === onchainToken1;
  if (!tokenIs0 && !tokenIs1) {
    throw new Error(
      `TOKEN_ADDRESS is not in the pool.
       TOKEN_ADDRESS=${TOKEN_ADDRESS}
       pool.token0=${onchainToken0}
       pool.token1=${onchainToken1}`
    );
  }

  console.log("Signer:", signer.address);
  console.log("Token:", TOKEN_ADDRESS);
  console.log("NFT:", JSTVIP_ADDRESS);
  console.log("Pool:", POOL_ADDRESS);
  console.log("Pool token0:", onchainToken0);
  console.log("Pool token1:", onchainToken1);
  console.log("State file:", STATE_FILE);
  console.log("Legacy balance mint enabled:", legacyMintEnabled);
  console.log("Swap mint enabled:", swapMintEnabled);
  console.log("Threshold (raw):", threshold.toString(), `(decimals=${decimals})`);
  console.log("Mint failure cooldown (ms):", mintFailureCooldownMs);
  console.log("Throw on mint failure:", throwOnMintFailure);
  console.log("Index transfers:", indexTransfers);
  if (sqliteDb) {
    console.log("SQLite indexer enabled:", sqlitePath);
  }

  if (cohortEnabled && !cohortSalt) {
    throw new Error("COHORT_ENABLED=true but COHORT_SALT missing");
  }

  let state = loadState(STATE_FILE);

  // Initialize cursors on first run
  const latest = await httpProvider.getBlockNumber();
  const initFrom = Math.max(0, latest - 2000);

  if (!state.lastProcessedTransferBlock || state.lastProcessedTransferBlock <= 0) {
    state.lastProcessedTransferBlock = initFrom;
  }
  if (!state.lastProcessedSwapBlock || state.lastProcessedSwapBlock <= 0) {
    state.lastProcessedSwapBlock = initFrom;
  }
  saveState(STATE_FILE, state);

  /* ---------------- legacy eligibility (optional) ---------------- */

  async function handleEligibleLegacy(to, blockNumber) {
    if (!legacyMintEnabled) return;

    if (!ethers.isAddress(to) || to === ethers.ZeroAddress) return;
    if (!isInEligibleCohort(to, cohortEnabled, cohortEligiblePercent, cohortSalt)) return;

    const toL = toLowerAddr(to);
    if (state.mintedCache[toL]) return;

    const already = await nft.hasMinted(to);
    if (already) {
      state.mintedCache[toL] = true;
      saveState(STATE_FILE, state);
      return;
    }

    const bal = await token.balanceOf(to);
    if (bal < threshold) return;

    console.log(`[LEGACY ELIGIBLE] ${to} block=${blockNumber}`);

    // Legacy path is optional; keep behavior simple (throwing is acceptable here).
    const tx = await nft.mint(to);
    await tx.wait();

    state.mintedCache[toL] = true;
    saveState(STATE_FILE, state);
    console.log(`[LEGACY MINTED] ${to}`);
  }

  async function queryTransferLogs(fromBlock, toBlock) {
    const filter = token.filters.Transfer(null, null);
    let start = fromBlock;
    let end = toBlock;

    while (true) {
      try {
        return await token.queryFilter(filter, start, end);
      } catch (e) {
        if (start === end) throw e;
        end = start + Math.floor((end - start) / 2);
        await sleep(300);
      }
    }
  }

  async function backfillTransfers() {
    if (!legacyMintEnabled && !indexTransfers) return;

    const current = await httpProvider.getBlockNumber();
    const target = current - confirmations;
    if (target <= state.lastProcessedTransferBlock) return;

    let from = state.lastProcessedTransferBlock + 1;
    while (from <= target) {
      const to = Math.min(from + chunkSize - 1, target);
      const logs = await queryTransferLogs(from, to);

      for (const log of logs) {
        if (legacyMintEnabled) {
          await handleEligibleLegacy(log.args.to, log.blockNumber);
        }
        await dbRun(
          sqliteDb,
          `INSERT OR IGNORE INTO controller_transfers
           (block_number, tx_hash, log_index, from_address, to_address, value_raw)
           VALUES (?,?,?,?,?,?)`,
          [
            log.blockNumber,
            log.transactionHash,
            log.logIndex,
            toLowerAddr(log.args.from),
            toLowerAddr(log.args.to),
            log.args.value?.toString?.() || String(log.args.value),
          ]
        );
      }

      state.lastProcessedTransferBlock = to;
      saveState(STATE_FILE, state);
      from = to + 1;
    }
  }

  /* ---------------- swap ingestion + swap-driven minting ---------------- */

  async function querySwapLogs(fromBlock, toBlock) {
    const filter = pool.filters.Swap(null, null);
    let start = fromBlock;
    let end = toBlock;

    while (true) {
      try {
        return await pool.queryFilter(filter, start, end);
      } catch (e) {
        if (start === end) throw e;
        end = start + Math.floor((end - start) / 2);
        await sleep(300);
      }
    }
  }

  function fmtI256(x) {
    try {
      return x.toString();
    } catch {
      return String(x);
    }
  }

  function getTokenBoughtFromSwap(amount0, amount1) {
    // Determine "token bought" for the tracked token.
    // If tracked token is token0:
    //   buy => pool sends token0 to recipient => amount0 < 0 => tokenBought = -amount0
    // If tracked token is token1:
    //   buy => pool sends token1 to recipient => amount1 < 0 => tokenBought = -amount1
    if (tokenIs0) {
      const a0 = parseBigintSafe(amount0);
      return a0 < 0n ? -a0 : 0n;
    }
    const a1 = parseBigintSafe(amount1);
    return a1 < 0n ? -a1 : 0n;
  }

  async function maybeMintFromCumulativeBuys(buyer, blockNumber) {
    if (!swapMintEnabled) return;

    // Basic hygiene checks
    if (!ethers.isAddress(buyer) || buyer === ethers.ZeroAddress) return;
    if (!isInEligibleCohort(buyer, cohortEnabled, cohortEligiblePercent, cohortSalt)) return;

    const buyerL = toLowerAddr(buyer);

    // Cooldown: if we recently failed to mint for this buyer, do not spam retries.
    const nowMs = Date.now();
    const lastFailMs = state.mintFailures[buyerL] ? parseInt(state.mintFailures[buyerL], 10) : 0;
    if (lastFailMs && nowMs - lastFailMs < mintFailureCooldownMs) {
      return;
    }

    // If cache says minted, verify it against chain.
    // Cache is only a performance hint, never a source of truth.
    if (state.mintedCache[buyerL]) {
      const already = await nft.hasMinted(buyer);
      if (already) return;

      // Cache was wrong; clear it and continue.
      delete state.mintedCache[buyerL];
      saveState(STATE_FILE, state);
    }

    // On-chain guard (authoritative)
    const already = await nft.hasMinted(buyer);
    if (already) {
      state.mintedCache[buyerL] = true;
      saveState(STATE_FILE, state);
      return;
    }

    // Threshold check
    const cum = parseBigintSafe(state.cumulativeBuys[buyerL] || "0");
    if (cum < threshold) return;

    console.log(
      `[SWAP ELIGIBLE] ${buyer} block=${blockNumber} cumulativeBuys=${cum.toString()}`
    );

    // Attempt mint; on failure, record timestamp for cooldown.
    try {
      const tx = await nft.mint(buyer);
      console.log(`[MINT SENT] to=${buyer} tx=${tx.hash}`);
      const receipt = await tx.wait();
      console.log(`[MINT CONFIRMED] to=${buyer} tx=${receipt.hash}`);

      if (sqliteDb) {
        let mintedTokenId = null;
        let logIndex = 0;
        for (const log of receipt.logs || []) {
          try {
            const parsed = nft.interface.parseLog(log);
            if (parsed?.name === "Transfer" && parsed?.args?.to?.toLowerCase() === buyer.toLowerCase()) {
              mintedTokenId = parsed.args.tokenId?.toString?.() || String(parsed.args.tokenId);
              logIndex = log.logIndex || 0;
              break;
            }
          } catch {
            continue;
          }
        }
        await dbRun(
          sqliteDb,
          `INSERT OR IGNORE INTO controller_mints
           (block_number, tx_hash, log_index, to_address, token_id)
           VALUES (?,?,?,?,?)`,
          [
            receipt.blockNumber || blockNumber,
            receipt.hash || receipt.transactionHash || tx.hash,
            logIndex,
            toLowerAddr(buyer),
            mintedTokenId || "0",
          ]
        );
      }

      state.mintedCache[buyerL] = true;
      delete state.mintFailures[buyerL]; // clear failures on success
      saveState(STATE_FILE, state);
    } catch (e) {
      // Record failure and optionally keep running quietly.
      state.mintFailures[buyerL] = String(Date.now());
      saveState(STATE_FILE, state);

      console.error(
        `[MINT FAILED] to=${buyer} reason=${e?.shortMessage || e?.message || e}`
      );

      // If you want strict behavior, set THROW_ON_MINT_FAILURE=true.
      if (throwOnMintFailure) throw e;
    }
  }

  async function backfillSwaps() {
    const current = await httpProvider.getBlockNumber();
    const target = current - confirmations;
    if (target <= state.lastProcessedSwapBlock) return;

    let from = state.lastProcessedSwapBlock + 1;
    while (from <= target) {
      const to = Math.min(from + chunkSize - 1, target);
      const logs = await querySwapLogs(from, to);

      for (const log of logs) {
        const recipient = (log.args.recipient || "").toString();
        const amount0 = log.args.amount0;
        const amount1 = log.args.amount1;
        const sender = (log.args.sender || "").toString();
        const sqrtPriceX96 = log.args.sqrtPriceX96;
        const liquidity = log.args.liquidity;
        const tick = log.args.tick;

        // Always print ingestion proof line
        console.log(
          `[SWAP] block=${log.blockNumber} tx=${log.transactionHash} recipient=${recipient} amount0=${fmtI256(
            amount0
          )} amount1=${fmtI256(amount1)}`
        );

        // Swap-driven accounting
        // For Uniswap V3, recipient receives the output token.
        const buyer = recipient;

        // Only count "buys" of the tracked token.
        const tokenBought = getTokenBoughtFromSwap(amount0, amount1);
        if (tokenBought > 0n) {
          const buyerL = toLowerAddr(buyer);
          const prev = parseBigintSafe(state.cumulativeBuys[buyerL] || "0");
          const next = prev + tokenBought;
          state.cumulativeBuys[buyerL] = next.toString();

          // Mint check (still safe due to hasMinted guard).
          await maybeMintFromCumulativeBuys(buyer, log.blockNumber);
        }

        await dbRun(
          sqliteDb,
          `INSERT OR IGNORE INTO controller_swaps
           (block_number, tx_hash, log_index, sender, recipient, amount0, amount1, sqrt_price_x96, liquidity, tick, token_bought_raw)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
          [
            log.blockNumber,
            log.transactionHash,
            log.logIndex,
            toLowerAddr(sender),
            toLowerAddr(recipient),
            fmtI256(amount0),
            fmtI256(amount1),
            sqrtPriceX96?.toString?.() || String(sqrtPriceX96),
            liquidity?.toString?.() || String(liquidity),
            Number(tick),
            tokenBought.toString(),
          ]
        );
      }

      state.lastProcessedSwapBlock = to;
      saveState(STATE_FILE, state);
      from = to + 1;
    }
  }

  /* ---------------- startup backfills ---------------- */

  if (legacyMintEnabled) {
    await backfillTransfers();
  } else {
    if (indexTransfers) {
      console.log("Legacy balance-mint path disabled; indexing transfers only.");
      await backfillTransfers();
    } else {
      console.log("Legacy balance-mint path disabled; skipping ERC-20 Transfer backfill.");
    }
  }

  await backfillSwaps();

  /* ---------------- steady-state loop ---------------- */

  let lastRun = 0;
  wsProvider.on("block", async () => {
    const now = Date.now();
    if (now - lastRun < minBackfillIntervalMs) return;
    lastRun = now;

    try {
      if (legacyMintEnabled || indexTransfers) await backfillTransfers();
      await backfillSwaps();
    } catch (e) {
      // This is now expected to be quieter because mint failures are handled per-wallet.
      console.error("[BACKFILL ERROR]", e?.shortMessage || e?.message || e);
    }
  });

  // If the websocket drops, exit so a supervisor can restart the process.
  wsProvider.websocket.on("close", () => process.exit(1));
  wsProvider.websocket.on("error", () => process.exit(1));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
