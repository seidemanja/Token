/**
 * Reward Controller (Sepolia)
 *
 * Watches ERC-20 Transfer events for TOKEN_ADDRESS and mints JSTVIP NFT once per wallet
 * when balance >= THRESHOLD_TOKENS.
 *
 * Adds off-chain cohort gating:
 * - 50% of addresses are "eligible cohort"
 * - 50% are "control cohort" and will never be minted, even if they meet threshold
 *
 * Env vars (existing):
 * - SEPOLIA_WSS_URL
 * - PRIVATE_KEY
 * - TOKEN_ADDRESS
 * - JSTVIP_ADDRESS
 * - TOKEN_DECIMALS
 * - THRESHOLD_TOKENS
 * - CONFIRMATIONS
 * - STATE_FILE
 * - CHUNK_SIZE
 *
 * Env vars (new/required for stability):
 * - SEPOLIA_RPC_URL  (HTTP RPC; used for eth_getLogs, reads, and sending txs)
 *
 * Env vars (cohort gating):
 * - COHORT_ENABLED=true|false (default true)
 * - COHORT_ELIGIBLE_PERCENT=50 (default 50)
 * - COHORT_SALT=<random string> (required if COHORT_ENABLED=true)
 */

require("dotenv").config();
const fs = require("fs");
const { ethers } = require("ethers");

function mustEnv(name) {
  const v = (process.env[name] || "").trim();
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function envBool(name, defaultValue) {
  const raw = (process.env[name] || "").trim().toLowerCase();
  if (!raw) return defaultValue;
  return raw === "true" || raw === "1" || raw === "yes" || raw === "y";
}

function envInt(name, defaultValue) {
  const raw = (process.env[name] || "").trim();
  if (!raw) return defaultValue;
  const n = parseInt(raw, 10);
  if (Number.isNaN(n)) return defaultValue;
  return n;
}

function loadState(path) {
  try {
    return JSON.parse(fs.readFileSync(path, "utf8"));
  } catch {
    return { lastProcessedBlock: 0 };
  }
}

function saveState(path, state) {
  fs.writeFileSync(path, JSON.stringify(state, null, 2));
}

const ERC20_ABI = [
  "event Transfer(address indexed from, address indexed to, uint256 value)",
  "function balanceOf(address) view returns (uint256)",
];

const NFT_ABI = [
  "function hasMinted(address) view returns (bool)",
  "function mint(address to) returns (uint256)",
];

/**
 * Deterministic cohort assignment (off-chain).
 * bucket = keccak256(lower(address) + ":" + salt) % 100
 * Eligible if bucket < eligiblePercent.
 */
function cohortBucket(address, salt) {
  const addr = address.toLowerCase();
  const input = `${addr}:${salt}`;
  const h = ethers.keccak256(ethers.toUtf8Bytes(input));
  // Take first 4 bytes => 32-bit integer, then mod 100
  const first4bytes = h.slice(0, 10); // 0x + 8 hex chars
  const n = parseInt(first4bytes, 16);
  return n % 100;
}

function isInEligibleCohort(address, enabled, eligiblePercent, salt) {
  if (!enabled) return true; // cohort gating disabled => everyone eligible
  if (!salt) throw new Error("COHORT_SALT is required when COHORT_ENABLED=true");
  if (eligiblePercent <= 0) return false;
  if (eligiblePercent >= 100) return true;
  const b = cohortBucket(address, salt);
  return b < eligiblePercent;
}

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const wssUrl = mustEnv("SEPOLIA_WSS_URL");
  const rpcUrl = mustEnv("SEPOLIA_RPC_URL");

  let lastBackfillRunAt = 0;
  const minBackfillIntervalMs = envInt("MIN_BACKFILL_INTERVAL_MS", 2000);

  const privateKey = mustEnv("PRIVATE_KEY");
  const tokenAddress = mustEnv("TOKEN_ADDRESS");
  const nftAddress = mustEnv("JSTVIP_ADDRESS");

  const decimals = parseInt(mustEnv("TOKEN_DECIMALS"), 10);
  const thresholdTokens = mustEnv("THRESHOLD_TOKENS");
  const threshold = ethers.parseUnits(thresholdTokens, decimals);

  const confirmations = envInt("CONFIRMATIONS", 2);
  const stateFile = (process.env.STATE_FILE || ".reward_state_sepolia.json").trim();
  const chunkSize = envInt("CHUNK_SIZE", 1000);

  // Cohort settings
  const cohortEnabled = envBool("COHORT_ENABLED", true);
  const cohortEligiblePercent = envInt("COHORT_ELIGIBLE_PERCENT", 50);
  const cohortSalt = (process.env.COHORT_SALT || "").trim();

  // Ethers v6: add 0x prefix if user stored raw hex
  const pk = privateKey.startsWith("0x") ? privateKey : `0x${privateKey}`;

  // WS for new blocks; HTTP for logs/reads/txs (much more stable than WS for eth_getLogs)
  const wsProvider = new ethers.WebSocketProvider(wssUrl);
  const httpProvider = new ethers.JsonRpcProvider(rpcUrl);

  // Send transactions via HTTP provider
  const signer = new ethers.Wallet(pk, httpProvider);

  // Token reads and log queries via HTTP provider
  const token = new ethers.Contract(tokenAddress, ERC20_ABI, httpProvider);
  const nft = new ethers.Contract(nftAddress, NFT_ABI, signer);

  console.log("Reward controller running");
  console.log("Signer:", signer.address);
  console.log("Token:", tokenAddress);
  console.log("NFT:", nftAddress);
  console.log(`Threshold: ${thresholdTokens} tokens (decimals=${decimals})`);
  console.log(`Confirmations: ${confirmations}`);
  console.log(`State file: ${stateFile}`);
  console.log(`Chunk size: ${chunkSize}`);
  console.log(`RPC: ${rpcUrl}`);
  console.log(`WSS: ${wssUrl}`);
  console.log(`Cohort gating: enabled=${cohortEnabled} eligiblePercent=${cohortEligiblePercent}`);

  if (cohortEnabled && !cohortSalt) {
    throw new Error("COHORT_ENABLED=true but COHORT_SALT is empty. Set COHORT_SALT in env.");
  }

  let state = loadState(stateFile);

  // Initialize start block if first run (last ~2000 blocks)
  const latest = await httpProvider.getBlockNumber();
  if (!state.lastProcessedBlock || state.lastProcessedBlock <= 0) {
    state.lastProcessedBlock = Math.max(0, latest - 2000);
    saveState(stateFile, state);
    console.log("Initialized lastProcessedBlock to:", state.lastProcessedBlock);
  }

  async function handleEligible(to, blockNumber) {
    if (!ethers.isAddress(to)) return;
    if (to === ethers.ZeroAddress) return;

    // Off-chain cohort gate
    const cohortOk = isInEligibleCohort(
      to,
      cohortEnabled,
      cohortEligiblePercent,
      cohortSalt
    );
    if (!cohortOk) return;

    // On-chain guard: already minted?
    const already = await nft.hasMinted(to);
    if (already) return;

    // Check balance eligibility
    const bal = await token.balanceOf(to);
    if (bal < threshold) return;

    const bucket = cohortEnabled ? cohortBucket(to, cohortSalt) : -1;

    console.log(
      `[ELIGIBLE] ${to} at block ${blockNumber} balance=${ethers.formatUnits(
        bal,
        decimals
      )} bucket=${bucket}`
    );

    // Mint (requires signer has MINTER_ROLE)
    const tx = await nft.mint(to);
    console.log(`[MINT SENT] to=${to} tx=${tx.hash}`);
    const receipt = await tx.wait();
    console.log(`[MINT CONFIRMED] to=${to} tx=${receipt.hash}`);
  }

  /**
   * Resilient log query:
   * - tries requested range
   * - on provider error, shrinks range and retries
   * This mitigates Infura/JSON-RPC intermittent "internal error" on eth_getLogs.
   */
  async function queryTransferLogs(fromBlock, toBlock) {
    const filter = token.filters.Transfer(null, null);

    let start = fromBlock;
    let end = toBlock;

    while (true) {
      try {
        return await token.queryFilter(filter, start, end);
      } catch (e) {
        const msg = e?.shortMessage || e?.message || String(e);
        const span = end - start + 1;

        // Detect Infura rate-limit
        const isRateLimited =
          msg.toLowerCase().includes("too many requests") ||
          e?.code === "BAD_DATA" && Array.isArray(e?.value) && e.value.some(v => v?.code === -32005);

        console.error(`[getLogs ERROR] ${start}->${end} (${span} blocks): ${msg}`);

        if (isRateLimited) {
          // Back off and retry same range (do not shrink immediately)
          const waitMs = envInt("RATE_LIMIT_BACKOFF_MS", 1500);
          console.log(`[getLogs BACKOFF] rate-limited; sleeping ${waitMs}ms then retrying ${start}->${end}`);
          await sleep(waitMs);
          continue;
        }

        if (start === end) {
          // For single-block non-rate-limit errors, brief delay then retry once more
          await sleep(500);
          // retry once; if it fails again, rethrow
          try {
            return await token.queryFilter(filter, start, end);
          } catch (e2) {
            throw e2;
          }
        }

        // shrink to half and retry
        const mid = start + Math.floor((end - start) / 2);
        console.log(`[getLogs RETRY] shrinking range to ${start}->${mid}`);
        end = mid;

        await sleep(350);
      }

    }
  }

  async function backfill() {
    const current = await httpProvider.getBlockNumber();
    const target = current - confirmations;
    if (target <= state.lastProcessedBlock) return;

    let fromBlock = state.lastProcessedBlock + 1;

    while (fromBlock <= target) {
      const toBlock = Math.min(fromBlock + chunkSize - 1, target);

      console.log(`[BACKFILL] scanning Transfer logs from ${fromBlock} to ${toBlock}`);

      const logs = await queryTransferLogs(fromBlock, toBlock);

      for (const log of logs) {
        const to = log.args.to;
        await handleEligible(to, log.blockNumber);
      }

      // Persist progress after every chunk
      state.lastProcessedBlock = toBlock;
      saveState(stateFile, state);

      fromBlock = toBlock + 1;
    }

    console.log(`[BACKFILL DONE] lastProcessedBlock=${state.lastProcessedBlock}`);
  }

  // Initial backfill, then keep catching up each block
  await backfill();

  wsProvider.on("block", async () => {
    const now = Date.now();
    if (now - lastBackfillRunAt < minBackfillIntervalMs) return;
    lastBackfillRunAt = now;

  try {
    await backfill();
  } catch (e) {
    console.error("[ERROR] backfill failed:", e);
  }
});

  // Exit cleanly on WS disconnect so you can restart (or run under pm2 later)
  wsProvider.websocket.on("close", (code) => {
    console.error(`[WS CLOSED] code=${code}. Exiting.`);
    process.exit(1);
  });
  wsProvider.websocket.on("error", (err) => {
    console.error("[WS ERROR]", err);
  });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
