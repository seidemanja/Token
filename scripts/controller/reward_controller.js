/**
 * Reward Controller (Sepolia)
 *
 * Watches ERC-20 Transfer events for TOKEN_ADDRESS and mints JSTVIP NFT once per wallet
 * when balance >= THRESHOLD_TOKENS.
 *
 * Uses env vars exactly as in your .env:
 * - SEPOLIA_WSS_URL
 * - PRIVATE_KEY
 * - TOKEN_ADDRESS
 * - JSTVIP_ADDRESS
 * - TOKEN_DECIMALS
 * - THRESHOLD_TOKENS
 * - CONFIRMATIONS
 * - STATE_FILE
 */

require("dotenv").config();
const fs = require("fs");
const { ethers } = require("ethers");

function mustEnv(name) {
  const v = (process.env[name] || "").trim();
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
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

async function main() {
  const wssUrl = mustEnv("SEPOLIA_WSS_URL");
  const privateKey = mustEnv("PRIVATE_KEY");
  const tokenAddress = mustEnv("TOKEN_ADDRESS");
  const nftAddress = mustEnv("JSTVIP_ADDRESS");

  const decimals = parseInt(mustEnv("TOKEN_DECIMALS"), 10);
  const thresholdTokens = mustEnv("THRESHOLD_TOKENS");
  const threshold = ethers.parseUnits(thresholdTokens, decimals);

  const confirmations = parseInt((process.env.CONFIRMATIONS || "2").trim(), 10);
  const stateFile = (process.env.STATE_FILE || ".reward_state_sepolia.json").trim();

  // Ethers v6: add 0x prefix if user stored raw hex
  const pk = privateKey.startsWith("0x") ? privateKey : `0x${privateKey}`;

  const provider = new ethers.WebSocketProvider(wssUrl);
  const signer = new ethers.Wallet(pk, provider);

  const token = new ethers.Contract(tokenAddress, ERC20_ABI, provider);
  const nft = new ethers.Contract(nftAddress, NFT_ABI, signer);

  console.log("Reward controller running");
  console.log("Signer:", signer.address);
  console.log("Token:", tokenAddress);
  console.log("NFT:", nftAddress);
  console.log(`Threshold: ${thresholdTokens} tokens (decimals=${decimals})`);
  console.log(`Confirmations: ${confirmations}`);
  console.log(`State file: ${stateFile}`);

  let state = loadState(stateFile);

  // Initialize start block if first run (last ~2000 blocks)
  const latest = await provider.getBlockNumber();
  if (!state.lastProcessedBlock || state.lastProcessedBlock <= 0) {
    state.lastProcessedBlock = Math.max(0, latest - 2000);
    saveState(stateFile, state);
    console.log("Initialized lastProcessedBlock to:", state.lastProcessedBlock);
  }

  async function handleEligible(to, blockNumber) {
    if (!ethers.isAddress(to)) return;
    if (to === ethers.ZeroAddress) return;

    // On-chain guard: already minted?
    const already = await nft.hasMinted(to);
    if (already) return;

    // Check balance eligibility
    const bal = await token.balanceOf(to);
    if (bal < threshold) return;

    console.log(`[ELIGIBLE] ${to} at block ${blockNumber} balance=${ethers.formatUnits(bal, decimals)}`);

    // Mint (requires signer has MINTER_ROLE)
    const tx = await nft.mint(to);
    console.log(`[MINT SENT] to=${to} tx=${tx.hash}`);
    const receipt = await tx.wait();
    console.log(`[MINT CONFIRMED] to=${to} tx=${receipt.hash}`);
  }

  async function backfill() {
  const current = await provider.getBlockNumber();
  const target = current - confirmations;
  if (target <= state.lastProcessedBlock) return;

  const chunkSize = parseInt((process.env.CHUNK_SIZE || "1000").trim(), 10);
  let fromBlock = state.lastProcessedBlock + 1;

  while (fromBlock <= target) {
    const toBlock = Math.min(fromBlock + chunkSize - 1, target);

    console.log(`[BACKFILL] scanning Transfer logs from ${fromBlock} to ${toBlock}`);

    const filter = token.filters.Transfer(null, null);
    const logs = await token.queryFilter(filter, fromBlock, toBlock);

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

  provider.on("block", async () => {
    try {
      await backfill();
    } catch (e) {
      console.error("[ERROR] backfill failed:", e);
    }
  });

  // Exit cleanly on WS disconnect so you can restart (or use pm2 later)
  provider.websocket.on("close", (code) => {
    console.error(`[WS CLOSED] code=${code}. Exiting.`);
    process.exit(1);
  });
  provider.websocket.on("error", (err) => {
    console.error("[WS ERROR]", err);
  });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
