/**
 * Shared environment resolver
 *
 * Centralizes network-aware environment variable resolution.
 * This file contains NO secrets and SHOULD be committed to git.
 *
 * Usage:
 *   const { TOKEN_ADDRESS, RPC_URL } = require("./env");
 */

require("dotenv").config();

const network = (process.env.NETWORK || "local").toLowerCase();

function mustEnv(name) {
  const v = process.env[name];
  if (!v) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return v;
}

function resolveByNetwork(localName, sepoliaName) {
  if (network === "local") return mustEnv(localName);
  if (network === "sepolia") return mustEnv(sepoliaName);
  throw new Error(`Unsupported NETWORK=${network}`);
}

module.exports = {
  // Selected network
  network,

  // RPC endpoints
  RPC_URL: resolveByNetwork("LOCAL_RPC_URL", "SEPOLIA_RPC_URL"),
  WSS_URL: resolveByNetwork("LOCAL_WSS_URL", "SEPOLIA_WSS_URL"),

  // Core contracts
  TOKEN_ADDRESS: resolveByNetwork(
    "LOCAL_TOKEN_ADDRESS",
    "SEPOLIA_TOKEN_ADDRESS"
  ),

  JSTVIP_ADDRESS: resolveByNetwork(
    "LOCAL_JSTVIP_ADDRESS",
    "SEPOLIA_JSTVIP_ADDRESS"
  ),

  // Uniswap V3 pool
  POOL_ADDRESS: resolveByNetwork(
    "LOCAL_UNISWAP_V3_POOL_ADDRESS",
    "SEPOLIA_UNISWAP_V3_POOL_ADDRESS"
  ),

  POOL_TOKEN0: resolveByNetwork(
    "LOCAL_POOL_TOKEN0_ADDRESS",
    "SEPOLIA_POOL_TOKEN0_ADDRESS"
  ),

  POOL_TOKEN1: resolveByNetwork(
    "LOCAL_POOL_TOKEN1_ADDRESS",
    "SEPOLIA_POOL_TOKEN1_ADDRESS"
  ),

  // Optional per-network state file
  STATE_FILE: resolveByNetwork(
    "LOCAL_STATE_FILE",
    "SEPOLIA_STATE_FILE"
  ),
};
