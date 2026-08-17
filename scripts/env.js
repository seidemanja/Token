/**
 * Shared environment resolver
 *
 * Centralizes network-aware environment variable resolution.
 * This file contains NO secrets and SHOULD be committed to git.
 *
 * Usage:
 *   const { TOKEN_ADDRESS, RPC_URL, requireValue } = require("./env");
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

function optionalEnv(name) {
  return process.env[name] || "";
}

function resolveByNetwork(localName, sepoliaName, required = false) {
  if (network === "local") return required ? mustEnv(localName) : optionalEnv(localName);
  if (network === "sepolia") return required ? mustEnv(sepoliaName) : optionalEnv(sepoliaName);
  throw new Error(`Unsupported NETWORK=${network}`);
}

function requireValue(value, label) {
  if (!value) {
    throw new Error(`Missing required resolved env value: ${label}`);
  }
  return value;
}

module.exports = {
  // Selected network
  network,

  // Helpers for scripts that need to fail fast on specific values.
  mustEnv,
  requireValue,

  // RPC endpoints. Keep these required because every script needs a target network.
  RPC_URL: resolveByNetwork("LOCAL_RPC_URL", "SEPOLIA_RPC_URL", true),
  WSS_URL: resolveByNetwork("LOCAL_WSS_URL", "SEPOLIA_WSS_URL", true),

  // Core contracts. Optional at import time so fresh deployment scripts can run.
  TOKEN_ADDRESS: resolveByNetwork(
    "LOCAL_TOKEN_ADDRESS",
    "SEPOLIA_TOKEN_ADDRESS"
  ),

  JSTVIP_ADDRESS: resolveByNetwork(
    "LOCAL_JSTVIP_ADDRESS",
    "SEPOLIA_JSTVIP_ADDRESS"
  ),

  // Uniswap V3 pool. Optional at import time; AMM/controller scripts validate before use.
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

  // Optional per-network state file.
  STATE_FILE: resolveByNetwork(
    "LOCAL_STATE_FILE",
    "SEPOLIA_STATE_FILE"
  ),
};
