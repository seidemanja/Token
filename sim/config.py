"""
sim/config.py

Loads configuration from your existing .env structure.
This mirrors the intent of scripts/env.js:
- NETWORK selects local vs sepolia
- network-prefixed variables supply addresses/endpoints

For now, we will only *run* on NETWORK=local, but this config supports both.

NEW (Step 27):
- Adds hard safety caps on trade inputs:
    SIM_MAX_BUY_WETH
    SIM_MAX_SELL_TOKEN
These caps are enforced in run_sim.py (next step) to prevent pathological
price jumps in thin Uniswap V3 liquidity.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Always load .env from repo root reliably (no find_dotenv() stack-frame issues)
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)




# sim/config.py
import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Always load .env from repo root reliably (no find_dotenv() stack-frame issues)
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)



def _must(name: str) -> str:
    """Fetch a required environment variable."""
    v = os.getenv(name)
    if not v:
        raise ValueError(f"Missing required env var: {name}")
    return v


def _opt(name: str) -> str | None:
    """Fetch an optional environment variable."""
    v = os.getenv(name)
    return v if v else None


def _by_network(local_key: str, sepolia_key: str) -> str:
    """Resolve a value based on NETWORK=local|sepolia."""
    network = (os.getenv("NETWORK") or "local").lower().strip()
    if network == "local":
        return _must(local_key)
    if network == "sepolia":
        return _must(sepolia_key)
    raise ValueError(f"Unsupported NETWORK={network}")


def _by_network_opt(local_key: str, sepolia_key: str) -> str | None:
    """Resolve an optional value based on NETWORK=local|sepolia."""
    network = (os.getenv("NETWORK") or "local").lower().strip()
    if network == "local":
        return _opt(local_key)
    if network == "sepolia":
        return _opt(sepolia_key)
    raise ValueError(f"Unsupported NETWORK={network}")


def _as_addr(x: str) -> str:
    """Basic validation for Ethereum address strings."""
    if not x.startswith("0x") or len(x) < 10:
        raise ValueError(f"Not an address: {x}")
    return x


def _env_int(name: str, default: int) -> int:
    """Read an int env var with a default."""
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    """Read a float env var with a default."""
    return float(os.getenv(name, str(default)))


@dataclass(frozen=True)
class SimConfig:
    # Network + RPC
    network: str
    rpc_url: str

    # Addresses
    token: str
    pool: str
    pool_token0: str
    pool_token1: str
    weth: str  # derived from pool token0/token1 unless overridden
    jstvip: str | None

    # Simulation parameters
    num_agents: int
    num_days: int

    agent_start_eth: float
    agent_start_weth: float
    agent_start_token: float

    # Phase-0 random policy parameters
    trade_prob: float
    buy_prob: float
    sell_prob: float

    buy_weth_min: float
    buy_weth_max: float
    sell_token_min: float
    sell_token_max: float

    # NEW: Hard safety caps (Step 27)
    # These are *absolute* caps applied right before trade execution.
    max_buy_weth: float
    max_sell_token: float


def load_config() -> SimConfig:
    """
    Load env config and derive WETH as "the other token in the pool"
    if no explicit LOCAL_WETH_ADDRESS/SEPOLIA_WETH_ADDRESS is provided.
    """
    network = (os.getenv("NETWORK") or "local").lower().strip()

    rpc_url = _by_network("LOCAL_RPC_URL", "SEPOLIA_RPC_URL")

    token = _as_addr(_by_network("LOCAL_TOKEN_ADDRESS", "SEPOLIA_TOKEN_ADDRESS"))
    pool = _as_addr(_by_network("LOCAL_UNISWAP_V3_POOL_ADDRESS", "SEPOLIA_UNISWAP_V3_POOL_ADDRESS"))

    pool_token0 = _as_addr(_by_network("LOCAL_POOL_TOKEN0_ADDRESS", "SEPOLIA_POOL_TOKEN0_ADDRESS"))
    pool_token1 = _as_addr(_by_network("LOCAL_POOL_TOKEN1_ADDRESS", "SEPOLIA_POOL_TOKEN1_ADDRESS"))

    # Optional explicit WETH address override.
    weth_override = _by_network_opt("LOCAL_WETH_ADDRESS", "SEPOLIA_WETH_ADDRESS")

    if weth_override:
        weth = _as_addr(weth_override)
    else:
        # Derive WETH as the pool token that is not TOKEN_ADDRESS.
        tok = token.lower()
        t0 = pool_token0.lower()
        t1 = pool_token1.lower()

        if tok == t0:
            weth = pool_token1
        elif tok == t1:
            weth = pool_token0
        else:
            # This indicates your env is pointing at a pool that does not include your TOKEN_ADDRESS.
            raise ValueError(
                "TOKEN_ADDRESS is neither POOL_TOKEN0 nor POOL_TOKEN1. "
                "Fix env values or point to the correct pool."
            )

    jstvip = _by_network_opt("LOCAL_JSTVIP_ADDRESS", "SEPOLIA_JSTVIP_ADDRESS")
    if jstvip:
        jstvip = _as_addr(jstvip)

    return SimConfig(
        network=network,
        rpc_url=rpc_url,

        token=token,
        pool=pool,
        pool_token0=pool_token0,
        pool_token1=pool_token1,
        weth=weth,
        jstvip=jstvip,

        num_agents=_env_int("SIM_NUM_AGENTS", 10),
        num_days=_env_int("SIM_NUM_DAYS", 30),

        agent_start_eth=_env_float("SIM_AGENT_START_ETH", 50.0),
        agent_start_weth=_env_float("SIM_AGENT_START_WETH", 10.0),
        agent_start_token=_env_float("SIM_AGENT_START_TOKEN", 1000.0),

        trade_prob=_env_float("SIM_TRADE_PROB", 0.25),
        buy_prob=_env_float("SIM_BUY_PROB", 0.50),
        sell_prob=_env_float("SIM_SELL_PROB", 0.50),

        buy_weth_min=_env_float("SIM_BUY_WETH_MIN", 0.01),
        buy_weth_max=_env_float("SIM_BUY_WETH_MAX", 0.10),
        sell_token_min=_env_float("SIM_SELL_TOKEN_MIN", 1.0),
        sell_token_max=_env_float("SIM_SELL_TOKEN_MAX", 50.0),

        # NEW: hard caps (defaults are conservative for local thin liquidity)
        max_buy_weth=_env_float("SIM_MAX_BUY_WETH", 0.01),
        max_sell_token=_env_float("SIM_MAX_SELL_TOKEN", 50.0),
    )
