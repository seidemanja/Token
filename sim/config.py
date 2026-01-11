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
from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Optional
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


def _opt(name: str) -> "Optional[str]":
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


def _by_network_opt(local_key: str, sepolia_key: str) -> "Optional[str]":
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
    jstvip: "Optional[str]"

    # Simulation parameters
    num_agents: int
    num_days: int

    agent_start_eth: float
    agent_start_weth: float
    agent_start_token: float

    # NEW: Hard safety caps
    # These are *absolute* caps applied right before trade execution.
    max_buy_weth: float
    max_sell_token: float

    # Regime + sentiment
    regime_p11: float
    regime_p00: float
    sentiment_alpha: float
    sentiment_mu_bear: float
    sentiment_mu_bull: float

    # Fair value (log) process
    fair_value_start: float
    fair_value_mu: float
    fair_value_beta: float
    fair_value_sigma: float
    fair_value_floor: float

    ticks_per_day: int
    trades_per_tick_lambda: float

    # Perceived value + launch premium
    perceived_bias_sigma: float
    perceived_idio_rho: float
    perceived_idio_sigma: float
    launch_premium_l0: float
    launch_premium_tau: float

    # Mispricing + trade size
    mispricing_theta: float
    trade_q0: float
    trade_qmax: float
    size_logn_mean: float
    size_logn_sigma: float

    # Activity model
    activity_base: float
    activity_sentiment_scale: float
    activity_launch_scale: float

    # Holder entry/churn model
    entry_lambda0: float
    entry_k_launch: float
    entry_k_sentiment: float
    entry_k_return: float
    entry_return_mult_min: float
    entry_return_mult_max: float
    churn_pi0: float
    churn_c_sentiment: float
    churn_c_return: float

    entry_agent_eth: float
    entry_agent_weth: float
    entry_agent_token: float

    # Circulating supply + liquidity policy
    circulating_supply_start: float
    circulating_supply_daily_unlock: float
    liquidity_policy: str

    # Liquidity-scaled caps
    max_trade_pct_buy: float
    max_trade_pct_sell: float

    # Guardrails
    max_slippage: float
    amm_fee_pct: float


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

        # Hard caps (defaults are conservative for local thin liquidity)
        max_buy_weth=_env_float("SIM_MAX_BUY_WETH", 0.01),
        max_sell_token=_env_float("SIM_MAX_SELL_TOKEN", 50.0),

        # Regime + sentiment
        regime_p11=_env_float("SIM_REGIME_P11", 0.90),
        regime_p00=_env_float("SIM_REGIME_P00", 0.90),
        sentiment_alpha=_env_float("SIM_SENTIMENT_ALPHA", 0.10),
        sentiment_mu_bear=_env_float("SIM_SENTIMENT_MU_BEAR", -1.0),
        sentiment_mu_bull=_env_float("SIM_SENTIMENT_MU_BULL", 1.0),

        # Fair value (log) process
        fair_value_start=_env_float("SIM_FAIR_VALUE_START", 1.0),
        fair_value_mu=_env_float("SIM_FAIR_VALUE_MU", 0.0),
        fair_value_beta=_env_float("SIM_FAIR_VALUE_BETA", 0.10),
        fair_value_sigma=_env_float("SIM_FAIR_VALUE_SIGMA", 0.01),
        fair_value_floor=_env_float("SIM_FAIR_VALUE_FLOOR", 0.01),

        ticks_per_day=_env_int("SIM_TICKS_PER_DAY", 24),
        trades_per_tick_lambda=_env_float("SIM_TRADES_PER_TICK_LAMBDA", 0.5),

        # Perceived value + launch premium
        perceived_bias_sigma=_env_float("SIM_PERCEIVED_BIAS_SIGMA", 0.05),
        perceived_idio_rho=_env_float("SIM_PERCEIVED_IDIO_RHO", 0.95),
        perceived_idio_sigma=_env_float("SIM_PERCEIVED_IDIO_SIGMA", 0.01),
        launch_premium_l0=_env_float("SIM_LAUNCH_PREMIUM_L0", 0.10),
        launch_premium_tau=_env_float("SIM_LAUNCH_PREMIUM_TAU", 10.0),

        # Mispricing + trade size
        mispricing_theta=_env_float("SIM_MISPRICING_THETA", 0.02),
        trade_q0=_env_float("SIM_TRADE_Q0", 1.0),
        trade_qmax=_env_float("SIM_TRADE_QMAX", 10.0),
        size_logn_mean=_env_float("SIM_SIZE_LOGN_MEAN", 0.0),
        size_logn_sigma=_env_float("SIM_SIZE_LOGN_SIGMA", 0.5),

        # Activity model
        activity_base=_env_float("SIM_ACTIVITY_BASE", 0.25),
        activity_sentiment_scale=_env_float("SIM_ACTIVITY_SENTIMENT_SCALE", 0.25),
        activity_launch_scale=_env_float("SIM_ACTIVITY_LAUNCH_SCALE", 0.25),

        # Holder entry/churn model
        entry_lambda0=_env_float("SIM_ENTRY_LAMBDA0", 1.0),
        entry_k_launch=_env_float("SIM_ENTRY_K_L", 0.5),
        entry_k_sentiment=_env_float("SIM_ENTRY_K_S", 0.5),
        entry_k_return=_env_float("SIM_ENTRY_K_R", 0.5),
        entry_return_mult_min=_env_float("SIM_ENTRY_RETURN_MULT_MIN", 0.3),
        entry_return_mult_max=_env_float("SIM_ENTRY_RETURN_MULT_MAX", 3.0),
        churn_pi0=_env_float("SIM_CHURN_PI0", 0.01),
        churn_c_sentiment=_env_float("SIM_CHURN_C_S", 0.5),
        churn_c_return=_env_float("SIM_CHURN_C_R", 0.5),

        entry_agent_eth=_env_float("SIM_ENTRY_AGENT_ETH", 5.0),
        entry_agent_weth=_env_float("SIM_ENTRY_AGENT_WETH", 1.0),
        entry_agent_token=_env_float("SIM_ENTRY_AGENT_TOKEN", 10.0),

        circulating_supply_start=_env_float("SIM_CIRC_SUPPLY_START", 0.0),
        circulating_supply_daily_unlock=_env_float("SIM_CIRC_SUPPLY_DAILY_UNLOCK", 0.0),
        liquidity_policy=os.getenv("SIM_LIQUIDITY_POLICY", "fixed").strip().lower(),

        # Liquidity-scaled caps
        max_trade_pct_buy=_env_float("SIM_MAX_TRADE_PCT_BUY", 0.02),
        max_trade_pct_sell=_env_float("SIM_MAX_TRADE_PCT_SELL", 0.02),

        # Guardrails
        max_slippage=_env_float("SIM_MAX_SLIPPAGE", 0.05),
        amm_fee_pct=_env_float("SIM_AMM_FEE_PCT", 0.003),
    )
