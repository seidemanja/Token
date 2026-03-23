"""
sim/config.py

Configuration loader for on-chain simulation runs.

The market model is intentionally compact:
- regime transitions across bear/bull/hype
- sentiment and fair-value dynamics
- flow signal and impact
- operational controls for runtime/execution safety (agent count, caps, slippage)

Backward-compatibility:
- New env names are preferred.
- Legacy env names are still accepted as fallbacks.
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


def _env_float_alias(name: str, fallbacks: tuple[str, ...], default: float) -> float:
    """
    Read float env var from preferred `name`; if missing, try fallback names.
    """
    v = os.getenv(name)
    if v not in (None, ""):
        return float(v)
    for fb in fallbacks:
        v_fb = os.getenv(fb)
        if v_fb not in (None, ""):
            return float(v_fb)
    return float(default)


def _env_int_alias(name: str, fallbacks: tuple[str, ...], default: int) -> int:
    """
    Read int env var from preferred `name`; if missing, try fallback names.
    """
    v = os.getenv(name)
    if v not in (None, ""):
        return int(v)
    for fb in fallbacks:
        v_fb = os.getenv(fb)
        if v_fb not in (None, ""):
            return int(v_fb)
    return int(default)


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

    # Simulation runtime controls
    num_agents: int
    max_agents: int
    num_days: int
    agent_start_eth: float
    agent_start_weth: float
    agent_start_token: float
    max_buy_weth: float
    max_sell_token: float
    ticks_per_day: int
    max_trade_pct_buy: float
    max_trade_pct_sell: float
    max_slippage: float
    amm_fee_pct: float
    circulating_supply_start: float
    circulating_supply_daily_unlock: float
    fast_mode: bool

    # Regime model (bear/bull/hype)
    regime_bull_persist: float
    regime_bear_persist: float
    hype_initial_min_days: int
    hype_initial_max_days: int
    hype_persist_start: float
    hype_persist_floor: float
    hype_decay_tau: float
    hype_exit_to_bull_prob: float
    hype_reentry_prob: float
    sentiment_alpha: float
    sentiment_regime_level: float
    sentiment_hype_mult: float
    fair_mu: float
    fair_beta: float
    fair_sigma: float
    fair_reversion: float
    flow_intensity: float
    flow_mispricing_scale: float
    flow_regime_tilt: float
    flow_noise_sigma: float
    impact_kappa: float
    # Simple participant lifecycle (entry/churn)
    entry_lambda_base: float
    churn_prob_base: float


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

        num_agents=_env_int("SIM_NUM_AGENTS", 20),
        max_agents=_env_int("SIM_MAX_AGENTS", 180),
        num_days=_env_int("SIM_NUM_DAYS", 45),
        agent_start_eth=_env_float("SIM_AGENT_START_ETH", 10.0),
        agent_start_weth=_env_float("SIM_AGENT_START_WETH", 2.5),
        agent_start_token=_env_float("SIM_AGENT_START_TOKEN", 0.2),
        max_buy_weth=_env_float("SIM_MAX_BUY_WETH", 0.5),
        max_sell_token=_env_float("SIM_MAX_SELL_TOKEN", 500000.0),
        ticks_per_day=_env_int("SIM_TICKS_PER_DAY", 24),
        max_trade_pct_buy=_env_float("SIM_MAX_TRADE_PCT_BUY", 0.04),
        max_trade_pct_sell=_env_float("SIM_MAX_TRADE_PCT_SELL", 0.04),
        max_slippage=_env_float("SIM_MAX_SLIPPAGE", 0.05),
        amm_fee_pct=_env_float("SIM_AMM_FEE_PCT", 0.003),
        circulating_supply_start=_env_float("SIM_CIRC_SUPPLY_START", 0.0),
        circulating_supply_daily_unlock=_env_float("SIM_CIRC_SUPPLY_DAILY_UNLOCK", 0.0),
        fast_mode=(os.getenv("SIM_FAST_MODE", "false").strip().lower() in {"1", "true", "yes", "y"}),

        # Regime model (bear/bull/hype)
        regime_bull_persist=_env_float_alias("SIM_REGIME_BULL_PERSIST", ("SIM_REGIME_P11",), 0.90),
        regime_bear_persist=_env_float_alias("SIM_REGIME_BEAR_PERSIST", ("SIM_REGIME_P00",), 0.86),
        hype_initial_min_days=_env_int_alias("SIM_HYPE_INITIAL_MIN_DAYS", (), 10),
        hype_initial_max_days=_env_int_alias("SIM_HYPE_INITIAL_MAX_DAYS", (), 15),
        hype_persist_start=_env_float("SIM_HYPE_PERSIST_START", 0.92),
        hype_persist_floor=_env_float("SIM_HYPE_PERSIST_FLOOR", 0.05),
        hype_decay_tau=_env_float("SIM_HYPE_DECAY_TAU", 8.0),
        hype_exit_to_bull_prob=_env_float("SIM_HYPE_EXIT_TO_BULL_PROB", 0.70),
        hype_reentry_prob=_env_float("SIM_HYPE_REENTRY_PROB", 0.01),
        sentiment_alpha=_env_float("SIM_SENTIMENT_ALPHA", 0.20),
        sentiment_regime_level=_env_float_alias(
            "SIM_SENTIMENT_REGIME_LEVEL",
            ("SIM_SENTIMENT_MAGNITUDE",),
            0.7,
        ),
        sentiment_hype_mult=_env_float("SIM_SENTIMENT_HYPE_MULT", 1.8),
        fair_mu=_env_float_alias("SIM_FAIR_MU", ("SIM_FAIR_VALUE_MU",), 0.0),
        fair_beta=_env_float_alias("SIM_FAIR_BETA", ("SIM_FAIR_VALUE_BETA",), 0.02),
        fair_sigma=_env_float_alias("SIM_FAIR_SIGMA", ("SIM_FAIR_VALUE_SIGMA",), 0.01),
        fair_reversion=_env_float_alias("SIM_FAIR_REVERSION", ("SIM_FAIR_VALUE_KAPPA",), 0.05),
        flow_intensity=_env_float_alias("SIM_FLOW_LAMBDA", ("SIM_FLOW_INTENSITY",), 1.8),
        flow_mispricing_scale=_env_float_alias("SIM_FLOW_THETA", ("SIM_MISPRICING_THETA",), 0.04),
        flow_regime_tilt=_env_float_alias("SIM_FLOW_RHO", (), 0.20),
        flow_noise_sigma=_env_float_alias("SIM_FLOW_SIGMA", (), 0.15),
        impact_kappa=_env_float_alias("SIM_PRICE_KAPPA", ("SIM_IMPACT_KAPPA",), 1.35),
        entry_lambda_base=_env_float("SIM_ENTRY_LAMBDA_BASE", 0.45),
        churn_prob_base=_env_float("SIM_CHURN_PROB_BASE", 0.0045),
    )
