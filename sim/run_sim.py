"""
sim/run_sim.py

On-chain simulation runner with a 3-regime market model:
- Regime states: bear, bull, hype
- Forced initial hype window (10-15 days by default)
- Probabilistic hype decay and rare hype re-entry
- Regime-conditioned sentiment and fair value dynamics
- Net-flow signal from fair-value mispricing
"""

import argparse
import json
import os
import random
import sqlite3
import math
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
from typing import Optional

from eth_account import Account

from sim.config import load_config
from sim.chain import Chain, Agent
from sim.db import SimDB
from web3.exceptions import TransactionNotFound


def utc_now_iso() -> str:
    """UTC timestamp for logging."""
    return datetime.now(timezone.utc).isoformat()


def clamp(x: float, lo: float, hi: float) -> float:
    """Clamp a float to [lo, hi]."""
    return max(lo, min(hi, x))


def _maybe_start_reward_controller(run_dir: Path) -> dict:
    """
    Start the reward controller if enabled and not already running.
    Returns a dict with status metadata for the manifest.
    """
    enabled = os.getenv("SIM_START_REWARD_CONTROLLER", "true").strip().lower() in {"1", "true", "yes", "y"}
    status = {
        "enabled": bool(enabled),
        "started": False,
        "pid": None,
        "note": "",
    }
    status_path = run_dir / "reward_controller.status.json"
    if not enabled:
        status["note"] = "disabled via SIM_START_REWARD_CONTROLLER"
        status_path.write_text(json.dumps(status, indent=2) + "\n")
        return status

    # Stop any existing controller to keep per-run logs isolated.
    try:
        pids = subprocess.check_output(["pgrep", "-f", "reward_controller_amm_swaps.js"]).decode().strip().split()
    except Exception:
        pids = []
    if pids:
        for pid in pids:
            try:
                subprocess.check_call(["kill", pid])
            except Exception:
                continue

    log_path = run_dir / "reward_controller.log"
    try:
        proc = subprocess.Popen(
            ["node", "scripts/controller/reward_controller_amm_swaps.js"],
            stdout=log_path.open("a"),
            stderr=subprocess.STDOUT,
        )
        (run_dir / "reward_controller.pid").write_text(str(proc.pid) + "\n")
        status["started"] = True
        status["pid"] = proc.pid
        status["note"] = f"log={log_path}"
    except Exception as exc:
        status["note"] = f"failed_to_start: {exc}"

    status_path.write_text(json.dumps(status, indent=2) + "\n")
    return status


def _top_up_admin_balance_if_needed(chain: Chain, target_eth: float = 1_000_000.0) -> None:
    """
    For local dev nodes, ensure the admin has plenty of ETH.
    Attempts hardhat_setBalance (Hardhat) and anvil_setBalance (Anvil).
    """
    try:
        current = chain.w3.eth.get_balance(chain.admin.address)
    except Exception:
        return

    target_wei = chain.w3.to_wei(target_eth, "ether")
    if current >= target_wei:
        return

    target_hex = hex(int(target_wei))
    for method in ("hardhat_setBalance", "anvil_setBalance"):
        try:
            resp = chain.w3.provider.make_request(method, [chain.admin.address, target_hex])
            if resp and resp.get("error") is None:
                return
        except Exception:
            continue


def _configure_fast_mining(chain: Chain) -> None:
    """
    For local dev nodes, enable instant mining to speed up fast mode.
    """
    for method, params in (
        ("hardhat_setAutomine", [True]),
        ("hardhat_setIntervalMining", [0]),
        ("anvil_setAutomine", [True]),
        ("anvil_setIntervalMining", [0]),
    ):
        try:
            chain.w3.provider.make_request(method, params)
        except Exception:
            continue


def _load_agents_from_latest_run() -> "tuple[list[Agent], Optional[str]]":
    latest_path = Path("sim/out/latest.txt")
    if not latest_path.exists():
        return [], None
    run_dir = Path(latest_path.read_text().strip())
    db_path = run_dir / "sim.db"
    if not db_path.exists():
        return [], None

    conn = sqlite3.connect(str(db_path))
    try:
        try:
            rows = conn.execute(
                "SELECT agent_id, address, private_key, executor, agent_type FROM agents ORDER BY agent_id ASC"
            ).fetchall()
        except sqlite3.OperationalError:
            rows = conn.execute(
                "SELECT agent_id, address, private_key, executor FROM agents ORDER BY agent_id ASC"
            ).fetchall()
        run_id_row = conn.execute(
            "SELECT run_id FROM sim_runs ORDER BY created_at_utc DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    agents = []
    for row in rows:
        if len(row) == 5:
            agent_id, address, private_key, executor, agent_type = row
        else:
            agent_id, address, private_key, executor = row
            agent_type = "retail"
        agents.append(
            Agent(
                agent_id=int(agent_id),
                address=str(address),
                private_key=str(private_key),
                executor=(str(executor) if executor else None),
                agent_type=str(agent_type or "retail"),
            )
        )
    prior_run_id = str(run_id_row[0]) if run_id_row else None
    return agents, prior_run_id


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-days", type=int, default=None, help="Override SIM_NUM_DAYS for this run.")
    args = parser.parse_args()

    cfg = load_config()
    num_days = int(args.num_days) if args.num_days is not None else cfg.num_days

    # Safety: this initial runner is intended for local only.
    if cfg.network != "local":
        raise RuntimeError("This runner is currently intended for NETWORK=local only.")

    # Create output directory for this run
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path("sim/out") / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    # Maintain a pointer to the most recent run for convenience.
    latest_path = Path("sim/out/latest.txt")
    latest_path.write_text(str(out_dir) + "\n")

    jsonl_path = out_dir / "trades.jsonl"
    db_path = out_dir / "sim.db"

    # Initialize DB + chain connection
    db = SimDB(str(db_path), fast_mode=cfg.fast_mode)
    db.insert_run(run_id, cfg.network, cfg.rpc_url, cfg.token, cfg.pool, cfg.weth, utc_now_iso())

    chain = Chain(cfg.rpc_url, cfg.token, cfg.pool, cfg.weth, fast_mode=cfg.fast_mode)
    if cfg.network == "local":
        _top_up_admin_balance_if_needed(chain)
        if cfg.fast_mode:
            _configure_fast_mining(chain)

    agent_active: dict[int, bool] = {}
    balance_cache: dict[tuple[int, str], int] = {}
    token_contract_cache: dict[str, object] = {}
    pending_agents: set[int] = set()
    agent_is_eligible: dict[int, bool] = {}
    agent_buy_count: dict[int, int] = {}
    agent_threshold_hit: dict[int, bool] = {}

    # Cohort + NFT incentive settings (aligned to compute_cohorts hash logic).
    cohort_enabled = os.getenv("COHORT_ENABLED", "true").strip().lower() in {"1", "true", "yes", "y"}
    cohort_pct = int(os.getenv("COHORT_ELIGIBLE_PERCENT", "50"))
    cohort_pct = max(0, min(100, cohort_pct))
    cohort_salt = (os.getenv("COHORT_SALT") or "").strip()
    if cohort_enabled and not cohort_salt:
        raise RuntimeError("COHORT_ENABLED=true requires COHORT_SALT in .env")
    threshold_tokens = max(0.0, float(os.getenv("THRESHOLD_TOKENS", "0.00015")))

    # Record run_start_block BEFORE we do agent funding/trading.
    # This becomes the left boundary for extraction.
    run_start_block = chain.w3.eth.block_number

    # Start reward controller if configured.
    controller_meta = _maybe_start_reward_controller(out_dir)

    print("Resolved addresses:")
    print(f"  TOKEN  = {cfg.token}")
    print(f"  POOL   = {cfg.pool}")
    print(f"  TOKEN0 = {cfg.pool_token0}")
    print(f"  TOKEN1 = {cfg.pool_token1}")
    print(f"  WETH   = {cfg.weth}")
    print("")

    def _log_safe(x: float) -> float:
        return math.log(max(x, 1e-12))

    def _spot_price_weth_per_token() -> Optional[float]:
        try:
            slot0 = chain.pool.functions.slot0().call()
            sqrt_price_x96 = int(slot0[0])
            price_token1_per_token0 = (sqrt_price_x96 * sqrt_price_x96) / (2**192)
        except Exception:
            return None

        t0 = cfg.pool_token0.lower()
        t1 = cfg.pool_token1.lower()
        token = cfg.token.lower()
        weth = cfg.weth.lower()

        if token == t0 and weth == t1:
            return float(price_token1_per_token0)
        if token == t1 and weth == t0:
            if price_token1_per_token0 == 0:
                return None
            return float(1.0 / price_token1_per_token0)
        return None

    def _pool_reserves() -> tuple[float, float]:
        token_bal = chain.token.functions.balanceOf(chain.pool_addr).call()
        weth = chain.w3.eth.contract(address=chain.weth_addr, abi=chain.erc20_abi)
        weth_bal = weth.functions.balanceOf(chain.pool_addr).call()
        return token_bal / 1e18, weth_bal / 1e18

    def _erc20_balance(addr: str, holder: str) -> float:
        token = chain.w3.eth.contract(address=addr, abi=chain.erc20_abi)
        return token.functions.balanceOf(holder).call() / 1e18

    def _erc20_balance_wei(addr: str, holder: str) -> int:
        token = chain.w3.eth.contract(address=addr, abi=chain.erc20_abi)
        return int(token.functions.balanceOf(holder).call())

    def _erc20_balance_wei_cached(addr: str, holder: str) -> int:
        if not cfg.fast_mode:
            return _erc20_balance_wei(addr, holder)
        token = token_contract_cache.get(addr)
        if token is None:
            token = chain.w3.eth.contract(address=addr, abi=chain.erc20_abi)
            token_contract_cache[addr] = token
        return int(token.functions.balanceOf(holder).call())

    def _get_balance_wei(a: Agent, token_addr: str) -> int:
        if not cfg.fast_mode:
            return _erc20_balance_wei(token_addr, a.address)
        # Avoid stale cache for agents with pending txs in fast mode.
        if a.agent_id in pending_agents:
            return _erc20_balance_wei_cached(token_addr, a.address)
        key = (a.agent_id, token_addr)
        cached = balance_cache.get(key)
        if cached is not None:
            return cached
        bal = _erc20_balance_wei_cached(token_addr, a.address)
        balance_cache[key] = bal
        return bal

    def _estimated_slippage(
        amount_in: float,
        is_buy: bool,
        token_reserve: float,
        weth_reserve: float,
        fee_pct: float,
    ) -> Optional[float]:
        if amount_in <= 0 or token_reserve <= 0 or weth_reserve <= 0:
            return None
        pre_price = weth_reserve / token_reserve
        k = token_reserve * weth_reserve
        amt_eff = amount_in * (1.0 - fee_pct)
        if is_buy:
            new_weth = weth_reserve + amt_eff
            new_token = k / new_weth
        else:
            new_token = token_reserve + amt_eff
            new_weth = k / new_token
        post_price = new_weth / new_token if new_token > 0 else None
        if post_price is None or pre_price <= 0:
            return None
        return abs(post_price - pre_price) / pre_price

    def _max_amount_for_slippage(
        is_buy: bool,
        token_reserve: float,
        weth_reserve: float,
        fee_pct: float,
        max_slippage: float,
    ) -> float:
        """
        Closed-form max input size for constant-product AMM given a slippage limit.
        """
        if token_reserve <= 0 or weth_reserve <= 0:
            return 0.0
        s = max(0.0, min(float(max_slippage), 0.999999))
        fee_mult = max(1e-9, 1.0 - float(fee_pct))
        if s <= 0.0:
            return 0.0

        if is_buy:
            # slippage = (1 + delta_eff / y)^2 - 1 <= s
            delta_eff = weth_reserve * (math.sqrt(1.0 + s) - 1.0)
        else:
            # slippage = 1 - 1/(1 + delta_eff / x)^2 <= s
            delta_eff = token_reserve * ((1.0 / math.sqrt(1.0 - s)) - 1.0)

        return max(0.0, float(delta_eff) / fee_mult)

    def _poisson_sample(lam: float) -> int:
        """
        Draw from Poisson(lam) without external dependencies.
        Used for daily entry process.
        """
        if lam <= 0.0:
            return 0
        if lam > 30.0:
            return max(0, int(round(random.gauss(lam, math.sqrt(lam)))))
        l = math.exp(-lam)
        k = 0
        p = 1.0
        while p > l:
            k += 1
            p *= random.random()
        return max(0, k - 1)

    def _sample_binomial(n: int, p: float) -> int:
        """
        Simple Binomial(n, p) draw without external dependencies.
        """
        if n <= 0 or p <= 0.0:
            return 0
        if p >= 1.0:
            return n
        hits = 0
        for _ in range(n):
            if random.random() < p:
                hits += 1
        return hits

    def _stochastic_round_nonneg(x: float) -> int:
        """
        Stochastic rounding for nonnegative expected counts.
        E.g., x=0.65 -> 0 with p=0.35, 1 with p=0.65.
        """
        if x <= 0.0:
            return 0
        base = int(math.floor(x))
        frac = x - base
        return base + (1 if random.random() < frac else 0)

    def _cohort_bucket(address_lc: str) -> int:
        msg = f"{address_lc}:{cohort_salt}".encode("utf-8")
        h = chain.w3.keccak(msg)
        first4 = int.from_bytes(h[:4], "big")
        return first4 % 100

    def _compute_agent_eligible(address: str) -> bool:
        if not cohort_enabled:
            return True
        if cohort_pct <= 0:
            return False
        if cohort_pct >= 100:
            return True
        return bool(_cohort_bucket(address.lower()) < cohort_pct)

    def _register_agent_profile(a: Agent) -> None:
        agent_is_eligible[a.agent_id] = _compute_agent_eligible(a.address)
        agent_buy_count[a.agent_id] = 0
        agent_threshold_hit[a.agent_id] = False

    def _update_threshold_flag_from_holdings(a: Agent) -> None:
        """
        Threshold basis is held token balance (not cumulative bought).
        One-way latch: once threshold is hit, keep post-threshold state for the run.
        """
        if threshold_tokens <= 0.0:
            agent_threshold_hit[a.agent_id] = False
            return
        if agent_threshold_hit.get(a.agent_id, False):
            return
        token_balance_wei = _erc20_balance_wei(chain.token_addr, a.address)
        token_balance = token_balance_wei / float(10**18)
        if token_balance >= threshold_tokens:
            agent_threshold_hit[a.agent_id] = True

    def _create_agent(agent_id: int) -> Agent:
        acct = Account.create()
        return Agent(agent_id=agent_id, address=acct.address, private_key=acct.key.hex(), agent_type="retail")

    def _init_agent_state(a: Agent) -> None:
        agent_active[a.agent_id] = True
        _register_agent_profile(a)

    def _init_agent_onchain(
        a: Agent,
        start_eth: Optional[float] = None,
        start_weth: Optional[float] = None,
        start_token: Optional[float] = None,
    ) -> None:
        eth_amount = cfg.agent_start_eth if start_eth is None else start_eth
        weth_amount = cfg.agent_start_weth if start_weth is None else start_weth
        token_amount = cfg.agent_start_token if start_token is None else start_token
        # Fund ETH
        txh = chain.fund_eth(a.address, eth_amount)
        chain.wait_receipt(txh)

        # Wrap ETH to WETH (for BUY trades)
        agent_acct = Account.from_key(a.private_key)
        txh = chain.wrap_eth_to_weth(agent_acct, weth_amount)
        chain.wait_receipt(txh)

        # Seed TOKEN (for SELL trades)
        txh = chain.transfer_token(a.address, token_amount)
        chain.wait_receipt(txh)

        # Deploy payer-bound executor
        exec_addr = chain.deploy_executor_for_agent(a)
        a.executor = exec_addr
        if cfg.fast_mode:
            chain.preapprove_agent(a, exec_addr)

        # Persist agent info
        db.upsert_agent(run_id, a.agent_id, a.address, a.private_key, a.executor, a.agent_type)

    # ----------------------------
    # Create initial agents (or continue from latest run)
    # ----------------------------
    continue_from_latest = os.getenv("SIM_CONTINUE_FROM_LATEST", "false").strip().lower() in {"1", "true", "yes", "y"}
    agents: list[Agent] = []
    prior_run_id: Optional[str] = None
    if continue_from_latest:
        agents, prior_run_id = _load_agents_from_latest_run()
        if agents:
            print(f"Continuing from latest run: {prior_run_id} (agents={len(agents)})")
        else:
            print("SIM_CONTINUE_FROM_LATEST enabled, but no prior run found; starting fresh.")

    if not agents:
        next_agent_id = 0
        for _ in range(cfg.num_agents):
            new_agent = _create_agent(next_agent_id)
            _init_agent_state(new_agent)
            agents.append(new_agent)
            next_agent_id += 1
    else:
        next_agent_id = max(a.agent_id for a in agents) + 1
        for a in agents:
            _init_agent_state(a)

    # ----------------------------
    # Fund + seed + deploy executors
    # ----------------------------
    if prior_run_id is None:
        print(f"Initializing {len(agents)} agents...")
        total_agents = len(agents)
        for idx, a in enumerate(agents, start=1):
            if idx == 1 or idx == total_agents or idx % 5 == 0:
                print(f"  init agent {idx}/{total_agents} ...")
            _init_agent_onchain(a)
    else:
        print(f"Reusing {len(agents)} agents from prior run; no re-funding or re-deploy.")
        for a in agents:
            db.upsert_agent(run_id, a.agent_id, a.address, a.private_key, a.executor, a.agent_type)

    print("Agent initialization complete.\n")
    # Snapshot block for day-0 wallet balances (after seeding/reuse, before day-0 trades).
    wallet_day0_block = int(chain.w3.eth.block_number)
    db.set_run_stat("wallet_day0_block", str(wallet_day0_block))

    def _spawn_entry_agent() -> bool:
        """
        Add one entrant wallet with BUY-side inventory only.
        TOKEN starts at zero so holder count grows through market buys.
        """
        nonlocal next_agent_id
        new_agent = _create_agent(next_agent_id)
        _init_agent_state(new_agent)
        try:
            _init_agent_onchain(
                new_agent,
                start_eth=cfg.agent_start_eth,
                start_weth=cfg.agent_start_weth,
                start_token=0.0,
            )
        except Exception as e:
            print(f"  entry init failed (agent_id={next_agent_id}): {e}")
            agent_active[new_agent.agent_id] = False
            return False
        agents.append(new_agent)
        next_agent_id += 1
        return True

    def _churn_agent(a: Agent) -> bool:
        """
        Deactivate one agent and clear TOKEN balance to represent exit.
        """
        if cfg.fast_mode and a.agent_id in pending_agents:
            return False
        try:
            token_bal = _get_balance_wei(a, chain.token_addr)
        except Exception:
            token_bal = 0
        if token_bal > 0:
            try:
                txh = chain.transfer_erc20_from_agent(a, chain.token_addr, chain.admin.address, token_bal)
                rcpt = chain.wait_receipt(txh)
                if rcpt.status != 1:
                    print(f"  churn transfer reverted (agent_id={a.agent_id})")
            except Exception as e:
                print(f"  churn transfer failed (agent_id={a.agent_id}): {e}")
        agent_active[a.agent_id] = False
        pending_agents.discard(a.agent_id)
        balance_cache.pop((a.agent_id, chain.weth_addr), None)
        balance_cache.pop((a.agent_id, chain.token_addr), None)
        return True

    initial_price_weth_per_token = _spot_price_weth_per_token()
    if initial_price_weth_per_token:
        db.set_run_stat("initial_price_weth_per_token", str(initial_price_weth_per_token))

    hype_min_days = max(1, int(cfg.hype_initial_min_days))
    hype_max_days = max(hype_min_days, int(cfg.hype_initial_max_days))
    initial_hype_days = random.randint(hype_min_days, hype_max_days)
    # Fixed constants used both in runtime behavior and manifest metadata.
    flow_noise_floor = 0.20
    # Cohort treatment: only pre-threshold eligible BUYs are boosted.
    # Post-threshold behavior and churn/sell dynamics are baseline-equivalent to control.
    eligible_buy_weight_pre_threshold = 4.00
    eligible_buy_weight_post_threshold = 1.00
    eligible_sell_weight_pre_threshold = 1.00
    eligible_sell_weight_post_threshold = 1.00
    eligible_churn_mult_pre_threshold = 1.00
    eligible_churn_mult_post_threshold = 1.00

    # Write manifest after agents/prior_run_id are known.
    manifest = {
        "run_id": run_id,
        "network": cfg.network,
        "rpc_url": cfg.rpc_url,
        "token": cfg.token,
        "pool": cfg.pool,
        "pool_token0": cfg.pool_token0,
        "pool_token1": cfg.pool_token1,
        "weth": cfg.weth,
        "jstvip": cfg.jstvip,
        "num_agents": len(agents) if prior_run_id else cfg.num_agents,
        "num_days": num_days,
        "max_agents": cfg.max_agents,
        "agent_start_eth": cfg.agent_start_eth,
        "agent_start_weth": cfg.agent_start_weth,
        "agent_start_token": cfg.agent_start_token,
        "max_buy_weth": cfg.max_buy_weth,
        "max_sell_token": cfg.max_sell_token,
        "ticks_per_day": cfg.ticks_per_day,
        "max_trade_pct_buy": cfg.max_trade_pct_buy,
        "max_trade_pct_sell": cfg.max_trade_pct_sell,
        "max_slippage": cfg.max_slippage,
        "amm_fee_pct": cfg.amm_fee_pct,
        "circulating_supply_start": cfg.circulating_supply_start,
        "circulating_supply_daily_unlock": cfg.circulating_supply_daily_unlock,
        "fast_mode": cfg.fast_mode,
        # Core market-model parameters
        "regime_bull_persist": cfg.regime_bull_persist,
        "regime_bear_persist": cfg.regime_bear_persist,
        "hype_initial_min_days": hype_min_days,
        "hype_initial_max_days": hype_max_days,
        "hype_initial_days_sampled": initial_hype_days,
        "hype_persist_start": cfg.hype_persist_start,
        "hype_persist_floor": cfg.hype_persist_floor,
        "hype_decay_tau": cfg.hype_decay_tau,
        "hype_exit_to_bull_prob": cfg.hype_exit_to_bull_prob,
        "hype_reentry_prob": cfg.hype_reentry_prob,
        "sentiment_alpha": cfg.sentiment_alpha,
        "sentiment_regime_level": cfg.sentiment_regime_level,
        "sentiment_hype_mult": cfg.sentiment_hype_mult,
        "fair_mu": cfg.fair_mu,
        "fair_beta": cfg.fair_beta,
        "fair_sigma": cfg.fair_sigma,
        "fair_reversion": cfg.fair_reversion,
        "flow_intensity": cfg.flow_intensity,
        "flow_mispricing_scale": cfg.flow_mispricing_scale,
        "flow_regime_tilt": cfg.flow_regime_tilt,
        "flow_noise_sigma": cfg.flow_noise_sigma,
        "impact_kappa": cfg.impact_kappa,
        "entry_lambda_base": cfg.entry_lambda_base,
        "churn_prob_base": cfg.churn_prob_base,
        "cohort_enabled": cohort_enabled,
        "cohort_eligible_percent": cohort_pct,
        "nft_threshold_tokens": threshold_tokens,
        "nft_threshold_basis": "held_token_balance",
        "nft_threshold_latched": True,
        "fair_value_space": "normalized_log",
        # Fixed (non-env) constants for reproducibility with fewer knobs.
        "fixed_entry_sentiment_sensitivity": 1.05,
        "fixed_entry_saturation_power": 1.55,
        "fixed_churn_sentiment_sensitivity": 1.00,
        "fixed_churn_crowding_sensitivity": 1.65,
        "fixed_trade_base_participation": 0.08,
        "fixed_trade_signal_participation": 0.12,
        "fixed_trade_sentiment_participation": 0.06,
        "fixed_trade_regime_activity_bull": 0.03,
        "fixed_trade_regime_activity_hype": 0.12,
        "fixed_trade_regime_activity_bear": -0.04,
        "fixed_entry_regime_mult_hype": 1.75,
        "fixed_entry_regime_mult_bull": 1.15,
        "fixed_entry_regime_mult_bear": 0.70,
        "fixed_flow_noise_floor": 0.20,
        "fixed_eligible_buy_weight_pre_threshold": eligible_buy_weight_pre_threshold,
        "fixed_eligible_buy_weight_post_threshold": eligible_buy_weight_post_threshold,
        "fixed_eligible_sell_weight_pre_threshold": eligible_sell_weight_pre_threshold,
        "fixed_eligible_sell_weight_post_threshold": eligible_sell_weight_post_threshold,
        "fixed_eligible_churn_mult_pre_threshold": eligible_churn_mult_pre_threshold,
        "fixed_eligible_churn_mult_post_threshold": eligible_churn_mult_post_threshold,
        "initial_price_weth_per_token": initial_price_weth_per_token,
        "wallet_day0_block": int(wallet_day0_block),
        "run_start_block": int(run_start_block),
        "created_at_utc": utc_now_iso(),
        "reward_controller": controller_meta,
        "continued_from_run_id": prior_run_id,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    fast_no_receipts = (
        cfg.fast_mode and os.getenv("SIM_FAST_NO_RECEIPTS", "true").strip().lower() in {"1", "true", "yes", "y"}
    )
    fast_aggregate = (
        cfg.fast_mode and os.getenv("SIM_FAST_AGGREGATE_TICK", "false").strip().lower() in {"1", "true", "yes", "y"}
    )

    admin_agent: Optional[Agent] = None
    admin_executor: Optional[str] = None
    if fast_aggregate:
        admin_agent = Agent(
            agent_id=-1,
            address=chain.admin.address,
            private_key=chain.admin.key.hex(),
            agent_type="admin",
        )
        print("Fast mode aggregate enabled: deploying admin executor ...")
        admin_executor = chain.deploy_executor_for_agent(admin_agent)
        chain.preapprove_agent(admin_agent, admin_executor)
        # Pre-wrap a large amount of WETH for aggregated BUYs.
        wrap_weth = float(os.getenv("SIM_FAST_AGG_WETH", "100000"))
        try:
            txh = chain.wrap_eth_to_weth(Account.from_key(admin_agent.private_key), wrap_weth)
            chain.wait_receipt(txh)
        except Exception:
            pass

    # ----------------------------
    # Run simulation + fair value series
    # ----------------------------
    regime = "hype"
    hype_days_in_episode = 1
    # day 0 is already hype, so lock the remaining days.
    hype_lock_remaining = max(0, initial_hype_days - 1)
    sentiment = cfg.sentiment_regime_level
    initial_price = initial_price_weth_per_token or 1.0
    # Keep latent fair value in normalized-log space so it is directly
    # comparable to price_log = log(price / initial_price).
    fair_value_log = 0.0
    # Keep lifecycle/activity sensitivities fixed in code to avoid parameter bloat.
    entry_sentiment_sensitivity = 1.05
    entry_saturation_power = 1.55
    churn_sentiment_sensitivity = 1.00
    churn_crowding_sensitivity = 1.65
    trade_base_participation = 0.08
    trade_signal_participation = 0.12
    trade_sentiment_participation = 0.06
    trade_activity_sigma = 0.35
    # Keep small per-agent inventory buffers so participation does not collapse
    # from wallets getting fully depleted early in the run.
    min_weth_buffer_wei = int(max(0.0, cfg.agent_start_weth * 0.05) * (10**18))
    min_token_buffer_wei = int(max(0.0, cfg.agent_start_token * 0.05) * (10**18))

    def _hype_stay_probability(days_in_episode: int) -> float:
        tau = max(1e-6, float(cfg.hype_decay_tau))
        if days_in_episode <= initial_hype_days:
            return 1.0
        extra_days = float(days_in_episode - initial_hype_days)
        base = float(cfg.hype_persist_start) * math.exp(-extra_days / tau)
        return clamp(base, float(cfg.hype_persist_floor), 0.999)

    def _regime_sentiment_target(state: str) -> float:
        if state == "hype":
            return cfg.sentiment_regime_level * cfg.sentiment_hype_mult
        if state == "bull":
            return cfg.sentiment_regime_level
        return -cfg.sentiment_regime_level

    def _entry_regime_multiplier(state: str) -> float:
        if state == "hype":
            return 1.75
        if state == "bull":
            return 1.15
        return 0.70

    def _trade_regime_activity(state: str) -> float:
        if state == "hype":
            return 0.12
        if state == "bull":
            return 0.03
        return -0.04

    def _flow_regime_factor(state: str) -> float:
        if state == "hype":
            return clamp(1.0 + (2.0 * cfg.flow_regime_tilt), 0.05, 5.0)
        if state == "bull":
            return clamp(1.0 + cfg.flow_regime_tilt, 0.05, 5.0)
        return clamp(1.0 - cfg.flow_regime_tilt, 0.05, 5.0)

    def _regime_code(state: str) -> int:
        if state == "bear":
            return 0
        if state == "bull":
            return 1
        return 2

    with jsonl_path.open("a") as f:
        pending_receipts: list[tuple[str, int, int, str, str, str, str]] = []
        poll_limit = int(os.getenv("SIM_FAST_POLL_LIMIT", "200"))
        fast_poll_every = int(os.getenv("SIM_FAST_POLL_EVERY_TICKS", "5" if fast_no_receipts else "1"))
        fast_jsonl_flush_ticks = int(os.getenv("SIM_FAST_JSONL_FLUSH_TICKS", "10"))
        fast_price_ticks = int(os.getenv("SIM_FAST_PRICE_TICKS", "5"))
        cached_spot_price: Optional[float] = None
        cached_price_norm: Optional[float] = None
        cached_price_log: Optional[float] = None
        cached_token_reserve: Optional[float] = None
        cached_weth_reserve: Optional[float] = None
        cached_price_tick: Optional[int] = None

        def _poll_pending_receipts(max_checks: Optional[int] = None) -> None:
            """
            Non-blocking receipt polling to avoid stalling fast mode.
            Leaves still-pending txs in the queue.
            """
            if not pending_receipts:
                return
            remaining: list[tuple[str, int, int, str, str, str, str]] = []
            checks = 0
            for tx_hash, day_idx, agent_id, side, token_in, token_out, amount_in_wei_str in pending_receipts:
                if max_checks is not None and checks >= max_checks:
                    remaining.append((tx_hash, day_idx, agent_id, side, token_in, token_out, amount_in_wei_str))
                    continue
                checks += 1
                try:
                    rcpt = chain.w3.eth.get_transaction_receipt(tx_hash)
                    if rcpt.status == 1:
                        db.insert_trade(run_id, day_idx, agent_id, side, amount_in_wei_str,
                                        token_in, token_out, tx_hash, "MINED", None, rcpt.blockNumber, rcpt.gasUsed)
                        balance_cache.pop((agent_id, chain.weth_addr), None)
                        balance_cache.pop((agent_id, chain.token_addr), None)
                        pending_agents.discard(agent_id)
                    else:
                        db.insert_trade(run_id, day_idx, agent_id, side, amount_in_wei_str,
                                        token_in, token_out, tx_hash, "REVERT", "receipt.status=0", rcpt.blockNumber, rcpt.gasUsed)
                        pending_agents.discard(agent_id)
                except TransactionNotFound:
                    remaining.append((tx_hash, day_idx, agent_id, side, token_in, token_out, amount_in_wei_str))
                except Exception as e:
                    db.insert_trade(run_id, day_idx, agent_id, side, amount_in_wei_str,
                                    token_in, token_out, tx_hash, "REVERT", str(e), None, None)
                    pending_agents.discard(agent_id)
            pending_receipts.clear()
            pending_receipts.extend(remaining)

        def _finalize_pending_receipts_fast() -> None:
            """
            Final best-effort reconciliation at end of run for fast-no-receipt mode.
            """
            if not pending_receipts:
                return
            remaining: list[tuple[str, int, int, str, str, str, str]] = []
            for tx_hash, day_idx, agent_id, side, token_in, token_out, amount_in_wei_str in pending_receipts:
                try:
                    rcpt = chain.w3.eth.get_transaction_receipt(tx_hash)
                    if rcpt.status == 1:
                        db.insert_trade(run_id, day_idx, agent_id, side, amount_in_wei_str,
                                        token_in, token_out, tx_hash, "MINED", None, rcpt.blockNumber, rcpt.gasUsed)
                    else:
                        db.insert_trade(run_id, day_idx, agent_id, side, amount_in_wei_str,
                                        token_in, token_out, tx_hash, "REVERT", "receipt.status=0", rcpt.blockNumber, rcpt.gasUsed)
                except TransactionNotFound:
                    remaining.append((tx_hash, day_idx, agent_id, side, token_in, token_out, amount_in_wei_str))
                except Exception as e:
                    db.insert_trade(run_id, day_idx, agent_id, side, amount_in_wei_str,
                                    token_in, token_out, tx_hash, "REVERT", str(e), None, None)
            pending_receipts.clear()
            pending_receipts.extend(remaining)

        for day in range(num_days):
            jsonl_buf: list[str] = []
            def _flush_jsonl() -> None:
                if not jsonl_buf:
                    return
                f.write("".join(jsonl_buf))
                f.flush()
                jsonl_buf.clear()

            # Regime + sentiment update once per day.
            if day > 0:
                if regime == "hype":
                    if hype_lock_remaining > 0:
                        hype_lock_remaining -= 1
                        hype_days_in_episode += 1
                    else:
                        if random.random() < _hype_stay_probability(hype_days_in_episode):
                            hype_days_in_episode += 1
                        else:
                            regime = "bull" if random.random() < cfg.hype_exit_to_bull_prob else "bear"
                            hype_days_in_episode = 0
                else:
                    if random.random() < cfg.hype_reentry_prob:
                        regime = "hype"
                        hype_days_in_episode = 1
                    elif regime == "bull":
                        regime = "bull" if random.random() < cfg.regime_bull_persist else "bear"
                    else:
                        regime = "bear" if random.random() < cfg.regime_bear_persist else "bull"

            mu_regime = _regime_sentiment_target(regime)
            sentiment = (1.0 - cfg.sentiment_alpha) * sentiment + cfg.sentiment_alpha * mu_regime

            # Participant lifecycle (daily entry/churn).
            active_before_lifecycle = sum(1 for a in agents if agent_active.get(a.agent_id, True))
            # Refresh threshold flags daily using current held TOKEN balance.
            for a in agents:
                if agent_active.get(a.agent_id, True):
                    _update_threshold_flag_from_holdings(a)
            active_ratio = active_before_lifecycle / max(1.0, float(cfg.max_agents))
            churn_prob = clamp(
                cfg.churn_prob_base
                * (1.0 + churn_crowding_sensitivity * active_ratio)
                * math.exp(churn_sentiment_sensitivity * max(0.0, -sentiment)),
                0.0,
                0.35,
            )
            churned_today = 0
            churn_candidates = [
                a for a in agents
                if agent_active.get(a.agent_id, True)
                and (not cfg.fast_mode or a.agent_id not in pending_agents)
            ]
            for a in churn_candidates:
                churn_mult = 1.0
                if agent_is_eligible.get(a.agent_id, False):
                    churn_mult = (
                        eligible_churn_mult_post_threshold
                        if agent_threshold_hit.get(a.agent_id, False)
                        else eligible_churn_mult_pre_threshold
                    )
                agent_churn_prob = clamp(churn_prob * churn_mult, 0.0, 0.35)
                if random.random() < agent_churn_prob and _churn_agent(a):
                    churned_today += 1

            entry_lambda_raw = (
                cfg.entry_lambda_base
                * _entry_regime_multiplier(regime)
                * math.exp(entry_sentiment_sensitivity * max(0.0, sentiment))
            )
            entry_saturation = max(0.0, 1.0 - active_ratio) ** entry_saturation_power
            entry_lambda = entry_lambda_raw * entry_saturation
            slots_left = max(0, int(cfg.max_agents) - len(agents))
            entrants_target = min(slots_left, _poisson_sample(entry_lambda))
            entered_today = 0
            for _ in range(entrants_target):
                if _spawn_entry_agent():
                    entered_today += 1

            if entered_today > 0 or churned_today > 0:
                active_after_lifecycle = sum(1 for a in agents if agent_active.get(a.agent_id, True))
                print(
                    f"  lifecycle day {day + 1}: +{entered_today} entries, -{churned_today} churn "
                    f"(active={active_after_lifecycle}, total={len(agents)})"
                )

            # Eligible agents for this day.
            active_agents: list[Agent] = []
            for a in agents:
                if not agent_active.get(a.agent_id, True):
                    continue
                if cfg.fast_mode and fast_no_receipts and a.agent_id in pending_agents:
                    continue
                active_agents.append(a)

            ticks = max(1, int(cfg.ticks_per_day))
            drift_daily = cfg.fair_mu + (cfg.fair_beta * sentiment)
            drift_tick = drift_daily / ticks
            sigma_tick = cfg.fair_sigma / math.sqrt(ticks)

            cap_stats = {"BUY": {"trades": 0, "caps": 0}, "SELL": {"trades": 0, "caps": 0}}
            day_trades = 0

            for _tick in range(ticks):
                if cfg.fast_mode:
                    if _tick == 0:
                        print(f"  day {day + 1}/{num_days} tick {_tick + 1}/{ticks} ...")
                else:
                    if _tick % max(1, ticks // 4) == 0:
                        print(f"  day {day + 1}/{num_days} tick {_tick + 1}/{ticks} ...")
                reversion_tick = -cfg.fair_reversion * fair_value_log / ticks
                fair_value_log = fair_value_log + drift_tick + reversion_tick + random.gauss(0.0, sigma_tick)

                if not active_agents:
                    continue

                # Fast mode: cache spot price and pool reserves every N ticks.
                if cfg.fast_mode:
                    refresh = (
                        cached_price_tick is None
                        or (_tick - cached_price_tick) >= max(1, fast_price_ticks)
                    )
                    if refresh:
                        cached_spot_price = _spot_price_weth_per_token()
                        if cached_spot_price is None or cached_spot_price <= 0:
                            continue
                        cached_price_norm = cached_spot_price / initial_price
                        cached_price_log = _log_safe(cached_price_norm)
                        cached_token_reserve, cached_weth_reserve = _pool_reserves()
                        cached_price_tick = _tick

                tick_intents: list[tuple[int, str, int, str, str]] = []
                tick_buy_total = 0
                tick_sell_total = 0
                if cfg.fast_mode:
                    if cached_spot_price is None or cached_price_norm is None or cached_price_log is None:
                        continue
                    if cached_token_reserve is None or cached_weth_reserve is None:
                        continue
                    spot_price = cached_spot_price
                    price_log = cached_price_log
                    token_reserve = cached_token_reserve
                    weth_reserve = cached_weth_reserve
                else:
                    spot_price = _spot_price_weth_per_token()
                    if spot_price is None or spot_price <= 0:
                        continue
                    price_norm = spot_price / initial_price
                    price_log = _log_safe(price_norm)
                    token_reserve, weth_reserve = _pool_reserves()

                mispricing = fair_value_log - price_log
                scale = max(cfg.flow_mispricing_scale, 1e-6)
                regime_factor = _flow_regime_factor(regime)
                signal_core = math.tanh(mispricing / scale)
                signal_strength = abs(signal_core)
                deterministic_signal = cfg.flow_intensity * signal_core * regime_factor
                flow_noise_sigma_eff = cfg.flow_noise_sigma * (
                    flow_noise_floor + ((1.0 - flow_noise_floor) * signal_strength)
                )
                noisy_signal = deterministic_signal + random.gauss(0.0, flow_noise_sigma_eff)
                # Normalize by ticks so daily behavior stays stable when
                # ticks_per_day changes.
                net_flow_weth = (cfg.impact_kappa * noisy_signal) / ticks

                if abs(net_flow_weth) <= 1e-9:
                    continue

                side = "BUY" if net_flow_weth > 0 else "SELL"
                if side == "BUY":
                    total_target_amount = abs(net_flow_weth)
                else:
                    total_target_amount = abs(net_flow_weth) / max(spot_price, 1e-12)

                max_buy_liq = weth_reserve * cfg.max_trade_pct_buy
                max_sell_liq = token_reserve * cfg.max_trade_pct_sell
                max_buy_slippage = _max_amount_for_slippage(
                    is_buy=True,
                    token_reserve=token_reserve,
                    weth_reserve=weth_reserve,
                    fee_pct=cfg.amm_fee_pct,
                    max_slippage=cfg.max_slippage,
                )
                max_sell_slippage = _max_amount_for_slippage(
                    is_buy=False,
                    token_reserve=token_reserve,
                    weth_reserve=weth_reserve,
                    fee_pct=cfg.amm_fee_pct,
                    max_slippage=cfg.max_slippage,
                )
                cap_buy = min(cfg.max_buy_weth, max_buy_liq, max_buy_slippage)
                cap_sell = min(cfg.max_sell_token, max_sell_liq, max_sell_slippage)

                def _eligible_agents_for_side(side_name: str) -> list[tuple[Agent, int]]:
                    token_addr = chain.weth_addr if side_name == "BUY" else chain.token_addr
                    out: list[tuple[Agent, int]] = []
                    for ag in active_agents:
                        bal_wei = _get_balance_wei(ag, token_addr)
                        if bal_wei > 0:
                            out.append((ag, bal_wei))
                    return out

                candidates = _eligible_agents_for_side(side)
                if not candidates:
                    # Fallback to the opposite side if no one can fund the intended side.
                    side = "SELL" if side == "BUY" else "BUY"
                    if side == "BUY":
                        total_target_amount = abs(net_flow_weth)
                    else:
                        total_target_amount = abs(net_flow_weth) / max(spot_price, 1e-12)
                    candidates = _eligible_agents_for_side(side)
                if not candidates:
                    continue

                cap = cap_buy if side == "BUY" else cap_sell
                if cap <= 0:
                    continue

                flow_to_cap_ratio = total_target_amount / max(cap, 1e-12)
                # Trade-count model (no time-growth knob):
                # participation responds to regime, sentiment, and signal strength.
                regime_activity = _trade_regime_activity(regime)
                participation_p = clamp(
                    trade_base_participation
                    + (trade_signal_participation * signal_strength)
                    + (trade_sentiment_participation * max(0.0, sentiment))
                    + regime_activity,
                    0.01,
                    0.55,
                )
                flow_pressure = clamp(flow_to_cap_ratio, 0.35, 3.5)
                raw_orders = _sample_binomial(len(candidates), participation_p)
                activity_noise = random.lognormvariate(-0.5 * (trade_activity_sigma**2), trade_activity_sigma)
                expected_orders = float(raw_orders) * flow_pressure * activity_noise
                expected_orders = min(float(len(candidates)), expected_orders)
                order_count = _stochastic_round_nonneg(expected_orders)
                order_count = min(len(candidates), order_count)
                if order_count == 0 and signal_strength > 0.55 and random.random() < 0.30:
                    order_count = 1
                if order_count <= 0:
                    continue

                def _agent_order_weight(agent_obj: Agent, side_name: str) -> float:
                    if not agent_is_eligible.get(agent_obj.agent_id, False):
                        return 1.0
                    if side_name == "BUY":
                        return (
                            eligible_buy_weight_post_threshold
                            if agent_threshold_hit.get(agent_obj.agent_id, False)
                            else eligible_buy_weight_pre_threshold
                        )
                    return (
                        eligible_sell_weight_post_threshold
                        if agent_threshold_hit.get(agent_obj.agent_id, False)
                        else eligible_sell_weight_pre_threshold
                    )

                def _weighted_sample_without_replacement(
                    items: list[tuple[Agent, int]],
                    k: int,
                    side_name: str,
                ) -> list[tuple[Agent, int]]:
                    pool = list(items)
                    chosen: list[tuple[Agent, int]] = []
                    target = min(k, len(pool))
                    for _ in range(target):
                        weights = [max(0.0, _agent_order_weight(ag, side_name)) for ag, _ in pool]
                        total = sum(weights)
                        if total <= 0:
                            chosen.extend(random.sample(pool, target - len(chosen)))
                            break
                        r = random.random() * total
                        csum = 0.0
                        pick_i = 0
                        for i, w in enumerate(weights):
                            csum += w
                            if csum >= r:
                                pick_i = i
                                break
                        chosen.append(pool.pop(pick_i))
                    return chosen

                selected_agents = _weighted_sample_without_replacement(candidates, order_count, side)

                remaining_target = total_target_amount
                for idx, (a, balance_wei) in enumerate(selected_agents):
                    slots_left = max(1, order_count - idx)
                    per_order_target = remaining_target / slots_left
                    target_wei = int(max(per_order_target, 0.0) * (10**18))
                    cap_wei = int(max(cap, 0.0) * (10**18))
                    balance_buffer_wei = min_weth_buffer_wei if side == "BUY" else min_token_buffer_wei
                    spendable_wei = max(0, int(balance_wei) - int(balance_buffer_wei))
                    if cap_wei <= 0 or spendable_wei <= 0:
                        continue

                    amount_in_wei = min(target_wei, cap_wei, spendable_wei)
                    if amount_in_wei <= 0:
                        continue
                    clamped_amount = amount_in_wei / float(10**18)
                    cap_hit = target_wei > cap_wei

                    slippage = _estimated_slippage(
                        clamped_amount,
                        is_buy=(side == "BUY"),
                        token_reserve=token_reserve,
                        weth_reserve=weth_reserve,
                        fee_pct=cfg.amm_fee_pct,
                    )
                    if slippage is not None and slippage > cfg.max_slippage:
                        continue

                    token_in = cfg.weth if side == "BUY" else cfg.token
                    token_out = cfg.token if side == "BUY" else cfg.weth

                    cap_stats[side]["trades"] += 1
                    if cap_hit:
                        cap_stats[side]["caps"] += 1

                    if fast_aggregate:
                        tick_intents.append((a.agent_id, side, amount_in_wei, token_in, token_out))
                        if side == "BUY":
                            tick_buy_total += amount_in_wei
                        else:
                            tick_sell_total += amount_in_wei
                    else:
                        try:
                            tx_hash = chain.execute_swap_exact_in(
                                a,
                                a.executor,
                                token_in_addr=token_in,
                                amount_in_wei=amount_in_wei,
                                pool_token0=cfg.pool_token0,
                                pool_token1=cfg.pool_token1,
                            )
                            db.insert_trade(run_id, day, a.agent_id, side, str(amount_in_wei),
                                            token_in, token_out, tx_hash, "SENT", None, None, None)
                            if cfg.fast_mode:
                                pending_receipts.append((tx_hash, day, a.agent_id, side, token_in, token_out, str(amount_in_wei)))
                                pending_agents.add(a.agent_id)
                            else:
                                rcpt = chain.wait_receipt(tx_hash)
                                if rcpt.status == 1:
                                    db.insert_trade(run_id, day, a.agent_id, side, str(amount_in_wei),
                                                    token_in, token_out, tx_hash, "MINED", None, rcpt.blockNumber, rcpt.gasUsed)
                                else:
                                    db.insert_trade(run_id, day, a.agent_id, side, str(amount_in_wei),
                                                    token_in, token_out, tx_hash, "REVERT", "receipt.status=0", rcpt.blockNumber, rcpt.gasUsed)

                        except Exception as e:
                            db.insert_trade(run_id, day, a.agent_id, side, str(amount_in_wei),
                                            token_in, token_out, None, "REVERT", str(e), None, None)

                    line = json.dumps({
                        "run_id": run_id,
                        "day": day,
                        "agent_id": a.agent_id,
                        "address": a.address,
                        "executor": a.executor,
                        "agent_type": a.agent_type,
                        "side": side,
                        "mispricing": mispricing,
                        "net_flow_weth": net_flow_weth,
                        "amount_in_target": per_order_target,
                        "amount_in_clamped": clamped_amount,
                        "cap_limit": cap,
                        "cap_hit": cap_hit,
                        "amount_in_wei": str(amount_in_wei),
                        "token_in": token_in,
                        "token_out": token_out,
                        "ts_utc": utc_now_iso(),
                    }) + "\n"
                    if cfg.fast_mode:
                        jsonl_buf.append(line)
                    else:
                        f.write(line)
                        f.flush()
                    day_trades += 1
                    if side == "BUY":
                        agent_buy_count[a.agent_id] = int(agent_buy_count.get(a.agent_id, 0)) + 1
                    # In non-fast mode receipts are synchronous; keep threshold status
                    # aligned to actual held balance after each successful trade.
                    if not cfg.fast_mode:
                        _update_threshold_flag_from_holdings(a)
                    remaining_target = max(0.0, remaining_target - clamped_amount)

                if fast_aggregate and tick_intents:
                    # Write agent-level intents without per-agent on-chain execution.
                    for agent_id, side, amount_in_wei, token_in, token_out in tick_intents:
                        db.insert_trade(run_id, day, agent_id, side, str(amount_in_wei),
                                        token_in, token_out, None, "AGG_INTENT", None, None, None)

                    if admin_agent and admin_executor:
                        if tick_buy_total > 0:
                            try:
                                tx_hash = chain.execute_swap_exact_in(
                                    admin_agent,
                                    admin_executor,
                                    token_in_addr=cfg.weth,
                                    amount_in_wei=tick_buy_total,
                                    pool_token0=cfg.pool_token0,
                                    pool_token1=cfg.pool_token1,
                                )
                                db.insert_trade(run_id, day, -1, "BUY", str(tick_buy_total),
                                                cfg.weth, cfg.token, tx_hash, "SENT", None, None, None)
                                if cfg.fast_mode:
                                    pending_receipts.append((tx_hash, day, -1, "BUY", cfg.weth, cfg.token, str(tick_buy_total)))
                            except Exception as e:
                                db.insert_trade(run_id, day, -1, "BUY", str(tick_buy_total),
                                                cfg.weth, cfg.token, None, "REVERT", str(e), None, None)

                        if tick_sell_total > 0:
                            try:
                                tx_hash = chain.execute_swap_exact_in(
                                    admin_agent,
                                    admin_executor,
                                    token_in_addr=cfg.token,
                                    amount_in_wei=tick_sell_total,
                                    pool_token0=cfg.pool_token0,
                                    pool_token1=cfg.pool_token1,
                                )
                                db.insert_trade(run_id, day, -1, "SELL", str(tick_sell_total),
                                                cfg.token, cfg.weth, tx_hash, "SENT", None, None, None)
                                if cfg.fast_mode:
                                    pending_receipts.append((tx_hash, day, -1, "SELL", cfg.token, cfg.weth, str(tick_sell_total)))
                            except Exception as e:
                                db.insert_trade(run_id, day, -1, "SELL", str(tick_sell_total),
                                                cfg.token, cfg.weth, None, "REVERT", str(e), None, None)

                if cfg.fast_mode:
                    if (_tick + 1) % max(1, fast_poll_every) == 0:
                        _poll_pending_receipts(max_checks=poll_limit)
                    if (_tick + 1) % max(1, fast_jsonl_flush_ticks) == 0:
                        _flush_jsonl()

            if cfg.fast_mode:
                _flush_jsonl()
                _poll_pending_receipts()

            spot_price = _spot_price_weth_per_token()
            price_norm = (spot_price / initial_price) if spot_price else None
            hype_flag_day = 1.0 if regime == "hype" else 0.0
            db.insert_run_factors(
                run_id,
                day,
                sentiment,
                math.exp(fair_value_log),
                hype_flag_day,
                price_norm,
                regime_code=_regime_code(regime),
            )
            circulating_supply = cfg.circulating_supply_start + (day * cfg.circulating_supply_daily_unlock)
            db.insert_circulating_supply(run_id, day, circulating_supply)

            avg_perceived_log = fair_value_log

            db.insert_fair_value(run_id, day, math.exp(fair_value_log))
            db.insert_perceived_fair_value(run_id, day, avg_perceived_log)
            db.insert_trade_cap_daily(run_id, day, "BUY", cap_stats["BUY"]["trades"], cap_stats["BUY"]["caps"])
            db.insert_trade_cap_daily(run_id, day, "SELL", cap_stats["SELL"]["trades"], cap_stats["SELL"]["caps"])

            active_count = sum(1 for a in agents if agent_active.get(a.agent_id, True))
            print(
                f"Completed day {day + 1}/{num_days} "
                f"(active={active_count} total={len(agents)} trades={day_trades})"
            )

        # Final drain of any outstanding receipts before closing the JSONL handle.
        if cfg.fast_mode:
            if fast_no_receipts:
                _finalize_pending_receipts_fast()
            else:
                _poll_pending_receipts()

    # Record run_end_block AFTER all sim activity.
    run_end_block = chain.w3.eth.block_number

    # Persist block window for downstream extraction.
    db.set_run_block_window(run_id, int(run_start_block), int(run_end_block))

    # Also append to manifest for human debugging.
    manifest["run_end_block"] = int(run_end_block)
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    db.close()
    print(f"\nDone. Outputs written to: {out_dir}")
    print(f"Run block window: start={run_start_block} end={run_end_block}")

    # Kick off post_run analytics + warehouse append automatically.
    print("Running post_run (cohorts, swaps, prices, mints, wallet activity, warehouse append) ...")
    subprocess.check_call(
        [sys.executable, "-m", "sim.post_run", str(db_path), "--run-id", run_id]
    )
    print("post_run complete.")

    # Generate report plots for this run.
    report_outdir = Path("sim/reports") / run_id
    print(f"Running report (plots -> {report_outdir}) ...")
    subprocess.check_call(
        [sys.executable, "-m", "sim.report", "--outdir", str(report_outdir), "--runs", run_id]
    )
    print("report complete.")


if __name__ == "__main__":
    main()
