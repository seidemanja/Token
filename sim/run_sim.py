"""
sim/run_sim.py

Phase-0 simulation runner.

Key change:
- Records run_start_block and run_end_block in sim_runs so downstream analytics
  can extract swaps/mints scoped exactly to this run, not a broad lookback window.
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
    db = SimDB(str(db_path))
    db.insert_run(run_id, cfg.network, cfg.rpc_url, cfg.token, cfg.pool, cfg.weth, utc_now_iso())

    chain = Chain(cfg.rpc_url, cfg.token, cfg.pool, cfg.weth)
    if cfg.network == "local":
        _top_up_admin_balance_if_needed(chain)

    agent_bias: dict[int, float] = {}
    agent_size: dict[int, float] = {}
    agent_idio: dict[int, float] = {}
    agent_active: dict[int, bool] = {}

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

    def _launch_premium(day: int) -> float:
        if cfg.launch_premium_tau <= 0:
            return 0.0
        return cfg.launch_premium_l0 * math.exp(-day / cfg.launch_premium_tau)

    def _poisson_sample(lam: float) -> int:
        if lam <= 0:
            return 0
        l = math.exp(-lam)
        k = 0
        p = 1.0
        while p > l:
            k += 1
            p *= random.random()
        return max(0, k - 1)

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

    def _create_agent(agent_id: int) -> Agent:
        acct = Account.create()
        return Agent(agent_id=agent_id, address=acct.address, private_key=acct.key.hex(), agent_type="retail")

    def _init_agent_state(a: Agent) -> None:
        agent_bias[a.agent_id] = random.gauss(0.0, cfg.perceived_bias_sigma)
        agent_size[a.agent_id] = random.lognormvariate(cfg.size_logn_mean, cfg.size_logn_sigma)
        agent_idio[a.agent_id] = 0.0
        agent_active[a.agent_id] = True

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
        for a in agents:
            _init_agent_onchain(a)
    else:
        print(f"Reusing {len(agents)} agents from prior run; no re-funding or re-deploy.")
        for a in agents:
            db.upsert_agent(run_id, a.agent_id, a.address, a.private_key, a.executor, a.agent_type)

    print("Agent initialization complete.\n")

    initial_price_weth_per_token = _spot_price_weth_per_token()

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
        "agent_start_eth": cfg.agent_start_eth,
        "agent_start_weth": cfg.agent_start_weth,
        "agent_start_token": cfg.agent_start_token,
        "max_buy_weth": cfg.max_buy_weth,
        "max_sell_token": cfg.max_sell_token,
        "regime_p11": cfg.regime_p11,
        "regime_p00": cfg.regime_p00,
        "sentiment_alpha": cfg.sentiment_alpha,
        "sentiment_mu_bear": cfg.sentiment_mu_bear,
        "sentiment_mu_bull": cfg.sentiment_mu_bull,
        "fair_value_start": cfg.fair_value_start,
        "fair_value_mu": cfg.fair_value_mu,
        "fair_value_beta": cfg.fair_value_beta,
        "fair_value_sigma": cfg.fair_value_sigma,
        "fair_value_floor": cfg.fair_value_floor,
        "ticks_per_day": cfg.ticks_per_day,
        "trades_per_tick_lambda": cfg.trades_per_tick_lambda,
        "perceived_bias_sigma": cfg.perceived_bias_sigma,
        "perceived_idio_rho": cfg.perceived_idio_rho,
        "perceived_idio_sigma": cfg.perceived_idio_sigma,
        "launch_premium_l0": cfg.launch_premium_l0,
        "launch_premium_tau": cfg.launch_premium_tau,
        "mispricing_theta": cfg.mispricing_theta,
        "trade_q0": cfg.trade_q0,
        "trade_qmax": cfg.trade_qmax,
        "size_logn_mean": cfg.size_logn_mean,
        "size_logn_sigma": cfg.size_logn_sigma,
        "activity_base": cfg.activity_base,
        "activity_sentiment_scale": cfg.activity_sentiment_scale,
        "activity_launch_scale": cfg.activity_launch_scale,
        "entry_lambda0": cfg.entry_lambda0,
        "entry_k_launch": cfg.entry_k_launch,
        "entry_k_sentiment": cfg.entry_k_sentiment,
        "entry_k_return": cfg.entry_k_return,
        "entry_return_mult_min": cfg.entry_return_mult_min,
        "entry_return_mult_max": cfg.entry_return_mult_max,
        "churn_pi0": cfg.churn_pi0,
        "churn_c_sentiment": cfg.churn_c_sentiment,
        "churn_c_return": cfg.churn_c_return,
        "entry_agent_eth": cfg.entry_agent_eth,
        "entry_agent_weth": cfg.entry_agent_weth,
        "entry_agent_token": cfg.entry_agent_token,
        "circulating_supply_start": cfg.circulating_supply_start,
        "circulating_supply_daily_unlock": cfg.circulating_supply_daily_unlock,
        "liquidity_policy": cfg.liquidity_policy,
        "max_trade_pct_buy": cfg.max_trade_pct_buy,
        "max_trade_pct_sell": cfg.max_trade_pct_sell,
        "max_slippage": cfg.max_slippage,
        "amm_fee_pct": cfg.amm_fee_pct,
        "initial_price_weth_per_token": initial_price_weth_per_token,
        "run_start_block": int(run_start_block),
        "created_at_utc": utc_now_iso(),
        "reward_controller": controller_meta,
        "continued_from_run_id": prior_run_id,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    # ----------------------------
    # Run simulation + fair value series
    # ----------------------------
    regime = 1
    sentiment = cfg.sentiment_mu_bull if regime == 1 else cfg.sentiment_mu_bear
    fair_value_log = _log_safe(cfg.fair_value_start)
    initial_price = initial_price_weth_per_token or 1.0
    prev_price_log = _log_safe(1.0)

    with jsonl_path.open("a") as f:
        for day in range(num_days):
            launch_premium = _launch_premium(day)
            launch_premium_lag = _launch_premium(day - 1) if day > 0 else launch_premium

            # Regime + sentiment update once per day.
            if day > 0:
                if regime == 1:
                    regime = 1 if random.random() < cfg.regime_p11 else 0
                else:
                    regime = 0 if random.random() < cfg.regime_p00 else 1

                mu_regime = cfg.sentiment_mu_bull if regime == 1 else cfg.sentiment_mu_bear
                sentiment = (1.0 - cfg.sentiment_alpha) * sentiment + cfg.sentiment_alpha * mu_regime

            # Update idiosyncratic perception terms (daily cadence).
            for a in agents:
                prev_idio = agent_idio.get(a.agent_id, 0.0)
                agent_idio[a.agent_id] = (cfg.perceived_idio_rho * prev_idio) + random.gauss(0.0, cfg.perceived_idio_sigma)

            # Daily activity flags.
            active_agents = []
            for a in agents:
                if not agent_active.get(a.agent_id, True):
                    continue
                activity_prob = cfg.activity_base * math.exp(
                    (cfg.activity_sentiment_scale * sentiment) + (cfg.activity_launch_scale * launch_premium_lag)
                )
                activity_prob = clamp(activity_prob, 0.0, 1.0)
                if random.random() < activity_prob:
                    active_agents.append(a)

            ticks = max(1, int(cfg.ticks_per_day))
            drift_daily = cfg.fair_value_mu + (cfg.fair_value_beta * sentiment)
            drift_tick = drift_daily / ticks
            sigma_tick = cfg.fair_value_sigma / math.sqrt(ticks)

            cap_stats = {"BUY": {"trades": 0, "caps": 0}, "SELL": {"trades": 0, "caps": 0}}
            day_trades = 0

            for _tick in range(ticks):
                fair_value_log = fair_value_log + drift_tick + random.gauss(0.0, sigma_tick)
                fair_value_log = max(fair_value_log, math.log(max(cfg.fair_value_floor, 1e-12)))

                if not active_agents:
                    continue

                n_trades = _poisson_sample(cfg.trades_per_tick_lambda)
                for _ in range(n_trades):
                    a = random.choice(active_agents)

                    spot_price = _spot_price_weth_per_token()
                    if spot_price is None or spot_price <= 0:
                        continue
                    price_norm = spot_price / initial_price
                    price_log = _log_safe(price_norm)

                    v_i = fair_value_log + agent_bias.get(a.agent_id, 0.0) + agent_idio.get(a.agent_id, 0.0) + launch_premium_lag
                    mispricing = v_i - price_log
                    if mispricing > cfg.mispricing_theta:
                        action = 1
                    elif mispricing < -cfg.mispricing_theta:
                        action = -1
                    else:
                        action = 0

                    if action == 0:
                        continue

                    d_i = max(0.0, abs(mispricing) - cfg.mispricing_theta)
                    size_mult = agent_size.get(a.agent_id, 1.0)
                    q_base = cfg.trade_q0 * size_mult * d_i
                    q_i = min(cfg.trade_qmax * size_mult, q_base)
                    if q_i <= 0:
                        continue

                    token_reserve, weth_reserve = _pool_reserves()
                    max_buy_liq = weth_reserve * cfg.max_trade_pct_buy
                    max_sell_liq = token_reserve * cfg.max_trade_pct_sell

                    cap_buy = min(cfg.max_buy_weth, max_buy_liq)
                    cap_sell = min(cfg.max_sell_token, max_sell_liq)
                    value_matched_sell_cap = cap_buy / spot_price
                    cap_sell = min(cap_sell, value_matched_sell_cap)

                    if action > 0:
                        side = "BUY"
                        amount_in = q_i
                        cap = cap_buy
                        balance = _erc20_balance(chain.weth_addr, a.address)
                    else:
                        side = "SELL"
                        amount_in = q_i / spot_price
                        cap = cap_sell
                        balance = _erc20_balance(chain.token_addr, a.address)

                    amount_in = min(amount_in, balance)
                    if amount_in <= 0 or cap <= 0:
                        continue

                    clamped_amount = min(amount_in, cap)
                    cap_hit = amount_in > cap

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
                    amount_in_wei = int(clamped_amount * (10**18))

                    cap_stats[side]["trades"] += 1
                    if cap_hit:
                        cap_stats[side]["caps"] += 1

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

                    f.write(json.dumps({
                        "run_id": run_id,
                        "day": day,
                        "agent_id": a.agent_id,
                        "address": a.address,
                        "executor": a.executor,
                        "agent_type": a.agent_type,
                        "side": side,
                        "amount_in_target": q_i,
                        "amount_in_clamped": clamped_amount,
                        "cap_limit": cap,
                        "cap_hit": cap_hit,
                        "amount_in_wei": str(amount_in_wei),
                        "token_in": token_in,
                        "token_out": token_out,
                        "ts_utc": utc_now_iso(),
                    }) + "\n")
                    f.flush()
                    day_trades += 1

            spot_price = _spot_price_weth_per_token()
            price_norm = (spot_price / initial_price) if spot_price else None
            db.insert_run_factors(run_id, day, sentiment, math.exp(fair_value_log), launch_premium, price_norm)
            circulating_supply = cfg.circulating_supply_start + (day * cfg.circulating_supply_daily_unlock)
            db.insert_circulating_supply(run_id, day, circulating_supply)

            r_t = _log_safe(price_norm if price_norm is not None else 1.0) - prev_price_log
            prev_price_log = _log_safe(price_norm if price_norm is not None else 1.0)

            holders: list[tuple[Agent, float]] = []
            for a in agents:
                bal = _erc20_balance(chain.token_addr, a.address)
                if bal > 0:
                    holders.append((a, bal))
            h_t = len(holders)

            return_mult = math.exp(cfg.entry_k_return * r_t)
            return_mult = clamp(return_mult, cfg.entry_return_mult_min, cfg.entry_return_mult_max)
            lambda_in = cfg.entry_lambda0 * math.exp(
                (cfg.entry_k_launch * launch_premium) + (cfg.entry_k_sentiment * sentiment)
            ) * return_mult
            new_agents_today = _poisson_sample(lambda_in)

            pi_t = cfg.churn_pi0 * math.exp(
                (cfg.churn_c_sentiment * (-sentiment)) + (cfg.churn_c_return * (-r_t))
            )
            lambda_out = h_t * max(0.0, pi_t)
            churn_today = _poisson_sample(lambda_out)

            if new_agents_today > 0:
                for _ in range(new_agents_today):
                    new_agent = _create_agent(next_agent_id)
                    next_agent_id += 1
                    _init_agent_state(new_agent)
                    _init_agent_onchain(
                        new_agent,
                        start_eth=cfg.entry_agent_eth,
                        start_weth=cfg.entry_agent_weth,
                        start_token=cfg.entry_agent_token,
                    )
                    agents.append(new_agent)

            if churn_today > 0 and holders:
                holders.sort(key=lambda item: item[1])
                for a, _ in holders[: min(churn_today, len(holders))]:
                    agent_active[a.agent_id] = False

            for a in agents:
                if a.agent_id not in agent_active:
                    agent_active[a.agent_id] = True

            perceived_vals = []
            for a in agents:
                v_i = fair_value_log + agent_bias.get(a.agent_id, 0.0) + agent_idio.get(a.agent_id, 0.0) + launch_premium_lag
                perceived_vals.append(v_i)
            avg_perceived_log = sum(perceived_vals) / len(perceived_vals) if perceived_vals else fair_value_log

            db.insert_fair_value(run_id, day, math.exp(fair_value_log))
            db.insert_perceived_fair_value(run_id, day, avg_perceived_log)
            db.insert_trade_cap_daily(run_id, day, "BUY", cap_stats["BUY"]["trades"], cap_stats["BUY"]["caps"])
            db.insert_trade_cap_daily(run_id, day, "SELL", cap_stats["SELL"]["trades"], cap_stats["SELL"]["caps"])

            print(f"Completed day {day + 1}/{num_days} (agents={len(agents)} trades={day_trades})")

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
