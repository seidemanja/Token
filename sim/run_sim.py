"""
sim/run_sim.py

Phase-0 simulation runner.

Key change:
- Records run_start_block and run_end_block in sim_runs so downstream analytics
  can extract swaps/mints scoped exactly to this run, not a broad lookback window.
"""

import json
import random
from datetime import datetime, timezone
from pathlib import Path

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


def main() -> None:
    cfg = load_config()

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

    # Record run_start_block BEFORE we do agent funding/trading.
    # This becomes the left boundary for extraction.
    run_start_block = chain.w3.eth.block_number

    # Write manifest
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
        "num_agents": cfg.num_agents,
        "num_days": cfg.num_days,
        "agent_start_eth": cfg.agent_start_eth,
        "agent_start_weth": cfg.agent_start_weth,
        "agent_start_token": cfg.agent_start_token,
        "trade_prob": cfg.trade_prob,
        "buy_prob": cfg.buy_prob,
        "sell_prob": cfg.sell_prob,
        "buy_weth_min": cfg.buy_weth_min,
        "buy_weth_max": cfg.buy_weth_max,
        "sell_token_min": cfg.sell_token_min,
        "sell_token_max": cfg.sell_token_max,
        "max_buy_weth": cfg.max_buy_weth,
        "max_sell_token": cfg.max_sell_token,
        "run_start_block": int(run_start_block),
        "created_at_utc": utc_now_iso(),
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    print("Resolved addresses:")
    print(f"  TOKEN  = {cfg.token}")
    print(f"  POOL   = {cfg.pool}")
    print(f"  TOKEN0 = {cfg.pool_token0}")
    print(f"  TOKEN1 = {cfg.pool_token1}")
    print(f"  WETH   = {cfg.weth}")
    print("")

    # ----------------------------
    # Create agents
    # ----------------------------
    agents: list[Agent] = []
    for i in range(cfg.num_agents):
        acct = Account.create()
        agents.append(Agent(agent_id=i, address=acct.address, private_key=acct.key.hex()))

    # ----------------------------
    # Fund + seed + deploy executors
    # ----------------------------
    print(f"Initializing {cfg.num_agents} agents...")
    for a in agents:
        # Fund ETH
        txh = chain.fund_eth(a.address, cfg.agent_start_eth)
        chain.wait_receipt(txh)

        # Wrap ETH to WETH (for BUY trades)
        agent_acct = Account.from_key(a.private_key)
        txh = chain.wrap_eth_to_weth(agent_acct, cfg.agent_start_weth)
        chain.wait_receipt(txh)

        # Seed TOKEN (for SELL trades)
        txh = chain.transfer_token(a.address, cfg.agent_start_token)
        chain.wait_receipt(txh)

        # Deploy payer-bound executor
        exec_addr = chain.deploy_executor_for_agent(a)
        a.executor = exec_addr

        # Persist agent info
        db.upsert_agent(run_id, a.agent_id, a.address, a.private_key, a.executor)

    print("Agent initialization complete.\n")

    # ----------------------------
    # Run simulation
    # ----------------------------
    with jsonl_path.open("a") as f:
        for day in range(cfg.num_days):
            for a in agents:
                if random.random() > cfg.trade_prob:
                    continue

                do_buy = (random.random() < cfg.buy_prob)

                if do_buy:
                    side = "BUY"

                    sampled_weth = random.uniform(cfg.buy_weth_min, cfg.buy_weth_max)
                    clamped_weth = clamp(sampled_weth, cfg.buy_weth_min, min(cfg.buy_weth_max, cfg.max_buy_weth))

                    token_in = cfg.weth
                    token_out = cfg.token
                    amount_in_wei = int(clamped_weth * (10**18))

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
                        "side": side,
                        "amount_in_sampled": sampled_weth,
                        "amount_in_clamped": clamped_weth,
                        "amount_in_wei": str(amount_in_wei),
                        "token_in": token_in,
                        "token_out": token_out,
                        "ts_utc": utc_now_iso(),
                    }) + "\n")
                    f.flush()

                else:
                    side = "SELL"

                    sampled_token = random.uniform(cfg.sell_token_min, cfg.sell_token_max)
                    clamped_token = clamp(sampled_token, cfg.sell_token_min, min(cfg.sell_token_max, cfg.max_sell_token))

                    token_in = cfg.token
                    token_out = cfg.weth
                    amount_in_wei = int(clamped_token * (10**18))

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
                        "side": side,
                        "amount_in_sampled": sampled_token,
                        "amount_in_clamped": clamped_token,
                        "amount_in_wei": str(amount_in_wei),
                        "token_in": token_in,
                        "token_out": token_out,
                        "ts_utc": utc_now_iso(),
                    }) + "\n")
                    f.flush()

            print(f"Completed day {day + 1}/{cfg.num_days}")

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


if __name__ == "__main__":
    main()
