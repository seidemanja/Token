"""
sim/post_run.py

One command to build all derived analytics tables for the latest run,
SCOPED STRICTLY TO THE RUN'S BLOCK WINDOW.

What it does (for latest run in sim_runs unless --run-id provided):
1) compute_cohorts
   - creates/refreshes run_wallets + wallet_cohorts for the run (from agents)
2) import_reward_state (optional)
   - if reward_state_local.json exists in run dir, imports/enriches reward_wallets
3) extract_swaps (run-scoped)
   - extracts Uniswap V3 Swap logs ONLY from [run_start_block, run_end_block]
   - writes swaps + daily_market
   - writes run_stats(day0_block=run_start_block, extract_from_block, extract_to_block)
4) compute_prices
   - writes swap_prices + daily_prices (day bucketing aligned to day0_block in run_stats)
5) extract_mints (run-scoped)
   - extracts NFT mint logs ONLY from [run_start_block, run_end_block]
6) compute_wallet_activity
   - builds wallet_activity (run-scoped mapping wallets -> first_buy_day etc.)

Usage:
  python -m sim.post_run <path/to/sim.db> [--run-id RUN_ID]
"""

from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
from pathlib import Path


def _run(cmd: list[str]) -> None:
    subprocess.check_call(cmd)


def _ensure_run_stats(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS run_stats (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        """
    )
    conn.commit()


def _get_latest_run_id(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT run_id FROM sim_runs ORDER BY created_at_utc DESC LIMIT 1"
    ).fetchone()
    if not row:
        raise RuntimeError("No sim_runs rows found. Run sim.run_sim first.")
    return str(row[0])


def _get_run_meta(conn: sqlite3.Connection, run_id: str) -> dict:
    row = conn.execute(
        """
        SELECT network, rpc_url, run_start_block, run_end_block
        FROM sim_runs
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()

    if not row:
        raise RuntimeError(f"run_id not found in sim_runs: {run_id}")

    network, rpc_url, start_block, end_block = row
    if start_block is None or end_block is None:
        raise RuntimeError(
            f"sim_runs is missing run_start_block/run_end_block for run_id={run_id}. "
            "Ensure your sim/run_sim.py records these fields at the end of the run."
        )

    return {
        "network": str(network),
        "rpc_url": str(rpc_url),
        "run_start_block": int(start_block),
        "run_end_block": int(end_block),
    }


def _set_run_stats_window(conn: sqlite3.Connection, start_block: int, end_block: int) -> None:
    _ensure_run_stats(conn)
    conn.execute("INSERT OR REPLACE INTO run_stats(key, value) VALUES (?,?)", ("day0_block", str(int(start_block))))
    conn.execute("INSERT OR REPLACE INTO run_stats(key, value) VALUES (?,?)", ("extract_from_block", str(int(start_block))))
    conn.execute("INSERT OR REPLACE INTO run_stats(key, value) VALUES (?,?)", ("extract_to_block", str(int(end_block))))
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", help="Path to sim.db")
    parser.add_argument("--run-id", dest="run_id", default=None, help="Optional explicit run_id")
    args = parser.parse_args()

    db_path = Path(args.db_path).resolve()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    run_dir = db_path.parent

    conn = sqlite3.connect(str(db_path))
    try:
        run_id = args.run_id or _get_latest_run_id(conn)
        meta = _get_run_meta(conn, run_id)

        print("post_run starting")
        print(f"  network={meta['network']}")
        print(f"  rpc_url={meta['rpc_url']}")
        print(f"  db={db_path}")
        print(f"  run_id={run_id}")
        print(f"  run_dir={run_dir}")
        print(f"  run block window: start={meta['run_start_block']} end={meta['run_end_block']}")
    finally:
        conn.close()

    # 1) compute cohorts (also builds run_wallets)
    print("Running compute_cohorts ...")
    _run([sys.executable, "-m", "sim.compute_cohorts", str(db_path), "--run-id", run_id])

    # 2) reward_state import (optional)
    reward_state = run_dir / "reward_state_local.json"
    if reward_state.exists():
        print("Running import_reward_state ...")
        _run([sys.executable, "-m", "sim.import_reward_state", str(db_path), str(reward_state)])
    else:
        print(f"Note: {reward_state} not found; skipping import_reward_state.")

    # 3) run-scoped swap extraction + day0 alignment
    conn = sqlite3.connect(str(db_path))
    try:
        _set_run_stats_window(conn, meta["run_start_block"], meta["run_end_block"])
    finally:
        conn.close()

    print("Running extract_swaps (run-scoped) ...")
    _run(
        [
            sys.executable,
            "-m",
            "sim.extract_swaps",
            str(db_path),
            str(meta["run_start_block"]),
            str(meta["run_end_block"]),
        ]
    )

    # 4) prices
    print("Running compute_prices ...")
    _run([sys.executable, "-m", "sim.compute_prices", str(db_path)])

    # 5) run-scoped mint extraction
    print("Running extract_mints (run-scoped) ...")
    _run(
        [
            sys.executable,
            "-m",
            "sim.extract_mints",
            str(db_path),
            str(meta["run_start_block"]),
            str(meta["run_end_block"]),
        ]
    )

    # 6) wallet activity
    print("Running compute_wallet_activity ...")
    _run([sys.executable, "-m", "sim.compute_wallet_activity", str(db_path)])

    print("post_run complete.")


if __name__ == "__main__":
    main()
