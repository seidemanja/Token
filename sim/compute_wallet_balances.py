"""
sim/compute_wallet_balances.py

Fetches token balances for run wallets at each simulated day boundary and
writes wallet_balances_daily for holder-count and concentration diagnostics.

Usage:
  python -m sim.compute_wallet_balances <path/to/sim.db> [--run-id RUN_ID]
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Optional

from web3 import Web3

from sim.abi import load_artifact_abi, token_artifact_path
from sim.config import load_config


def _ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS wallet_balances_daily (
          run_id TEXT NOT NULL,
          day INTEGER NOT NULL,
          address TEXT NOT NULL,
          token_balance_raw TEXT NOT NULL,
          PRIMARY KEY (run_id, day, address)
        );
        """
    )
    conn.commit()


def _get_latest_run_id(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT run_id FROM sim_runs ORDER BY created_at_utc DESC LIMIT 1"
    ).fetchone()
    if not row:
        raise RuntimeError("No sim_runs rows found.")
    return str(row[0])


def _get_blocks_per_day(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM run_stats WHERE key='blocks_per_day'"
    ).fetchone()
    if row and row[0]:
        return int(row[0])
    return 100


def _get_day0_block(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM run_stats WHERE key='day0_block'"
    ).fetchone()
    if row and row[0]:
        return int(row[0])
    raise RuntimeError("run_stats.day0_block missing. Run extract_swaps first.")


def _get_max_day(conn: sqlite3.Connection, run_id: str) -> int:
    row = conn.execute(
        "SELECT MAX(day) FROM trades WHERE run_id=?",
        (run_id,),
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _get_run_end_block(conn: sqlite3.Connection, run_id: str) -> "Optional[int]":
    row = conn.execute(
        "SELECT run_end_block FROM sim_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    if row and row[0] is not None:
        return int(row[0])
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", help="Path to sim.db")
    parser.add_argument("--run-id", dest="run_id", default=None, help="Optional explicit run_id")
    args = parser.parse_args()

    db_path = Path(args.db_path).resolve()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    cfg = load_config()
    conn = sqlite3.connect(str(db_path))
    try:
        run_id = args.run_id or _get_latest_run_id(conn)
        _ensure_tables(conn)
        day0_block = _get_day0_block(conn)
        blocks_per_day = _get_blocks_per_day(conn)
        max_day = _get_max_day(conn, run_id)
        run_end_block = _get_run_end_block(conn, run_id)
        wallets = [
            r[0] for r in conn.execute(
                "SELECT address FROM run_wallets WHERE run_id=? ORDER BY address ASC",
                (run_id,),
            ).fetchall()
        ]
    finally:
        conn.close()

    if not wallets:
        print("No run_wallets found; skipping wallet balance extraction.")
        return

    w3 = Web3(Web3.HTTPProvider(cfg.rpc_url))
    if not w3.is_connected():
        raise RuntimeError(f"Could not connect to RPC: {cfg.rpc_url}")

    token_abi = load_artifact_abi(token_artifact_path())
    token = w3.eth.contract(address=Web3.to_checksum_address(cfg.token), abi=token_abi)

    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_tables(conn)
        latest_block = int(w3.eth.block_number)
        max_block = latest_block
        if run_end_block is not None:
            max_block = min(max_block, int(run_end_block))

        print(f"Computing wallet balances for run_id={run_id}, days=0..{max_day}")
        for day in range(0, max_day + 1):
            block = int(day0_block) + int(day) * int(blocks_per_day)
            if block > max_block:
                break
            for addr in wallets:
                bal = token.functions.balanceOf(Web3.to_checksum_address(addr)).call(
                    block_identifier=block
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO wallet_balances_daily(run_id, day, address, token_balance_raw)
                    VALUES (?,?,?,?)
                    """,
                    (run_id, int(day), addr.lower(), str(int(bal))),
                )
            conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
