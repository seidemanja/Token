"""
sim/compute_cohort_stats.py

Daily cohort + mint aggregates for retention/segmentation plots.

Usage:
  python -m sim.compute_cohort_stats <path/to/sim.db> [--run-id RUN_ID]
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Optional


def _ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cohort_daily_stats (
          run_id TEXT NOT NULL,
          day INTEGER NOT NULL,
          eligible_wallets INTEGER NOT NULL,
          control_wallets INTEGER NOT NULL,
          minted_eligible INTEGER NOT NULL,
          minted_control INTEGER NOT NULL,
          minted_total INTEGER NOT NULL,
          PRIMARY KEY (run_id, day)
        );
        """
    )
    conn.commit()


def _get_latest_run_id(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT run_id FROM sim_runs ORDER BY created_at_utc DESC LIMIT 1"
    ).fetchone()
    if not row:
        raise RuntimeError("No sim_runs found.")
    return str(row[0])


def _get_run_stats(conn: sqlite3.Connection, run_id: str) -> tuple[int, int]:
    row = conn.execute(
        """
        SELECT
          MAX(CASE WHEN key='day0_block' THEN value END),
          MAX(CASE WHEN key='blocks_per_day' THEN value END)
        FROM run_stats
        """
    ).fetchone()
    day0_block = int(row[0]) if row and row[0] is not None else None
    blocks_per_day = int(row[1]) if row and row[1] is not None else None

    if day0_block is None:
        row = conn.execute(
            "SELECT run_start_block FROM sim_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if row and row[0] is not None:
            day0_block = int(row[0])
    if blocks_per_day is None:
        blocks_per_day = 100

    if day0_block is None:
        raise RuntimeError("run_stats missing day0_block and sim_runs missing run_start_block.")
    return int(day0_block), int(blocks_per_day)


def _get_max_day(conn: sqlite3.Connection, run_id: str) -> int:
    row = conn.execute(
        "SELECT MAX(day) FROM fair_value_daily WHERE run_id=?",
        (run_id,),
    ).fetchone()
    if row and row[0] is not None:
        return int(row[0])
    row = conn.execute(
        "SELECT MAX(day) FROM trades WHERE run_id=?",
        (run_id,),
    ).fetchone()
    return int(row[0] or 0)


def _cohort_counts(conn: sqlite3.Connection, run_id: str) -> tuple[int, int, dict[str, int]]:
    eligible = conn.execute(
        "SELECT COUNT(*) FROM wallet_cohorts WHERE run_id=? AND eligible=1",
        (run_id,),
    ).fetchone()[0]
    control = conn.execute(
        "SELECT COUNT(*) FROM wallet_cohorts WHERE run_id=? AND eligible=0",
        (run_id,),
    ).fetchone()[0]
    rows = conn.execute(
        "SELECT address, eligible FROM wallet_cohorts WHERE run_id=?",
        (run_id,),
    ).fetchall()
    elig_map = {str(addr).lower(): int(el) for addr, el in rows}
    return int(eligible), int(control), elig_map


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", help="Path to sim.db")
    parser.add_argument("--run-id", default=None, help="Run id (default: latest)")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_tables(conn)
        run_id = args.run_id or _get_latest_run_id(conn)
        day0_block, blocks_per_day = _get_run_stats(conn, run_id)
        last_day = _get_max_day(conn, run_id)

        eligible_wallets, control_wallets, elig_map = _cohort_counts(conn, run_id)

        minted_by_day = {}
        if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='nft_mints'").fetchone():
            rows = conn.execute(
                "SELECT to_address, block_number FROM nft_mints ORDER BY block_number ASC"
            ).fetchall()
            for to_addr, block_num in rows:
                day = int((int(block_num) - day0_block) // blocks_per_day) if blocks_per_day > 0 else 0
                day = max(0, min(day, last_day))
                bucket = minted_by_day.setdefault(day, {"eligible": 0, "control": 0})
                is_eligible = elig_map.get(str(to_addr).lower(), 0)
                if is_eligible:
                    bucket["eligible"] += 1
                else:
                    bucket["control"] += 1

        for day in range(last_day + 1):
            bucket = minted_by_day.get(day, {"eligible": 0, "control": 0})
            minted_eligible = int(bucket["eligible"])
            minted_control = int(bucket["control"])
            minted_total = minted_eligible + minted_control
            conn.execute(
                """
                INSERT OR REPLACE INTO cohort_daily_stats(
                  run_id, day, eligible_wallets, control_wallets, minted_eligible, minted_control, minted_total
                )
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    day,
                    eligible_wallets,
                    control_wallets,
                    minted_eligible,
                    minted_control,
                    minted_total,
                ),
            )

        conn.commit()
        print(
            f"Wrote cohort_daily_stats rows for run_id={run_id} days=0..{last_day} "
            f"(eligible={eligible_wallets} control={control_wallets})."
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
