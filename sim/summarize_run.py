"""
sim/summarize_run.py

Reads the SQLite DB from a completed run and prints basic diagnostics.
This is the first step toward a real dashboard, but runs entirely locally.

Usage:
  python -m sim.summarize_run sim/out/<run_id>/sim.db
"""

import sqlite3
import sys
from pathlib import Path


def q(conn: sqlite3.Connection, sql: str, params: tuple = ()):
    """Convenience query helper."""
    cur = conn.execute(sql, params)
    return cur.fetchall()


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m sim.summarize_run sim/out/<run_id>/sim.db")

    db_path = Path(sys.argv[1]).resolve()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))

    # Run metadata
    run = q(conn, "SELECT run_id, network, token, pool, weth, created_at_utc FROM sim_runs LIMIT 1")
    if not run:
        raise SystemExit("No sim_runs row found. DB may be empty/corrupt.")
    run_id, network, token, pool, weth, created_at = run[0]

    print("Run metadata")
    print(f"  run_id    : {run_id}")
    print(f"  network   : {network}")
    print(f"  created   : {created_at}")
    print(f"  token     : {token}")
    print(f"  pool      : {pool}")
    print(f"  weth      : {weth}")
    print("")

    # Basic trade counts
    total = q(conn, "SELECT COUNT(*) FROM trades")[0][0]
    mined = q(conn, "SELECT COUNT(*) FROM trades WHERE status='MINED'")[0][0]
    reverted = q(conn, "SELECT COUNT(*) FROM trades WHERE status='REVERT'")[0][0]

    buys = q(conn, "SELECT COUNT(*) FROM trades WHERE side='BUY'")[0][0]
    sells = q(conn, "SELECT COUNT(*) FROM trades WHERE side='SELL'")[0][0]

    print("Trades summary")
    print(f"  total attempts : {total}")
    print(f"  mined          : {mined}")
    print(f"  reverted       : {reverted}")
    print(f"  buys           : {buys}")
    print(f"  sells          : {sells}")
    print("")

    # Per-day mined trades
    print("Mined trades by day (count)")
    rows = q(conn, """
        SELECT day, COUNT(*)
        FROM trades
        WHERE status='MINED'
        GROUP BY day
        ORDER BY day ASC
    """)
    for day, cnt in rows:
        print(f"  day {day:>3}: {cnt}")
    print("")

    # Per-agent mined trades
    print("Mined trades by agent (count)")
    rows = q(conn, """
        SELECT agent_id, COUNT(*)
        FROM trades
        WHERE status='MINED'
        GROUP BY agent_id
        ORDER BY COUNT(*) DESC
    """)
    for agent_id, cnt in rows:
        print(f"  agent {agent_id:>3}: {cnt}")

    conn.close()


if __name__ == "__main__":
    main()
