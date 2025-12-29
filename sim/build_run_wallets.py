"""
sim/build_run_wallets.py

Creates a run-scoped wallet list from the agents table.
This allows all analytics to be run-scoped (agent-only) even when the DB contains
swaps/controller state for other wallets.

Writes:
  run_wallets(run_id, address)

Usage:
  python -m sim.build_run_wallets <path/to/sim.db>
"""

import sqlite3
import sys


def ensure(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS run_wallets (
          run_id TEXT NOT NULL,
          address TEXT NOT NULL,
          PRIMARY KEY (run_id, address)
        );
        """
    )
    conn.commit()


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m sim.build_run_wallets <path/to/sim.db>")

    db_path = sys.argv[1]
    conn = sqlite3.connect(db_path)
    ensure(conn)

    # Determine latest run_id from wallet_cohorts (already run-scoped)
    row = conn.execute("SELECT MAX(run_id) FROM wallet_cohorts").fetchone()
    if not row or not row[0]:
        raise SystemExit("wallet_cohorts is empty. Run sim.compute_cohorts first.")
    run_id = row[0]

    # Prefer selecting from agents if run_id exists there; otherwise use wallet_cohorts
    agent_cols = [r[1] for r in conn.execute("PRAGMA table_info(agents);").fetchall()]
    if "run_id" in agent_cols:
        wallets = conn.execute("SELECT address FROM agents WHERE run_id=?", (run_id,)).fetchall()
    else:
        wallets = conn.execute("SELECT address FROM wallet_cohorts WHERE run_id=?", (run_id,)).fetchall()

    if not wallets:
        raise SystemExit(f"No wallets found for run_id={run_id}")

    inserted = 0
    for (addr,) in wallets:
        conn.execute(
            "INSERT OR REPLACE INTO run_wallets(run_id, address) VALUES (?,?)",
            (run_id, str(addr).lower()),
        )
        inserted += 1

    conn.commit()
    conn.close()
    print(f"Wrote {inserted} run_wallets rows for run_id={run_id}.")


if __name__ == "__main__":
    main()
