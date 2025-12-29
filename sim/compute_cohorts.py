"""
sim/compute_cohorts.py

Deterministically assigns wallets to cohort buckets and writes wallet_cohorts.

- Reads run_id from sim_runs (latest by created_at_utc) unless provided.
- Reads wallets from run_wallets for that run if available.
  If run_wallets is empty/missing, it falls back to agents and backfills run_wallets.
- Computes cohort eligibility using COHORT_* env vars (same logic as controller).
- Creates/refreshes:
    - wallet_cohorts(run_id, address, bucket, eligible)
    - run_wallets(run_id, address)  [derived from agents if needed]

Usage:
  python -m sim.compute_cohorts <path/to/sim.db> [--run-id RUN_ID]
"""

import argparse
import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv


def load_env_explicit() -> None:
    """
    Load .env reliably by explicit path.

    This avoids python-dotenv's find_dotenv() edge cases that can occur in newer Python versions
    or when execution frames are unusual.
    """
    repo_root = Path(__file__).resolve().parents[1]
    env_path = repo_root / ".env"
    load_dotenv(dotenv_path=env_path)


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y")


def _env_int(name: str, default: int) -> int:
    v = (os.getenv(name) or "").strip()
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def cohort_bucket(address_lc: str, salt: str) -> int:
    """
    Match reward_controller_amm_swaps.js:
      bucket = keccak256(utf8(address:salt)) % 100

    Implementation notes:
    - ethers.keccak256(utf8Bytes(...)) => keccak of the utf-8 bytes
    - We then take a stable reduction mod 100
    - We mirror prior logic that effectively uses the first 4 bytes of the hash
    """
    from web3 import Web3  # local import to keep module deps scoped

    msg = f"{address_lc}:{salt}".encode("utf-8")
    h = Web3.keccak(msg)  # bytes32
    first4 = int.from_bytes(h[:4], "big")
    return first4 % 100


def is_eligible(address_lc: str, enabled: bool, pct: int, salt: str) -> tuple[int, int]:
    """
    Return (eligible_int, bucket_int)
    """
    if not enabled:
        # If cohort gating disabled, everyone is eligible; bucket not meaningful
        return 1, 0

    if not salt:
        raise RuntimeError("COHORT_ENABLED=true but COHORT_SALT is missing in env.")

    if pct <= 0:
        return 0, 0
    if pct >= 100:
        # Everyone eligible; still compute bucket for completeness
        return 1, cohort_bucket(address_lc, salt)

    b = cohort_bucket(address_lc, salt)
    return (1 if b < pct else 0), b


def ensure_tables(conn: sqlite3.Connection) -> None:
    """
    Ensure cohort/run wallet tables exist.
    """
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS wallet_cohorts (
          run_id TEXT NOT NULL,
          address TEXT NOT NULL,
          bucket INTEGER NOT NULL,
          eligible INTEGER NOT NULL,
          PRIMARY KEY (run_id, address)
        );

        CREATE TABLE IF NOT EXISTS run_wallets (
          run_id TEXT NOT NULL,
          address TEXT NOT NULL,
          PRIMARY KEY (run_id, address)
        );
        """
    )
    conn.commit()


def get_latest_run_id(conn: sqlite3.Connection) -> str:
    """
    Use sim_runs as the canonical source of runs for this DB.
    """
    row = conn.execute(
        "SELECT run_id FROM sim_runs ORDER BY created_at_utc DESC LIMIT 1"
    ).fetchone()
    if not row:
        raise RuntimeError("No sim_runs rows found. Run sim.run_sim first.")
    return str(row[0])


def refresh_run_wallets_from_agents(conn: sqlite3.Connection, run_id: str) -> None:
    """
    Make run_wallets deterministic and canonical for a run by deriving from agents.

    Even if run_wallets was already created elsewhere, refreshing it here keeps the cohort
    pipeline reproducible.
    """
    conn.execute("DELETE FROM run_wallets WHERE run_id = ?", (run_id,))
    conn.execute(
        """
        INSERT OR REPLACE INTO run_wallets(run_id, address)
        SELECT run_id, LOWER(address)
        FROM agents
        WHERE run_id = ?
        """,
        (run_id,),
    )
    conn.commit()


def get_wallets_for_run(conn: sqlite3.Connection, run_id: str) -> list[str]:
    """
    Prefer run_wallets; if empty, backfill from agents.
    """
    rows = conn.execute(
        "SELECT address FROM run_wallets WHERE run_id = ? ORDER BY address",
        (run_id,),
    ).fetchall()

    if rows:
        return [str(r[0]).lower() for r in rows]

    # If run_wallets has no rows, rebuild it from agents and try again
    refresh_run_wallets_from_agents(conn, run_id)
    rows = conn.execute(
        "SELECT address FROM run_wallets WHERE run_id = ? ORDER BY address",
        (run_id,),
    ).fetchall()

    return [str(r[0]).lower() for r in rows]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", help="Path to sim.db")
    parser.add_argument("--run-id", dest="run_id", default=None)
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    # Load .env reliably
    load_env_explicit()

    enabled = _env_bool("COHORT_ENABLED", True)
    pct = _env_int("COHORT_ELIGIBLE_PERCENT", 50)
    salt = (os.getenv("COHORT_SALT") or "").strip()

    conn = sqlite3.connect(str(db_path))
    ensure_tables(conn)

    run_id = args.run_id or get_latest_run_id(conn)

    # Prefer run_wallets; if missing/empty, backfill from agents
    wallets = get_wallets_for_run(conn, run_id)

    if not wallets:
        conn.close()
        raise RuntimeError(f"No wallets found for run_id={run_id} (agents/run_wallets empty).")

    # Write cohorts deterministically
    written = 0
    for addr_l in wallets:
        elig, bucket = is_eligible(addr_l, enabled, pct, salt)
        conn.execute(
            """
            INSERT OR REPLACE INTO wallet_cohorts(run_id, address, bucket, eligible)
            VALUES (?,?,?,?)
            """,
            (run_id, addr_l, int(bucket), int(elig)),
        )
        written += 1

    conn.commit()
    conn.close()

    print(
        f"Wrote {written} wallet_cohorts rows for run_id={run_id} "
        f"(enabled={enabled} pct={pct})."
    )


if __name__ == "__main__":
    main()
