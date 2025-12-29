"""
sim/import_reward_state.py

Imports the Node controller state JSON into SQLite and enriches it with:
- cohort eligibility (deterministic, matches controller)
- threshold reached (cumulative buys >= threshold)
- minted_onchain (optional, checked from chain)
- final classification label

Usage:
  python -m sim.import_reward_state <sim.db> <reward_state.json>

Notes:
- Cohort assignment uses the exact same rule as reward_controller_amm_swaps.js:
    bucket = keccak256(f"{address}:{salt}") and bucket % 100 < pct
- mintedCache is NOT authoritative; minted_onchain is authoritative.
"""

import json
import os
import sqlite3
import sys
from pathlib import Path

from eth_utils import keccak

from sim.config import load_config
from sim.chain import Chain


def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS reward_wallets;

        CREATE TABLE reward_wallets (
          wallet TEXT PRIMARY KEY,

          -- From controller state
          cumulative_buys_raw TEXT NOT NULL,

          -- Computed locally
          cohort_eligible INTEGER NOT NULL,
          threshold_reached INTEGER NOT NULL,

          -- Mint truth
          minted_cache INTEGER NOT NULL,
          minted_onchain INTEGER NOT NULL,

          -- Convenience label for analysis
          status TEXT NOT NULL
        );
        """
    )
    conn.commit()


def to_lower(a: str) -> str:
    return str(a).lower()


def cohort_bucket(address: str, salt: str) -> int:
    """
    Match Node logic:
      input = `${address.toLowerCase()}:${salt}`
      h = keccak256(utf8(input))
      bucket = parseInt(h.slice(2, 10), 16) % 100
    """
    s = f"{address.lower()}:{salt}".encode("utf-8")
    h = keccak(s).hex()  # hex string without 0x
    first8 = h[:8]
    return int(first8, 16) % 100


def is_in_eligible_cohort(address: str, enabled: bool, pct: int, salt: str) -> bool:
    """Cohort gating identical to controller."""
    if not enabled:
        return True
    if not salt:
        raise ValueError("COHORT_SALT required when COHORT_ENABLED=true")
    if pct <= 0:
        return False
    if pct >= 100:
        return True
    return cohort_bucket(address, salt) < pct


def classify(cohort_eligible: bool, threshold_reached: bool, minted_onchain: bool) -> str:
    """
    4-way segmentation (plus unexpected conditions):
    """
    if not cohort_eligible and minted_onchain:
        return "INELIGIBLE_BUT_MINTED (BUG)"
    if cohort_eligible and threshold_reached and minted_onchain:
        return "ELIGIBLE_REACHED_MINTED"
    if cohort_eligible and threshold_reached and not minted_onchain:
        return "ELIGIBLE_REACHED_NOT_MINTED (LATENCY/BUG)"
    if cohort_eligible and not threshold_reached and minted_onchain:
        return "ELIGIBLE_NOT_REACHED_BUT_MINTED (BUG)"
    if cohort_eligible and not threshold_reached and not minted_onchain:
        return "ELIGIBLE_NOT_REACHED_NOT_MINTED"
    if not cohort_eligible and not minted_onchain:
        return "INELIGIBLE_NOT_MINTED"
    return "UNKNOWN"


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("Usage: python -m sim.import_reward_state <sim.db> <reward_state.json>")

    db_path = sys.argv[1]
    state_path = Path(sys.argv[2])
    if not state_path.exists():
        raise SystemExit(f"State JSON not found: {state_path}")

    # Load config + chain so we can query on-chain mint truth
    cfg = load_config()
    chain = Chain(cfg.rpc_url, cfg.token, cfg.pool, cfg.weth)

    # Read controller state
    state = json.loads(state_path.read_text())
    cumulative = state.get("cumulativeBuys", {}) or {}
    minted_cache = state.get("mintedCache", {}) or {}

    # Controller env params (must match how controller ran)
    cohort_enabled = (os.getenv("COHORT_ENABLED", "true").strip().lower() in ("true", "1", "yes", "y"))
    cohort_pct = int(os.getenv("COHORT_ELIGIBLE_PERCENT", "50"))
    cohort_salt = (os.getenv("COHORT_SALT", "") or "").strip()

    decimals = int(os.getenv("TOKEN_DECIMALS", "18"))
    threshold_tokens = os.getenv("THRESHOLD_TOKENS", "0")
    # Compare in raw base units (same as controller)
    from web3 import Web3
    threshold_raw = int(Web3.to_wei(float(threshold_tokens), "ether")) if decimals == 18 else None
    if threshold_raw is None:
        raise SystemExit("This importer currently assumes TOKEN_DECIMALS=18 for threshold conversion.")

    conn = sqlite3.connect(db_path)
    ensure_tables(conn)

    # Wallet set = union of seen wallets (cumulative buys or minted cache)
    wallets = set(to_lower(k) for k in cumulative.keys()) | set(to_lower(k) for k in minted_cache.keys())

    # Also include all simulation agents (so you can analyze agents that never appeared in controller state)
    agent_rows = conn.execute("SELECT address FROM agents").fetchall()
    for (addr,) in agent_rows:
        wallets.add(to_lower(addr))

    # Prepare NFT contract call for authoritative minted status.
    # If JSTVIP not in config (should be), we cannot check on-chain.
    if not cfg.jstvip:
        raise SystemExit("JSTVIP address missing from env; cannot check minted_onchain.")
    nft = chain.w3.eth.contract(
        address=Web3.to_checksum_address(cfg.jstvip),
        abi=[
            {"inputs": [{"internalType": "address", "name": "", "type": "address"}],
             "name": "hasMinted",
             "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
             "stateMutability": "view",
             "type": "function"}
        ],
    )

    inserted = 0
    for w in sorted(wallets):
        cum_raw_s = str(cumulative.get(w, "0"))
        try:
            cum_raw = int(cum_raw_s)
        except Exception:
            cum_raw = 0

        eligible = is_in_eligible_cohort(w, cohort_enabled, cohort_pct, cohort_salt)
        reached = cum_raw >= threshold_raw
        cache_minted = 1 if bool(minted_cache.get(w, False)) else 0

        # Authoritative on-chain truth
        try:
            minted_chain = bool(nft.functions.hasMinted(Web3.to_checksum_address(w)).call())
        except Exception:
            minted_chain = False

        status = classify(eligible, reached, minted_chain)

        conn.execute(
            """
            INSERT OR REPLACE INTO reward_wallets
              (wallet, cumulative_buys_raw, cohort_eligible, threshold_reached, minted_cache, minted_onchain, status)
            VALUES (?,?,?,?,?,?,?)
            """,
            (w, str(cum_raw), 1 if eligible else 0, 1 if reached else 0, cache_minted, 1 if minted_chain else 0, status),
        )
        inserted += 1

    conn.commit()
    conn.close()

    print(f"Imported/enriched {inserted} reward_wallets rows.")
    print("Next: run SQL grouping by status to see the segmentation.")


if __name__ == "__main__":
    main()
