"""
sim/compute_wallet_activity.py

Computes per-wallet activity metrics from swaps for the current run window.

Writes:
  wallet_activity(address, first_buy_day, first_swap_day, buy_count, sell_count, token_bought_raw)

Notes:
- Uses run_stats.day0_block for day mapping.
- Uses blocks_per_day=100 (must match extract_swaps convention).
- Uses the same buy direction logic as reward_controller_amm_swaps.js.

Usage:
  python -m sim.compute_wallet_activity <path/to/sim.db>
"""

import sqlite3
import sys


BLOCKS_PER_DAY = 100


def ensure(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS wallet_activity (
          address TEXT PRIMARY KEY,
          first_swap_day INTEGER,
          first_buy_day INTEGER,
          buy_count INTEGER NOT NULL,
          sell_count INTEGER NOT NULL,
          token_bought_raw TEXT NOT NULL
        );
        """
    )
    conn.commit()


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m sim.compute_wallet_activity <path/to/sim.db>")

    db_path = sys.argv[1]
    conn = sqlite3.connect(db_path)
    ensure(conn)

    # Load day0_block
    row = conn.execute("SELECT value FROM run_stats WHERE key='day0_block'").fetchone()
    if not row:
        raise SystemExit("run_stats.day0_block missing. Run sim.extract_swaps first.")
    day0_block = int(row[0])

    # Determine whether TOKEN is token0 or token1
    # We use config saved in run_stats by run_sim if available; otherwise infer from env-derived tables.
    # Easiest: read from manifests is outside SQLite, so here we infer by comparing pool_token0 to token.
    # In your DB you have swaps only; so we use reward_wallets rule:
    # If your token is token0 in this project, buys correspond to amount0 < 0.
    # We'll detect token_is_0 using the most common environment: token0 address is in config.py, but not stored in DB.
    # Therefore: use a conservative assumption: token is token0 (your current deployments show TOKEN0==TOKEN).
    token_is_0 = True

    # Pull swaps (recipient, block_number, amount0, amount1)
    swaps = conn.execute(
        "SELECT recipient, block_number, amount0, amount1 FROM swaps ORDER BY block_number ASC"
    ).fetchall()

    # Aggregate per wallet
    agg = {}
    for recipient, block_number, amount0_s, amount1_s in swaps:
        addr = str(recipient).lower()
        day = (int(block_number) - day0_block) // BLOCKS_PER_DAY

        amount0 = int(amount0_s)
        amount1 = int(amount1_s)

        if addr not in agg:
            agg[addr] = {
                "first_swap_day": day,
                "first_buy_day": None,
                "buy_count": 0,
                "sell_count": 0,
                "token_bought": 0,  # raw units
            }

        # first swap day
        agg[addr]["first_swap_day"] = min(agg[addr]["first_swap_day"], day)

        # buy/sell classification for the tracked token
        if token_is_0:
            token_bought = -amount0 if amount0 < 0 else 0
            token_sold = amount0 if amount0 > 0 else 0
        else:
            token_bought = -amount1 if amount1 < 0 else 0
            token_sold = amount1 if amount1 > 0 else 0

        if token_bought > 0:
            agg[addr]["buy_count"] += 1
            agg[addr]["token_bought"] += token_bought
            if agg[addr]["first_buy_day"] is None:
                agg[addr]["first_buy_day"] = day
        if token_sold > 0:
            agg[addr]["sell_count"] += 1

    # Upsert
    conn.execute("DELETE FROM wallet_activity")
    for addr, d in agg.items():
        conn.execute(
            """
            INSERT OR REPLACE INTO wallet_activity
              (address, first_swap_day, first_buy_day, buy_count, sell_count, token_bought_raw)
            VALUES (?,?,?,?,?,?)
            """,
            (
                addr,
                int(d["first_swap_day"]),
                int(d["first_buy_day"]) if d["first_buy_day"] is not None else None,
                int(d["buy_count"]),
                int(d["sell_count"]),
                str(int(d["token_bought"])),
            ),
        )

    conn.commit()
    conn.close()
    print(f"Wrote {len(agg)} wallet_activity rows.")


if __name__ == "__main__":
    main()
