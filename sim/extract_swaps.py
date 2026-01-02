"""
sim/extract_swaps.py

Extracts Uniswap V3 Swap events from the configured pool over a block range,
stores them into the run SQLite DB, and computes basic daily aggregates.

Why:
- Locally you are not using a subgraph, so you need a light indexer.
- This produces the same metrics you will later compute via a subgraph on Sepolia.

Behavior (IMPORTANT):
- day0_block is the anchor for "day" bucketing.
- We ALWAYS set day0_block to the first swap block observed in this DB (MIN(block_number) from swaps).
  This makes day indexing match simulation activity, not the CLI from_block.

Usage:
  python -m sim.extract_swaps sim/out/<run_id>/sim.db <from_block> <to_block>
"""

import sqlite3
import sys
from typing import Any, Optional

from web3 import Web3

from sim.config import load_config
from sim.chain import Chain

# Minimal ABI containing only the Uniswap V3 Swap event.
UNISWAP_V3_SWAP_EVENT_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "sender", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "recipient", "type": "address"},
            {"indexed": False, "internalType": "int256", "name": "amount0", "type": "int256"},
            {"indexed": False, "internalType": "int256", "name": "amount1", "type": "int256"},
            {"indexed": False, "internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
            {"indexed": False, "internalType": "uint128", "name": "liquidity", "type": "uint128"},
            {"indexed": False, "internalType": "int24", "name": "tick", "type": "int24"},
        ],
        "name": "Swap",
        "type": "event",
    }
]


def ensure_tables(conn: sqlite3.Connection) -> None:
    """Create tables for swap events and daily aggregates if missing."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS swaps (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          block_number INTEGER NOT NULL,
          tx_hash TEXT NOT NULL,
          log_index INTEGER NOT NULL,
          sender TEXT NOT NULL,
          recipient TEXT NOT NULL,
          amount0 TEXT NOT NULL,          -- int256 as string
          amount1 TEXT NOT NULL,          -- int256 as string
          sqrt_price_x96 TEXT NOT NULL,   -- uint160 as string
          liquidity TEXT NOT NULL,        -- uint128 as string
          tick INTEGER NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS swaps_uniq ON swaps(tx_hash, log_index);

        CREATE TABLE IF NOT EXISTS daily_market (
          day INTEGER PRIMARY KEY,
          swap_count INTEGER NOT NULL,
          volume_token_in REAL NOT NULL,
          volume_weth_in REAL NOT NULL,
          avg_tick REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS run_stats (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        """
    )
    conn.commit()


def get_run_stat(conn: sqlite3.Connection, key: str) -> Optional[str]:
    """Fetch a run_stats value if it exists."""
    row = conn.execute("SELECT value FROM run_stats WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def set_run_stat(conn: sqlite3.Connection, key: str, value: str) -> None:
    """Upsert a run_stats key/value."""
    conn.execute("INSERT OR REPLACE INTO run_stats(key, value) VALUES (?,?)", (key, value))


def i256_to_int(x: Any) -> int:
    """web3 may return int already; normalize."""
    return int(x)


def u_to_int(x: Any) -> int:
    """Convert unsigned int-like to int."""
    return int(x)


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit("Usage: python -m sim.extract_swaps sim/out/<run_id>/sim.db <from_block> <to_block>")

    db_path = sys.argv[1]
    from_block = int(sys.argv[2])
    to_block = int(sys.argv[3])

    cfg = load_config()
    chain = Chain(cfg.rpc_url, cfg.token, cfg.pool, cfg.weth)

    conn = sqlite3.connect(db_path)
    ensure_tables(conn)

    # Always record extraction window bounds (auditable)
    set_run_stat(conn, "extract_from_block", str(from_block))
    set_run_stat(conn, "extract_to_block", str(to_block))
    conn.commit()

    # Create a contract instance that ONLY knows how to decode Swap events.
    pool_events_only = chain.w3.eth.contract(address=chain.pool_addr, abi=UNISWAP_V3_SWAP_EVENT_ABI)
    swap_event = pool_events_only.events.Swap()

    print(f"Extracting swaps from block {from_block} to {to_block} ...")

    # web3.py 5.x expects fromBlock/toBlock; 6.x accepts from_block/to_block.
    # Use the camelCase kwargs for compatibility with older installed versions.
    logs = swap_event.get_logs(fromBlock=from_block, toBlock=to_block)

    # Insert swaps
    inserted = 0
    for ev in logs:
        args = ev["args"]
        tx_hash = ev["transactionHash"].hex()
        log_index = ev["logIndex"]

        row = (
            int(ev["blockNumber"]),
            tx_hash,
            int(log_index),
            Web3.to_checksum_address(args["sender"]),
            Web3.to_checksum_address(args["recipient"]),
            str(i256_to_int(args["amount0"])),
            str(i256_to_int(args["amount1"])),
            str(u_to_int(args["sqrtPriceX96"])),
            str(u_to_int(args["liquidity"])),
            int(args["tick"]),
        )

        try:
            conn.execute(
                """
                INSERT INTO swaps(
                  block_number, tx_hash, log_index, sender, recipient,
                  amount0, amount1, sqrt_price_x96, liquidity, tick
                )
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                row,
            )
            inserted += 1
        except sqlite3.IntegrityError:
            # Duplicate (tx_hash, log_index); safe to ignore
            pass

    conn.commit()
    print(f"Inserted {inserted} new swaps (raw logs={len(logs)}).")

    # ALWAYS set day0_block to the first swap block present in this DB.
    # This migrates old DBs that previously set day0_block=CLI from_block.
    row = conn.execute("SELECT MIN(block_number) FROM swaps").fetchone()
    if not row or row[0] is None:
        print("No swaps present in DB after extraction; skipping day0_block update and daily aggregation.")
        conn.close()
        return

    min_swap_block = int(row[0])
    set_run_stat(conn, "day0_block", str(min_swap_block))
    set_run_stat(conn, "day0_block_source", "min_swap_block(swaps)")
    conn.commit()

    day0_block = min_swap_block

    # ----------------------------
    # Rebuild daily aggregates deterministically
    # ----------------------------
    blocks_per_day_s = get_run_stat(conn, "blocks_per_day")
    blocks_per_day = int(blocks_per_day_s) if blocks_per_day_s else 100
    if blocks_per_day <= 0:
        blocks_per_day = 100
    set_run_stat(conn, "blocks_per_day", str(int(blocks_per_day)))
    print(f"Computing daily aggregates using blocks_per_day={blocks_per_day}...")

    # IMPORTANT:
    # Rebuild from ALL swaps in DB so results are deterministic and we never keep stale day buckets.
    rows = conn.execute(
        "SELECT block_number, amount0, amount1, tick FROM swaps ORDER BY block_number ASC"
    ).fetchall()

    token_is_0 = cfg.token.lower() == cfg.pool_token0.lower()

    daily: dict[int, dict[str, float]] = {}
    for block_number, amount0_s, amount1_s, tick in rows:
        day = (int(block_number) - day0_block) // blocks_per_day

        amount0 = int(amount0_s)
        amount1 = int(amount1_s)

        token0_in = max(amount0, 0)
        token1_in = max(amount1, 0)

        if token_is_0:
            token_in = token0_in
            weth_in = token1_in
        else:
            token_in = token1_in
            weth_in = token0_in

        token_in_f = token_in / 1e18
        weth_in_f = weth_in / 1e18

        if day not in daily:
            daily[day] = {
                "swap_count": 0,
                "volume_token_in": 0.0,
                "volume_weth_in": 0.0,
                "tick_sum": 0,
                "tick_n": 0,
            }

        daily[day]["swap_count"] += 1
        daily[day]["volume_token_in"] += token_in_f
        daily[day]["volume_weth_in"] += weth_in_f
        daily[day]["tick_sum"] += int(tick)
        daily[day]["tick_n"] += 1

    # Clear ALL old daily_market rows (prevents stale buckets like 44–50)
    conn.execute("DELETE FROM daily_market")

    # Insert fresh rows
    for day, d in daily.items():
        avg_tick = d["tick_sum"] / max(d["tick_n"], 1)
        conn.execute(
            """
            INSERT OR REPLACE INTO daily_market(day, swap_count, volume_token_in, volume_weth_in, avg_tick)
            VALUES (?,?,?,?,?)
            """,
            (int(day), int(d["swap_count"]), float(d["volume_token_in"]), float(d["volume_weth_in"]), float(avg_tick)),
        )

    conn.commit()
    print(f"Wrote {len(daily)} daily_market rows.")

    conn.close()


if __name__ == "__main__":
    main()
