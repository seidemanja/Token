"""
sim/compute_prices.py

Reads swaps from SQLite, computes:
- price_weth_per_token (float)
- normalized_price = price / anchor_price

Writes:
- swap_prices (per-swap)
- daily_prices (per-day)
- run_stats (anchor price and metadata)

Anchor policy (robust):
- anchor_policy = FIRST_NONEMPTY_DAY_MEDIAN
- day bucketing uses run_stats.day0_block (written by extract_swaps)
- anchor_day = minimum day that actually has swaps
- anchor_price = median price on anchor_day

Usage:
  python -m sim.compute_prices <path/to/sim.db>
"""

import sqlite3
import sys
from typing import Optional

from sim.config import load_config
from sim.price import sqrt_price_x96_to_price_token1_per_token0


def ensure_tables(conn: sqlite3.Connection) -> None:
    """
    Drop/recreate price tables so reruns are deterministic and we don't mix old logic.
    Keep run_stats (do NOT drop it) because other scripts write to it (e.g., extract_swaps).
    """
    conn.executescript(
        """
        DROP TABLE IF EXISTS swap_prices;
        DROP TABLE IF EXISTS daily_prices;

        CREATE TABLE IF NOT EXISTS swap_prices (
          tx_hash TEXT NOT NULL,
          log_index INTEGER NOT NULL,
          block_number INTEGER NOT NULL,
          sqrt_price_x96 TEXT NOT NULL,
          tick INTEGER NOT NULL,
          price_weth_per_token REAL NOT NULL,
          normalized_price REAL NOT NULL,
          PRIMARY KEY (tx_hash, log_index)
        );

        CREATE TABLE IF NOT EXISTS daily_prices (
          day INTEGER PRIMARY KEY,
          swap_count INTEGER NOT NULL,
          avg_price_weth_per_token REAL NOT NULL,
          avg_normalized_price REAL NOT NULL,
          open_price_weth_per_token REAL NOT NULL,
          high_price_weth_per_token REAL NOT NULL,
          low_price_weth_per_token REAL NOT NULL,
          close_price_weth_per_token REAL NOT NULL,
          open_normalized_price REAL NOT NULL,
          high_normalized_price REAL NOT NULL,
          low_normalized_price REAL NOT NULL,
          close_normalized_price REAL NOT NULL,
          volume_weth_in REAL,
          trades_count INTEGER,
          fair_value_close REAL
        );

        CREATE TABLE IF NOT EXISTS run_stats (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        """
    )
    conn.commit()


def price_weth_per_token_from_sqrt(cfg, sqrt_price_x96: int) -> float:
    """
    Convert sqrtPriceX96 to WETH/TOKEN using pool token ordering.

    Uniswap V3 gives price = token1/token0 (in raw units).
    We then map that to WETH/TOKEN depending on where TOKEN sits.
    """
    p_token1_per_token0 = sqrt_price_x96_to_price_token1_per_token0(sqrt_price_x96)

    token_is_0 = cfg.token.lower() == cfg.pool_token0.lower()
    token_is_1 = cfg.token.lower() == cfg.pool_token1.lower()

    if token_is_0:
        return float(p_token1_per_token0)

    if token_is_1:
        return float(1.0 / p_token1_per_token0) if p_token1_per_token0 != 0 else 0.0

    raise ValueError("Config TOKEN is not pool token0/token1; cannot compute price.")


def get_run_stat(conn: sqlite3.Connection, key: str) -> Optional[str]:
    """Fetch a run_stats value if it exists."""
    row = conn.execute("SELECT value FROM run_stats WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def set_run_stat(conn: sqlite3.Connection, key: str, value: str) -> None:
    """Upsert a run_stats key/value."""
    conn.execute("INSERT OR REPLACE INTO run_stats(key, value) VALUES (?,?)", (key, value))


def median(xs: list[float]) -> float:
    """Compute the median of a non-empty list."""
    xs_sorted = sorted(xs)
    n = len(xs_sorted)
    mid = n // 2
    if n % 2 == 1:
        return xs_sorted[mid]
    return 0.5 * (xs_sorted[mid - 1] + xs_sorted[mid])


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m sim.compute_prices <path/to/sim.db>")

    db_path = sys.argv[1]
    cfg = load_config()

    conn = sqlite3.connect(db_path)
    ensure_tables(conn)

    swaps = conn.execute(
        """
        SELECT block_number, tx_hash, log_index, sqrt_price_x96, tick
        FROM swaps
        ORDER BY block_number ASC, tx_hash ASC, log_index ASC
        """
    ).fetchall()

    if not swaps:
        raise SystemExit("No swaps found in DB. Run extract_swaps first.")

    # Day bucketing anchor: prefer run_stats.day0_block (written by extract_swaps).
    # If missing, fall back to first swap block and persist it.
    first_block = int(swaps[0][0])
    day0_block_s = get_run_stat(conn, "day0_block")
    if day0_block_s is None:
        day0_block = first_block
        set_run_stat(conn, "day0_block", str(day0_block))
    else:
        day0_block = int(day0_block_s)

    blocks_per_day_s = get_run_stat(conn, "blocks_per_day")
    blocks_per_day = int(blocks_per_day_s) if blocks_per_day_s else 100
    if blocks_per_day <= 0:
        blocks_per_day = 100
    set_run_stat(conn, "blocks_per_day", str(blocks_per_day))

    # --- Pass 1: compute price for all swaps and group prices by computed day ---
    all_rows: list[tuple[int, str, int, str, int, float, int]] = []
    prices_by_day: dict[int, list[float]] = {}

    for block_number, tx_hash, log_index, sqrt_price_x96_s, tick in swaps:
        b = int(block_number)
        p = float(price_weth_per_token_from_sqrt(cfg, int(sqrt_price_x96_s)))
        day = (b - day0_block) // blocks_per_day

        all_rows.append((b, tx_hash, int(log_index), str(int(sqrt_price_x96_s)), int(tick), p, int(day)))

        prices_by_day.setdefault(int(day), []).append(p)

    # Choose the first day that actually has swaps
    anchor_day = min(prices_by_day.keys())
    anchor_prices = prices_by_day[anchor_day]

    if not anchor_prices:
        raise SystemExit(
            "Anchor day selection failed: no prices found. "
            "This should be impossible if swaps exist; please report."
        )

    anchor_price = median(anchor_prices)
    if anchor_price <= 0:
        raise SystemExit(f"Anchor price computed as <= 0 ({anchor_price}). Check pool/token mapping.")

    # Persist anchor metadata
    set_run_stat(conn, "anchor_policy", "FIRST_NONEMPTY_DAY_MEDIAN")
    set_run_stat(conn, "anchor_day", str(anchor_day))
    set_run_stat(conn, "anchor_price_weth_per_token", str(anchor_price))
    set_run_stat(conn, "anchor_day_swap_count", str(len(anchor_prices)))
    set_run_stat(conn, "blocks_per_day", str(blocks_per_day))
    conn.commit()

    # --- Pass 2: write swap_prices using the computed anchor ---
    inserted = 0
    for b, tx_hash, log_index, sqrt_s, tick, price_weth_per_token, _day in all_rows:
        normalized = float(price_weth_per_token) / float(anchor_price)

        conn.execute(
            """
            INSERT OR REPLACE INTO swap_prices
              (tx_hash, log_index, block_number, sqrt_price_x96, tick, price_weth_per_token, normalized_price)
            VALUES (?,?,?,?,?,?,?)
            """,
            (
                tx_hash,
                int(log_index),
                int(b),
                sqrt_s,
                int(tick),
                float(price_weth_per_token),
                float(normalized),
            ),
        )
        inserted += 1

    conn.commit()

    print(f"Wrote {inserted} swap_prices rows.")
    print(
        f"Anchor policy=FIRST_NONEMPTY_DAY_MEDIAN "
        f"anchor_day={anchor_day} anchor_price_weth_per_token={anchor_price} (swaps_on_anchor_day={len(anchor_prices)})."
    )
    print(f"Day bucketing uses day0_block={day0_block} (run_stats.day0_block).")

    # --- Daily aggregation ---
    daily: dict[int, dict[str, float]] = {}
    for b, tx_hash, log_index, _sqrt, _tick, _p, day in all_rows:
        r = conn.execute(
            "SELECT price_weth_per_token, normalized_price FROM swap_prices WHERE tx_hash=? AND log_index=?",
            (tx_hash, int(log_index)),
        ).fetchone()
        if r is None:
            continue

        p, n = float(r[0]), float(r[1])

        if day not in daily:
            daily[day] = {
                "count": 0.0,
                "p_sum": 0.0,
                "n_sum": 0.0,
                "open_p": p,
                "close_p": p,
                "high_p": p,
                "low_p": p,
                "open_n": n,
                "close_n": n,
                "high_n": n,
                "low_n": n,
            }

        daily[day]["count"] += 1.0
        daily[day]["p_sum"] += p
        daily[day]["n_sum"] += n
        daily[day]["close_p"] = p
        daily[day]["close_n"] = n
        daily[day]["high_p"] = max(daily[day]["high_p"], p)
        daily[day]["low_p"] = min(daily[day]["low_p"], p)
        daily[day]["high_n"] = max(daily[day]["high_n"], n)
        daily[day]["low_n"] = min(daily[day]["low_n"], n)

    volumes = {}
    if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_market'").fetchone():
        volumes = {
            int(row[0]): (float(row[1]), int(row[2]))
            for row in conn.execute(
                "SELECT day, volume_weth_in, swap_count FROM daily_market ORDER BY day ASC"
            ).fetchall()
        }
    fair_values = {}
    if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fair_value_daily'").fetchone():
        fair_values = {
            int(row[0]): float(row[1])
            for row in conn.execute(
                "SELECT day, fair_value FROM fair_value_daily ORDER BY day ASC"
            ).fetchall()
        }

    for day, d in daily.items():
        cnt = int(d["count"])
        volume_weth_in, trades_count = volumes.get(int(day), (None, None))
        fair_value_close = fair_values.get(int(day))
        conn.execute(
            """
            INSERT OR REPLACE INTO daily_prices(
              day,
              swap_count,
              avg_price_weth_per_token,
              avg_normalized_price,
              open_price_weth_per_token,
              high_price_weth_per_token,
              low_price_weth_per_token,
              close_price_weth_per_token,
              open_normalized_price,
              high_normalized_price,
              low_normalized_price,
              close_normalized_price,
              volume_weth_in,
              trades_count,
              fair_value_close
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(day),
                cnt,
                float(d["p_sum"] / cnt),
                float(d["n_sum"] / cnt),
                float(d["open_p"]),
                float(d["high_p"]),
                float(d["low_p"]),
                float(d["close_p"]),
                float(d["open_n"]),
                float(d["high_n"]),
                float(d["low_n"]),
                float(d["close_n"]),
                (float(volume_weth_in) if volume_weth_in is not None else None),
                (int(trades_count) if trades_count is not None else None),
                (float(fair_value_close) if fair_value_close is not None else None),
            ),
        )

    conn.commit()
    conn.close()

    print(f"Wrote {len(daily)} daily_prices rows.")


if __name__ == "__main__":
    main()
