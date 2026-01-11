"""
sim/append_to_warehouse.py

Appends run-level analytics from a single run DB into a cross-run warehouse
SQLite file (sim/warehouse.db by default). This keeps the existing per-run
sim/out/<run_id>/sim.db files unchanged while providing run_id-keyed tables
for multi-run analysis.

Usage:
  python -m sim.append_to_warehouse [--run-db sim/out/<run_id>/sim.db] [--warehouse sim/warehouse.db]

If --run-db is omitted, the script uses sim/out/latest.txt to find the latest run.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def _latest_run_db() -> Path:
    """Resolve the latest run's sim.db using sim/out/latest.txt."""
    latest_txt = Path(__file__).resolve().parent / "out" / "latest.txt"
    if not latest_txt.exists():
        raise FileNotFoundError(f"{latest_txt} not found. Provide --run-db explicitly.")

    run_dir = Path(latest_txt.read_text().strip()).expanduser()
    db_path = run_dir / "sim.db"
    if not db_path.exists():
        raise FileNotFoundError(f"Latest run db not found at {db_path}")
    return db_path


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return bool(row)


def _ensure_warehouse_schema(conn: sqlite3.Connection) -> None:
    """
    Create cross-run tables. These are run_id-keyed so runs can be appended
    without losing per-run isolation.
    """
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS runs (
          run_id TEXT PRIMARY KEY,
          created_at_utc TEXT NOT NULL,
          network TEXT NOT NULL,
          token TEXT NOT NULL,
          pool TEXT NOT NULL,
          weth TEXT NOT NULL,
          run_start_block INTEGER,
          run_end_block INTEGER
        );

        CREATE TABLE IF NOT EXISTS run_agents (
          run_id TEXT NOT NULL,
          agent_id INTEGER NOT NULL,
          address TEXT NOT NULL,
          private_key TEXT NOT NULL,
          executor TEXT,
          agent_type TEXT DEFAULT 'retail',
          PRIMARY KEY (run_id, agent_id)
        );

        CREATE TABLE IF NOT EXISTS run_trades (
          run_id TEXT NOT NULL,
          id INTEGER NOT NULL,
          day INTEGER NOT NULL,
          agent_id INTEGER NOT NULL,
          side TEXT NOT NULL,
          amount_in_wei TEXT NOT NULL,
          token_in TEXT NOT NULL,
          token_out TEXT NOT NULL,
          tx_hash TEXT,
          status TEXT NOT NULL,
          revert_reason TEXT,
          block_number INTEGER,
          gas_used INTEGER,
          created_at_utc TEXT,
          PRIMARY KEY (run_id, id)
        );

        CREATE TABLE IF NOT EXISTS run_swaps (
          run_id TEXT NOT NULL,
          id INTEGER NOT NULL,
          block_number INTEGER NOT NULL,
          tx_hash TEXT NOT NULL,
          log_index INTEGER NOT NULL,
          sender TEXT NOT NULL,
          recipient TEXT NOT NULL,
          amount0 TEXT NOT NULL,
          amount1 TEXT NOT NULL,
          sqrt_price_x96 TEXT NOT NULL,
          liquidity TEXT NOT NULL,
          tick INTEGER NOT NULL,
          PRIMARY KEY (run_id, id)
        );

        CREATE TABLE IF NOT EXISTS run_swap_prices (
          run_id TEXT NOT NULL,
          tx_hash TEXT NOT NULL,
          log_index INTEGER NOT NULL,
          block_number INTEGER NOT NULL,
          sqrt_price_x96 TEXT NOT NULL,
          tick INTEGER NOT NULL,
          price_weth_per_token REAL NOT NULL,
          normalized_price REAL NOT NULL,
          PRIMARY KEY (run_id, tx_hash, log_index)
        );

        CREATE TABLE IF NOT EXISTS run_nft_mints (
          run_id TEXT NOT NULL,
          tx_hash TEXT NOT NULL,
          log_index INTEGER NOT NULL,
          block_number INTEGER NOT NULL,
          to_address TEXT NOT NULL,
          token_id TEXT NOT NULL,
          PRIMARY KEY (run_id, tx_hash, log_index)
        );

        CREATE TABLE IF NOT EXISTS run_fair_value_daily (
          run_id TEXT NOT NULL,
          day INTEGER NOT NULL,
          fair_value REAL NOT NULL,
          PRIMARY KEY (run_id, day)
        );

        CREATE TABLE IF NOT EXISTS run_perceived_fair_value_daily (
          run_id TEXT NOT NULL,
          day INTEGER NOT NULL,
          avg_perceived_log REAL NOT NULL,
          PRIMARY KEY (run_id, day)
        );

        CREATE TABLE IF NOT EXISTS run_circulating_supply_daily (
          run_id TEXT NOT NULL,
          day INTEGER NOT NULL,
          circulating_supply REAL NOT NULL,
          PRIMARY KEY (run_id, day)
        );

        CREATE TABLE IF NOT EXISTS run_factors_daily (
          run_id TEXT NOT NULL,
          day INTEGER NOT NULL,
          sentiment REAL NOT NULL,
          fair_value REAL NOT NULL,
          launch_mult REAL NOT NULL,
          price_norm REAL,
          PRIMARY KEY (run_id, day)
        );

        CREATE TABLE IF NOT EXISTS run_trade_cap_daily (
          run_id TEXT NOT NULL,
          day INTEGER NOT NULL,
          side TEXT NOT NULL,
          trade_count INTEGER NOT NULL,
          cap_hits INTEGER NOT NULL,
          PRIMARY KEY (run_id, day, side)
        );

        CREATE TABLE IF NOT EXISTS run_cohort_daily_stats (
          run_id TEXT NOT NULL,
          day INTEGER NOT NULL,
          eligible_wallets INTEGER NOT NULL,
          control_wallets INTEGER NOT NULL,
          minted_eligible INTEGER NOT NULL,
          minted_control INTEGER NOT NULL,
          minted_total INTEGER NOT NULL,
          PRIMARY KEY (run_id, day)
        );

        CREATE TABLE IF NOT EXISTS run_wallet_balances_daily (
          run_id TEXT NOT NULL,
          day INTEGER NOT NULL,
          address TEXT NOT NULL,
          token_balance_raw TEXT NOT NULL,
          PRIMARY KEY (run_id, day, address)
        );

        CREATE TABLE IF NOT EXISTS run_wallet_activity (
          run_id TEXT NOT NULL,
          address TEXT NOT NULL,
          first_swap_day INTEGER,
          first_buy_day INTEGER,
          buy_count INTEGER,
          sell_count INTEGER,
          token_bought_raw TEXT,
          PRIMARY KEY (run_id, address)
        );

        CREATE TABLE IF NOT EXISTS run_wallets (
          run_id TEXT NOT NULL,
          address TEXT NOT NULL,
          PRIMARY KEY (run_id, address)
        );

        CREATE TABLE IF NOT EXISTS run_wallet_cohorts (
          run_id TEXT NOT NULL,
          address TEXT NOT NULL,
          bucket INTEGER NOT NULL,
          eligible INTEGER NOT NULL,
          PRIMARY KEY (run_id, address)
        );

        CREATE TABLE IF NOT EXISTS run_reward_wallets (
          run_id TEXT NOT NULL,
          wallet TEXT NOT NULL,
          cumulative_buys_raw TEXT NOT NULL,
          cohort_eligible INTEGER NOT NULL,
          threshold_reached INTEGER NOT NULL,
          minted_cache INTEGER NOT NULL,
          minted_onchain INTEGER NOT NULL,
          status TEXT NOT NULL,
          PRIMARY KEY (run_id, wallet)
        );

        CREATE TABLE IF NOT EXISTS run_daily_prices (
          run_id TEXT NOT NULL,
          day INTEGER NOT NULL,
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
          fair_value_close REAL,
          PRIMARY KEY (run_id, day)
        );

        CREATE TABLE IF NOT EXISTS run_daily_market (
          run_id TEXT NOT NULL,
          day INTEGER NOT NULL,
          swap_count INTEGER NOT NULL,
          volume_token_in REAL NOT NULL,
          volume_weth_in REAL NOT NULL,
          avg_tick REAL NOT NULL,
          PRIMARY KEY (run_id, day)
        );

        CREATE TABLE IF NOT EXISTS run_stats (
          run_id TEXT NOT NULL,
          key TEXT NOT NULL,
          value TEXT NOT NULL,
          PRIMARY KEY (run_id, key)
        );

        CREATE TABLE IF NOT EXISTS run_summary (
          run_id TEXT PRIMARY KEY,
          created_at_utc TEXT NOT NULL,
          network TEXT NOT NULL,
          token TEXT NOT NULL,
          pool TEXT NOT NULL,
          weth TEXT NOT NULL,
          run_start_block INTEGER,
          run_end_block INTEGER,
          num_agents INTEGER,
          num_run_wallets INTEGER,
          num_wallet_cohorts INTEGER,
          trade_count INTEGER,
          mined_trades INTEGER,
          reverted_trades INTEGER,
          buy_trades INTEGER,
          sell_trades INTEGER,
          swap_events INTEGER,
          mint_events INTEGER,
          latest_trade_day INTEGER,
          anchor_price REAL,
          anchor_day INTEGER,
          total_volume_token_in REAL,
          total_volume_weth_in REAL,
          price_days INTEGER,
          market_days INTEGER
        );
        """
    )
    cols_agents = [r[1] for r in conn.execute("PRAGMA table_info(run_agents);").fetchall()]
    if "agent_type" not in cols_agents:
        conn.execute("ALTER TABLE run_agents ADD COLUMN agent_type TEXT DEFAULT 'retail';")
    conn.commit()


def _fetch_scalar(conn: sqlite3.Connection, sql: str, params: tuple = (), default=None):
    row = conn.execute(sql, params).fetchone()
    if not row or row[0] is None:
        return default
    return row[0]


def _load_run_metadata(conn: sqlite3.Connection) -> dict:
    row = conn.execute(
        """
        SELECT run_id, network, rpc_url, token, pool, weth, created_at_utc, run_start_block, run_end_block
        FROM sim_runs
        ORDER BY created_at_utc DESC
        LIMIT 1
        """
    ).fetchone()

    if not row:
        raise RuntimeError("sim_runs is empty; cannot append to warehouse.")

    return {
        "run_id": str(row[0]),
        "network": str(row[1]),
        "rpc_url": str(row[2]),
        "token": str(row[3]),
        "pool": str(row[4]),
        "weth": str(row[5]),
        "created_at_utc": str(row[6]),
        "run_start_block": int(row[7]) if row[7] is not None else None,
        "run_end_block": int(row[8]) if row[8] is not None else None,
    }


def _load_daily_rows(conn: sqlite3.Connection, table: str) -> list[tuple]:
    if not _table_exists(conn, table):
        return []
    cols = {
        "daily_prices": (
            "day, swap_count, avg_price_weth_per_token, avg_normalized_price, "
            "open_price_weth_per_token, high_price_weth_per_token, low_price_weth_per_token, close_price_weth_per_token, "
            "open_normalized_price, high_normalized_price, low_normalized_price, close_normalized_price, "
            "volume_weth_in, trades_count, fair_value_close"
        ),
        "daily_market": "day, swap_count, volume_token_in, volume_weth_in, avg_tick",
    }.get(table)
    if cols is None:
        return []
    return conn.execute(f"SELECT {cols} FROM {table} ORDER BY day ASC").fetchall()


def _compute_summary(conn: sqlite3.Connection) -> dict:
    def count(sql: str, params: tuple = ()) -> int:
        return int(_fetch_scalar(conn, sql, params, default=0) or 0)

    summary = {
        "num_agents": count("SELECT COUNT(*) FROM agents"),
        "num_run_wallets": 0,
        "num_wallet_cohorts": 0,
        "trade_count": count("SELECT COUNT(*) FROM trades"),
        "mined_trades": count("SELECT COUNT(*) FROM trades WHERE status='MINED'"),
        "reverted_trades": count("SELECT COUNT(*) FROM trades WHERE status='REVERT'"),
        "buy_trades": count("SELECT COUNT(*) FROM trades WHERE side='BUY'"),
        "sell_trades": count("SELECT COUNT(*) FROM trades WHERE side='SELL'"),
        "swap_events": 0,
        "mint_events": 0,
        "latest_trade_day": _fetch_scalar(conn, "SELECT MAX(day) FROM trades", default=None),
        "anchor_price": None,
        "anchor_day": None,
        "total_volume_token_in": 0.0,
        "total_volume_weth_in": 0.0,
        "price_days": 0,
        "market_days": 0,
    }

    if _table_exists(conn, "run_wallets"):
        summary["num_run_wallets"] = count("SELECT COUNT(*) FROM run_wallets")
    if _table_exists(conn, "wallet_cohorts"):
        summary["num_wallet_cohorts"] = count("SELECT COUNT(*) FROM wallet_cohorts")
    if _table_exists(conn, "swaps"):
        summary["swap_events"] = count("SELECT COUNT(*) FROM swaps")
    if _table_exists(conn, "nft_mints"):
        summary["mint_events"] = count("SELECT COUNT(*) FROM nft_mints")

    if _table_exists(conn, "run_stats"):
        anchor_price = _fetch_scalar(conn, "SELECT value FROM run_stats WHERE key='anchor_price_weth_per_token'")
        anchor_day = _fetch_scalar(conn, "SELECT value FROM run_stats WHERE key='anchor_day'")
        summary["anchor_price"] = float(anchor_price) if anchor_price is not None else None
        summary["anchor_day"] = int(anchor_day) if anchor_day is not None else None

    if _table_exists(conn, "daily_market"):
        vols = conn.execute(
            "SELECT COALESCE(SUM(volume_token_in),0.0), COALESCE(SUM(volume_weth_in),0.0), COUNT(*) FROM daily_market"
        ).fetchone()
        if vols:
            summary["total_volume_token_in"] = float(vols[0])
            summary["total_volume_weth_in"] = float(vols[1])
            summary["market_days"] = int(vols[2])

    if _table_exists(conn, "daily_prices"):
        summary["price_days"] = count("SELECT COUNT(*) FROM daily_prices")

    return summary


def _delete_existing(conn: sqlite3.Connection, run_id: str) -> None:
    tables = [
        "run_agents",
        "run_trades",
        "run_swaps",
        "run_swap_prices",
        "run_nft_mints",
        "run_fair_value_daily",
        "run_perceived_fair_value_daily",
        "run_circulating_supply_daily",
        "run_factors_daily",
        "run_trade_cap_daily",
        "run_cohort_daily_stats",
        "run_wallet_balances_daily",
        "run_wallet_activity",
        "run_wallets",
        "run_wallet_cohorts",
        "run_reward_wallets",
        "run_daily_prices",
        "run_daily_market",
        "run_stats",
        "run_summary",
        "runs",
    ]
    for tbl in tables:
        if tbl == "runs":
            conn.execute("DELETE FROM runs WHERE run_id=?", (run_id,))
        else:
            conn.execute(f"DELETE FROM {tbl} WHERE run_id=?", (run_id,))
    conn.commit()


def append_to_warehouse(run_db: Path, warehouse_db: Path) -> None:
    if not run_db.exists():
        raise FileNotFoundError(f"Run db not found: {run_db}")

    run_conn = sqlite3.connect(str(run_db))
    try:
        meta = _load_run_metadata(run_conn)
        daily_prices = _load_daily_rows(run_conn, "daily_prices")
        daily_market = _load_daily_rows(run_conn, "daily_market")
        summary = _compute_summary(run_conn)

        try:
            agents = run_conn.execute(
                "SELECT agent_id, address, private_key, executor, agent_type FROM agents ORDER BY agent_id ASC"
            ).fetchall()
        except sqlite3.OperationalError:
            agents = run_conn.execute(
                "SELECT agent_id, address, private_key, executor FROM agents ORDER BY agent_id ASC"
            ).fetchall()
        trades = run_conn.execute(
            """
            SELECT id, day, agent_id, side, amount_in_wei, token_in, token_out, tx_hash, status,
                   revert_reason, block_number, gas_used, created_at_utc
            FROM trades
            ORDER BY id ASC
            """
        ).fetchall()
        swaps = run_conn.execute(
            """
            SELECT id, block_number, tx_hash, log_index, sender, recipient, amount0, amount1, sqrt_price_x96, liquidity, tick
            FROM swaps
            ORDER BY block_number ASC, tx_hash ASC, log_index ASC
            """
        ).fetchall()
        swap_prices = []
        if _table_exists(run_conn, "swap_prices"):
            swap_prices = run_conn.execute(
                """
                SELECT tx_hash, log_index, block_number, sqrt_price_x96, tick, price_weth_per_token, normalized_price
                FROM swap_prices
                ORDER BY block_number ASC, tx_hash ASC, log_index ASC
                """
            ).fetchall()
        nft_mints = []
        if _table_exists(run_conn, "nft_mints"):
            nft_mints = run_conn.execute(
                """
                SELECT tx_hash, log_index, block_number, to_address, token_id
                FROM nft_mints
                ORDER BY block_number ASC, tx_hash ASC, log_index ASC
                """
            ).fetchall()

        wallet_activity = []
        if _table_exists(run_conn, "wallet_activity"):
            wallet_activity = run_conn.execute(
                """
                SELECT address, first_swap_day, first_buy_day, buy_count, sell_count, token_bought_raw
                FROM wallet_activity
                ORDER BY address ASC
                """
            ).fetchall()

        run_wallets = []
        if _table_exists(run_conn, "run_wallets"):
            run_wallets = run_conn.execute(
                "SELECT address FROM run_wallets ORDER BY address ASC"
            ).fetchall()

        wallet_cohorts = []
        if _table_exists(run_conn, "wallet_cohorts"):
            wallet_cohorts = run_conn.execute(
                "SELECT address, bucket, eligible FROM wallet_cohorts ORDER BY address ASC"
            ).fetchall()

        reward_wallets = []
        if _table_exists(run_conn, "reward_wallets"):
            reward_wallets = run_conn.execute(
                """
                SELECT wallet, cumulative_buys_raw, cohort_eligible, threshold_reached, minted_cache, minted_onchain, status
                FROM reward_wallets
                ORDER BY wallet ASC
                """
            ).fetchall()

        fair_values = []
        if _table_exists(run_conn, "fair_value_daily"):
            fair_values = run_conn.execute(
                "SELECT day, fair_value FROM fair_value_daily ORDER BY day ASC"
            ).fetchall()

        perceived_values = []
        if _table_exists(run_conn, "perceived_fair_value_daily"):
            perceived_values = run_conn.execute(
                "SELECT day, avg_perceived_log FROM perceived_fair_value_daily ORDER BY day ASC"
            ).fetchall()

        circulating_supply = []
        if _table_exists(run_conn, "circulating_supply_daily"):
            circulating_supply = run_conn.execute(
                "SELECT day, circulating_supply FROM circulating_supply_daily ORDER BY day ASC"
            ).fetchall()

        run_factors = []
        if _table_exists(run_conn, "run_factors_daily"):
            run_factors = run_conn.execute(
                "SELECT day, sentiment, fair_value, launch_mult, price_norm FROM run_factors_daily ORDER BY day ASC"
            ).fetchall()

        trade_caps = []
        if _table_exists(run_conn, "trade_cap_daily"):
            trade_caps = run_conn.execute(
                "SELECT day, side, trade_count, cap_hits FROM trade_cap_daily ORDER BY day ASC, side ASC"
            ).fetchall()

        cohort_daily = []
        if _table_exists(run_conn, "cohort_daily_stats"):
            cohort_daily = run_conn.execute(
                """
                SELECT day, eligible_wallets, control_wallets, minted_eligible, minted_control, minted_total
                FROM cohort_daily_stats
                ORDER BY day ASC
                """
            ).fetchall()

        wallet_balances = []
        if _table_exists(run_conn, "wallet_balances_daily"):
            wallet_balances = run_conn.execute(
                "SELECT day, address, token_balance_raw FROM wallet_balances_daily ORDER BY day ASC, address ASC"
            ).fetchall()

        run_stats_rows = []
        if _table_exists(run_conn, "run_stats"):
            run_stats_rows = run_conn.execute(
                "SELECT key, value FROM run_stats ORDER BY key ASC"
            ).fetchall()
    finally:
        run_conn.close()

    warehouse_conn = sqlite3.connect(str(warehouse_db))
    try:
        _ensure_warehouse_schema(warehouse_conn)
        _delete_existing(warehouse_conn, meta["run_id"])

        warehouse_conn.execute(
            """
            INSERT INTO runs(run_id, created_at_utc, network, token, pool, weth, run_start_block, run_end_block)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                meta["run_id"],
                meta["created_at_utc"],
                meta["network"],
                meta["token"],
                meta["pool"],
                meta["weth"],
                meta["run_start_block"],
                meta["run_end_block"],
            ),
        )

        if daily_prices:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_daily_prices(
                  run_id,
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
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        meta["run_id"],
                        int(day),
                        int(cnt),
                        float(p),
                        float(n),
                        float(op),
                        float(hp),
                        float(lp),
                        float(cp),
                        float(on),
                        float(hn),
                        float(ln),
                        float(cn),
                        (float(vw) if vw is not None else None),
                        (int(tc) if tc is not None else None),
                        (float(fv) if fv is not None else None),
                    )
                    for (
                        day,
                        cnt,
                        p,
                        n,
                        op,
                        hp,
                        lp,
                        cp,
                        on,
                        hn,
                        ln,
                        cn,
                        vw,
                        tc,
                        fv,
                    ) in daily_prices
                ],
            )

        if daily_market:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_daily_market(run_id, day, swap_count, volume_token_in, volume_weth_in, avg_tick)
                VALUES (?,?,?,?,?,?)
                """,
                [
                    (
                        meta["run_id"],
                        int(day),
                        int(cnt),
                        float(vt),
                        float(vw),
                        float(avg_tick),
                    )
                    for day, cnt, vt, vw, avg_tick in daily_market
                ],
            )

        if agents:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_agents(run_id, agent_id, address, private_key, executor, agent_type)
                VALUES (?,?,?,?,?,?)
                """,
                [
                    (
                        meta["run_id"],
                        int(aid),
                        addr,
                        pk,
                        exec or "",
                        (atype if len(row) == 5 else "retail"),
                    )
                    for row in agents
                    for aid, addr, pk, exec, atype in [row if len(row) == 5 else (*row, "retail")]
                ],
            )

        if trades:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_trades(
                  run_id, id, day, agent_id, side, amount_in_wei, token_in, token_out, tx_hash,
                  status, revert_reason, block_number, gas_used, created_at_utc
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        meta["run_id"],
                        int(tid),
                        int(day),
                        int(agent_id),
                        side,
                        amt,
                        t_in,
                        t_out,
                        txh,
                        status,
                        rr,
                        int(bn) if bn is not None else None,
                        int(gu) if gu is not None else None,
                        created,
                    )
                    for tid, day, agent_id, side, amt, t_in, t_out, txh, status, rr, bn, gu, created in trades
                ],
            )

        if swaps:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_swaps(
                  run_id, id, block_number, tx_hash, log_index, sender, recipient,
                  amount0, amount1, sqrt_price_x96, liquidity, tick
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        meta["run_id"],
                        int(sid),
                        int(bn),
                        txh,
                        int(li),
                        sender,
                        recipient,
                        a0,
                        a1,
                        sp,
                        liq,
                        int(tick),
                    )
                    for sid, bn, txh, li, sender, recipient, a0, a1, sp, liq, tick in swaps
                ],
            )

        if swap_prices:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_swap_prices(
                  run_id, tx_hash, log_index, block_number, sqrt_price_x96, tick, price_weth_per_token, normalized_price
                )
                VALUES (?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        meta["run_id"],
                        txh,
                        int(li),
                        int(bn),
                        sp,
                        int(tick),
                        float(p),
                        float(n),
                    )
                    for txh, li, bn, sp, tick, p, n in swap_prices
                ],
            )

        if nft_mints:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_nft_mints(run_id, tx_hash, log_index, block_number, to_address, token_id)
                VALUES (?,?,?,?,?,?)
                """,
                [
                    (
                        meta["run_id"],
                        txh,
                        int(li),
                        int(bn),
                        to_addr,
                        token_id,
                    )
                    for txh, li, bn, to_addr, token_id in nft_mints
                ],
            )

        if fair_values:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_fair_value_daily(run_id, day, fair_value)
                VALUES (?,?,?)
                """,
                [(meta["run_id"], int(day), float(val)) for day, val in fair_values],
            )

        if perceived_values:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_perceived_fair_value_daily(run_id, day, avg_perceived_log)
                VALUES (?,?,?)
                """,
                [(meta["run_id"], int(day), float(val)) for day, val in perceived_values],
            )

        if circulating_supply:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_circulating_supply_daily(run_id, day, circulating_supply)
                VALUES (?,?,?)
                """,
                [(meta["run_id"], int(day), float(val)) for day, val in circulating_supply],
            )

        if run_factors:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_factors_daily(run_id, day, sentiment, fair_value, launch_mult, price_norm)
                VALUES (?,?,?,?,?,?)
                """,
                [
                    (
                        meta["run_id"],
                        int(day),
                        float(sent),
                        float(fv),
                        float(lm),
                        (float(pn) if pn is not None else None),
                    )
                    for day, sent, fv, lm, pn in run_factors
                ],
            )

        if trade_caps:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_trade_cap_daily(run_id, day, side, trade_count, cap_hits)
                VALUES (?,?,?,?,?)
                """,
                [
                    (meta["run_id"], int(day), side, int(tc), int(ch))
                    for day, side, tc, ch in trade_caps
                ],
            )

        if cohort_daily:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_cohort_daily_stats(
                  run_id, day, eligible_wallets, control_wallets, minted_eligible, minted_control, minted_total
                )
                VALUES (?,?,?,?,?,?,?)
                """,
                [
                    (
                        meta["run_id"],
                        int(day),
                        int(eligible),
                        int(control),
                        int(m_elig),
                        int(m_ctrl),
                        int(m_total),
                    )
                    for day, eligible, control, m_elig, m_ctrl, m_total in cohort_daily
                ],
            )

        if wallet_balances:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_wallet_balances_daily(run_id, day, address, token_balance_raw)
                VALUES (?,?,?,?)
                """,
                [
                    (meta["run_id"], int(day), addr, bal)
                    for day, addr, bal in wallet_balances
                ],
            )

        if wallet_activity:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_wallet_activity(run_id, address, first_swap_day, first_buy_day, buy_count, sell_count, token_bought_raw)
                VALUES (?,?,?,?,?,?,?)
                """,
                [
                    (
                        meta["run_id"],
                        addr,
                        int(fs) if fs is not None else None,
                        int(fb) if fb is not None else None,
                        int(bc) if bc is not None else None,
                        int(sc) if sc is not None else None,
                        tbr,
                    )
                    for addr, fs, fb, bc, sc, tbr in wallet_activity
                ],
            )

        if run_wallets:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_wallets(run_id, address)
                VALUES (?,?)
                """,
                [(meta["run_id"], addr) for (addr,) in run_wallets],
            )

        if wallet_cohorts:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_wallet_cohorts(run_id, address, bucket, eligible)
                VALUES (?,?,?,?)
                """,
                [(meta["run_id"], addr, int(bucket), int(el)) for addr, bucket, el in wallet_cohorts],
            )

        if reward_wallets:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_reward_wallets(
                  run_id, wallet, cumulative_buys_raw, cohort_eligible, threshold_reached, minted_cache, minted_onchain, status
                )
                VALUES (?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        meta["run_id"],
                        wallet,
                        cbr,
                        int(ce),
                        int(tr),
                        int(mc),
                        int(mo),
                        status,
                    )
                    for wallet, cbr, ce, tr, mc, mo, status in reward_wallets
                ],
            )

        if run_stats_rows:
            warehouse_conn.executemany(
                """
                INSERT OR REPLACE INTO run_stats(run_id, key, value)
                VALUES (?,?,?)
                """,
                [(meta["run_id"], key, val) for key, val in run_stats_rows],
            )

        warehouse_conn.execute(
            """
            INSERT INTO run_summary(
              run_id,
              created_at_utc,
              network,
              token,
              pool,
              weth,
              run_start_block,
              run_end_block,
              num_agents,
              num_run_wallets,
              num_wallet_cohorts,
              trade_count,
              mined_trades,
              reverted_trades,
              buy_trades,
              sell_trades,
              swap_events,
              mint_events,
              latest_trade_day,
              anchor_price,
              anchor_day,
              total_volume_token_in,
              total_volume_weth_in,
              price_days,
              market_days
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                meta["run_id"],
                meta["created_at_utc"],
                meta["network"],
                meta["token"],
                meta["pool"],
                meta["weth"],
                meta["run_start_block"],
                meta["run_end_block"],
                summary["num_agents"],
                summary["num_run_wallets"],
                summary["num_wallet_cohorts"],
                summary["trade_count"],
                summary["mined_trades"],
                summary["reverted_trades"],
                summary["buy_trades"],
                summary["sell_trades"],
                summary["swap_events"],
                summary["mint_events"],
                summary["latest_trade_day"],
                summary["anchor_price"],
                summary["anchor_day"],
                summary["total_volume_token_in"],
                summary["total_volume_weth_in"],
                summary["price_days"],
                summary["market_days"],
            ),
        )

        warehouse_conn.commit()
    finally:
        warehouse_conn.close()

    print(f"Appended run {meta['run_id']} into warehouse: {warehouse_db}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-db", dest="run_db", default=None, help="Path to a run sim.db (defaults to latest).")
    parser.add_argument("--warehouse", dest="warehouse", default=None, help="Warehouse db path (defaults to sim/warehouse.db).")
    args = parser.parse_args()

    run_db = Path(args.run_db) if args.run_db else _latest_run_db()
    warehouse = Path(args.warehouse) if args.warehouse else Path(__file__).resolve().parent / "warehouse.db"

    append_to_warehouse(run_db, warehouse)


if __name__ == "__main__":
    main()
