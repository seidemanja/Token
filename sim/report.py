"""
sim/report.py

Reads the cross-run warehouse (sim/warehouse.db) and produces:
- console/text summary per run
- PNG plots for price paths, market volume, and trade/mint activity

Usage:
  python -m sim.report                       # uses sim/warehouse.db, outputs to sim/reports/<timestamp>/
  python -m sim.report --warehouse path/to/warehouse.db --outdir sim/reports/my-run-review
  python -m sim.report --runs 20251228T145504Z,20251226T141200Z

Dependencies: matplotlib (install via `pip install matplotlib` if missing).
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sqlite3
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass
class RunMeta:
    run_id: str
    created_at_utc: str
    network: str
    token: str
    pool: str
    weth: str
    run_start_block: Optional[int]
    run_end_block: Optional[int]


@dataclass
class CohortRunData:
    run_id: str
    max_day: int
    threshold_tokens: float
    wallets_by_cohort: Dict[str, List[str]]
    wallets_by_group: Dict[str, List[str]]
    balances_by_wallet: Dict[str, List[float]]
    buy_counts_by_day: Dict[int, Dict[str, int]]
    buy_tokens_by_day: Dict[int, Dict[str, float]]
    sell_tokens_by_day: Dict[int, Dict[str, float]]
    total_buy_counts: Dict[str, int]
    total_buy_tokens: Dict[str, float]
    threshold_cross_day: Dict[str, Optional[int]]


COHORT_GROUPS: List[Tuple[str, str, str]] = [
    ("eligible_hit_threshold", "eligible & hit threshold", "#2ca02c"),
    ("eligible_not_hit_threshold", "eligible & not hit threshold", "#1f77b4"),
    ("control", "control", "#ff7f0e"),
]


def _cohort_group_label(group_key: str) -> str:
    for key, label, _ in COHORT_GROUPS:
        if key == group_key:
            return label
    return group_key


def _cohort_group_color(group_key: str) -> str:
    for key, _, color in COHORT_GROUPS:
        if key == group_key:
            return color
    return "#444444"


def _connect(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(f"Warehouse DB not found: {path}")
    return sqlite3.connect(str(path))


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return bool(row)


def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return any(str(r[1]) == column for r in rows)


def _load_manifest_for_run(run_id: str) -> Optional[dict]:
    base = Path(__file__).resolve().parent / "out" / run_id / "manifest.json"
    if not base.exists():
        return None
    try:
        return json.loads(base.read_text())
    except Exception:
        return None


def _load_runs(conn: sqlite3.Connection) -> Dict[str, RunMeta]:
    rows = conn.execute(
        """
        SELECT run_id, created_at_utc, network, token, pool, weth, run_start_block, run_end_block
        FROM runs
        ORDER BY created_at_utc ASC
        """
    ).fetchall()
    return {
        r[0]: RunMeta(
            run_id=r[0],
            created_at_utc=r[1],
            network=r[2],
            token=r[3],
            pool=r[4],
            weth=r[5],
            run_start_block=r[6],
            run_end_block=r[7],
        )
        for r in rows
    }


def _load_run_ids_ordered(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        "SELECT run_id FROM runs ORDER BY created_at_utc ASC"
    ).fetchall()
    return [r[0] for r in rows]


def _load_run_summary(conn: sqlite3.Connection, run_id: str) -> Optional[dict]:
    has_weth_total = _table_has_column(conn, "run_summary", "total_volume_weth_total")
    if has_weth_total:
        row = conn.execute(
            """
            SELECT run_id, trade_count, mined_trades, reverted_trades, buy_trades, sell_trades,
                   swap_events, mint_events, anchor_price, anchor_day,
                   total_volume_token_in, total_volume_weth_in, total_volume_weth_total, price_days, market_days
            FROM run_summary WHERE run_id=?
            """,
            (run_id,),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT run_id, trade_count, mined_trades, reverted_trades, buy_trades, sell_trades,
                   swap_events, mint_events, anchor_price, anchor_day,
                   total_volume_token_in, total_volume_weth_in, price_days, market_days
            FROM run_summary WHERE run_id=?
            """,
            (run_id,),
        ).fetchone()
    if not row:
        return None
    if has_weth_total:
        keys = [
            "run_id",
            "trade_count",
            "mined_trades",
            "reverted_trades",
            "buy_trades",
            "sell_trades",
            "swap_events",
            "mint_events",
            "anchor_price",
            "anchor_day",
            "total_volume_token_in",
            "total_volume_weth_in",
            "total_volume_weth_total",
            "price_days",
            "market_days",
        ]
        return dict(zip(keys, row))
    out = dict(
        zip(
            [
                "run_id",
                "trade_count",
                "mined_trades",
                "reverted_trades",
                "buy_trades",
                "sell_trades",
                "swap_events",
                "mint_events",
                "anchor_price",
                "anchor_day",
                "total_volume_token_in",
                "total_volume_weth_in",
                "price_days",
                "market_days",
            ],
            row,
        )
    )
    out["total_volume_weth_total"] = out.get("total_volume_weth_in")
    return out


def _load_daily_prices(conn: sqlite3.Connection, run_id: str) -> List[dict]:
    if not _table_exists(conn, "run_daily_prices"):
        return []
    has_close = _table_has_column(conn, "run_daily_prices", "close_normalized_price")
    if has_close:
        rows = conn.execute(
            """
            SELECT day, swap_count, avg_price_weth_per_token, avg_normalized_price,
                   open_normalized_price, high_normalized_price, low_normalized_price, close_normalized_price
            FROM run_daily_prices
            WHERE run_id=?
            ORDER BY day ASC
            """,
            (run_id,),
        ).fetchall()
        return [
            {
                "day": int(day),
                "swap_count": int(cnt),
                "avg_price_weth_per_token": float(p),
                "avg_normalized_price": float(n),
                "open_normalized_price": float(op),
                "high_normalized_price": float(hp),
                "low_normalized_price": float(lp),
                "close_normalized_price": float(cp),
            }
            for day, cnt, p, n, op, hp, lp, cp in rows
        ]
    rows = conn.execute(
        """
        SELECT day, swap_count, avg_price_weth_per_token, avg_normalized_price
        FROM run_daily_prices
        WHERE run_id=?
        ORDER BY day ASC
        """,
        (run_id,),
    ).fetchall()
    return [
        {
            "day": int(day),
            "swap_count": int(cnt),
            "avg_price_weth_per_token": float(p),
            "avg_normalized_price": float(n),
        }
        for day, cnt, p, n in rows
    ]


def _load_run_stats(conn: sqlite3.Connection, run_id: str) -> Dict[str, str]:
    if not _table_exists(conn, "run_stats"):
        return {}
    rows = conn.execute(
        """
        SELECT key, value
        FROM run_stats
        WHERE run_id=?
        """,
        (run_id,),
    ).fetchall()
    return {str(k): str(v) for k, v in rows}


def _get_sim_max_day(conn: sqlite3.Connection, run_id: str) -> Optional[int]:
    """
    Return the maximum simulation day available for a run from any day-indexed table.
    """
    day_tables = [
        "run_factors_daily",
        "run_fair_value_daily",
        "run_circulating_supply_daily",
        "run_wallet_balances_daily",
        "run_trades",
    ]
    max_days: List[int] = []
    for table in day_tables:
        if not _table_exists(conn, table):
            continue
        row = conn.execute(
            f"SELECT MAX(day) FROM {table} WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if row and row[0] is not None:
            max_days.append(int(row[0]))
    if not max_days:
        return None
    return max(max_days)


def _load_daily_close_prices(conn: sqlite3.Connection, run_id: str) -> List[dict]:
    """
    Compute daily close prices from swap prices + run_stats day0_block/blocks_per_day.
    Falls back to daily average prices if needed.
    """
    if _table_has_column(conn, "run_daily_prices", "close_normalized_price"):
        rows = conn.execute(
            """
            SELECT day, close_normalized_price
            FROM run_daily_prices
            WHERE run_id=?
            ORDER BY day ASC
            """,
            (run_id,),
        ).fetchall()
        sparse = [{"day": int(day), "avg_normalized_price": float(price)} for day, price in rows]
    elif not _table_exists(conn, "run_swap_prices"):
        sparse = _load_daily_prices(conn, run_id)
    else:
        stats = _load_run_stats(conn, run_id)
        day0_block = int(stats.get("day0_block", "0") or 0)
        blocks_per_day = int(stats.get("blocks_per_day", "100") or 100)
        if day0_block <= 0 or blocks_per_day <= 0:
            sparse = _load_daily_prices(conn, run_id)
        else:
            rows = conn.execute(
                """
                SELECT block_number, normalized_price
                FROM run_swap_prices
                WHERE run_id=?
                ORDER BY block_number ASC
                """,
                (run_id,),
            ).fetchall()
            if not rows:
                sparse = _load_daily_prices(conn, run_id)
            else:
                closes: Dict[int, float] = {}
                for block_number, price in rows:
                    day = int((int(block_number) - day0_block) // blocks_per_day)
                    closes[day] = float(price)
                sparse = [{"day": day, "avg_normalized_price": price} for day, price in sorted(closes.items())]

    if not sparse:
        return sparse

    sim_max_day = _get_sim_max_day(conn, run_id)
    if sim_max_day is None:
        return sparse
    if int(sparse[-1]["day"]) >= sim_max_day:
        return sparse

    by_day = {int(r["day"]): float(r["avg_normalized_price"]) for r in sparse}
    first_price = float(sparse[0]["avg_normalized_price"])
    dense: List[dict] = []
    prev_price = first_price
    for day in range(sim_max_day + 1):
        price = by_day.get(day)
        if price is not None:
            prev_price = float(price)
        dense.append({"day": int(day), "avg_normalized_price": float(prev_price)})
    return dense


def _load_daily_market(conn: sqlite3.Connection, run_id: str) -> List[dict]:
    if not _table_exists(conn, "run_daily_market"):
        return []
    has_weth_total = _table_has_column(conn, "run_daily_market", "volume_weth_total")
    if has_weth_total:
        rows = conn.execute(
            """
            SELECT day, swap_count, volume_token_in, volume_weth_in, volume_weth_total, avg_tick
            FROM run_daily_market
            WHERE run_id=?
            ORDER BY day ASC
            """,
            (run_id,),
        ).fetchall()
        sparse = [
            {
                "day": int(day),
                "swap_count": int(cnt),
                "volume_token_in": float(vt),
                "volume_weth_in": float(vw),
                "volume_weth_total": float(vwt),
                "avg_tick": float(tick),
            }
            for day, cnt, vt, vw, vwt, tick in rows
        ]
    else:
        rows = conn.execute(
            """
            SELECT day, swap_count, volume_token_in, volume_weth_in, avg_tick
            FROM run_daily_market
            WHERE run_id=?
            ORDER BY day ASC
            """,
            (run_id,),
        ).fetchall()
        sparse = [
            {
                "day": int(day),
                "swap_count": int(cnt),
                "volume_token_in": float(vt),
                "volume_weth_in": float(vw),
                "volume_weth_total": float(vw),
                "avg_tick": float(tick),
            }
            for day, cnt, vt, vw, tick in rows
        ]
    if not sparse:
        return sparse

    sim_max_day = _get_sim_max_day(conn, run_id)
    if sim_max_day is None:
        return sparse
    if int(sparse[-1]["day"]) >= sim_max_day:
        return sparse

    by_day = {int(r["day"]): r for r in sparse}
    dense: List[dict] = []
    for day in range(sim_max_day + 1):
        r = by_day.get(day)
        if r is None:
            dense.append(
                {
                    "day": int(day),
                    "swap_count": 0,
                    "volume_token_in": 0.0,
                    "volume_weth_in": 0.0,
                    "volume_weth_total": 0.0,
                    "avg_tick": 0.0,
                }
            )
        else:
            dense.append(r)
    return dense


def _load_daily_returns(conn: sqlite3.Connection, run_id: str) -> List[dict]:
    closes = _load_daily_close_prices(conn, run_id)
    rows = [(int(r["day"]), float(r["avg_normalized_price"])) for r in closes]
    if len(rows) < 2:
        return []
    # Drop the last day to avoid partial-day artifacts at run boundaries.
    rows = rows[:-1]
    returns = []
    prev_price = float(rows[0][1])
    for day, price in rows[1:]:
        price_f = float(price)
        ret = (price_f / prev_price) - 1.0 if prev_price > 0 else 0.0
        returns.append({"day": int(day), "return": ret})
        prev_price = price_f
    return returns


def _load_trade_sizes(conn: sqlite3.Connection) -> Dict[str, List[float]]:
    """
    Return trade sizes (in token units) for BUY and SELL across all runs.
    """
    if not _table_exists(conn, "run_trades"):
        return {"BUY": [], "SELL": []}
    rows = conn.execute(
        """
        SELECT t.side, t.amount_in_wei, p.avg_price_weth_per_token
        FROM run_trades t
        LEFT JOIN run_daily_prices p
          ON p.run_id = t.run_id AND p.day = t.day
        WHERE t.status='MINED'
        """
    ).fetchall()
    sizes = {"BUY": [], "SELL": []}
    for side, amt, avg_price in rows:
        try:
            amt_in = float(int(amt)) / 1e18
            if amt_in <= 0:
                continue
            side_str = str(side)
            if side_str == "BUY":
                price = float(avg_price) if avg_price is not None else 0.0
                if price <= 0:
                    continue
                sizes[side_str].append(amt_in / price)
            else:
                sizes[side_str].append(amt_in)
        except Exception:
            continue
    return sizes


def _load_fair_values(conn: sqlite3.Connection, run_id: str) -> List[dict]:
    if _table_exists(conn, "run_factors_daily"):
        rows = conn.execute(
            """
            SELECT day, fair_value
            FROM run_factors_daily
            WHERE run_id=?
            ORDER BY day ASC
            """,
            (run_id,),
        ).fetchall()
    elif _table_exists(conn, "run_fair_value_daily"):
        rows = conn.execute(
            """
            SELECT day, fair_value
            FROM run_fair_value_daily
            WHERE run_id=?
            ORDER BY day ASC
            """,
            (run_id,),
        ).fetchall()
    else:
        rows = []
    return [{"day": int(day), "fair_value": float(val)} for day, val in rows]


def _load_regime_trace(conn: sqlite3.Connection, run_id: str) -> List[dict]:
    """
    Load regime trace.
    Preferred path uses exact stored regime_code from run_factors_daily:
      0=bear, 1=bull, 2=hype
    Fallback for older runs infers from launch_mult/sentiment.
    """
    if not _table_exists(conn, "run_factors_daily"):
        return []
    has_regime_code = _table_has_column(conn, "run_factors_daily", "regime_code")
    if has_regime_code:
        rows = conn.execute(
            """
            SELECT day, sentiment, regime_code, launch_mult
            FROM run_factors_daily
            WHERE run_id=?
            ORDER BY day ASC
            """,
            (run_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT day, sentiment, NULL as regime_code, launch_mult
            FROM run_factors_daily
            WHERE run_id=?
            ORDER BY day ASC
            """,
            (run_id,),
        ).fetchall()
    out: List[dict] = []
    for day, sentiment, regime_code_raw, launch_mult in rows:
        sent = float(sentiment)
        if regime_code_raw is not None:
            regime_code = int(regime_code_raw)
            if regime_code == 2:
                regime = "hype"
            elif regime_code == 1:
                regime = "bull"
            else:
                regime = "bear"
        else:
            # Backward-compatible fallback for legacy runs.
            hype_flag = float(launch_mult)
            if hype_flag >= 0.5:
                regime = "hype"
                regime_code = 2
            elif sent >= 0.0:
                regime = "bull"
                regime_code = 1
            else:
                regime = "bear"
                regime_code = 0
        out.append(
            {
                "day": int(day),
                "regime": regime,
                "regime_code": int(regime_code),
                "sentiment": sent,
            }
        )
    return out


def _load_perceived_fair_values(conn: sqlite3.Connection, run_id: str) -> List[dict]:
    if _table_exists(conn, "run_perceived_fair_value_daily"):
        rows = conn.execute(
            """
            SELECT day, avg_perceived_log
            FROM run_perceived_fair_value_daily
            WHERE run_id=?
            ORDER BY day ASC
            """,
            (run_id,),
        ).fetchall()
    else:
        rows = []
    return [{"day": int(day), "avg_perceived_log": float(val)} for day, val in rows]


def _load_wallet_balances(conn: sqlite3.Connection, run_id: str) -> Dict[int, List[int]]:
    """
    Returns {day: [balances]} for holder count + concentration.
    """
    if not _table_exists(conn, "run_wallet_balances_daily"):
        return {}
    rows = conn.execute(
        """
        SELECT day, token_balance_raw
        FROM run_wallet_balances_daily
        WHERE run_id=?
        ORDER BY day ASC
        """,
        (run_id,),
    ).fetchall()
    by_day: Dict[int, List[int]] = {}
    for day, bal in rows:
        by_day.setdefault(int(day), []).append(int(bal))
    return by_day


def _load_swap_ticks_prices(conn: sqlite3.Connection, run_id: str) -> List[tuple]:
    if not _table_exists(conn, "run_swap_prices"):
        return []
    return conn.execute(
        """
        SELECT tick, normalized_price
        FROM run_swap_prices
        WHERE run_id=?
        ORDER BY block_number ASC
        """,
        (run_id,),
    ).fetchall()


def _load_liquidity_series(conn: sqlite3.Connection, run_id: str) -> List[tuple]:
    if not _table_exists(conn, "run_swaps"):
        return []
    return conn.execute(
        """
        SELECT block_number, liquidity, tick
        FROM run_swaps
        WHERE run_id=?
        ORDER BY block_number ASC
        """,
        (run_id,),
    ).fetchall()


def _load_repeat_buy_rates(conn: sqlite3.Connection) -> Dict[str, dict]:
    """
    Compute repeat-buy rates (buy_count >= 2) by cohort eligibility.
    Returns {run_id: {"eligible_rate": r, "eligible_n": n, "control_rate": r, "control_n": n}}.
    """
    if not _table_exists(conn, "run_wallet_activity") or not _table_exists(conn, "run_wallet_cohorts"):
        return {}
    rows = conn.execute(
        """
        SELECT a.run_id, c.eligible, a.buy_count
        FROM run_wallet_activity a
        JOIN run_wallet_cohorts c
          ON a.run_id = c.run_id AND a.address = c.address
        """
    ).fetchall()
    by_run: Dict[str, Dict[str, List[bool]]] = {}
    for run_id, eligible, buy_count in rows:
        key = "eligible" if int(eligible) == 1 else "control"
        by_run.setdefault(run_id, {"eligible": [], "control": []})
        by_run[run_id][key].append(int(buy_count or 0) >= 2)

    rates = {}
    for run_id, buckets in by_run.items():
        rates[run_id] = {}
        for key in ["eligible", "control"]:
            vals = buckets.get(key, [])
            n = len(vals)
            rate = (sum(1 for v in vals if v) / n) if n else 0.0
            rates[run_id][f"{key}_rate"] = rate
            rates[run_id][f"{key}_n"] = n
    return rates


def _to_token_units(raw_wei: object) -> float:
    try:
        return float(int(str(raw_wei))) / 1e18
    except Exception:
        return 0.0


def _binomial_ci(successes: int, n: int, z: float = 1.96) -> Tuple[float, float]:
    if n <= 0:
        return 0.0, 0.0
    p = max(0.0, min(1.0, float(successes) / float(n)))
    denom = 1.0 + (z * z) / n
    center = (p + (z * z) / (2.0 * n)) / denom
    margin = (z * math.sqrt((p * (1.0 - p) + (z * z) / (4.0 * n)) / n)) / denom
    lo = max(0.0, center - margin)
    hi = min(1.0, center + margin)
    return lo, hi


def _mean_ci(vals: List[float], z: float = 1.96) -> Tuple[float, float, float]:
    if not vals:
        return 0.0, 0.0, 0.0
    n = len(vals)
    mean_v = float(sum(vals)) / float(n)
    if n < 2:
        return mean_v, mean_v, mean_v
    std_v = statistics.pstdev(vals)
    se = std_v / math.sqrt(float(n))
    lo = mean_v - (z * se)
    hi = mean_v + (z * se)
    return mean_v, lo, hi


def _median_ci(vals: List[float], n_boot: int = 250) -> Tuple[float, float, float]:
    if not vals:
        return 0.0, 0.0, 0.0
    med = float(statistics.median(vals))
    if len(vals) < 2:
        return med, med, med
    rng = random.Random(42 + len(vals))
    boots: List[float] = []
    n = len(vals)
    for _ in range(max(50, n_boot)):
        sample = [vals[rng.randrange(n)] for _ in range(n)]
        boots.append(float(statistics.median(sample)))
    boots.sort()
    lo_idx = max(0, int(0.025 * len(boots)))
    hi_idx = min(len(boots) - 1, int(0.975 * len(boots)))
    return med, boots[lo_idx], boots[hi_idx]


def _load_cohort_run_data(conn: sqlite3.Connection, run_id: str) -> Optional[CohortRunData]:
    required_tables = ["run_wallet_cohorts", "run_wallet_balances_daily", "run_agents", "run_trades"]
    for t in required_tables:
        if not _table_exists(conn, t):
            return None

    cohort_rows = conn.execute(
        """
        SELECT address, eligible
        FROM run_wallet_cohorts
        WHERE run_id=?
        ORDER BY address ASC
        """,
        (run_id,),
    ).fetchall()
    if not cohort_rows:
        return None

    eligible_wallets: List[str] = []
    control_wallets: List[str] = []
    eligible_by_addr: Dict[str, int] = {}
    for addr, eligible in cohort_rows:
        a = str(addr).lower()
        e = int(eligible or 0)
        eligible_by_addr[a] = e
        if e == 1:
            eligible_wallets.append(a)
        else:
            control_wallets.append(a)

    balances_rows = conn.execute(
        """
        SELECT day, address, token_balance_raw
        FROM run_wallet_balances_daily
        WHERE run_id=?
        ORDER BY day ASC, address ASC
        """,
        (run_id,),
    ).fetchall()
    max_day = _get_sim_max_day(conn, run_id) or 0
    if balances_rows:
        max_day = max(max_day, max(int(r[0]) for r in balances_rows))

    balances_by_wallet: Dict[str, List[Optional[float]]] = {
        w: [None] * (max_day + 1) for w in eligible_by_addr.keys()
    }
    for day, addr, bal_raw in balances_rows:
        a = str(addr).lower()
        if a not in balances_by_wallet:
            continue
        d = int(day)
        if 0 <= d <= max_day:
            balances_by_wallet[a][d] = _to_token_units(bal_raw)

    for w, series in balances_by_wallet.items():
        prev = 0.0
        for i in range(len(series)):
            if series[i] is None:
                series[i] = prev
            else:
                prev = float(series[i])
        balances_by_wallet[w] = [float(x or 0.0) for x in series]

    price_by_day: Dict[int, float] = {}
    if _table_exists(conn, "run_daily_prices"):
        for day, avg_price in conn.execute(
            """
            SELECT day, avg_price_weth_per_token
            FROM run_daily_prices
            WHERE run_id=?
            """,
            (run_id,),
        ).fetchall():
            try:
                price_by_day[int(day)] = float(avg_price)
            except Exception:
                pass

    buy_counts_by_day: Dict[int, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    buy_tokens_by_day: Dict[int, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    sell_tokens_by_day: Dict[int, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    total_buy_counts: Dict[str, int] = {w: 0 for w in eligible_by_addr.keys()}
    total_buy_tokens: Dict[str, float] = {w: 0.0 for w in eligible_by_addr.keys()}

    trade_rows = conn.execute(
        """
        SELECT t.day, LOWER(a.address), t.side, t.amount_in_wei
        FROM run_trades t
        JOIN run_agents a
          ON t.run_id = a.run_id AND t.agent_id = a.agent_id
        WHERE t.run_id=?
          AND t.status IN ('MINED', 'AGG_INTENT')
        ORDER BY t.day ASC, t.id ASC
        """,
        (run_id,),
    ).fetchall()

    for day, addr, side, amount_in_wei in trade_rows:
        a = str(addr).lower()
        if a not in eligible_by_addr:
            continue
        d = int(day)
        if d < 0:
            continue
        amount = _to_token_units(amount_in_wei)
        side_u = str(side).upper()
        if side_u == "BUY":
            px = float(price_by_day.get(d, 0.0))
            bought_tokens = (amount / px) if px > 0 else 0.0
            buy_counts_by_day[d][a] += 1
            buy_tokens_by_day[d][a] += bought_tokens
            total_buy_counts[a] += 1
            total_buy_tokens[a] += bought_tokens
        elif side_u == "SELL":
            sell_tokens_by_day[d][a] += amount

    manifest = _load_manifest_for_run(run_id) or {}
    try:
        threshold_tokens = max(0.0, float(manifest.get("nft_threshold_tokens", 0.00015)))
    except Exception:
        threshold_tokens = 0.00015

    threshold_cross_day: Dict[str, Optional[int]] = {}
    for a in eligible_by_addr.keys():
        if threshold_tokens <= 0:
            threshold_cross_day[a] = None
            continue
        series = balances_by_wallet.get(a, [])
        cross_day: Optional[int] = None
        for d, held in enumerate(series):
            if float(held) >= threshold_tokens:
                cross_day = d
                break
        threshold_cross_day[a] = cross_day

    eligible_hit_wallets = [w for w in eligible_wallets if threshold_cross_day.get(w) is not None]
    eligible_not_hit_wallets = [w for w in eligible_wallets if threshold_cross_day.get(w) is None]
    wallets_by_group = {
        "eligible_hit_threshold": eligible_hit_wallets,
        "eligible_not_hit_threshold": eligible_not_hit_wallets,
        "control": control_wallets,
    }

    return CohortRunData(
        run_id=run_id,
        max_day=max_day,
        threshold_tokens=threshold_tokens,
        wallets_by_cohort={"eligible": eligible_wallets, "control": control_wallets},
        wallets_by_group=wallets_by_group,
        balances_by_wallet={k: [float(v) for v in vals] for k, vals in balances_by_wallet.items()},
        buy_counts_by_day={int(k): dict(v) for k, v in buy_counts_by_day.items()},
        buy_tokens_by_day={int(k): dict(v) for k, v in buy_tokens_by_day.items()},
        sell_tokens_by_day={int(k): dict(v) for k, v in sell_tokens_by_day.items()},
        total_buy_counts=total_buy_counts,
        total_buy_tokens=total_buy_tokens,
        threshold_cross_day=threshold_cross_day,
    )


def _load_cohort_analytics(conn: sqlite3.Connection, run_ids: List[str]) -> Dict[str, CohortRunData]:
    out: Dict[str, CohortRunData] = {}
    for run_id in run_ids:
        data = _load_cohort_run_data(conn, run_id)
        if data is not None:
            out[run_id] = data
    return out


def _plot_repeat_buy_rate_rolling7(outdir: Path, cohort_data: Dict[str, CohortRunData], window: int = 7) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok or not cohort_data:
        return None
    max_day = max(d.max_day for d in cohort_data.values())
    days = list(range(0, max_day + 1))
    fig, ax = plt.subplots(figsize=(10, 6))
    for group_key, group_label, color in COHORT_GROUPS:
        median_rate: List[float] = []
        lo: List[float] = []
        hi: List[float] = []
        group_total_n = sum(len(d.wallets_by_group.get(group_key, [])) for d in cohort_data.values())
        for day in days:
            run_rates: List[float] = []
            for d in cohort_data.values():
                wallets = d.wallets_by_group.get(group_key, [])
                n = len(wallets)
                if n <= 0:
                    continue
                start = max(0, day - window + 1)
                successes = 0
                for w in wallets:
                    window_buys = 0
                    for k in range(start, day + 1):
                        window_buys += int(d.buy_counts_by_day.get(k, {}).get(w, 0))
                    if window_buys >= 2:
                        successes += 1
                run_rates.append(float(successes) / float(n))
            med, ci_lo, ci_hi = _median_ci(run_rates)
            median_rate.append(med)
            lo.append(ci_lo)
            hi.append(ci_hi)

        ax.plot(days, median_rate, color=color, label=f"{group_label} (N={group_total_n})")
        ax.fill_between(days, lo, hi, color=color, alpha=0.18)

    ax.set_ylim(0.0, 1.0)
    ax.set_title("Rolling 7-day median repeat-buy rate by static cohort (buy_count >= 2)")
    ax.set_xlabel("Day (sim)")
    ax.set_ylabel("Median repeat-buy rate across runs")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    out = outdir / "cohort_repeat_buy_rate_rolling7.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_retention_curve(outdir: Path, cohort_data: Dict[str, CohortRunData]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok or not cohort_data:
        return None
    max_day = max(d.max_day for d in cohort_data.values())
    days = list(range(0, max_day + 1))
    fig, ax = plt.subplots(figsize=(10, 6))
    for group_key, group_label, color in COHORT_GROUPS:
        rate: List[float] = []
        lo: List[float] = []
        hi: List[float] = []
        group_total_n = sum(len(d.wallets_by_group.get(group_key, [])) for d in cohort_data.values())
        for day in days:
            run_rates: List[float] = []
            for d in cohort_data.values():
                wallets = d.wallets_by_group.get(group_key, [])
                if not wallets:
                    continue
                active = 0
                n = 0
                for w in wallets:
                    series = d.balances_by_wallet.get(w, [])
                    if day < len(series):
                        n += 1
                        if float(series[day]) > 0.0:
                            active += 1
                if n > 0:
                    run_rates.append(float(active) / float(n))
            med, ci_lo, ci_hi = _median_ci(run_rates)
            rate.append(med)
            lo.append(ci_lo)
            hi.append(ci_hi)
        ax.plot(days, rate, color=color, label=f"{group_label} (N={group_total_n})")
        ax.fill_between(days, lo, hi, color=color, alpha=0.18)

    ax.set_ylim(0.0, 1.0)
    ax.set_title("Median retention / survival by static cohort (active = token balance > 0)")
    ax.set_xlabel("Day (sim)")
    ax.set_ylabel("Median active wallet share across runs")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    out = outdir / "cohort_retention_curve.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_buy_intensity_distributions(outdir: Path, cohort_data: Dict[str, CohortRunData]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok or not cohort_data:
        return None

    group_keys = [key for key, _, _ in COHORT_GROUPS]
    buys: Dict[str, List[float]] = {k: [] for k in group_keys}
    tokens: Dict[str, List[float]] = {k: [] for k in group_keys}
    for d in cohort_data.values():
        for group_key, _, _ in COHORT_GROUPS:
            for w in d.wallets_by_group.get(group_key, []):
                buys[group_key].append(float(d.total_buy_counts.get(w, 0)))
                tokens[group_key].append(float(d.total_buy_tokens.get(w, 0.0)))

    if not any(buys[k] for k in group_keys):
        return None

    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    bins_buys = 20
    bins_tokens = 24

    for group_key, group_label, color in COHORT_GROUPS:
        if buys[group_key]:
            axes[0][0].hist(
                buys[group_key],
                bins=bins_buys,
                alpha=0.35,
                label=f"{group_label} (N={len(buys[group_key])})",
                color=color,
            )
            xs = sorted(buys[group_key])
            ys = [(i + 1) / len(xs) for i in range(len(xs))]
            axes[0][1].plot(xs, ys, label=f"{group_label} (N={len(xs)})", color=color)
        if tokens[group_key]:
            axes[1][0].hist(
                tokens[group_key],
                bins=bins_tokens,
                alpha=0.35,
                label=f"{group_label} (N={len(tokens[group_key])})",
                color=color,
            )
            xs_t = sorted(tokens[group_key])
            ys_t = [(i + 1) / len(xs_t) for i in range(len(xs_t))]
            axes[1][1].plot(xs_t, ys_t, label=f"{group_label} (N={len(xs_t)})", color=color)

    for group_key, group_label, color in COHORT_GROUPS:
        if buys[group_key]:
            med, lo, hi = _median_ci(buys[group_key])
            axes[0][0].axvline(med, linestyle="--", linewidth=1.2, color=color)
            axes[0][0].text(
                med,
                axes[0][0].get_ylim()[1] * (0.92 - 0.08 * COHORT_GROUPS.index((group_key, group_label, color))),
                f"{group_label} med={med:.2f}\n95% CI [{lo:.2f},{hi:.2f}]",
                color=color,
                fontsize=8,
            )
        if tokens[group_key]:
            med_t, lo_t, hi_t = _median_ci(tokens[group_key])
            axes[1][0].axvline(med_t, linestyle="--", linewidth=1.2, color=color)
            axes[1][0].text(
                med_t,
                axes[1][0].get_ylim()[1] * (0.92 - 0.08 * COHORT_GROUPS.index((group_key, group_label, color))),
                f"{group_label} med={med_t:.4f}\n95% CI [{lo_t:.4f},{hi_t:.4f}]",
                color=color,
                fontsize=8,
            )

    axes[0][0].set_title("Buys per wallet (histogram)")
    axes[0][0].set_xlabel("Buy count")
    axes[0][0].set_ylabel("Wallet frequency")
    axes[0][0].grid(True, linestyle="--", alpha=0.3)
    axes[0][0].legend()

    axes[0][1].set_title("Buys per wallet (CDF)")
    axes[0][1].set_xlabel("Buy count")
    axes[0][1].set_ylabel("CDF")
    axes[0][1].grid(True, linestyle="--", alpha=0.3)
    axes[0][1].legend()

    axes[1][0].set_title("Token bought per wallet (histogram)")
    axes[1][0].set_xlabel("Token bought")
    axes[1][0].set_ylabel("Wallet frequency")
    axes[1][0].grid(True, linestyle="--", alpha=0.3)
    axes[1][0].legend()

    axes[1][1].set_title("Token bought per wallet (CDF)")
    axes[1][1].set_xlabel("Token bought")
    axes[1][1].set_ylabel("CDF")
    axes[1][1].grid(True, linestyle="--", alpha=0.3)
    axes[1][1].legend()

    out = outdir / "cohort_buy_intensity_distributions.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_net_flow_median_by_cohort(outdir: Path, cohort_data: Dict[str, CohortRunData]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok or not cohort_data:
        return None
    max_day = max(d.max_day for d in cohort_data.values())
    if max_day <= 0:
        return None
    days = list(range(1, max_day + 1))
    fig, ax = plt.subplots(figsize=(10, 6))

    for group_key, group_label, color in COHORT_GROUPS:
        med_series: List[float] = []
        lo_series: List[float] = []
        hi_series: List[float] = []
        cohort_n = sum(len(d.wallets_by_group.get(group_key, [])) for d in cohort_data.values())
        for day in days:
            vals: List[float] = []
            for d in cohort_data.values():
                for w in d.wallets_by_group.get(group_key, []):
                    s = d.balances_by_wallet.get(w, [])
                    if day < len(s):
                        vals.append(float(s[day]) - float(s[day - 1]))
            med, lo, hi = _median_ci(vals)
            med_series.append(med)
            lo_series.append(lo)
            hi_series.append(hi)
        ax.plot(days, med_series, color=color, label=f"{group_label} (N={cohort_n})")
        ax.fill_between(days, lo_series, hi_series, color=color, alpha=0.18)

    ax.axhline(0.0, color="#666666", linestyle="--", linewidth=1.0)
    ax.set_title("Daily median net token flow per wallet by cohort (balance delta)")
    ax.set_xlabel("Day (sim)")
    ax.set_ylabel("Median token flow (buy - sell proxy)")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    out = outdir / "cohort_net_flow_median_timeseries.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_median_holdings_bar(outdir: Path, cohort_data: Dict[str, CohortRunData]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok or not cohort_data:
        return None

    group_keys = [key for key, _, _ in COHORT_GROUPS]
    final_vals = {k: [] for k in group_keys}
    for d in cohort_data.values():
        for group_key, _, _ in COHORT_GROUPS:
            for w in d.wallets_by_group.get(group_key, []):
                s = d.balances_by_wallet.get(w, [])
                if s:
                    final_vals[group_key].append(float(s[-1]))

    if not any(final_vals[k] for k in group_keys):
        return None

    cohorts = [k for k, _, _ in COHORT_GROUPS]
    meds: List[float] = []
    lo_err: List[float] = []
    hi_err: List[float] = []
    labels: List[str] = []
    for c in cohorts:
        med, lo, hi = _median_ci(final_vals[c])
        meds.append(med)
        lo_err.append(max(0.0, med - lo))
        hi_err.append(max(0.0, hi - med))
        labels.append(f"{_cohort_group_label(c)}\nN={len(final_vals[c])}")

    fig, ax = plt.subplots(figsize=(11, 6))
    x = list(range(len(cohorts)))
    colors = [_cohort_group_color(c) for c in cohorts]
    ax.bar(x, meds, color=colors, alpha=0.85)
    ax.errorbar(x, meds, yerr=[lo_err, hi_err], fmt="none", ecolor="black", capsize=5)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Median token held per wallet")
    ax.set_title("Median token held per wallet by cohort (final day, 95% CI)")
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    out = outdir / "cohort_median_token_held_bar.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_median_holdings_timeseries(outdir: Path, cohort_data: Dict[str, CohortRunData]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok or not cohort_data:
        return None
    max_day = max(d.max_day for d in cohort_data.values())
    days = list(range(0, max_day + 1))
    fig, ax = plt.subplots(figsize=(10, 6))
    for group_key, group_label, color in COHORT_GROUPS:
        med_series: List[float] = []
        lo_series: List[float] = []
        hi_series: List[float] = []
        cohort_n = sum(len(d.wallets_by_group.get(group_key, [])) for d in cohort_data.values())
        for day in days:
            vals: List[float] = []
            for d in cohort_data.values():
                for w in d.wallets_by_group.get(group_key, []):
                    s = d.balances_by_wallet.get(w, [])
                    if day < len(s):
                        vals.append(float(s[day]))
            med, lo, hi = _median_ci(vals)
            med_series.append(med)
            lo_series.append(lo)
            hi_series.append(hi)
        ax.plot(days, med_series, color=color, label=f"{group_label} (N={cohort_n})")
        ax.fill_between(days, lo_series, hi_series, color=color, alpha=0.18)
    ax.set_title("Median token held per wallet over time by static cohort")
    ax.set_xlabel("Day (sim)")
    ax.set_ylabel("Median token held")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    out = outdir / "cohort_median_token_held_timeseries.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_avg_holdings_prepost_control(outdir: Path, cohort_data: Dict[str, CohortRunData]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok or not cohort_data:
        return None
    max_day = max(d.max_day for d in cohort_data.values())
    days = list(range(0, max_day + 1))
    fig, ax = plt.subplots(figsize=(10, 6))
    series_map = {g[0]: {"median": [], "lo": [], "hi": [], "n": 0} for g in COHORT_GROUPS}

    for day in days:
        vals_by_group: Dict[str, List[float]] = {k: [] for k, _, _ in COHORT_GROUPS}
        for d in cohort_data.values():
            for group_key, _, _ in COHORT_GROUPS:
                for w in d.wallets_by_group.get(group_key, []):
                    s = d.balances_by_wallet.get(w, [])
                    if day < len(s):
                        vals_by_group[group_key].append(float(s[day]))

        for g, _, _ in COHORT_GROUPS:
            m, lo, hi = _median_ci(vals_by_group[g])
            series_map[g]["median"].append(m)
            series_map[g]["lo"].append(lo)
            series_map[g]["hi"].append(hi)
            series_map[g]["n"] = max(series_map[g]["n"], len(vals_by_group[g]))

    for g, label, color in COHORT_GROUPS:
        ax.plot(days, series_map[g]["median"], color=color, label=f"{label} (N~{series_map[g]['n']})")
        ax.fill_between(days, series_map[g]["lo"], series_map[g]["hi"], color=color, alpha=0.16)

    ax.set_title("Median token held: static cohort split (smoothed view)")
    ax.set_xlabel("Day (sim)")
    ax.set_ylabel("Median token held")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    out = outdir / "cohort_avg_token_held_prepost_control.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_threshold_event_window(outdir: Path, cohort_data: Dict[str, CohortRunData], window: int = 14) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok or not cohort_data:
        return None

    rel_days = list(range(-window, window + 1))
    vals_by_group: Dict[str, Dict[int, List[float]]] = {
        key: defaultdict(list) for key, _, _ in COHORT_GROUPS
    }
    aligned_counts: Dict[str, int] = {key: 0 for key, _, _ in COHORT_GROUPS}

    global_cross_days: List[int] = []
    for d in cohort_data.values():
        for w in d.wallets_by_group.get("eligible_hit_threshold", []):
            cross = d.threshold_cross_day.get(w)
            if cross is not None:
                global_cross_days.append(int(cross))
    if not global_cross_days:
        return None

    fallback_day = int(statistics.median(global_cross_days))
    rng = random.Random(4242)

    for d in cohort_data.values():
        run_cross_days = [
            int(d.threshold_cross_day[w])
            for w in d.wallets_by_group.get("eligible_hit_threshold", [])
            if d.threshold_cross_day.get(w) is not None
        ]
        for group_key, _, _ in COHORT_GROUPS:
            wallets = d.wallets_by_group.get(group_key, [])
            for w in wallets:
                if group_key == "eligible_hit_threshold":
                    cross = d.threshold_cross_day.get(w)
                    if cross is None:
                        continue
                    align_day = int(cross)
                else:
                    source_days = run_cross_days if run_cross_days else global_cross_days
                    align_day = int(rng.choice(source_days)) if source_days else fallback_day
                s = d.balances_by_wallet.get(w, [])
                if not s:
                    continue
                aligned_counts[group_key] += 1
                for k in rel_days:
                    day = align_day + k
                    if 0 <= day < len(s):
                        vals_by_group[group_key][k].append(float(s[day]))

    fig, ax = plt.subplots(figsize=(10, 6))
    for group_key, group_label, color in COHORT_GROUPS:
        med_s: List[float] = []
        lo_s: List[float] = []
        hi_s: List[float] = []
        for k in rel_days:
            m, lo, hi = _median_ci(vals_by_group[group_key].get(k, []))
            med_s.append(m)
            lo_s.append(lo)
            hi_s.append(hi)
        ax.plot(rel_days, med_s, color=color, label=f"{group_label} (N={aligned_counts[group_key]})")
        ax.fill_between(rel_days, lo_s, hi_s, color=color, alpha=0.16)

    ax.axvline(0, color="#555555", linestyle="--", linewidth=1.0)
    ax.set_title("Pre/post threshold-aligned median token held by static cohort")
    ax.set_xlabel("Days relative to threshold crossing")
    ax.set_ylabel("Median token held")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    out = outdir / "cohort_threshold_event_window.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _merge_daily_series(
    series_by_run: Dict[str, List[dict]],
    value_key: str,
    drop_last_day: bool = False,
) -> List[dict]:
    """
    Concatenate per-run daily series into a single global timeline.
    """
    merged: List[dict] = []
    offset = 0
    for run_id, rows in series_by_run.items():
        if rows and drop_last_day:
            rows = rows[:-1]
        for r in rows:
            merged.append({"day": int(r["day"]) + offset, value_key: r[value_key]})
        if rows:
            offset += int(rows[-1]["day"]) + 1
    return merged


def _get_matplotlib() -> Tuple[bool, Optional[object], Optional[object]]:
    """
    Lazy import matplotlib so environments without it can still generate text summaries.
    Returns (available, plt, plt_close_fn)
    """
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:  # noqa: BLE001
        print(f"  matplotlib unavailable; skipping plots ({exc})")
        return False, None, None
    return True, plt, plt.close


def _plot_price_paths(outdir: Path, daily_prices_by_run: Dict[str, List[dict]]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok or not daily_prices_by_run:
        return None
    fig, ax = plt.subplots(figsize=(10, 6))
    merged = _merge_daily_series(daily_prices_by_run, "avg_normalized_price")
    days = [r["day"] for r in merged]
    norm_prices = [r["avg_normalized_price"] for r in merged]
    ax.plot(days, norm_prices, marker="o", label="all runs")
    ax.set_title("Normalized price path (all runs)")
    ax.set_xlabel("Day (sim)")
    ax.set_ylabel("Avg normalized price")
    ax.grid(True, linestyle="--", alpha=0.4)
    out = outdir / "price_paths.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_volume_and_swaps(outdir: Path, daily_market_by_run: Dict[str, List[dict]]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok or not daily_market_by_run:
        return None
    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax2 = ax1.twinx()
    merged_weth_in = _merge_daily_series(daily_market_by_run, "volume_weth_in", drop_last_day=True)
    merged_weth_total = _merge_daily_series(daily_market_by_run, "volume_weth_total", drop_last_day=True)
    merged_swaps = _merge_daily_series(daily_market_by_run, "swap_count", drop_last_day=True)
    days = [r["day"] for r in merged_weth_in]
    vols_in = [r["volume_weth_in"] for r in merged_weth_in]
    vols_total = [r["volume_weth_total"] for r in merged_weth_total] if merged_weth_total else vols_in
    swaps = [r["swap_count"] for r in merged_swaps]
    ax1.plot(days, vols_in, marker="o", label="buy-side WETH in (weth_in)")
    ax1.plot(days, vols_total, marker=".", linestyle="--", label="gross WETH volume (weth_total)")
    ax2.plot(days, swaps, marker="s", linestyle="--", label="swaps")
    ax1.set_title("Daily WETH volume (buy-side vs gross) and swap count (all runs)")
    ax1.set_xlabel("Day (sim)")
    ax1.set_ylabel("Volume (WETH)")
    ax2.set_ylabel("Swap count")
    ax1.grid(True, linestyle="--", alpha=0.4)
    ax1.legend(loc="upper left")
    out = outdir / "volume_and_swaps.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_market_volume(outdir: Path, daily_market_by_run: Dict[str, List[dict]]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok or not daily_market_by_run:
        return None
    fig, ax = plt.subplots(figsize=(10, 6))
    merged_in = _merge_daily_series(daily_market_by_run, "volume_weth_in", drop_last_day=True)
    merged_total = _merge_daily_series(daily_market_by_run, "volume_weth_total", drop_last_day=True)
    days = [r["day"] for r in merged_in]
    vols_in = [r["volume_weth_in"] for r in merged_in]
    vols_total = [r["volume_weth_total"] for r in merged_total] if merged_total else vols_in
    ax.plot(days, vols_in, marker="s", label="buy-side WETH in (weth_in)")
    ax.plot(days, vols_total, marker=".", linestyle="--", label="gross WETH volume (weth_total)")
    ax.set_title("Daily WETH volume (buy-side vs gross) (all runs)")
    ax.set_xlabel("Day (sim)")
    ax.set_ylabel("Volume (WETH)")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    out = outdir / "volume_weth_in.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_rolling_vol_vs_volume(
    outdir: Path,
    daily_market_by_run: Dict[str, List[dict]],
    daily_returns_by_run: Dict[str, List[dict]],
    window: int = 5,
) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok:
        return None
    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax2 = ax1.twinx()
    merged_returns = _merge_daily_series(daily_returns_by_run, "return")
    if not merged_returns:
        return None
    days = [r["day"] for r in merged_returns]
    returns = [r["return"] for r in merged_returns]
    rolling = []
    for i in range(len(returns)):
        if i + 1 < window:
            rolling.append(0.0)
            continue
        win = returns[i + 1 - window : i + 1]
        mean = sum(win) / len(win)
        var = sum((x - mean) ** 2 for x in win) / len(win)
        rolling.append(var**0.5)
    ax1.plot(days, rolling, label="vol")

    merged_vols = _merge_daily_series(daily_market_by_run, "volume_weth_total", drop_last_day=True)
    if merged_vols:
        vol_days = [r["day"] for r in merged_vols]
        vol = [r["volume_weth_total"] for r in merged_vols]
        ax2.plot(vol_days, vol, linestyle="--", label="gross WETH volume")
    ax1.set_title(f"Rolling volatility (window={window}) vs daily volume (all runs)")
    ax1.set_xlabel("Day (sim)")
    ax1.set_ylabel("Rolling vol (std of returns)")
    ax2.set_ylabel("Gross volume WETH (weth_total)")
    ax1.grid(True, linestyle="--", alpha=0.4)
    out = outdir / "rolling_vol_vs_volume.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_return_distributions(outdir: Path, daily_returns_by_run: Dict[str, List[dict]]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok:
        return None
    all_returns = []
    for rows in daily_returns_by_run.values():
        all_returns.extend([r["return"] for r in rows])
    if not all_returns:
        return None
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    axes[0].hist(all_returns, bins=30, color="#607d8b", alpha=0.8)
    axes[0].set_title("Daily return histogram (all runs)")
    axes[0].set_xlabel("Return")
    axes[0].set_ylabel("Frequency")
    axes[0].grid(True, linestyle="--", alpha=0.4)

    sorted_rets = sorted(abs(r) for r in all_returns if r is not None)
    n = len(sorted_rets)
    if n > 0:
        xs = sorted_rets
        ys = [1.0 - (i + 1) / n for i in range(n)]
        axes[1].plot(xs, ys, color="#ff7043")
        axes[1].set_yscale("log")
        axes[1].set_xscale("log")
        axes[1].set_title("Return magnitude CCDF (log-log)")
        axes[1].set_xlabel("|Return|")
        axes[1].set_ylabel("P(|r| >= x)")
        axes[1].grid(True, which="both", linestyle="--", alpha=0.4)

    out = outdir / "return_distributions.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_trade_size_distributions(outdir: Path, trade_sizes: Dict[str, List[float]]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok:
        return None
    buys = trade_sizes.get("BUY", [])
    sells = trade_sizes.get("SELL", [])
    if not buys and not sells:
        return None
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    if buys:
        axes[0].hist(buys, bins=30, color="#4caf50", alpha=0.8)
        axes[0].set_title("BUY size histogram (TOKEN)")
        axes[0].set_xlabel("Size")
    else:
        axes[0].set_title("BUY size histogram (TOKEN) - no data")
    if sells:
        axes[1].hist(sells, bins=30, color="#f44336", alpha=0.8)
        axes[1].set_title("SELL size histogram (TOKEN)")
        axes[1].set_xlabel("Size")
    else:
        axes[1].set_title("SELL size histogram (TOKEN) - no data")
    for ax in axes:
        ax.set_ylabel("Frequency")
        ax.grid(True, linestyle="--", alpha=0.4)
    out = outdir / "trade_size_hist.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_price_vs_fair_value(
    outdir: Path,
    daily_prices_by_run: Dict[str, List[dict]],
    fair_values_by_run: Dict[str, List[dict]],
) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok:
        return None
    fig, ax = plt.subplots(figsize=(10, 6))
    merged_prices = _merge_daily_series(daily_prices_by_run, "avg_normalized_price")
    merged_fairs = _merge_daily_series(fair_values_by_run, "fair_value")
    if not merged_prices or not merged_fairs:
        return None
    fairs_map = {r["day"]: r["fair_value"] for r in merged_fairs}
    days = [r["day"] for r in merged_prices if r["day"] in fairs_map]
    prices = [r["avg_normalized_price"] for r in merged_prices if r["day"] in fairs_map]
    fairs = [fairs_map.get(d, None) for d in days]
    ax.plot(days, prices, label="price")
    ax.plot(days, fairs, linestyle="--", label="fair")
    ax.set_title("Normalized price vs latent fair value (all runs)")
    ax.set_xlabel("Day (sim)")
    ax.set_ylabel("Value (normalized)")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    out = outdir / "price_vs_fair_value.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_price_fair_spread(
    outdir: Path,
    daily_prices_by_run: Dict[str, List[dict]],
    fair_values_by_run: Dict[str, List[dict]],
) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok:
        return None
    fig, ax = plt.subplots(figsize=(10, 6))
    merged_prices = _merge_daily_series(daily_prices_by_run, "avg_normalized_price")
    merged_fairs = _merge_daily_series(fair_values_by_run, "fair_value")
    if not merged_prices or not merged_fairs:
        return None
    fairs_map = {r["day"]: r["fair_value"] for r in merged_fairs}
    days = [r["day"] for r in merged_prices if r["day"] in fairs_map]
    spread = []
    for r in merged_prices:
        if r["day"] not in fairs_map:
            continue
        fv = fairs_map.get(r["day"])
        spread.append(r["avg_normalized_price"] - fv if fv is not None else 0.0)
    ax.plot(days, spread, label="all runs")
    ax.set_title("Price - fair value spread (normalized) (all runs)")
    ax.set_xlabel("Day (sim)")
    ax.set_ylabel("Spread")
    ax.grid(True, linestyle="--", alpha=0.4)
    out = outdir / "price_fair_spread.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_price_fair_log(
    outdir: Path,
    daily_prices_by_run: Dict[str, List[dict]],
    fair_values_by_run: Dict[str, List[dict]],
) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok:
        return None
    merged_prices = _merge_daily_series(daily_prices_by_run, "avg_normalized_price")
    merged_fairs = _merge_daily_series(fair_values_by_run, "fair_value")
    if not merged_prices or not merged_fairs:
        return None

    fairs_map = {r["day"]: r["fair_value"] for r in merged_fairs}
    days = [r["day"] for r in merged_prices if r["day"] in fairs_map]
    prices_log = [math.log(max(r["avg_normalized_price"], 1e-12)) for r in merged_prices if r["day"] in fairs_map]
    fairs_log = [math.log(max(fairs_map.get(d, 1e-12), 1e-12)) for d in days]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(days, fairs_log, label="fair value (log)")
    ax.plot(days, prices_log, label="price (log)")
    ax.set_title("Price vs fair value (log) (all runs)")
    ax.set_xlabel("Day (sim)")
    ax.set_ylabel("Log value")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    out = outdir / "price_fair_log.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_regime_trace(outdir: Path, regime_by_run: Dict[str, List[dict]]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok or not regime_by_run:
        return None
    merged_regime = _merge_daily_series(regime_by_run, "regime_code")
    merged_sent = _merge_daily_series(regime_by_run, "sentiment")
    if not merged_regime:
        return None

    days = [r["day"] for r in merged_regime]
    codes = [r["regime_code"] for r in merged_regime]
    sent_days = [r["day"] for r in merged_sent]
    sentiments = [r["sentiment"] for r in merged_sent]

    fig, ax1 = plt.subplots(figsize=(10, 4.8))
    ax2 = ax1.twinx()

    ax1.step(days, codes, where="post", color="#1f77b4", linewidth=2.0, label="regime")
    ax1.set_yticks([0, 1, 2])
    ax1.set_yticklabels(["bear", "bull", "hype"])
    ax1.set_ylim(-0.2, 2.2)
    ax1.set_xlabel("Day (sim)")
    ax1.set_ylabel("Regime")
    ax1.set_title("Regime trace (bear/bull/hype) + sentiment (all runs)")
    ax1.grid(True, linestyle="--", alpha=0.4)

    ax2.plot(sent_days, sentiments, color="#ff7043", linestyle="--", alpha=0.85, label="sentiment")
    ax2.set_ylabel("Sentiment")
    ax2.axhline(0.0, color="#777", linewidth=1.0, alpha=0.6)

    out = outdir / "regime_trace.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_holder_counts(outdir: Path, balances_by_run: Dict[str, Dict[int, List[int]]]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok:
        return None
    fig, ax = plt.subplots(figsize=(10, 6))
    merged = _merge_daily_series(
        {rid: [{"day": d, "holders": sum(1 for b in bals if b > 0)} for d, bals in sorted(day_map.items())]
         for rid, day_map in balances_by_run.items()},
        "holders",
    )
    if not merged:
        return None
    days = [r["day"] for r in merged]
    holders = [r["holders"] for r in merged]
    ax.plot(days, holders, label="all runs")
    ax.set_title("Holder count over time (all runs)")
    ax.set_xlabel("Day (sim)")
    ax.set_ylabel("Holders (balance > 0)")
    ax.grid(True, linestyle="--", alpha=0.4)
    out = outdir / "holder_count.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_balance_concentration(outdir: Path, balances_by_run: Dict[str, Dict[int, List[int]]]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok:
        return None
    fig, ax = plt.subplots(figsize=(10, 6))
    merged = _merge_daily_series(
        {rid: [{"day": d, "top10": (sum(sorted([b for b in bals if b > 0], reverse=True)[:10]) / sum(b for b in bals if b > 0)) if sum(b for b in bals if b > 0) > 0 else 0.0}
              for d, bals in sorted(day_map.items())]
         for rid, day_map in balances_by_run.items()},
        "top10",
    )
    if not merged:
        return None
    days = [r["day"] for r in merged]
    top10 = [r["top10"] for r in merged]
    ax.plot(days, top10, label="all runs")
    ax.set_title("Top-10 balance concentration over time (all runs)")
    ax.set_xlabel("Day (sim)")
    ax.set_ylabel("Top-10 share")
    ax.grid(True, linestyle="--", alpha=0.4)
    out = outdir / "top10_concentration.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_tick_price_scatter(outdir: Path, ticks_prices_by_run: Dict[str, List[tuple]]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok:
        return None
    fig, ax = plt.subplots(figsize=(10, 6))
    all_rows = []
    for run_id, rows in sorted(ticks_prices_by_run.items()):
        all_rows.extend(rows)
    if not all_rows:
        return None
    ticks = [float(t) for t, _ in all_rows]
    prices = [float(p) for _, p in all_rows]
    ax.scatter(ticks, prices, s=10, alpha=0.5)
    ax.set_title("Normalized price vs tick (all runs)")
    ax.set_xlabel("Tick")
    ax.set_ylabel("Normalized price")
    ax.grid(True, linestyle="--", alpha=0.4)
    out = outdir / "price_vs_tick.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_liquidity_over_time(outdir: Path, liquidity_by_run: Dict[str, List[tuple]]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok:
        return None
    fig, ax = plt.subplots(figsize=(10, 6))
    all_rows = []
    for run_id, rows in sorted(liquidity_by_run.items()):
        all_rows.extend(rows)
    if not all_rows:
        return None
    blocks = [int(b) for b, _, _ in all_rows]
    liqs = []
    for _, lq, _ in all_rows:
        try:
            liqs.append(float(lq))
        except Exception:
            liqs.append(0.0)
    if liqs and max(liqs) == min(liqs):
        # No mint/burns; liquidity is constant.
        return None
    ax.plot(blocks, liqs)
    ax.set_title("Pool liquidity over time (raw)")
    ax.set_xlabel("Block")
    ax.set_ylabel("Liquidity")
    ax.grid(True, linestyle="--", alpha=0.4)
    out = outdir / "liquidity_over_time.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_repeat_buy_rates(outdir: Path, cohort_data: Dict[str, CohortRunData]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok or not cohort_data:
        return None

    groups = [k for k, _, _ in COHORT_GROUPS]
    med_rates: List[float] = []
    lo_err: List[float] = []
    hi_err: List[float] = []
    labels: List[str] = []
    colors: List[str] = []
    x = list(range(len(groups)))

    for group_key, group_label, color in COHORT_GROUPS:
        run_rates: List[float] = []
        total_wallets = 0
        for d in cohort_data.values():
            wallets = d.wallets_by_group.get(group_key, [])
            n = len(wallets)
            total_wallets += n
            if n <= 0:
                continue
            repeaters = sum(1 for w in wallets if int(d.total_buy_counts.get(w, 0)) >= 2)
            run_rates.append(float(repeaters) / float(n))
        med, lo, hi = _median_ci(run_rates)
        med_rates.append(med)
        lo_err.append(max(0.0, med - lo))
        hi_err.append(max(0.0, hi - med))
        labels.append(f"{group_label}\nN={total_wallets}")
        colors.append(color)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(x, med_rates, width=0.6, color=colors, alpha=0.9)
    ax.errorbar(x, med_rates, yerr=[lo_err, hi_err], fmt="none", ecolor="black", capsize=4)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylim(0.0, 1.0)
    ax.set_ylabel("Median repeat-buy rate across runs (buy_count >= 2)")
    ax.set_title("Repeat-buy rate by static cohort (median across runs, 95% CI)")
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    out = outdir / "repeat_buy_rates.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _plot_trade_outcomes(outdir: Path, summaries: Dict[str, dict]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok or not summaries:
        return None
    mined = sum(summaries[r].get("mined_trades", 0) or 0 for r in summaries)
    reverted = sum(summaries[r].get("reverted_trades", 0) or 0 for r in summaries)
    swap_events = sum(summaries[r].get("swap_events", 0) or 0 for r in summaries)
    mint_events = sum(summaries[r].get("mint_events", 0) or 0 for r in summaries)

    x = [0]
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    axes[0].bar(x, [mined], color="#4caf50", label="mined")
    axes[0].bar(x, [reverted], bottom=[mined], color="#e57373", label="reverted")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(["all runs"])
    axes[0].set_ylabel("Trade attempts")
    axes[0].set_title("Trades (mined vs reverted) (all runs)")
    axes[0].legend()
    axes[0].grid(True, axis="y", linestyle="--", alpha=0.4)

    axes[1].bar(x, [swap_events], color="#2196f3", label="swaps")
    axes[1].bar(x, [mint_events], bottom=[swap_events], color="#9c27b0", label="mints")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(["all runs"])
    axes[1].set_ylabel("On-chain events")
    axes[1].set_title("Swaps + mints (all runs)")
    axes[1].legend()
    axes[1].grid(True, axis="y", linestyle="--", alpha=0.4)

    fig.tight_layout()
    out = outdir / "trade_and_mint_outcomes.png"
    fig.savefig(out, dpi=150)
    plt_close(fig)
    return out


def _write_summary(outdir: Path, runs: Dict[str, RunMeta], summaries: Dict[str, dict]) -> Path:
    lines: List[str] = []
    for run_id in sorted(runs.keys()):
        meta = runs[run_id]
        s = summaries.get(run_id, {}) if summaries else {}
        lines.append(f"run_id: {run_id}")
        lines.append(f"  created_at_utc : {meta.created_at_utc}")
        lines.append(f"  network        : {meta.network}")
        lines.append(f"  pool           : {meta.pool}")
        lines.append(f"  token          : {meta.token}")
        lines.append(f"  weth           : {meta.weth}")
        lines.append(f"  block_window   : {meta.run_start_block} -> {meta.run_end_block}")
        lines.append(f"  trades         : total={s.get('trade_count')} mined={s.get('mined_trades')} reverted={s.get('reverted_trades')}")
        lines.append(f"  sides          : buys={s.get('buy_trades')} sells={s.get('sell_trades')}")
        lines.append(f"  onchain_events : swaps={s.get('swap_events')} mints={s.get('mint_events')}")
        lines.append(f"  anchor_price   : {s.get('anchor_price')} (anchor_day={s.get('anchor_day')})")
        lines.append(
            "  volumes        : "
            f"token_in={s.get('total_volume_token_in')} "
            f"weth_in_buy_side={s.get('total_volume_weth_in')} "
            f"weth_total_gross={s.get('total_volume_weth_total')}"
        )
        lines.append(f"  coverage_days  : price_days={s.get('price_days')} market_days={s.get('market_days')}")
        manifest = _load_manifest_for_run(run_id)
        if manifest:
            lines.append("  params:")
            keys = [
                "num_days",
                "num_agents",
                "max_agents",
                "agent_start_eth",
                "agent_start_weth",
                "agent_start_token",
                "max_buy_weth",
                "max_sell_token",
                "ticks_per_day",
                "regime_bull_persist",
                "regime_bear_persist",
                "hype_initial_min_days",
                "hype_initial_max_days",
                "hype_initial_days_sampled",
                "hype_persist_start",
                "hype_persist_floor",
                "hype_decay_tau",
                "hype_exit_to_bull_prob",
                "hype_reentry_prob",
                "sentiment_alpha",
                "sentiment_regime_level",
                "sentiment_hype_mult",
                "fair_mu",
                "fair_beta",
                "fair_sigma",
                "fair_reversion",
                "flow_intensity",
                "flow_mispricing_scale",
                "flow_regime_tilt",
                "flow_noise_sigma",
                "impact_kappa",
                "entry_lambda_base",
                "churn_prob_base",
                "fixed_entry_sentiment_sensitivity",
                "fixed_entry_saturation_power",
                "fixed_churn_sentiment_sensitivity",
                "fixed_churn_crowding_sensitivity",
                "fixed_trade_base_participation",
                "fixed_trade_signal_participation",
                "fixed_trade_sentiment_participation",
                "fixed_trade_regime_activity_bull",
                "fixed_trade_regime_activity_hype",
                "fixed_trade_regime_activity_bear",
                "fixed_entry_regime_mult_hype",
                "fixed_entry_regime_mult_bull",
                "fixed_entry_regime_mult_bear",
                "cohort_enabled",
                "cohort_eligible_percent",
                "nft_threshold_tokens",
                "nft_threshold_basis",
                "fixed_eligible_buy_weight_pre_threshold",
                "fixed_eligible_buy_weight_post_threshold",
                "fixed_eligible_sell_weight_pre_threshold",
                "fixed_eligible_sell_weight_post_threshold",
                "fixed_eligible_churn_mult_pre_threshold",
                "fixed_eligible_churn_mult_post_threshold",
                "circulating_supply_start",
                "circulating_supply_daily_unlock",
                "max_trade_pct_buy",
                "max_trade_pct_sell",
                "max_slippage",
                "amm_fee_pct",
                "fast_mode",
            ]
            for key in keys:
                if key in manifest:
                    lines.append(f"    {key} = {manifest.get(key)}")
        lines.append("")
    out = outdir / "summary.txt"
    out.write_text("\n".join(lines))
    return out


def generate_report(warehouse: Path, outdir: Path, run_filter: Optional[List[str]]) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    conn = _connect(warehouse)

    try:
        runs = _load_runs(conn)
        if not runs:
            raise RuntimeError("No runs found in warehouse. Append runs first.")

        run_ids = _load_run_ids_ordered(conn)
        if run_filter:
            run_ids = [r for r in run_ids if r in set(run_filter)]

        if run_filter:
            runs = {rid: meta for rid, meta in runs.items() if rid in set(run_filter)}
            if not runs:
                raise RuntimeError("No matching runs after applying --runs filter.")

        summaries = {}
        for run_id in run_ids:
            summary = _load_run_summary(conn, run_id)
            if summary:
                summaries[run_id] = summary

        daily_prices = {rid: _load_daily_prices(conn, rid) for rid in run_ids}
        daily_close_prices = {rid: _load_daily_close_prices(conn, rid) for rid in run_ids}
        daily_market = {rid: _load_daily_market(conn, rid) for rid in run_ids}
        daily_returns = {rid: _load_daily_returns(conn, rid) for rid in run_ids}
        trade_sizes = _load_trade_sizes(conn)
        fair_values = {rid: _load_fair_values(conn, rid) for rid in run_ids}
        regime_trace = {rid: _load_regime_trace(conn, rid) for rid in run_ids}
        balances = {rid: _load_wallet_balances(conn, rid) for rid in run_ids}
        ticks_prices = {rid: _load_swap_ticks_prices(conn, rid) for rid in run_ids}
        liquidity_series = {rid: _load_liquidity_series(conn, rid) for rid in run_ids}
        cohort_analytics = _load_cohort_analytics(conn, run_ids)
    finally:
        conn.close()

    print(f"Report output: {outdir}")

    price_plot = _plot_price_paths(outdir, daily_close_prices)
    if price_plot:
        print(f"  wrote {price_plot}")
    else:
        print("  skipped price plot (no data)")

    volume_plot = _plot_market_volume(outdir, daily_market)
    if volume_plot:
        print(f"  wrote {volume_plot}")
    else:
        print("  skipped volume plot (no data)")

    price_fair_plot = _plot_price_vs_fair_value(outdir, daily_close_prices, fair_values)
    if price_fair_plot:
        print(f"  wrote {price_fair_plot}")

    spread_plot = _plot_price_fair_spread(outdir, daily_close_prices, fair_values)
    if spread_plot:
        print(f"  wrote {spread_plot}")

    fair_log_plot = _plot_price_fair_log(outdir, daily_close_prices, fair_values)
    if fair_log_plot:
        print(f"  wrote {fair_log_plot}")

    regime_plot = _plot_regime_trace(outdir, regime_trace)
    if regime_plot:
        print(f"  wrote {regime_plot}")

    volume_swaps_plot = _plot_volume_and_swaps(outdir, daily_market)
    if volume_swaps_plot:
        print(f"  wrote {volume_swaps_plot}")

    roll_vol_plot = _plot_rolling_vol_vs_volume(outdir, daily_market, daily_returns, window=5)
    if roll_vol_plot:
        print(f"  wrote {roll_vol_plot}")

    return_dist_plot = _plot_return_distributions(outdir, daily_returns)
    if return_dist_plot:
        print(f"  wrote {return_dist_plot}")

    trade_size_plot = _plot_trade_size_distributions(outdir, trade_sizes)
    if trade_size_plot:
        print(f"  wrote {trade_size_plot}")

    holder_plot = _plot_holder_counts(outdir, balances)
    if holder_plot:
        print(f"  wrote {holder_plot}")

    concentration_plot = _plot_balance_concentration(outdir, balances)
    if concentration_plot:
        print(f"  wrote {concentration_plot}")

    tick_price_plot = _plot_tick_price_scatter(outdir, ticks_prices)
    if tick_price_plot:
        print(f"  wrote {tick_price_plot}")

    liq_plot = _plot_liquidity_over_time(outdir, liquidity_series)
    if liq_plot:
        print(f"  wrote {liq_plot}")

    repeat_buy_plot = _plot_repeat_buy_rates(outdir, cohort_analytics)
    if repeat_buy_plot:
        print(f"  wrote {repeat_buy_plot}")

    cohort_repeat_roll = _plot_repeat_buy_rate_rolling7(outdir, cohort_analytics, window=7)
    if cohort_repeat_roll:
        print(f"  wrote {cohort_repeat_roll}")

    cohort_retention = _plot_retention_curve(outdir, cohort_analytics)
    if cohort_retention:
        print(f"  wrote {cohort_retention}")

    cohort_intensity = _plot_buy_intensity_distributions(outdir, cohort_analytics)
    if cohort_intensity:
        print(f"  wrote {cohort_intensity}")

    cohort_net_flow = _plot_net_flow_median_by_cohort(outdir, cohort_analytics)
    if cohort_net_flow:
        print(f"  wrote {cohort_net_flow}")

    cohort_median_bar = _plot_median_holdings_bar(outdir, cohort_analytics)
    if cohort_median_bar:
        print(f"  wrote {cohort_median_bar}")

    cohort_median_ts = _plot_median_holdings_timeseries(outdir, cohort_analytics)
    if cohort_median_ts:
        print(f"  wrote {cohort_median_ts}")

    cohort_avg_prepost = _plot_avg_holdings_prepost_control(outdir, cohort_analytics)
    if cohort_avg_prepost:
        print(f"  wrote {cohort_avg_prepost}")

    cohort_thresh_window = _plot_threshold_event_window(outdir, cohort_analytics, window=14)
    if cohort_thresh_window:
        print(f"  wrote {cohort_thresh_window}")

    outcomes_plot = _plot_trade_outcomes(outdir, summaries)
    if outcomes_plot:
        print(f"  wrote {outcomes_plot}")
    else:
        print("  skipped trade outcomes plot (no data)")

    summary_txt = _write_summary(outdir, runs, summaries)
    print(f"  wrote {summary_txt}")

    latest_txt = outdir.parent / "latest.txt"
    latest_txt.write_text(str(outdir))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", default=None, help="Path to warehouse db (default: sim/warehouse.db)")
    parser.add_argument("--outdir", default=None, help="Output directory for plots (default: sim/reports/<timestamp>/)")
    parser.add_argument("--runs", default=None, help="Comma-separated run_ids to include (default: all)")
    args = parser.parse_args()

    warehouse = Path(args.warehouse) if args.warehouse else Path(__file__).resolve().parent / "warehouse.db"

    if args.outdir:
        outdir = Path(args.outdir)
    else:
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        outdir = Path(__file__).resolve().parent / "reports" / stamp

    run_filter = [r.strip() for r in args.runs.split(",")] if args.runs else None
    generate_report(warehouse, outdir, run_filter)


if __name__ == "__main__":
    main()
