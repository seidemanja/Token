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
import math
import sqlite3
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
        "price_days",
        "market_days",
    ]
    return dict(zip(keys, row))


def _load_daily_prices(conn: sqlite3.Connection, run_id: str) -> List[dict]:
    if not _table_exists(conn, "run_daily_prices"):
        return []
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


def _load_daily_market(conn: sqlite3.Connection, run_id: str) -> List[dict]:
    if not _table_exists(conn, "run_daily_market"):
        return []
    rows = conn.execute(
        """
        SELECT day, swap_count, volume_token_in, volume_weth_in, avg_tick
        FROM run_daily_market
        WHERE run_id=?
        ORDER BY day ASC
        """,
        (run_id,),
    ).fetchall()
    return [
        {
            "day": int(day),
            "swap_count": int(cnt),
            "volume_token_in": float(vt),
            "volume_weth_in": float(vw),
            "avg_tick": float(tick),
        }
        for day, cnt, vt, vw, tick in rows
    ]


def _load_daily_returns(conn: sqlite3.Connection, run_id: str) -> List[dict]:
    rows = conn.execute(
        """
        SELECT day, avg_normalized_price
        FROM run_daily_prices
        WHERE run_id=?
        ORDER BY day ASC
        """,
        (run_id,),
    ).fetchall()
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
    merged_vols = _merge_daily_series(daily_market_by_run, "volume_weth_in", drop_last_day=True)
    merged_swaps = _merge_daily_series(daily_market_by_run, "swap_count", drop_last_day=True)
    days = [r["day"] for r in merged_vols]
    vols = [r["volume_weth_in"] for r in merged_vols]
    swaps = [r["swap_count"] for r in merged_swaps]
    ax1.plot(days, vols, marker="o", label="volume")
    ax2.plot(days, swaps, marker="s", linestyle="--", label="swaps")
    ax1.set_title("Daily volume (WETH) and swap count (all runs)")
    ax1.set_xlabel("Day (sim)")
    ax1.set_ylabel("Volume WETH in")
    ax2.set_ylabel("Swap count")
    ax1.grid(True, linestyle="--", alpha=0.4)
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
    merged_vols = _merge_daily_series(daily_market_by_run, "volume_weth_in", drop_last_day=True)
    days = [r["day"] for r in merged_vols]
    vols = [r["volume_weth_in"] for r in merged_vols]
    ax.plot(days, vols, marker="s", label="all runs")
    ax.set_title("Daily volume (WETH in) (all runs)")
    ax.set_xlabel("Day (sim)")
    ax.set_ylabel("Volume WETH in")
    ax.grid(True, linestyle="--", alpha=0.4)
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

    merged_vols = _merge_daily_series(daily_market_by_run, "volume_weth_in", drop_last_day=True)
    if merged_vols:
        vol_days = [r["day"] for r in merged_vols]
        vol = [r["volume_weth_in"] for r in merged_vols]
        ax2.plot(vol_days, vol, linestyle="--", label="volume")
    ax1.set_title(f"Rolling volatility (window={window}) vs daily volume (all runs)")
    ax1.set_xlabel("Day (sim)")
    ax1.set_ylabel("Rolling vol (std of returns)")
    ax2.set_ylabel("Volume WETH in")
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


def _plot_price_fair_perceived(
    outdir: Path,
    daily_prices_by_run: Dict[str, List[dict]],
    fair_values_by_run: Dict[str, List[dict]],
    perceived_by_run: Dict[str, List[dict]],
) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok:
        return None
    merged_prices = _merge_daily_series(daily_prices_by_run, "avg_normalized_price")
    merged_fairs = _merge_daily_series(fair_values_by_run, "fair_value")
    merged_perceived = _merge_daily_series(perceived_by_run, "avg_perceived_log")
    if not merged_prices or not merged_fairs or not merged_perceived:
        return None

    fairs_map = {r["day"]: r["fair_value"] for r in merged_fairs}
    perceived_map = {r["day"]: r["avg_perceived_log"] for r in merged_perceived}
    days = [r["day"] for r in merged_prices if r["day"] in fairs_map and r["day"] in perceived_map]
    prices_log = [math.log(max(r["avg_normalized_price"], 1e-12)) for r in merged_prices if r["day"] in fairs_map and r["day"] in perceived_map]
    fairs_log = [math.log(max(fairs_map.get(d, 1e-12), 1e-12)) for d in days]
    perceived_log = [perceived_map.get(d, None) for d in days]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(days, fairs_log, label="fair value (log)")
    ax.plot(days, perceived_log, label="avg perceived fair value (log)")
    ax.plot(days, prices_log, label="price (log)")
    ax.set_title("Price vs fair value vs perceived fair value (log) (all runs)")
    ax.set_xlabel("Day (sim)")
    ax.set_ylabel("Log value")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    out = outdir / "price_fair_perceived.png"
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


def _plot_repeat_buy_rates(outdir: Path, rates: Dict[str, dict]) -> Optional[Path]:
    ok, plt, plt_close = _get_matplotlib()
    if not ok or not rates:
        return None
    eligible_success = 0.0
    eligible_n = 0
    control_success = 0.0
    control_n = 0
    for r in rates.values():
        eligible_n += int(r.get("eligible_n", 0))
        control_n += int(r.get("control_n", 0))
        eligible_success += float(r.get("eligible_rate", 0.0)) * int(r.get("eligible_n", 0))
        control_success += float(r.get("control_rate", 0.0)) * int(r.get("control_n", 0))
    eligible = (eligible_success / eligible_n) if eligible_n else 0.0
    control = (control_success / control_n) if control_n else 0.0
    x = [0, 1]
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar([0], [eligible], width=0.6, label="eligible")
    ax.bar([1], [control], width=0.6, label="control")
    ax.set_xticks(x)
    ax.set_xticklabels(["eligible", "control"])
    ax.set_ylabel("Repeat-buy rate (buy_count >= 2)")
    ax.set_title("Repeat-buy rate by cohort eligibility (all runs)")
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    if control_n == 0:
        ax.text(1, 0.02, "no control cohort in data", ha="center", fontsize=9)
    ax.legend()
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
        lines.append(f"  volumes        : token_in={s.get('total_volume_token_in')} weth_in={s.get('total_volume_weth_in')}")
        lines.append(f"  coverage_days  : price_days={s.get('price_days')} market_days={s.get('market_days')}")
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
        daily_market = {rid: _load_daily_market(conn, rid) for rid in run_ids}
        daily_returns = {rid: _load_daily_returns(conn, rid) for rid in run_ids}
        trade_sizes = _load_trade_sizes(conn)
        fair_values = {rid: _load_fair_values(conn, rid) for rid in run_ids}
        perceived_values = {rid: _load_perceived_fair_values(conn, rid) for rid in run_ids}
        balances = {rid: _load_wallet_balances(conn, rid) for rid in run_ids}
        ticks_prices = {rid: _load_swap_ticks_prices(conn, rid) for rid in run_ids}
        liquidity_series = {rid: _load_liquidity_series(conn, rid) for rid in run_ids}
        repeat_buy_rates = _load_repeat_buy_rates(conn)
    finally:
        conn.close()

    print(f"Report output: {outdir}")

    price_plot = _plot_price_paths(outdir, daily_prices)
    if price_plot:
        print(f"  wrote {price_plot}")
    else:
        print("  skipped price plot (no data)")

    volume_plot = _plot_market_volume(outdir, daily_market)
    if volume_plot:
        print(f"  wrote {volume_plot}")
    else:
        print("  skipped volume plot (no data)")

    price_fair_plot = _plot_price_vs_fair_value(outdir, daily_prices, fair_values)
    if price_fair_plot:
        print(f"  wrote {price_fair_plot}")

    spread_plot = _plot_price_fair_spread(outdir, daily_prices, fair_values)
    if spread_plot:
        print(f"  wrote {spread_plot}")

    perceived_plot = _plot_price_fair_perceived(outdir, daily_prices, fair_values, perceived_values)
    if perceived_plot:
        print(f"  wrote {perceived_plot}")

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

    repeat_buy_plot = _plot_repeat_buy_rates(outdir, repeat_buy_rates)
    if repeat_buy_plot:
        print(f"  wrote {repeat_buy_plot}")

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
