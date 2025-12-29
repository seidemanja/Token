"""
sim/db.py

SQLite persistence layer for simulation runs.

This module:
- creates tables
- writes run metadata (including start/end block windows)
- writes agents and trades

Key change:
- sim_runs now stores run_start_block and run_end_block so post_run can extract
  swaps/mints scoped exactly to the simulation window.
"""

from __future__ import annotations

import sqlite3
from typing import Optional


class SimDB:
    def __init__(self, path: str) -> None:
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._ensure_schema()

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()

    def _ensure_schema(self) -> None:
        """
        Create tables if missing. Also perform small forward-compatible migrations.
        """
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sim_runs (
              run_id TEXT PRIMARY KEY,
              network TEXT NOT NULL,
              rpc_url TEXT NOT NULL,
              token TEXT NOT NULL,
              pool TEXT NOT NULL,
              weth TEXT NOT NULL,
              created_at_utc TEXT NOT NULL,

              -- NEW: run-scoped extraction window (inclusive)
              run_start_block INTEGER,
              run_end_block INTEGER
            );

            CREATE TABLE IF NOT EXISTS agents (
              run_id TEXT NOT NULL,
              agent_id INTEGER NOT NULL,
              address TEXT NOT NULL,
              private_key TEXT NOT NULL,
              executor TEXT,
              PRIMARY KEY (run_id, agent_id)
            );

            CREATE TABLE IF NOT EXISTS trades (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_id TEXT NOT NULL,
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
              created_at_utc TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        # Migration: add run_start_block / run_end_block if table existed previously.
        cols = [r[1] for r in self.conn.execute("PRAGMA table_info(sim_runs);").fetchall()]
        if "run_start_block" not in cols:
            self.conn.execute("ALTER TABLE sim_runs ADD COLUMN run_start_block INTEGER;")
        if "run_end_block" not in cols:
            self.conn.execute("ALTER TABLE sim_runs ADD COLUMN run_end_block INTEGER;")

        self.conn.commit()

    def insert_run(
        self,
        run_id: str,
        network: str,
        rpc_url: str,
        token: str,
        pool: str,
        weth: str,
        created_at_utc: str,
    ) -> None:
        """
        Insert run metadata. The run_start_block and run_end_block are set later.
        """
        self.conn.execute(
            """
            INSERT OR REPLACE INTO sim_runs
              (run_id, network, rpc_url, token, pool, weth, created_at_utc, run_start_block, run_end_block)
            VALUES (?,?,?,?,?,?,?,NULL,NULL)
            """,
            (run_id, network, rpc_url, token, pool, weth, created_at_utc),
        )
        self.conn.commit()

    def set_run_block_window(self, run_id: str, start_block: int, end_block: int) -> None:
        """
        Persist the authoritative run-scoped extraction window.
        """
        self.conn.execute(
            """
            UPDATE sim_runs
            SET run_start_block = ?, run_end_block = ?
            WHERE run_id = ?
            """,
            (int(start_block), int(end_block), run_id),
        )
        self.conn.commit()

    def get_latest_run_id(self) -> str:
        row = self.conn.execute(
            "SELECT run_id FROM sim_runs ORDER BY created_at_utc DESC LIMIT 1"
        ).fetchone()
        if not row:
            raise RuntimeError("No sim_runs found.")
        return str(row[0])

    def get_run_block_window(self, run_id: str) -> tuple[int, int]:
        row = self.conn.execute(
            "SELECT run_start_block, run_end_block FROM sim_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if not row or row[0] is None or row[1] is None:
            raise RuntimeError(
                f"Run {run_id} is missing run_start_block/run_end_block. "
                "Make sure run_sim sets them."
            )
        return int(row[0]), int(row[1])

    def upsert_agent(self, run_id: str, agent_id: int, address: str, private_key: str, executor: Optional[str]) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO agents(run_id, agent_id, address, private_key, executor)
            VALUES (?,?,?,?,?)
            """,
            (run_id, int(agent_id), address.lower(), private_key, (executor or "")),
        )
        self.conn.commit()

    def insert_trade(
        self,
        run_id: str,
        day: int,
        agent_id: int,
        side: str,
        amount_in_wei: str,
        token_in: str,
        token_out: str,
        tx_hash: Optional[str],
        status: str,
        revert_reason: Optional[str],
        block_number: Optional[int],
        gas_used: Optional[int],
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO trades
              (run_id, day, agent_id, side, amount_in_wei, token_in, token_out, tx_hash, status, revert_reason, block_number, gas_used)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                run_id,
                int(day),
                int(agent_id),
                side,
                str(amount_in_wei),
                token_in.lower(),
                token_out.lower(),
                tx_hash,
                status,
                revert_reason,
                (int(block_number) if block_number is not None else None),
                (int(gas_used) if gas_used is not None else None),
            ),
        )
        self.conn.commit()
