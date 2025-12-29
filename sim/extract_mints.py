"""
sim/extract_mints.py

Extracts ERC-721 Transfer events (mint = from zero address) for the JSTVIP NFT
over a block range and stores them in SQLite.

Writes:
  nft_mints(tx_hash, log_index, block_number, to_address, token_id)

Usage:
  python -m sim.extract_mints <path/to/sim.db> <from_block> <to_block>
"""

import sqlite3
import sys
from typing import Any

from web3 import Web3

from sim.config import load_config
from sim.chain import Chain

ERC721_TRANSFER_EVENT_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "from", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "to", "type": "address"},
            {"indexed": True, "internalType": "uint256", "name": "tokenId", "type": "uint256"},
        ],
        "name": "Transfer",
        "type": "event",
    }
]


def ensure(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS nft_mints (
          tx_hash TEXT NOT NULL,
          log_index INTEGER NOT NULL,
          block_number INTEGER NOT NULL,
          to_address TEXT NOT NULL,
          token_id TEXT NOT NULL,
          PRIMARY KEY (tx_hash, log_index)
        );
        """
    )
    conn.commit()


def u256_to_int(x: Any) -> int:
    return int(x)


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit("Usage: python -m sim.extract_mints <path/to/sim.db> <from_block> <to_block>")

    db_path = sys.argv[1]
    from_block = int(sys.argv[2])
    to_block = int(sys.argv[3])

    cfg = load_config()
    if not cfg.jstvip:
        raise SystemExit("JSTVIP address missing in env/config; cannot extract mints.")

    chain = Chain(cfg.rpc_url, cfg.token, cfg.pool, cfg.weth)
    conn = sqlite3.connect(db_path)
    ensure(conn)

    nft_events_only = chain.w3.eth.contract(address=Web3.to_checksum_address(cfg.jstvip), abi=ERC721_TRANSFER_EVENT_ABI)
    transfer = nft_events_only.events.Transfer()

    print(f"Extracting NFT mints from block {from_block} to {to_block} ...")

    logs = transfer.get_logs(from_block=from_block, to_block=to_block)

    inserted = 0
    for ev in logs:
        args = ev["args"]
        from_addr = str(args["from"]).lower()
        if from_addr != "0x0000000000000000000000000000000000000000":
            continue  # not a mint

        tx_hash = ev["transactionHash"].hex()
        log_index = int(ev["logIndex"])
        to_addr = str(args["to"]).lower()
        token_id = str(u256_to_int(args["tokenId"]))

        try:
            conn.execute(
                """
                INSERT INTO nft_mints(tx_hash, log_index, block_number, to_address, token_id)
                VALUES (?,?,?,?,?)
                """,
                (tx_hash, log_index, int(ev["blockNumber"]), to_addr, token_id),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()
    print(f"Inserted {inserted} new nft_mints rows (raw logs={len(logs)}).")


if __name__ == "__main__":
    main()
