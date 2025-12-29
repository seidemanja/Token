"""
sim/abi.py

Loads ABIs from Hardhat artifacts so we do not guess function signatures.
This makes the Python runner resilient to contract changes as long as artifacts are updated.
"""

import json
from pathlib import Path
from typing import Any


def load_artifact_abi(artifact_path: str) -> list[dict[str, Any]]:
    """Load the ABI array from a Hardhat artifact JSON file."""
    p = Path(artifact_path)
    if not p.exists():
        raise FileNotFoundError(f"Artifact not found: {artifact_path}")

    data = json.loads(p.read_text())
    abi = data.get("abi")
    if not abi:
        raise ValueError(f"No ABI in artifact: {artifact_path}")
    return abi


def _find_first_existing(paths: list[str]) -> str:
    """Pick the first path that exists (helps if you rearrange contracts)."""
    for rel in paths:
        p = Path(rel)
        if p.exists():
            return str(p)
    raise FileNotFoundError("Could not find artifact. Tried:\n" + "\n".join(paths))


def token_artifact_path() -> str:
    return _find_first_existing([
        "artifacts/contracts/MyToken.sol/MyToken.json",
    ])


def weth_artifact_path() -> str:
    # In your repo you likely only have the minimal interface artifact.
    return _find_first_existing([
        "artifacts/contracts/interfaces/IWETH9Minimal.sol/IWETH9Minimal.json",
    ])


def pool_artifact_path() -> str:
    return _find_first_existing([
        "artifacts/contracts/interfaces/IUniswapV3PoolMinimal.sol/IUniswapV3PoolMinimal.json",
    ])


def executor_artifact_path() -> str:
    return _find_first_existing([
        "artifacts/contracts/PoolSwapExecutor.sol/PoolSwapExecutor.json",
    ])
