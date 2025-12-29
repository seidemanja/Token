"""
sim/chain.py

This is the on-chain execution layer:
- Connect to RPC
- Use Hardhat account #0 as the admin/funder (LOCAL ONLY)
- Deploy PoolSwapExecutor(pool, payer) for each agent
- Approve token-in to executor
- Call executor.executeSwap(zeroForOne, amountSpecified, sqrtPriceLimitX96)

Important:
- amountSpecified > 0 means "exact input" in Uniswap V3.
- zeroForOne indicates swapping token0 -> token1.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from eth_account import Account
from web3 import Web3
from web3.contract import Contract

from sim.abi import (
    load_artifact_abi,
    token_artifact_path,
    pool_artifact_path,
    weth_artifact_path,
    executor_artifact_path,
)

# Hardhat default private key #0 (LOCAL ONLY).
# This key exists only for local dev networks and must never be used on public networks.
HARDHAT_DEFAULT_PRIVKEY0 = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

# Uniswap V3 sqrt price bounds (from TickMath)
MIN_SQRT_RATIO = 4295128739
MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342


@dataclass
class Agent:
    agent_id: int
    address: str
    private_key: str
    executor: Optional[str] = None


def to_wei_amount(amount: float, decimals: int = 18) -> int:
    """Convert a decimal amount into token base units."""
    return int(amount * (10 ** decimals))


class Chain:
    def __init__(self, rpc_url: str, token: str, pool: str, weth: str):
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        if not self.w3.is_connected():
            raise RuntimeError(f"Could not connect to RPC: {rpc_url}")

        # Normalize addresses
        self.token_addr = Web3.to_checksum_address(token)
        self.pool_addr = Web3.to_checksum_address(pool)
        self.weth_addr = Web3.to_checksum_address(weth)

        # Load ABIs from artifacts
        self.token_abi = load_artifact_abi(token_artifact_path())
        self.pool_abi = load_artifact_abi(pool_artifact_path())
        self.weth_abi = load_artifact_abi(weth_artifact_path())
        self.exec_abi = load_artifact_abi(executor_artifact_path())

        # Instantiate contracts
        self.token: Contract = self.w3.eth.contract(address=self.token_addr, abi=self.token_abi)
        self.pool: Contract = self.w3.eth.contract(address=self.pool_addr, abi=self.pool_abi)
        self.weth: Contract = self.w3.eth.contract(address=self.weth_addr, abi=self.weth_abi)

        # Admin is Hardhat account #0
        self.admin = Account.from_key(HARDHAT_DEFAULT_PRIVKEY0)

        # Load PoolSwapExecutor bytecode for deployment
        exec_artifact_path = executor_artifact_path()
        artifact = json.loads(Path(exec_artifact_path).read_text())
        self.exec_bytecode = artifact.get("bytecode")
        if not self.exec_bytecode:
            raise ValueError(f"No bytecode found in executor artifact: {exec_artifact_path}")

    def _build_and_send(self, from_acct: Account, tx: dict[str, Any]) -> str:
        """
        Sign and broadcast a transaction.

        Important:
        - Use EIP-1559 dynamic fee fields (maxFeePerGas, maxPriorityFeePerGas).
        - Do NOT set gasPrice, because eth-account will treat the tx as a typed transaction
        and reject unknown fields like gasPrice (your current error).
        """
        # Nonce + chain ID
        tx.setdefault("nonce", self.w3.eth.get_transaction_count(from_acct.address))
        tx.setdefault("chainId", self.w3.eth.chain_id)

        # Fee fields (EIP-1559)
        # Use the node's suggested max priority fee if available; otherwise fall back.
        try:
            priority = self.w3.eth.max_priority_fee  # supported on many clients
        except Exception:
            priority = self.w3.to_wei(1, "gwei")     # safe fallback for local

        # baseFeePerGas is present in latest blocks on EIP-1559 networks (Hardhat supports this).
        latest_block = self.w3.eth.get_block("latest")
        base_fee = latest_block.get("baseFeePerGas")

        if base_fee is None:
            # If the chain is not reporting baseFeePerGas, fall back to legacy gasPrice safely.
            # In this case, we must NOT include EIP-1559 fields.
            tx.setdefault("gasPrice", self.w3.eth.gas_price)
        else:
            # Standard approach: maxFee = 2*baseFee + priority
            max_fee = int(base_fee * 2 + priority)
            tx.setdefault("maxPriorityFeePerGas", int(priority))
            tx.setdefault("maxFeePerGas", int(max_fee))

            # Explicitly set type=2 to ensure typed tx encoding.
            tx.setdefault("type", 2)

            # Ensure we do not accidentally carry a gasPrice field.
            tx.pop("gasPrice", None)

        # Gas estimation (do this after fee fields are present)
        if "gas" not in tx:
            tx["gas"] = self.w3.eth.estimate_gas(tx)

        # Sign and send
        signed = from_acct.sign_transaction(tx)

        # web3.py / eth-account compatibility: handle rawTransaction vs raw_transaction
        raw = getattr(signed, "rawTransaction", None) or getattr(signed, "raw_transaction")
        tx_hash = self.w3.eth.send_raw_transaction(raw)

        return tx_hash.hex()


    def wait_receipt(self, tx_hash: str, timeout_s: int = 60) -> Any:
        """Wait for a transaction receipt."""
        return self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout_s)

    # ----------------------------
    # Funding / seeding helpers
    # ----------------------------

    def fund_eth(self, to_addr: str, eth_amount: float) -> str:
        """Send ETH from admin to an agent."""
        tx = {
            "from": self.admin.address,
            "to": Web3.to_checksum_address(to_addr),
            "value": self.w3.to_wei(eth_amount, "ether"),
        }
        return self._build_and_send(self.admin, tx)

    def wrap_eth_to_weth(self, acct: Account, eth_amount: float) -> str:
        """Wrap ETH into WETH by calling WETH.deposit()."""
        fn = self.weth.functions.deposit()
        tx = fn.build_transaction({
            "from": acct.address,
            "value": self.w3.to_wei(eth_amount, "ether"),
        })
        return self._build_and_send(acct, tx)

    def transfer_token(self, to_addr: str, token_amount: float) -> str:
        """Transfer simulation TOKEN from admin to an agent so agents can SELL."""
        amt = to_wei_amount(token_amount, 18)
        fn = self.token.functions.transfer(Web3.to_checksum_address(to_addr), amt)
        tx = fn.build_transaction({"from": self.admin.address})
        return self._build_and_send(self.admin, tx)

    def approve_erc20(self, acct: Account, token_contract: Contract, spender: str, amount_wei: int) -> str:
        """Approve spender to pull ERC20 tokens from acct."""
        fn = token_contract.functions.approve(Web3.to_checksum_address(spender), amount_wei)
        tx = fn.build_transaction({"from": acct.address})
        return self._build_and_send(acct, tx)

    # ----------------------------
    # Executor deployment
    # ----------------------------

    def deploy_executor_for_agent(self, agent: Agent) -> str:
        """
        Deploy PoolSwapExecutor(pool, payer) for an agent.

        ABI-confirmed constructor:
          constructor(address _pool, address _payer)
        """
        agent_acct = Account.from_key(agent.private_key)
        factory = self.w3.eth.contract(abi=self.exec_abi, bytecode=self.exec_bytecode)

        tx = factory.constructor(self.pool_addr, agent_acct.address).build_transaction({"from": agent_acct.address})
        tx_hash = self._build_and_send(agent_acct, tx)
        rcpt = self.wait_receipt(tx_hash)

        if rcpt.status != 1:
            raise RuntimeError("Executor deployment reverted")

        return rcpt.contractAddress

    def get_executor(self, executor_addr: str) -> Contract:
        """Get a Contract instance for a deployed executor."""
        return self.w3.eth.contract(address=Web3.to_checksum_address(executor_addr), abi=self.exec_abi)

    # ----------------------------
    # Uniswap V3 swap helpers
    # ----------------------------

    @staticmethod
    def compute_zero_for_one(token_in: str, pool_token0: str, pool_token1: str) -> bool:
        """
        Determine the Uniswap V3 zeroForOne flag.

        - zeroForOne=True  means swapping token0 -> token1
        - zeroForOne=False means swapping token1 -> token0
        """
        tin = token_in.lower()
        t0 = pool_token0.lower()
        t1 = pool_token1.lower()

        if tin == t0:
            return True
        if tin == t1:
            return False
        raise ValueError("token_in is neither pool token0 nor token1")

    @staticmethod
    def sqrt_price_limit_for_direction(zero_for_one: bool) -> int:
        """
        Provide a valid sqrtPriceLimitX96 for the swap direction.

        Uniswap V3 requires direction-consistent bounds:
        - zeroForOne=True  => limit must be > MIN_SQRT_RATIO
        - zeroForOne=False => limit must be < MAX_SQRT_RATIO

        Using "near bound" is effectively "no limit" while satisfying constraints.
        """
        if zero_for_one:
            return MIN_SQRT_RATIO + 1
        return MAX_SQRT_RATIO - 1

    def execute_swap_exact_in(
        self,
        agent: Agent,
        executor_addr: str,
        token_in_addr: str,
        amount_in_wei: int,
        pool_token0: str,
        pool_token1: str,
    ) -> str:
        """
        Execute an exact-input swap via PoolSwapExecutor.executeSwap(...).

        PoolSwapExecutor ABI:
          executeSwap(bool zeroForOne, int256 amountSpecified, uint160 sqrtPriceLimitX96)

        Conventions:
          amountSpecified > 0 means exact input.
        """
        acct = Account.from_key(agent.private_key)
        executor = self.get_executor(executor_addr)

        zero_for_one = self.compute_zero_for_one(token_in_addr, pool_token0, pool_token1)
        sqrt_limit = self.sqrt_price_limit_for_direction(zero_for_one)

        # Approve token_in to the executor.
        # We approve a large allowance to avoid re-approving every trade.
        if token_in_addr.lower() == self.weth_addr.lower():
            self.approve_erc20(acct, self.weth, executor_addr, to_wei_amount(10_000, 18))
        elif token_in_addr.lower() == self.token_addr.lower():
            self.approve_erc20(acct, self.token, executor_addr, to_wei_amount(10_000_000, 18))
        else:
            raise ValueError("token_in_addr is not WETH or TOKEN (unexpected for this project).")

        # Build + send the swap tx
        fn = executor.functions.executeSwap(
            zero_for_one,
            int(amount_in_wei),   # int256
            int(sqrt_limit)       # uint160
        )
        tx = fn.build_transaction({"from": acct.address})
        tx_hash = self._build_and_send(acct, tx)
        return tx_hash
