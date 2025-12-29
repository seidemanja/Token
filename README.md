# MyToken AMM (Uniswap V3 – Direct Pool Interaction)

## Overview

This repository contains a custom ERC-20 token, an NFT incentive mechanism, and a Uniswap V3–compatible AMM setup designed specifically for **local simulation and testnet validation**.

Instead of relying on Uniswap routers, this project interacts **directly with Uniswap V3 pools**. This approach:

- Eliminates router inconsistencies across networks
- Produces identical AMM math (price impact, ticks, liquidity behavior)
- Is deterministic, faster, and easier to simulate at scale
- Is well-suited for automated multi-wallet trading simulations

The architecture mirrors Uniswap V3 core mechanics while remaining intentionally minimal and controlled.

---

## Repository Structure

    contracts/
    ├─ MyToken.sol
    ├─ PoolSwapExecutor.sol
    └─ interfaces/
       ├─ IUniswapV3FactoryMinimal.sol
       ├─ IUniswapV3PoolMinimal.sol
       ├─ INonfungiblePositionManagerMinimal.sol
       └─ IWETH9Minimal.sol

    scripts/
    ├─ deploy.ts
    └─ amm/
       ├─ 02_create_and_init_pool.ts
       ├─ 03_mint_liquidity.ts
       └─ 06_direct_pool_swap.ts

    config/
    └─ uniswap.sepolia.ts

---

## Smart Contracts

### MyToken.sol

A fixed-supply ERC-20 token.

Key properties:
- 18 decimals
- Fixed total supply defined at deployment
- Pausable transfers
- Ownable administration

This token is one side of the Uniswap V3 pool.

---

### PoolSwapExecutor.sol

A minimal Uniswap V3 swap executor.

Purpose:
- Executes swaps directly against a Uniswap V3 pool
- Implements the Uniswap V3 swap callback
- Transfers owed tokens during the callback

This contract is the core AMM interaction layer used for simulations.

---

## Interfaces (Minimal)

All interfaces are intentionally minimal to avoid dependency sprawl.

### IUniswapV3FactoryMinimal.sol
Used to:
- Find or verify the pool address for (tokenA, tokenB, fee)

---

### IUniswapV3PoolMinimal.sol
Used to:
- Execute swaps
- Read pool state (token0, token1, slot0, liquidity)

This interface represents the AMM itself.

---

### INonfungiblePositionManagerMinimal.sol
Used to:
- Mint concentrated liquidity positions
- Manage LP NFTs

Required for adding liquidity.

---

### IWETH9Minimal.sol
Used to:
- Wrap ETH into WETH
- Interact with Uniswap V3 pools (which do not accept native ETH)

---

## AMM Scripts

### scripts/deploy.ts
Deploys the MyToken contract.

Outputs the token address, which is reused by all AMM scripts.

---

### 02_create_and_init_pool.ts
Creates and initializes the Uniswap V3 pool.

Actions:
1. Looks up the pool via the factory
2. Creates it if it does not exist
3. Initializes it with a chosen initial price

Idempotent and safe to run multiple times.

---

### 03_mint_liquidity.ts
Mints concentrated liquidity into the pool.

Actions:
- Wraps ETH to WETH
- Approves token and WETH
- Mints a Uniswap V3 liquidity position

Liquidity range and amounts are configurable.

---

### 06_direct_pool_swap.ts
Executes a swap directly against the Uniswap V3 pool.

Actions:
1. Wraps ETH to WETH
2. Approves the PoolSwapExecutor
3. Calls pool.swap(...)
4. Handles the swap callback
5. Transfers tokens atomically

This script is the foundation for all trading simulations.

---

## Why No Router?

Routers are intentionally excluded.

Reasons:
- Router deployments differ across networks
- Router ABIs change
- Permit2 complexity is unnecessary
- Routers introduce opaque behavior

Direct pool swaps:
- Use the same AMM math as routers
- Are deterministic and reliable
- Are ideal for simulations

Routers can be added later without changing the AMM core.

---

## Running Locally vs Sepolia Testnet

This project is designed to run in **two modes** using the same code.

---

### Local Development (Recommended)

Local mode uses a Hardhat node, optionally forked from Sepolia.

Characteristics:
- Free and instant transactions
- Unlimited ETH
- Fast block times
- Persistent state while the node is running
- Ideal for simulations and parameter tuning

Typical usage:

    npx hardhat node --fork <SEPOLIA_RPC_URL>

In a second terminal:

    npx hardhat run scripts/deploy.ts --network localhost
    TOKEN_ADDRESS=0x... npx hardhat run scripts/amm/02_create_and_init_pool.ts --network localhost
    TOKEN_ADDRESS=0x... npx hardhat run scripts/amm/03_mint_liquidity.ts --network localhost
    TOKEN_ADDRESS=0x... npx hardhat run scripts/amm/06_direct_pool_swap.ts --network localhost

This is the mode you should use for:
- Multi-wallet simulations
- High-volume testing
- Long-horizon AMM dynamics

---

### Sepolia Testnet (Validation Only)

Sepolia mode deploys contracts to the real testnet.

Characteristics:
- Transactions cost Sepolia ETH
- Faucet-limited throughput
- Slower block times
- Persistent global state

Usage:

    npx hardhat run scripts/deploy.ts --network sepolia
    TOKEN_ADDRESS=0x... npx hardhat run scripts/amm/02_create_and_init_pool.ts --network sepolia
    TOKEN_ADDRESS=0x... npx hardhat run scripts/amm/03_mint_liquidity.ts --network sepolia
    TOKEN_ADDRESS=0x... npx hardhat run scripts/amm/06_direct_pool_swap.ts --network sepolia

Requirements:
- .env must define SEPOLIA_RPC_URL and PRIVATE_KEY
- Sepolia ETH must be managed carefully

Sepolia should be used to:
- Validate deployment correctness
- Confirm real-network behavior

It is **not recommended** for large-scale simulations.

---

## Simulation Philosophy

This repository is designed for:
- Multi-wallet simulations
- Realistic AMM price impact
- Concentrated liquidity dynamics
- Parameter tuning against real Uniswap V3 behavior

Because swaps interact directly with the pool, simulation results are mathematically identical to router-based swaps.

---

## Summary

This project implements:
- A clean ERC-20 token
- A Uniswap V3 pool
- Concentrated liquidity
- Direct AMM swaps
- A minimal, deterministic architecture

It is intentionally designed to be boring, predictable, and correct — ideal for simulation-driven protocol design.

Reward Controller (AMM Swap–Driven NFT Incentives)
Overview
The project now includes an off-chain reward controller that observes Uniswap V3 pool activity and conditionally mints NFTs based on actual AMM swap behavior, rather than ERC-20 balances alone.
This controller is designed to run alongside the AMM simulation environment and provides a bridge between:
On-chain trading activity (Uniswap V3 swaps)
Off-chain eligibility logic (cohorts, thresholds, cumulative behavior)
On-chain incentives (NFT minting)
The controller is intentionally off-chain to enable:
Fast iteration on eligibility rules
Rich simulation logic
Deterministic replay in local environments
Gradual migration of selected logic on-chain later if desired
Reward Controller Architecture
Location
scripts/controller/reward_controller_amm_swaps.js
Responsibilities
The reward controller:
Connects to a JSON-RPC provider (HTTP) for reads and transactions
Connects to a WebSocket provider for new block notifications
Watches a specific Uniswap V3 pool for Swap events
Attributes swaps to wallet addresses
Accumulates per-wallet swap volume
Applies cohort gating and threshold logic
Mints a single NFT per eligible wallet
Persists progress and cumulative state off-chain
The controller is network-aware and runs unmodified against:
A local Hardhat node (recommended for simulation)
Sepolia testnet (for validation)
Network-Aware Configuration
All scripts—including the reward controller—resolve configuration through a shared environment resolver.
Key characteristics:
A single NETWORK variable (local or sepolia) selects the active environment
Network-specific variables are prefixed (e.g. LOCAL_…, SEPOLIA_…)
Scripts consume resolved values (TOKEN_ADDRESS, POOL_ADDRESS, RPC/WSS URLs) without conditional logic
Pool token ordering is validated at startup to prevent silent misconfiguration
This prevents common failure modes such as:
Watching a different pool than the one being traded
Using stale token or pool addresses after redeployment
Accidentally mixing local and testnet state
Swap-Based Eligibility (Current State)
The controller currently supports swap-driven eligibility, with legacy balance-based logic gated behind feature flags.
Swap Observation
Listens to Swap events emitted by the configured Uniswap V3 pool
Logs swap metadata (block, tx hash, recipient, signed amounts)
Correctly interprets signed amount0 / amount1 values
Supports direct pool swaps executed via PoolSwapExecutor
Attribution Model
Swap attribution is based on the recipient field emitted by the pool
This aligns with the executor pattern where each trader deploys an executor bound to a payer address
This model scales naturally to multi-wallet simulations
Cumulative Accounting
Swap amounts are accumulated per wallet in base token units
State is persisted off-chain in a network-specific JSON file
Progress is tracked via lastProcessedBlock to allow safe restarts
Confirmation depth is respected to avoid reorg-related inconsistencies
Minting Behavior
Once a wallet’s cumulative eligible swap volume exceeds the configured threshold:
A single NFT is minted
Subsequent swaps from that wallet are ignored
Minting is guarded by an on-chain hasMinted check to ensure idempotency
Cohort Gating (A/B Control)
The reward controller includes deterministic cohort assignment:
Wallets are hashed into buckets using a salt
A configurable percentage is marked “eligible”
The remainder form a control cohort that will never mint
Assignment is deterministic and reproducible across runs
This enables:
A/B testing of incentive effects
Counterfactual analysis in simulations
Clean separation of “behavior” vs “reward”
Legacy Balance-Based Minting (Deprecated)
A legacy ERC-20 balance-based minting path still exists in the controller but is disabled by default.
Purpose:
Backward compatibility
Controlled comparison against swap-based eligibility
Safe rollback during development
This logic is explicitly gated via environment flags and can be fully removed once swap-based eligibility is finalized.
Multi-Wallet Simulation Compatibility
The AMM + controller stack is now verified to support:
Multiple independent trading wallets
One swap executor per wallet (payer-bound)
Correct attribution of swaps per wallet
Independent eligibility tracking per wallet
This design is essential for:
Agent-based simulations
Heterogeneous trading strategies
Distributional analysis of incentives
Operational Notes (Local Development)
Closing terminals does not require redeployment as long as the Hardhat node remains running
Restarting the controller is safe; state is resumed from disk
Resetting controller state (.reward_state_*.json) forces reprocessing and re-minting (use cautiously)
Pool/token mismatches are detected and fail fast
Current Status
At this point, the system supports:
Deterministic local Uniswap V3 pools
Concentrated liquidity
Direct pool swaps
Multi-wallet trading
Swap-based NFT incentives
Cohort-gated eligibility
Network-safe configuration
The system is now suitable for:
Large-scale local simulations
Iterative tuning of incentive thresholds
Behavioral experimentation using real AMM mechanics
