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
