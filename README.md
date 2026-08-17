# MyToken AMM (Uniswap V3 – Direct Pool Interaction)

## Overview

This repository contains a custom ERC-20 token, an NFT incentive mechanism, and a Uniswap V3–compatible AMM setup designed specifically for **local simulation and testnet validation**.

Instead of relying on Uniswap routers, this project interacts **directly with Uniswap V3 pools**. This approach:

- Eliminates router inconsistencies across networks
- Produces identical AMM math (price impact, ticks, liquidity behavior)
- Is deterministic, faster, and easier to simulate at scale
- Is well-suited for automated multi-wallet trading simulations

The architecture mirrors Uniswap V3 core mechanics while remaining intentionally minimal and controlled.

For stable design decisions and future-agent guardrails, see `PROJECT_SPEC.md`.

---

## Repository Structure

    contracts/
    ├─ MyToken.sol
    ├─ JSTVIP.sol
    ├─ PoolSwapExecutor.sol
    ├─ WETH9.sol
    └─ interfaces/
       ├─ IUniswapV3FactoryMinimal.sol
       ├─ IUniswapV3PoolMinimal.sol
       ├─ INonfungiblePositionManagerMinimal.sol
       └─ IWETH9Minimal.sol

    scripts/
    ├─ deploy.ts
    ├─ env.js
    └─ amm/
       ├─ 00_deploy_uniswap_local.ts
       ├─ 02_create_and_init_pool.ts
       ├─ 03_mint_liquidity.ts
       └─ 06_direct_pool_swap.ts

    scripts/controller/
    └─ reward_controller_amm_swaps.js

    scripts/nft/
    ├─ deploy_jstvip.js
    └─ grant_minter.js

    sim/
    ├─ run_sim.py
    ├─ post_run.py
    ├─ report.py
    └─ READ_ME_sim.md

    test/
    └─ MyToken.test.ts

    config/
    └─ uniswap.sepolia.ts

---

## Quick Start

Install dependencies and run the Solidity test suite:

    npm ci
    npm test

Compile contracts:

    npm run compile

Create a local environment file from the committed template:

    cp .env.example .env

The `.env` file is intentionally ignored by git. For local work, use Hardhat development accounts only. Never place a private key that controls real funds or production roles in this repo.

For local AMM/simulation work, start a Hardhat node in one terminal:

    npx hardhat node

Then deploy local Uniswap V3 support contracts, the token, the NFT contract, pool, and liquidity from another terminal. The scripts print the addresses that must be copied into your local `.env` file:

    npx hardhat run scripts/amm/00_deploy_uniswap_local.ts --network localhost
    NETWORK=local npx hardhat run scripts/deploy.ts --network localhost
    NETWORK=local npx hardhat run scripts/nft/deploy_jstvip.js --network localhost
    NETWORK=local npx hardhat run scripts/amm/02_create_and_init_pool.ts --network localhost
    NETWORK=local npx hardhat run scripts/amm/03_mint_liquidity.ts --network localhost

For the NFT deploy script, set `JSTVIP_ADMIN` and `JSTVIP_MINTER` in `.env`. In local simulation, these can be Hardhat-controlled addresses. After deployment, set `LOCAL_JSTVIP_ADDRESS` to the deployed NFT address. Also set the local AMM/token values printed by the deployment scripts:

- `LOCAL_UNISWAP_V3_FACTORY`
- `LOCAL_UNISWAP_V3_POSITION_MANAGER`
- `LOCAL_WETH_ADDRESS`
- `LOCAL_TOKEN_ADDRESS`
- `LOCAL_JSTVIP_ADDRESS`
- `LOCAL_UNISWAP_V3_POOL_ADDRESS`
- `LOCAL_POOL_TOKEN0_ADDRESS`
- `LOCAL_POOL_TOKEN1_ADDRESS`

Verify the end-to-end pipeline with a small smoke simulation:

    SIM_NUM_AGENTS=6 SIM_MAX_AGENTS=12 THRESHOLD_TOKENS=0.00001 python -m sim.run_sim --num-days 2

The low threshold in this smoke command is intentional: it forces the reward controller path to mint NFTs during a short run. For normal simulations, use the threshold in `.env`.

Run a normal simulation with the defaults from `.env` and `sim/config.py`:

    python -m sim.run_sim --num-days 45

For a larger employer/demo-style run similar to the tuned 45-day outputs, source the committed safe preset after your local `.env` values are loaded:

    set -a
    source .env
    source sim/presets/employer_45d.env
    set +a
    python -m sim.run_sim --num-days 45

The preset contains only simulation knobs such as population scale, regime probabilities, market-noise settings, and the NFT threshold. It does not contain private keys, addresses, RPC credentials, or deployment-specific values.

`sim/run_sim.py` writes a run database under `sim/out/<run_id>/`, runs post-processing, appends to `sim/warehouse.db`, and generates plots under `sim/reports/<timestamp>/`.

Generate reports manually from the warehouse:

    python -m sim.report --warehouse sim/warehouse.db

Generated outputs are intentionally ignored by git.

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
- A role-gated ERC-721 incentive NFT
- A Uniswap V3 pool
- Concentrated liquidity
- Direct AMM swaps
- A multi-wallet AMM simulation pipeline
- Cohort-based incentive analytics

It is intentionally designed to be boring, predictable, and correct — ideal for simulation-driven protocol design.

---

## Reward Controller and NFT Incentives

The project includes an off-chain reward controller at:

    scripts/controller/reward_controller_amm_swaps.js

The controller is designed to run alongside the AMM simulation environment. It bridges:

- On-chain trading activity from Uniswap V3 `Swap` events.
- Deterministic off-chain cohort eligibility.
- On-chain JSTVIP NFT minting.

Current incentive rule:

- The controller watches the configured Uniswap V3 pool.
- It attributes buys to swap recipients.
- It assigns wallets to eligible/control cohorts using `COHORT_SALT` and `COHORT_ELIGIBLE_PERCENT`.
- Eligible wallets mint at most one JSTVIP NFT.
- The mint threshold is based on current held TOKEN balance, not cumulative bought volume.
- Cumulative buy accounting is retained for diagnostics only.
- Minting is guarded by controller state and the NFT contract's `hasMinted` check.

This design is intentionally off-chain so eligibility logic can be iterated quickly in simulation while still producing real on-chain mint events in local or testnet environments.

The simulation runner can start the reward controller automatically:

    SIM_START_REWARD_CONTROLLER=true

When enabled, controller logs and state are written under the run output directory. These runtime files are ignored by git.

---

## Cohort and Incentive Metrics

The reporting pipeline includes plots for evaluating whether the NFT incentive is associated with different behavior across cohorts. Current cohort reporting uses static groups:

- `eligible & hit threshold`
- `eligible & not hit threshold`
- `control`

Implemented cohort views include:

- Rolling 7-day repeat-buy rate.
- Retention/survival by cohort.
- Buy-count and token-bought distributions.
- Median token held by cohort.
- Daily median net token flow per wallet.
- Threshold-aligned event windows.

These are useful for demonstrating the analysis framework. They should be interpreted as simulation evidence, not proof of real-world causal lift.

---

## Environment and Generated Files

Required deployment/runtime values belong in a local `.env` file and must not be committed. Network-specific values are prefixed with `LOCAL_` or `SEPOLIA_`.

Important local variables include:

- `NETWORK=local`
- `LOCAL_RPC_URL`
- `LOCAL_WSS_URL`
- `LOCAL_TOKEN_ADDRESS`
- `LOCAL_JSTVIP_ADDRESS`
- `LOCAL_UNISWAP_V3_POOL_ADDRESS`
- `LOCAL_POOL_TOKEN0_ADDRESS`
- `LOCAL_POOL_TOKEN1_ADDRESS`
- `LOCAL_WETH_ADDRESS`
- `COHORT_SALT`
- `THRESHOLD_TOKENS`

Generated simulation databases, reports, controller logs, controller state files, Hardhat artifacts, caches, and private environment files are ignored by `.gitignore`.
