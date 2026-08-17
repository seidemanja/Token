# Project Spec

This file is the stable project contract. Update it only when a core design decision changes intentionally. It exists to prevent future context drift and accidental rollback to older designs.

## Purpose

This repository demonstrates a token launch simulation stack built around real Uniswap V3 mechanics:

- ERC-20 token and ERC-721 incentive NFT contracts.
- Direct Uniswap V3 pool swaps through a minimal executor.
- Local multi-wallet agent simulation against on-chain AMM state.
- Post-run indexing, analytics, and reporting for market behavior and incentive effects.

## Non-Goals

- This is not a production mainnet launch repo.
- This is not intended to store private keys, `.env` files, generated databases, simulation outputs, or runtime controller state.
- The simulation is a behavioral testbed, not a claim that one parameter set predicts a real market.

## Contract Architecture

- `contracts/MyToken.sol` is a fixed-supply ERC-20 with pausable transfers and owner administration.
- `contracts/JSTVIP.sol` is the ERC-721 incentive NFT with role-gated minting.
- `contracts/PoolSwapExecutor.sol` executes direct Uniswap V3 pool swaps and implements the swap callback.
- Minimal Uniswap interfaces are used to avoid unnecessary dependency surface.

## AMM Architecture

- The project intentionally uses direct Uniswap V3 pool interaction instead of routers.
- Direct pool swaps preserve real Uniswap V3 price impact, ticks, liquidity behavior, and swap callback semantics.
- Local simulations run against a Hardhat node and produce real on-chain Swap events.
- Sepolia is for validation only, not high-volume simulation.

## Simulation Architecture

- `sim/run_sim.py` is the main simulation runner.
- It creates and funds agent wallets, deploys one payer-bound `PoolSwapExecutor` per agent, executes trades over simulated days, and writes a per-run SQLite database.
- `sim/post_run.py` runs after the simulation and extracts swaps, prices, mints, cohorts, wallet activity, wallet balances, and warehouse rows.
- `sim/report.py` generates market and cohort plots from `sim/warehouse.db`.
- Generated DBs, reports, and run outputs are intentionally ignored by git.

## Market Model

- Regimes are `hype`, `bull`, and `bear`.
- Simulations start in hype for a configurable 10-15 day launch window.
- Hype can exit into bull or bear according to configured probabilities.
- Later hype re-entry is rare and stochastic.
- Bull and bear durations are stochastic through persistence probabilities, not fixed cycles.
- Sentiment is token-specific latent sentiment driven by regime.
- Fair value follows sentiment, noise, and mean reversion.
- Trade flow responds to mispricing, sentiment/regime effects, and noise.

## NFT Incentive Design

- Cohort assignment is deterministic using wallet address, `COHORT_SALT`, and `COHORT_ELIGIBLE_PERCENT`.
- Eligible wallets can receive one JSTVIP NFT when they cross the threshold.
- The threshold basis is current held TOKEN balance, not cumulative bought volume.
- The reward controller observes AMM Swap events, then checks the buyer's held TOKEN balance before minting.
- Cumulative buy tracking is retained for diagnostics only.
- Minting is idempotent through controller state and the NFT contract's `hasMinted` guard.
- Simulation analytics latch threshold-hit status once reached so pre/post segmentation is stable.

## Cohort Behavior Model

- Eligible wallets receive a stronger pre-threshold buy weight.
- Post-threshold buy behavior is baseline-equivalent to control.
- Sell and churn behavior are baseline-equivalent between eligible and control wallets.
- This isolates the modeled incentive effect to pre-threshold acquisition/engagement pressure.

## Reporting Spec

Core market reports include:

- Holder count over time.
- Price versus latent fair value.
- Price/fair-value spread.
- Normalized price path.
- Daily volume and swap count.
- Daily return distribution and tail diagnostics.
- Rolling volatility versus volume.
- Top-10 balance concentration.
- Regime trace and sentiment.

Cohort reports use static groups:

- `eligible & hit threshold`.
- `eligible & not hit threshold`.
- `control`.

Cohort plots should prefer medians and confidence intervals where feasible to reduce outlier distortion. The incentive readout is behavioral, not causal proof by itself. Relevant plots include repeat-buy rate, retention/survival, buy intensity, median token held, net token flow, and threshold-aligned event windows.

## Repository Hygiene

- Do not commit `.env`, private keys, SQLite DBs, `sim/out/`, `sim/reports/`, Hardhat artifacts, caches, controller logs, controller PID files, or reward state JSON.
- If any secret is ever committed, assume it is compromised even after history cleanup.
- Before public or reviewer-facing handoff, run at minimum `npm test`, `npm audit --omit=dev`, and a Python syntax/import check for `sim/`.
