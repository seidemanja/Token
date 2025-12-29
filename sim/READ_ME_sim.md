
This repo now supports:
1) Running multi-wallet Uniswap V3 swap simulations on a local persistent node
2) Indexing on-chain Swap + Mint events into a per-run SQLite DB
3) Computing daily market/price aggregates
4) Importing the off-chain reward controller state into the run DB
5) Cohort-gating analysis and wallet-level activity features

Each simulation run outputs to:
`sim/out/<run_id>/`
- `sim.db` (SQLite facts + derived analytics)
- `trades.jsonl` (ground-truth trade intents + executed inputs)
- `manifest.json` (run configuration snapshot)
- `reward_state_local.json` (optional copy of controller state)

`sim/out/latest.txt` always points to the latest run directory.

---

## Core scripts and what they do

### 1) `sim/run_sim.py`
Phase-0 simulation runner that:
- creates N agent wallets
- funds ETH, wraps to WETH, seeds TOKEN
- deploys a payer-bound `PoolSwapExecutor` per agent
- runs for `SIM_NUM_DAYS`, each day sampling BUY/SELL actions
- writes:
  - SQLite `sim_runs`, `agents`, `trades`
  - JSONL `trades.jsonl`
  - `manifest.json`

Also enforces *hard safety caps* on trade amounts:
- `SIM_MAX_BUY_WETH`
- `SIM_MAX_SELL_TOKEN`
and logs both sampled and clamped amounts for auditing.

### 2) `sim/post_run.py`
“One command” post-processing for the latest run DB. It orchestrates:
- cohort computation (`compute_cohorts`)
- optional reward state import (`import_reward_state`) if `reward_state_local.json` exists
- run-scoped swap extraction (`extract_swaps`) using the run’s start/end block window
- price computation (`compute_prices`)
- NFT mint log extraction (`extract_mints`)
- wallet activity features (`compute_wallet_activity`)

It writes/updates these tables in the run’s `sim.db`:
- `run_wallets`
- `wallet_cohorts`
- `reward_wallets` (if reward state imported)
- `run_stats`
- `swaps`
- `daily_market`
- `swap_prices`
- `daily_prices`
- `nft_mints`
- `wallet_activity`

### 3) `sim/compute_cohorts.py`
Creates/refreshes:
- `run_wallets(run_id, address)` from `agents`
- `wallet_cohorts(run_id, address, bucket, eligible)` using env:
  - `COHORT_ENABLED`
  - `COHORT_ELIGIBLE_PERCENT`
  - `COHORT_SALT`
Logic matches the JS reward controller cohort hash.

### 4) `sim/extract_swaps.py`
Lightweight local “indexer” for Uniswap V3 Swap logs:
- input: `<db> <from_block> <to_block>`
- inserts into `swaps`
- computes `daily_market` using `blocks_per_day=100`
- writes run-scoping values to `run_stats`:
  - `day0_block`
  - `extract_from_block`
  - `extract_to_block`

### 5) `sim/compute_prices.py` + `sim/price.py`
Reads `swaps`, computes:
- per-swap `price_weth_per_token`
- `normalized_price` anchored by an explicit policy (stored in `run_stats`)
Writes:
- `swap_prices`
- `daily_prices` (day bucketing uses `run_stats.day0_block`)

### 6) `sim/extract_mints.py`
Indexes JSTVIP/NFT mint events into:
- `nft_mints(to_address, block_number, tx_hash, log_index, ...)`
Used to compare:
- controller “minted_onchain” state vs
- mint truth derived from logs

### 7) `sim/import_reward_state.py`
Imports the JS controller state JSON (`reward_state_local.json`) into:
- `reward_wallets`
Fields include:
- wallet
- cumulative buy totals
- cohort eligibility
- threshold reached
- minted cache vs minted on-chain checks
- status label (human readable segmentation)

### 8) `sim/compute_wallet_activity.py`
Computes wallet-level features (schema depends on current version) from on-chain/DB facts, such as:
- first buy day
- counts of buys/sells (if implemented)
- other run-scoped wallet metrics
