
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
Simulation runner that:
- creates N agent wallets
- funds ETH, wraps to WETH, seeds TOKEN
- deploys a payer-bound `PoolSwapExecutor` per agent
- runs for `SIM_NUM_DAYS`, each day updating:
  - regime (`R_t`) via a 3-state Markov chain (`hype`, `bull`, `bear`)
    with forced initial hype days and rare later hype re-entry
  - sentiment (`S_t`) as a regime-conditioned smoother
  - fair value (`v_t`) with drift + sentiment tilt + noise
  - net flow from mispricing (`v_t - p_t`)
- writes:
  - SQLite `sim_runs`, `agents`, `trades`
  - JSONL `trades.jsonl`
  - `manifest.json`
- automatically triggers `sim/post_run.py` (which in turn appends to the warehouse) when the simulation finishes
- automatically runs `sim/report.py` and writes plots to `sim/reports/<run_id>/`
- optional CLI override: `--num-days` to run longer/shorter than `SIM_NUM_DAYS` for a single run
- automatically starts the JS reward controller (unless `SIM_START_REWARD_CONTROLLER=false`)

Model equations (core):
- `S_t = (1-a)S_(t-1) + a*mu(R_t)`, where `mu(hype) > mu(bull) > mu(bear)`
- `v_t = v_(t-1) + fair_mu + fair_beta*S_t + fair_sigma*eps_t`
- `F_t = impact_kappa * (flow_intensity*tanh((v_t-p_t)/flow_mispricing_scale)*regime_factor(R_t) + flow_noise_sigma*z_t)`
- `F_t` is converted to BUY/SELL orders and executed on-chain with existing slippage/cap guardrails

Core dynamics parameters:
- `SIM_REGIME_BULL_PERSIST`
- `SIM_REGIME_BEAR_PERSIST`
- `SIM_HYPE_INITIAL_MIN_DAYS`
- `SIM_HYPE_INITIAL_MAX_DAYS`
- `SIM_HYPE_PERSIST_START`
- `SIM_HYPE_PERSIST_FLOOR`
- `SIM_HYPE_DECAY_TAU`
- `SIM_HYPE_EXIT_TO_BULL_PROB`
- `SIM_HYPE_REENTRY_PROB`
- `SIM_SENTIMENT_ALPHA`
- `SIM_SENTIMENT_REGIME_LEVEL`
- `SIM_SENTIMENT_HYPE_MULT`
- `SIM_FAIR_MU`
- `SIM_FAIR_BETA`
- `SIM_FAIR_SIGMA`
- `SIM_FAIR_REVERSION`
- `SIM_FLOW_LAMBDA`
- `SIM_FLOW_THETA`
- `SIM_FLOW_RHO`
- `SIM_FLOW_SIGMA`
- `SIM_PRICE_KAPPA`

Operational controls (not part of core dynamics) still apply:
- `SIM_MAX_BUY_WETH`
- `SIM_MAX_SELL_TOKEN`
- `SIM_TICKS_PER_DAY`
- `SIM_MAX_TRADE_PCT_BUY`
- `SIM_MAX_TRADE_PCT_SELL`
- `SIM_MAX_SLIPPAGE`
- `SIM_AMM_FEE_PCT`
- `SIM_CIRC_SUPPLY_START`
- `SIM_CIRC_SUPPLY_DAILY_UNLOCK`

Reward controller auto-start (optional):
- `SIM_START_REWARD_CONTROLLER=true|false` (default true)
- When enabled, `sim/run_sim.py` launches `scripts/controller/reward_controller_amm_swaps.js`
  and writes `reward_controller.log` + `reward_controller.pid` in the run directory.

### 2) `sim/post_run.py`
“One command” post-processing for the latest run DB. It orchestrates:
- cohort computation (`compute_cohorts`)
- optional reward state import (`import_reward_state`) if `reward_state_local.json` exists
- run-scoped swap extraction (`extract_swaps`) using the run’s start/end block window
- price computation (`compute_prices`)
- NFT mint log extraction (`extract_mints`)
- wallet activity features (`compute_wallet_activity`)
- wallet balances per day (`compute_wallet_balances`)

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
- `wallet_balances_daily`
- `fair_value_daily`
- `perceived_fair_value_daily`
- `circulating_supply_daily`

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

### 9) `sim/compute_wallet_balances.py`
Reads `run_wallets` and `run_stats.day0_block` to fetch per-day token balances
for all run wallets, writing `wallet_balances_daily` (used for holder counts and
concentration diagnostics).

### 10) `sim/append_to_warehouse.py`
Appends run-level analytics into a cross-run SQLite warehouse (`sim/warehouse.db`):
- copies per-run tables (`agents`, `trades`, `swaps`, `swap_prices`, `daily_prices`, `daily_market`, `wallet_*`, `reward_wallets`, `nft_mints`, `run_stats`, `fair_value_daily`, `wallet_balances_daily`) into run_id-keyed warehouse tables
- records run metadata in `runs`
- writes a coarse `run_summary` (trades, swaps, cohorts, volumes, anchor price)

Usage:
```
python -m sim.append_to_warehouse              # uses sim/out/latest.txt
python -m sim.append_to_warehouse --run-db sim/out/<run_id>/sim.db --warehouse sim/warehouse.db
```
Re-running for the same `run_id` overwrites that run’s rows in the warehouse.

### 11) `sim/report.py`
Reads `sim/warehouse.db` and produces plots + text summaries for dashboard prep:
- normalized price paths (per-run) from `run_daily_prices`
- price vs fair value + spread
- price vs fair value vs perceived fair value (log)
- daily volume + swap counts (per-run)
- rolling volatility vs volume (per-run)
- return distribution histogram + CCDF (all runs)
- trade size histogram (BUY vs SELL, all runs)
- normalized price vs tick scatter + liquidity over time
- holder counts + top-10 concentration over time
- repeat-buy rate by cohort eligibility (if cohorts + wallet activity exist)
- daily WETH volume per run from `run_daily_market`
- trade outcomes (mined/reverted) and swaps/mints from `run_summary`
- writes plots + `summary.txt` under `sim/reports/<timestamp>/` and updates `sim/reports/latest.txt`

Usage:
```
python -m sim.report                                   # default warehouse + timestamped outdir
python -m sim.report --runs runA,runB                  # subset of run_ids
python -m sim.report --warehouse sim/warehouse.db --outdir sim/reports/mycheck
```
Dependency: `matplotlib` (install via `pip install matplotlib`).

### Automatic warehouse update
`sim/post_run.py` now appends the run to `sim/warehouse.db` automatically after all analytics steps complete. You can still run `sim.append_to_warehouse` manually if needed.
Carry forward wallets:
- `SIM_CONTINUE_FROM_LATEST=true|false` (default false)
- When true, `sim/run_sim.py` loads agents from the latest run DB and continues trading with those wallets (no re-funding or re-deploy).
