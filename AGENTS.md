# Agent Instructions

Before making behavioral or documentation changes, read `PROJECT_SPEC.md`. It is the stable source of truth for the project design.

Do not reintroduce older designs that conflict with `PROJECT_SPEC.md`, especially:

- Cumulative-bought NFT threshold logic as the primary incentive rule.
- Two-regime-only market logic.
- Launch-multiplier/decay knobs that duplicate the hype regime.
- Cohort reports that mix incompatible eligible/control definitions without labeling.

If a core behavior intentionally changes, update both `PROJECT_SPEC.md` and `README.md` in the same change.

Do not commit generated or sensitive files, including `.env`, private keys, SQLite DBs, `sim/out/`, `sim/reports/`, Hardhat artifacts, caches, reward controller logs, PID files, or reward state JSON.

Recommended validation before handoff:

```bash
npm test
npm audit --omit=dev
python -m compileall sim
```
