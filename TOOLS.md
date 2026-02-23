# TOOLS.md

## Core Runtime
- Python/package runner: `uv`
- Repo root: `/home/karson/SPY_2`
- Data root (default): `/mnt/bulk/spy2_data`

## Web Research
- Search provider: Brave (configured in OpenClaw global config)
- Requirement: deep, cross-checked research for high-impact decisions

## IBKR (Paper Only)
- Gateway paper host/port: `127.0.0.1:4002`
- TWS paper host/port: `127.0.0.1:7497`
- Never use live ports (`4001`, `7496`)

## High-Value Commands
- IBKR paper connectivity check:
  - `uv run python -m spy2 ibkr check --gateway --confirm-read-only-unchecked`
- Backtest run (example skeleton):
  - `uv run python -m spy2 backtest run --start <YYYY-MM-DD> --end <YYYY-MM-DD> --root /mnt/bulk/spy2_data ...`
- Test suite:
  - `uv run pytest -q`
