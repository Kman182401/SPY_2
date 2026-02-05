#!/usr/bin/env bash
set -euo pipefail

DAY="${1:?usage: golden_baseline.sh YYYY-MM-DD [DATA_ROOT]}"
ROOT="${2:-${SPY2_DATA_ROOT:-}}"

ROOT_ARG=()
if [[ -n "${ROOT}" ]]; then
  ROOT_ARG=(--root "${ROOT}")
fi

make qa
scripts/run_ingest_day.sh "$DAY" "${ROOT:-}"
uv run python -m spy2 data validate-day "$DAY" "${ROOT_ARG[@]}"
uv run python -m spy2 snapshots head "$DAY" --n 3 "${ROOT_ARG[@]}"
uv run python -m spy2 backtest demo "$DAY" --time 14:30 "${ROOT_ARG[@]}"
