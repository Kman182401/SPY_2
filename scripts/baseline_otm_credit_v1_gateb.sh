#!/usr/bin/env bash
set -euo pipefail
export TZ=UTC

ROOT="${1:-/mnt/bulk/spy2_data}"

uv run python -m spy2 backtest run \
  --start "${START:-2025-09-02}" --end "${END:-2026-02-09}" \
  --root "$ROOT" \
  --strategy baseline_otm_credit \
  --right P --width 1.0 \
  --structure credit \
  --fill-model conservative --fill-alpha 0.5 \
  --slippage-bps 20 \
  --fill-sensitivity \
  --sel-dte-min 21 --sel-dte-max 45 \
  --sel-otm-pct 0.020 --sel-min-credit 0.36 \
  --exit-profit-take-frac 0.5 --exit-stop-loss-frac 0.5 \
  --exit-max-hold-sessions 10 \
  --force-close-dte 1
