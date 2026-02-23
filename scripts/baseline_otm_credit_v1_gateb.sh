#!/usr/bin/env bash
set -euo pipefail
export TZ="${TZ:-UTC}"

ROOT="${1:-/mnt/bulk/spy2_data}"

START="${START:-2025-09-02}"
END="${END:-2026-02-09}"

STRATEGY="${STRATEGY:-baseline_otm_credit}"
RIGHT="${RIGHT:-P}"
WIDTH="${WIDTH:-1.0}"
STRUCTURE="${STRUCTURE:-credit}"

FILL_MODEL="${FILL_MODEL:-conservative}"
FILL_ALPHA="${FILL_ALPHA:-0.5}"
SBP="${SBP:-20}"

SEL_DTE_MIN="${SEL_DTE_MIN:-21}"
SEL_DTE_MAX="${SEL_DTE_MAX:-45}"
OTM="${OTM:-0.020}"
MC="${MC:-0.36}"

PT="${PT:-0.5}"
SL="${SL:-0.5}"
MAX_HOLD="${MAX_HOLD:-10}"
FCDTE="${FCDTE:-1}"

FILL_SENSITIVITY="${FILL_SENSITIVITY:-1}"
fill_sensitivity_flag=()
case "${FILL_SENSITIVITY,,}" in
  1|true|yes|on) fill_sensitivity_flag+=(--fill-sensitivity) ;;
  0|false|no|off) fill_sensitivity_flag+=(--no-fill-sensitivity) ;;
  *)
    echo "Invalid FILL_SENSITIVITY=${FILL_SENSITIVITY} (use 1/0, true/false, yes/no, on/off)." >&2
    exit 2
    ;;
esac

uv run python -m spy2 backtest run \
  --start "$START" --end "$END" \
  --root "$ROOT" \
  --strategy "$STRATEGY" \
  --right "$RIGHT" --width "$WIDTH" \
  --structure "$STRUCTURE" \
  --fill-model "$FILL_MODEL" --fill-alpha "$FILL_ALPHA" \
  --slippage-bps "$SBP" \
  "${fill_sensitivity_flag[@]}" \
  --sel-dte-min "$SEL_DTE_MIN" --sel-dte-max "$SEL_DTE_MAX" \
  --sel-otm-pct "$OTM" --sel-min-credit "$MC" \
  --exit-profit-take-frac "$PT" --exit-stop-loss-frac "$SL" \
  --exit-max-hold-sessions "$MAX_HOLD" \
  --force-close-dte "$FCDTE"
