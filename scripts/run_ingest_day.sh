#!/usr/bin/env bash
set -euo pipefail

DAY="${1:?usage: run_ingest_day.sh YYYY-MM-DD [DATA_ROOT]}"
ROOT="${2:-${SPY2_DATA_ROOT:-}}"
LOG_DIR="artifacts/logs"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
LOG="$LOG_DIR/ingest_${DAY}_${TS}.log"
ROOT_ARG=()
if [[ -n "${ROOT}" ]]; then
  ROOT_ARG=(--root "${ROOT}")
fi
CMD=(uv run python -m spy2 databento ingest "$DAY" --auto-clamp "${ROOT_ARG[@]}")

mkdir -p "$LOG_DIR"
printf "\033[1;34m▶ START\033[0m %s | %s\n" "$TS" "$PWD"
printf "\033[1;36m▶ CMD  \033[0m %s\n" "${CMD[*]}"
printf "\033[1;36m▶ LOG  \033[0m %s\n" "$LOG"

start_ts="$(date +%s)"
(
  stdbuf -oL -eL "${CMD[@]}" 2>&1 \
    | sed -u 's/^/[spy2] /' \
    | tee "$LOG"
) & pid=$!

trap 'kill "$pid" 2>/dev/null || true' INT TERM

spin='|/-\'
i=0
while kill -0 "$pid" 2>/dev/null; do
  c="${spin:$((i % 4)):1}"
  printf "\r\033[1;33m[%s] running... pid=%s\033[0m\033[K" "$c" "$pid"
  i=$((i + 1))
  sleep 0.5
done

wait "$pid"; rc=$?
dur="$(( $(date +%s) - start_ts ))"
printf "\n\033[1;32m▶ DONE\033[0m rc=%s dur=%ss log=%s\n" "$rc" "$dur" "$LOG"
exit "$rc"
