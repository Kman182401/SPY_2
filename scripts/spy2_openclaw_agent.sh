#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 \"<task message>\""
  exit 1
fi

openclaw agent \
  --agent spy2-pro \
  --thinking high \
  --message "$*" \
  --json
