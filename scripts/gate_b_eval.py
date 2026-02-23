import json
import sys
from pathlib import Path

if len(sys.argv) != 2:
    raise SystemExit("Usage: uv run python scripts/gate_b_eval.py <RUN_ID>")

run_id = sys.argv[1]
root = Path("/mnt/bulk/spy2_data/artifacts/backtests") / run_id

s = json.loads((root / "summary.json").read_text())
fs = json.loads((root / "fill_sensitivity.json").read_text())

models = fs.get("models", {})
keys = ["conservative", "mid_with_slippage", "spread_inside_with_slippage"]

values: list[tuple[str, float, int | None]] = []
for key in keys:
    model = models.get(key)
    if isinstance(model, dict) and model.get("final_cash") is not None:
        values.append((key, float(model["final_cash"]), model.get("trade_count")))

if not values:
    raise SystemExit("No eligible models found in fill_sensitivity.json")

worst = min(values, key=lambda x: x[1])
print("run_id:", s.get("run_id"))
print("window:", s.get("start"), "..", s.get("end"))
print("worst_slippage_aware:", worst)

# Gate B threshold
sys.exit(0 if worst[1] >= 1000.0 else 2)
