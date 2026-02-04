from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, TypedDict


def _repo_root(start: Path | None = None) -> Path:
    if start is None:
        start = Path.cwd()
    for path in [start, *start.parents]:
        if (path / "pyproject.toml").is_file():
            return path
    return start


def _require_pyarrow_dataset():
    try:
        import pyarrow.dataset as ds  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:  # pragma: no cover - import guard
        raise SystemExit(
            "Missing dependency 'pyarrow' in the current environment. "
            "Install with `uv run pip install pyarrow`."
        ) from exc
    return ds


def validate_day(
    date_str: str,
    *,
    quotes_schema: str = "cbbo-1m",
    root: Path | None = None,
) -> Path:
    try:
        trade_date = dt.date.fromisoformat(date_str)
    except ValueError as exc:
        raise SystemExit(f"Invalid date '{date_str}'. Use YYYY-MM-DD.") from exc

    root = _repo_root(root)
    artifacts_dir = root / "artifacts" / "validation"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    ds = _require_pyarrow_dataset()

    class ValidationSpec(TypedDict):
        name: str
        dataset: str
        schema: str
        required: set[str]

    specs: list[ValidationSpec] = [
        {
            "name": "underlying_spy",
            "dataset": "EQUS.MINI",
            "schema": "ohlcv-1m",
            "required": {
                "ts_event",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "symbol",
            },
        },
        {
            "name": "opra_definition",
            "dataset": "OPRA.PILLAR",
            "schema": "definition",
            "required": {
                "symbol",
                "underlying",
                "strike_price",
                "expiration",
            },
        },
        {
            "name": "opra_quotes",
            "dataset": "OPRA.PILLAR",
            "schema": quotes_schema,
            "required": {
                "ts_event",
                "symbol",
                "bid_px_00",
                "ask_px_00",
            },
        },
    ]

    results: list[dict[str, Any]] = []
    failures: list[str] = []

    for spec in specs:
        dataset_name = spec["dataset"]
        schema_name = spec["schema"]
        required = spec["required"]
        path = (
            root
            / "data"
            / "raw"
            / dataset_name
            / schema_name
            / f"date={trade_date.isoformat()}"
        )
        entry: dict[str, Any] = {
            "name": spec["name"],
            "dataset": dataset_name,
            "schema": schema_name,
            "path": str(path.relative_to(root)),
            "ok": False,
        }
        if not path.exists():
            entry["error"] = "missing partition directory"
            failures.append(f"{dataset_name} {schema_name} missing")
            results.append(entry)
            continue

        dataset = ds.dataset(str(path), format="parquet")
        fields = set(dataset.schema.names)
        missing = sorted(required - fields)
        entry["fields"] = sorted(fields)
        entry["missing_fields"] = missing
        entry["rows"] = dataset.count_rows()
        entry["ok"] = not missing
        if missing:
            failures.append(f"{dataset_name} {schema_name} missing fields: {missing}")
        results.append(entry)

    payload = {
        "date": trade_date.isoformat(),
        "validated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "quotes_schema": quotes_schema,
        "results": results,
    }

    output_path = artifacts_dir / f"validate_{trade_date.isoformat()}.json"
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

    if failures:
        raise SystemExit(
            "Validation failed:\n" + "\n".join(f"- {failure}" for failure in failures)
        )
    return output_path
