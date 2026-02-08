from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sys
import subprocess
import time
from pathlib import Path
from typing import Any, TypedDict

from spy2.common.paths import repo_root, resolve_root

db: Any | None = None
try:  # Lazy dependency surface for CLI help.
    import databento as _db  # type: ignore[import-untyped]

    db = _db
except ModuleNotFoundError:  # pragma: no cover - import guard
    db = None


def _resolve_api_key(api_key: str | None) -> str:
    if api_key:
        return api_key.strip()
    env_key = os.getenv("DATABENTO_API_KEY")
    if env_key:
        return env_key.strip()
    raise SystemExit(
        "Missing Databento API key. Set DATABENTO_API_KEY or pass --api-key."
    )


def _repo_root(start: Path | None = None) -> Path:
    return repo_root(start)


def _git_sha(root: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip() or None


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _parquet_row_count(path: Path) -> int:
    pq = _require_pyarrow()
    parquet = pq.ParquetFile(path)
    metadata = parquet.metadata
    return 0 if metadata is None else metadata.num_rows


def _historical_client(api_key: str | None) -> Any:
    _require_databento()
    assert db is not None
    return db.Historical(_resolve_api_key(api_key))


def _call_list_schemas(client: Any, dataset: str) -> list[str]:
    try:
        schemas = client.metadata.list_schemas(dataset)
    except AttributeError:
        schemas = client.list_schemas(dataset)
    return [str(schema) for schema in schemas]


def _call_dataset_range(client: Any, dataset: str) -> object:
    try:
        return client.metadata.get_dataset_range(dataset)
    except AttributeError:
        return client.get_dataset_range(dataset)


def _range_to_dict(range_info: object) -> dict[str, Any]:
    if isinstance(range_info, dict):
        return range_info
    for method in ("model_dump", "dict"):
        if hasattr(range_info, method):
            return getattr(range_info, method)()
    if hasattr(range_info, "__dict__"):
        return dict(range_info.__dict__)
    return {"value": str(range_info)}


def _parse_iso_datetime(value: str) -> dt.datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "." in text:
        prefix, suffix = text.split(".", 1)
        tz_part = ""
        frac_part = suffix
        if "+" in suffix:
            frac_part, tz_part = suffix.split("+", 1)
            tz_part = f"+{tz_part}"
        elif "-" in suffix and suffix.rfind("-") > 0:
            frac_part, tz_part = suffix.split("-", 1)
            tz_part = f"-{tz_part}"
        frac_part = (frac_part + "000000")[:6]
        text = f"{prefix}.{frac_part}{tz_part}"
    parsed = dt.datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def _extract_range(
    range_info: object, *, schema: str | None = None
) -> tuple[dt.datetime | None, dt.datetime | None, dict[str, Any]]:
    range_dict = _range_to_dict(range_info)
    start_value = range_dict.get("start")
    end_value = range_dict.get("end")

    if schema:
        schema_ranges = range_dict.get("schema")
        if isinstance(schema_ranges, dict):
            schema_info = schema_ranges.get(schema)
            if isinstance(schema_info, dict):
                start_value = schema_info.get("start", start_value)
                end_value = schema_info.get("end", end_value)

    if isinstance(start_value, dt.datetime):
        start_dt = start_value
    elif isinstance(start_value, dt.date):
        start_dt = dt.datetime.combine(start_value, dt.time.min, tzinfo=dt.timezone.utc)
    elif isinstance(start_value, str):
        start_dt = _parse_iso_datetime(start_value)
    else:
        start_dt = None

    if isinstance(end_value, dt.datetime):
        end_dt = end_value
    elif isinstance(end_value, dt.date):
        end_dt = dt.datetime.combine(end_value, dt.time.min, tzinfo=dt.timezone.utc)
    elif isinstance(end_value, str):
        end_dt = _parse_iso_datetime(end_value)
    else:
        end_dt = None

    if start_dt and start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=dt.timezone.utc)
    if end_dt and end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=dt.timezone.utc)
    return start_dt, end_dt, range_dict


def _require_databento() -> None:
    if db is None:
        raise SystemExit(
            "Missing dependency 'databento' in "
            f"{sys.executable}. Install with "
            "`uv run pip install databento` (or "
            f"`{sys.executable} -m pip install databento`)."
        )


def _require_pyarrow():
    try:
        import pyarrow.parquet as pq  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:  # pragma: no cover - import guard
        raise SystemExit(
            "Missing dependency 'pyarrow' in "
            f"{sys.executable}. Install with "
            "`uv run pip install pyarrow` (or "
            f"`{sys.executable} -m pip install pyarrow`)."
        ) from exc
    return pq


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value.strip())
    except ValueError as exc:
        raise SystemExit(f"Invalid {name}={value!r} (expected int).") from exc


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return float(value.strip())
    except ValueError as exc:
        raise SystemExit(f"Invalid {name}={value!r} (expected float).") from exc


def _retry_sleep_seconds(attempt: int, *, base: float, cap: float) -> float:
    # attempt=1 => base, attempt=2 => 2*base, ...
    return min(cap, base * (2 ** (attempt - 1)))


def list_schemas_to_artifact(
    dataset: str,
    api_key: str | None = None,
    *,
    root: Path | None = None,
) -> Path:
    client = _historical_client(api_key)
    try:
        schemas = _call_list_schemas(client, dataset)
    except Exception as exc:  # pragma: no cover - safety net for API errors
        raise SystemExit(f"Databento list-schemas failed: {exc}") from exc

    root = resolve_root(root)
    artifacts_dir = root / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    today = dt.date.today().isoformat()
    output_path = artifacts_dir / f"schemas_{today}.json"

    payload = {
        "dataset": dataset,
        "date": today,
        "retrieved_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "schema_count": len(schemas),
        "schemas": sorted(schemas),
    }
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return output_path


def ingest_day(
    date_str: str,
    api_key: str | None = None,
    quotes_schema: str = "cbbo-1m",
    *,
    root: Path | None = None,
    auto_clamp: bool = True,
) -> Path:
    try:
        trade_date = dt.date.fromisoformat(date_str)
    except ValueError as exc:  # pragma: no cover - argparse ensures format
        raise SystemExit(f"Invalid date '{date_str}'. Use YYYY-MM-DD.") from exc

    start_dt = dt.datetime.combine(trade_date, dt.time.min, tzinfo=dt.timezone.utc)
    end_dt = start_dt + dt.timedelta(days=1)

    repo = _repo_root()
    root = resolve_root(root)
    data_root = root / "data" / "raw"
    manifests_dir = root / "artifacts" / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)

    class IngestRequest(TypedDict):
        name: str
        dataset: str
        schema: str
        symbols: list[str]
        stype_in: str

    requests: list[IngestRequest] = [
        {
            "name": "underlying_spy",
            "dataset": "EQUS.MINI",
            "schema": "ohlcv-1m",
            "symbols": ["SPY"],
            "stype_in": "raw_symbol",
        },
        {
            "name": "opra_definition",
            "dataset": "OPRA.PILLAR",
            "schema": "definition",
            "symbols": ["SPY.OPT"],
            "stype_in": "parent",
        },
        {
            "name": "opra_quotes",
            "dataset": "OPRA.PILLAR",
            "schema": quotes_schema,
            "symbols": ["SPY.OPT"],
            "stype_in": "parent",
        },
        {
            "name": "opra_statistics",
            "dataset": "OPRA.PILLAR",
            "schema": "statistics",
            "symbols": ["SPY.OPT"],
            "stype_in": "parent",
        },
    ]

    client = _historical_client(api_key)
    run_started = dt.datetime.now(dt.timezone.utc)
    results: list[dict[str, object]] = []
    dataset_range_cache: dict[str, object] = {}

    # Keep ingest resilient to transient network / server errors. Configurable via env.
    max_retries = _env_int("SPY2_DATABENTO_MAX_RETRIES", 3)
    retry_base_seconds = _env_float("SPY2_DATABENTO_RETRY_BASE_SECONDS", 2.0)
    retry_cap_seconds = _env_float("SPY2_DATABENTO_RETRY_CAP_SECONDS", 30.0)
    max_attempts = max_retries + 1

    from databento.common.error import (  # type: ignore[import-not-found]
        BentoClientError,
        BentoServerError,
    )

    def _is_retryable(exc: Exception) -> bool:
        # Avoid retry loops for permanent errors (bad auth, entitlement, invalid params).
        if isinstance(exc, BentoClientError):
            return exc.http_status in (408, 429)
        if isinstance(exc, BentoServerError):
            return True
        # Non-HTTP exceptions are usually transport / streaming issues.
        return True

    def _get_dataset_range(dataset: str) -> object:
        if dataset in dataset_range_cache:
            return dataset_range_cache[dataset]

        last_exc: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                range_info = _call_dataset_range(client, dataset)
            except Exception as exc:  # pragma: no cover - API / transport errors
                last_exc = exc
                if attempt >= max_attempts or not _is_retryable(exc):
                    raise SystemExit(
                        f"Databento dataset-range failed for {trade_date.isoformat()} "
                        f"{dataset}: {exc}"
                    ) from exc

                sleep_s = _retry_sleep_seconds(
                    attempt, base=retry_base_seconds, cap=retry_cap_seconds
                )
                print(
                    f"{trade_date.isoformat()} {dataset} dataset-range: attempt "
                    f"{attempt}/{max_attempts} failed ({exc}); retrying in {sleep_s:.1f}s",
                    file=sys.stderr,
                )
                time.sleep(sleep_s)
            else:
                dataset_range_cache[dataset] = range_info
                return range_info

        # Defensive: should have returned or raised above.
        assert last_exc is not None
        raise SystemExit(
            f"Databento dataset-range failed for {trade_date.isoformat()} "
            f"{dataset}: {last_exc}"
        ) from last_exc

    for req in requests:
        dataset = req["dataset"]
        schema = req["schema"]
        symbols = req["symbols"]
        stype_in = req["stype_in"]
        output_dir = data_root / dataset / schema / f"date={trade_date.isoformat()}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "part-0000.parquet"
        tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")

        range_info = _get_dataset_range(dataset)
        range_start, range_end, range_raw = _extract_range(range_info, schema=schema)

        effective_start = start_dt
        effective_end = end_dt
        if auto_clamp:
            if range_start and range_start > effective_start:
                effective_start = range_start
            if range_end and range_end < effective_end:
                effective_end = range_end

        if effective_end <= effective_start:
            available = {
                "dataset": dataset,
                "schema": schema,
                "available_start": range_start.isoformat() if range_start else None,
                "available_end": range_end.isoformat() if range_end else None,
            }
            raise SystemExit(
                "No available data for requested window. "
                f"Requested {start_dt.isoformat()} -> {end_dt.isoformat()}. "
                f"Availability: {available}."
            )

        last_exc: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                if tmp_file.exists():
                    tmp_file.unlink()
                data = client.timeseries.get_range(
                    dataset=dataset,
                    schema=schema,
                    symbols=symbols,
                    stype_in=stype_in,
                    start=effective_start.isoformat(),
                    end=effective_end.isoformat(),
                )
                # Write atomically so a failed ingest doesn't corrupt previously-ingested data.
                data.to_parquet(
                    str(tmp_file),
                    pretty_ts=True,
                    map_symbols=True,
                )
                tmp_file.replace(output_file)
            except Exception as exc:  # pragma: no cover - API / transport errors
                last_exc = exc

                # Best-effort cleanup to avoid empty/misleading partitions.
                try:
                    if tmp_file.exists():
                        tmp_file.unlink()
                except OSError:
                    pass
                try:
                    if (
                        not output_file.exists()
                        and output_dir.exists()
                        and not any(output_dir.iterdir())
                    ):
                        output_dir.rmdir()
                except OSError:
                    pass

                if attempt >= max_attempts or not _is_retryable(exc):
                    raise SystemExit(
                        f"Databento ingest failed for {trade_date.isoformat()} "
                        f"{dataset} {schema}: {exc}"
                    ) from exc

                sleep_s = _retry_sleep_seconds(
                    attempt, base=retry_base_seconds, cap=retry_cap_seconds
                )
                print(
                    f"{trade_date.isoformat()} {dataset} {schema}: attempt "
                    f"{attempt}/{max_attempts} failed ({exc}); retrying in {sleep_s:.1f}s",
                    file=sys.stderr,
                )
                time.sleep(sleep_s)
            else:
                last_exc = None
                break
        if last_exc is not None:  # pragma: no cover - defensive
            raise SystemExit(
                f"Databento ingest failed for {trade_date.isoformat()} "
                f"{dataset} {schema}: {last_exc}"
            ) from last_exc

        if (not output_file.exists()) or output_file.stat().st_size == 0:
            availability = {
                "dataset": dataset,
                "schema": schema,
                "symbols": symbols,
                "requested_start": start_dt.isoformat(),
                "requested_end": end_dt.isoformat(),
                "effective_start": effective_start.isoformat(),
                "effective_end": effective_end.isoformat(),
                "available_start": range_start.isoformat() if range_start else None,
                "available_end": range_end.isoformat() if range_end else None,
            }
            raise SystemExit(
                "Databento returned no data for the requested window. "
                "This can happen on market holidays, if the symbol is invalid, "
                "or if your account lacks dataset entitlements. "
                f"Details: {availability}"
            )

        results.append(
            {
                "name": req["name"],
                "dataset": dataset,
                "schema": schema,
                "symbols": symbols,
                "stype_in": stype_in,
                "requested_start": start_dt.isoformat(),
                "requested_end": end_dt.isoformat(),
                "effective_start": effective_start.isoformat(),
                "effective_end": effective_end.isoformat(),
                "available_start": range_start.isoformat() if range_start else None,
                "available_end": range_end.isoformat() if range_end else None,
                "dataset_range": range_raw,
                "output": str(output_file.relative_to(root)),
                "rows": _parquet_row_count(output_file),
                "sha256": _sha256_file(output_file),
                "bytes": output_file.stat().st_size,
            }
        )

    manifest = {
        "run_started_at": run_started.isoformat(),
        "run_finished_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "date": trade_date.isoformat(),
        "git_sha": _git_sha(repo),
        "requests": results,
    }

    timestamp = run_started.strftime("%Y%m%dT%H%M%SZ")
    manifest_path = manifests_dir / f"ingest_{timestamp}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest_path


def ingest_range(
    start_date: str,
    end_date: str,
    api_key: str | None = None,
    quotes_schema: str = "cbbo-1m",
    *,
    root: Path | None = None,
    auto_clamp: bool = True,
) -> list[Path]:
    try:
        start_dt = dt.date.fromisoformat(start_date)
        end_dt = dt.date.fromisoformat(end_date)
    except ValueError as exc:
        raise SystemExit("Invalid date. Use YYYY-MM-DD.") from exc

    if end_dt < start_dt:
        raise SystemExit("End date must be on or after start date.")

    from spy2.common.calendar import trading_sessions

    manifests: list[Path] = []
    for session in trading_sessions(start_dt, end_dt):
        manifests.append(
            ingest_day(
                session.isoformat(),
                api_key=api_key,
                quotes_schema=quotes_schema,
                root=root,
                auto_clamp=auto_clamp,
            )
        )
    return manifests
