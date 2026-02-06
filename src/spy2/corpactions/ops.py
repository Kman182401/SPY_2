from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd

from spy2.common.paths import repo_root, resolve_root


def _resolve_api_key(api_key: str | None) -> str:
    if api_key:
        return api_key.strip()
    env_key = os.getenv("DATABENTO_API_KEY")
    if env_key:
        return env_key.strip()
    raise SystemExit(
        "Missing Databento API key. Set DATABENTO_API_KEY or pass --api-key."
    )


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


def ingest_dividends(
    *,
    symbol: str,
    start_date: str,
    end_date: str,
    api_key: str | None = None,
    stype_in: str = "raw_symbol",
    pit: bool = False,
    root: Path | None = None,
) -> tuple[Path, Path]:
    """
    Fetch and persist a dividend calendar using Databento corporate actions.

    Writes a canonical parquet file under `data/ref/dividends/` and a manifest
    under `artifacts/manifests/`.
    """
    import databento as db  # type: ignore[import-untyped]
    from databento.common.error import BentoClientError  # type: ignore[import-untyped]

    try:
        start = dt.date.fromisoformat(start_date)
        end_inclusive = dt.date.fromisoformat(end_date)
    except ValueError as exc:
        raise SystemExit("Invalid date. Use YYYY-MM-DD.") from exc

    if end_inclusive < start:
        raise SystemExit("End date must be on or after start date.")

    # Databento uses an exclusive `end`.
    end_exclusive = end_inclusive + dt.timedelta(days=1)

    root = resolve_root(root)
    repo = repo_root()
    symbol = symbol.upper()

    ref = db.Reference(_resolve_api_key(api_key))
    try:
        df = ref.corporate_actions.get_range(
            start=start,
            end=end_exclusive,
            index="ex_date",
            symbols=symbol,
            stype_in=stype_in,
            events="DIV",
            # Use nested payloads so we can extract fields without depending on
            # Databento's flattening column naming.
            flatten=False,
            pit=pit,
        )
    except BentoClientError as exc:
        message = str(exc)
        if "license_reference_dataset_no_subscription" in message:
            raise SystemExit(
                "Databento corporate actions dataset requires a reference-data "
                "subscription. Either add the subscription in the Databento portal, "
                "or import dividends from a local CSV into "
                "`data/ref/dividends/` (see `spy2 corpactions dividends --help`)."
            ) from exc
        raise SystemExit(f"Databento corporate actions request failed: {exc}") from exc

    df = df.reset_index()
    for col in ("ex_date", "record_date", "payment_date", "event_date", "ts_record"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    def _get_nested(row: pd.Series, key: str) -> dict[str, Any]:
        value = row.get(key)
        if isinstance(value, dict):
            return value
        return {}

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        date_info = _get_nested(row, "date_info")
        event_info = _get_nested(row, "event_info")
        gross = event_info.get("gross_dividend")
        currency = event_info.get("currency")

        rows.append(
            {
                "symbol": row.get("symbol"),
                "event_unique_id": row.get("event_unique_id"),
                "event_type": row.get("event_type") or row.get("event"),
                "event_subtype": row.get("event_subtype"),
                "event_date": row.get("event_date"),
                "ex_date": row.get("ex_date"),
                "record_date": date_info.get("record_date"),
                "payment_date": date_info.get("payment_date")
                or date_info.get("pay_date"),
                "gross_dividend": gross,
                "currency": currency,
                "ts_record": row.get("ts_record"),
                "date_info_json": json.dumps(date_info, sort_keys=True),
                "event_info_json": json.dumps(event_info, sort_keys=True),
            }
        )

    df_out = pd.DataFrame(rows)
    df_out["ex_date"] = pd.to_datetime(df_out["ex_date"], utc=True, errors="coerce")
    df_out["record_date"] = pd.to_datetime(
        df_out["record_date"], utc=True, errors="coerce"
    )
    df_out["payment_date"] = pd.to_datetime(
        df_out["payment_date"], utc=True, errors="coerce"
    )
    df_out["event_date"] = pd.to_datetime(
        df_out["event_date"], utc=True, errors="coerce"
    )
    df_out["ts_record"] = pd.to_datetime(df_out["ts_record"], utc=True, errors="coerce")

    if df_out["gross_dividend"].isna().all():
        raise SystemExit(
            "Corporate actions payload did not include `event_info.gross_dividend`. "
            "Unable to build a dividend calendar."
        )

    output_dir = root / "data" / "ref" / "dividends" / f"symbol={symbol}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "dividends.parquet"

    if output_path.exists():
        existing_df = pd.read_parquet(output_path)
        df_out = pd.concat([existing_df, df_out], ignore_index=True)
        if "event_unique_id" in df_out.columns:
            df_out = df_out.drop_duplicates(subset=["event_unique_id"], keep="last")
        else:
            df_out = df_out.drop_duplicates(keep="last")

    if "ex_date" in df_out.columns:
        df_out = df_out.sort_values("ex_date")

    df_out.to_parquet(output_path, index=False)

    manifests_dir = root / "artifacts" / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    manifest_path = manifests_dir / f"dividends_{symbol}_{ts}.json"
    manifest: dict[str, Any] = {
        "run_started_at": ts,
        "symbol": symbol,
        "start_date": start.isoformat(),
        "end_date": end_inclusive.isoformat(),
        "stype_in": stype_in,
        "pit": pit,
        "git_sha": _git_sha(repo),
        "output": str(output_path.relative_to(root)),
        "rows": int(len(df_out)),
        "sha256": _sha256_file(output_path),
        "bytes": output_path.stat().st_size,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

    return output_path, manifest_path


def import_dividends_csv(
    *,
    symbol: str,
    csv_path: Path,
    root: Path | None = None,
) -> tuple[Path, Path]:
    root = resolve_root(root)
    repo = repo_root()
    symbol = symbol.upper()

    df = pd.read_csv(csv_path)
    if "ex_date" not in df.columns or "gross_dividend" not in df.columns:
        raise SystemExit("CSV must include `ex_date` and `gross_dividend` columns.")

    df["ex_date"] = pd.to_datetime(df["ex_date"], utc=True, errors="coerce")
    df["gross_dividend"] = pd.to_numeric(df["gross_dividend"], errors="coerce")
    df = df.dropna(subset=["ex_date", "gross_dividend"])
    df = df.sort_values("ex_date")

    output_dir = root / "data" / "ref" / "dividends" / f"symbol={symbol}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "dividends.parquet"
    df.to_parquet(output_path, index=False)

    manifests_dir = root / "artifacts" / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    manifest_path = manifests_dir / f"dividends_import_{symbol}_{ts}.json"
    start = df["ex_date"].min().to_pydatetime().date()
    end = df["ex_date"].max().to_pydatetime().date()
    manifest: dict[str, Any] = {
        "run_started_at": ts,
        "symbol": symbol,
        "source_csv": str(csv_path),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "git_sha": _git_sha(repo),
        "output": str(output_path.relative_to(root)),
        "rows": int(len(df)),
        "sha256": _sha256_file(output_path),
        "bytes": output_path.stat().st_size,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

    return output_path, manifest_path
