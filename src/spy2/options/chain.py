from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Iterator

import pandas as pd

from spy2.common.paths import resolve_root
from spy2.options.models import OptionChainSnapshot
from spy2.options.symbols import parse_opra_symbol


def _load_dataset(
    path: Path,
    *,
    columns: list[str] | None = None,
    filter_expr=None,
) -> pd.DataFrame:
    import pyarrow.dataset as ds

    if not path.exists():
        raise SystemExit(f"Missing data directory: {path}")
    dataset = ds.dataset(str(path), format="parquet")
    table = dataset.to_table(columns=columns, filter=filter_expr)
    df = table.to_pandas()
    index_names = []
    if hasattr(df.index, "names"):
        index_names = [name for name in df.index.names if name]
    elif df.index.name:
        index_names = [df.index.name]
    if "ts_event" not in df.columns and "ts_event" in index_names:
        df = df.reset_index()
    return df


def load_underlying_bars(
    trade_date: dt.date,
    *,
    root: Path | None = None,
    symbol: str = "SPY",
) -> pd.DataFrame:
    root = resolve_root(root)
    path = (
        root
        / "data"
        / "raw"
        / "EQUS.MINI"
        / "ohlcv-1m"
        / f"date={trade_date.isoformat()}"
    )
    df = _load_dataset(path, columns=["ts_event", "close", "symbol"])
    if symbol:
        df = df[df["symbol"] == symbol]
    return df.sort_values("ts_event")


def load_option_definitions(
    trade_date: dt.date,
    *,
    root: Path | None = None,
) -> pd.DataFrame:
    root = resolve_root(root)
    path = (
        root
        / "data"
        / "raw"
        / "OPRA.PILLAR"
        / "definition"
        / f"date={trade_date.isoformat()}"
    )
    df = _load_dataset(
        path,
        columns=["symbol", "underlying", "strike_price", "expiration"],
    )
    df["expiration"] = pd.to_datetime(df["expiration"], utc=True).dt.date
    df["right"] = df["symbol"].map(lambda value: parse_opra_symbol(value).right)
    df["strike"] = df["strike_price"].astype(float)
    return df[["symbol", "underlying", "expiration", "strike", "right"]]


def load_option_statistics(
    trade_date: dt.date,
    *,
    root: Path | None = None,
) -> pd.DataFrame:
    """
    Load OPRA statistics and extract per-symbol daily-ish fields.

    Databento's OPRA `statistics` schema is a stream of stat updates keyed by
    `stat_type`. For selection realism we primarily care about:
    - cleared volume
    - open interest
    """
    root = resolve_root(root)
    path = (
        root
        / "data"
        / "raw"
        / "OPRA.PILLAR"
        / "statistics"
        / f"date={trade_date.isoformat()}"
    )
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "open_interest", "volume"])

    import pyarrow.dataset as ds

    # Databento DBN StatType codes (stable): 6=CLEARED_VOLUME, 9=OPEN_INTEREST.
    vol_type = 6
    oi_type = 9

    filter_expr = ds.field("stat_type").isin([vol_type, oi_type])
    df = _load_dataset(
        path,
        columns=["ts_event", "symbol", "stat_type", "quantity"],
        filter_expr=filter_expr,
    )
    if df.empty:
        return pd.DataFrame(columns=["symbol", "open_interest", "volume"])

    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True, errors="coerce")
    df["stat_type"] = pd.to_numeric(df["stat_type"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df = df.dropna(subset=["symbol", "stat_type", "quantity", "ts_event"])

    if df.empty:
        return pd.DataFrame(columns=["symbol", "open_interest", "volume"])

    df = df.sort_values("ts_event")
    last = df.groupby(["symbol", "stat_type"], sort=False).tail(1)
    wide = last.pivot(
        index="symbol", columns="stat_type", values="quantity"
    ).reset_index()
    wide = wide.rename(columns={oi_type: "open_interest", vol_type: "volume"})

    for col in ("open_interest", "volume"):
        if col in wide.columns:
            wide[col] = pd.to_numeric(wide[col], errors="coerce")

    cols = ["symbol"]
    if "open_interest" in wide.columns:
        cols.append("open_interest")
    else:
        wide["open_interest"] = pd.NA
        cols.append("open_interest")

    if "volume" in wide.columns:
        cols.append("volume")
    else:
        wide["volume"] = pd.NA
        cols.append("volume")

    return wide[cols]


def load_option_quotes(
    trade_date: dt.date,
    *,
    root: Path | None = None,
    schema: str = "cbbo-1m",
) -> pd.DataFrame:
    root = resolve_root(root)
    path = (
        root
        / "data"
        / "raw"
        / "OPRA.PILLAR"
        / schema
        / f"date={trade_date.isoformat()}"
    )
    df = _load_dataset(
        path,
        columns=[
            "ts_event",
            "symbol",
            "bid_px_00",
            "ask_px_00",
            "bid_sz_00",
            "ask_sz_00",
        ],
    )
    df = df.rename(
        columns={
            "bid_px_00": "bid",
            "ask_px_00": "ask",
            "bid_sz_00": "bid_size",
            "ask_sz_00": "ask_size",
        }
    )
    df["mid"] = (df["bid"] + df["ask"]) / 2.0
    return df.sort_values("ts_event")


def load_option_quotes_for_symbols(
    trade_date: dt.date,
    symbols: list[str],
    *,
    root: Path | None = None,
    schema: str = "cbbo-1m",
) -> pd.DataFrame:
    root = resolve_root(root)
    if not symbols:
        return pd.DataFrame(
            columns=[
                "ts_event",
                "symbol",
                "bid",
                "ask",
                "bid_size",
                "ask_size",
                "mid",
            ]
        )

    path = (
        root
        / "data"
        / "raw"
        / "OPRA.PILLAR"
        / schema
        / f"date={trade_date.isoformat()}"
    )
    import pyarrow.dataset as ds

    filter_expr = ds.field("symbol").isin([str(sym) for sym in symbols])
    df = _load_dataset(
        path,
        columns=[
            "ts_event",
            "symbol",
            "bid_px_00",
            "ask_px_00",
            "bid_sz_00",
            "ask_sz_00",
        ],
        filter_expr=filter_expr,
    )
    df = df.rename(
        columns={
            "bid_px_00": "bid",
            "ask_px_00": "ask",
            "bid_sz_00": "bid_size",
            "ask_sz_00": "ask_size",
        }
    )
    df["mid"] = (df["bid"] + df["ask"]) / 2.0
    return df.sort_values("ts_event")


def iter_chain_snapshots(
    trade_date: dt.date,
    *,
    root: Path | None = None,
    quotes_schema: str = "cbbo-1m",
    underlying_symbol: str = "SPY",
    asof_tolerance_seconds: int = 60,
) -> Iterator[OptionChainSnapshot]:
    definitions = load_option_definitions(trade_date, root=root)
    quotes = load_option_quotes(trade_date, root=root, schema=quotes_schema)
    stats = load_option_statistics(trade_date, root=root)
    underlying = load_underlying_bars(
        trade_date,
        root=root,
        symbol=underlying_symbol,
    )
    underlying_sorted = underlying.dropna(subset=["ts_event"]).sort_values("ts_event")
    quotes_sorted = quotes.dropna(subset=["ts_event"]).sort_values("ts_event")
    asof_tolerance = pd.Timedelta(seconds=asof_tolerance_seconds)
    underlying_prices = underlying_sorted[["ts_event", "close"]].rename(
        columns={"close": "underlying_price"}
    )
    quotes_with_underlying = pd.merge_asof(
        quotes_sorted,
        underlying_prices,
        on="ts_event",
        direction="backward",
        tolerance=asof_tolerance,
    )

    chain = quotes_with_underlying.merge(definitions, on="symbol", how="left")
    if not stats.empty:
        chain = chain.merge(stats, on="symbol", how="left")

    for ts_event, group in chain.groupby("ts_event", sort=True):
        underlying_price = None
        if "underlying_price" in group.columns and not group["underlying_price"].empty:
            underlying_price = group["underlying_price"].iloc[0]
        snapshot = OptionChainSnapshot(
            ts_event=ts_event.to_pydatetime(warn=False),
            underlying_price=underlying_price,
            # groupby already materializes a per-timestamp frame; avoid deep copying
            # each snapshot to reduce CPU/memory pressure on long backtests.
            chain=group,
        )
        yield snapshot
