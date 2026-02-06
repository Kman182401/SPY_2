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

    for ts_event, group in chain.groupby("ts_event", sort=True):
        underlying_price = None
        if "underlying_price" in group.columns and not group["underlying_price"].empty:
            underlying_price = group["underlying_price"].iloc[0]
        snapshot = OptionChainSnapshot(
            ts_event=ts_event.to_pydatetime(warn=False),
            underlying_price=underlying_price,
            chain=group.copy(),
        )
        yield snapshot
