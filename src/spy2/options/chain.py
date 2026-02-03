from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Iterator

import pandas as pd

from spy2.options.models import OptionChainSnapshot
from spy2.options.symbols import parse_opra_symbol


def _repo_root(start: Path | None = None) -> Path:
    if start is None:
        start = Path.cwd()
    for path in [start, *start.parents]:
        if (path / "pyproject.toml").is_file():
            return path
    return start


def _load_dataset(path: Path, *, columns: list[str] | None = None) -> pd.DataFrame:
    import pyarrow.dataset as ds

    if not path.exists():
        raise SystemExit(f"Missing data directory: {path}")
    dataset = ds.dataset(str(path), format="parquet")
    table = dataset.to_table(columns=columns)
    return table.to_pandas()


def load_underlying_bars(
    trade_date: dt.date,
    *,
    root: Path | None = None,
    symbol: str = "SPY",
) -> pd.DataFrame:
    root = _repo_root(root)
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
    root = _repo_root(root)
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
    root = _repo_root(root)
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


def iter_chain_snapshots(
    trade_date: dt.date,
    *,
    root: Path | None = None,
    quotes_schema: str = "cbbo-1m",
    underlying_symbol: str = "SPY",
) -> Iterator[OptionChainSnapshot]:
    definitions = load_option_definitions(trade_date, root=root)
    quotes = load_option_quotes(trade_date, root=root, schema=quotes_schema)
    underlying = load_underlying_bars(
        trade_date,
        root=root,
        symbol=underlying_symbol,
    )
    underlying_map = dict(zip(underlying["ts_event"], underlying["close"]))

    chain = quotes.merge(definitions, on="symbol", how="left")

    for ts_event, group in chain.groupby("ts_event", sort=True):
        underlying_price = underlying_map.get(ts_event)
        snapshot = OptionChainSnapshot(
            ts_event=ts_event.to_pydatetime(),
            underlying_price=underlying_price,
            chain=group.copy(),
        )
        yield snapshot
