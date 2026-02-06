from __future__ import annotations

import dataclasses
import datetime as dt
from pathlib import Path

import pandas as pd

from spy2.common.paths import resolve_root


def dividends_path(*, symbol: str, root: Path | None = None) -> Path:
    root = resolve_root(root)
    return (
        root
        / "data"
        / "ref"
        / "dividends"
        / f"symbol={symbol.upper()}"
        / "dividends.parquet"
    )


@dataclasses.dataclass(frozen=True)
class DividendCalendar:
    """A tiny helper for ex-dividend lookups."""

    by_ex_date: dict[dt.date, float]

    def amount_on(self, ex_date: dt.date) -> float | None:
        return self.by_ex_date.get(ex_date)


def load_dividend_calendar(
    *,
    symbol: str,
    root: Path | None = None,
) -> DividendCalendar | None:
    path = dividends_path(symbol=symbol, root=root)
    if not path.exists():
        return None

    df = pd.read_parquet(path)
    if df.empty:
        return DividendCalendar(by_ex_date={})

    if "ex_date" not in df.columns:
        raise SystemExit(f"Dividend file missing ex_date column: {path}")
    if "gross_dividend" not in df.columns:
        raise SystemExit(f"Dividend file missing gross_dividend column: {path}")

    by_ex_date: dict[dt.date, float] = {}
    for row in df.itertuples(index=False):
        ex = row.ex_date
        if isinstance(ex, pd.Timestamp):
            ex_date = ex.to_pydatetime().date()
        elif isinstance(ex, dt.datetime):
            ex_date = ex.date()
        elif isinstance(ex, dt.date):
            ex_date = ex
        else:
            ex_date = dt.date.fromisoformat(str(ex))

        amount = float(row.gross_dividend) if row.gross_dividend is not None else 0.0
        by_ex_date[ex_date] = by_ex_date.get(ex_date, 0.0) + amount

    return DividendCalendar(by_ex_date=by_ex_date)
