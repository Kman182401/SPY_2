from __future__ import annotations

import datetime as dt
from functools import lru_cache

import pandas as pd


@lru_cache(maxsize=8)
def _calendar(name: str):
    # Lazy import so `spy2 --help` doesn't require optional deps at import time.
    import exchange_calendars as xcals  # type: ignore[import-not-found]

    return xcals.get_calendar(name)


def trading_sessions(
    start: dt.date,
    end: dt.date,
    *,
    calendar: str = "XNYS",
) -> list[dt.date]:
    cal = _calendar(calendar)
    sessions = cal.sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))
    return [ts.to_pydatetime().date() for ts in sessions]


def session_open_close_utc(
    session: dt.date,
    *,
    calendar: str = "XNYS",
) -> tuple[dt.datetime, dt.datetime]:
    cal = _calendar(calendar)
    label = pd.Timestamp(session)
    open_ts = cal.session_open(label)
    close_ts = cal.session_close(label)
    return (open_ts.to_pydatetime(), close_ts.to_pydatetime())
