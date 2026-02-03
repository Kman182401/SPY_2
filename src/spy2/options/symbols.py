from __future__ import annotations

import dataclasses
import datetime as dt


@dataclasses.dataclass(frozen=True)
class ParsedOptionSymbol:
    root: str
    expiration: dt.date
    right: str
    strike: float
    raw: str


def parse_opra_symbol(symbol: str) -> ParsedOptionSymbol:
    if not symbol:
        raise ValueError("OPRA symbol is empty.")

    compact = symbol.replace(" ", "")
    if len(compact) < 15:
        raise ValueError(f"OPRA symbol too short: {symbol!r}")

    strike_raw = compact[-8:]
    right = compact[-9]
    date_raw = compact[-15:-9]
    root = compact[:-15]

    if len(date_raw) != 6 or not date_raw.isdigit():
        raise ValueError(f"Invalid expiration in OPRA symbol: {symbol!r}")
    if right not in {"C", "P"}:
        raise ValueError(f"Invalid right in OPRA symbol: {symbol!r}")
    if not strike_raw.isdigit():
        raise ValueError(f"Invalid strike in OPRA symbol: {symbol!r}")

    year = 2000 + int(date_raw[:2])
    month = int(date_raw[2:4])
    day = int(date_raw[4:6])
    expiration = dt.date(year, month, day)
    strike = int(strike_raw) / 1000.0

    return ParsedOptionSymbol(
        root=root,
        expiration=expiration,
        right=right,
        strike=strike,
        raw=symbol,
    )
