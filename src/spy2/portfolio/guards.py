from __future__ import annotations

import dataclasses
import datetime as dt

from spy2.options.models import OptionChainSnapshot, VerticalSpread


@dataclasses.dataclass(frozen=True)
class DividendGuardConfig:
    enabled: bool = True
    extrinsic_buffer: float = 0.0
    close_on_missing_data: bool = True


@dataclasses.dataclass(frozen=True)
class DividendGuardEval:
    should_close: bool
    reason: str | None
    dividend_amount: float | None = None
    option_mid: float | None = None
    intrinsic: float | None = None
    extrinsic: float | None = None


def evaluate_ex_dividend_guard(
    *,
    spread: VerticalSpread,
    snapshot: OptionChainSnapshot,
    dividend_amount: float,
    config: DividendGuardConfig,
) -> DividendGuardEval:
    if not config.enabled:
        return DividendGuardEval(should_close=False, reason=None)

    # Only applies to short calls.
    if spread.right != "C":
        return DividendGuardEval(should_close=False, reason=None)

    short_call = spread.short_leg
    underlying = snapshot.underlying_price
    if underlying is None:
        if config.close_on_missing_data:
            return DividendGuardEval(
                should_close=True,
                reason="DIV_GUARD_MISSING_UNDERLYING",
                dividend_amount=dividend_amount,
            )
        return DividendGuardEval(should_close=False, reason=None)

    if underlying <= short_call.strike:
        return DividendGuardEval(should_close=False, reason=None)

    chain = snapshot.chain
    row = chain[chain["symbol"] == short_call.symbol]
    if row.empty:
        if config.close_on_missing_data:
            return DividendGuardEval(
                should_close=True,
                reason="DIV_GUARD_MISSING_QUOTE_ROW",
                dividend_amount=dividend_amount,
            )
        return DividendGuardEval(should_close=False, reason=None)

    bid = row["bid"].iloc[0]
    ask = row["ask"].iloc[0]
    if bid is None or ask is None:
        if config.close_on_missing_data:
            return DividendGuardEval(
                should_close=True,
                reason="DIV_GUARD_MISSING_BID_ASK",
                dividend_amount=dividend_amount,
            )
        return DividendGuardEval(should_close=False, reason=None)

    option_mid = (float(bid) + float(ask)) / 2.0
    intrinsic = max(0.0, float(underlying) - float(short_call.strike))
    extrinsic = option_mid - intrinsic

    threshold = float(dividend_amount) + float(config.extrinsic_buffer)
    if extrinsic < threshold:
        return DividendGuardEval(
            should_close=True,
            reason="DIV_GUARD_EXTRINSIC_LT_DIV",
            dividend_amount=dividend_amount,
            option_mid=option_mid,
            intrinsic=intrinsic,
            extrinsic=extrinsic,
        )

    return DividendGuardEval(
        should_close=False,
        reason=None,
        dividend_amount=dividend_amount,
        option_mid=option_mid,
        intrinsic=intrinsic,
        extrinsic=extrinsic,
    )


@dataclasses.dataclass(frozen=True)
class PdtGuardConfig:
    enabled: bool = True
    # Common broker enforcement threshold for PDT in the US.
    min_equity: float = 25_000.0
    window_sessions: int = 5
    max_day_trades: int = 3
    # Some brokers proactively limit *openings* to avoid PDT flags.
    max_open_transactions: int = 3


@dataclasses.dataclass(frozen=True)
class PdtGuardEval:
    allowed: bool
    reason: str | None
    rolling_open_transactions: int
    rolling_day_trades: int


def evaluate_pdt_open_guard(
    *,
    session_date: dt.date,
    sessions: list[dt.date],
    open_transactions_by_date: dict[dt.date, int],
    day_trades_by_date: dict[dt.date, int],
    account_equity: float,
    config: PdtGuardConfig,
) -> PdtGuardEval:
    if not config.enabled or account_equity >= config.min_equity:
        return PdtGuardEval(
            allowed=True,
            reason=None,
            rolling_open_transactions=0,
            rolling_day_trades=0,
        )

    # Rolling N-session window including the current session.
    try:
        idx = sessions.index(session_date)
    except ValueError:
        idx = len(sessions) - 1
    start_idx = max(0, idx - (config.window_sessions - 1))
    window = sessions[start_idx : idx + 1]

    rolling_opens = sum(open_transactions_by_date.get(day, 0) for day in window)
    rolling_day_trades = sum(day_trades_by_date.get(day, 0) for day in window)

    if rolling_day_trades >= config.max_day_trades:
        return PdtGuardEval(
            allowed=False,
            reason="PDT_GUARD_MAX_DAY_TRADES",
            rolling_open_transactions=rolling_opens,
            rolling_day_trades=rolling_day_trades,
        )

    if rolling_opens >= config.max_open_transactions:
        return PdtGuardEval(
            allowed=False,
            reason="PDT_GUARD_MAX_OPEN_TRANSACTIONS",
            rolling_open_transactions=rolling_opens,
            rolling_day_trades=rolling_day_trades,
        )

    return PdtGuardEval(
        allowed=True,
        reason=None,
        rolling_open_transactions=rolling_opens,
        rolling_day_trades=rolling_day_trades,
    )
