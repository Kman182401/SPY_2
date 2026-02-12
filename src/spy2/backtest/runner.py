from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import os
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from spy2.common.calendar import session_open_close_utc, trading_sessions
from spy2.common.paths import resolve_root
from spy2.corpactions.dividends import load_dividend_calendar
from spy2.fees.ibkr import IbkrFeeSchedule, estimate_spread_fees
from spy2.fees.tick import tick_size_for_symbol
from spy2.options.chain import iter_chain_snapshots, load_option_quotes_for_symbols
from spy2.options.fill import fill_vertical_spread, fill_vertical_spread_inside
from spy2.options.liquidity import LiquidityFilterConfig
from spy2.options.models import OptionChainSnapshot, VerticalSpread
from spy2.options.selection import priced_spread_from_fill, select_vertical_spread
from spy2.portfolio.guards import (
    DividendGuardConfig,
    PdtGuardConfig,
    evaluate_ex_dividend_guard,
    evaluate_pdt_open_guard,
)
from spy2.portfolio.models import (
    PortfolioState,
    SpreadPosition,
    build_close_spread,
    cashflow_from_fill,
    safe_float,
)


@dataclasses.dataclass(frozen=True)
class BacktestOutputs:
    run_id: str
    output_dir: Path
    trades_path: Path
    equity_curve_path: Path
    summary_path: Path


def run_backtest_range(
    *,
    start: dt.date,
    end: dt.date,
    root: Path | None,
    strategy: str,
    right: Literal["C", "P"] = "P",
    width: float = 1.0,
    quotes_schema: str = "cbbo-1m",
    slippage_bps: float = 0.0,
    initial_cash: float = 1000.0,
    calendar: str = "XNYS",
    force_close_dte: int = 1,
    fill_model: Literal[
        "conservative",
        "mid",
        "mid_with_slippage",
        "spread_inside",
        "spread_inside_with_slippage",
    ] = "conservative",
    fill_alpha: float = 0.5,
    fill_sensitivity: bool = False,
    pdt_guard: PdtGuardConfig | None = None,
    dividend_guard: DividendGuardConfig | None = None,
    dividend_symbol: str = "SPY",
) -> BacktestOutputs:
    if end < start:
        raise ValueError("end must be on or after start")

    root = resolve_root(root)
    run_id = _make_run_id()
    out_dir = root / "artifacts" / "backtests" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    base_outputs, base_summary = _run_backtest_model(
        start=start,
        end=end,
        root=root,
        out_dir=out_dir,
        strategy=strategy,
        right=right,
        width=width,
        quotes_schema=quotes_schema,
        slippage_bps=slippage_bps,
        initial_cash=initial_cash,
        calendar=calendar,
        force_close_dte=force_close_dte,
        fill_model=fill_model,
        fill_alpha=fill_alpha,
        pdt_guard=pdt_guard,
        dividend_guard=dividend_guard,
        dividend_symbol=dividend_symbol,
    )

    if fill_sensitivity:
        sensitivity_dir = out_dir / "fill_sensitivity"
        sensitivity_dir.mkdir(parents=True, exist_ok=True)
        models: list[
            Literal[
                "conservative",
                "mid",
                "mid_with_slippage",
                "spread_inside",
                "spread_inside_with_slippage",
            ]
        ] = [
            "conservative",
            "mid",
            "mid_with_slippage",
            "spread_inside",
            "spread_inside_with_slippage",
        ]
        # Keep a copy of the base summary so we can safely embed a
        # fill-sensitivity section into `base_summary` without creating JSON
        # cycles.
        summaries: dict[str, dict[str, Any]] = {fill_model: dict(base_summary)}
        for model in models:
            if model == fill_model:
                continue
            model_dir = sensitivity_dir / f"fill_model={model}"
            model_dir.mkdir(parents=True, exist_ok=True)
            _, summary = _run_backtest_model(
                start=start,
                end=end,
                root=root,
                out_dir=model_dir,
                strategy=strategy,
                right=right,
                width=width,
                quotes_schema=quotes_schema,
                slippage_bps=slippage_bps,
                initial_cash=initial_cash,
                calendar=calendar,
                force_close_dte=force_close_dte,
                fill_model=model,
                fill_alpha=fill_alpha,
                pdt_guard=pdt_guard,
                dividend_guard=dividend_guard,
                dividend_symbol=dividend_symbol,
            )
            summaries[model] = summary

        base = summaries.get("conservative", base_summary)
        deltas: dict[str, Any] = {}
        for model_name, summary in summaries.items():
            if model_name == "conservative":
                continue
            deltas[model_name] = {
                "final_equity_conservative": _delta(
                    base.get("final_equity_conservative"),
                    summary.get("final_equity_conservative"),
                ),
                "realized_pnl": _delta(
                    base.get("realized_pnl"), summary.get("realized_pnl")
                ),
                "fees_total": _delta(base.get("fees_total"), summary.get("fees_total")),
                "trade_count": _delta(
                    base.get("trade_count"), summary.get("trade_count")
                ),
            }

        base_summary["fill_model_sensitivity"] = {
            "base_model": "conservative",
            "models": summaries,
            "deltas_vs_conservative": deltas,
        }

        (out_dir / "fill_sensitivity.json").write_text(
            json.dumps(base_summary["fill_model_sensitivity"], indent=2, sort_keys=True)
            + "\n"
        )

        base_outputs.summary_path.write_text(
            json.dumps(base_summary, indent=2, sort_keys=True) + "\n"
        )

    return base_outputs


def _run_backtest_model(
    *,
    start: dt.date,
    end: dt.date,
    root: Path,
    out_dir: Path,
    strategy: str,
    right: Literal["C", "P"],
    width: float,
    quotes_schema: str,
    slippage_bps: float,
    initial_cash: float,
    calendar: str,
    force_close_dte: int,
    fill_model: Literal[
        "conservative",
        "mid",
        "mid_with_slippage",
        "spread_inside",
        "spread_inside_with_slippage",
    ],
    fill_alpha: float,
    pdt_guard: PdtGuardConfig | None,
    dividend_guard: DividendGuardConfig | None,
    dividend_symbol: str,
) -> tuple[BacktestOutputs, dict[str, Any]]:
    portfolio = PortfolioState(cash=float(initial_cash))
    schedule = IbkrFeeSchedule.from_env()
    liquidity = LiquidityFilterConfig.from_env()
    progress_enabled = os.getenv("SPY2_PROGRESS", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    trades: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []

    pdt_guard = pdt_guard or PdtGuardConfig()
    dividend_guard = dividend_guard or DividendGuardConfig()
    pdt_open_tx: dict[dt.date, int] = {}
    pdt_day_trades: dict[dt.date, int] = {}
    pdt_blocked_opens = 0

    sessions = trading_sessions(start, end, calendar=calendar)
    if not sessions:
        raise SystemExit("No trading sessions in the requested range.")
    if progress_enabled:
        print(
            f"Backtest model={fill_model} sessions={len(sessions)} start={start} end={end}",
            flush=True,
        )

    dividend_calendar = load_dividend_calendar(symbol=dividend_symbol, root=root)

    for idx, session_date in enumerate(sessions):
        if progress_enabled:
            print(
                f"Backtest session {idx + 1}/{len(sessions)} date={session_date} model={fill_model}",
                flush=True,
            )
        is_last_session = idx == (len(sessions) - 1)
        next_session_date = sessions[idx + 1] if idx + 1 < len(sessions) else None
        open_dt, close_dt = session_open_close_utc(session_date, calendar=calendar)
        close_target = close_dt - dt.timedelta(minutes=1)

        entry_pred: Callable[[OptionChainSnapshot], bool] | None = None
        if strategy == "demo_vertical":

            def _can_enter(snap: OptionChainSnapshot) -> bool:
                return (
                    select_vertical_spread(
                        snap,
                        right=right,
                        width=width,
                        allow_fallback_right=True,
                        liquidity=liquidity,
                    )
                    is not None
                )

            entry_pred = _can_enter

        entry_snapshot, close_snapshot = _load_entry_and_close_snapshots(
            session_date,
            open_dt,
            close_target,
            root=root,
            quotes_schema=quotes_schema,
            entry_pred=entry_pred,
        )
        if entry_snapshot is None or close_snapshot is None:
            continue

        if (
            dividend_calendar is not None
            and next_session_date is not None
            and dividend_guard.enabled
        ):
            dividend_amount = dividend_calendar.amount_on(next_session_date)
            if dividend_amount is not None and dividend_amount > 0:
                _close_positions_for_dividend_guard(
                    portfolio,
                    close_snapshot,
                    root=root,
                    quotes_schema=quotes_schema,
                    schedule=schedule,
                    slippage_bps=slippage_bps,
                    dividend_amount=dividend_amount,
                    config=dividend_guard,
                    trades=trades,
                    day_trades_by_date=pdt_day_trades,
                    fill_model=fill_model,
                    fill_alpha=fill_alpha,
                )

        _close_positions_if_needed(
            portfolio,
            close_snapshot,
            root=root,
            quotes_schema=quotes_schema,
            schedule=schedule,
            slippage_bps=slippage_bps,
            force_close_dte=force_close_dte,
            trades=trades,
            close_reason="FORCE_CLOSE_DTE",
            day_trades_by_date=pdt_day_trades,
            fill_model=fill_model,
            fill_alpha=fill_alpha,
        )

        if not portfolio.open_positions():
            if strategy == "demo_vertical":
                pdt_eval = evaluate_pdt_open_guard(
                    session_date=session_date,
                    sessions=sessions,
                    open_transactions_by_date=pdt_open_tx,
                    day_trades_by_date=pdt_day_trades,
                    account_equity=portfolio.cash,
                    config=pdt_guard,
                )
                if not pdt_eval.allowed:
                    pdt_blocked_opens += 1
                    trades.append(
                        {
                            "stage": "SKIP",
                            "ts_event": close_snapshot.ts_event,
                            "reason": pdt_eval.reason,
                            "rolling_open_transactions": pdt_eval.rolling_open_transactions,
                            "rolling_day_trades": pdt_eval.rolling_day_trades,
                        }
                    )
                    equity_rows.append(
                        _equity_row(
                            portfolio,
                            close_snapshot,
                            schedule=schedule,
                            slippage_bps=slippage_bps,
                            calendar=calendar,
                        )
                    )
                    continue

                selection = select_vertical_spread(
                    entry_snapshot,
                    right=right,
                    width=width,
                    allow_fallback_right=True,
                    liquidity=liquidity,
                )
                if selection is None:
                    trades.append(
                        {
                            "stage": "SKIP",
                            "ts_event": entry_snapshot.ts_event,
                            "reason": "NO_TRADE_NO_ELIGIBLE_CANDIDATE",
                        }
                    )
                else:
                    spread, _used_right = selection
                    pos = _open_position(
                        portfolio,
                        entry_snapshot,
                        spread,
                        schedule=schedule,
                        slippage_bps=slippage_bps,
                        fill_model=fill_model,
                        fill_alpha=fill_alpha,
                    )
                    if pos is not None:
                        pdt_open_tx[session_date] = pdt_open_tx.get(session_date, 0) + 1
                        trades.append(_position_to_trade_row(pos, stage="OPEN"))
            else:
                raise SystemExit(f"Unknown strategy: {strategy}")

        if is_last_session:
            _close_positions_if_needed(
                portfolio,
                close_snapshot,
                root=root,
                quotes_schema=quotes_schema,
                schedule=schedule,
                slippage_bps=slippage_bps,
                force_close_dte=10_000,
                trades=trades,
                close_reason="END_OF_RUN",
                day_trades_by_date=pdt_day_trades,
                fill_model=fill_model,
                fill_alpha=fill_alpha,
            )

        equity_rows.append(
            _equity_row(
                portfolio,
                close_snapshot,
                schedule=schedule,
                slippage_bps=slippage_bps,
                calendar=calendar,
            )
        )

    trades_path = out_dir / "trades.parquet"
    equity_path = out_dir / "equity_curve.parquet"
    summary_path = out_dir / "summary.json"

    if trades:
        pd.DataFrame(trades).to_parquet(trades_path, index=False)
    else:
        pd.DataFrame([], columns=["stage"]).to_parquet(trades_path, index=False)

    equity_df = pd.DataFrame(equity_rows)
    equity_df.to_parquet(equity_path, index=False)

    summary = _build_summary(
        equity_df,
        trades=pd.DataFrame(trades) if trades else pd.DataFrame([]),
        run_id=out_dir.name,
        start=start,
        end=end,
        strategy=strategy,
        initial_cash=float(initial_cash),
        portfolio=portfolio,
        pdt_blocked_opens=pdt_blocked_opens,
    )
    summary["fill_model"] = fill_model
    run_config = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "strategy": strategy,
        "right": right,
        "width": width,
        "quotes_schema": quotes_schema,
        "slippage_bps": float(slippage_bps),
        "initial_cash": float(initial_cash),
        "calendar": calendar,
        "force_close_dte": int(force_close_dte),
        "fill_model": fill_model,
        "fill_alpha": float(fill_alpha),
        "liquidity": dataclasses.asdict(liquidity),
        "pdt_guard": dataclasses.asdict(pdt_guard),
        "dividend_guard": dataclasses.asdict(dividend_guard),
        "dividend_symbol": dividend_symbol,
    }
    summary["config"] = run_config
    summary["config_sha256"] = _sha256_json(run_config)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")

    outputs = BacktestOutputs(
        run_id=out_dir.name,
        output_dir=out_dir,
        trades_path=trades_path,
        equity_curve_path=equity_path,
        summary_path=summary_path,
    )
    return outputs, summary


def _delta(base: Any, other: Any) -> float | None:
    try:
        if base is None or other is None:
            return None
        return float(other) - float(base)
    except (TypeError, ValueError):
        return None


def _sha256_json(payload: Any) -> str:
    data = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _load_entry_and_close_snapshots(
    trade_date: dt.date,
    entry_target: dt.datetime,
    close_target: dt.datetime,
    *,
    root: Path,
    quotes_schema: str,
    entry_pred: Callable[[OptionChainSnapshot], bool] | None = None,
) -> tuple[OptionChainSnapshot | None, OptionChainSnapshot | None]:
    entry: OptionChainSnapshot | None = None
    close: OptionChainSnapshot | None = None
    last: OptionChainSnapshot | None = None

    snapshots = iter_chain_snapshots(
        trade_date,
        root=root,
        quotes_schema=quotes_schema,
    )
    for snap in snapshots:
        last = snap
        if entry is None and snap.ts_event >= entry_target:
            if entry_pred is None or entry_pred(snap):
                entry = snap
        if snap.ts_event <= close_target:
            close = snap
        if entry is not None and snap.ts_event > close_target:
            break

    if close is None:
        close = last
    if entry is None:
        entry = close
    return (entry, close)


def _open_position(
    portfolio: PortfolioState,
    snapshot: OptionChainSnapshot,
    spread: VerticalSpread,
    *,
    schedule: IbkrFeeSchedule,
    slippage_bps: float,
    fill_model: Literal[
        "conservative",
        "mid",
        "mid_with_slippage",
        "spread_inside",
        "spread_inside_with_slippage",
    ],
    fill_alpha: float,
) -> SpreadPosition | None:
    fill = _fill_spread(
        spread,
        snapshot,
        fill_model=fill_model,
        slippage_bps=slippage_bps,
        fill_alpha=fill_alpha,
    )
    if fill.net_debit is None:
        return None

    fees = estimate_spread_fees(fill, schedule=schedule)
    cashflow = cashflow_from_fill(fill, fees=fees)

    leg_prices = {leg.symbol: leg.price for leg in fill.leg_fills}
    priced_spread = priced_spread_from_fill(spread, leg_prices=leg_prices)
    max_profit = priced_spread.max_profit
    max_loss = priced_spread.max_loss

    contracts = spread.quantity
    max_profit_dollars = (
        None if max_profit is None else max_profit * spread.multiplier * contracts
    )
    max_loss_dollars = (
        None if max_loss is None else max_loss * spread.multiplier * contracts
    )
    risk_exposure = 0.0 if max_loss_dollars is None else float(max_loss_dollars)

    margin_required = 0.0
    if fill.net_debit < 0 and max_loss_dollars is not None:
        margin_required = float(max_loss_dollars)

    pos = SpreadPosition(
        position_id=f"pos_{uuid.uuid4().hex[:12]}",
        spread=spread,
        opened_at=snapshot.ts_event,
        entry_fill=fill,
        entry_fees=fees,
        entry_cashflow=cashflow,
        max_profit_dollars=max_profit_dollars,
        max_loss_dollars=max_loss_dollars,
        risk_exposure=risk_exposure,
        margin_required=margin_required,
    )

    # Basic capital check: cash after trade must cover reserved margin.
    projected_cash = portfolio.cash + cashflow
    projected_reserved = portfolio.reserved_margin + margin_required
    if projected_cash < projected_reserved:
        return None

    portfolio.open_position(pos)
    return pos


def _close_positions_if_needed(
    portfolio: PortfolioState,
    snapshot: OptionChainSnapshot,
    *,
    root: Path,
    quotes_schema: str,
    schedule: IbkrFeeSchedule,
    slippage_bps: float,
    force_close_dte: int,
    trades: list[dict[str, Any]],
    close_reason: str,
    day_trades_by_date: dict[dt.date, int] | None = None,
    fill_model: Literal[
        "conservative",
        "mid",
        "mid_with_slippage",
        "spread_inside",
        "spread_inside_with_slippage",
    ],
    fill_alpha: float,
) -> None:
    for pos in list(portfolio.open_positions()):
        dte = (pos.spread.expiration - snapshot.ts_event.date()).days
        if dte > force_close_dte:
            continue

        closed = _close_position(
            portfolio,
            pos,
            snapshot,
            root=root,
            quotes_schema=quotes_schema,
            schedule=schedule,
            slippage_bps=slippage_bps,
            reason=close_reason,
            trades=trades,
            day_trades_by_date=day_trades_by_date,
            fill_model=fill_model,
            fill_alpha=fill_alpha,
        )
        if closed is None:
            continue


def _close_positions_for_dividend_guard(
    portfolio: PortfolioState,
    snapshot: OptionChainSnapshot,
    *,
    root: Path,
    quotes_schema: str,
    schedule: IbkrFeeSchedule,
    slippage_bps: float,
    dividend_amount: float,
    config: DividendGuardConfig,
    trades: list[dict[str, Any]],
    day_trades_by_date: dict[dt.date, int] | None = None,
    fill_model: Literal[
        "conservative",
        "mid",
        "mid_with_slippage",
        "spread_inside",
        "spread_inside_with_slippage",
    ],
    fill_alpha: float,
) -> None:
    for pos in list(portfolio.open_positions()):
        eval = evaluate_ex_dividend_guard(
            spread=pos.spread,
            snapshot=snapshot,
            dividend_amount=dividend_amount,
            config=config,
        )
        if not eval.should_close:
            continue

        _close_position(
            portfolio,
            pos,
            snapshot,
            root=root,
            quotes_schema=quotes_schema,
            schedule=schedule,
            slippage_bps=slippage_bps,
            reason=eval.reason or "DIVIDEND_GUARD",
            trades=trades,
            day_trades_by_date=day_trades_by_date,
            fill_model=fill_model,
            fill_alpha=fill_alpha,
            extra={
                "dividend_amount": eval.dividend_amount,
                "dividend_option_mid": eval.option_mid,
                "dividend_intrinsic": eval.intrinsic,
                "dividend_extrinsic": eval.extrinsic,
            },
        )


def _close_position(
    portfolio: PortfolioState,
    pos: SpreadPosition,
    snapshot: OptionChainSnapshot,
    *,
    root: Path,
    quotes_schema: str,
    schedule: IbkrFeeSchedule,
    slippage_bps: float,
    reason: str,
    trades: list[dict[str, Any]],
    day_trades_by_date: dict[dt.date, int] | None = None,
    extra: dict[str, Any] | None = None,
    fill_model: Literal[
        "conservative",
        "mid",
        "mid_with_slippage",
        "spread_inside",
        "spread_inside_with_slippage",
    ],
    fill_alpha: float,
) -> SpreadPosition | None:
    close_spread = build_close_spread(pos.spread)
    close_fill = _fill_spread(
        close_spread,
        snapshot,
        fill_model=fill_model,
        slippage_bps=slippage_bps,
        fill_alpha=fill_alpha,
    )
    if close_fill.net_debit is None:
        quotes_by_symbol = _quotes_asof_for_symbols(
            trade_date=snapshot.ts_event.date(),
            ts_event=snapshot.ts_event,
            symbols=[close_spread.long_leg.symbol, close_spread.short_leg.symbol],
            root=root,
            quotes_schema=quotes_schema,
        )
        close_fill = _fill_spread_from_quotes(
            close_spread,
            quotes_by_symbol,
            fill_model=fill_model,
            slippage_bps=slippage_bps,
            fill_alpha=fill_alpha,
        )
        if close_fill.net_debit is None:
            return None
    close_fees = estimate_spread_fees(close_fill, schedule=schedule)
    close_cashflow = cashflow_from_fill(
        close_fill,
        fees=close_fees,
    )
    closed = portfolio.close_position(
        pos.position_id,
        closed_at=snapshot.ts_event,
        exit_fill=close_fill,
        exit_fees=close_fees,
        exit_cashflow=close_cashflow,
    )

    if (
        day_trades_by_date is not None
        and closed.closed_at is not None
        and closed.opened_at.date() == closed.closed_at.date()
    ):
        close_date = closed.closed_at.date()
        day_trades_by_date[close_date] = day_trades_by_date.get(close_date, 0) + 1

    row = _position_to_trade_row(closed, stage="CLOSE", reason=reason)
    if extra:
        row.update(extra)
    trades.append(row)
    return closed


def _equity_row(
    portfolio: PortfolioState,
    snapshot: OptionChainSnapshot,
    *,
    schedule: IbkrFeeSchedule,
    slippage_bps: float,
    calendar: str,
) -> dict[str, Any]:
    open_positions = portfolio.open_positions()
    liquidation_mid = 0.0
    liquidation_cons = 0.0
    open_risk_exposure = 0.0

    for pos in open_positions:
        open_risk_exposure += pos.risk_exposure
        mid_cf = _liquidation_cashflow(
            pos,
            snapshot,
            schedule=schedule,
            slippage_bps=slippage_bps,
            mode="mid",
        )
        cons_cf = _liquidation_cashflow(
            pos,
            snapshot,
            schedule=schedule,
            slippage_bps=slippage_bps,
            mode="conservative",
        )
        if mid_cf is not None:
            liquidation_mid += mid_cf
        if cons_cf is not None:
            liquidation_cons += cons_cf

    return {
        "ts_event": snapshot.ts_event,
        "session_date": snapshot.ts_event.date().isoformat(),
        "calendar": calendar,
        "cash": portfolio.cash,
        "reserved_margin": portfolio.reserved_margin,
        "open_positions": len(open_positions),
        "open_risk_exposure": open_risk_exposure,
        "realized_pnl": portfolio.realized_pnl,
        "fees_total": portfolio.fees.total,
        "equity_mid": portfolio.cash + liquidation_mid,
        "equity_conservative": portfolio.cash + liquidation_cons,
    }


def _liquidation_cashflow(
    pos: SpreadPosition,
    snapshot: OptionChainSnapshot,
    *,
    schedule: IbkrFeeSchedule,
    slippage_bps: float,
    mode: Literal["mid", "conservative"],
) -> float | None:
    close_spread = build_close_spread(pos.spread)
    quotes_by_symbol = _quotes_map(snapshot)

    if mode == "mid":
        # Bid=ask=mid to model a mark at mid.
        mid_quotes: dict[str, tuple[float | None, float | None]] = {}
        for sym in (close_spread.long_leg.symbol, close_spread.short_leg.symbol):
            bid, ask = quotes_by_symbol.get(sym, (None, None))
            if bid is None or ask is None:
                return None
            mid = (bid + ask) / 2.0
            mid_quotes[sym] = (mid, mid)
        use_quotes = mid_quotes
    else:
        use_quotes = quotes_by_symbol

    fill = fill_vertical_spread(
        close_spread,
        use_quotes,
        slippage_bps=slippage_bps,
        tick_size_fn=tick_size_for_symbol,
    )
    if fill.net_debit is None:
        return None
    fees = estimate_spread_fees(fill, schedule=schedule)
    return cashflow_from_fill(fill, fees=fees)


def _quotes_map(
    snapshot: OptionChainSnapshot,
) -> dict[str, tuple[float | None, float | None]]:
    return {
        row.symbol: (safe_float(row.bid), safe_float(row.ask))
        for row in snapshot.chain.itertuples(index=False)
    }


def _quotes_asof_for_symbols(
    *,
    trade_date: dt.date,
    ts_event: dt.datetime,
    symbols: list[str],
    root: Path,
    quotes_schema: str,
) -> dict[str, tuple[float | None, float | None]]:
    if not symbols:
        return {}

    df = load_option_quotes_for_symbols(
        trade_date,
        symbols,
        root=root,
        schema=quotes_schema,
    )
    if df.empty:
        return {sym: (None, None) for sym in symbols}

    target = pd.Timestamp(ts_event)
    quotes_by_symbol: dict[str, tuple[float | None, float | None]] = {}

    def _last_finite(series: pd.Series) -> float | None:
        # Quotes can have NaN/inf due to data quality quirks. We want the last *finite*
        # value at-or-before the target time; if none exists, treat as missing.
        for value in series.to_numpy()[::-1]:
            out = safe_float(value)
            if out is not None:
                return out
        return None

    for sym in symbols:
        sym_df = df[df["symbol"] == sym]
        sym_df = sym_df[sym_df["ts_event"] <= target].sort_values("ts_event")
        if sym_df.empty:
            quotes_by_symbol[sym] = (None, None)
            continue
        bid = _last_finite(sym_df["bid"])
        ask = _last_finite(sym_df["ask"])
        quotes_by_symbol[sym] = (bid, ask)

    return quotes_by_symbol


def _fill_spread_from_quotes(
    spread: VerticalSpread,
    quotes_by_symbol: dict[str, tuple[float | None, float | None]],
    *,
    fill_model: Literal[
        "conservative",
        "mid",
        "mid_with_slippage",
        "spread_inside",
        "spread_inside_with_slippage",
    ],
    slippage_bps: float,
    fill_alpha: float,
):
    if fill_model in ("spread_inside", "spread_inside_with_slippage"):
        inside_slippage = 0.0 if fill_model == "spread_inside" else slippage_bps
        return fill_vertical_spread_inside(
            spread,
            quotes_by_symbol,
            alpha=float(fill_alpha),
            slippage_bps=float(inside_slippage),
            net_tick_size_fn=tick_size_for_symbol,
            leg_tick_size_fn=None,
        )

    use_quotes: dict[str, tuple[float | None, float | None]]
    use_slippage: float
    if fill_model == "conservative":
        use_quotes = quotes_by_symbol
        use_slippage = slippage_bps
    else:
        use_slippage = 0.0 if fill_model == "mid" else slippage_bps
        use_quotes = {}
        for sym in (spread.long_leg.symbol, spread.short_leg.symbol):
            bid, ask = quotes_by_symbol.get(sym, (None, None))
            if bid is None or ask is None:
                use_quotes[sym] = (None, None)
                continue
            mid = (bid + ask) / 2.0
            use_quotes[sym] = (mid, mid)

    return fill_vertical_spread(
        spread,
        use_quotes,
        slippage_bps=use_slippage,
        tick_size_fn=tick_size_for_symbol,
    )


def _fill_spread(
    spread: VerticalSpread,
    snapshot: OptionChainSnapshot,
    *,
    fill_model: Literal[
        "conservative",
        "mid",
        "mid_with_slippage",
        "spread_inside",
        "spread_inside_with_slippage",
    ],
    slippage_bps: float,
    fill_alpha: float,
):
    quotes_by_symbol = _quotes_map(snapshot)
    return _fill_spread_from_quotes(
        spread,
        quotes_by_symbol,
        fill_model=fill_model,
        slippage_bps=slippage_bps,
        fill_alpha=fill_alpha,
    )


def _position_to_trade_row(
    pos: SpreadPosition, *, stage: Literal["OPEN", "CLOSE"], reason: str | None = None
) -> dict[str, Any]:
    entry_leg_prices = {f.symbol: f.price for f in pos.entry_fill.leg_fills}
    row: dict[str, Any] = {
        "stage": stage,
        "reason": reason,
        "position_id": pos.position_id,
        "opened_at": pos.opened_at,
        "closed_at": pos.closed_at,
        "right": pos.spread.right,
        "expiration": pos.spread.expiration.isoformat(),
        "contracts": pos.spread.quantity,
        "long_symbol": pos.spread.long_leg.symbol,
        "short_symbol": pos.spread.short_leg.symbol,
        "long_strike": pos.spread.long_leg.strike,
        "short_strike": pos.spread.short_leg.strike,
        "multiplier": pos.spread.multiplier,
        "entry_net_debit": pos.entry_fill.net_debit,
        "entry_cashflow": pos.entry_cashflow,
        "entry_fees_total": pos.entry_fees.total,
        "entry_commission": pos.entry_fees.commission,
        "entry_regulatory": pos.entry_fees.regulatory,
        "entry_transaction": pos.entry_fees.transaction,
        "entry_sec_fee": pos.entry_fees.sec_fee,
        "max_profit_dollars": pos.max_profit_dollars,
        "max_loss_dollars": pos.max_loss_dollars,
        "risk_exposure": pos.risk_exposure,
        "margin_required": pos.margin_required,
        "entry_long_price": entry_leg_prices.get(pos.spread.long_leg.symbol),
        "entry_short_price": entry_leg_prices.get(pos.spread.short_leg.symbol),
    }
    if pos.exit_fill is not None and pos.exit_fees is not None:
        exit_leg_prices = {f.symbol: f.price for f in pos.exit_fill.leg_fills}
        row.update(
            {
                "exit_net_debit": pos.exit_fill.net_debit,
                "exit_cashflow": pos.exit_cashflow,
                "exit_fees_total": pos.exit_fees.total,
                "exit_commission": pos.exit_fees.commission,
                "exit_regulatory": pos.exit_fees.regulatory,
                "exit_transaction": pos.exit_fees.transaction,
                "exit_sec_fee": pos.exit_fees.sec_fee,
                "realized_pnl": pos.realized_pnl,
                "exit_buyback_price": exit_leg_prices.get(
                    build_close_spread(pos.spread).long_leg.symbol
                ),
                "exit_sell_price": exit_leg_prices.get(
                    build_close_spread(pos.spread).short_leg.symbol
                ),
            }
        )
    return row


def _make_run_id() -> str:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}_{uuid.uuid4().hex[:8]}"


def _build_summary(
    equity_df: pd.DataFrame,
    *,
    trades: pd.DataFrame,
    run_id: str,
    start: dt.date,
    end: dt.date,
    strategy: str,
    initial_cash: float,
    portfolio: PortfolioState,
    pdt_blocked_opens: int,
) -> dict[str, Any]:
    equity = equity_df.get("equity_conservative")
    returns = None
    equity_for_stats = None
    if equity is not None and not equity_df.empty:
        series = equity_df["equity_conservative"].astype(float).reset_index(drop=True)
        # Include the starting equity so drawdown/returns reflect the full run.
        if (
            not series.empty
            and pd.notna(series.iloc[0])
            and abs(float(series.iloc[0]) - float(initial_cash)) < 1e-9
        ):
            equity_for_stats = series
        else:
            equity_for_stats = pd.concat(
                [pd.Series([float(initial_cash)]), series],
                ignore_index=True,
            )
        returns = equity_for_stats.pct_change().dropna()

    sharpe = None
    sortino = None
    if returns is not None and len(returns) >= 2:
        mean = float(returns.mean())
        std = float(returns.std(ddof=1))
        if std > 0:
            sharpe = mean / std * (252**0.5)
        downside = returns[returns < 0]
        if len(downside) >= 2:
            dstd = float(downside.std(ddof=1))
            if dstd > 0:
                sortino = mean / dstd * (252**0.5)

    max_dd = None
    if equity_for_stats is not None and not equity_for_stats.empty:
        peak = equity_for_stats.cummax()
        dd = (equity_for_stats / peak) - 1.0
        max_dd = float(dd.min())

    tail_loss_p05 = None
    worst_daily_return = None
    if returns is not None and len(returns) >= 1:
        tail_loss_p05 = float(returns.quantile(0.05))
        worst_daily_return = float(returns.min())

    trade_count = 0
    if not trades.empty and "stage" in trades.columns:
        trade_count = int((trades["stage"] == "CLOSE").sum())
    open_tx_count = (
        int((trades["stage"] == "OPEN").sum()) if "stage" in trades.columns else 0
    )
    day_trade_count = 0
    if (
        not trades.empty
        and "opened_at" in trades.columns
        and "closed_at" in trades.columns
    ):
        closed = trades[trades["stage"] == "CLOSE"].copy()
        if not closed.empty:
            opened = pd.to_datetime(closed["opened_at"], utc=True, errors="coerce")
            closed_at = pd.to_datetime(closed["closed_at"], utc=True, errors="coerce")
            day_trade_count = int((opened.dt.date == closed_at.dt.date).sum())

    gross_pnl = None
    turnover_dollars = None
    fee_drag_pct_gross = None
    turnover_over_avg_equity = None
    if not trades.empty and "stage" in trades.columns:
        closed = trades[trades["stage"] == "CLOSE"].copy()
        if not closed.empty:
            for col in (
                "entry_cashflow",
                "entry_fees_total",
                "exit_cashflow",
                "exit_fees_total",
                "entry_net_debit",
                "exit_net_debit",
                "multiplier",
                "contracts",
            ):
                if col in closed.columns:
                    closed[col] = pd.to_numeric(closed[col], errors="coerce")

            # Cashflows already include fees. Add them back to estimate pre-fee PnL.
            gross_pnl = float(
                (
                    closed["entry_cashflow"]
                    + closed["entry_fees_total"]
                    + closed["exit_cashflow"]
                    + closed["exit_fees_total"]
                ).sum()
            )

            if all(col in closed.columns for col in ("contracts", "multiplier")):
                turnover_dollars = float(
                    (
                        (
                            closed["entry_net_debit"]
                            * closed["multiplier"]
                            * closed["contracts"]
                        ).abs()
                        + (
                            closed["exit_net_debit"]
                            * closed["multiplier"]
                            * closed["contracts"]
                        ).abs()
                    ).sum()
                )

            if gross_pnl != 0:
                fee_drag_pct_gross = float(portfolio.fees.total / abs(gross_pnl))

            if equity is not None and not equity_df.empty:
                avg_equity = float(
                    equity_df["equity_conservative"].astype(float).mean()
                )
                if avg_equity != 0 and turnover_dollars is not None:
                    turnover_over_avg_equity = float(turnover_dollars / avg_equity)

    final_equity = None
    if equity is not None and not equity_df.empty:
        final_equity = float(equity_df["equity_conservative"].iloc[-1])

    max_margin_used = None
    max_risk_exposure = None
    max_risk_exposure_pct_equity = None
    if not equity_df.empty:
        if "reserved_margin" in equity_df.columns:
            max_margin_used = float(equity_df["reserved_margin"].astype(float).max())
        if "open_risk_exposure" in equity_df.columns:
            max_risk_exposure = float(
                equity_df["open_risk_exposure"].astype(float).max()
            )
            eq = equity_df.get("equity_conservative")
            if eq is not None:
                denom = (
                    equity_df["equity_conservative"].astype(float).replace(0.0, pd.NA)
                )
                ratio = equity_df["open_risk_exposure"].astype(float) / denom
                ratio = ratio.dropna()
                if not ratio.empty:
                    max_risk_exposure_pct_equity = float(ratio.max())

    exit_reason_counts: dict[str, int] = {}
    if not trades.empty and "stage" in trades.columns and "reason" in trades.columns:
        closed = trades[trades["stage"] == "CLOSE"]
        for reason in closed["reason"].dropna().astype(str):
            exit_reason_counts[reason] = exit_reason_counts.get(reason, 0) + 1

    return {
        "run_id": run_id,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "strategy": strategy,
        "initial_cash": initial_cash,
        "final_cash": portfolio.cash,
        "final_equity_conservative": final_equity,
        "realized_pnl": portfolio.realized_pnl,
        "fees_total": portfolio.fees.total,
        "gross_pnl_estimate": gross_pnl,
        "fee_drag_pct_gross": fee_drag_pct_gross,
        "turnover_dollars": turnover_dollars,
        "turnover_over_avg_equity": turnover_over_avg_equity,
        "trade_count": trade_count,
        "open_transactions": open_tx_count,
        "day_trades": day_trade_count,
        "pdt_blocked_opens": pdt_blocked_opens,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_drawdown": max_dd,
        "max_margin_used": max_margin_used,
        "max_risk_exposure": max_risk_exposure,
        "max_risk_exposure_pct_equity": max_risk_exposure_pct_equity,
        "exit_reason_counts": exit_reason_counts,
        "tail_loss_p05": tail_loss_p05,
        "worst_daily_return": worst_daily_return,
    }
