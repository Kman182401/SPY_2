from __future__ import annotations

import dataclasses

from spy2.portfolio.models import SpreadPosition


@dataclasses.dataclass(frozen=True)
class ExitRuleConfig:
    """
    Basic exit rules (v1).

    Interpretation:
    - profit_take_frac: close when unrealized_pnl >= profit_take_frac * max_profit_dollars.
      If max_profit_dollars is missing, uses profit_take_frac * abs(entry_cashflow).
    - stop_loss_frac: close when -unrealized_pnl >= stop_loss_frac * max_loss_dollars.
      If max_loss_dollars is missing, uses stop_loss_frac * abs(entry_cashflow).
    - max_hold_sessions: close when held_sessions >= max_hold_sessions.
    """

    enabled: bool = False
    profit_take_frac: float | None = None
    stop_loss_frac: float | None = None
    max_hold_sessions: int | None = None


@dataclasses.dataclass(frozen=True)
class ExitEval:
    should_close: bool
    reason: str | None
    unrealized_pnl: float | None = None
    held_sessions: int | None = None
    profit_target: float | None = None
    stop_threshold: float | None = None


def evaluate_exit_rules(
    *,
    pos: SpreadPosition,
    liquidation_cashflow: float | None,
    held_sessions: int | None,
    config: ExitRuleConfig,
) -> ExitEval:
    if not config.enabled:
        return ExitEval(should_close=False, reason=None)

    unrealized_pnl: float | None = None
    if liquidation_cashflow is not None:
        unrealized_pnl = float(pos.entry_cashflow) + float(liquidation_cashflow)

    # Time stop can trigger even if we can't price liquidation at this instant.
    if config.max_hold_sessions is not None and held_sessions is not None:
        if held_sessions >= int(config.max_hold_sessions):
            return ExitEval(
                should_close=True,
                reason="TIME_STOP",
                unrealized_pnl=unrealized_pnl,
                held_sessions=held_sessions,
            )

    # Price-based exits require a liquidation mark.
    if unrealized_pnl is None:
        return ExitEval(should_close=False, reason=None, held_sessions=held_sessions)

    fallback = abs(float(pos.entry_cashflow))
    profit_target = None
    if config.profit_take_frac is not None:
        base = (
            pos.max_profit_dollars if pos.max_profit_dollars is not None else fallback
        )
        profit_target = float(config.profit_take_frac) * float(base)
        if unrealized_pnl >= profit_target:
            return ExitEval(
                should_close=True,
                reason="PROFIT_TAKE",
                unrealized_pnl=unrealized_pnl,
                held_sessions=held_sessions,
                profit_target=profit_target,
            )

    stop_threshold = None
    if config.stop_loss_frac is not None:
        base = pos.max_loss_dollars if pos.max_loss_dollars is not None else fallback
        stop_threshold = float(config.stop_loss_frac) * float(base)
        if -unrealized_pnl >= stop_threshold:
            return ExitEval(
                should_close=True,
                reason="STOP_LOSS",
                unrealized_pnl=unrealized_pnl,
                held_sessions=held_sessions,
                stop_threshold=stop_threshold,
            )

    return ExitEval(
        should_close=False,
        reason=None,
        unrealized_pnl=unrealized_pnl,
        held_sessions=held_sessions,
        profit_target=profit_target,
        stop_threshold=stop_threshold,
    )
