import datetime as dt

import pandas as pd
import pytest

from spy2.backtest import runner as bt_runner
from spy2.portfolio.models import PortfolioState


def test_summary_returns_and_drawdown_include_initial_cash() -> None:
    # Equity curve starts below initial cash (e.g., opened a debit spread).
    equity_df = pd.DataFrame(
        {
            "equity_conservative": [90.0, 90.0, 95.0],
        }
    )
    portfolio = PortfolioState(cash=95.0, realized_pnl=-5.0)

    summary = bt_runner._build_summary(  # type: ignore[attr-defined]
        equity_df,
        trades=pd.DataFrame(),
        run_id="test_run",
        start=dt.date(2026, 1, 1),
        end=dt.date(2026, 1, 3),
        strategy="test",
        initial_cash=100.0,
        portfolio=portfolio,
        pdt_blocked_opens=0,
    )

    assert summary["max_drawdown"] == pytest.approx(-0.1)
    assert summary["worst_daily_return"] == pytest.approx(-0.1)
    assert summary["sharpe"] is None or summary["sharpe"] < 0
