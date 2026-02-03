from __future__ import annotations

import dataclasses
import datetime as dt
from collections.abc import Callable, Iterable

from spy2.options.fill import SpreadFill, fill_vertical_spread
from spy2.options.models import OptionChainSnapshot, VerticalSpread


@dataclasses.dataclass(frozen=True)
class SpreadTrade:
    ts_event: dt.datetime
    spread: VerticalSpread
    fill: SpreadFill


class BacktestEngine:
    def __init__(
        self,
        *,
        strategy: Callable[[OptionChainSnapshot], Iterable[VerticalSpread]],
        slippage_bps: float = 0.0,
    ) -> None:
        self._strategy = strategy
        self._slippage_bps = slippage_bps

    def run(
        self,
        snapshots: Iterable[OptionChainSnapshot],
    ) -> list[SpreadTrade]:
        trades: list[SpreadTrade] = []
        for snapshot in snapshots:
            spreads = list(self._strategy(snapshot))
            if not spreads:
                continue
            quotes_by_symbol = {
                row.symbol: (row.bid, row.ask)
                for row in snapshot.chain.itertuples(index=False)
            }
            for spread in spreads:
                fill = fill_vertical_spread(
                    spread,
                    quotes_by_symbol,
                    slippage_bps=self._slippage_bps,
                )
                trades.append(
                    SpreadTrade(
                        ts_event=snapshot.ts_event,
                        spread=spread,
                        fill=fill,
                    )
                )
        return trades
