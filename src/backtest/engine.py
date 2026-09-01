"""单进程、同步、确定性的日频回测循环。"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time

from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.data import MarketData, SessionData, event_time
from backtest.execution import LOT_SIZE, ExecutionEngine
from backtest.portfolio import Portfolio
from backtest.types import EquitySnapshot, Fill, OrderReason, OrderResult, OrderSide
from strategies import validate_target_weights


@dataclass(frozen=True, slots=True)
class BacktestResult:
    sessions: tuple[date, ...]
    orders: tuple[OrderResult, ...]
    fills: tuple[Fill, ...]
    equity: tuple[EquitySnapshot, ...]


class BacktestEngine:
    """盘前处理账户，开盘执行昨日目标，收盘生成新目标。"""

    def __init__(
        self,
        *,
        config: BacktestConfig,
        data: MarketData,
        strategy: Callable[[SessionData], Mapping[str, float] | None],
    ) -> None:
        self.config = config
        self.data = data
        self.strategy = strategy
        self.portfolio = Portfolio(config.initial_cash)
        self.execution = ExecutionEngine(config)
        self.actions = CorporateActionProcessor(data.corporate_actions)
        self._equity: list[EquitySnapshot] = []

    def run(self) -> BacktestResult:
        if not self.data.sessions:
            self.data.load()
            self.actions = CorporateActionProcessor(self.data.corporate_actions)

        for session_index, session in enumerate(self.data.sessions):
            bars = self.data.prepare_session(session)
            pre_open = event_time(session, time(9, 25))
            self.portfolio.unlock_t1()
            self.actions.pre_open(
                pre_open,
                portfolio=self.portfolio,
                execution=self.execution,
            )
            self._write_off_delisted(session)

            market_open = event_time(session, time(9, 30))
            symbols = self.execution.pending_symbols
            statuses = self.data.statuses(session, symbols) if symbols else {}
            fills = self.execution.execute_open(
                event_time=market_open,
                bars=bars,
                statuses=statuses,
                previous_volumes=self.data.previous_volumes(symbols),
                cash=self.portfolio.cash,
                total_quantities={
                    symbol: position.total_quantity
                    for symbol, position in self.portfolio.positions.items()
                },
                sellable_quantities={
                    symbol: position.sellable_quantity
                    for symbol, position in self.portfolio.positions.items()
                },
            )
            for fill in fills:
                self.portfolio.apply_fill(fill)

            close_at = event_time(session, time(16, 5))
            self.data.release_close(session, session_index)
            self.portfolio.mark_to_market(
                {
                    symbol: bar.close
                    for symbol, bar in self.data.released_bars.items()
                    if bar.close is not None
                }
            )
            self.actions.capture_record_date(close_at, self.portfolio)
            self._equity.append(self.portfolio.snapshot(session))

            targets = self.strategy(self.data.session_data(session, session_index))
            next_session = self.data.next_session(session)
            if targets is not None and next_session is not None:
                self._submit_rebalance(
                    validate_target_weights(targets),
                    submitted_at=close_at,
                    earliest_session=next_session,
                )
            self.portfolio.assert_invariants()

        self.execution.expire_all()
        return BacktestResult(
            sessions=self.data.sessions,
            orders=tuple(self.execution.results),
            fills=tuple(self.execution.fills),
            equity=tuple(self._equity),
        )

    def _submit_rebalance(
        self,
        target_weights: Mapping[str, float],
        *,
        submitted_at: datetime,
        earliest_session: date,
    ) -> None:
        symbols = set(target_weights)
        symbols.update(
            symbol
            for symbol, position in self.portfolio.positions.items()
            if position.total_quantity > 0
        )
        for symbol in sorted(symbols):
            weight = target_weights.get(symbol, 0.0)
            position = self.portfolio.positions.get(symbol)
            current_quantity = position.total_quantity if position else 0
            target_quantity = self._target_quantity(symbol, weight)
            difference = target_quantity - current_quantity
            if difference == 0:
                continue
            side = OrderSide.BUY if difference > 0 else OrderSide.SELL
            quantity = abs(difference)
            if side is OrderSide.BUY or target_quantity != 0:
                quantity = quantity // LOT_SIZE * LOT_SIZE
            if quantity <= 0:
                continue
            self.execution.submit_order(
                symbol=symbol,
                side=side,
                quantity=quantity,
                submitted_at=submitted_at,
                earliest_fill_at=event_time(earliest_session, time(9, 30)),
                target_weight=weight,
            )

    def _target_quantity(self, symbol: str, weight: float) -> int:
        if weight == 0:
            return 0
        price = self.data.last_prices.get(symbol)
        if price is None or not math.isfinite(price) or price <= 0:
            position = self.portfolio.positions.get(symbol)
            return position.total_quantity if position else 0
        return math.floor(self.portfolio.total_equity * weight / price / LOT_SIZE) * LOT_SIZE

    def _write_off_delisted(self, session: date) -> None:
        listed = self.data.listed_symbols(session)
        active = [
            symbol
            for symbol, position in self.portfolio.positions.items()
            if position.total_quantity > 0 and symbol not in listed
        ]
        for symbol in sorted(active):
            self.execution.cancel_symbol(symbol, reason=OrderReason.DELISTED)
            self.portfolio.write_off(symbol)
