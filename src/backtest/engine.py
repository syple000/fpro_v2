"""单进程、同步、确定性的日频回测循环。"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time

from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.data import DataPortal, event_time
from backtest.execution import ExecutionEngine
from backtest.portfolio import Portfolio
from backtest.strategy import PortfolioView, Strategy, validated_target_weights
from backtest.types import (
    CorporateActionEvent,
    EquitySnapshot,
    Fill,
    OrderReason,
    OrderResult,
    OrderSide,
)


@dataclass(frozen=True, slots=True)
class BacktestResult:
    run_id: str
    strategy_id: str
    sessions: tuple[date, ...]
    orders: tuple[OrderResult, ...]
    fills: tuple[Fill, ...]
    equity: tuple[EquitySnapshot, ...]
    corporate_actions: tuple[CorporateActionEvent, ...]
    warnings: tuple[str, ...]


class BacktestEngine:
    """盘前处理账户，开盘执行昨日目标，收盘生成新目标。"""

    def __init__(
        self,
        *,
        run_id: str,
        config: BacktestConfig,
        portal: DataPortal,
        strategy: Strategy,
    ) -> None:
        self.run_id = run_id
        self.config = config
        self.portal = portal
        self.strategy = strategy
        self.portfolio = Portfolio(config.initial_cash)
        self.execution = ExecutionEngine(
            execution=config.execution,
            fees=config.fee,
        )
        self.actions = CorporateActionProcessor(
            portal.corporate_actions,
            config.corporate_actions,
        )
        self._equity: list[EquitySnapshot] = []
        self._warnings = [
            "2017 年之前缺少交易日历，既有股票的上市交易日龄按 252/365.2425 估计。",
            "第一版只处理 dividend 数据集可识别的现金分红与送转；"
            "配股、换股等无数据事件无法自动发现。",
        ]

    def run(self) -> BacktestResult:
        if not self.portal.sessions:
            self.portal.load()
            self.actions = CorporateActionProcessor(
                self.portal.corporate_actions,
                self.config.corporate_actions,
            )

        for session_index, session in enumerate(self.portal.sessions):
            bars = self.portal.prepare_session(session)
            pre_open = event_time(session, time(9, 25))
            self.portfolio.unlock_t1()
            self.actions.pre_open(
                pre_open,
                portfolio=self.portfolio,
                execution=self.execution,
            )
            self._write_off_delisted(session, pre_open)

            market_open = event_time(session, time(9, 30))
            symbols = self.execution.pending_symbols
            statuses = self.portal.statuses(session, symbols) if symbols else {}
            fills = self.execution.execute_open(
                event_time=market_open,
                bars=bars,
                statuses=statuses,
                previous_volumes=self.portal.previous_volumes(symbols),
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
            self.portal.release_close(session, session_index)
            self.portfolio.mark_to_market(
                {
                    symbol: bar.close
                    for symbol, bar in self.portal.released_bars.items()
                    if bar.close is not None
                }
            )
            self.actions.capture_record_date(close_at, self.portfolio)
            self._equity.append(self.portfolio.snapshot(session))

            targets = self.strategy.on_close(
                self.portal.session_data(session, session_index),
                self._portfolio_view(),
            )
            next_session = self.portal.next_session(session)
            if targets is not None and next_session is not None:
                self._submit_rebalance(
                    validated_target_weights(targets),
                    submitted_at=close_at,
                    earliest_session=next_session,
                )
            self.portfolio.assert_invariants()

        self.execution.expire_all()
        return BacktestResult(
            run_id=self.run_id,
            strategy_id=self.strategy.strategy_id,
            sessions=self.portal.sessions,
            orders=tuple(self.execution.results),
            fills=tuple(self.execution.fills),
            equity=tuple(self._equity),
            corporate_actions=tuple(self.actions.events),
            warnings=tuple(self._warnings),
        )

    def _portfolio_view(self) -> PortfolioView:
        return PortfolioView.from_positions(
            cash=self.portfolio.cash,
            total_equity=self.portfolio.total_equity,
            positions=self.portfolio.positions,
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
                quantity = quantity // self.config.execution.lot_size
                quantity *= self.config.execution.lot_size
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
        price = self.portal.last_prices.get(symbol)
        if price is None or not math.isfinite(price) or price <= 0:
            position = self.portfolio.positions.get(symbol)
            return position.total_quantity if position else 0
        lot_size = self.config.execution.lot_size
        return math.floor(self.portfolio.total_equity * weight / price / lot_size) * lot_size

    def _write_off_delisted(self, session: date, event_at: datetime) -> None:
        if not self.config.corporate_actions.write_off_delisted:
            return
        listed = self.portal.listed_symbols(session)
        active = [
            symbol
            for symbol, position in self.portfolio.positions.items()
            if position.total_quantity > 0 and symbol not in listed
        ]
        for symbol in sorted(active):
            self.execution.cancel_symbol(symbol, reason=OrderReason.DELISTED)
            quantity, loss = self.portfolio.write_off(symbol)
            self.actions.events.append(
                CorporateActionEvent(
                    event_time=event_at,
                    action_id=f"DELIST-{symbol}-{session.isoformat()}",
                    symbol=symbol,
                    event_type="DELIST_WRITE_OFF",
                    quantity=quantity,
                    amount=-loss,
                    note="终止上市且无后续估值，核销为零",
                )
            )
