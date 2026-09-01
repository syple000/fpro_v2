"""单进程、同步、确定性的日频事件循环。"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, time

from backtest.broker import SimBroker
from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.data import DataPortal, event_time
from backtest.portfolio import Portfolio
from backtest.strategy import PortfolioView, RebalanceRequest, Strategy, StrategyContext
from backtest.types import (
    CorporateActionEvent,
    EquitySnapshot,
    EventType,
    Fill,
    Order,
    OrderEvent,
    OrderReason,
    OrderSide,
)


@dataclass(frozen=True, slots=True)
class BacktestResult:
    run_id: str
    strategy_id: str
    sessions: tuple[date, ...]
    events: tuple[dict[str, object], ...]
    orders: tuple[Order, ...]
    order_events: tuple[OrderEvent, ...]
    fills: tuple[Fill, ...]
    positions: tuple[dict[str, object], ...]
    equity: tuple[EquitySnapshot, ...]
    corporate_actions: tuple[CorporateActionEvent, ...]
    warnings: tuple[str, ...]


class BacktestEngine:
    """按 09:25 → 09:30 → 16:05 → 17:05 推进模拟时钟。"""

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
        self.broker = SimBroker(
            run_id=run_id,
            strategy_id=strategy.strategy_id,
            execution=config.execution,
            fees=config.fee,
        )
        self.actions = CorporateActionProcessor(
            portal.corporate_actions,
            config.corporate_actions,
        )
        self._events: list[dict[str, object]] = []
        self._positions: list[dict[str, object]] = []
        self._equity: list[EquitySnapshot] = []
        self._warnings = [
            "2017 年之前缺少交易日历，既有股票的上市交易日龄按 252/365.2425 估计。",
            "第一版只处理 dividend 数据集可识别的现金分红与送转；"
            "配股、换股等无数据事件无法自动发现。",
            "未配置指数行情，因此本次报告不计算基准与超额收益。",
        ]

    def run(self) -> BacktestResult:
        if not self.portal.sessions:
            self.portal.load()
            # load 后才获得公司行动，重建一次处理器。
            self.actions = CorporateActionProcessor(
                self.portal.corporate_actions,
                self.config.corporate_actions,
            )
        first = self.portal.sessions[0]
        initialize_context = self._context(event_time(first, time(9, 25)))
        self.strategy.initialize(initialize_context)
        if initialize_context._rebalance_request is not None:
            raise ValueError("initialize() 不能下单；请使用 on_pre_open()")

        for session_index, session in enumerate(self.portal.sessions):
            bars = self.portal.prepare_session(session)
            pre_open = event_time(session, time(9, 25))
            self._record_event(pre_open, EventType.PRE_OPEN)
            self.portfolio.unlock_t1()
            self.actions.pre_open(pre_open, portfolio=self.portfolio, broker=self.broker)
            self._write_off_delisted(session, pre_open)

            pre_context = self._context(pre_open)
            pre_data = self.portal.strategy_view(session, session_index)
            self.strategy.on_pre_open(pre_context, pre_data)
            self._apply_context(pre_context, earliest_session=session)

            market_open = event_time(session, time(9, 30))
            self._record_event(market_open, EventType.MARKET_OPEN)
            open_symbols = tuple(order.symbol for order in self.broker.open_orders)
            statuses = self.portal.statuses(session, open_symbols) if open_symbols else {}
            self.broker.match_open(
                event_time=market_open,
                bars=bars,
                statuses=statuses,
                previous_volumes=self.portal.previous_volumes(open_symbols),
                portfolio=self.portfolio,
            )

            daily_ready = event_time(session, time(16, 5))
            self._record_event(daily_ready, EventType.DAILY_READY)
            self.portal.release_close(session, session_index)
            close_prices = {
                symbol: bar.close
                for symbol, bar in self.portal.released_bars.items()
                if bar.close is not None
            }
            self.portfolio.mark_to_market(close_prices)
            self.actions.capture_record_date(daily_ready, self.portfolio)
            snapshot = self.portfolio.snapshot(session)
            self._equity.append(snapshot)
            self._record_positions(session)

            close_context = self._context(daily_ready)
            close_data = self.portal.strategy_view(session, session_index)
            self.strategy.on_close(close_context, close_data)
            next_session = self.portal.next_session(session)
            if next_session is not None:
                self._apply_context(close_context, earliest_session=next_session)

            metrics_ready = event_time(session, time(17, 5))
            self._record_event(metrics_ready, EventType.DAILY_METRICS_READY)
            self.portfolio.assert_invariants()

        final_time = event_time(self.portal.sessions[-1], time(23, 59, 59))
        self.broker.expire_all(final_time, self.portfolio)
        return BacktestResult(
            run_id=self.run_id,
            strategy_id=self.strategy.strategy_id,
            sessions=self.portal.sessions,
            events=tuple(self._events),
            orders=tuple(self.broker.orders),
            order_events=tuple(self.broker.order_events),
            fills=tuple(self.broker.fills),
            positions=tuple(self._positions),
            equity=tuple(self._equity),
            corporate_actions=tuple(self.actions.events),
            warnings=tuple(self._warnings),
        )

    def _context(self, now: datetime) -> StrategyContext:
        return StrategyContext(
            now=now,
            portfolio=PortfolioView(
                cash=self.portfolio.cash,
                total_equity=self.portfolio.total_equity,
                positions=self.portfolio.positions,
            ),
        )

    def _apply_context(self, context: StrategyContext, *, earliest_session: date) -> None:
        for order_id in context._cancel_order_ids:
            self.broker.cancel(order_id, context.now, self.portfolio)
        request = context._rebalance_request
        if request is None:
            return
        self._submit_rebalance(request, context=context, earliest_session=earliest_session)

    def _submit_rebalance(
        self,
        request: RebalanceRequest,
        *,
        context: StrategyContext,
        earliest_session: date,
    ) -> None:
        symbols = set(request.target_weights)
        if request.liquidate_omitted:
            symbols.update(
                symbol
                for symbol, position in self.portfolio.positions.items()
                if position.total_quantity > 0
            )
        for symbol in sorted(symbols):
            weight = request.target_weights.get(symbol, 0.0)
            current = self.portfolio.positions.get(symbol)
            current_quantity = current.total_quantity if current else 0
            if weight == 0:
                target_quantity = 0
            else:
                price = self.portal.last_prices.get(symbol)
                if price is None or not math.isfinite(price) or price <= 0:
                    continue
                target_quantity = math.floor(
                    self.portfolio.total_equity * weight
                    / price
                    / self.config.execution.lot_size
                ) * self.config.execution.lot_size
            difference = target_quantity - current_quantity
            if difference == 0:
                continue
            if difference > 0:
                side = OrderSide.BUY
                quantity = difference // self.config.execution.lot_size
                quantity *= self.config.execution.lot_size
            else:
                side = OrderSide.SELL
                quantity = -difference
                if target_quantity != 0:
                    quantity = quantity // self.config.execution.lot_size
                    quantity *= self.config.execution.lot_size
            if quantity <= 0:
                continue
            self.broker.submit_market_order(
                symbol=symbol,
                side=side,
                quantity=quantity,
                submitted_at=context.now,
                earliest_fill_at=event_time(earliest_session, time(9, 30)),
                portfolio=self.portfolio,
                target_weight=weight,
            )

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
            self.broker.cancel_symbol(
                symbol,
                event_time=event_at,
                portfolio=self.portfolio,
                reason=OrderReason.DELISTED,
            )
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

    def _record_positions(self, session: date) -> None:
        for symbol, position in sorted(self.portfolio.positions.items()):
            if position.total_quantity <= 0:
                continue
            self._positions.append(
                {
                    "session": session,
                    "symbol": symbol,
                    "total_quantity": position.total_quantity,
                    "sellable_quantity": position.sellable_quantity,
                    "pending_listing_quantity": position.pending_listing_quantity,
                    "frozen_quantity": position.frozen_quantity,
                    "average_cost": position.average_cost,
                    "last_price": position.last_price,
                    "market_value": position.market_value,
                    "realized_pnl": position.realized_pnl,
                    "unrealized_pnl": position.unrealized_pnl,
                    "stale_price": position.stale_price,
                }
            )

    def _record_event(self, at: datetime, event_type: EventType) -> None:
        self._events.append({"event_time": at, "event_type": event_type.value})
