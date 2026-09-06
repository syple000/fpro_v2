"""把时钟、数据、撮合、账户和策略串成一条同步流水线。"""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import date, datetime, time

from backtest.broker import SimulatedBroker
from backtest.clock import Clock, Event, at_time, market_timeline
from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.domain import BacktestResult, Bar, EquitySnapshot, OrderReason
from backtest.errors import DataError
from backtest.orders import create_orders
from backtest.portfolio import Portfolio
from backtest.strategy import Strategy
from backtest.universe import listed_symbols
from market_data import ALL_SYMBOLS, DataReader, DataView


class BacktestEngine:
    """按固定顺序执行回测，不实现数据、撮合或账户的内部规则。"""

    def __init__(
        self,
        *,
        reader: DataReader,
        config: BacktestConfig,
        sessions: Sequence[date],
        calendar: Sequence[date],
        strategy: Strategy,
        actions: CorporateActionProcessor | None = None,
    ) -> None:
        """创建回测所需的时钟、账户和模拟 Broker。"""
        if not sessions:
            raise DataError("回测区间内没有交易日")
        self.reader = reader
        self.config = config
        self.strategy = strategy
        self.events = market_timeline(sessions, calendar, config.frequency)
        self.clock = Clock()
        self.portfolio = Portfolio(config.initial_cash)
        self.broker = SimulatedBroker(config)
        self.actions = actions or CorporateActionProcessor(())
        self._equity: list[EquitySnapshot] = []

    def run(self) -> BacktestResult:
        """逐个事件推进，最后返回订单、成交和每日净值。"""
        active_session: date | None = None
        for event in self.events:
            if event.session != active_session:
                start_at = at_time(event.session, time(9, 25))
                self.clock.move_to(start_at)
                self._start_session(start_at, self.reader.at(start_at))
                active_session = event.session
            self.clock.move_to(event.at)
            self._process_bar(event, self.reader.at(event.at))
            if event.is_session_end:
                self._end_session(event)
            self.portfolio.assert_valid()

        if self.clock.now is not None:
            self.broker.expire_all(self.clock.now)
        equity = tuple(self._equity)
        return BacktestResult(
            sessions=tuple(snapshot.session for snapshot in equity),
            orders=self.broker.orders,
            order_updates=self.broker.updates,
            fills=self.broker.fills,
            equity=equity,
        )

    def _start_session(self, at: datetime, data: DataView) -> None:
        """日初解锁 T+1，处理公司行动，并核销退市持仓。"""
        self.portfolio.unlock_t1()
        self.actions.on_session_start(at, self.portfolio, self.broker)

        held = {holding.symbol for holding in self.portfolio.account_snapshot().holdings}
        managed = set(self.broker.pending_symbols) | held
        if not managed:
            return
        for symbol in sorted(managed - listed_symbols(data)):
            self.broker.cancel_symbol(symbol, OrderReason.DELISTED, at)
            self.portfolio.write_off(symbol)

    def _process_bar(self, event: Event, data: DataView) -> None:
        """读取 Bar，然后依次撮合、估值、执行策略和提交订单。"""
        bars = self._read_bars(event, data)

        # 先撮合旧订单，保证本次策略产生的订单只能使用下一根 Bar 的开盘价。
        if self.broker.pending_symbols:
            fills = self.broker.match_bar(
                event=event,
                bars=bars,
                account=self.portfolio.account_snapshot(),
                data=data,
            )
            for fill in fills:
                self.portfolio.apply_fill(fill)

        prices = {
            symbol: float(bar.close)
            for symbol, bar in bars.items()
            if bar.close is not None and math.isfinite(bar.close) and bar.close > 0
        }
        self.portfolio.mark_to_market(prices)

        weights = self.strategy.on_bar(data, event, self.portfolio.account_snapshot())
        if weights is None:
            return
        for order in create_orders(
            weights,
            self.portfolio.account_snapshot(),
            prices,
        ):
            self.broker.submit(order, event.at)

    def _read_bars(self, event: Event, data: DataView) -> dict[str, Bar]:
        """读取当前事件区间内用于撮合和估值的 open、close。"""
        rows = data.market.bars(
            symbols=self.config.symbols or ALL_SYMBOLS,
            frequency=event.frequency,
            start=event.interval_start,
            end=event.at,
            fields=("open", "close"),
            adjustment="none",
        ).table.to_pylist()
        bars: dict[str, Bar] = {}
        for row in rows:
            symbol = row["symbol"]
            if symbol in bars:
                raise DataError(f"一个 K 线事件出现重复证券: {symbol}")
            bars[symbol] = Bar(
                symbol=symbol,
                interval_start=row["interval_start"],
                open=row.get("open"),
                close=row.get("close"),
            )
        return bars

    def _end_session(self, event: Event) -> None:
        """日终登记公司行动权益并记录账户净值。"""
        self.actions.on_session_end(event.at, self.portfolio)
        self._equity.append(self.portfolio.equity_snapshot(event.session))
