"""把时钟、数据、撮合、账户和策略串成一条同步流水线。"""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import date, datetime

from backtest.broker import SimulatedBroker
from backtest.clock import Clock, Event, market_timeline
from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.domain import BacktestResult, Bar, EquitySnapshot, OrderReason
from backtest.errors import DataError
from backtest.orders import create_orders, validate_target_weights
from backtest.portfolio import Portfolio
from backtest.strategy import Strategy, StrategyContext
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
        calendar: Sequence[date] = (),
        strategy: Strategy,
        actions: CorporateActionProcessor | None = None,
    ) -> None:
        """创建回测所需的时钟、账户和模拟 Broker。"""
        if not sessions:
            raise DataError("回测区间内没有交易日")
        self.reader = reader
        self.config = config
        self.strategy = strategy
        market_events = market_timeline(sessions, config.frequency, config.market)
        strategy_events = strategy.schedule.events(market_events, calendar, config.market)
        # 同一时刻先处理市场，再调用策略，最后登记权益与净值。
        priority = {"session_start": 0, "auction": 1, "bar": 1, "strategy": 2, "session_end": 3}
        self.events = tuple(
            sorted(
                (*market_events, *strategy_events),
                key=lambda event: (event.at, priority[event.kind]),
            )
        )
        self.clock = Clock()
        self.portfolio = Portfolio(config.initial_cash)
        self.broker = SimulatedBroker(config)
        self.actions = actions or CorporateActionProcessor(())
        self.actions.set_sessions(
            sessions, start_date=config.start_date, end_date=config.end_date
        )
        self._equity: list[EquitySnapshot] = []

    def run(self) -> BacktestResult:
        """逐个事件推进，最后返回订单、成交和每日净值。"""
        for event in self.events:
            self.clock.move_to(event.at)
            data = self.reader.at(event.at)
            if event.kind == "session_start":
                self._start_session(event.at, data)
            elif event.kind in {"bar", "auction"}:
                self._process_bar(event, data)
            elif event.kind == "strategy":
                self._run_strategy(event, data)
            elif event.kind == "session_end":
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
        """每根 Bar 都撮合和估值，与策略是否调用无关。"""
        bars = self._read_bars(event, data)
        prices: dict[str, float] = {}
        for symbol, bar in bars.items():
            if bar.close is None:
                continue
            if not math.isfinite(bar.close) or bar.close <= 0:
                raise DataError(f"{symbol} {event.at.isoformat()} 收盘价无效: {bar.close}")
            prices[symbol] = float(bar.close)

        # 先撮合旧订单，保证本次策略产生的订单只能使用下一根 Bar 的开盘价。
        if event.kind == "bar" and self.broker.pending_symbols:
            fills = self.broker.match_bar(
                event=event,
                bars=bars,
                account=self.portfolio.account_snapshot(),
                data=self.reader.at(event.interval_start),
            )
            for fill in fills:
                self.portfolio.apply_fill(fill)

        self.portfolio.mark_to_market(prices)

    def _run_strategy(self, event: Event, data: DataView) -> None:
        """只在策略声明的时间调用；历史价格继续通过 DataView 查询。"""
        account = self.portfolio.account_snapshot()
        context = StrategyContext(data, event, account, self.broker)
        weights = self.strategy.on_event(context)
        if weights is None:
            return
        targets = validate_target_weights(weights)
        if self.config.symbols is not None and set(targets) - set(self.config.symbols):
            raise ValueError("目标组合包含本次回测 symbols 之外的证券")
        symbols = tuple(symbol for symbol, weight in targets.items() if weight > 0)
        rows = (
            data.market.bars(
                symbols=symbols,
                frequency=self.config.frequency,
                count=1,
                fields=("close",),
                adjustment="none",
            ).table.to_pylist()
            if symbols
            else []
        )
        prices = {row["symbol"]: row["close"] for row in rows}
        for order in create_orders(targets, account, prices, trading_date=event.session):
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
