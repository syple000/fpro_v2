"""订单状态和 A 股开盘价撮合规则。"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import replace
from datetime import date, datetime

from backtest.clock import Event
from backtest.config import BacktestConfig
from backtest.domain import (
    AccountSnapshot,
    Bar,
    Fill,
    MarketStatus,
    Order,
    OrderReason,
    OrderRequest,
    OrderStatus,
    OrderUpdate,
    Side,
)
from backtest.trading_rules import QuantityRule, quantity_rule
from market_data import DataView
from market_data.identity import SecurityCodeHistory, SecurityMappingError


class SimulatedBroker:
    """保存订单，并在下一根完整 K 线到达时按其开盘价撮合一次。"""

    def __init__(
        self, config: BacktestConfig, *, identities: SecurityCodeHistory | None = None
    ) -> None:
        """固定本次回测的费用、滑点和成交量限制。"""
        self.config = config
        self.identities = identities
        self._session = config.start_date
        self._pending: list[Order] = []
        self._orders: list[Order] = []
        self._updates: list[OrderUpdate] = []
        self._fills: list[Fill] = []
        self._next_order_id = 1
        self._next_fill_id = 1

    @property
    def orders(self) -> tuple[Order, ...]:
        """全部原始订单。"""
        return tuple(self._orders)

    @property
    def updates(self) -> tuple[OrderUpdate, ...]:
        """全部订单状态变化。"""
        return tuple(self._updates)

    @property
    def fills(self) -> tuple[Fill, ...]:
        """全部成交。"""
        return tuple(self._fills)

    @property
    def pending_symbols(self) -> tuple[str, ...]:
        """仍在等待下一根 K 线的证券代码。"""
        symbols = {self._trading_symbol(order, self._session) for order in self._pending}
        return tuple(sorted(symbols))

    def set_session(self, session: date) -> None:
        self._session = session

    def _trading_symbol(self, order: Order, session: date) -> str:
        if self.identities is None:
            return order.symbol
        assert order.sid is not None
        return self.identities.code_at(order.sid, session)

    @property
    def pending_orders(self) -> tuple[Order, ...]:
        return tuple(self._pending)

    def submit(self, request: OrderRequest, submitted_at: datetime) -> Order:
        """接收订单并记录 SUBMITTED 状态。"""
        if not request.symbol:
            raise ValueError("证券代码不能为空")
        sid = self.identities.sid(request.symbol) if self.identities is not None else None
        if request.sid is not None and request.sid != sid:
            raise SecurityMappingError("订单 sid 与证券代码不一致")
        symbol = request.symbol
        if self.identities is not None and sid is not None:
            symbol = self.identities.code_at(sid, submitted_at.date())
        allowed = self.config.symbols
        if allowed is not None and self.identities is not None:
            allowed = self.identities.symbols_at(allowed, submitted_at.date())
        if allowed is not None and symbol not in allowed:
            raise ValueError(f"{request.symbol} 不在本次回测的 symbols 中")
        if (
            isinstance(request.quantity, bool)
            or not isinstance(request.quantity, int)
            or request.quantity <= 0
        ):
            raise ValueError("委托数量必须为正整数")
        rule = quantity_rule(symbol, submitted_at.date())
        # 零股卖出需等撮合时结合账户余额校验；买入可以立即检查完整规则。
        if request.quantity > rule.maximum or (
            Side(request.side) is Side.BUY and not rule.valid(request.quantity)
        ):
            raise ValueError(f"{request.symbol} 委托数量不符合申报规则: {request.quantity}")
        order = Order(
            order_id=f"O{self._next_order_id:08d}",
            symbol=symbol,
            side=Side(request.side),
            quantity=request.quantity,
            submitted_at=submitted_at,
            target_weight=request.target_weight,
            sid=sid,
        )
        self._next_order_id += 1
        self._orders.append(order)
        self._pending.append(order)
        self._updates.append(OrderUpdate(order, OrderStatus.SUBMITTED, submitted_at))
        return order

    def cancel(self, order_id: str, at: datetime) -> bool:
        """撤销仍待撮合的订单；已结束或不存在的订单返回 False。"""
        for order in self._pending:
            if order.order_id == order_id:
                self._pending.remove(order)
                self._updates.append(
                    OrderUpdate(order, OrderStatus.CANCELED, at, reason=OrderReason.STRATEGY)
                )
                return True
        return False

    def match_bar(
        self,
        *,
        event: Event,
        bars: Mapping[str, Bar],
        account: AccountSnapshot,
        data: DataView,
    ) -> tuple[Fill, ...]:
        """撮合当前所有待处理订单，并返回本次新成交。"""
        if not self._pending:
            return ()

        # 只允许在开盘时已经提交的订单参与本根 Bar，不能回填过去的成交。
        orders = [order for order in self._pending if order.submitted_at <= event.interval_start]
        self._pending = [
            order for order in self._pending if order.submitted_at > event.interval_start
        ]
        if not orders:
            return ()
        orders.sort(key=lambda order: (order.side is Side.BUY, order.symbol, order.order_id))
        symbols = tuple(sorted({self._trading_symbol(order, event.session) for order in orders}))
        statuses = self._market_statuses(data, symbols)
        previous_volumes = self._previous_volumes(
            data,
            symbols,
            frequency=event.frequency,
            interval_start=event.interval_start,
        )

        # 同一批次先卖后买，卖出释放的现金可供随后买入。
        cash = account.cash
        sellable = {holding.symbol: holding.sellable_quantity for holding in account.holdings}
        fills: list[Fill] = []
        for order in orders:
            symbol = self._trading_symbol(order, event.session)
            executable_order = replace(order, symbol=symbol)
            bar = bars.get(symbol)
            filled_at = bar.interval_start if bar is not None else event.interval_start
            quantity, reason, price = self._executable_quantity(
                executable_order,
                bar=bar,
                status=statuses.get(symbol, MarketStatus(symbol)),
                previous_volume=previous_volumes.get(symbol),
                cash=cash,
                sellable=sellable.get(symbol, 0),
                trading_date=filled_at.date(),
            )
            if quantity == 0 or price is None:
                self._updates.append(
                    OrderUpdate(
                        order,
                        OrderStatus.NOT_FILLED,
                        event.at,
                        reason=reason,
                    )
                )
                continue

            assert bar is not None and bar.open is not None
            fill = self._make_fill(executable_order, filled_at, quantity, bar.open, price)
            fills.append(fill)
            if order.side is Side.BUY:
                cash -= fill.notional + fill.total_fee
            else:
                cash += fill.notional - fill.total_fee
                sellable[symbol] = sellable.get(symbol, 0) - quantity
            status = (
                OrderStatus.FILLED if quantity == order.quantity else OrderStatus.PARTIALLY_FILLED
            )
            self._updates.append(OrderUpdate(order, status, filled_at, quantity, reason))

        self._fills.extend(fills)
        return tuple(fills)

    def cancel_symbol(self, symbol: str | int, reason: OrderReason, at: datetime) -> None:
        """取消指定证券的全部待处理订单。"""
        remaining: list[Order] = []
        sid = None
        if self.identities is not None:
            sid = self.identities.sid(symbol) if isinstance(symbol, str) else symbol
        for order in self._pending:
            if (sid is not None and order.sid == sid) or order.symbol == symbol:
                self._updates.append(OrderUpdate(order, OrderStatus.CANCELED, at, reason=reason))
            else:
                remaining.append(order)
        self._pending = remaining

    def expire_all(self, at: datetime) -> None:
        """回测结束时把待处理订单标记为 EXPIRED。"""
        for order in self._pending:
            self._updates.append(
                OrderUpdate(
                    order,
                    OrderStatus.EXPIRED,
                    at,
                    reason=OrderReason.END_OF_BACKTEST,
                )
            )
        self._pending.clear()

    @staticmethod
    def _market_statuses(
        data: DataView,
        symbols: tuple[str, ...],
    ) -> dict[str, MarketStatus]:
        """读取本次撮合需要的停牌和涨跌停数据。"""
        rows = data.market.status(
            symbols=symbols,
            fields=("suspended", "up_limit", "down_limit", "price_limit_status"),
        ).table.to_pylist()
        return {
            row["symbol"]: MarketStatus(
                symbol=row["symbol"],
                suspended=row.get("suspended"),
                up_limit=row.get("up_limit"),
                down_limit=row.get("down_limit"),
                price_limit_status=row.get("price_limit_status") or "unknown",
            )
            for row in rows
        }

    def _previous_volumes(
        self,
        data: DataView,
        symbols: tuple[str, ...],
        *,
        frequency: str,
        interval_start: datetime,
    ) -> dict[str, float | None]:
        """读取每只证券位于当前 K 线之前的最近成交量。"""
        result: dict[str, float | None] = {symbol: None for symbol in symbols}
        if self.config.volume_limit is None:
            return result
        rows = data.market.bars(
            symbols=symbols,
            frequency=frequency,
            count=2,
            fields=("volume",),
            adjustment="none",
        ).table.to_pylist()
        for row in rows:
            if row["interval_start"] < interval_start:
                result[row["symbol"]] = row.get("volume")
        return result

    def _executable_quantity(
        self,
        order: Order,
        *,
        bar: Bar | None,
        status: MarketStatus,
        previous_volume: float | None,
        cash: float,
        sellable: int,
        trading_date: date,
    ) -> tuple[int, OrderReason, float | None]:
        """依次应用行情状态、容量、持仓和现金约束。"""
        if order.quantity <= 0:
            return 0, OrderReason.INVALID_QUANTITY, None
        open_price = bar.open if bar is not None else None
        if not _valid_price(open_price):
            return 0, OrderReason.MISSING_OPEN, None
        if status.suspended is True:
            return 0, OrderReason.SUSPENDED, None
        if status.suspended is not False or not _known_price_limits(status):
            return 0, OrderReason.UNKNOWN_MARKET_STATUS, None

        assert open_price is not None
        if (status.down_limit is not None and open_price < status.down_limit - 1e-9) or (
            status.up_limit is not None and open_price > status.up_limit + 1e-9
        ):
            return 0, OrderReason.INVALID_OPEN, None
        if order.side is Side.BUY and _reaches_limit(open_price, status.up_limit, buy=True):
            return 0, OrderReason.LIMIT_UP, None
        if order.side is Side.SELL and _reaches_limit(open_price, status.down_limit, buy=False):
            return 0, OrderReason.LIMIT_DOWN, None

        direction = 1 if order.side is Side.BUY else -1
        price = open_price * (1 + direction * self.config.slippage_bps / 10_000)
        # 最终成交价先限制在合法价格区间，再计算数量、费用和滑点成本。
        if _valid_price(status.up_limit):
            assert status.up_limit is not None
            price = min(price, status.up_limit)
        if _valid_price(status.down_limit):
            assert status.down_limit is not None
            price = max(price, status.down_limit)
        quantity = order.quantity
        reason = OrderReason.NONE
        rule = quantity_rule(order.symbol, trading_date)
        if not rule.valid(quantity, liquidating=order.side is Side.SELL and quantity == sellable):
            return 0, OrderReason.INVALID_QUANTITY, None
        capacity = self._volume_capacity(previous_volume)
        if capacity is not None and quantity > capacity:
            quantity = capacity
            reason = OrderReason.VOLUME_LIMIT
        if order.side is Side.SELL and quantity > sellable:
            quantity = sellable
            reason = OrderReason.INSUFFICIENT_SELLABLE
        if not (order.side is Side.SELL and quantity == sellable):
            quantity = rule.round_down(quantity)
        if order.side is Side.BUY:
            affordable = self._affordable_quantity(quantity, price, cash, trading_date, rule)
            if affordable < quantity:
                quantity = affordable
                reason = OrderReason.INSUFFICIENT_CASH
        elif quantity > 0:
            notional = price * quantity
            fees = sum(self._fees(Side.SELL, notional, trading_date))
            if cash + notional - fees < -1e-9:
                return 0, OrderReason.INSUFFICIENT_CASH, None
        return max(quantity, 0), reason, price

    def _make_fill(
        self,
        order: Order,
        at: datetime,
        quantity: int,
        market_price: float,
        execution_price: float,
    ) -> Fill:
        """应用滑点和交易费用，生成成交记录。"""
        notional = execution_price * quantity
        commission, stamp_tax, transfer_fee = self._fees(order.side, notional, at.date())
        fill = Fill(
            fill_id=f"F{self._next_fill_id:08d}",
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            filled_at=at,
            quantity=quantity,
            market_price=market_price,
            execution_price=execution_price,
            notional=notional,
            commission=commission,
            stamp_tax=stamp_tax,
            transfer_fee=transfer_fee,
            slippage_cost=abs(execution_price - market_price) * quantity,
            sid=order.sid,
        )
        self._next_fill_id += 1
        return fill

    def _fees(self, side: Side, notional: float, trading_date: date) -> tuple[float, ...]:
        """计算佣金、印花税和过户费。"""
        commission = max(
            notional * self.config.commission_rate,
            self.config.minimum_commission,
        )
        stamp_tax = 0.0
        if side is Side.SELL:
            stamp_rate = 0.0005 if trading_date >= date(2023, 8, 28) else 0.001
            stamp_tax = notional * stamp_rate
        transfer_rate = 0.00001 if trading_date >= date(2022, 4, 29) else 0.00002
        return commission, stamp_tax, notional * transfer_rate

    def _affordable_quantity(
        self,
        requested: int,
        execution_price: float,
        cash: float,
        trading_date: date,
        rule: QuantityRule,
    ) -> int:
        """在最低佣金存在时逐手寻找可负担数量。"""
        quantity = rule.round_down(requested)
        while quantity > 0:
            notional = execution_price * quantity
            fees = sum(self._fees(Side.BUY, notional, trading_date))
            if notional + fees <= cash + 1e-9:
                return quantity
            quantity = rule.round_down(quantity - rule.step)
        return 0

    def _volume_capacity(self, previous_volume: float | None) -> int | None:
        """把上一根 K 线成交量参与率转换成整手容量。"""
        if self.config.volume_limit is None:
            return None
        if previous_volume is None or not math.isfinite(previous_volume):
            return 0
        return max(0, math.floor(previous_volume * self.config.volume_limit))


def _valid_price(value: float | None) -> bool:
    """价格必须存在、有限且大于零。"""
    return value is not None and math.isfinite(value) and value > 0


def _known_price_limits(status: MarketStatus) -> bool:
    """无限制必须由来源明确声明；空值或矛盾的价格边界均不能放行。"""
    if status.price_limit_status == "unlimited":
        return status.up_limit is None and status.down_limit is None
    if status.price_limit_status == "limited":
        return (
            status.up_limit is not None
            and status.down_limit is not None
            and _valid_price(status.up_limit)
            and _valid_price(status.down_limit)
            and status.down_limit <= status.up_limit
        )
    return False


def _reaches_limit(price: float, limit: float | None, *, buy: bool) -> bool:
    """判断买入是否涨停或卖出是否跌停。"""
    if not _valid_price(limit):
        return False
    assert limit is not None
    return price >= limit - 1e-9 if buy else price <= limit + 1e-9
