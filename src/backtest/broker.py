"""确定性的开盘订单状态机、A 股约束、费用与滑点。"""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal

from backtest.config import ExecutionConfig, FeeConfig
from backtest.portfolio import Portfolio
from backtest.types import (
    DailyBar,
    Fill,
    MarketStatus,
    Order,
    OrderEvent,
    OrderReason,
    OrderSide,
    OrderStatus,
)


def _round_money(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _round_price(value: float, tick: float) -> float:
    steps = Decimal(str(value)) / Decimal(str(tick))
    rounded = steps.quantize(Decimal("1"), rounding=ROUND_HALF_UP) * Decimal(str(tick))
    return float(rounded)


class FeeModel:
    """按成交日选择印花税与过户费政策。"""

    def __init__(self, config: FeeConfig) -> None:
        self.config = config

    def calculate(self, *, side: OrderSide, notional: float, session: date) -> tuple[float, ...]:
        commission = _round_money(
            max(self.config.minimum_commission, notional * self.config.commission_rate)
        )
        stamp_rate = (
            self.config.stamp_tax_rate_from_2023_08_28
            if session >= date(2023, 8, 28)
            else self.config.stamp_tax_rate_before_2023_08_28
        )
        stamp_tax = _round_money(notional * stamp_rate) if side is OrderSide.SELL else 0.0
        transfer_rate = (
            self.config.transfer_fee_rate_from_2022_04_29
            if session >= date(2022, 4, 29)
            else self.config.transfer_fee_rate_before_2022_04_29
        )
        transfer_fee = _round_money(notional * transfer_rate)
        return commission, stamp_tax, transfer_fee


class SimBroker:
    """只在 MARKET_OPEN 事件处理 DAY 市价单。"""

    def __init__(
        self,
        *,
        run_id: str,
        strategy_id: str,
        execution: ExecutionConfig,
        fees: FeeConfig,
    ) -> None:
        self.run_id = run_id
        self.strategy_id = strategy_id
        self.execution = execution
        self.fee_model = FeeModel(fees)
        self.orders: list[Order] = []
        self.order_events: list[OrderEvent] = []
        self.fills: list[Fill] = []
        self._next_order_id = 1
        self._next_fill_id = 1

    @property
    def open_orders(self) -> tuple[Order, ...]:
        return tuple(order for order in self.orders if order.status is OrderStatus.ACCEPTED)

    def submit_market_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        quantity: int,
        submitted_at: datetime,
        earliest_fill_at: datetime,
        portfolio: Portfolio,
        target_weight: float | None = None,
    ) -> Order:
        order = Order(
            order_id=f"O{self._next_order_id:08d}",
            run_id=self.run_id,
            strategy_id=self.strategy_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            submitted_at=submitted_at,
            earliest_fill_at=earliest_fill_at,
            target_weight=target_weight,
        )
        self._next_order_id += 1
        self.orders.append(order)
        self._transition(order, OrderStatus.NEW, submitted_at)

        if quantity <= 0:
            self._transition(
                order, OrderStatus.REJECTED, submitted_at, OrderReason.INVALID_QUANTITY
            )
            return order
        if side is OrderSide.BUY and quantity % self.execution.lot_size:
            self._transition(
                order, OrderStatus.REJECTED, submitted_at, OrderReason.INVALID_QUANTITY
            )
            return order
        if side is OrderSide.SELL:
            position = portfolio.position(symbol)
            is_full_exit = quantity == position.total_quantity
            if quantity % self.execution.lot_size and not (
                self.execution.allow_odd_lot_full_exit and is_full_exit
            ):
                self._transition(
                    order, OrderStatus.REJECTED, submitted_at, OrderReason.INVALID_QUANTITY
                )
                return order
            if not portfolio.freeze_position(symbol, quantity):
                self._transition(
                    order,
                    OrderStatus.REJECTED,
                    submitted_at,
                    OrderReason.INSUFFICIENT_SELLABLE,
                )
                return order
        self._transition(order, OrderStatus.ACCEPTED, submitted_at)
        return order

    def cancel(self, order_id: str, event_time: datetime, portfolio: Portfolio) -> None:
        order = next((item for item in self.orders if item.order_id == order_id), None)
        if order is None or order.status is not OrderStatus.ACCEPTED:
            return
        self._release_remaining(order, portfolio)
        self._transition(
            order, OrderStatus.CANCELLED, event_time, OrderReason.CANCELLED_BY_STRATEGY
        )

    def cancel_symbol_for_corporate_action(
        self, symbol: str, event_time: datetime, portfolio: Portfolio
    ) -> None:
        self.cancel_symbol(
            symbol,
            event_time=event_time,
            portfolio=portfolio,
            reason=OrderReason.CORPORATE_ACTION,
        )

    def cancel_symbol(
        self,
        symbol: str,
        *,
        event_time: datetime,
        portfolio: Portfolio,
        reason: OrderReason,
    ) -> None:
        for order in self.open_orders:
            if order.symbol != symbol:
                continue
            self._release_remaining(order, portfolio)
            self._transition(order, OrderStatus.CANCELLED, event_time, reason)

    def expire_all(self, event_time: datetime, portfolio: Portfolio) -> None:
        """回测区间结束时关闭仍未到撮合事件的 DAY 订单。"""

        for order in self.open_orders:
            self._release_remaining(order, portfolio)
            self._transition(order, OrderStatus.EXPIRED, event_time, OrderReason.MISSING_OPEN)

    def match_open(
        self,
        *,
        event_time: datetime,
        bars: Mapping[str, DailyBar],
        statuses: Mapping[str, MarketStatus],
        previous_volumes: Mapping[str, float | None],
        portfolio: Portfolio,
    ) -> list[Fill]:
        eligible = [
            order
            for order in self.open_orders
            if order.earliest_fill_at <= event_time
        ]
        before = len(self.fills)
        sells = sorted(
            (item for item in eligible if item.side is OrderSide.SELL),
            key=lambda item: (item.symbol, item.order_id),
        )
        buys = sorted(
            (item for item in eligible if item.side is OrderSide.BUY),
            key=lambda item: (item.symbol, item.order_id),
        )
        for order in sells:
            validated = self._validated_quantity(
                order, bars, statuses, previous_volumes, event_time, portfolio
            )
            if validated is not None:
                quantity, market_price, execution_price = validated
                self._fill(
                    order,
                    quantity=quantity,
                    market_price=market_price,
                    execution_price=execution_price,
                    event_time=event_time,
                    portfolio=portfolio,
                )
            self._expire_remainder(order, event_time, portfolio)

        candidates: list[tuple[Order, int, float, float]] = []
        for order in buys:
            validated = self._validated_quantity(
                order, bars, statuses, previous_volumes, event_time, portfolio
            )
            if validated is not None:
                candidates.append((order, *validated))

        requested_cost = sum(
            self._total_buy_cost(quantity, execution_price, event_time.date())
            for _, quantity, _, execution_price in candidates
        )
        scale = min(portfolio.cash / requested_cost, 1.0) if requested_cost > 0 else 0.0
        allocations: list[tuple[Order, int, float, float]] = []
        for order, quantity, market_price, execution_price in candidates:
            allocated = quantity
            if scale < 1.0:
                allocated = math.floor(quantity * scale / self.execution.lot_size)
                allocated *= self.execution.lot_size
            if allocated > 0:
                allocations.append((order, allocated, market_price, execution_price))

        # 最低佣金会使线性比例略有误差；统一缩小一次，绝不按代码顺序争抢尾款。
        allocation_cost = sum(
            self._total_buy_cost(quantity, price, event_time.date())
            for _, quantity, _, price in allocations
        )
        if allocation_cost > portfolio.cash and allocations:
            second_scale = portfolio.cash / allocation_cost
            allocations = [
                (order, math.floor(quantity * second_scale / self.execution.lot_size)
                 * self.execution.lot_size, market_price, execution_price)
                for order, quantity, market_price, execution_price in allocations
            ]
            allocations = [item for item in allocations if item[1] > 0]

        allocated_by_id = {order.order_id: quantity for order, quantity, _, _ in allocations}
        for order, quantity, market_price, execution_price in allocations:
            cost = self._total_buy_cost(quantity, execution_price, event_time.date())
            if not portfolio.freeze_cash_for_immediate_fill(cost):
                # 理论上只可能由浮点舍入触发；明确记录而不是让账户透支。
                order.reason = OrderReason.INSUFFICIENT_CASH
                continue
            order.frozen_cash = cost
            self._fill(
                order,
                quantity=quantity,
                market_price=market_price,
                execution_price=execution_price,
                event_time=event_time,
                portfolio=portfolio,
            )
        for order in buys:
            if (
                order.status is OrderStatus.ACCEPTED
                and allocated_by_id.get(order.order_id, 0) == 0
                and order.reason is OrderReason.NONE
            ):
                order.reason = OrderReason.INSUFFICIENT_CASH
            self._expire_remainder(order, event_time, portfolio)
        return self.fills[before:]

    def _validated_quantity(
        self,
        order: Order,
        bars: Mapping[str, DailyBar],
        statuses: Mapping[str, MarketStatus],
        previous_volumes: Mapping[str, float | None],
        event_time: datetime,
        portfolio: Portfolio,
    ) -> tuple[int, float, float] | None:
        bar = bars.get(order.symbol)
        status = statuses.get(
            order.symbol,
            MarketStatus(order.symbol, None, None, None, None),
        )
        if status.suspended is True:
            order.reason = OrderReason.SUSPENDED
            return None
        if bar is None or bar.open is None or not math.isfinite(bar.open) or bar.open <= 0:
            order.reason = OrderReason.MISSING_OPEN
            return None
        if self.execution.strict_price_limits and (
            status.up_limit is None or status.down_limit is None
        ):
            order.reason = OrderReason.MISSING_PRICE_LIMIT
            return None
        tolerance = self.execution.price_tick / 2
        if (
            order.side is OrderSide.BUY
            and status.up_limit is not None
            and bar.open >= status.up_limit - tolerance
        ):
            order.reason = OrderReason.LIMIT_UP
            return None
        if (
            order.side is OrderSide.SELL
            and status.down_limit is not None
            and bar.open <= status.down_limit + tolerance
        ):
            order.reason = OrderReason.LIMIT_DOWN
            return None

        quantity = order.remaining_quantity
        participation = self.execution.max_previous_volume_participation
        if participation is not None:
            previous_volume = previous_volumes.get(order.symbol)
            if (
                previous_volume is None
                or not math.isfinite(previous_volume)
                or previous_volume <= 0
            ):
                order.reason = OrderReason.CAPACITY
                return None
            capacity = math.floor(
                previous_volume * participation / self.execution.lot_size
            ) * self.execution.lot_size
            if order.side is OrderSide.SELL and quantity < self.execution.lot_size:
                capacity = math.floor(previous_volume * participation)
            quantity = min(quantity, capacity)
        if quantity <= 0:
            order.reason = OrderReason.CAPACITY
            return None
        if order.side is OrderSide.SELL:
            position = portfolio.position(order.symbol)
            quantity = min(quantity, position.frozen_quantity)
        if quantity <= 0:
            order.reason = OrderReason.INSUFFICIENT_SELLABLE
            return None

        direction = 1.0 if order.side is OrderSide.BUY else -1.0
        execution_price = _round_price(
            bar.open * (1 + direction * self.execution.slippage_bps / 10_000),
            self.execution.price_tick,
        )
        if status.up_limit is not None:
            execution_price = min(execution_price, status.up_limit)
        if status.down_limit is not None:
            execution_price = max(execution_price, status.down_limit)
        return quantity, bar.open, execution_price

    def _total_buy_cost(self, quantity: int, price: float, session: date) -> float:
        notional = _round_money(quantity * price)
        commission, stamp_tax, transfer_fee = self.fee_model.calculate(
            side=OrderSide.BUY, notional=notional, session=session
        )
        return notional + commission + stamp_tax + transfer_fee

    def _fill(
        self,
        order: Order,
        *,
        quantity: int,
        market_price: float,
        execution_price: float,
        event_time: datetime,
        portfolio: Portfolio,
    ) -> None:
        notional = _round_money(quantity * execution_price)
        commission, stamp_tax, transfer_fee = self.fee_model.calculate(
            side=order.side, notional=notional, session=event_time.date()
        )
        fill = Fill(
            fill_id=f"F{self._next_fill_id:08d}",
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            filled_at=event_time,
            quantity=quantity,
            market_price=market_price,
            execution_price=execution_price,
            notional=notional,
            commission=commission,
            stamp_tax=stamp_tax,
            transfer_fee=transfer_fee,
            slippage_cost=_round_money(abs(execution_price - market_price) * quantity),
        )
        self._next_fill_id += 1
        portfolio.apply_fill(fill)
        order.filled_quantity += quantity
        order.frozen_cash = 0.0
        status = (
            OrderStatus.FILLED
            if order.remaining_quantity == 0
            else OrderStatus.PARTIALLY_FILLED
        )
        self.fills.append(fill)
        self._transition(order, status, event_time)

    def _expire_remainder(
        self, order: Order, event_time: datetime, portfolio: Portfolio
    ) -> None:
        if order.remaining_quantity <= 0 or order.status not in {
            OrderStatus.ACCEPTED,
            OrderStatus.PARTIALLY_FILLED,
        }:
            return
        self._release_remaining(order, portfolio)
        reason = order.reason
        if reason is OrderReason.NONE:
            reason = OrderReason.CAPACITY
        self._transition(order, OrderStatus.EXPIRED, event_time, reason)

    @staticmethod
    def _release_remaining(order: Order, portfolio: Portfolio) -> None:
        if order.side is OrderSide.SELL:
            remaining = min(
                order.remaining_quantity,
                portfolio.position(order.symbol).frozen_quantity,
            )
            if remaining:
                portfolio.release_position(order.symbol, remaining)

    def _transition(
        self,
        order: Order,
        status: OrderStatus,
        event_time: datetime,
        reason: OrderReason = OrderReason.NONE,
    ) -> None:
        order.status = status
        if reason is not OrderReason.NONE:
            order.reason = reason
        self.order_events.append(
            OrderEvent(
                order_id=order.order_id,
                event_time=event_time,
                status=status,
                filled_quantity=order.filled_quantity,
                remaining_quantity=order.remaining_quantity,
                reason=order.reason,
            )
        )
