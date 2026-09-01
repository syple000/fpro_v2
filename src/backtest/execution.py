"""确定性的次日开盘执行计算；不直接修改账户。"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal

from backtest.config import BacktestConfig
from backtest.types import (
    DailyBar,
    Fill,
    MarketStatus,
    Order,
    OrderReason,
    OrderResult,
    OrderSide,
)


def _round_money(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _round_price(value: float, tick: float) -> float:
    steps = Decimal(str(value)) / Decimal(str(tick))
    rounded = steps.quantize(Decimal("1"), rounding=ROUND_HALF_UP) * Decimal(str(tick))
    return float(rounded)


LOT_SIZE = 100
PRICE_TICK = 0.01


class FeeModel:
    """按成交日选择印花税与过户费政策。"""

    def __init__(self, commission_rate: float = 0.0003, minimum_commission: float = 5.0) -> None:
        self.commission_rate = commission_rate
        self.minimum_commission = minimum_commission

    def calculate(self, *, side: OrderSide, notional: float, session: date) -> tuple[float, ...]:
        commission = _round_money(max(self.minimum_commission, notional * self.commission_rate))
        stamp_rate = 0.0005 if session >= date(2023, 8, 28) else 0.001
        stamp_tax = _round_money(notional * stamp_rate) if side is OrderSide.SELL else 0.0
        transfer_rate = 0.00001 if session >= date(2022, 4, 29) else 0.00002
        transfer_fee = _round_money(notional * transfer_rate)
        return commission, stamp_tax, transfer_fee


@dataclass(frozen=True, slots=True)
class _Decision:
    quantity: int
    market_price: float
    execution_price: float
    partial_reason: OrderReason = OrderReason.NONE


@dataclass(slots=True)
class _OpenInputs:
    bars: Mapping[str, DailyBar]
    statuses: Mapping[str, MarketStatus]
    previous_volumes: Mapping[str, float | None]
    total_quantities: Mapping[str, int]
    sellable_quantities: dict[str, int]


class ExecutionEngine:
    """订单在目标开盘尝试一次，输出结果和成交，不持有账户引用。"""

    def __init__(self, config: BacktestConfig) -> None:
        self.config = config
        self.fee_model = FeeModel(config.commission_rate, config.minimum_commission)
        self.pending_orders: list[Order] = []
        self.results: list[OrderResult] = []
        self.fills: list[Fill] = []
        self._next_order_id = 1
        self._next_fill_id = 1

    @property
    def pending_symbols(self) -> tuple[str, ...]:
        return tuple(sorted({order.symbol for order in self.pending_orders}))

    def submit_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        quantity: int,
        submitted_at: datetime,
        earliest_fill_at: datetime,
        target_weight: float | None = None,
    ) -> Order:
        order = Order(
            order_id=f"O{self._next_order_id:08d}",
            symbol=symbol,
            side=side,
            quantity=quantity,
            submitted_at=submitted_at,
            earliest_fill_at=earliest_fill_at,
            target_weight=target_weight,
        )
        self._next_order_id += 1
        self.pending_orders.append(order)
        return order

    def cancel_symbol(
        self,
        symbol: str,
        *,
        reason: OrderReason,
    ) -> None:
        retained: list[Order] = []
        for order in self.pending_orders:
            if order.symbol == symbol:
                self.results.append(OrderResult(order=order, filled_quantity=0, reason=reason))
            else:
                retained.append(order)
        self.pending_orders = retained

    def expire_all(self) -> None:
        for order in self.pending_orders:
            self.results.append(
                OrderResult(
                    order=order,
                    filled_quantity=0,
                    reason=OrderReason.END_OF_BACKTEST,
                )
            )
        self.pending_orders.clear()

    def execute_open(
        self,
        *,
        event_time: datetime,
        bars: Mapping[str, DailyBar],
        statuses: Mapping[str, MarketStatus],
        previous_volumes: Mapping[str, float | None],
        cash: float,
        total_quantities: Mapping[str, int],
        sellable_quantities: Mapping[str, int],
    ) -> list[Fill]:
        eligible = sorted(
            (order for order in self.pending_orders if order.earliest_fill_at <= event_time),
            key=lambda order: (order.side is OrderSide.BUY, order.symbol, order.order_id),
        )
        if not eligible:
            return []
        eligible_ids = {order.order_id for order in eligible}
        self.pending_orders = [
            order for order in self.pending_orders if order.order_id not in eligible_ids
        ]

        inputs = _OpenInputs(
            bars=bars,
            statuses=statuses,
            previous_volumes=previous_volumes,
            total_quantities=total_quantities,
            sellable_quantities=dict(sellable_quantities),
        )
        sells = [order for order in eligible if order.side is OrderSide.SELL]
        buys = [order for order in eligible if order.side is OrderSide.BUY]
        before = len(self.fills)
        available_cash = self._execute_sells(sells, inputs, event_time, cash)
        self._execute_buys(buys, inputs, event_time, available_cash)
        return self.fills[before:]

    def _execute_sells(
        self,
        orders: list[Order],
        inputs: _OpenInputs,
        event_time: datetime,
        cash: float,
    ) -> float:
        for order in orders:
            decision = self._decision(order, inputs)
            if isinstance(decision, OrderReason):
                self._reject(order, decision)
                continue
            fill = self._make_fill(order, decision.quantity, decision, event_time)
            inputs.sellable_quantities[order.symbol] -= fill.quantity
            cash += fill.notional - fill.total_fee
            self.fills.append(fill)
            self.results.append(OrderResult(order, fill.quantity, decision.partial_reason))
        return cash

    def _execute_buys(
        self,
        orders: list[Order],
        inputs: _OpenInputs,
        event_time: datetime,
        cash: float,
    ) -> None:
        for order in orders:
            decision = self._decision(order, inputs)
            if isinstance(decision, OrderReason):
                self._reject(order, decision)
                continue
            quantity = self._affordable_quantity(
                decision.quantity,
                decision.execution_price,
                cash,
                event_time.date(),
            )
            if quantity <= 0:
                self._reject(order, OrderReason.INSUFFICIENT_CASH)
                continue
            fill = self._make_fill(order, quantity, decision, event_time)
            cash -= fill.notional + fill.total_fee
            self.fills.append(fill)
            reason = decision.partial_reason
            if quantity < decision.quantity:
                reason = OrderReason.INSUFFICIENT_CASH
            self.results.append(OrderResult(order=order, filled_quantity=quantity, reason=reason))

    def _reject(self, order: Order, reason: OrderReason) -> None:
        self.results.append(OrderResult(order=order, filled_quantity=0, reason=reason))

    def _decision(
        self,
        order: Order,
        inputs: _OpenInputs,
    ) -> _Decision | OrderReason:
        quantity_reason = self._validated_order_quantity(
            order,
            inputs.total_quantities.get(order.symbol, 0),
        )
        if quantity_reason is not None:
            return quantity_reason
        status = inputs.statuses.get(
            order.symbol,
            MarketStatus(order.symbol, None, None, None, None),
        )
        market_price = self._market_price(order, inputs.bars.get(order.symbol), status)
        if isinstance(market_price, OrderReason):
            return market_price
        quantity, partial_reason = self._capacity_quantity(
            order,
            inputs.previous_volumes.get(order.symbol),
        )
        sellable = inputs.sellable_quantities.get(order.symbol, 0)
        if order.side is OrderSide.SELL and sellable < quantity:
            quantity = sellable
            partial_reason = OrderReason.INSUFFICIENT_SELLABLE
        if quantity <= 0:
            return partial_reason
        execution_price = self._slipped_price(order.side, market_price, status)
        return _Decision(quantity, market_price, execution_price, partial_reason)

    def _capacity_quantity(
        self,
        order: Order,
        previous_volume: float | None,
    ) -> tuple[int, OrderReason]:
        participation = self.config.max_volume_fraction
        if participation is None:
            return order.quantity, OrderReason.NONE
        if previous_volume is None or not math.isfinite(previous_volume) or previous_volume <= 0:
            return 0, OrderReason.CAPACITY
        capacity = math.floor(previous_volume * participation / LOT_SIZE) * LOT_SIZE
        if order.side is OrderSide.SELL and order.quantity < LOT_SIZE:
            capacity = math.floor(previous_volume * participation)
        quantity = min(order.quantity, capacity)
        reason = OrderReason.CAPACITY if quantity < order.quantity else OrderReason.NONE
        return quantity, reason

    def _validated_order_quantity(
        self,
        order: Order,
        total_quantity: int,
    ) -> OrderReason | None:
        if order.quantity <= 0:
            return OrderReason.INVALID_QUANTITY
        if order.side is OrderSide.BUY and order.quantity % LOT_SIZE:
            return OrderReason.INVALID_QUANTITY
        is_odd_sell = order.side is OrderSide.SELL and order.quantity % LOT_SIZE
        is_full_exit = order.quantity == total_quantity
        if is_odd_sell and not is_full_exit:
            return OrderReason.INVALID_QUANTITY
        return None

    def _market_price(
        self,
        order: Order,
        bar: DailyBar | None,
        status: MarketStatus,
    ) -> float | OrderReason:
        if status.suspended is True:
            return OrderReason.SUSPENDED
        if bar is None or bar.open is None or not math.isfinite(bar.open) or bar.open <= 0:
            return OrderReason.MISSING_OPEN
        if status.up_limit is None or status.down_limit is None:
            return OrderReason.MISSING_PRICE_LIMIT
        tolerance = PRICE_TICK / 2
        if (
            order.side is OrderSide.BUY
            and status.up_limit is not None
            and bar.open >= status.up_limit - tolerance
        ):
            return OrderReason.LIMIT_UP
        if (
            order.side is OrderSide.SELL
            and status.down_limit is not None
            and bar.open <= status.down_limit + tolerance
        ):
            return OrderReason.LIMIT_DOWN
        return bar.open

    def _slipped_price(
        self,
        side: OrderSide,
        market_price: float,
        status: MarketStatus,
    ) -> float:
        direction = 1.0 if side is OrderSide.BUY else -1.0
        execution_price = _round_price(
            market_price * (1 + direction * self.config.slippage_bps / 10_000),
            PRICE_TICK,
        )
        if status.up_limit is not None:
            execution_price = min(execution_price, status.up_limit)
        if status.down_limit is not None:
            execution_price = max(execution_price, status.down_limit)
        return execution_price

    def _affordable_quantity(
        self,
        maximum: int,
        price: float,
        cash: float,
        session: date,
    ) -> int:
        transfer_rate = 0.00001 if session >= date(2022, 4, 29) else 0.00002
        estimated_per_share = price * (1 + self.config.commission_rate + transfer_rate)
        quantity = min(maximum, math.floor(cash / estimated_per_share / LOT_SIZE) * LOT_SIZE)
        while quantity > 0 and self._total_buy_cost(quantity, price, session) > cash + 1e-6:
            quantity -= LOT_SIZE
        return quantity

    def _total_buy_cost(self, quantity: int, price: float, session: date) -> float:
        notional = _round_money(quantity * price)
        commission, stamp_tax, transfer_fee = self.fee_model.calculate(
            side=OrderSide.BUY,
            notional=notional,
            session=session,
        )
        return notional + commission + stamp_tax + transfer_fee

    def _make_fill(
        self,
        order: Order,
        quantity: int,
        decision: _Decision,
        event_time: datetime,
    ) -> Fill:
        notional = _round_money(quantity * decision.execution_price)
        commission, stamp_tax, transfer_fee = self.fee_model.calculate(
            side=order.side,
            notional=notional,
            session=event_time.date(),
        )
        fill = Fill(
            fill_id=f"F{self._next_fill_id:08d}",
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            filled_at=event_time,
            quantity=quantity,
            market_price=decision.market_price,
            execution_price=decision.execution_price,
            notional=notional,
            commission=commission,
            stamp_tax=stamp_tax,
            transfer_fee=transfer_fee,
            slippage_cost=_round_money(
                abs(decision.execution_price - decision.market_price) * quantity
            ),
        )
        self._next_fill_id += 1
        return fill
