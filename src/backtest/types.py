"""回测内核共享的少量固定模型。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum


class EventType(StrEnum):
    PRE_OPEN = "PRE_OPEN"
    MARKET_OPEN = "MARKET_OPEN"
    DAILY_READY = "DAILY_READY"
    DAILY_METRICS_READY = "DAILY_METRICS_READY"


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    MARKET = "MARKET"


class TimeInForce(StrEnum):
    DAY = "DAY"


class OrderStatus(StrEnum):
    NEW = "NEW"
    ACCEPTED = "ACCEPTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class OrderReason(StrEnum):
    NONE = "NONE"
    INVALID_QUANTITY = "INVALID_QUANTITY"
    INSUFFICIENT_CASH = "INSUFFICIENT_CASH"
    INSUFFICIENT_SELLABLE = "INSUFFICIENT_SELLABLE"
    MISSING_OPEN = "MISSING_OPEN"
    MISSING_PRICE_LIMIT = "MISSING_PRICE_LIMIT"
    SUSPENDED = "SUSPENDED"
    LIMIT_UP = "LIMIT_UP"
    LIMIT_DOWN = "LIMIT_DOWN"
    CAPACITY = "CAPACITY"
    CORPORATE_ACTION = "CORPORATE_ACTION"
    DELISTED = "DELISTED"
    CANCELLED_BY_STRATEGY = "CANCELLED_BY_STRATEGY"


@dataclass(frozen=True, slots=True)
class DailyBar:
    symbol: str
    session: date
    open: float | None
    close: float | None
    pre_close: float | None
    volume: float | None
    amount: float | None


@dataclass(frozen=True, slots=True)
class MarketStatus:
    symbol: str
    suspended: bool | None
    up_limit: float | None
    down_limit: float | None
    st_type: str | None


@dataclass(slots=True)
class Order:
    order_id: str
    run_id: str
    strategy_id: str
    symbol: str
    side: OrderSide
    quantity: int
    submitted_at: datetime
    earliest_fill_at: datetime
    order_type: OrderType = OrderType.MARKET
    time_in_force: TimeInForce = TimeInForce.DAY
    target_weight: float | None = None
    filled_quantity: int = 0
    status: OrderStatus = OrderStatus.NEW
    reason: OrderReason = OrderReason.NONE
    frozen_cash: float = 0.0

    @property
    def remaining_quantity(self) -> int:
        return self.quantity - self.filled_quantity


@dataclass(frozen=True, slots=True)
class OrderEvent:
    order_id: str
    event_time: datetime
    status: OrderStatus
    filled_quantity: int
    remaining_quantity: int
    reason: OrderReason


@dataclass(frozen=True, slots=True)
class Fill:
    fill_id: str
    order_id: str
    symbol: str
    side: OrderSide
    filled_at: datetime
    quantity: int
    market_price: float
    execution_price: float
    notional: float
    commission: float
    stamp_tax: float
    transfer_fee: float
    slippage_cost: float

    @property
    def total_fee(self) -> float:
        return self.commission + self.stamp_tax + self.transfer_fee


@dataclass(slots=True)
class Position:
    symbol: str
    total_quantity: int = 0
    sellable_quantity: int = 0
    pending_listing_quantity: int = 0
    frozen_quantity: int = 0
    average_cost: float = 0.0
    last_price: float | None = None
    realized_pnl: float = 0.0
    stale_price: bool = False
    opened_on: date | None = None

    @property
    def available_to_sell(self) -> int:
        return max(self.sellable_quantity - self.frozen_quantity, 0)

    @property
    def market_value(self) -> float:
        if self.last_price is None:
            return 0.0
        return self.total_quantity * self.last_price

    @property
    def unrealized_pnl(self) -> float:
        if self.last_price is None:
            return 0.0
        return (self.last_price - self.average_cost) * self.total_quantity


@dataclass(frozen=True, slots=True)
class EquitySnapshot:
    session: date
    cash: float
    frozen_cash: float
    dividend_receivable: float
    market_value: float
    total_equity: float
    daily_return: float | None
    holding_count: int
    stale_position_count: int


@dataclass(frozen=True, slots=True)
class CorporateAction:
    action_id: str
    symbol: str
    visible_at: datetime
    record_date: date | None
    ex_date: date | None
    pay_date: date | None
    listing_date: date | None
    cash_dividend: float | None
    cash_dividend_before_tax: float | None
    stock_dividend: float


@dataclass(frozen=True, slots=True)
class CorporateActionEvent:
    event_time: datetime
    action_id: str
    symbol: str
    event_type: str
    quantity: int
    amount: float
    note: str
