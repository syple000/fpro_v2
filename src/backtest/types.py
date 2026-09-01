"""回测内核共享的固定数据模型。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(StrEnum):
    """一次开盘撮合后的最终结果，不表达瞬时状态。"""

    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    NOT_FILLED = "NOT_FILLED"


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
    END_OF_BACKTEST = "END_OF_BACKTEST"


@dataclass(frozen=True, slots=True)
class DailyBar:
    symbol: str
    session: date
    open: float | None
    close: float | None
    pre_close: float | None
    volume: float | None


@dataclass(frozen=True, slots=True)
class MarketStatus:
    symbol: str
    suspended: bool | None
    up_limit: float | None
    down_limit: float | None
    st_type: str | None


@dataclass(frozen=True, slots=True)
class Order:
    """等待下一个开盘尝试一次的订单意图。"""

    order_id: str
    symbol: str
    side: OrderSide
    quantity: int
    submitted_at: datetime
    earliest_fill_at: datetime
    target_weight: float | None = None


@dataclass(frozen=True, slots=True)
class OrderResult:
    """订单经过一次开盘撮合后的不可变最终结果。"""

    order: Order
    filled_quantity: int
    reason: OrderReason = OrderReason.NONE

    @property
    def remaining_quantity(self) -> int:
        return self.order.quantity - self.filled_quantity

    @property
    def status(self) -> OrderStatus:
        if self.filled_quantity == self.order.quantity:
            return OrderStatus.FILLED
        if self.filled_quantity > 0:
            return OrderStatus.PARTIALLY_FILLED
        return OrderStatus.NOT_FILLED


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
    average_cost: float = 0.0
    last_price: float | None = None
    stale_price: bool = False

    @property
    def market_value(self) -> float:
        if self.last_price is None:
            return 0.0
        return self.total_quantity * self.last_price


@dataclass(frozen=True, slots=True)
class EquitySnapshot:
    session: date
    cash: float
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
