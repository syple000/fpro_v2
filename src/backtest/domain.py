"""跨模块共享的不可变业务对象。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum


class Side(StrEnum):
    """订单方向。"""

    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(StrEnum):
    """一次订单从提交到终态可能产生的状态。"""

    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    NOT_FILLED = "NOT_FILLED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"


class OrderReason(StrEnum):
    """订单未完全成交、取消或过期的机器可读原因。"""

    NONE = "NONE"
    INVALID_QUANTITY = "INVALID_QUANTITY"
    INSUFFICIENT_CASH = "INSUFFICIENT_CASH"
    INSUFFICIENT_SELLABLE = "INSUFFICIENT_SELLABLE"
    MISSING_OPEN = "MISSING_OPEN"
    SUSPENDED = "SUSPENDED"
    UNKNOWN_MARKET_STATUS = "UNKNOWN_MARKET_STATUS"
    LIMIT_UP = "LIMIT_UP"
    LIMIT_DOWN = "LIMIT_DOWN"
    VOLUME_LIMIT = "VOLUME_LIMIT"
    CORPORATE_ACTION = "CORPORATE_ACTION"
    DELISTED = "DELISTED"
    END_OF_BACKTEST = "END_OF_BACKTEST"
    STRATEGY = "STRATEGY"


@dataclass(frozen=True, slots=True)
class Bar:
    """撮合和估值实际需要的一根 K 线字段。"""

    symbol: str
    interval_start: datetime
    open: float | None
    close: float | None


@dataclass(frozen=True, slots=True)
class MarketStatus:
    """某时刻撮合所需的停牌和涨跌停状态。"""

    symbol: str
    suspended: bool | None = None
    up_limit: float | None = None
    down_limit: float | None = None
    price_limit_status: str = "unknown"


@dataclass(frozen=True, slots=True)
class OrderRequest:
    """数量订单；目标权重仅在组合调仓时填写。"""

    symbol: str
    side: Side
    quantity: int
    target_weight: float | None = None
    sid: int | None = None


@dataclass(frozen=True, slots=True)
class Order:
    """券商接收后生成的不可变订单。"""

    order_id: str
    symbol: str
    side: Side
    quantity: int
    submitted_at: datetime
    target_weight: float | None = None
    sid: int | None = None


@dataclass(frozen=True, slots=True)
class OrderUpdate:
    """订单生命周期中的一次状态通知。"""

    order: Order
    status: OrderStatus
    updated_at: datetime
    filled_quantity: int = 0
    reason: OrderReason = OrderReason.NONE

    @property
    def remaining_quantity(self) -> int:
        """订单原始数量减去本次累计成交数量。"""
        return self.order.quantity - self.filled_quantity


@dataclass(frozen=True, slots=True)
class Fill:
    """一次实际成交及其完整成本明细。"""

    fill_id: str
    order_id: str
    symbol: str
    side: Side
    filled_at: datetime
    quantity: int
    market_price: float
    execution_price: float
    notional: float
    commission: float
    stamp_tax: float
    transfer_fee: float
    slippage_cost: float
    sid: int | None = None

    @property
    def total_fee(self) -> float:
        """不含滑点的显式交易费用合计。"""
        return self.commission + self.stamp_tax + self.transfer_fee


@dataclass(frozen=True, slots=True)
class Holding:
    """策略可见的单个持仓快照。"""

    symbol: str
    quantity: int
    sellable_quantity: int
    market_value: float
    sid: int | None = None


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    """传给策略的只读账户视图，避免策略直接改账。"""

    cash: float
    dividend_receivable: float
    market_value: float
    total_equity: float
    holdings: tuple[Holding, ...]

    def holding(self, symbol: str | int) -> Holding | None:
        """按证券代码读取持仓；没有持仓时返回 None。"""
        return next((holding for holding in self.holdings
                     if holding.symbol == symbol or holding.sid == symbol), None)


@dataclass(frozen=True, slots=True)
class EquitySnapshot:
    """每个交易日结束时记录的账户净值。"""

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
    """一次现金分红或送转事件及其关键业务日期。"""

    action_id: str
    symbol: str
    record_date: date | None
    ex_date: date | None
    pay_date: date | None
    listing_date: date | None
    cash_dividend: float | None
    cash_dividend_before_tax: float | None
    stock_dividend: float
    sid: int | None = None


@dataclass(frozen=True, slots=True)
class MarketDataCoverage:
    """请求范围内实际回放到的行情数量与未覆盖范围。"""

    expected_events: int
    events_with_bars: int
    bar_count: int
    symbols_with_bars: tuple[str, ...]
    sessions_without_bars: tuple[date, ...]
    requested_symbols_without_bars: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BacktestResult:
    """一次回测的订单、成交和每日净值结果。"""

    sessions: tuple[date, ...]
    orders: tuple[Order, ...]
    order_updates: tuple[OrderUpdate, ...]
    fills: tuple[Fill, ...]
    equity: tuple[EquitySnapshot, ...]
    market_data_coverage: MarketDataCoverage | None = None
