"""现金账户、持仓、T+1 和公司行动记账。"""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date

from backtest.errors import AccountInvariantError, BacktestDataError
from backtest.types import EquitySnapshot, Fill, OrderSide, Position

_EPSILON = 1e-6


class Portfolio:
    """只通过明确业务方法变化的现金账户。"""

    def __init__(self, initial_cash: float) -> None:
        if not math.isfinite(initial_cash) or initial_cash <= 0:
            raise ValueError("initial_cash 必须是有限正数")
        self.cash = float(initial_cash)
        self.positions: dict[str, Position] = {}
        self._receivables: dict[str, float] = {}
        self._entitlements: dict[str, int] = {}
        self._pending_stock: dict[str, tuple[str, int]] = {}
        self._last_equity: float | None = None

    @property
    def dividend_receivable(self) -> float:
        return sum(self._receivables.values())

    @property
    def market_value(self) -> float:
        return sum(position.market_value for position in self.positions.values())

    @property
    def total_equity(self) -> float:
        return self.cash + self.dividend_receivable + self.market_value

    def position(self, symbol: str) -> Position:
        return self.positions.setdefault(symbol, Position(symbol=symbol))

    def unlock_t1(self) -> None:
        """盘前把此前买入、且不是待上市股份的数量变为可卖。"""

        for position in self.positions.values():
            position.sellable_quantity = position.total_quantity - position.pending_listing_quantity
        self.assert_invariants()

    def apply_fill(self, fill: Fill) -> None:
        """应用一笔已经通过券商约束检查的成交。"""

        position = self.position(fill.symbol)
        fee = fill.total_fee
        if fill.side is OrderSide.BUY:
            cost = fill.notional + fee
            if self.cash + _EPSILON < cost:
                raise AccountInvariantError("买入成交消耗超过可用资金")
            self.cash -= cost
            old_cost = position.average_cost * position.total_quantity
            position.total_quantity += fill.quantity
            position.average_cost = (old_cost + cost) / position.total_quantity
        else:
            if (
                fill.quantity > position.total_quantity
                or fill.quantity > position.sellable_quantity
            ):
                raise AccountInvariantError("卖出成交超过总持仓或可卖持仓")
            proceeds = fill.notional - fee
            pnl = (fill.execution_price - position.average_cost) * fill.quantity - fee
            self.cash += proceeds
            position.total_quantity -= fill.quantity
            position.sellable_quantity -= fill.quantity
            position.realized_pnl += pnl
            if position.total_quantity == 0:
                position.average_cost = 0.0
                position.last_price = None
                position.stale_price = False
        self.assert_invariants()

    def mark_to_market(self, prices: Mapping[str, float]) -> None:
        """用当日未复权收盘价估值；缺少行情的持仓沿用旧价并标记 stale。"""

        for symbol, position in self.positions.items():
            if position.total_quantity == 0:
                continue
            price = prices.get(symbol)
            if price is None:
                if position.last_price is None:
                    raise BacktestDataError(f"持仓 {symbol} 从未获得有效估值价格")
                position.stale_price = True
                continue
            if not math.isfinite(price) or price <= 0:
                raise BacktestDataError(f"持仓 {symbol} 收盘价无效: {price}")
            position.last_price = price
            position.stale_price = False
        self.assert_invariants()

    def capture_entitlement(self, action_id: str, symbol: str) -> int:
        """登记日收盘按总持仓锁定公司行动权益。"""

        if action_id in self._entitlements:
            return self._entitlements[action_id]
        quantity = self.positions.get(symbol, Position(symbol)).total_quantity
        self._entitlements[action_id] = quantity
        return quantity

    def entitlement(self, action_id: str) -> int | None:
        return self._entitlements.get(action_id)

    def recognize_dividend(self, action_id: str, amount: float) -> None:
        if action_id in self._receivables:
            raise AccountInvariantError(f"分红应收款重复确认: {action_id}")
        if not math.isfinite(amount) or amount < 0:
            raise AccountInvariantError("分红金额无效")
        self._receivables[action_id] = amount
        self.assert_invariants()

    def settle_dividend(self, action_id: str) -> float:
        amount = self._receivables.pop(action_id, 0.0)
        self.cash += amount
        self.assert_invariants()
        return amount

    def apply_stock_dividend(
        self,
        *,
        action_id: str,
        symbol: str,
        entitlement_quantity: int,
        ratio: float,
    ) -> int:
        """除权日增加总持仓，新股在上市日前不可卖。"""

        if action_id in self._pending_stock:
            raise AccountInvariantError(f"送转股份重复确认: {action_id}")
        if ratio < 0 or not math.isfinite(ratio):
            raise AccountInvariantError("送转比例无效")
        new_quantity = math.floor(entitlement_quantity * ratio + 1e-9)
        if new_quantity == 0:
            self._pending_stock[action_id] = (symbol, 0)
            return 0
        position = self.position(symbol)
        old_total_cost = position.average_cost * position.total_quantity
        position.total_quantity += new_quantity
        position.pending_listing_quantity += new_quantity
        position.average_cost = old_total_cost / position.total_quantity
        self._pending_stock[action_id] = (symbol, new_quantity)
        self.assert_invariants()
        return new_quantity

    def list_pending_stock(self, action_id: str) -> int:
        item = self._pending_stock.pop(action_id, None)
        if item is None:
            return 0
        symbol, quantity = item
        position = self.position(symbol)
        if quantity > position.pending_listing_quantity:
            raise AccountInvariantError("上市股份超过待上市股份")
        position.pending_listing_quantity -= quantity
        position.sellable_quantity += quantity
        self.assert_invariants()
        return quantity

    def write_off(self, symbol: str) -> tuple[int, float]:
        """终止上市且没有估值依据时，把剩余股份明确核销为零。"""

        position = self.positions.get(symbol)
        if position is None or position.total_quantity == 0:
            return 0, 0.0
        quantity = position.total_quantity
        loss = position.market_value
        position.realized_pnl -= position.average_cost * quantity
        position.total_quantity = 0
        position.sellable_quantity = 0
        position.pending_listing_quantity = 0
        position.average_cost = 0.0
        position.last_price = None
        position.stale_price = False
        self.assert_invariants()
        return quantity, loss

    def snapshot(self, session: date) -> EquitySnapshot:
        equity = self.total_equity
        daily_return = None
        if self._last_equity is not None and self._last_equity > 0:
            daily_return = equity / self._last_equity - 1.0
        self._last_equity = equity
        active = [item for item in self.positions.values() if item.total_quantity > 0]
        return EquitySnapshot(
            session=session,
            cash=self.cash,
            dividend_receivable=self.dividend_receivable,
            market_value=self.market_value,
            total_equity=equity,
            daily_return=daily_return,
            holding_count=len(active),
            stale_position_count=sum(item.stale_price for item in active),
        )

    def assert_invariants(self) -> None:
        values = (self.cash, self.dividend_receivable, self.market_value)
        if any(not math.isfinite(value) or value < -_EPSILON for value in values):
            raise AccountInvariantError(f"账户出现负数或非有限值: {values}")
        for symbol, position in self.positions.items():
            quantities = (
                position.total_quantity,
                position.sellable_quantity,
                position.pending_listing_quantity,
            )
            if any(value < 0 for value in quantities):
                raise AccountInvariantError(f"{symbol} 持仓数量为负: {quantities}")
            if (
                position.sellable_quantity + position.pending_listing_quantity
                > position.total_quantity
            ):
                raise AccountInvariantError(f"{symbol} 可卖与待上市数量超过总持仓")
