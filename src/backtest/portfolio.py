"""现金账户、持仓和估值。"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

from backtest.domain import AccountSnapshot, EquitySnapshot, Fill, Holding, Side
from backtest.errors import AccountError, DataError
from market_data.identity import SecurityCodeHistory, SecurityMappingError

_EPSILON = 1e-6


@dataclass(slots=True)
class Position:
    """账户内部的可变持仓；不会直接暴露给策略。"""

    symbol: str
    quantity: int = 0
    sellable_quantity: int = 0
    pending_listing_quantity: int = 0
    last_price: float | None = None
    stale_price: bool = False
    sid: int | None = None

    @property
    def market_value(self) -> float:
        """按最后一个有效价格计算持仓市值。"""
        return self.quantity * self.last_price if self.last_price is not None else 0.0


class Portfolio:
    """账户只接受成交和公司行动，不参与撮合决策。"""

    def __init__(
        self, initial_cash: float, *, identities: SecurityCodeHistory | None = None
    ) -> None:
        """用初始现金建立无持仓、无应收款的账户。"""
        if not math.isfinite(initial_cash) or initial_cash <= 0:
            raise ValueError("initial_cash 必须是有限正数")
        self.cash = float(initial_cash)
        self.identities = identities
        self._session: date | None = None
        self.positions: dict[int | str, Position] = {}
        # 应收股利计入权益但尚不可用；权益数量按公司行动 ID 固定在登记日。
        self._receivables: dict[str, float] = {}
        self._entitlements: dict[str, int] = {}
        # 红股在除权日计入总持仓，到上市日才转为可卖。
        self._pending_stock: dict[str, tuple[int | str, int]] = {}
        self._last_equity = self.cash

    @property
    def dividend_receivable(self) -> float:
        """已确认但尚未到账的现金分红。"""
        return sum(self._receivables.values())

    @property
    def market_value(self) -> float:
        """所有持仓按最新有效价格计算的总市值。"""
        return sum(position.market_value for position in self.positions.values())

    @property
    def total_equity(self) -> float:
        """现金、应收股利和证券市值之和。"""
        return self.cash + self.dividend_receivable + self.market_value

    def identity(self, symbol: str | int) -> int | str:
        if self.identities is None:
            return symbol
        if isinstance(symbol, str):
            return self.identities.sid(symbol)
        self.identities.aliases(symbol)
        return symbol

    def set_session(self, session: date) -> None:
        """更码只更新展示代码；持仓字典及登记权益始终使用原 sid。"""
        self._session = session
        if self.identities is not None:
            for position in self.positions.values():
                if position.quantity == 0:
                    continue
                assert position.sid is not None
                position.symbol = self.identities.code_at(position.sid, session)

    def position(self, symbol: str | int) -> Position:
        """读取内部持仓；不存在时创建一个零持仓。"""
        key = self.identity(symbol)
        if key not in self.positions:
            if self.identities is None:
                self.positions[key] = Position(str(symbol))
            else:
                assert isinstance(key, int)
                code = self.identities.aliases(key)[0]
                if self._session is not None:
                    code = self.identities.code_at(key, self._session)
                self.positions[key] = Position(code, sid=key)
        return self.positions[key]

    def account_snapshot(self) -> AccountSnapshot:
        """创建策略和 Broker 使用的不可变账户快照。"""
        holdings = tuple(
            Holding(
                symbol=position.symbol,
                quantity=position.quantity,
                sellable_quantity=position.sellable_quantity,
                market_value=position.market_value,
                sid=position.sid,
            )
            for position in sorted(self.positions.values(), key=lambda item: item.symbol)
            if position.quantity > 0
        )
        return AccountSnapshot(
            cash=self.cash,
            dividend_receivable=self.dividend_receivable,
            market_value=self.market_value,
            total_equity=self.total_equity,
            holdings=holdings,
        )

    def unlock_t1(self) -> None:
        """交易日开始时把此前买入的普通股票变为可卖。"""
        for position in self.positions.values():
            position.sellable_quantity = position.quantity - position.pending_listing_quantity
        self.assert_valid()

    def apply_fill(self, fill: Fill) -> None:
        """消费一笔已由 Broker 决定的成交，更新现金、数量和成本。"""
        self.assert_valid()
        if (
            isinstance(fill.quantity, bool)
            or not isinstance(fill.quantity, int)
            or fill.quantity <= 0
        ):
            raise AccountError("成交数量必须是正整数")
        if any(
            not math.isfinite(value) or value < 0
            for value in (
                fill.notional,
                fill.commission,
                fill.stamp_tax,
                fill.transfer_fee,
            )
        ):
            raise AccountError("成交金额和费用必须有限且非负")
        # 先验证全部约束，再提交现金和持仓；失败时不会创建持仓或改动账户。
        if (
            self.identities is not None and fill.sid is not None
            and self.identities.sid(fill.symbol) != fill.sid
        ):
            raise SecurityMappingError(f"成交 {fill.fill_id} 的 sid 与代码不一致")
        key = self.identity(fill.sid if fill.sid is not None else fill.symbol)
        position = self.positions.get(key) or Position(
            fill.symbol, sid=key if isinstance(key, int) else None
        )
        if fill.side is Side.BUY:
            cost = fill.notional + fill.total_fee
            if cost > self.cash + _EPSILON:
                raise AccountError("买入成交超过可用现金")
            new_cash = self.cash - cost
        else:
            if fill.quantity > position.quantity:
                raise AccountError("卖出成交超过持仓")
            if fill.quantity > position.sellable_quantity:
                raise AccountError("卖出成交超过可卖持仓")
            new_cash = self.cash + fill.notional - fill.total_fee
            if new_cash < -_EPSILON:
                raise AccountError("卖出成交收入及现金不足以支付费用")
        self.cash = max(0.0, new_cash)
        self.positions[key] = position
        if fill.side is Side.BUY:
            position.quantity += fill.quantity
            # 当日买入不增加 sellable_quantity，从而自然实现 T+1。
        else:
            position.quantity -= fill.quantity
            position.sellable_quantity -= fill.quantity
            if position.quantity == 0:
                position.last_price = None
                position.stale_price = False
        self.assert_valid()

    def mark_to_market(self, prices: Mapping[str, float]) -> None:
        """用当前批次收盘价估值；缺失行情时沿用旧价并标记 stale。"""
        normalized: dict[str | int, float] = {}
        for symbol, price in prices.items():
            key = self.identity(symbol)
            if key in normalized and normalized[key] != price:
                raise DataError(f"同一证券 {key} 的别名估值价格冲突")
            normalized[key] = price
        for symbol, position in self.positions.items():
            if position.quantity == 0:
                continue
            price = normalized.get(symbol)
            if price is None:
                if position.last_price is None:
                    raise DataError(f"持仓 {symbol} 没有估值价格")
                position.stale_price = True
                continue
            if not math.isfinite(price) or price <= 0:
                raise DataError(f"持仓 {symbol} 的估值价格无效: {price}")
            position.last_price = price
            position.stale_price = False
        self.assert_valid()

    def capture_entitlement(self, action_id: str, symbol: str | int) -> int:
        """在股权登记日冻结参与本次公司行动的持股数量。"""
        if action_id not in self._entitlements:
            position = self.positions.get(self.identity(symbol))
            self._entitlements[action_id] = position.quantity if position else 0
        return self._entitlements[action_id]

    def entitlement(self, action_id: str) -> int | None:
        """读取登记日权益；None 表示从未成功登记。"""
        return self._entitlements.get(action_id)

    def recognize_dividend(self, action_id: str, amount: float) -> None:
        """除息日确认应收股利，此时计入权益但不增加现金。"""
        if action_id in self._receivables:
            raise AccountError(f"重复确认分红: {action_id}")
        if not math.isfinite(amount) or amount < 0:
            raise AccountError("分红金额无效")
        self._receivables[action_id] = amount
        self.assert_valid()

    def settle_dividend(self, action_id: str) -> None:
        """派息日把应收股利转成可用现金。"""
        self.cash += self._receivables.pop(action_id, 0.0)
        self.assert_valid()

    def add_stock_dividend(
        self,
        action_id: str,
        symbol: str | int,
        entitlement: int,
        ratio: float,
    ) -> int:
        """除权日增加红股总数量，并保持其在上市前不可卖。"""
        if action_id in self._pending_stock:
            raise AccountError(f"重复确认送股: {action_id}")
        quantity = math.floor(entitlement * ratio + 1e-9)
        position = self.position(symbol)
        position.quantity += quantity
        position.pending_listing_quantity += quantity
        self._pending_stock[action_id] = (self.identity(symbol), quantity)
        self.assert_valid()
        return quantity

    def list_stock_dividend(self, action_id: str) -> None:
        """红股上市日把待上市数量转为可卖数量。"""
        pending = self._pending_stock.pop(action_id, None)
        if pending is None:
            return
        symbol, quantity = pending
        position = self.position(symbol)
        if quantity > position.pending_listing_quantity:
            raise AccountError("红股上市数量超过待上市数量")
        position.pending_listing_quantity -= quantity
        position.sellable_quantity += quantity
        self.assert_valid()

    def write_off(self, symbol: str | int) -> None:
        """证券退市时按零价值核销持仓。"""
        key = self.identity(symbol)
        position = self.positions.get(key)
        if position is None:
            return
        position.quantity = 0
        position.sellable_quantity = 0
        position.pending_listing_quantity = 0
        position.last_price = None
        position.stale_price = False
        # 红股已随总持仓核销，不能在原上市日再次解锁或恢复。
        self._pending_stock = {
            action_id: pending
            for action_id, pending in self._pending_stock.items()
            if pending[0] != key
        }
        self.assert_valid()

    def equity_snapshot(self, session: date) -> EquitySnapshot:
        """生成日终净值；首日以初始资金、后续以上日净值计算收益率。"""
        equity = self.total_equity
        daily_return = None
        if self._last_equity > 0:
            daily_return = equity / self._last_equity - 1
        self._last_equity = equity
        active = [position for position in self.positions.values() if position.quantity > 0]
        return EquitySnapshot(
            session=session,
            cash=self.cash,
            dividend_receivable=self.dividend_receivable,
            market_value=self.market_value,
            total_equity=equity,
            daily_return=daily_return,
            holding_count=len(active),
            stale_position_count=sum(position.stale_price for position in active),
        )

    def assert_valid(self) -> None:
        """集中检查账户不变量，让错误在产生处附近立即暴露。"""
        values = (self.cash, self.dividend_receivable, self.market_value)
        if any(not math.isfinite(value) or value < -_EPSILON for value in values):
            raise AccountError(f"账户金额无效: {values}")
        for position in self.positions.values():
            quantities = (
                position.quantity,
                position.sellable_quantity,
                position.pending_listing_quantity,
            )
            if any(quantity < 0 for quantity in quantities):
                raise AccountError(f"{position.symbol} 持仓数量为负")
            if position.sellable_quantity + position.pending_listing_quantity > position.quantity:
                raise AccountError(f"{position.symbol} 可卖与待上市数量超过总持仓")
