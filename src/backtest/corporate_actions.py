"""未复权账户必需的现金分红和送转记账。"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, datetime, timedelta

from backtest.errors import UnsupportedCorporateActionError
from backtest.execution import ExecutionEngine
from backtest.portfolio import Portfolio
from backtest.types import CorporateAction, OrderReason


class CorporateActionProcessor:
    """按登记、除权、派息和上市日期依次处理公司行动。"""

    def __init__(self, actions: tuple[CorporateAction, ...]) -> None:
        self._record = self._index(actions, "record_date")
        self._ex = self._index(actions, "ex_date")
        self._pay = self._index(actions, "pay_date")
        self._listing = self._index(actions, "listing_date")
        self._last_pre_open: date | None = None

    @staticmethod
    def _index(
        actions: tuple[CorporateAction, ...],
        field: str,
    ) -> dict[date, list[CorporateAction]]:
        index: dict[date, list[CorporateAction]] = defaultdict(list)
        for action in actions:
            event_date = getattr(action, field)
            if event_date is not None:
                index[event_date].append(action)
        for rows in index.values():
            rows.sort(key=lambda item: (item.symbol, item.action_id))
        return index

    def capture_record_date(self, event_time: datetime, portfolio: Portfolio) -> None:
        for action in self._record.get(event_time.date(), ()):
            if action.visible_at <= event_time:
                portfolio.capture_entitlement(action.action_id, action.symbol)

    def pre_open(
        self,
        event_time: datetime,
        *,
        portfolio: Portfolio,
        execution: ExecutionEngine,
    ) -> None:
        current = (
            self._last_pre_open + timedelta(days=1) if self._last_pre_open else event_time.date()
        )
        while current <= event_time.date():
            for action in self._ex.get(current, ()):
                self._apply_ex_date(action, event_time, portfolio, execution)
            for action in self._pay.get(current, ()):
                self._apply_pay_date(action, event_time, portfolio)
            for action in self._listing.get(current, ()):
                self._apply_listing_date(action, event_time, portfolio)
            current += timedelta(days=1)
        self._last_pre_open = event_time.date()

    def _apply_ex_date(
        self,
        action: CorporateAction,
        event_time: datetime,
        portfolio: Portfolio,
        execution: ExecutionEngine,
    ) -> None:
        position = portfolio.positions.get(action.symbol)
        current_quantity = position.total_quantity if position else 0
        if action.visible_at > event_time:
            if current_quantity:
                raise UnsupportedCorporateActionError(f"{action.symbol} 的除权事件尚不可见")
            return
        entitlement = portfolio.entitlement(action.action_id)
        if entitlement is None:
            if current_quantity:
                raise UnsupportedCorporateActionError(f"{action.symbol} 除权时缺少登记日权益")
            entitlement = 0
        if entitlement:
            execution.cancel_symbol(action.symbol, reason=OrderReason.CORPORATE_ACTION)
        self._apply_cash_dividend(action, entitlement, portfolio)
        self._apply_stock_dividend(action, entitlement, portfolio)

    def _apply_cash_dividend(
        self,
        action: CorporateAction,
        entitlement: int,
        portfolio: Portfolio,
    ) -> None:
        value = action.cash_dividend
        if value is None:
            if entitlement and action.cash_dividend_before_tax not in (None, 0.0):
                raise UnsupportedCorporateActionError(f"{action.symbol} 缺少税后现金分红")
            return
        if not math.isfinite(value) or value < 0:
            raise UnsupportedCorporateActionError(f"{action.symbol} 现金分红无效")
        amount = round(entitlement * value + 1e-9, 2)
        if amount:
            if action.pay_date is None:
                raise UnsupportedCorporateActionError(f"{action.symbol} 现金分红缺少派息日")
            portfolio.recognize_dividend(action.action_id, amount)

    @staticmethod
    def _apply_stock_dividend(
        action: CorporateAction,
        entitlement: int,
        portfolio: Portfolio,
    ) -> None:
        if not action.stock_dividend:
            return
        if entitlement and action.listing_date is None:
            raise UnsupportedCorporateActionError(f"{action.symbol} 送转缺少红股上市日")
        portfolio.apply_stock_dividend(
            action_id=action.action_id,
            symbol=action.symbol,
            entitlement_quantity=entitlement,
            ratio=action.stock_dividend,
        )

    @staticmethod
    def _apply_pay_date(
        action: CorporateAction,
        event_time: datetime,
        portfolio: Portfolio,
    ) -> None:
        if action.visible_at <= event_time:
            portfolio.settle_dividend(action.action_id)

    @staticmethod
    def _apply_listing_date(
        action: CorporateAction,
        event_time: datetime,
        portfolio: Portfolio,
    ) -> None:
        if action.visible_at <= event_time:
            portfolio.list_pending_stock(action.action_id)
