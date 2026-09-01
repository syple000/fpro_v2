"""现金分红、送转和股份上市的账户事件。"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, datetime, timedelta

from backtest.broker import SimBroker
from backtest.config import CorporateActionConfig
from backtest.errors import UnsupportedCorporateActionError
from backtest.portfolio import Portfolio
from backtest.types import CorporateAction, CorporateActionEvent


class CorporateActionProcessor:
    """公司行动只按公开时间和实际业务日期处理一次。"""

    def __init__(
        self,
        actions: tuple[CorporateAction, ...],
        config: CorporateActionConfig,
    ) -> None:
        self.config = config
        self._record: dict[date, list[CorporateAction]] = defaultdict(list)
        self._ex: dict[date, list[CorporateAction]] = defaultdict(list)
        self._pay: dict[date, list[CorporateAction]] = defaultdict(list)
        self._listing: dict[date, list[CorporateAction]] = defaultdict(list)
        for action in actions:
            if action.record_date is not None:
                self._record[action.record_date].append(action)
            if action.ex_date is not None:
                self._ex[action.ex_date].append(action)
            if action.pay_date is not None:
                self._pay[action.pay_date].append(action)
            if action.listing_date is not None:
                self._listing[action.listing_date].append(action)
        for index in (self._record, self._ex, self._pay, self._listing):
            for rows in index.values():
                rows.sort(key=lambda item: (item.symbol, item.action_id))
        self.events: list[CorporateActionEvent] = []
        self._processed_ex: set[str] = set()
        self._processed_pay: set[str] = set()
        self._processed_listing: set[str] = set()
        self._last_pre_open: date | None = None

    def capture_record_date(self, event_time: datetime, portfolio: Portfolio) -> None:
        for action in self._record.get(event_time.date(), ()):
            if action.visible_at > event_time:
                continue
            quantity = portfolio.capture_entitlement(
                action.action_id, action.symbol
            )
            if quantity:
                self.events.append(
                    CorporateActionEvent(
                        event_time=event_time,
                        action_id=action.action_id,
                        symbol=action.symbol,
                        event_type="RECORD_ENTITLEMENT",
                        quantity=quantity,
                        amount=0.0,
                        note="登记日收盘持仓",
                    )
                )

    def pre_open(
        self,
        event_time: datetime,
        *,
        portfolio: Portfolio,
        broker: SimBroker,
    ) -> None:
        start = (
            self._last_pre_open + timedelta(days=1)
            if self._last_pre_open
            else event_time.date()
        )
        current = start
        while current <= event_time.date():
            for action in self._ex.get(current, ()):
                self._apply_ex_date(action, event_time, portfolio, broker)
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
        broker: SimBroker,
    ) -> None:
        if action.action_id in self._processed_ex:
            return
        position_quantity = portfolio.positions.get(action.symbol)
        current_quantity = position_quantity.total_quantity if position_quantity else 0
        entitlement = portfolio.entitlement(action.action_id)
        if action.visible_at > event_time:
            if current_quantity and self.config.strict_unknown_actions:
                raise UnsupportedCorporateActionError(
                    f"{action.symbol} 的除权事件在 {event_time.date()} 尚不可见"
                )
            self._processed_ex.add(action.action_id)
            return
        if entitlement is None:
            if current_quantity and self.config.strict_unknown_actions:
                raise UnsupportedCorporateActionError(
                    f"{action.symbol} 除权时缺少登记日权益快照: {action.action_id}"
                )
            entitlement = 0
        if entitlement:
            # 收盘目标股数跨除权日会失真；保守撤销，等待策略下次重算。
            broker.cancel_symbol_for_corporate_action(action.symbol, event_time, portfolio)
        cash_per_share = self._cash_per_share(action, entitlement)
        cash_amount = round(entitlement * cash_per_share + 1e-9, 2)
        if cash_amount:
            if action.pay_date is None and self.config.strict_unknown_actions:
                raise UnsupportedCorporateActionError(
                    f"{action.symbol} 现金分红缺少派息日: {action.action_id}"
                )
            portfolio.recognize_dividend(action.action_id, cash_amount)
            self.events.append(
                CorporateActionEvent(
                    event_time=event_time,
                    action_id=action.action_id,
                    symbol=action.symbol,
                    event_type="DIVIDEND_RECEIVABLE",
                    quantity=entitlement,
                    amount=cash_amount,
                    note="除息日确认应收股利",
                )
            )
        if action.stock_dividend:
            if action.listing_date is None and entitlement and self.config.strict_unknown_actions:
                raise UnsupportedCorporateActionError(
                    f"{action.symbol} 送转缺少红股上市日: {action.action_id}"
                )
            new_quantity = portfolio.apply_stock_dividend(
                action_id=action.action_id,
                symbol=action.symbol,
                entitlement_quantity=entitlement,
                ratio=action.stock_dividend,
            )
            if new_quantity:
                self.events.append(
                    CorporateActionEvent(
                        event_time=event_time,
                        action_id=action.action_id,
                        symbol=action.symbol,
                        event_type="STOCK_DIVIDEND",
                        quantity=new_quantity,
                        amount=0.0,
                        note=f"送转比例 {action.stock_dividend:.8f}",
                    )
                )
        self._processed_ex.add(action.action_id)

    def _cash_per_share(self, action: CorporateAction, entitlement: int) -> float:
        if self.config.dividend_mode == "disabled":
            return 0.0
        if self.config.dividend_mode == "after_tax":
            value = action.cash_dividend
            alternative = action.cash_dividend_before_tax
        else:
            value = action.cash_dividend_before_tax
            alternative = action.cash_dividend
        if value is None:
            if (
                entitlement
                and alternative not in (None, 0.0)
                and self.config.strict_unknown_actions
            ):
                raise UnsupportedCorporateActionError(
                    f"{action.symbol} 缺少配置口径的每股现金分红: {action.action_id}"
                )
            return 0.0
        if not math.isfinite(value) or value < 0:
            raise UnsupportedCorporateActionError(
                f"{action.symbol} 每股现金分红无效: {value}"
            )
        if self.config.dividend_mode == "before_tax":
            return value * (1 - self.config.fixed_dividend_tax_rate)
        return value

    def _apply_pay_date(
        self,
        action: CorporateAction,
        event_time: datetime,
        portfolio: Portfolio,
    ) -> None:
        if action.action_id in self._processed_pay or action.visible_at > event_time:
            return
        amount = portfolio.settle_dividend(action.action_id)
        if amount:
            self.events.append(
                CorporateActionEvent(
                    event_time=event_time,
                    action_id=action.action_id,
                    symbol=action.symbol,
                    event_type="DIVIDEND_PAID",
                    quantity=0,
                    amount=amount,
                    note="应收股利转入现金",
                )
            )
        self._processed_pay.add(action.action_id)

    def _apply_listing_date(
        self,
        action: CorporateAction,
        event_time: datetime,
        portfolio: Portfolio,
    ) -> None:
        if action.action_id in self._processed_listing or action.visible_at > event_time:
            return
        quantity = portfolio.list_pending_stock(action.action_id)
        if quantity:
            self.events.append(
                CorporateActionEvent(
                    event_time=event_time,
                    action_id=action.action_id,
                    symbol=action.symbol,
                    event_type="STOCK_LISTED",
                    quantity=quantity,
                    amount=0.0,
                    note="红股变为可卖",
                )
            )
        self._processed_listing.add(action.action_id)
