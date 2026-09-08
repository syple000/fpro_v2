"""分红送股处理；只按业务日期驱动账户记账。"""

from __future__ import annotations

import math
from bisect import bisect_left
from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import date, datetime

from backtest.broker import SimulatedBroker
from backtest.config import BacktestConfig
from backtest.domain import CorporateAction, OrderReason
from backtest.errors import CorporateActionError
from backtest.portfolio import Portfolio
from market_data import ALL_SYMBOLS, DataReader


class CorporateActionProcessor:
    """按业务日期索引公司行动，并驱动 Portfolio 完成相应记账。"""

    def __init__(self, actions: Iterable[CorporateAction]) -> None:
        """一次建立日期索引，避免每个交易日扫描全部历史事件。"""
        self._record: dict[date, list[CorporateAction]] = defaultdict(list)
        self._ex: dict[date, list[CorporateAction]] = defaultdict(list)
        self._pay: dict[date, list[CorporateAction]] = defaultdict(list)
        self._listing: dict[date, list[CorporateAction]] = defaultdict(list)
        self._sessions: frozenset[date] | None = None
        self._start_date = date.min
        self._end_date = date.max
        self._record_before_start: list[CorporateAction] = []
        self._record_after_close: dict[date, list[CorporateAction]] = defaultdict(list)
        for action in actions:
            if action.record_date is not None:
                self._record[action.record_date].append(action)
            if action.ex_date is not None:
                self._ex[action.ex_date].append(action)
            if action.pay_date is not None:
                self._pay[action.pay_date].append(action)
            if action.listing_date is not None:
                self._listing[action.listing_date].append(action)

    def set_sessions(
        self, sessions: Sequence[date], *, start_date: date, end_date: date
    ) -> None:
        """绑定实际回放日历；超出回放区间的未来结算无需在本次执行。"""
        ordered = sorted(set(sessions))
        self._sessions = frozenset(ordered)
        self._start_date, self._end_date = start_date, end_date
        self._record_before_start.clear()
        self._record_after_close.clear()
        for record_date, actions in self._record.items():
            if not start_date <= record_date <= end_date or record_date in self._sessions:
                continue
            index = bisect_left(ordered, record_date)
            if index == 0:
                self._record_before_start.extend(actions)
            else:
                # 空档内没有成交；前一交易日收盘后即可检查登记日异常。
                self._record_after_close[ordered[index - 1]].extend(actions)

    @classmethod
    def load(
        cls,
        reader: DataReader,
        config: BacktestConfig,
    ) -> CorporateActionProcessor:
        """读取当前数据快照中的实施记录，而不是模拟时刻可见的公告。

        策略通过 ``reader.at(clock.now)`` 读取 PIT 数据；这里读取的是账户需要
        回放的最终经济事实。两者使用同一份 market_data，但时间语义不同。
        """
        rows = reader.implemented_dividends(
            symbols=config.symbols or ALL_SYMBOLS,
        ).to_pylist()

        actions: list[CorporateAction] = []
        seen: set[tuple[object, ...]] = set()
        for row in rows:
            # market_data 保留预案和历史版本；账户只执行已经实施的事实。
            if row.get("div_proc") != "实施":
                continue
            record_date = row.get("record_date")
            if (
                record_date is None
                or not config.start_date <= record_date <= config.end_date
            ):
                continue

            stock_dividend = row.get("stock_dividend")
            if stock_dividend is None:
                stock_dividend = (row.get("stock_bonus_rate") or 0.0) + (
                    row.get("stock_conversion_rate") or 0.0
                )
            stock_dividend = float(stock_dividend or 0.0)
            key = _business_key(row, stock_dividend)
            if key in seen:
                continue
            seen.add(key)

            actions.append(
                CorporateAction(
                    action_id=_action_id(key),
                    symbol=row["symbol"],
                    record_date=record_date,
                    ex_date=row.get("ex_date"),
                    pay_date=row.get("pay_date"),
                    listing_date=row.get("listing_date"),
                    cash_dividend=row.get("cash_dividend"),
                    cash_dividend_before_tax=row.get("cash_dividend_before_tax"),
                    stock_dividend=stock_dividend,
                )
            )
        return cls(actions)

    def on_session_start(
        self,
        at: datetime,
        portfolio: Portfolio,
        broker: SimulatedBroker,
    ) -> None:
        """日初依次处理除权、派息和红股上市。"""
        for action in self._record_before_start:
            self._capture_entitlement(action, portfolio)
        self._record_before_start.clear()
        for action in self._ex.get(at.date(), ()):
            broker.cancel_symbol(
                action.symbol,
                OrderReason.CORPORATE_ACTION,
                at,
            )
            entitlement = self._entitlement(action, portfolio)
            if entitlement > 0:
                self._validate(action)
                self._recognize_cash(action, entitlement, portfolio)
                self._recognize_stock(action, entitlement, portfolio)

        for action in self._pay.get(at.date(), ()):
            # 特殊分配可能没有除权日，此时不猜日期，在真实派息日直接入账。
            if action.ex_date is None:
                entitlement = self._entitlement(action, portfolio)
                if entitlement > 0:
                    self._validate(action)
                    self._recognize_cash(action, entitlement, portfolio)
            portfolio.settle_dividend(action.action_id)

        for action in self._listing.get(at.date(), ()):
            # 没有除权日的送股，在真实上市日直接增加为可卖股票。
            if action.ex_date is None:
                entitlement = self._entitlement(action, portfolio)
                if entitlement > 0:
                    self._validate(action)
                    self._recognize_stock(action, entitlement, portfolio)
            portfolio.list_stock_dividend(action.action_id)

    def on_session_end(self, at: datetime, portfolio: Portfolio) -> None:
        """日终按收盘持仓记录权益；该内部快照不会暴露给策略。"""
        for action in (
            *self._record.get(at.date(), ()),
            *self._record_after_close.get(at.date(), ()),
        ):
            self._capture_entitlement(action, portfolio)

    def _capture_entitlement(self, action: CorporateAction, portfolio: Portfolio) -> None:
        entitlement = portfolio.capture_entitlement(action.action_id, action.symbol)
        # 无持仓的公司行动与账户无关；记住零权益，避免后来买入补得旧权益。
        if entitlement > 0:
            self._validate(action)

    @staticmethod
    def _recognize_cash(
        action: CorporateAction,
        entitlement: int,
        portfolio: Portfolio,
    ) -> None:
        """把登记数量换算成应收现金；调用前已经完成业务校验。"""
        cash_per_share = _cash_per_share(action)
        if cash_per_share is not None and cash_per_share > 0:
            portfolio.recognize_dividend(
                action.action_id, entitlement * cash_per_share
            )

    @staticmethod
    def _recognize_stock(
        action: CorporateAction,
        entitlement: int,
        portfolio: Portfolio,
    ) -> None:
        """把登记数量换算成待上市红股；调用前已经完成业务校验。"""
        if action.stock_dividend > 0:
            portfolio.add_stock_dividend(
                action.action_id,
                action.symbol,
                entitlement,
                action.stock_dividend,
            )

    def _validate(self, action: CorporateAction) -> None:
        """只校验会实际影响当前账户的公司行动。"""
        cash_per_share = _cash_per_share(action)
        if cash_per_share is not None and (
            not math.isfinite(cash_per_share) or cash_per_share < 0
        ):
            raise CorporateActionError(f"{action.action_id} 每股现金分红无效")
        if (
            cash_per_share is not None
            and cash_per_share > 0
            and action.pay_date is None
        ):
            raise CorporateActionError(f"{action.action_id} 缺少派息日")

        if not math.isfinite(action.stock_dividend) or action.stock_dividend < 0:
            raise CorporateActionError(f"{action.action_id} 送股比例无效")
        if action.stock_dividend > 0 and action.listing_date is None:
            raise CorporateActionError(f"{action.action_id} 缺少红股上市日")

        if action.record_date is None:
            raise CorporateActionError(f"{action.action_id} 缺少股权登记日")
        if action.ex_date is not None and action.ex_date <= action.record_date:
            raise CorporateActionError(f"{action.action_id} 除权日不晚于股权登记日")
        # 登记在日终，结算在日初；同一登记日也无法先登记再结算。
        if cash_per_share is not None and cash_per_share > 0 and action.pay_date is not None:
            if action.pay_date <= action.record_date:
                raise CorporateActionError(f"{action.action_id} 派息日不晚于股权登记日")
            if action.ex_date is not None and action.pay_date < action.ex_date:
                raise CorporateActionError(f"{action.action_id} 派息日早于除权日")
        if action.stock_dividend > 0 and action.listing_date is not None:
            if action.listing_date <= action.record_date:
                raise CorporateActionError(f"{action.action_id} 红股上市日不晚于股权登记日")
            if action.ex_date is not None and action.listing_date < action.ex_date:
                raise CorporateActionError(f"{action.action_id} 红股上市日早于除权日")

        if self._sessions is not None:
            dates = [("股权登记日", action.record_date), ("除权日", action.ex_date)]
            if cash_per_share is not None and cash_per_share > 0:
                dates.append(("派息日", action.pay_date))
            if action.stock_dividend > 0:
                dates.append(("红股上市日", action.listing_date))
            for label, business_date in dates:
                if (
                    business_date is not None
                    and self._start_date <= business_date <= self._end_date
                    and business_date not in self._sessions
                ):
                    raise CorporateActionError(
                        f"{action.action_id} {label} {business_date} 不在回放交易日历中"
                    )

    @staticmethod
    def _entitlement(action: CorporateAction, portfolio: Portfolio) -> int:
        """读取登记数量；持仓存在却没有登记快照时拒绝猜测。"""
        entitlement = portfolio.entitlement(action.action_id)
        if entitlement is not None:
            return entitlement
        holding = portfolio.account_snapshot().holding(action.symbol)
        if holding is not None and holding.quantity > 0:
            raise CorporateActionError(
                f"{action.action_id} 缺少股权登记日持仓快照"
            )
        return 0


def _cash_per_share(action: CorporateAction) -> float | None:
    """优先使用标准现金分红字段，缺失时退回税前字段。"""
    return (
        action.cash_dividend
        if action.cash_dividend is not None
        else action.cash_dividend_before_tax
    )


def _business_key(row: dict[str, object], stock_dividend: float) -> tuple[object, ...]:
    """忽略报告期等来源差异，识别实际只会执行一次的公司行动。"""
    return (
        row["symbol"],
        row.get("record_date"),
        row.get("ex_date"),
        row.get("pay_date"),
        row.get("listing_date"),
        row.get("cash_dividend"),
        row.get("cash_dividend_before_tax"),
        stock_dividend,
        row.get("base_date"),
        row.get("base_share"),
    )


def _action_id(key: tuple[object, ...]) -> str:
    """使用业务字段生成可读且不依赖查询顺序的稳定编号。"""
    return "CA:" + ":".join("-" if value is None else str(value) for value in key)
