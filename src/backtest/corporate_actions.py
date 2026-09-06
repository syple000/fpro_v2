"""分红送股处理；日期推进与账户记账集中在这里。"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable
from datetime import date, datetime, time

from backtest.broker import SimulatedBroker
from backtest.clock import at_time
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
        for action in actions:
            if action.record_date is not None:
                self._record[action.record_date].append(action)
            if action.ex_date is not None:
                self._ex[action.ex_date].append(action)
            if action.pay_date is not None:
                self._pay[action.pay_date].append(action)
            if action.listing_date is not None:
                self._listing[action.listing_date].append(action)

    @classmethod
    def load(
        cls,
        reader: DataReader,
        config: BacktestConfig,
    ) -> CorporateActionProcessor:
        """从 market_data 读取回测期末已知的公司行动并建立日期索引。"""
        final_at = at_time(config.end_date, time(23, 59, 59))
        rows = (
            reader.at(final_at)
            .corporate_actions.dividends(
                symbols=config.symbols or ALL_SYMBOLS,
                visible_end=final_at,
            )
            .table.to_pylist()
        )
        actions: list[CorporateAction] = []
        for index, row in enumerate(rows):
            stock_dividend = row.get("stock_dividend")
            if stock_dividend is None:
                stock_dividend = (row.get("stock_bonus_rate") or 0.0) + (
                    row.get("stock_conversion_rate") or 0.0
                )
            actions.append(
                CorporateAction(
                    action_id=f"CA{index:08d}",
                    symbol=row["symbol"],
                    visible_at=row["visible_at"],
                    record_date=row.get("record_date"),
                    ex_date=row.get("ex_date"),
                    pay_date=row.get("pay_date"),
                    listing_date=row.get("listing_date"),
                    cash_dividend=row.get("cash_dividend"),
                    cash_dividend_before_tax=row.get("cash_dividend_before_tax"),
                    stock_dividend=float(stock_dividend or 0.0),
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
        for action in self._ex.get(at.date(), ()):
            self._require_visible(action, at)
            broker.cancel_symbol(
                action.symbol,
                OrderReason.CORPORATE_ACTION,
                at,
            )
            entitlement = portfolio.entitlement(action.action_id)
            holding = portfolio.account_snapshot().holding(action.symbol)
            if entitlement is None:
                if holding is not None and holding.quantity > 0:
                    raise CorporateActionError(
                        f"{action.action_id} 缺少股权登记日持仓快照"
                    )
                entitlement = 0
            self._recognize(action, entitlement, portfolio)

        for action in self._pay.get(at.date(), ()):
            self._require_visible(action, at)
            portfolio.settle_dividend(action.action_id)

        for action in self._listing.get(at.date(), ()):
            self._require_visible(action, at)
            portfolio.list_stock_dividend(action.action_id)

    def on_session_end(self, at: datetime, portfolio: Portfolio) -> None:
        """日终按收盘持仓记录股权登记日权益。"""
        for action in self._record.get(at.date(), ()):
            self._require_visible(action, at)
            portfolio.capture_entitlement(action.action_id, action.symbol)

    @staticmethod
    def _recognize(
        action: CorporateAction,
        entitlement: int,
        portfolio: Portfolio,
    ) -> None:
        """除权日把登记数量换算成应收现金和待上市红股。"""
        cash_per_share = (
            action.cash_dividend
            if action.cash_dividend is not None
            else action.cash_dividend_before_tax
        )
        if cash_per_share is not None:
            if not math.isfinite(cash_per_share) or cash_per_share < 0:
                raise CorporateActionError(
                    f"{action.action_id} 每股现金分红无效"
                )
            if cash_per_share > 0 and action.pay_date is None:
                raise CorporateActionError(f"{action.action_id} 缺少派息日")
            portfolio.recognize_dividend(
                action.action_id, entitlement * cash_per_share
            )

        if not math.isfinite(action.stock_dividend) or action.stock_dividend < 0:
            raise CorporateActionError(f"{action.action_id} 送股比例无效")
        if action.stock_dividend > 0:
            if action.listing_date is None:
                raise CorporateActionError(f"{action.action_id} 缺少红股上市日")
            portfolio.add_stock_dividend(
                action.action_id,
                action.symbol,
                entitlement,
                action.stock_dividend,
            )

    @staticmethod
    def _require_visible(action: CorporateAction, at: datetime) -> None:
        """禁止使用模拟时刻之后才公布的公司行动，守住 PIT 边界。"""
        if action.visible_at > at:
            raise CorporateActionError(
                f"{action.action_id} 在 {at.isoformat()} 尚不可见，无法无前视地处理"
            )
