"""数据来源适配器的显式公共接口。"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, NoReturn

import pyarrow as pa

from market_data.errors import DataCapabilityNotSupportedError


class DataAdapter:
    """数据来源适配器基类。

    自定义适配器只需覆盖实际支持的方法；未覆盖的方法会明确报告不支持。
    Reader 直接调用这些方法，不通过方法名或运行期反射分派。
    """

    def _not_supported(self, method: str) -> NoReturn:
        raise DataCapabilityNotSupportedError(
            f"适配器 {type(self).__name__!r} 不支持方法 {method!r}"
        )

    def daily_bars(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        start: date | datetime | None,
        end: date | datetime,
        count: int | None,
        adjustment: Literal["none", "forward"],
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("daily_bars")

    def intraday_bars(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        frequency: str,
        start: date | datetime | None,
        end: date | datetime,
        count: int | None,
        adjustment: Literal["none", "forward"],
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("intraday_bars")

    def current(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("current")

    def daily_metrics(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        start: date,
        end: date,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("daily_metrics")

    def moneyflow(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        start: date,
        end: date,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("moneyflow")

    def suspensions(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("suspensions")

    def price_limits(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("price_limits")

    def st_status(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("st_status")

    def statements(
        self,
        *,
        kind: Literal["income", "balance_sheet", "cash_flow"],
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        report_start: date | None,
        report_end: date | None,
        company_type: str | None,
        periods: int | None,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("statements")

    def financial_indicators(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        report_start: date | None,
        report_end: date | None,
        periods: int | None,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("financial_indicators")

    def disclosures(
        self,
        *,
        kind: Literal["forecast", "express", "audit"],
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        visible_start: datetime | None,
        visible_end: datetime,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("disclosures")

    def dividends(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        visible_start: datetime | None,
        visible_end: datetime,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("dividends")

    def adjustment_factors(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        start: date | None,
        end: date | None,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("adjustment_factors")

    def industry(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        level: Literal[1, 2, 3],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("industry")

    def stocks(
        self,
        *,
        as_of: datetime,
        exchange: str | None,
        market: str | None,
        currency: str | None,
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("stocks")

    def sessions(
        self,
        *,
        as_of: datetime,
        start: date,
        end: date,
        exchange: str | None,
        order: Literal["asc", "desc"],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        self._not_supported("sessions")

    def previous_session(self, *, end: date, exchange: str) -> pa.Table:
        self._not_supported("previous_session")
