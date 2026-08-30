"""通过 qmt-agent 下载数据并写入 QMT 存储。"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from qmt_protocol import (
    DividendFactorsResponse,
    DividendType,
    FinancialDownloadResponse,
    FinancialQueryResponse,
    FinancialReportType,
    FinancialTable,
    HistoryDownloadResponse,
    HistoryMode,
    HistoryQueryResponse,
    XtDataPeriod,
)
from qmt_receiver.schemas import DAILY_FIELDS
from qmt_receiver.storage import QmtDataStore

_FINANCIAL_TABLES: tuple[FinancialTable, ...] = (
    "Balance",
    "Income",
    "CashFlow",
    "Pershareindex",
)
_INTRADAY_PERIODS = frozenset({"1m", "5m", "15m", "30m", "1h"})


class _SyncClient(Protocol):
    def download_history(
        self,
        stocks: Sequence[str],
        period: XtDataPeriod = "1d",
        start_time: str = "",
        end_time: str = "",
        mode: HistoryMode = "incremental",
    ) -> HistoryDownloadResponse: ...

    def query_history(
        self,
        stocks: Sequence[str],
        fields: Sequence[str] = (),
        period: XtDataPeriod = "1d",
        start_time: str = "",
        end_time: str = "",
        count: int = -1,
        dividend_type: DividendType = "none",
        fill_data: bool = True,
    ) -> HistoryQueryResponse: ...

    def download_financial(
        self,
        stocks: Sequence[str],
        tables: Sequence[FinancialTable] = (),
        start_time: str = "",
        end_time: str = "",
    ) -> FinancialDownloadResponse: ...

    def query_financial(
        self,
        stocks: Sequence[str],
        tables: Sequence[FinancialTable] = (),
        start_time: str = "",
        end_time: str = "",
        report_type: FinancialReportType = "report_time",
    ) -> FinancialQueryResponse: ...

    def query_dividend_factors(
        self,
        stocks: Sequence[str],
        start_time: str = "",
        end_time: str = "",
    ) -> DividendFactorsResponse: ...


@dataclass(frozen=True, slots=True)
class SyncResult:
    daily_rows: int
    intraday_rows: int
    financial_rows: int
    dividend_factor_rows: int


def sync_daily(
    client: _SyncClient,
    store: QmtDataStore,
    stocks: Sequence[str],
    start_time: str,
    end_time: str,
    *,
    force: bool = False,
) -> int:
    """同步不复权日线；force 时覆盖下载已有历史。"""
    client.download_history(
        stocks,
        period="1d",
        start_time=start_time,
        end_time=end_time,
        mode="full" if force else "incremental",
    )
    response = client.query_history(
        stocks,
        fields=DAILY_FIELDS,
        period="1d",
        start_time=start_time,
        end_time=end_time,
        dividend_type="none",
        fill_data=False,
    )
    return store.write_daily(response.data, "none")


def sync_intraday(
    client: _SyncClient,
    store: QmtDataStore,
    stocks: Sequence[str],
    start_time: str,
    end_time: str,
    *,
    period: XtDataPeriod,
    force: bool = False,
) -> int:
    """同步 QMT 原生不复权历史分钟线。"""
    if period not in _INTRADAY_PERIODS:
        raise ValueError(f"分钟线不支持周期 {period!r}")
    client.download_history(
        stocks,
        period=period,
        start_time=start_time,
        end_time=end_time,
        mode="full" if force else "incremental",
    )
    response = client.query_history(
        stocks,
        fields=DAILY_FIELDS,
        period=period,
        start_time=start_time,
        end_time=end_time,
        dividend_type="none",
        fill_data=False,
    )
    return store.write_intraday(response.data, period, "none")


def sync_financial(
    client: _SyncClient,
    store: QmtDataStore,
    stocks: Sequence[str],
    start_time: str,
    end_time: str,
    tables: Sequence[FinancialTable] = _FINANCIAL_TABLES,
) -> int:
    """补全本地财务数据并按报告期同步指定区间。"""
    client.download_financial(stocks, tables)
    response = client.query_financial(
        stocks,
        tables,
        start_time,
        end_time,
        report_type="report_time",
    )
    return store.write_financial(response.data)


def sync_dividend_factors(
    client: _SyncClient,
    store: QmtDataStore,
    stocks: Sequence[str],
    start_time: str,
    end_time: str,
) -> int:
    """同步除权因子。"""
    response = client.query_dividend_factors(stocks, start_time, end_time)
    return store.write_dividend_factors(response.data)


def sync_all(
    client: _SyncClient,
    store: QmtDataStore,
    stocks: Sequence[str],
    start_time: str,
    end_time: str,
    *,
    force: bool = False,
) -> SyncResult:
    """同步日线、1 分钟线、财务和除权因子；force 时覆盖已有历史行情。"""
    return SyncResult(
        daily_rows=sync_daily(
            client,
            store,
            stocks,
            start_time,
            end_time,
            force=force,
        ),
        intraday_rows=sync_intraday(
            client,
            store,
            stocks,
            start_time,
            end_time,
            period="1m",
            force=force,
        ),
        financial_rows=sync_financial(client, store, stocks, start_time, end_time),
        dividend_factor_rows=sync_dividend_factors(
            client,
            store,
            stocks,
            start_time,
            end_time,
        ),
    )
