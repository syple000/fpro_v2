"""通过 qmt-agent 下载数据并写入 QMT 存储。"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
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
    """同步不复权日线；已完成区间默认跳过。"""
    requested_start, requested_end = _requested_dates(start_time, end_time)
    total = 0
    for range_start, range_end, batch_stocks in _pending_batches(
        store,
        "daily",
        stocks,
        requested_start,
        requested_end,
        force=force,
    ):
        start = _format_date(range_start)
        end = _format_date(range_end)
        client.download_history(
            batch_stocks,
            period="1d",
            start_time=start,
            end_time=end,
            mode="full" if force else "incremental",
        )
        response = client.query_history(
            batch_stocks,
            fields=DAILY_FIELDS,
            period="1d",
            start_time=start,
            end_time=end,
            dividend_type="none",
            fill_data=False,
        )
        total += store.write_daily(response.data, "none")
        for code in batch_stocks:
            store.mark_sync_completed("daily", code, range_start, range_end)
    return total


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
    """同步 QMT 原生不复权历史分钟线；已完成区间默认跳过。"""
    if period not in _INTRADAY_PERIODS:
        raise ValueError(f"分钟线不支持周期 {period!r}")
    requested_start, requested_end = _requested_dates(start_time, end_time)
    total = 0
    for range_start, range_end, batch_stocks in _pending_batches(
        store,
        "intraday",
        stocks,
        requested_start,
        requested_end,
        period=period,
        force=force,
    ):
        start = _format_date(range_start)
        end = _format_date(range_end)
        client.download_history(
            batch_stocks,
            period=period,
            start_time=start,
            end_time=end,
            mode="full" if force else "incremental",
        )
        response = client.query_history(
            batch_stocks,
            fields=DAILY_FIELDS,
            period=period,
            start_time=start,
            end_time=end,
            dividend_type="none",
            fill_data=False,
        )
        total += store.write_intraday(response.data, period, "none")
        for code in batch_stocks:
            store.mark_sync_completed("intraday", code, range_start, range_end, period=period)
    return total


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
    *,
    force: bool = False,
) -> int:
    """同步除权因子；已完成区间默认跳过。"""
    requested_start, requested_end = _requested_dates(start_time, end_time)
    total = 0
    for range_start, range_end, batch_stocks in _pending_batches(
        store,
        "dividend_factors",
        stocks,
        requested_start,
        requested_end,
        force=force,
    ):
        response = client.query_dividend_factors(
            batch_stocks,
            _format_date(range_start),
            _format_date(range_end),
        )
        total += store.write_dividend_factors(response.data)
        for code in batch_stocks:
            store.mark_sync_completed("dividend_factors", code, range_start, range_end)
    return total


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
            force=force,
        ),
    )


def _pending_batches(
    store: QmtDataStore,
    dataset: str,
    stocks: Sequence[str],
    requested_start: date,
    requested_end: date,
    *,
    period: XtDataPeriod | None = None,
    force: bool,
) -> list[tuple[date, date, tuple[str, ...]]]:
    normalized = tuple(dict.fromkeys(stock.strip().upper() for stock in stocks))
    if not normalized or any(not stock for stock in normalized):
        raise ValueError("stocks 不能为空")
    grouped: dict[tuple[date, date], list[str]] = {}
    for stock in normalized:
        completed = [] if force else store.sync_completed_ranges(dataset, stock, period=period)
        for missing in _missing_date_ranges(
            requested_start,
            requested_end,
            completed,
        ):
            grouped.setdefault(missing, []).append(stock)
    return [
        (range_start, range_end, tuple(batch_stocks))
        for (range_start, range_end), batch_stocks in sorted(grouped.items())
    ]


def _requested_dates(start_time: str, end_time: str) -> tuple[date, date]:
    if len(start_time) != 8 or len(end_time) != 8:
        raise ValueError("QMT 同步区间必须使用 YYYYMMDD 日期")
    try:
        start_date = datetime.strptime(start_time, "%Y%m%d").date()
        end_date = datetime.strptime(end_time, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError("QMT 同步区间包含无效日期") from exc
    if start_date > end_date:
        raise ValueError("start_time 不能晚于 end_time")
    return start_date, end_date


def _missing_date_ranges(
    requested_start: date,
    requested_end: date,
    completed: Sequence[tuple[date, date]],
) -> list[tuple[date, date]]:
    missing: list[tuple[date, date]] = []
    cursor = requested_start
    for completed_start, completed_end in completed:
        if completed_end < cursor:
            continue
        if completed_start > requested_end:
            break
        if completed_start > cursor:
            missing.append((cursor, min(requested_end, completed_start - timedelta(days=1))))
        cursor = max(cursor, completed_end + timedelta(days=1))
        if cursor > requested_end:
            break
    if cursor <= requested_end:
        missing.append((cursor, requested_end))
    return missing


def _format_date(value: date) -> str:
    return value.strftime("%Y%m%d")
