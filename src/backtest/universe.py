"""直接使用当前 DataView 构建股票池；引擎不强制策略使用本模块。"""

from __future__ import annotations

from bisect import bisect_left
from collections.abc import Iterable
from datetime import timedelta

from market_data import DataView

_STOCK_EXCHANGES = frozenset({"BSE", "SSE", "SZSE"})


def listed_symbols(data: DataView) -> frozenset[str]:
    """返回当前 DataView 中仍可见的人民币股票代码。"""
    rows = data.reference.stocks(currency="CNY", fields=()).table.to_pylist()
    return frozenset(row["symbol"] for row in rows)


def select_stock_universe(
    data: DataView,
    *,
    minimum_listing_sessions: int,
    exclude_st: bool,
    allowed_symbols: Iterable[str] | None = None,
) -> tuple[str, ...]:
    """按交易所、已上市交易日数和 ST 状态生成默认股票候选池。"""
    rows = data.reference.stocks(
        currency="CNY",
        fields=("exchange", "listing_date"),
    ).table.to_pylist()
    allowed = set(allowed_symbols) if allowed_symbols is not None else None
    stocks = [
        row
        for row in rows
        if row.get("exchange") in _STOCK_EXCHANGES
        and (allowed is None or row["symbol"] in allowed)
    ]
    if not stocks:
        return ()

    if minimum_listing_sessions > 0:
        first_listing = min(row["listing_date"] for row in stocks)
        calendar_rows = data.calendar.sessions(
            start=first_listing,
            end=data.as_of.date() + timedelta(days=1),
            exchange="SSE",
            fields=("is_open",),
        ).table.to_pylist()
        sessions = tuple(
            row["cal_date"] for row in calendar_rows if row["is_open"] is True
        )
        stocks = [
            row
            for row in stocks
            if len(sessions) - bisect_left(sessions, row["listing_date"])
            >= minimum_listing_sessions
        ]

    symbols = tuple(sorted(row["symbol"] for row in stocks))
    if not exclude_st or not symbols:
        return symbols
    statuses = data.market.status(symbols=symbols, fields=("st_type",)).table.to_pylist()
    st_types = {row["symbol"]: row.get("st_type") for row in statuses}
    return tuple(symbol for symbol in symbols if st_types.get(symbol) is None)
