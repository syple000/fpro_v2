"""直接使用当前 DataView 构建股票池；引擎不强制策略使用本模块。"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import timedelta

from backtest.errors import DataError
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
        if len(sessions) < minimum_listing_sessions:
            if not calendar_rows or calendar_rows[0]["cal_date"] > first_listing:
                raise DataError(
                    f"上市交易日判断所需的 SSE 日历覆盖不足：截至 {data.as_of.date()} "
                    f"仅查到 {len(sessions)} 个交易日，需要 {minimum_listing_sessions} 个，"
                    f"且未覆盖最早上市日 {first_listing}；请补齐历史交易日历"
                )
            # 已覆盖全部候选股的上市历史，交易日仍不足，说明它们确实未满门槛。
            return ()

        # 上市日计作第一天；只需最近 N 个交易日，不要求覆盖老股的完整上市历史。
        latest_listing_date = sessions[-minimum_listing_sessions]
        stocks = [
            row for row in stocks if row["listing_date"] <= latest_listing_date
        ]

    symbols = tuple(sorted(row["symbol"] for row in stocks))
    if not exclude_st or not symbols:
        return symbols
    statuses = data.market.status(symbols=symbols, fields=("st_type",)).table.to_pylist()
    st_types = {row["symbol"]: row.get("st_type") for row in statuses}
    return tuple(symbol for symbol in symbols if st_types.get(symbol) is None)
