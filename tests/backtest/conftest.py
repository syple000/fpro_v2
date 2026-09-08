from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, time, timedelta

import pyarrow as pa

from backtest.clock import SHANGHAI
from market_data import ALL_SYMBOLS
from models import (
    BAR_SCHEMA,
    SECURITY_LIFECYCLE_SCHEMA,
    SESSION_SCHEMA,
    STATUS_SCHEMA,
    STOCK_SCHEMA,
    QueryResult,
)


def timestamp(session: date, value: time) -> datetime:
    return datetime.combine(session, value, tzinfo=SHANGHAI)


def bar_table(rows: Iterable[dict[str, object]]) -> pa.Table:
    normalized = []
    for row in rows:
        item = {
            "amount": None,
            "pre_close": None,
            "volume": 100_000.0,
            **row,
        }
        normalized.append(item)
    return pa.Table.from_pylist(normalized, schema=BAR_SCHEMA)


def daily_bar(
    session: date,
    close: float,
    *,
    open_price: float | None = None,
) -> dict[str, object]:
    return {
        "symbol": "000001.SZ",
        "interval_start": timestamp(session, time(9, 30)),
        "interval_end": timestamp(session, time(15)),
        "open": open_price if open_price is not None else close,
        "high": close,
        "low": close,
        "close": close,
        "pre_close": close - 1,
    }


class MemoryDataReader:
    """测试用极小数据源，保留 DataReader.at(...).market/reference/calendar 形状。"""

    def __init__(
        self,
        bars: pa.Table,
        sessions: Sequence[date],
        *,
        listing_date: date = date(2020, 1, 1),
        statuses: Mapping[str, Mapping[str, object]] | None = None,
    ) -> None:
        self.bars = bars
        self.sessions = tuple(sessions)
        raw_symbols = bars.column("symbol").to_pylist()
        self.symbols = tuple(sorted({symbol for symbol in raw_symbols if isinstance(symbol, str)}))
        self.listing_date = listing_date
        self.statuses = statuses or {}

    def at(self, as_of: datetime) -> MemoryDataView:
        return MemoryDataView(self, as_of)

    def security_lifecycles(self, *, symbols: Sequence[str]) -> pa.Table:
        return pa.Table.from_pylist(
            [
                {"symbol": symbol, "listing_date": self.listing_date, "delisting_date": None}
                for symbol in symbols
                if symbol in self.symbols
            ],
            schema=SECURITY_LIFECYCLE_SCHEMA,
        )


class MemoryDataView:
    def __init__(self, source: MemoryDataReader, as_of: datetime) -> None:
        self.as_of = as_of
        self.market = MemoryMarket(source, as_of)
        self.reference = MemoryReference(source, as_of)
        self.calendar = MemoryCalendar(source, as_of)


class MemoryMarket:
    def __init__(self, source: MemoryDataReader, as_of: datetime) -> None:
        self._source = source
        self._as_of = as_of

    def bars(
        self,
        *,
        symbols: object,
        frequency: str,
        count: int | None = None,
        start: date | datetime | None = None,
        end: date | datetime | None = None,
        fields: Sequence[str] | None = None,
        adjustment: str = "none",
        order: str = "asc",
    ) -> QueryResult:
        del adjustment
        selected = None if symbols is ALL_SYMBOLS else set(symbols)  # type: ignore[arg-type]
        rows = []
        for row in self._source.bars.to_pylist():
            if selected is not None and row["symbol"] not in selected:
                continue
            visible_at = row["interval_end"]
            if frequency == "1d":
                visible_at = timestamp(row["interval_start"].date(), time(16, 5))
            if visible_at > self._as_of:
                continue
            if start is not None and row["interval_start"] < _as_datetime(start):
                continue
            if end is not None and row["interval_start"] >= _as_datetime(end):
                continue
            rows.append(row)

        rows.sort(key=lambda row: (row["interval_end"], row["symbol"]))
        if count is not None:
            grouped: dict[str, list[dict[str, object]]] = {}
            for row in rows:
                grouped.setdefault(row["symbol"], []).append(row)
            rows = [row for group in grouped.values() for row in group[-count:]]
            rows.sort(key=lambda row: (row["interval_end"], row["symbol"]))
        if order == "desc":
            rows.reverse()

        table = pa.Table.from_pylist(rows, schema=BAR_SCHEMA)
        if fields is not None:
            table = table.select(["symbol", "interval_start", "interval_end", *fields])
        return QueryResult(table, self._as_of, ("memory",))

    def status(
        self,
        *,
        symbols: Sequence[str],
        fields: Sequence[str] | None = None,
    ) -> QueryResult:
        rows = [
            {
                "symbol": symbol,
                "suspended": False,
                "up_limit": None,
                "down_limit": None,
                "st_type": None,
                **self._source.statuses.get(symbol, {}),
            }
            for symbol in symbols
        ]
        table = pa.Table.from_pylist(rows, schema=STATUS_SCHEMA)
        if fields is not None:
            table = table.select(["symbol", *fields])
        return QueryResult(table, self._as_of, ("memory",))


class MemoryReference:
    def __init__(self, source: MemoryDataReader, as_of: datetime) -> None:
        self._source = source
        self._as_of = as_of

    def stocks(
        self,
        *,
        currency: str | None = None,
        fields: Sequence[str] | None = None,
    ) -> QueryResult:
        del currency
        rows = [
            {
                "symbol": symbol,
                "exchange": "SZSE",
                "market": "MAIN",
                "currency": "CNY",
                "listing_date": self._source.listing_date,
            }
            for symbol in self._source.symbols
            if self._source.listing_date <= self._as_of.date()
        ]
        table = pa.Table.from_pylist(rows, schema=STOCK_SCHEMA)
        if fields is not None:
            table = table.select(["symbol", *fields])
        return QueryResult(table, self._as_of, ("memory",))


class MemoryCalendar:
    def __init__(self, source: MemoryDataReader, as_of: datetime) -> None:
        self._source = source
        self._as_of = as_of

    def sessions(
        self,
        *,
        start: date,
        end: date | None = None,
        exchange: str | None = None,
        fields: Sequence[str] | None = None,
    ) -> QueryResult:
        exchange = exchange or "SSE"
        upper = end or self._as_of.date() + timedelta(days=1)
        rows = [
            {
                "cal_date": session,
                "exchange": exchange,
                "is_open": True,
                "previous_session": (self._source.sessions[index - 1] if index > 0 else None),
            }
            for index, session in enumerate(self._source.sessions)
            if start <= session < upper
        ]
        table = pa.Table.from_pylist(rows, schema=SESSION_SCHEMA)
        if fields is not None:
            table = table.select(["cal_date", "exchange", *fields])
        return QueryResult(table, self._as_of, ("memory",))


def _as_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return timestamp(value, time.min)
