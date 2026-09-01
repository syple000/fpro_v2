"""绑定模拟时钟、批量释放行情的 DataPortal。"""

from __future__ import annotations

import hashlib
import json
import math
from bisect import bisect_left
from collections import defaultdict, deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, TypedDict
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc

from backtest.config import BacktestConfig
from backtest.errors import BacktestDataError
from backtest.types import CorporateAction, DailyBar, MarketStatus
from market_data import ALL_SYMBOLS, DataReader

if TYPE_CHECKING:
    from collections.abc import Sequence

SHANGHAI = ZoneInfo("Asia/Shanghai")


def event_time(session: date, value: time) -> datetime:
    return datetime.combine(session, value, tzinfo=SHANGHAI)


@dataclass(frozen=True, slots=True)
class HistoryPoint:
    session_index: int
    total_return_index: float
    volume: float | None


class StockRow(TypedDict):
    symbol: str
    exchange: str | None
    listing_date: date


class SessionData:
    """绑定单个交易日的策略数据，只返回已经释放的历史。"""

    __slots__ = ("_portal", "_session", "_session_index")

    def __init__(self, portal: DataPortal, session: date, session_index: int) -> None:
        self._portal = portal
        self._session = session
        self._session_index = session_index

    @property
    def session(self) -> date:
        return self._session

    @property
    def session_index(self) -> int:
        return self._session_index

    @property
    def is_month_end(self) -> bool:
        next_session = self._portal.next_session(self._session)
        return next_session is not None and next_session.month != self._session.month

    def candidate_symbols(self) -> tuple[str, ...]:
        return self._portal.candidate_symbols(self._session, self._session_index)

    def history(self, symbol: str) -> Sequence[HistoryPoint]:
        return self._portal.history(symbol)

    def close(self, symbol: str) -> float | None:
        bar = self._portal.released_bars.get(symbol)
        return bar.close if bar is not None else None


class DataPortal:
    """全市场行情只读取一次，并按事件时钟逐日释放。"""

    def __init__(
        self,
        reader: DataReader,
        config: BacktestConfig,
        *,
        history_window: int = 300,
    ) -> None:
        self.reader = reader
        self.config = config
        self.history_window = history_window
        self.sessions: tuple[date, ...] = ()
        self._all_sessions: tuple[date, ...] = ()
        self._bars: pa.Table | None = None
        self._day_offsets: dict[date, tuple[int, int]] = {}
        self._prepared_session: date | None = None
        self._prepared_bars: dict[str, DailyBar] = {}
        self.released_bars: dict[str, DailyBar] = {}
        self.last_prices: dict[str, float] = {}
        self._history: dict[str, deque[HistoryPoint]] = defaultdict(
            lambda: deque(maxlen=self.history_window)
        )
        self._last_total_return_index: dict[str, float] = {}
        self._stock_cache: dict[date, tuple[StockRow, ...]] = {}
        self.corporate_actions: tuple[CorporateAction, ...] = ()

    def load(self) -> None:
        """固定 Reader 当前快照，一次性载入日线和低频公司行动。"""

        calendar_end = self.config.end_date + timedelta(days=40)
        calendar_as_of = event_time(calendar_end, time(23, 59, 59))
        calendar = self.reader.at(calendar_as_of).calendar.sessions(
            start=self.config.start_date,
            end=calendar_end + timedelta(days=1),
            exchange="SSE",
            fields=("is_open",),
        )
        all_sessions = [
            row["cal_date"] for row in calendar.table.to_pylist() if row["is_open"] is True
        ]
        self._all_sessions = tuple(all_sessions)
        self.sessions = tuple(item for item in all_sessions if item <= self.config.end_date)
        if not self.sessions:
            raise BacktestDataError("配置区间内没有交易日")
        final_as_of = event_time(self.config.end_date, time(23, 59, 59))
        result = self.reader.at(final_as_of).market.bars(
            symbols=ALL_SYMBOLS,
            frequency="1d",
            start=self.config.start_date,
            end=final_as_of,
            fields=("open", "close", "pre_close", "volume"),
            adjustment="none",
        )
        self._bars = result.table
        self._day_offsets = self._build_day_offsets(result.table)
        missing = [session for session in self.sessions if session not in self._day_offsets]
        if missing:
            raise BacktestDataError(f"交易日缺少全市场日线分区: {missing[:5]}")
        self.corporate_actions = self._load_corporate_actions(final_as_of)

    @staticmethod
    def _build_day_offsets(table: pa.Table) -> dict[date, tuple[int, int]]:
        dates = pc.cast(table.column("interval_start"), pa.date32())
        counts = (
            pa.Table.from_arrays([dates], names=["session"])
            .group_by("session")
            .aggregate([("session", "count")])
        )
        ordered = sorted((item["session"], item["session_count"]) for item in counts.to_pylist())
        offsets: dict[date, tuple[int, int]] = {}
        offset = 0
        for session, count in ordered:
            offsets[session] = (offset, count)
            offset += count
        if offset != table.num_rows:
            raise BacktestDataError("日线日期索引与表行数不一致")
        return offsets

    def _load_corporate_actions(self, final_as_of: datetime) -> tuple[CorporateAction, ...]:
        table = (
            self.reader.at(final_as_of)
            .corporate_actions.dividends(
                symbols=ALL_SYMBOLS,
                visible_end=final_as_of,
            )
            .table
        )
        actions: list[CorporateAction] = []
        for ordinal, row in enumerate(table.to_pylist()):
            stock_dividend = row.get("stock_dividend")
            if stock_dividend is None:
                stock_dividend = (row.get("stock_bonus_rate") or 0.0) + (
                    row.get("stock_conversion_rate") or 0.0
                )
            identity = {
                "symbol": row["symbol"],
                "visible_at": row["visible_at"].isoformat(),
                "end_date": row["end_date"].isoformat() if row["end_date"] else None,
                "ann_date": row["ann_date"].isoformat() if row["ann_date"] else None,
                "ex_date": row["ex_date"].isoformat() if row["ex_date"] else None,
                "implementation_ann_date": (
                    row["implementation_ann_date"].isoformat()
                    if row["implementation_ann_date"]
                    else None
                ),
                "ordinal": ordinal,
            }
            action_id = (
                "CA"
                + hashlib.sha256(json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest()[
                    :16
                ]
            )
            actions.append(
                CorporateAction(
                    action_id=action_id,
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
        return tuple(actions)

    def prepare_session(self, session: date) -> Mapping[str, DailyBar]:
        if self._bars is None:
            raise BacktestDataError("DataPortal 尚未 load()")
        if self._prepared_session == session:
            return self._prepared_bars
        try:
            offset, length = self._day_offsets[session]
        except KeyError as exc:
            raise BacktestDataError(f"{session} 没有日线数据") from exc
        rows = self._bars.slice(offset, length).to_pylist()
        prepared: dict[str, DailyBar] = {}
        for row in rows:
            symbol = row["symbol"]
            prepared[symbol] = DailyBar(
                symbol=symbol,
                session=session,
                open=row.get("open"),
                close=row.get("close"),
                pre_close=row.get("pre_close"),
                volume=row.get("volume"),
            )
        self._prepared_session = session
        self._prepared_bars = prepared
        return prepared

    def release_close(self, session: date, session_index: int) -> None:
        bars = dict(self.prepare_session(session))
        self.released_bars = bars
        for symbol, bar in bars.items():
            close = bar.close
            pre_close = bar.pre_close
            if close is None or not math.isfinite(close) or close <= 0:
                continue
            self.last_prices[symbol] = close
            previous_index = self._last_total_return_index.get(symbol, 1.0)
            if pre_close is None or not math.isfinite(pre_close) or pre_close <= 0:
                if symbol in self._last_total_return_index:
                    continue
                total_return_index = 1.0
            else:
                total_return_index = previous_index * close / pre_close
            self._last_total_return_index[symbol] = total_return_index
            self._history[symbol].append(
                HistoryPoint(
                    session_index=session_index,
                    total_return_index=total_return_index,
                    volume=bar.volume,
                )
            )

    def session_data(self, session: date, session_index: int) -> SessionData:
        return SessionData(self, session, session_index)

    def history(self, symbol: str) -> tuple[HistoryPoint, ...]:
        """返回截至当前模拟时点已经释放的有限历史。"""

        return tuple(self._history.get(symbol, ()))

    def previous_volumes(self, symbols: Iterable[str]) -> dict[str, float | None]:
        result: dict[str, float | None] = {}
        for symbol in symbols:
            history = self._history.get(symbol)
            result[symbol] = history[-1].volume if history else None
        return result

    def candidate_symbols(self, session: date, session_index: int) -> tuple[str, ...]:
        rows = self._stocks(session, time(16, 5))
        symbols = tuple(
            row["symbol"]
            for row in rows
            if row.get("exchange") in self.config.universe.exchanges
            and self._listing_age(row["listing_date"], session_index)
            >= self.config.universe.minimum_listing_sessions
            and row["symbol"] in self.released_bars
        )
        if not self.config.universe.exclude_st or not symbols:
            return tuple(sorted(symbols))
        statuses = self.statuses(session, symbols, fields=("st_type",), at=time(16, 5))
        return tuple(sorted(symbol for symbol in symbols if statuses[symbol].st_type is None))

    def listed_symbols(self, session: date) -> frozenset[str]:
        return frozenset(row["symbol"] for row in self._stocks(session, time(9, 25)))

    def _stocks(self, session: date, at: time) -> tuple[StockRow, ...]:
        cached = self._stock_cache.get(session)
        if cached is not None:
            return cached
        table = (
            self.reader.at(event_time(session, at))
            .reference.stocks(
                currency=self.config.universe.currency,
                fields=("exchange", "listing_date"),
            )
            .table
        )
        rows = tuple(
            StockRow(
                symbol=row["symbol"],
                exchange=row["exchange"],
                listing_date=row["listing_date"],
            )
            for row in table.to_pylist()
        )
        self._stock_cache[session] = rows
        return rows

    def _listing_age(self, listing_date: date, session_index: int) -> int:
        first = self.sessions[0]
        if listing_date < first:
            # 同步数据从 2017 年开始，之前的交易日历不可用；252/365.2425 是明确、保守的估计。
            estimated_prior = math.floor((first - listing_date).days * 252 / 365.2425)
            return estimated_prior + session_index + 1
        listing_index = bisect_left(self.sessions, listing_date)
        return session_index - listing_index + 1

    def statuses(
        self,
        session: date,
        symbols: Iterable[str],
        *,
        fields: tuple[str, ...] = ("suspended", "up_limit", "down_limit", "st_type"),
        at: time = time(9, 25),
    ) -> dict[str, MarketStatus]:
        normalized = tuple(sorted(set(symbols)))
        if not normalized:
            return {}
        table = (
            self.reader.at(event_time(session, at))
            .market.status(
                symbols=normalized,
                fields=fields,
            )
            .table
        )
        result: dict[str, MarketStatus] = {}
        for row in table.to_pylist():
            result[row["symbol"]] = MarketStatus(
                symbol=row["symbol"],
                suspended=row.get("suspended"),
                up_limit=row.get("up_limit"),
                down_limit=row.get("down_limit"),
                st_type=row.get("st_type"),
            )
        return result

    def next_session(self, session: date) -> date | None:
        index = bisect_left(self._all_sessions, session)
        if index < len(self._all_sessions) and self._all_sessions[index] == session:
            index += 1
        return self._all_sessions[index] if index < len(self._all_sessions) else None
