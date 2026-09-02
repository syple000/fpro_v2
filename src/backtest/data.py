"""回测行情入口：一次读取历史数据，再按照模拟日期逐日开放给策略。"""

from __future__ import annotations

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
    """把交易日和时刻组合成上海时区时间，供 PIT 数据查询使用。"""

    return datetime.combine(session, value, tzinfo=SHANGHAI)


@dataclass(frozen=True, slots=True)
class HistoryPoint:
    """策略计算和次日成交量限制需要的一天历史。"""

    # 交易日在本次回测中的从 0 开始序号，比自然日更适合计算“过去 N 个交易日”。
    session_index: int
    # 由每日 close / pre_close 连乘得到的收益指数；动量只比较两个端点的比值。
    total_return_index: float
    # 当日成交量，单位为股；次日开盘模拟成交时用它限制最大成交数量。
    volume: float | None


class StockRow(TypedDict):
    """构造股票池时实际使用的证券基础信息。"""

    symbol: str
    exchange: str | None
    listing_date: date


class SessionData:
    """传给策略的只读数据窗口。

    每个实例只代表一个交易日。它没有暴露 ``DataReader`` 或尚未释放的日线，因此策略不能绕过
    回测时钟读取未来数据。
    """

    __slots__ = ("_market", "_session", "_session_index")

    def __init__(self, market: MarketData, session: date, session_index: int) -> None:
        self._market = market
        self._session = session
        self._session_index = session_index

    @property
    def session(self) -> date:
        """当前模拟交易日。"""

        return self._session

    @property
    def session_index(self) -> int:
        """当前交易日在本次回测中的从 0 开始序号。"""

        return self._session_index

    @property
    def is_month_end(self) -> bool:
        """当前交易日是否为本月最后一个交易日。"""

        next_session = self._market.next_session(self._session)
        return next_session is not None and next_session.month != self._session.month

    def candidate_symbols(self) -> tuple[str, ...]:
        """返回当日满足上市时间、市场范围和 ST 规则的候选股票。"""

        return self._market.candidate_symbols(self._session, self._session_index)

    def history(self, symbol: str) -> Sequence[HistoryPoint]:
        """返回该股票截至当前收盘已经释放的有限历史。"""

        return self._market.history(symbol)

    def close(self, symbol: str) -> float | None:
        """返回该股票当前交易日已释放的收盘价；没有有效日线时返回 None。"""

        bar = self._market.released_bars.get(symbol)
        return bar.close if bar is not None else None


class MarketData:
    """管理回测所需行情，并守住数据可见时间。

    使用顺序只有四步：

    1. ``load()`` 一次性读取交易日历、全市场日线和公司行动；
    2. 每个交易日盘前调用 ``prepare_session()``，供开盘撮合读取当日开盘价；
    3. 收盘调用 ``release_close()``，此后策略才能看到当日收盘价和历史点；
    4. ``session_data()`` 创建绑定当前日期的只读窗口并交给策略。

    ``_prepared_bars`` 和 ``released_bars`` 必须分开：前者可能已经含有当日收盘字段，但只供引擎
    内部使用；后者才是策略在当前模拟时间允许读取的数据。
    """

    def __init__(
        self,
        reader: DataReader,
        config: BacktestConfig,
        *,
        history_window: int = 300,
    ) -> None:
        # DataReader 统一处理数据源路由和 PIT（只能看到指定时点已公开的数据）。
        self.reader = reader
        # 日期、股票池规则和模拟成交参数。
        self.config = config
        # 每只股票最多在内存中保留多少个已释放交易日。
        self.history_window = history_window

        # 配置区间内真正运行回测的交易日；load() 前为空。
        self.sessions: tuple[date, ...] = ()
        # 比结束日期多加载一段的交易日历，用来判断月末及寻找下一交易日。
        self._all_sessions: tuple[date, ...] = ()
        # 配置区间的全市场未复权日线，保持 Arrow 表以避免复制大量 Python 对象。
        self._bars: pa.Table | None = None
        # 交易日 -> (_bars 中的起始行, 当日行数)，用于快速切出一天的数据。
        self._day_offsets: dict[date, tuple[int, int]] = {}

        # 当前已经准备给引擎的交易日及日线；它们尚不代表策略可见。
        self._prepared_session: date | None = None
        self._prepared_bars: dict[str, DailyBar] = {}
        # 收盘后已经释放、允许策略读取的当日日线。
        self.released_bars: dict[str, DailyBar] = {}
        # 每只持仓最近一个有效收盘价，用于估值和把目标权重换算成股数。
        self.last_prices: dict[str, float] = {}
        # 每只股票截至当前模拟时点已经释放的有限历史。
        self._history: dict[str, deque[HistoryPoint]] = defaultdict(
            lambda: deque(maxlen=self.history_window)
        )
        # 构造下一天收益指数时需要的上一值。
        self._last_total_return_index: dict[str, float] = {}
        # 同一天的证券基础信息最多查询一次。
        self._stock_cache: dict[date, tuple[StockRow, ...]] = {}
        # 整个配置区间内可能影响现金和股份的分红送转事件。
        self.corporate_actions: tuple[CorporateAction, ...] = ()

    def load(self) -> None:
        """一次性载入交易日历、全市场日线和公司行动。

        多读结束日期之后 40 个自然日的日历，只是为了寻找下一交易日和识别月末；日线与公司
        行动仍以配置的结束时间为边界。
        """

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
        """为全市场日线建立按交易日切片的位置索引。"""

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
        """读取并转换回测账户需要处理的现金分红和送转股。"""

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
            actions.append(
                CorporateAction(
                    action_id=f"CA{ordinal:08d}",
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
        """准备一个交易日的行情供引擎撮合，但不向策略释放。

        返回值按证券代码索引。开盘阶段的撮合器只使用其中的开盘价；策略必须等
        ``release_close()`` 后才能通过 ``SessionData`` 读取当日数据。
        """

        if self._bars is None:
            raise BacktestDataError("MarketData 尚未 load()")
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
        """在收盘事件发生后释放当日日线，并更新估值价格和策略历史。"""

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
        """创建当前交易日的策略只读窗口。"""

        return SessionData(self, session, session_index)

    def history(self, symbol: str) -> tuple[HistoryPoint, ...]:
        """返回截至当前模拟时点已经释放的有限历史。"""

        return tuple(self._history.get(symbol, ()))

    def previous_volumes(self, symbols: Iterable[str]) -> dict[str, float | None]:
        """返回各股票最近已释放的成交量。

        引擎在当日开盘、释放当日收盘前调用，所以这里自然得到上一交易日成交量。
        """

        result: dict[str, float | None] = {}
        for symbol in symbols:
            history = self._history.get(symbol)
            result[symbol] = history[-1].volume if history else None
        return result

    def candidate_symbols(self, session: date, session_index: int) -> tuple[str, ...]:
        """构造策略股票池。

        股票必须属于支持的人民币市场、满足最低上市交易日数、当日拥有已释放日线；配置要求时
        还会排除当日 ST 股票。返回值按证券代码排序，保证回测可重复。
        """

        rows = self._stocks(session, time(16, 5))
        symbols = tuple(
            row["symbol"]
            for row in rows
            if row.get("exchange") in {"BSE", "SSE", "SZSE"}
            and self._listing_age(row["listing_date"], session_index)
            >= self.config.minimum_listing_sessions
            and row["symbol"] in self.released_bars
        )
        if not self.config.exclude_st or not symbols:
            return tuple(sorted(symbols))
        statuses = self.statuses(session, symbols, fields=("st_type",), at=time(16, 5))
        return tuple(sorted(symbol for symbol in symbols if statuses[symbol].st_type is None))

    def listed_symbols(self, session: date) -> frozenset[str]:
        """返回当日盘前仍处于上市区间的股票，用于识别持仓退市。"""

        return frozenset(row["symbol"] for row in self._stocks(session, time(9, 25)))

    def _stocks(self, session: date, at: time) -> tuple[StockRow, ...]:
        """按给定模拟时刻查询当时可见的人民币股票基础信息。"""

        cached = self._stock_cache.get(session)
        if cached is not None:
            return cached
        table = (
            self.reader.at(event_time(session, at))
            .reference.stocks(
                currency="CNY",
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
        """计算股票截至当前日期已经上市的交易日数量。"""

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
        """读取指定模拟时刻可见的停牌、涨跌停价和 ST 状态。"""

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
        """返回当前日期之后的第一个交易日；日历结束时返回 None。"""

        index = bisect_left(self._all_sessions, session)
        if index < len(self._all_sessions) and self._all_sessions[index] == session:
            index += 1
        return self._all_sessions[index] if index < len(self._all_sessions) else None
