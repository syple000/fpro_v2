"""独立于行情数据的交易时钟和事件时间线。"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")

_FREQUENCY_MINUTES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "60m": 60,
}


def at_time(session: date, value: time) -> datetime:
    """把交易日和本地时间组合成带上海时区的时间戳。"""
    return datetime.combine(session, value, tzinfo=SHANGHAI)


@dataclass(frozen=True, slots=True)
class MarketHours:
    """市场时间配置；默认对应当前 A 股数据源的交易和日线可见时间。"""

    exchange: str = "SSE"
    timezone: str = "Asia/Shanghai"
    session_start: time = time(9, 25)
    session_end: time = time(16, 5)
    segments: tuple[tuple[time, time], ...] = (
        (time(9, 30), time(11, 30)),
        (time(13), time(15)),
    )
    daily_bar_at: time = time(16, 5)

    def __post_init__(self) -> None:
        ZoneInfo(self.timezone)
        if not self.segments:
            raise ValueError("交易时段不能为空")
        previous_end = self.session_start
        for start, end in self.segments:
            if not previous_end <= start < end:
                raise ValueError("交易时段必须按时间排序且不能重叠或跨自然日")
            previous_end = end
        if not previous_end <= self.daily_bar_at <= self.session_end:
            raise ValueError("日线可见时间必须位于最后收盘与交易日结束之间")

    def at(self, session: date, value: time) -> datetime:
        return datetime.combine(session, value, tzinfo=ZoneInfo(self.timezone))


@dataclass(frozen=True, slots=True)
class Event:
    """引擎事件。策略回调只接收 kind='strategy' 的事件。"""

    # 引擎处理事件的模拟时刻；Bar 在此时已完整可见，不是成交记录时间。
    at: datetime
    # 所属交易日，用于日初解锁、权益登记和每日净值。
    session: date
    # Bar 事件：该 Bar 的开始。策略事件：关联 Bar 的开始或当前市场边界；
    # 不表示策略的历史观察窗口，查询历史时应自行指定 start/count。
    interval_start: datetime
    # 撮合使用的行情周期，不是 Schedule 中的策略调用周期。
    frequency: str
    kind: Literal["session_start", "bar", "strategy", "session_end"] = "bar"


def market_timeline(
    sessions: Sequence[date],
    frequency: str,
    market: MarketHours | None = None,
) -> tuple[Event, ...]:
    """只生成市场事件；不包含任何策略调仓规则。"""
    if frequency != "1d" and frequency not in _FREQUENCY_MINUTES:
        raise ValueError(f"不支持的 K 线频率: {frequency!r}")

    market = market or MarketHours()
    events: list[Event] = []
    for session in sessions:
        start = market.at(session, market.session_start)
        events.append(Event(start, session, start, frequency, "session_start"))
        if frequency == "1d":
            events.append(
                Event(
                    at=market.at(session, market.daily_bar_at),
                    session=session,
                    interval_start=market.at(session, market.segments[0][0]),
                    frequency=frequency,
                )
            )
        else:
            events.extend(_intraday_events(session, frequency, market))
        end = market.at(session, market.session_end)
        events.append(Event(end, session, end, frequency, "session_end"))
    return tuple(events)


def _intraday_events(
    session: date,
    frequency: str,
    market: MarketHours,
) -> list[Event]:
    """按配置的连续交易时段生成 Bar，不补造跨休市区间的行情。"""
    step = timedelta(minutes=_FREQUENCY_MINUTES[frequency])
    events: list[Event] = []
    for segment_start, segment_end in market.segments:
        interval_start = market.at(session, segment_start)
        end = market.at(session, segment_end)
        if (end - interval_start) % step:
            raise ValueError(f"交易时段不能被 {frequency} 整除，需要对应的数据聚合规则")
        while interval_start < end:
            interval_end = interval_start + step
            events.append(
                Event(
                    at=interval_end,
                    session=session,
                    interval_start=interval_start,
                    frequency=frequency,
                )
            )
            interval_start = interval_end
    return events


class Clock:
    """只允许向前移动的模拟时钟。"""

    def __init__(self) -> None:
        self.now: datetime | None = None

    def move_to(self, at: datetime) -> None:
        """只保证模拟时间不倒退；数据可见性由 PIT DataView 负责。"""
        if self.now is not None and at < self.now:
            raise ValueError(f"时钟不能倒退: {at} < {self.now}")
        self.now = at
