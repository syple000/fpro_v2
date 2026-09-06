"""独立于行情数据的交易时钟和事件时间线。"""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
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
class Event:
    """一根完整 K 线到达时的确定时间信息。"""

    at: datetime
    session: date
    interval_start: datetime
    frequency: str
    is_session_end: bool = False
    is_month_end: bool = False


def market_timeline(
    sessions: Sequence[date],
    calendar: Sequence[date],
    frequency: str,
) -> tuple[Event, ...]:
    """仅根据交易日历和频率生成回测时间线，不读取任何 Bar。"""
    if frequency != "1d" and frequency not in _FREQUENCY_MINUTES:
        raise ValueError(f"不支持的 K 线频率: {frequency!r}")

    events: list[Event] = []
    for session in sessions:
        month_end = _is_month_end(session, calendar)
        if frequency == "1d":
            events.append(
                Event(
                    at=at_time(session, time(16, 5)),
                    session=session,
                    interval_start=at_time(session, time(9, 30)),
                    frequency=frequency,
                    is_session_end=True,
                    is_month_end=month_end,
                )
            )
        else:
            events.extend(_intraday_events(session, frequency, month_end))
    return tuple(events)


def _intraday_events(
    session: date,
    frequency: str,
    month_end: bool,
) -> list[Event]:
    """生成 A 股上午、下午两个连续交易区间内的 K 线结束事件。"""
    step = timedelta(minutes=_FREQUENCY_MINUTES[frequency])
    segments = ((time(9, 30), time(11, 30)), (time(13), time(15)))
    events: list[Event] = []
    for segment_start, segment_end in segments:
        interval_start = at_time(session, segment_start)
        end = at_time(session, segment_end)
        while interval_start < end:
            interval_end = interval_start + step
            events.append(
                Event(
                    at=interval_end,
                    session=session,
                    interval_start=interval_start,
                    frequency=frequency,
                    is_session_end=interval_end == at_time(session, time(15)),
                    is_month_end=month_end,
                )
            )
            interval_start = interval_end
    return events


def _is_month_end(session: date, calendar: Sequence[date]) -> bool:
    """通过下一交易日是否跨月判断当前交易日是否为月末。"""
    index = bisect_right(calendar, session)
    return index < len(calendar) and calendar[index].month != session.month


class Clock:
    """只允许向前移动的模拟时钟。"""

    def __init__(self) -> None:
        self.now: datetime | None = None

    def move_to(self, at: datetime) -> None:
        """移动模拟时间，并用单调性检查阻止未来数据倒灌。"""
        if self.now is not None and at < self.now:
            raise ValueError(f"时钟不能倒退: {at} < {self.now}")
        self.now = at
