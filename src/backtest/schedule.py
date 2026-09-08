"""策略调用时间；与撮合 Bar 的周期分别配置。"""

from bisect import bisect_right
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta

from backtest.clock import Event, MarketHours
from backtest.errors import DataError


@dataclass(frozen=True, slots=True)
class Schedule:
    """every 支持 bar/day/week/month 或 Nm；times 可直接指定调用日期时间。"""

    every: str = "bar"
    at: time | None = None
    times: tuple[datetime, ...] | None = None

    def __post_init__(self) -> None:
        minute_rule = (
            self.every.endswith("m") and self.every[:-1].isdigit() and int(self.every[:-1]) > 0
        )
        if self.every not in {"bar", "day", "week", "month"} and not minute_rule:
            raise ValueError("every 必须是 bar/day/week/month 或正整数分钟，例如 7m")
        if self.at is not None and self.every not in {"day", "week", "month"}:
            raise ValueError("at 只用于 day/week/month 调度")
        if self.times is not None:
            if self.every != "bar" or self.at is not None:
                raise ValueError("times 不能与 every/at 规则同时设置")
            if any(value.tzinfo is None or value.utcoffset() is None for value in self.times):
                raise ValueError("指定调用时间必须带时区")

    @property
    def needs_next_session(self) -> bool:
        return self.every in {"week", "month"}

    def events(
        self,
        market_events: Sequence[Event],
        calendar: Sequence[date],
        market: MarketHours,
    ) -> tuple[Event, ...]:
        """先计算调用时间，再检查边界并构造策略事件。"""
        times = self._trigger_times(market_events, calendar, market)
        return _build_strategy_events(times, market_events)

    def _trigger_times(
        self,
        market_events: Sequence[Event],
        calendar: Sequence[date],
        market: MarketHours,
    ) -> set[datetime]:
        """只回答策略何时调用；此处不复制或构造 Event。"""
        if self.times is not None:
            return set(self.times)

        bars: dict[date, list[Event]] = defaultdict(list)
        for event in market_events:
            if event.kind == "bar":
                bars[event.session].append(event)

        times: set[datetime] = set()
        for session, day_bars in bars.items():
            if self.needs_next_session:
                index = bisect_right(calendar, session)
                if index == len(calendar):
                    raise DataError(f"无法判断 {session} 的周末/月末：缺少下一交易日")
                following = calendar[index]
                if self.every == "week":
                    same_period = session.isocalendar()[:2] == following.isocalendar()[:2]
                else:
                    same_period = (session.year, session.month) == (
                        following.year,
                        following.month,
                    )
                if same_period:
                    continue
            if self.every == "bar":
                times.update(event.at for event in day_bars)
            elif self.every in {"day", "week", "month"}:
                times.add(market.at(session, self.at) if self.at else day_bars[-1].at)
            else:
                step = timedelta(minutes=int(self.every[:-1]))
                for start, end in market.segments:
                    at = market.at(session, start) + step
                    while at <= market.at(session, end):
                        times.add(at)
                        at += step
        return times


def _build_strategy_events(
    times: set[datetime], market_events: Sequence[Event]
) -> tuple[Event, ...]:
    """调用时刻必须匹配市场边界；同一时刻优先关联已经完成的 Bar。"""
    boundaries = {event.at: event for event in market_events if event.kind == "bar"}
    for event in market_events:
        boundaries.setdefault(event.at, event)
        if event.kind == "bar":
            # 在 Bar 开始时调用时，关联起点，不把该 Bar 当作已经完成。
            boundaries.setdefault(event.interval_start, replace(event, at=event.interval_start))

    missing = times - boundaries.keys()
    if missing:
        raise DataError(
            f"策略调用时间 {min(missing)} 不在市场事件或 Bar 边界上；"
            "请使用更细的撮合周期，或调整调用时间"
        )
    return tuple(replace(boundaries[at], kind="strategy") for at in sorted(times))
