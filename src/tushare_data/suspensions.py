"""Tushare 日内停牌时段的共用解析。"""

import re
from datetime import time

_INTERVAL = re.compile(
    r"\s*([0-9]{1,2}):([0-9]{2})(?::([0-9]{2}))?\s*-"
    r"\s*([0-9]{1,2}):([0-9]{2})(?::([0-9]{2}))?\s*"
)


def parse_suspension_timing(value: str | None) -> tuple[tuple[time, time], ...]:
    """解析全部逗号分隔时段，秒数可省略；空值无时段，起点必须早于终点。"""
    if value is None:
        return ()

    intervals: list[tuple[time, time]] = []
    for part in value.split(","):
        match = _INTERVAL.fullmatch(part)
        if match is None:
            raise ValueError(f"停牌时段格式无效：{value!r}")
        start_hour, start_minute, start_second, end_hour, end_minute, end_second = map(
            int, match.groups(default="0")
        )
        try:
            start = time(start_hour, start_minute, start_second)
            end = time(end_hour, end_minute, end_second)
        except ValueError as exc:
            raise ValueError(f"停牌时段格式无效：{value!r}") from exc
        if start >= end:
            raise ValueError(f"停牌时段格式无效：{value!r}")
        intervals.append((start, end))
    return tuple(intervals)
