"""项目统一使用的 Unix Epoch 微秒时间戳。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from time import time_ns

MICROSECONDS_PER_SECOND = 1_000_000
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def utc_now_us() -> int:
    """返回当前 Unix Epoch 微秒时间戳。"""
    return time_ns() // 1_000


def datetime_to_utc_us(value: datetime) -> int:
    """把带时区 datetime 精确转换为 Unix Epoch 微秒，不经过浮点数。"""
    if value.utcoffset() is None:
        raise ValueError("时间必须包含时区")
    delta = value.astimezone(UTC) - _EPOCH
    result = (
        (delta.days * 86_400 + delta.seconds) * MICROSECONDS_PER_SECOND
        + delta.microseconds
    )
    return require_utc_us(result)


def utc_us_to_datetime(value: int) -> datetime:
    """把 Unix Epoch 微秒时间戳转换成 UTC datetime，仅用于边界展示和分区。"""
    value = require_utc_us(value)
    try:
        return _EPOCH + timedelta(microseconds=value)
    except OverflowError as exc:
        raise ValueError(f"微秒时间戳超出 datetime 可表示范围: {value}") from exc


def require_utc_us(value: object, name: str = "timestamp_us") -> int:
    """校验值是 int64 范围内的微秒时间戳；bool 不作为整数接受。"""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} 必须是 Unix Epoch 微秒整数")
    if not INT64_MIN <= value <= INT64_MAX:
        raise ValueError(f"{name} 超出 int64 范围")
    return value


def normalise_unix_timestamp_us(value: object) -> int | None:
    """把 XtData 常见的秒/毫秒/微秒/纳秒整数统一成微秒。"""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    magnitude = abs(value)
    if magnitude < 100_000_000_000:
        result = value * 1_000_000
    elif magnitude < 100_000_000_000_000:
        result = value * 1_000
    elif magnitude < 100_000_000_000_000_000:
        result = value
    else:
        result = value // 1_000
    return result if INT64_MIN <= result <= INT64_MAX else None
