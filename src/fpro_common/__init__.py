"""项目各模块共用的少量基础规则。"""

from fpro_common.logging import BeijingFormatter, configure_beijing_logging
from fpro_common.time import (
    INT64_MAX,
    INT64_MIN,
    MICROSECONDS_PER_SECOND,
    datetime_to_utc_us,
    normalise_unix_timestamp_us,
    require_utc_us,
    utc_now_us,
    utc_us_to_datetime,
)

__all__ = [
    "BeijingFormatter",
    "INT64_MAX",
    "INT64_MIN",
    "MICROSECONDS_PER_SECOND",
    "configure_beijing_logging",
    "datetime_to_utc_us",
    "normalise_unix_timestamp_us",
    "require_utc_us",
    "utc_now_us",
    "utc_us_to_datetime",
]
