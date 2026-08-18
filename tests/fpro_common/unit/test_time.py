from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from fpro_common import (
    INT64_MAX,
    datetime_to_utc_us,
    normalise_unix_timestamp_us,
    require_utc_us,
    utc_now_us,
    utc_us_to_datetime,
)


def test_datetime_and_microseconds_round_trip_without_float() -> None:
    local = datetime(
        2026,
        8,
        18,
        9,
        30,
        0,
        123456,
        tzinfo=timezone(timedelta(hours=8)),
    )

    timestamp_us = datetime_to_utc_us(local)

    assert timestamp_us == 1_787_016_600_123_456
    assert utc_us_to_datetime(timestamp_us) == datetime(
        2026,
        8,
        18,
        1,
        30,
        0,
        123456,
        tzinfo=UTC,
    )


def test_xtdata_timestamp_units_are_normalised_to_microseconds() -> None:
    expected = 1_786_944_183_000_000

    assert normalise_unix_timestamp_us(1_786_944_183) == expected
    assert normalise_unix_timestamp_us(1_786_944_183_000) == expected
    assert normalise_unix_timestamp_us(1_786_944_183_000_000) == expected
    assert normalise_unix_timestamp_us(1_786_944_183_000_000_000) == expected


def test_microsecond_validation_is_strict_int64() -> None:
    assert require_utc_us(0) == 0
    assert isinstance(utc_now_us(), int)

    for invalid in (True, 1.0, "1"):
        with pytest.raises(TypeError, match="Unix Epoch 微秒整数"):
            require_utc_us(invalid)
    with pytest.raises(ValueError, match="int64"):
        require_utc_us(INT64_MAX + 1)
    with pytest.raises(ValueError, match="包含时区"):
        datetime_to_utc_us(datetime(2026, 8, 18, 1, 30))
