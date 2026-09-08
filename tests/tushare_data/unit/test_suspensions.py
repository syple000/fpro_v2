from datetime import time

import pytest

from tushare_data.suspensions import parse_suspension_timing


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, ()),
        ("9:30-9:40", ((time(9, 30), time(9, 40)),)),
        (
            "09:30-09:40,09:40-09:50",
            ((time(9, 30), time(9, 40)), (time(9, 40), time(9, 50))),
        ),
        (
            " 09:30 - 09:40 , 9:50-10:00 ",
            ((time(9, 30), time(9, 40)), (time(9, 50), time(10))),
        ),
        (
            "09:40-09:45,09:30-11:00",
            ((time(9, 40), time(9, 45)), (time(9, 30), time(11))),
        ),
        (
            "09:30-09:40,09:30-09:40",
            ((time(9, 30), time(9, 40)), (time(9, 30), time(9, 40))),
        ),
        (
            "09:34:21-09:44:21,09:46:15-09:56:15",
            ((time(9, 34, 21), time(9, 44, 21)), (time(9, 46, 15), time(9, 56, 15))),
        ),
        (" 9:30:01 - 9:30:59 ", ((time(9, 30, 1), time(9, 30, 59)),)),
        ("09:30-09:30:01", ((time(9, 30), time(9, 30, 1)),)),
        ("09:30:59-09:31", ((time(9, 30, 59), time(9, 31)),)),
    ],
)
def test_parse_suspension_timing_keeps_every_interval(
    value: str | None, expected: tuple[tuple[time, time], ...]
) -> None:
    assert parse_suspension_timing(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        "9:3-9:40",
        "009:30-09:40",
        "09:30-09:60",
        "24:00-24:10",
        "10:00-09:30",
        "09:30-09:30",
        "09:30-",
        "-09:40",
        "prefix09:30-09:40",
        "09:30-09:40suffix",
        "09:30-09:40,",
        ",09:30-09:40",
        "09:30-09:40,,09:50-10:00",
        "09:30-09:40,invalid",
        "09:30:60-09:40:00",
        "09:30:00-09:40:60",
        "09:30:1-09:40:00",
        "09:30:00-09:40:1",
        "09:30:-09:40:00",
        "09:30:00.1-09:40:00",
        "09:30:00:00-09:40:00",
        "09:30:01-09:30:01",
        "09:30:59-09:30:01",
    ],
)
def test_parse_suspension_timing_rejects_the_whole_malformed_value(value: str) -> None:
    with pytest.raises(ValueError, match="停牌时段格式无效"):
        parse_suspension_timing(value)
