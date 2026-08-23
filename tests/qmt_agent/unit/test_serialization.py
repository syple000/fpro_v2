from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from fpro_common import datetime_to_utc_us
from qmt_agent.serialization import dataframe_records


def test_dataframe_becomes_concrete_rows_with_original_index() -> None:
    frame = pd.DataFrame(
        {
            "close": [np.float64(10.5), np.nan],
            "volume": [np.int64(1), np.int64(2)],
        },
        index=pd.Index([20250101, 20250102]),
    )

    assert dataframe_records(frame) == [
        {"index": 20250101, "close": 10.5, "volume": 1},
        {"index": 20250102, "close": None, "volume": 2},
    ]


def test_dataframe_supports_explicit_index_name_and_datetime_scalar() -> None:
    local_time = datetime(
        2026,
        8,
        18,
        9,
        30,
        tzinfo=timezone(timedelta(hours=8)),
    )
    frame = pd.DataFrame({"time": [local_time]}, index=pd.Index(["20260818"]))

    assert dataframe_records(frame, index_name="date") == [
        {"date": "20260818", "time": datetime_to_utc_us(local_time)}
    ]


@pytest.mark.parametrize(
    "value, message",
    [
        ({"index": [1]}, "期望 pandas DataFrame"),
        (pd.DataFrame({"index": [1]}), "已包含保留字段"),
        (pd.DataFrame([[1]], columns=pd.Index([1])), "列名必须全部是字符串"),
        (
            pd.DataFrame([[1, 2]], columns=pd.Index(["close", "close"])),
            "列名不能重复",
        ),
        (pd.DataFrame({"close": [[1, 2]]}), "不支持的标量类型"),
    ],
)
def test_dataframe_rejects_ambiguous_structures(value: object, message: str) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        dataframe_records(value)


def test_non_finite_python_float_becomes_none() -> None:
    frame = pd.DataFrame({"value": [math.inf, -math.inf, math.nan]})
    assert dataframe_records(frame) == [
        {"index": 0, "value": None},
        {"index": 1, "value": None},
        {"index": 2, "value": None},
    ]
