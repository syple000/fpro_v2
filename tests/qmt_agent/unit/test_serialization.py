from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from fpro_common import datetime_to_utc_us
from qmt_agent.serialization import to_jsonable


class Scalar:
    def __init__(self, value: object) -> None:
        self.value = value

    def item(self) -> object:
        return self.value


class Array:
    def __init__(self, values: list[object]) -> None:
        self.values = values
        self.dtype = None

    def tolist(self) -> list[object]:
        return self.values


def test_scientific_values_are_json_safe() -> None:
    result = to_jsonable(
        {
            "array": Array([Scalar(3), math.nan, math.inf]),
            "bytes": b"ok",
        }
    )

    assert result == {"array": [3, None, None], "bytes": "ok"}


def test_numpy_and_dataframe_are_json_safe() -> None:
    frame = pd.DataFrame(
        [[np.float64(10.5), np.nan]],
        index=pd.Index(["000001.SZ"]),
        columns=pd.Index(["20250101", "20250102"]),
    )

    result = to_jsonable({"close": frame, "volume": np.array([1, 2])})

    assert result == {
        "close": {
            "index": ["000001.SZ"],
            "columns": ["20250101", "20250102"],
            "data": [[10.5, None]],
        },
        "volume": [1, 2],
    }


def test_datetime_boundary_is_normalised_to_microseconds_and_naive_is_rejected() -> None:
    local_time = datetime(
        2026,
        8,
        18,
        9,
        30,
        tzinfo=timezone(timedelta(hours=8)),
    )

    expected = datetime_to_utc_us(local_time)
    assert to_jsonable(local_time) == expected
    assert to_jsonable(local_time.astimezone(UTC)) == expected
    with pytest.raises(ValueError, match="必须包含时区"):
        to_jsonable(datetime(2026, 8, 18, 1, 30))
