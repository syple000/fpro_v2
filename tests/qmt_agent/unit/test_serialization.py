from __future__ import annotations

import math

import numpy as np
import pandas as pd

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
        index=["000001.SZ"],
        columns=["20250101", "20250102"],
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
