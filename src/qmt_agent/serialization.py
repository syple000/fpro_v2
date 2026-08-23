"""把 XtData DataFrame 拆成可以直接校验的具体记录。"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any, TypeAlias

from fpro_common import datetime_to_utc_us

DataFrameScalar: TypeAlias = str | bool | int | float | None


def dataframe_records(value: object, *, index_name: str = "index") -> list[dict[str, object]]:
    """逐行转换 pandas DataFrame，并把原 index 放入明确字段。"""
    frame: Any = value
    if not (frame.__class__.__module__.startswith("pandas") and getattr(frame, "ndim", 0) == 2):
        raise TypeError(f"期望 pandas DataFrame，实际为 {type(value)}")

    columns = list(frame.columns)
    if any(not isinstance(column, str) for column in columns):
        raise TypeError("DataFrame 列名必须全部是字符串")
    if len(columns) != len(set(columns)):
        raise ValueError("DataFrame 列名不能重复")
    if index_name in columns:
        raise ValueError(f"DataFrame 已包含保留字段 {index_name!r}")

    return [
        {
            index_name: _scalar(index),
            **{column: _scalar(item) for column, item in zip(columns, values, strict=True)},
        }
        for index, values in zip(frame.index, frame.itertuples(index=False), strict=True)
    ]


def _scalar(value: object) -> DataFrameScalar:
    """只接受具体行模型能够表达的标量，不提供任意 JSON 回退。"""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, datetime):
        return datetime_to_utc_us(value)
    if isinstance(value, date):
        return value.isoformat()
    if value.__class__.__name__ in {"NAType", "NaTType"}:
        return None

    # numpy 标量通过 item() 转为对应 Python 标量；容器和未知对象明确拒绝。
    item = getattr(value, "item", None)
    if callable(item):
        converted = item()
        if converted is not value:
            return _scalar(converted)
    raise TypeError(f"DataFrame 包含不支持的标量类型：{type(value)}")
