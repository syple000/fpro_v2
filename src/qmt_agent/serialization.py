"""把 pandas、numpy 和 xtdata 返回值转换成标准 JSON 数据。"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from typing import Any


def to_jsonable(value: Any) -> Any:
    """递归转换常见科学计算对象，并把 NaN/Inf 转为 null。"""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        return to_jsonable(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(item) for item in value]

    # 以下对象来自 pandas、numpy 或 xtdata，只能在运行时按能力判断。
    dynamic_value: Any = value

    # DataFrame 用 split 结构保留索引和列名，且不会混淆重复时间列。
    if dynamic_value.__class__.__module__.startswith("pandas") and getattr(
        dynamic_value, "ndim", 0
    ) == 2:
        split = dynamic_value.to_dict(orient="split")
        return to_jsonable(split)

    dtype = getattr(dynamic_value, "dtype", None)
    dtype_names = getattr(dtype, "names", None)
    if dtype_names:
        return [
            {name: to_jsonable(row[name]) for name in dtype_names}
            for row in dynamic_value
        ]

    item = getattr(dynamic_value, "item", None)
    if callable(item):
        try:
            return to_jsonable(item())
        except (TypeError, ValueError):
            pass

    tolist = getattr(dynamic_value, "tolist", None)
    if callable(tolist):
        return to_jsonable(tolist())

    return str(dynamic_value)
