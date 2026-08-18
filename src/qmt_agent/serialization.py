"""把 pandas、numpy 和 xtdata 返回值转换成标准 JSON 数据。"""

from __future__ import annotations

import logging
import math
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from typing import Any

from pydantic import JsonValue

from fpro_common import datetime_to_utc_us

logger = logging.getLogger(__name__)


def _mapping_to_jsonable(value: Mapping[object, object]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for key, item in value.items():
        json_key = str(key)
        if not isinstance(key, str):
            logger.debug("将非字符串 JSON 字段名转换为字符串：%r -> %r", key, json_key)
        if json_key in result:
            # 字符串化后的键发生冲突时，后一个值将覆盖前一个值；这是实际字段丢弃。
            logger.debug("JSON 字段名转换冲突，前一个字段将被丢弃：%r", json_key)
        result[json_key] = to_jsonable(item)
    return result


def to_jsonable(value: object) -> JsonValue:
    """递归转换常见科学计算对象，并把 NaN/Inf 转为 null。"""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    # 行情响应绝大多数是内置容器，先走快速路径，避免反复执行较昂贵的
    # dataclass 和 collections.abc.Mapping 实例检查。
    if isinstance(value, dict):
        return _mapping_to_jsonable(value)
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(item) for item in value]
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            # 非法字节会替换为 U+FFFD，这部分原始字节信息会丢失，必须留下记录。
            logger.debug("bytes 不是有效 UTF-8，序列化时替换无法解码的字节：%r", value)
            return value.decode("utf-8", errors="replace")
    if isinstance(value, datetime):
        return datetime_to_utc_us(value)
    if isinstance(value, date):
        return value.isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        return to_jsonable(asdict(value))
    if isinstance(value, Mapping):
        return _mapping_to_jsonable(value)

    # 以下对象来自 pandas、numpy 或 xtdata，只能在运行时按能力判断。
    dynamic_value: Any = value

    # DataFrame 用 split 结构保留索引和列名，且不会混淆重复时间列。
    if (
        dynamic_value.__class__.__module__.startswith("pandas")
        and getattr(dynamic_value, "ndim", 0) == 2
    ):
        split = dynamic_value.to_dict(orient="split")
        return to_jsonable(split)

    dtype = getattr(dynamic_value, "dtype", None)
    dtype_names = getattr(dtype, "names", None)
    if dtype_names:
        return [{name: to_jsonable(row[name]) for name in dtype_names} for row in dynamic_value]

    item = getattr(dynamic_value, "item", None)
    if callable(item):
        try:
            return to_jsonable(item())
        except (TypeError, ValueError):
            pass

    tolist = getattr(dynamic_value, "tolist", None)
    if callable(tolist):
        return to_jsonable(tolist())

    # 最后的字符串回退会丢失原对象类型和行为，只用于未被明确支持的第三方对象。
    logger.debug(
        "未知对象转换为字符串，原始类型信息将丢失：类型=%s，值=%r",
        type(dynamic_value),
        dynamic_value,
    )
    return str(dynamic_value)
