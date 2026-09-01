"""策略目标权重的最小公共约定。"""

from __future__ import annotations

import math
from collections.abc import Mapping


def validate_target_weights(weights: Mapping[str, float]) -> dict[str, float]:
    """校验完整目标组合并返回按证券代码排序的普通字典。"""

    normalized: dict[str, float] = {}
    for symbol, weight in weights.items():
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("目标权重证券代码不能为空")
        if (
            isinstance(weight, bool)
            or not isinstance(weight, (int, float))
            or not math.isfinite(weight)
            or not 0 <= weight <= 1
        ):
            raise ValueError(f"{symbol} 目标权重必须位于 [0, 1]")
        normalized[symbol] = float(weight)
    if sum(normalized.values()) > 1.0 + 1e-9:
        raise ValueError("目标权重之和不能超过 1")
    return dict(sorted(normalized.items()))
