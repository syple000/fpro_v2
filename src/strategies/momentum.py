"""与回测或实盘无关的动量决策。"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MomentumConfig:
    """月度动量策略参数。

    默认比较“当前交易日前第 120 日”到“第 20 日”的累计收益，在月末选择排名靠前的股票并
    等权持有。这里的比例都使用小数，例如 0.10 表示 10%。
    """

    # 动量区间较早的端点距离当前多少个交易日。默认 120。
    lookback_sessions: int = 120
    # 动量区间较近的端点距离当前多少个交易日。默认跳过最近 20 日，避免追逐短期波动。
    skip_sessions: int = 20
    # 从拥有有效动量分数的股票中取排名前多少比例。0.10 表示前 10%。
    top_fraction: float = 0.10
    # 最多持有多少只股票；最终数量还会受到 top_fraction 限制。
    max_positions: int = 30
    # 所有目标持仓权重之和。0.98 表示计划使用 98% 资金，留下约 2% 现金缓冲。
    gross_exposure: float = 0.98
    # 单只股票的最大目标权重。0.05 表示最多占总资产 5%。
    max_position_weight: float = 0.05
    # True 时只买入动量收益大于 0 的股票；没有合格股票时保持现金。
    require_positive_momentum: bool = False

    def __post_init__(self) -> None:
        if self.lookback_sessions <= self.skip_sessions or self.skip_sessions < 0:
            raise ValueError(
                "lookback_sessions 必须大于 skip_sessions，且 skip_sessions 不能为负"
            )
        if not 0 < self.top_fraction <= 1:
            raise ValueError("top_fraction 必须位于 (0, 1]")
        if self.max_positions < 1:
            raise ValueError("max_positions 必须是正整数")
        if not 0 < self.gross_exposure <= 1:
            raise ValueError("gross_exposure 必须位于 (0, 1]")
        if not 0 < self.max_position_weight <= 1:
            raise ValueError("max_position_weight 必须位于 (0, 1]")


def momentum_return(old_value: float | None, recent_value: float | None) -> float | None:
    """用两个总收益指数端点计算动量；数据如何取得由运行环境负责。"""

    if old_value is None or recent_value is None or old_value <= 0:
        return None
    value = recent_value / old_value - 1.0
    return value if math.isfinite(value) else None


def select_momentum_targets(
    scores: Mapping[str, float | None],
    config: MomentumConfig,
) -> dict[str, float]:
    """按动量分数生成完整目标权重，回测和实盘使用同一决策。"""

    ranked = [
        (symbol, score)
        for symbol, score in scores.items()
        if score is not None
        and math.isfinite(score)
        and (not config.require_positive_momentum or score > 0)
    ]
    ranked.sort(key=lambda item: (-item[1], item[0]))
    count = min(math.ceil(len(ranked) * config.top_fraction), config.max_positions)
    selected = ranked[:count]
    if not selected:
        return {}
    weight = min(config.gross_exposure / len(selected), config.max_position_weight)
    return {symbol: weight for symbol, _ in selected}
