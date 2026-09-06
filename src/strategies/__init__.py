"""项目自带的示例策略。"""

from strategies.momentum import (
    MomentumConfig,
    MonthlyMomentumStrategy,
    momentum_return,
    select_momentum_targets,
)

__all__ = [
    "MomentumConfig",
    "MonthlyMomentumStrategy",
    "momentum_return",
    "select_momentum_targets",
]
