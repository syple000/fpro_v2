"""回测与实盘共享的纯策略决策。"""

from strategies.momentum import MomentumConfig, momentum_return, select_momentum_targets
from strategies.weights import validate_target_weights

__all__ = [
    "MomentumConfig",
    "momentum_return",
    "select_momentum_targets",
    "validate_target_weights",
]
