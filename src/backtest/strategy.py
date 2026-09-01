"""把回测历史转换为共享动量决策的输入。"""

from __future__ import annotations

from backtest.data import SessionData
from strategies import MomentumConfig, momentum_return, select_momentum_targets


def monthly_momentum_targets(
    data: SessionData,
    *,
    config: MomentumConfig,
) -> dict[str, float] | None:
    """只负责从回测数据中取出共享策略需要的两个收益端点。"""

    if not data.is_month_end:
        return None
    old_session = data.session_index - config.lookback_sessions
    recent_session = data.session_index - config.skip_sessions
    scores: dict[str, float | None] = {}
    for symbol in data.candidate_symbols():
        indexes = {point.session_index: point.total_return_index for point in data.history(symbol)}
        scores[symbol] = momentum_return(
            indexes.get(old_session),
            indexes.get(recent_session),
        )
    return select_momentum_targets(scores, config)
