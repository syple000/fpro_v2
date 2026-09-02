from __future__ import annotations

from backtest.strategy import MonthlyMomentumStrategy
from strategies import MomentumConfig


def test_momentum_strategy_declares_required_history() -> None:
    strategy = MonthlyMomentumStrategy(
        MomentumConfig(lookback_sessions=120, skip_sessions=20)
    )

    assert strategy.history_window == 121
