"""清晰、同步、事件驱动的历史回测引擎。"""

from backtest.config import BacktestConfig, RunOptions
from backtest.domain import BacktestResult
from backtest.engine import BacktestEngine
from backtest.runner import (
    CompletedRun,
    default_source_config,
    run_backtest,
    run_from_storage,
)
from backtest.strategy import Strategy

__all__ = [
    "BacktestConfig",
    "BacktestEngine",
    "BacktestResult",
    "CompletedRun",
    "RunOptions",
    "Strategy",
    "default_source_config",
    "run_backtest",
    "run_from_storage",
]
