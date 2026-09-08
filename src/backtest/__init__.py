"""清晰、同步、事件驱动的历史回测引擎。"""

from backtest.clock import MarketHours
from backtest.config import BacktestConfig, RunOptions
from backtest.domain import BacktestResult, Side
from backtest.engine import BacktestEngine
from backtest.runner import (
    CompletedRun,
    default_source_config,
    run_backtest,
    run_from_storage,
)
from backtest.schedule import Schedule
from backtest.strategy import Strategy, StrategyContext

__all__ = [
    "BacktestConfig",
    "BacktestEngine",
    "BacktestResult",
    "CompletedRun",
    "RunOptions",
    "MarketHours",
    "Schedule",
    "Side",
    "Strategy",
    "StrategyContext",
    "default_source_config",
    "run_backtest",
    "run_from_storage",
]
