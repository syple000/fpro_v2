"""简单、严格的日频 A 股回测模块。"""

from backtest.config import BacktestConfig, RunOptions
from backtest.errors import (
    AccountInvariantError,
    BacktestConfigurationError,
    BacktestDataError,
    BacktestError,
    UnsupportedCorporateActionError,
)

__all__ = [
    "AccountInvariantError",
    "BacktestConfig",
    "BacktestConfigurationError",
    "BacktestDataError",
    "BacktestError",
    "RunOptions",
    "UnsupportedCorporateActionError",
]
