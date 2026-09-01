"""简单、严格、可复现的日频 A 股回测模块。"""

from backtest.config import (
    BacktestConfig,
    CorporateActionConfig,
    ExecutionConfig,
    FeeConfig,
    UniverseConfig,
)
from backtest.errors import (
    AccountInvariantError,
    ArtifactError,
    BacktestConfigurationError,
    BacktestDataError,
    BacktestError,
    UnsupportedCorporateActionError,
)

__all__ = [
    "AccountInvariantError",
    "ArtifactError",
    "BacktestConfig",
    "BacktestConfigurationError",
    "BacktestDataError",
    "BacktestError",
    "CorporateActionConfig",
    "ExecutionConfig",
    "FeeConfig",
    "UniverseConfig",
    "UnsupportedCorporateActionError",
]
