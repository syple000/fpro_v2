"""连接数据、策略、引擎和可选输出。"""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

from backtest.config import BacktestConfig, RunOptions
from backtest.data import MarketData
from backtest.engine import BacktestEngine, BacktestResult
from backtest.metrics import calculate_metrics
from backtest.output import write_results
from backtest.strategy import monthly_momentum_targets
from market_data import DataCatalog, DataReader, SourceConfig
from strategies import MomentumConfig

_ROUTES = {
    route: "tushare"
    for route in (
        "calendar.sessions",
        "corporate_actions.dividends",
        "market.daily_bars",
        "market.price_limits",
        "market.st_status",
        "market.suspensions",
        "reference.stocks",
    )
}


@dataclass(frozen=True, slots=True)
class CompletedRun:
    result: BacktestResult
    metrics: dict[str, Any]
    output_dir: Path | None


def run_monthly_momentum(
    config: BacktestConfig,
    strategy_config: MomentumConfig | None = None,
    options: RunOptions | None = None,
) -> CompletedRun:
    run_options = options or RunOptions()
    momentum_config = strategy_config or MomentumConfig()
    strategy = partial(monthly_momentum_targets, config=momentum_config)
    with DataCatalog(
        tushare_root=run_options.tushare_root,
        qmt_root=run_options.qmt_root,
    ) as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(_ROUTES),
            max_result_rows=20_000_000,
        )
        data = MarketData(
            reader,
            config,
            history_window=max(momentum_config.lookback_sessions + 5, 300),
        )
        result = BacktestEngine(
            config=config,
            data=data,
            strategy=strategy,
        ).run()
    metrics = calculate_metrics(result, config)
    output_dir = None
    if run_options.output_dir is not None:
        output_dir = write_results(run_options.output_dir, config, result, metrics)
    return CompletedRun(result=result, metrics=metrics, output_dir=output_dir)
