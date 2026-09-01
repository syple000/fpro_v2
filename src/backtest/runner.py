"""构造固定数据源、运行策略并保存全部产物。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backtest.artifacts import (
    build_data_snapshot,
    code_fingerprint,
    deterministic_run_id,
    environment_metadata,
    write_artifacts,
)
from backtest.config import BacktestConfig
from backtest.data import DataPortal
from backtest.engine import BacktestEngine, BacktestResult
from backtest.metrics import calculate_metrics
from market_data import DataCatalog, DataReader, SourceConfig
from strategies import MomentumConfig, MonthlyMomentumStrategy

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
    output_dir: Path
    result: BacktestResult
    metrics: dict[str, Any]


def run_monthly_momentum(
    config: BacktestConfig,
    strategy_config: MomentumConfig | None = None,
) -> CompletedRun:
    """正式运行月度中期动量策略。"""

    workspace = Path(__file__).resolve().parents[2]
    strategy = MonthlyMomentumStrategy(strategy_config)
    strategy_metadata: dict[str, Any] = {
        "strategy_id": strategy.strategy_id,
        **strategy.config.to_dict(),
        "signal": "close/pre_close 链接的 PIT 总收益指数",
        "decision_time": "16:05",
        "earliest_execution": "下一交易日 09:30",
    }
    data_snapshot = build_data_snapshot(config.tushare_root)
    code = code_fingerprint(workspace)
    run_id = deterministic_run_id(
        config=config,
        strategy=strategy_metadata,
        data_snapshot=data_snapshot,
        code=code,
    )
    environment = environment_metadata(workspace, code)
    with DataCatalog(
        tushare_root=config.tushare_root,
        qmt_root=config.qmt_root,
    ) as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(_ROUTES),
            max_result_rows=20_000_000,
        )
        portal = DataPortal(
            reader,
            config,
            history_window=max(strategy.config.lookback_sessions + 5, 300),
        )
        portal.load()
        result = BacktestEngine(
            run_id=run_id,
            config=config,
            portal=portal,
            strategy=strategy,
        ).run()
    metrics = calculate_metrics(result, config)
    strategy_metadata["rebalance_log"] = strategy.rebalance_log
    output_dir = write_artifacts(
        config=config,
        result=result,
        metrics=metrics,
        strategy=strategy_metadata,
        data_snapshot=data_snapshot,
        environment=environment,
    )
    return CompletedRun(output_dir=output_dir, result=result, metrics=metrics)
