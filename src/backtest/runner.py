"""生产运行入口：组装数据源、引擎并返回结果。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time, timedelta
from pathlib import Path
from typing import Any

from backtest.clock import at_time
from backtest.config import BacktestConfig, RunOptions
from backtest.corporate_actions import CorporateActionProcessor
from backtest.domain import BacktestResult
from backtest.engine import BacktestEngine
from backtest.errors import DataError
from backtest.metrics import calculate_metrics
from backtest.output import write_results
from backtest.strategy import Strategy
from market_data import DataCatalog, DataReader, SourceConfig
from strategies import MomentumConfig, MonthlyMomentumStrategy


def default_source_config() -> SourceConfig:
    """返回策略探索可直接使用的完整数据路由。

    日线和研究数据使用 Tushare，分钟线与实时行情使用 QMT。路由集中在这里，
    新策略只需调用 run_from_storage，不需要了解 DataReader 的底层注册细节。
    """
    return SourceConfig(
        routes={
            "market.daily_bars": "tushare",
            "market.intraday_bars": "qmt",
            "market.realtime_quotes": "qmt",
            "market.daily_metrics": "tushare",
            "market.moneyflow": "tushare",
            "market.suspensions": "tushare",
            "market.price_limits": "tushare",
            "market.st_status": "tushare",
            "fundamentals.income": "tushare",
            "fundamentals.balance_sheet": "tushare",
            "fundamentals.cashflow": "tushare",
            "fundamentals.indicators": "tushare",
            "fundamentals.forecast": "tushare",
            "fundamentals.express": "tushare",
            "fundamentals.audit": "tushare",
            "corporate_actions.dividends": "tushare",
            "corporate_actions.adjustment_factors": "tushare",
            "classification.industry": "tushare",
            "reference.stocks": "tushare",
            "calendar.sessions": "tushare",
        }
    )


@dataclass(frozen=True, slots=True)
class CompletedRun:
    """向调用者同时返回业务结果、汇总指标和可选输出位置。"""

    result: BacktestResult
    metrics: dict[str, Any]
    output_dir: Path | None


def run_backtest(
    *,
    reader: DataReader,
    config: BacktestConfig,
    strategy: Strategy,
    output_dir: Path | None = None,
) -> CompletedRun:
    """使用已有 DataReader 运行任意策略，适合测试或嵌入其他程序。"""
    sessions, calendar = _load_sessions(reader, config)
    engine = BacktestEngine(
        reader=reader,
        config=config,
        sessions=sessions,
        calendar=calendar,
        strategy=strategy,
        actions=CorporateActionProcessor.load(reader, config),
    )
    result = engine.run()
    metrics = calculate_metrics(result, config)
    written = None
    if output_dir is not None:
        written = write_results(output_dir, config, result, metrics)
    return CompletedRun(result, metrics, written)


def _load_sessions(
    reader: DataReader,
    config: BacktestConfig,
) -> tuple[tuple[date, ...], tuple[date, ...]]:
    """读取交易日历；额外读取未来一段仅用于判断区间末尾是否为月末。"""
    calendar_end = config.end_date + timedelta(days=40)
    rows = reader.at(at_time(calendar_end, time(23, 59, 59))).calendar.sessions(
        start=config.start_date,
        end=calendar_end + timedelta(days=1),
        exchange="SSE",
        fields=("is_open",),
    ).table.to_pylist()
    calendar = tuple(row["cal_date"] for row in rows if row["is_open"] is True)
    sessions = tuple(session for session in calendar if session <= config.end_date)
    if not sessions:
        raise DataError("回测区间内没有交易日")
    return sessions, calendar


def run_from_storage(
    *,
    config: BacktestConfig,
    strategy: Strategy,
    options: RunOptions | None = None,
) -> CompletedRun:
    """打开项目默认数据目录和路由，组装并运行完整生产流水线。"""
    options = options or RunOptions()
    routes = default_source_config()
    with DataCatalog(
        tushare_root=options.tushare_root,
        qmt_root=options.qmt_root,
    ) as catalog:
        reader = DataReader(catalog, sources=routes, max_result_rows=50_000_000)
        return run_backtest(
            reader=reader,
            config=config,
            strategy=strategy,
            output_dir=options.output_dir,
        )


def run_monthly_momentum(
    *,
    config: BacktestConfig,
    strategy_config: MomentumConfig | None = None,
    options: RunOptions | None = None,
) -> CompletedRun:
    """创建内置月度动量策略并通过生产数据源运行。"""
    if config.frequency != "1d":
        raise ValueError("月度动量示例策略要求 frequency='1d'")
    return run_from_storage(
        config=config,
        strategy=MonthlyMomentumStrategy(
            strategy_config,
            allowed_symbols=config.symbols,
        ),
        options=options,
    )
