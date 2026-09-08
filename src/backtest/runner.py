"""生产运行入口：组装数据源、引擎并返回结果。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time, timedelta
from pathlib import Path
from typing import Any

from backtest.config import BacktestConfig, RunOptions
from backtest.corporate_actions import CorporateActionProcessor
from backtest.domain import BacktestResult
from backtest.engine import BacktestEngine
from backtest.errors import DataError
from backtest.metadata import capture_run_metadata
from backtest.metrics import calculate_metrics
from backtest.output import write_results
from backtest.strategy import Strategy
from market_data import DataCatalog, DataReader, SourceConfig
from market_data.identity import SecurityCodeHistory


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
    metadata = capture_run_metadata(reader, strategy) if output_dir is not None else None
    sessions, calendar = _load_sessions(
        reader, config, needs_next_session=strategy.schedule.needs_next_session
    )
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
        written = write_results(output_dir, config, result, metrics, metadata=metadata)
    return CompletedRun(result, metrics, written)


def _load_sessions(
    reader: DataReader,
    config: BacktestConfig,
    *,
    needs_next_session: bool = False,
) -> tuple[tuple[date, ...], tuple[date, ...]]:
    """读取运行区间；只有周期末调度需要额外的下一交易日。"""
    market = config.market
    rows = (
        reader.at(market.at(config.end_date, time.max))
        .calendar.sessions(
            start=config.start_date,
            end=config.end_date + timedelta(days=1),
            exchange=market.exchange,
            fields=("is_open",),
        )
        .table.to_pylist()
    )
    _validate_calendar(rows, config.start_date, config.end_date, market.exchange)
    sessions = tuple(row["cal_date"] for row in rows if row["is_open"] is True)
    if not sessions:
        raise DataError("回测区间内没有交易日")
    calendar = sessions
    if needs_next_session:
        # 下一交易日是市场日历信息，不是未来行情或公告。
        following = (
            reader.at(market.at(date.max, time.min))
            .calendar.sessions(
                start=config.end_date + timedelta(days=1),
                end=date.max,
                exchange=market.exchange,
                fields=("is_open",),
            )
            .table.to_pylist()
        )
        next_session = next((row["cal_date"] for row in following if row["is_open"] is True), None)
        if next_session is None:
            raise DataError(f"{config.end_date} 之后缺少下一交易日，无法判断周期末")
        _validate_calendar(
            following, config.end_date + timedelta(days=1), next_session, market.exchange
        )
        calendar = (*sessions, next_session)
    return sessions, calendar


def _validate_calendar(
    rows: list[dict[str, Any]], start: date, end: date, exchange: str
) -> None:
    """每个自然日都必须有明确开休市状态，不能用缺行代表节假日。"""
    states = {row["cal_date"]: row["is_open"] for row in rows}
    day = start
    while day <= end:
        if day not in states:
            raise DataError(f"{exchange} 日历覆盖不足：缺少 {day} 的开休市记录")
        if states[day] is not True and states[day] is not False:
            raise DataError(f"{exchange} 日历 {day} 的开休市状态未知")
        day += timedelta(days=1)


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
        identities=(SecurityCodeHistory.load(options.security_code_history)
                    if options.security_code_history is not None else None),
    ) as catalog:
        reader = DataReader(
            catalog, sources=routes, max_result_rows=50_000_000,
            bar_availability=config.bar_availability,
        )
        return run_backtest(
            reader=reader,
            config=config,
            strategy=strategy,
            output_dir=options.output_dir,
        )
