"""一个刻意保持简单的月度动量示例策略。"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, time

from backtest.clock import Event
from backtest.config import BacktestConfig, RunOptions
from backtest.runner import CompletedRun, run_from_storage
from backtest.schedule import Schedule
from backtest.strategy import Strategy, StrategyContext
from backtest.universe import select_stock_universe
from market_data import DataView


@dataclass(frozen=True, slots=True)
class MomentumConfig:
    """月度动量策略自己的参数，不混入通用回测配置。"""

    # 从当前交易日向前比较多少个交易日的累计收益。
    lookback_sessions: int = 20
    # 每次调仓最多持有得分最高的多少只股票。
    selection_count: int = 10
    # 候选股票至少已经历多少个交易日。
    minimum_listing_sessions: int = 250
    # 是否排除当前处于 ST 状态的股票。
    exclude_st: bool = True

    def __post_init__(self) -> None:
        """拒绝没有业务意义的窗口和数量。"""
        if self.lookback_sessions < 1:
            raise ValueError("lookback_sessions 必须是正整数")
        if self.selection_count < 1:
            raise ValueError("selection_count 必须是正整数")
        if self.minimum_listing_sessions < 0:
            raise ValueError("minimum_listing_sessions 不能为负")


def momentum_return(closes: Sequence[float | None]) -> float | None:
    """用窗口首尾两个前复权收盘价计算动量收益。"""
    if len(closes) < 2:
        return None
    first, last = closes[0], closes[-1]
    if (
        first is None
        or last is None
        or not math.isfinite(first)
        or not math.isfinite(last)
        or first <= 0
    ):
        return None
    value = last / first - 1
    return value if math.isfinite(value) else None


def select_momentum_targets(
    data: DataView,
    event: Event,
    config: MomentumConfig,
    *,
    allowed_symbols: Iterable[str] | None = None,
) -> dict[str, float]:
    """从 DataView 查询候选池和历史行情，返回等权目标组合。"""
    candidates = select_stock_universe(
        data,
        minimum_listing_sessions=config.minimum_listing_sessions,
        exclude_st=config.exclude_st,
        allowed_symbols=allowed_symbols,
    )
    if not candidates:
        return {}

    # 历史窗口、复权和底层查询优化都由 market_data 处理；策略只声明所需数据。
    rows = data.market.bars(
        symbols=candidates,
        frequency="1d",
        count=config.lookback_sessions + 1,
        fields=("close",),
        adjustment="forward",
    ).table.to_pylist()
    histories: dict[str, list[tuple[datetime, float | None]]] = defaultdict(list)
    for row in rows:
        interval_start = row["interval_start"]
        if not isinstance(interval_start, datetime):
            raise TypeError("行情 interval_start 必须是 datetime")
        histories[row["symbol"]].append((interval_start, row.get("close")))

    scored: list[tuple[float, str]] = []
    required = config.lookback_sessions + 1
    for symbol in candidates:
        history = sorted(histories[symbol], key=lambda item: item[0])
        if len(history) != required or history[-1][0].date() != event.session:
            continue
        score = momentum_return([close for _, close in history])
        if score is not None:
            scored.append((score, symbol))

    selected = [
        symbol
        for _, symbol in sorted(scored, key=lambda item: (-item[0], item[1]))[
            : config.selection_count
        ]
    ]
    if not selected:
        return {}
    weight = 1 / len(selected)
    return {symbol: weight for symbol in selected}


class MonthlyMomentumStrategy(Strategy):
    """月末日线可见后等权调仓，撮合可使用日线或分钟线。"""

    schedule = Schedule("month", at=time(16, 5))

    def __init__(
        self,
        config: MomentumConfig | None = None,
        *,
        allowed_symbols: Iterable[str] | None = None,
    ) -> None:
        """保存策略参数；allowed_symbols 可进一步限制候选范围。"""
        self.config = config or MomentumConfig()
        self.allowed_symbols = tuple(allowed_symbols) if allowed_symbols is not None else None

    def on_event(self, context: StrategyContext) -> dict[str, float]:
        """调度已保证调用日期，策略只负责计算目标组合。"""
        return select_momentum_targets(
            context.data,
            context.event,
            self.config,
            allowed_symbols=self.allowed_symbols,
        )


def run_monthly_momentum(
    *,
    config: BacktestConfig,
    strategy_config: MomentumConfig | None = None,
    options: RunOptions | None = None,
) -> CompletedRun:
    """示例策略的运行便捷入口；通用回测层不依赖具体策略。"""
    return run_from_storage(
        config=config,
        strategy=MonthlyMomentumStrategy(strategy_config, allowed_symbols=config.symbols),
        options=options,
    )
