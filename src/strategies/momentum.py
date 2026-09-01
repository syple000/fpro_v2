"""月度中期横截面动量策略。"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any

from backtest.errors import BacktestConfigurationError
from backtest.strategy import BacktestData, StrategyContext


@dataclass(frozen=True, slots=True)
class MomentumConfig:
    """过去 lookback 日至 skip 日的收益率排名参数。"""

    lookback_sessions: int = 120
    skip_sessions: int = 20
    top_fraction: float = 0.10
    max_positions: int = 30
    gross_exposure: float = 0.98
    max_position_weight: float = 0.05
    require_positive_momentum: bool = False

    def __post_init__(self) -> None:
        if self.lookback_sessions <= self.skip_sessions or self.skip_sessions < 0:
            raise BacktestConfigurationError(
                "lookback_sessions 必须大于 skip_sessions，且 skip_sessions 不能为负"
            )
        if not 0 < self.top_fraction <= 1:
            raise BacktestConfigurationError("top_fraction 必须位于 (0, 1]")
        if self.max_positions < 1:
            raise BacktestConfigurationError("max_positions 必须是正整数")
        if not 0 < self.gross_exposure <= 1:
            raise BacktestConfigurationError("gross_exposure 必须位于 (0, 1]")
        if not 0 < self.max_position_weight <= 1:
            raise BacktestConfigurationError("max_position_weight 必须位于 (0, 1]")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class MonthlyMomentumStrategy:
    """每月末收盘排名，下一交易日开盘执行的多头等权策略。"""

    strategy_id = "monthly_medium_term_momentum_v1"

    def __init__(self, config: MomentumConfig | None = None) -> None:
        self.config = config or MomentumConfig()
        self.rebalance_log: list[dict[str, object]] = []

    def initialize(self, context: StrategyContext) -> None:
        del context

    def on_pre_open(self, context: StrategyContext, data: BacktestData) -> None:
        del context, data

    def on_close(self, context: StrategyContext, data: BacktestData) -> None:
        if not data.is_month_end:
            return
        candidates = data.candidate_symbols()
        scores: list[tuple[str, float]] = []
        for symbol in candidates:
            value = data.momentum_return(
                symbol,
                lookback=self.config.lookback_sessions,
                skip=self.config.skip_sessions,
            )
            if value is None or not math.isfinite(value):
                continue
            if self.config.require_positive_momentum and value <= 0:
                continue
            scores.append((symbol, value))
        scores.sort(key=lambda item: (-item[1], item[0]))
        fraction_count = max(math.ceil(len(scores) * self.config.top_fraction), 1) if scores else 0
        selected = scores[: min(fraction_count, self.config.max_positions)]
        if selected:
            weight = min(
                self.config.gross_exposure / len(selected),
                self.config.max_position_weight,
            )
            targets = {symbol: weight for symbol, _ in selected}
        else:
            weight = 0.0
            targets = {}
        context.rebalance(targets)
        self.rebalance_log.append(
            {
                "session": data.session.isoformat(),
                "candidate_count": len(candidates),
                "scored_count": len(scores),
                "selected_count": len(selected),
                "weight_per_position": weight,
                "minimum_selected_score": selected[-1][1] if selected else None,
                "maximum_selected_score": selected[0][1] if selected else None,
                "selected_symbols": [symbol for symbol, _ in selected],
            }
        )
