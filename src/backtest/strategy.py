"""回测引擎使用的固定策略接口和内置策略适配器。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping

from backtest.data import SessionData
from strategies import MomentumConfig, momentum_return, select_momentum_targets


class Strategy(ABC):
    """所有回测策略都必须实现的最小接口。"""

    @property
    def history_window(self) -> int:
        """策略需要保留的历史交易日数量；不使用历史时保留当天即可。"""

        return 1

    @abstractmethod
    def on_close(self, data: SessionData) -> Mapping[str, float] | None:
        """收盘生成完整目标权重；返回 None 表示本日不调仓。"""


class MonthlyMomentumStrategy(Strategy):
    """为回测封装共享的月度动量决策。"""

    def __init__(self, config: MomentumConfig | None = None) -> None:
        self._config = config or MomentumConfig()

    @property
    def history_window(self) -> int:
        # 包含当前交易日，所以 120 日回看需要保存 121 个端点。
        return self._config.lookback_sessions + 1

    def on_close(self, data: SessionData) -> Mapping[str, float] | None:
        if not data.is_month_end:
            return None
        old_session = data.session_index - self._config.lookback_sessions
        recent_session = data.session_index - self._config.skip_sessions
        scores: dict[str, float | None] = {}
        for symbol in data.candidate_symbols():
            indexes = {
                point.session_index: point.total_return_index for point in data.history(symbol)
            }
            scores[symbol] = momentum_return(
                indexes.get(old_session),
                indexes.get(recent_session),
            )
        return select_momentum_targets(scores, self._config)
