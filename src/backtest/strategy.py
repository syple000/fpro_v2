"""策略接口。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping

from backtest.clock import Event
from backtest.domain import AccountSnapshot
from market_data import DataView


class Strategy(ABC):
    """唯一要求：读取当前数据和账户，返回完整目标权重。"""

    @abstractmethod
    def on_bar(
        self,
        data: DataView,
        event: Event,
        account: AccountSnapshot,
    ) -> Mapping[str, float] | None:
        """在 K 线结束时读取当前 DataView，并可选返回完整目标权重。

        返回 None 表示本根 K 线不调仓；空字典表示把现有持仓目标全部降为零。
        """
