"""策略边界和只读账户快照。"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol

from backtest.errors import BacktestConfigurationError
from backtest.types import Position

if TYPE_CHECKING:
    from backtest.data import SessionData


@dataclass(frozen=True, slots=True)
class PositionView:
    symbol: str
    total_quantity: int
    sellable_quantity: int
    average_cost: float
    last_price: float | None
    market_value: float
    stale_price: bool


@dataclass(frozen=True, slots=True)
class PortfolioView:
    """策略可读、不可改的账户快照。"""

    cash: float
    total_equity: float
    positions: Mapping[str, PositionView]

    @classmethod
    def from_positions(
        cls,
        *,
        cash: float,
        total_equity: float,
        positions: Mapping[str, Position],
    ) -> PortfolioView:
        rows = {
            symbol: PositionView(
                symbol=symbol,
                total_quantity=position.total_quantity,
                sellable_quantity=position.sellable_quantity,
                average_cost=position.average_cost,
                last_price=position.last_price,
                market_value=position.market_value,
                stale_price=position.stale_price,
            )
            for symbol, position in positions.items()
            if position.total_quantity > 0
        }
        return cls(
            cash=cash,
            total_equity=total_equity,
            positions=MappingProxyType(rows),
        )


def validated_target_weights(weights: Mapping[str, float]) -> dict[str, float]:
    """校验一组完整目标权重并返回稳定排序的普通字典。"""

    normalized: dict[str, float] = {}
    for symbol, weight in weights.items():
        if not isinstance(symbol, str) or not symbol:
            raise BacktestConfigurationError("目标权重证券代码不能为空")
        if (
            isinstance(weight, bool)
            or not isinstance(weight, (int, float))
            or not math.isfinite(weight)
            or not 0 <= weight <= 1
        ):
            raise BacktestConfigurationError(f"{symbol} 目标权重必须位于 [0, 1]")
        normalized[symbol] = float(weight)
    if sum(normalized.values()) > 1.0 + 1e-9:
        raise BacktestConfigurationError("目标权重之和不能超过 1")
    return dict(sorted(normalized.items()))


class Strategy(Protocol):
    """收盘生成完整目标权重；返回 None 表示本日不调仓。"""

    def on_close(
        self,
        data: SessionData,
        portfolio: PortfolioView,
    ) -> Mapping[str, float] | None: ...
