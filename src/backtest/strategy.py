"""策略协议和只读上下文。"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from types import MappingProxyType
from typing import Protocol

from backtest.errors import BacktestConfigurationError
from backtest.types import Position


@dataclass(frozen=True, slots=True)
class PositionView:
    symbol: str
    total_quantity: int
    sellable_quantity: int
    average_cost: float
    last_price: float | None
    market_value: float
    stale_price: bool


class PortfolioView:
    """策略可读、不可改的账户快照。"""

    __slots__ = ("cash", "total_equity", "positions")

    def __init__(
        self,
        *,
        cash: float,
        total_equity: float,
        positions: Mapping[str, Position],
    ) -> None:
        self.cash = cash
        self.total_equity = total_equity
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
        self.positions = MappingProxyType(rows)


@dataclass(frozen=True, slots=True)
class RebalanceRequest:
    target_weights: Mapping[str, float]
    liquidate_omitted: bool


class StrategyContext:
    """引擎为每次回调创建的命令上下文。"""

    __slots__ = (
        "now",
        "session",
        "portfolio",
        "_rebalance_request",
        "_cancel_order_ids",
    )

    def __init__(self, *, now: datetime, portfolio: PortfolioView) -> None:
        self.now = now
        self.session = now.date()
        self.portfolio = portfolio
        self._rebalance_request: RebalanceRequest | None = None
        self._cancel_order_ids: list[str] = []

    def order_target_percent(self, symbol: str, weight: float) -> None:
        existing: dict[str, float] = {}
        if self._rebalance_request is not None:
            existing.update(self._rebalance_request.target_weights)
        existing[symbol] = weight
        self._rebalance_request = RebalanceRequest(
            target_weights=self._validated_weights(existing),
            liquidate_omitted=False,
        )

    def rebalance(self, target_weights: Mapping[str, float]) -> None:
        """提交一组完整目标权重；未出现的当前持仓目标为零。"""

        self._rebalance_request = RebalanceRequest(
            target_weights=self._validated_weights(target_weights),
            liquidate_omitted=True,
        )

    def cancel_order(self, order_id: str) -> None:
        if not isinstance(order_id, str) or not order_id:
            raise BacktestConfigurationError("order_id 不能为空")
        self._cancel_order_ids.append(order_id)

    @staticmethod
    def _validated_weights(weights: Mapping[str, float]) -> Mapping[str, float]:
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
        return MappingProxyType(dict(sorted(normalized.items())))


class BacktestData(Protocol):
    """策略所能看到的数据面。"""

    @property
    def session(self) -> date: ...

    @property
    def is_month_end(self) -> bool: ...

    def candidate_symbols(self) -> tuple[str, ...]: ...

    def momentum_return(self, symbol: str, *, lookback: int, skip: int) -> float | None: ...

    def close(self, symbol: str) -> float | None: ...


class Strategy(Protocol):
    """日频第一版的最小策略协议。"""

    strategy_id: str

    def initialize(self, context: StrategyContext) -> None: ...

    def on_pre_open(self, context: StrategyContext, data: BacktestData) -> None: ...

    def on_close(self, context: StrategyContext, data: BacktestData) -> None: ...
