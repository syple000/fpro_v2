"""策略声明调度规则，通过当前上下文读取数据和下单。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field

from backtest.broker import SimulatedBroker
from backtest.clock import Event
from backtest.domain import AccountSnapshot, Order, OrderRequest, Side
from backtest.schedule import Schedule
from market_data import DataView


@dataclass(frozen=True, slots=True)
class StrategyContext:
    """本次调用的数据与账户快照；提交订单不会立即改变该账户快照。"""

    data: DataView
    event: Event
    account: AccountSnapshot
    _broker: SimulatedBroker = field(repr=False)

    @property
    def pending_orders(self) -> tuple[Order, ...]:
        return self._broker.pending_orders

    def order(self, symbol: str, side: Side, quantity: int) -> Order:
        """按股数提交订单，在后续符合时间条件的 Bar 开盘价撮合。"""
        return self._broker.submit(OrderRequest(symbol, side, quantity), self.event.at)

    def cancel(self, order_id: str) -> bool:
        return self._broker.cancel(order_id, self.event.at)


class Strategy(ABC):
    """默认每根 Bar 调用；子类可声明其他调度规则。"""

    schedule: Schedule = Schedule()

    @abstractmethod
    def on_event(self, context: StrategyContext) -> Mapping[str, float] | None:
        """只在 schedule 触发时调用，不接收所有市场事件。

        返回完整目标权重，或直接通过 context 下单并返回 None。
        None 仅表示不做权重调仓，不代表没有提交数量订单。
        空字典表示清空持仓；非空字典中遗漏的已有持仓也以零为目标。
        """
