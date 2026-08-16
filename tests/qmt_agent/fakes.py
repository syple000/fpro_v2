"""qmt-agent 测试共享的可控行情网关。"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from threading import RLock
from typing import Any

from qmt_agent.gateway import QmtGatewayError


class FakeGateway:
    """线程安全的内存网关，用于验证业务正确性而不是模拟 xtdata 细节。"""

    def __init__(self) -> None:
        self.next_id = 1
        self.active: dict[
            int, tuple[list[str], str | None, Callable[[dict[str, Any]], None]]
        ] = {}
        self.unsubscribed: list[int] = []
        self.fail_next_subscribe = False
        self.history_download: dict[str, Any] | None = None
        self._lock = RLock()

    def subscribe_market_quote(
        self, market: str, callback: Callable[[dict[str, Any]], None]
    ) -> int:
        with self._lock:
            if self.fail_next_subscribe:
                self.fail_next_subscribe = False
                raise QmtGatewayError("模拟订阅失败")
            subscription_id = self.next_id
            self.next_id += 1
            self.active[subscription_id] = ([market], None, callback)
            return subscription_id

    def subscribe_stock_quote(
        self,
        stock: str,
        period: str,
        callback: Callable[[dict[str, Any]], None],
    ) -> int:
        with self._lock:
            if self.fail_next_subscribe:
                self.fail_next_subscribe = False
                raise QmtGatewayError("模拟订阅失败")
            subscription_id = self.next_id
            self.next_id += 1
            self.active[subscription_id] = ([stock], period, callback)
            return subscription_id

    def unsubscribe(self, subscription_id: int) -> None:
        with self._lock:
            self.unsubscribed.append(subscription_id)
            self.active.pop(subscription_id, None)

    def get_full_tick(self, codes: Sequence[str]) -> dict[str, Any]:
        return {"000001.SZ": {"lastPrice": 10.5, "markets": list(codes)}}

    def download_history(
        self,
        stocks: Sequence[str],
        period: str,
        start_time: str,
        end_time: str,
        incrementally: bool,
    ) -> None:
        with self._lock:
            self.history_download = {
                "stocks": list(stocks),
                "period": period,
                "start_time": start_time,
                "end_time": end_time,
                "incrementally": incrementally,
            }

    def get_history(
        self,
        stocks: Sequence[str],
        fields: Sequence[str],
        period: str,
        start_time: str,
        end_time: str,
        count: int,
        dividend_type: str,
        fill_data: bool,
    ) -> Any:
        return {"close": {stock: [10.0] for stock in stocks}}

    def push(self, subscription_id: int, data: dict[str, Any]) -> None:
        with self._lock:
            callback = self.active[subscription_id][2]
        callback(data)

    def push_to_all(self, data: dict[str, Any]) -> None:
        with self._lock:
            callbacks = [callback for _, _, callback in self.active.values()]
        for callback in callbacks:
            callback(data)

    def active_codes(self) -> list[list[str]]:
        with self._lock:
            return [list(codes) for codes, _, _ in self.active.values()]

    def active_subscription_ids(self) -> dict[str, int]:
        """返回 fake 内部的代码到订阅号映射，供白盒测试驱动回调。"""
        with self._lock:
            return {
                codes[0]: subscription_id
                for subscription_id, (codes, _, _) in self.active.items()
            }

    def active_stock_periods(self) -> dict[str, str]:
        with self._lock:
            return {
                codes[0]: period
                for codes, period, _ in self.active.values()
                if period is not None
            }
