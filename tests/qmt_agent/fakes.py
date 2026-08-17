"""qmt-agent 测试共享的可控行情网关。"""

from __future__ import annotations

from collections.abc import Sequence
from threading import RLock
from typing import Any, cast

from qmt_agent.gateway import (
    MarketQuoteCallback,
    QmtGatewayError,
    StockQuoteCallback,
)
from qmt_protocol import (
    DividendType,
    HistoryFrame,
    QuotePayload,
    TickQuote,
    XtDataPeriod,
    validate_quote,
)


class FakeGateway:
    """线程安全的内存网关，用于验证业务正确性而不是模拟 xtdata 细节。"""

    def __init__(self) -> None:
        self.next_id = 1
        self.active: dict[
            int,
            tuple[
                list[str],
                XtDataPeriod | None,
                MarketQuoteCallback | StockQuoteCallback,
            ],
        ] = {}
        self.unsubscribed: list[int] = []
        self.fail_next_subscribe = False
        self.history_download: dict[str, Any] | None = None
        self._lock = RLock()

    def subscribe_market_quote(self, market: str, callback: MarketQuoteCallback) -> int:
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
        period: XtDataPeriod,
        callback: StockQuoteCallback,
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

    def get_full_tick(self, codes: Sequence[str]) -> dict[str, TickQuote]:
        return {"000001.SZ": TickQuote.model_validate({"lastPrice": 10.5, "markets": list(codes)})}

    def download_history(
        self,
        stocks: Sequence[str],
        period: XtDataPeriod,
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
        period: XtDataPeriod,
        start_time: str,
        end_time: str,
        count: int,
        dividend_type: DividendType,
        fill_data: bool,
    ) -> dict[str, HistoryFrame]:
        columns = list(fields) or ["close"]
        return {
            stock: HistoryFrame(
                index=[20250101],
                columns=columns,
                data=[[10.0 for _ in columns]],
            )
            for stock in stocks
        }

    def push(self, subscription_id: int, data: dict[str, Any]) -> None:
        with self._lock:
            _, period, callback = self.active[subscription_id]
        if period is None:
            market_callback = cast(MarketQuoteCallback, callback)
            market_callback({code: TickQuote.model_validate(quote) for code, quote in data.items()})
            return

        stock_callback = cast(StockQuoteCallback, callback)
        rows: dict[str, list[QuotePayload]] = {}
        for code, raw_rows in data.items():
            source_rows = raw_rows if isinstance(raw_rows, (list, tuple)) else [raw_rows]
            rows[code] = [validate_quote(period, quote) for quote in source_rows]
        stock_callback(rows)

    def push_to_all(self, data: dict[str, Any]) -> None:
        with self._lock:
            subscription_ids = list(self.active)
        for subscription_id in subscription_ids:
            self.push(subscription_id, data)

    def active_codes(self) -> list[list[str]]:
        with self._lock:
            return [list(codes) for codes, _, _ in self.active.values()]

    def active_subscription_ids(self) -> dict[str, int]:
        """返回 fake 内部的代码到订阅号映射，供白盒测试驱动回调。"""
        with self._lock:
            return {
                codes[0]: subscription_id for subscription_id, (codes, _, _) in self.active.items()
            }

    def active_stock_periods(self) -> dict[str, str]:
        with self._lock:
            return {
                codes[0]: period for codes, period, _ in self.active.values() if period is not None
            }
