"""订阅状态、行情缓存和历史数据业务。"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from threading import RLock
from typing import Any

from qmt_agent.gateway import MarketDataGateway, QmtGatewayError

logger = logging.getLogger(__name__)


class SubscriptionLimitError(ValueError):
    """列表订阅数量超过上限。"""


@dataclass(slots=True)
class _SubscriptionSlot:
    subscription_id: int | None = None
    codes: set[str] = field(default_factory=set)


class QmtMarketService:
    """维护两个订阅：全市场订阅和不超过 300 个合约的列表订阅。"""

    def __init__(self, gateway: MarketDataGateway, max_stock_subscriptions: int = 300) -> None:
        if not 1 <= max_stock_subscriptions <= 300:
            raise ValueError("列表订阅上限必须在 1 到 300 之间")

        self._gateway = gateway
        self._max_stock_subscriptions = max_stock_subscriptions
        self._market_slot = _SubscriptionSlot()
        self._stock_slot = _SubscriptionSlot()
        self._operation_lock = RLock()
        self._quote_lock = RLock()
        self._latest_quotes: dict[str, Any] = {}
        self._quote_updated_at: dict[str, str] = {}

    def subscribe_markets(self, markets: Iterable[str]) -> dict[str, Any]:
        requested = set(markets)
        with self._operation_lock:
            current = set(self._market_slot.codes)
            target = current | requested
            self._replace_subscription(self._market_slot, target, "全市场")
            return self._subscription_result(self._market_slot, target - current, set())

    def unsubscribe_markets(self, markets: Iterable[str] | None = None) -> dict[str, Any]:
        with self._operation_lock:
            current = set(self._market_slot.codes)
            requested = current if markets is None else set(markets)
            removed = current & requested
            missing = requested - current
            self._replace_subscription(self._market_slot, current - requested, "全市场")
            return self._subscription_result(self._market_slot, set(), removed, missing)

    def subscribe_stocks(self, stocks: Iterable[str]) -> dict[str, Any]:
        requested = set(stocks)
        with self._operation_lock:
            current = set(self._stock_slot.codes)
            target = current | requested
            if len(target) > self._max_stock_subscriptions:
                raise SubscriptionLimitError(
                    f"列表订阅总数不能超过 {self._max_stock_subscriptions}，"
                    f"当前 {len(current)}，请求后 {len(target)}"
                )
            self._replace_subscription(self._stock_slot, target, "列表")
            return self._subscription_result(self._stock_slot, target - current, set())

    def unsubscribe_stocks(self, stocks: Iterable[str]) -> dict[str, Any]:
        requested = set(stocks)
        with self._operation_lock:
            current = set(self._stock_slot.codes)
            removed = current & requested
            missing = requested - current
            self._replace_subscription(self._stock_slot, current - requested, "列表")
            return self._subscription_result(self._stock_slot, set(), removed, missing)

    def get_market_snapshot(self, markets: Iterable[str]) -> dict[str, Any]:
        return self._gateway.get_full_tick(sorted(set(markets)))

    def get_stock_snapshot(self, stocks: Iterable[str]) -> dict[str, Any]:
        return self._gateway.get_full_tick(sorted(set(stocks)))

    def get_subscribed_quotes(self, stocks: Iterable[str] | None = None) -> dict[str, Any]:
        with self._operation_lock:
            subscribed_stocks = set(self._stock_slot.codes)
            subscribed_markets = set(self._market_slot.codes)

        with self._quote_lock:
            if stocks is None:
                requested = {
                    code
                    for code in self._latest_quotes
                    if self._is_subscribed(code, subscribed_stocks, subscribed_markets)
                }
                not_subscribed: set[str] = set()
            else:
                requested = set(stocks)
                not_subscribed = {
                    code
                    for code in requested
                    if not self._is_subscribed(code, subscribed_stocks, subscribed_markets)
                }

            allowed = requested - not_subscribed
            data = {
                code: self._latest_quotes[code]
                for code in sorted(allowed)
                if code in self._latest_quotes
            }
            updated_at = {
                code: self._quote_updated_at[code]
                for code in data
                if code in self._quote_updated_at
            }

        missing = allowed - data.keys()
        return {
            "data": data,
            "updated_at": updated_at,
            "missing": sorted(missing),
            "not_subscribed": sorted(not_subscribed),
        }

    @staticmethod
    def _is_subscribed(
        code: str, subscribed_stocks: set[str], subscribed_markets: set[str]
    ) -> bool:
        if code in subscribed_stocks:
            return True
        _, separator, market = code.rpartition(".")
        return bool(separator) and market in subscribed_markets

    def status(self) -> dict[str, Any]:
        with self._operation_lock:
            return {
                "markets": sorted(self._market_slot.codes),
                "stocks": sorted(self._stock_slot.codes),
                "stock_count": len(self._stock_slot.codes),
                "stock_limit": self._max_stock_subscriptions,
            }

    def download_history(
        self,
        stocks: list[str],
        period: str,
        start_time: str,
        end_time: str,
        incrementally: bool,
    ) -> dict[str, Any]:
        self._gateway.download_history(
            stocks, period, start_time, end_time, incrementally=incrementally
        )
        return {
            "stocks": stocks,
            "period": period,
            "mode": "incremental" if incrementally else "full",
            "completed": True,
        }

    def get_history(
        self,
        stocks: list[str],
        fields: list[str],
        period: str,
        start_time: str,
        end_time: str,
        count: int,
        dividend_type: str,
        fill_data: bool,
    ) -> Any:
        return self._gateway.get_history(
            stocks,
            fields,
            period,
            start_time,
            end_time,
            count,
            dividend_type,
            fill_data,
        )

    def close(self) -> None:
        """进程退出时尽力释放订阅；失败也不阻碍退出。"""
        with self._operation_lock:
            for slot in (self._market_slot, self._stock_slot):
                if slot.subscription_id is None:
                    continue
                try:
                    self._gateway.unsubscribe(slot.subscription_id)
                except QmtGatewayError:
                    logger.exception("退出时取消订阅失败，订阅号=%s", slot.subscription_id)
                finally:
                    slot.subscription_id = None
                    slot.codes.clear()

    def _replace_subscription(
        self, slot: _SubscriptionSlot, target: set[str], name: str
    ) -> None:
        """运行中替换订阅；新订阅失败时尽力恢复原订阅。"""
        current = set(slot.codes)
        if current == target:
            return

        old_subscription_id = slot.subscription_id
        if old_subscription_id is not None:
            self._gateway.unsubscribe(old_subscription_id)

        if not target:
            slot.subscription_id = None
            slot.codes.clear()
            return

        try:
            new_subscription_id = self._gateway.subscribe_full_quote(
                sorted(target), self._on_quotes
            )
        except QmtGatewayError as switch_error:
            if old_subscription_id is None or not current:
                raise

            try:
                restored_id = self._gateway.subscribe_full_quote(
                    sorted(current), self._on_quotes
                )
            except QmtGatewayError as restore_error:
                slot.subscription_id = None
                slot.codes.clear()
                raise QmtGatewayError(
                    f"{name}订阅热切换失败，恢复原订阅也失败：{restore_error}"
                ) from switch_error

            slot.subscription_id = restored_id
            slot.codes = current
            raise QmtGatewayError(f"{name}订阅热切换失败，已恢复原订阅") from switch_error

        slot.subscription_id = new_subscription_id
        slot.codes = set(target)

    def _on_quotes(self, quotes: dict[str, Any]) -> None:
        """xtdata 可能从自己的线程回调，所以这里只做很短的内存写入。"""
        if not isinstance(quotes, dict):
            logger.warning("忽略无法识别的行情回调：%r", type(quotes))
            return

        now = datetime.now(UTC).isoformat()
        with self._quote_lock:
            for code, quote in quotes.items():
                # subscribe_quote 返回列表，全推接口返回单条字典；两种形式都兼容。
                if isinstance(quote, (list, tuple)):
                    if not quote:
                        continue
                    quote = quote[-1]
                normalized_code = str(code).strip().upper()
                self._latest_quotes[normalized_code] = quote
                self._quote_updated_at[normalized_code] = now

    @staticmethod
    def _subscription_result(
        slot: _SubscriptionSlot,
        added: set[str],
        removed: set[str],
        missing: set[str] | None = None,
    ) -> dict[str, Any]:
        return {
            "subscription_id": slot.subscription_id,
            "subscribed": sorted(slot.codes),
            "added": sorted(added),
            "removed": sorted(removed),
            "not_found": sorted(missing or set()),
        }
