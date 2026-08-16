"""订阅状态、行情缓存和历史数据业务。"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from functools import partial
from threading import Lock
from typing import Any
from uuid import uuid4

from qmt_agent.gateway import MarketDataGateway, QmtGatewayError
from qmt_agent.quote_sequence import QuoteSequenceBuffer
from qmt_agent.subscription_callback import QuoteCallbackGate

logger = logging.getLogger(__name__)


class SubscriptionLimitError(ValueError):
    """列表订阅数量超过上限。"""


class SubscriptionPeriodConflictError(ValueError):
    """合约已使用其他周期订阅。"""


@dataclass(slots=True)
class _MarketSubscription:
    subscription_id: int
    callback: QuoteCallbackGate


@dataclass(slots=True)
class _StockSubscription:
    subscription_id: int
    period: str
    callback: QuoteCallbackGate


class QmtMarketService:
    """维护全市场全推订阅，以及逐合约、带周期的列表订阅。"""

    def __init__(
        self,
        gateway: MarketDataGateway,
        max_stock_subscriptions: int = 50,
        quote_buffer_capacity: int = 10_000,
    ) -> None:
        if not 1 <= max_stock_subscriptions <= 50:
            raise ValueError("列表订阅上限必须在 1 到 50 之间")
        self._gateway = gateway
        self._instance_id = uuid4().hex
        self._max_stock_subscriptions = max_stock_subscriptions
        self._market_subscriptions: dict[str, _MarketSubscription] = {}
        self._stock_subscriptions: dict[str, _StockSubscription] = {}
        self._market_operation_lock = Lock()
        self._stock_operation_lock = Lock()
        self._market_quote_lock = Lock()
        self._stock_quote_lock = Lock()
        self._market_quotes: dict[str, Any] = {}
        self._market_quote_updated_at: dict[str, str] = {}
        self._stock_quotes: dict[str, Any] = {}
        self._stock_quote_updated_at: dict[str, str] = {}
        self._quote_sequence = QuoteSequenceBuffer(quote_buffer_capacity)

    def subscribe_markets(self, markets: Iterable[str]) -> dict[str, Any]:
        requested = set(markets)
        with self._market_operation_lock:
            added = requested - self._market_subscriptions.keys()
            for market in sorted(added):
                self._clear_market_cache(market)
                callback = QuoteCallbackGate(
                    partial(self._on_market_quotes, market=market)
                )
                try:
                    subscription_id = self._gateway.subscribe_market_quote(
                        market, callback
                    )
                except Exception:
                    callback.close()
                    raise

                self._market_subscriptions[market] = _MarketSubscription(
                    subscription_id, callback
                )
                callback.activate()
            return self._market_subscription_result(added=added)

    def unsubscribe_markets(self, markets: Iterable[str] | None = None) -> dict[str, Any]:
        with self._market_operation_lock:
            current = set(self._market_subscriptions)
            requested = current if markets is None else set(markets)
            removed = current & requested
            missing = requested - current
            for market in sorted(removed):
                subscription = self._market_subscriptions[market]
                subscription.callback.suspend()
                try:
                    self._gateway.unsubscribe(subscription.subscription_id)
                except Exception:
                    subscription.callback.activate()
                    raise

                subscription.callback.close()
                del self._market_subscriptions[market]
            return self._market_subscription_result(removed=removed, missing=missing)

    def subscribe_stocks(self, stocks: Iterable[str], period: str) -> dict[str, Any]:
        requested = set(stocks)
        with self._stock_operation_lock:
            current = set(self._stock_subscriptions)
            target = current | requested
            if len(target) > self._max_stock_subscriptions:
                raise SubscriptionLimitError(
                    f"列表订阅总数不能超过 {self._max_stock_subscriptions}，"
                    f"当前 {len(current)}，请求后 {len(target)}"
                )

            conflicts = {
                stock: self._stock_subscriptions[stock].period
                for stock in requested & current
                if self._stock_subscriptions[stock].period != period
            }
            if conflicts:
                details = "，".join(
                    f"{stock} 当前为 {current_period}"
                    for stock, current_period in sorted(conflicts.items())
                )
                raise SubscriptionPeriodConflictError(
                    f"合约已使用其他周期订阅（{details}）；"
                    "请先按原周期显式取消，再订阅新周期"
                )

            added = requested - current
            for stock in sorted(added):
                with self._stock_quote_lock:
                    self._stock_quotes.pop(stock, None)
                    self._stock_quote_updated_at.pop(stock, None)
                callback = QuoteCallbackGate(
                    partial(self._on_stock_quotes, stock=stock, period=period)
                )
                try:
                    subscription_id = self._gateway.subscribe_stock_quote(
                        stock, period, callback
                    )
                except Exception:
                    callback.close()
                    raise

                self._stock_subscriptions[stock] = _StockSubscription(
                    subscription_id, period, callback
                )
                callback.activate()
            return self._stock_subscription_result(added=added)

    def unsubscribe_stocks(self, stocks: Iterable[str], period: str) -> dict[str, Any]:
        requested = set(stocks)
        with self._stock_operation_lock:
            current = set(self._stock_subscriptions)
            missing = requested - current
            period_mismatches = {
                stock: self._stock_subscriptions[stock].period
                for stock in requested & current
                if self._stock_subscriptions[stock].period != period
            }
            removed = (current & requested) - period_mismatches.keys()
            for stock in sorted(removed):
                subscription = self._stock_subscriptions[stock]
                subscription.callback.suspend()
                try:
                    self._gateway.unsubscribe(subscription.subscription_id)
                except Exception:
                    subscription.callback.activate()
                    raise

                subscription.callback.close()
                del self._stock_subscriptions[stock]
            return self._stock_subscription_result(
                removed=removed,
                missing=missing,
                period_mismatches=period_mismatches,
            )

    def get_market_snapshot(self, markets: Iterable[str]) -> dict[str, Any]:
        return self._gateway.get_full_tick(sorted(set(markets)))

    def get_stock_snapshot(self, stocks: Iterable[str]) -> dict[str, Any]:
        return self._gateway.get_full_tick(sorted(set(stocks)))

    def get_market_quotes(self, stocks: Iterable[str] | None = None) -> dict[str, Any]:
        """读取全市场订阅产生的最新 tick，不混入单股订阅缓存。"""
        requested = None if stocks is None else set(stocks)
        with self._market_operation_lock:
            subscribed_markets = set(self._market_subscriptions)

            if requested is None:
                not_subscribed: set[str] = set()
            else:
                not_subscribed = {
                    code
                    for code in requested
                    if code.rpartition(".")[2] not in subscribed_markets
                }

            with self._market_quote_lock:
                if requested is None:
                    data = {
                        code: quote
                        for code, quote in self._market_quotes.items()
                        if code.rpartition(".")[2] in subscribed_markets
                    }
                else:
                    allowed = requested - not_subscribed
                    data = {
                        code: self._market_quotes[code]
                        for code in sorted(allowed)
                        if code in self._market_quotes
                    }
                updated_at = {
                    code: self._market_quote_updated_at[code]
                    for code in data
                    if code in self._market_quote_updated_at
                }

        missing = set() if requested is None else requested - not_subscribed - data.keys()

        return {
            "data": dict(sorted(data.items())),
            "updated_at": updated_at,
            "periods": dict.fromkeys(sorted(data), "tick"),
            "missing": sorted(missing),
            "not_subscribed": sorted(not_subscribed),
        }

    def get_stock_quotes(self, stocks: Iterable[str] | None = None) -> dict[str, Any]:
        """读取单股订阅产生的最新行情，不混入全市场订阅缓存。"""
        requested = None if stocks is None else set(stocks)
        with self._stock_operation_lock:
            stock_periods = {
                stock: subscription.period
                for stock, subscription in self._stock_subscriptions.items()
            }
            if requested is None:
                allowed = set(stock_periods)
                not_subscribed: set[str] = set()
            else:
                not_subscribed = requested - stock_periods.keys()
                allowed = requested - not_subscribed

            with self._stock_quote_lock:
                data = {
                    code: self._stock_quotes[code]
                    for code in sorted(allowed)
                    if code in self._stock_quotes
                }
                updated_at = {
                    code: self._stock_quote_updated_at[code]
                    for code in data
                    if code in self._stock_quote_updated_at
                }

        return {
            "data": data,
            "updated_at": updated_at,
            "periods": {code: stock_periods[code] for code in data},
            "missing": sorted(allowed - data.keys()),
            "not_subscribed": sorted(not_subscribed),
        }

    def get_subscribed_quotes(self, stocks: Iterable[str] | None = None) -> dict[str, Any]:
        """兼容旧接口；单股订阅优先于同代码的全市场 tick。"""
        market_result = self.get_market_quotes(stocks)
        stock_result = self.get_stock_quotes(stocks)
        stock_owned = stock_result["data"].keys() | set(stock_result["missing"])

        data = {
            code: quote
            for code, quote in market_result["data"].items()
            if code not in stock_owned
        }
        data.update(stock_result["data"])
        updated_at = {
            code: timestamp
            for code, timestamp in market_result["updated_at"].items()
            if code not in stock_owned
        }
        updated_at.update(stock_result["updated_at"])
        periods = {
            code: period
            for code, period in market_result["periods"].items()
            if code not in stock_owned
        }
        periods.update(stock_result["periods"])

        return {
            "data": dict(sorted(data.items())),
            "updated_at": dict(sorted(updated_at.items())),
            "periods": dict(sorted(periods.items())),
            "missing": sorted(
                (set(market_result["missing"]) - stock_owned)
                | set(stock_result["missing"])
            ),
            "not_subscribed": sorted(
                set(market_result["not_subscribed"])
                & set(stock_result["not_subscribed"])
            ),
        }

    def get_subscribed_quote_sequence(
        self,
        seq: int,
        limit: int,
        stocks: Iterable[str] | None = None,
        wait_ms: int = 0,
    ) -> dict[str, Any]:
        """从指定序号开始读取一个连续窗口，并可在窗口内按合约筛选。"""
        return self._quote_sequence.read(seq, limit, stocks, wait_ms)

    def quote_sequence_status(self) -> dict[str, int | None]:
        return self._quote_sequence.status()

    def status(self) -> dict[str, Any]:
        with self._market_operation_lock, self._stock_operation_lock:
            markets = sorted(self._market_subscriptions)
            stocks = sorted(self._stock_subscriptions)
            return {
                "instance_id": self._instance_id,
                "markets": markets,
                "stocks": stocks,
                "stock_periods": {
                    stock: self._stock_subscriptions[stock].period for stock in stocks
                },
                "stock_count": len(stocks),
                "stock_limit": self._max_stock_subscriptions,
                "quote_sequence": self.quote_sequence_status(),
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
        with self._market_operation_lock, self._stock_operation_lock:
            for market, subscription in list(self._market_subscriptions.items()):
                subscription.callback.suspend()
                try:
                    self._gateway.unsubscribe(subscription.subscription_id)
                except QmtGatewayError:
                    logger.exception(
                        "退出时取消 %s 全推订阅失败，订阅号=%s",
                        market,
                        subscription.subscription_id,
                    )
                finally:
                    subscription.callback.close()
                    del self._market_subscriptions[market]

            for stock, subscription in list(self._stock_subscriptions.items()):
                subscription.callback.suspend()
                try:
                    self._gateway.unsubscribe(subscription.subscription_id)
                except QmtGatewayError:
                    logger.exception(
                        "退出时取消 %s 订阅失败，订阅号=%s",
                        stock,
                        subscription.subscription_id,
                    )
                finally:
                    subscription.callback.close()
                    del self._stock_subscriptions[stock]

    def _clear_market_cache(self, market: str) -> None:
        with self._market_quote_lock:
            stale_codes = [
                code
                for code in self._market_quotes
                if code.rpartition(".")[2] == market
            ]
            for code in stale_codes:
                self._market_quotes.pop(code, None)
                self._market_quote_updated_at.pop(code, None)

    def _on_market_quotes(
        self,
        quotes: dict[str, Any],
        received_at: str,
        *,
        market: str,
    ) -> None:
        """全市场回调的每个代码对应一条 tick，逐项写入缓存。"""
        if not isinstance(quotes, dict):
            logger.warning("忽略无法识别的全市场行情回调：%r", type(quotes))
            return

        normalized: dict[str, Any] = {}
        sequence_items: list[tuple[str, Any]] = []
        for code, quote in quotes.items():
            normalized_code = str(code).strip().upper()
            sequence_items.append((normalized_code, quote))
            normalized[normalized_code] = quote

        if not sequence_items:
            return

        self._quote_sequence.append(
            sequence_items,
            source="market",
            subscription=market,
            period="tick",
            received_at=received_at,
        )

        with self._market_quote_lock:
            self._market_quotes.update(normalized)
            self._market_quote_updated_at.update(
                dict.fromkeys(normalized, received_at)
            )

    def _on_stock_quotes(
        self,
        quotes: dict[str, Any],
        received_at: str,
        *,
        stock: str,
        period: str,
    ) -> None:
        """单股回调可能携带多条行情，逐条入队并单独维护最新值。"""
        if not isinstance(quotes, dict):
            logger.warning("忽略无法识别的单股行情回调：%r", type(quotes))
            return

        latest: dict[str, Any] = {}
        sequence_items: list[tuple[str, Any]] = []
        for code, quote_rows in quotes.items():
            normalized_code = str(code).strip().upper()
            if isinstance(quote_rows, (list, tuple)):
                if not quote_rows:
                    continue
                sequence_items.extend(
                    (normalized_code, quote) for quote in quote_rows
                )
                latest[normalized_code] = quote_rows[-1]
            else:
                # fake 网关和部分客户端版本可能直接给出单条字典。
                sequence_items.append((normalized_code, quote_rows))
                latest[normalized_code] = quote_rows

        if not sequence_items:
            return

        self._quote_sequence.append(
            sequence_items,
            source="stock",
            subscription=stock,
            period=period,
            received_at=received_at,
        )

        with self._stock_quote_lock:
            self._stock_quotes.update(latest)
            self._stock_quote_updated_at.update(
                dict.fromkeys(latest, received_at)
            )

    def _market_subscription_result(
        self,
        *,
        added: set[str] | None = None,
        removed: set[str] | None = None,
        missing: set[str] | None = None,
    ) -> dict[str, Any]:
        markets = sorted(self._market_subscriptions)
        return {
            "subscribed": markets,
            "added": sorted(added or set()),
            "removed": sorted(removed or set()),
            "not_found": sorted(missing or set()),
        }

    def _stock_subscription_result(
        self,
        *,
        added: set[str] | None = None,
        updated: set[str] | None = None,
        removed: set[str] | None = None,
        missing: set[str] | None = None,
        period_mismatches: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        stocks = sorted(self._stock_subscriptions)
        return {
            "periods": {
                stock: self._stock_subscriptions[stock].period for stock in stocks
            },
            "subscribed": stocks,
            "added": sorted(added or set()),
            "updated": sorted(updated or set()),
            "removed": sorted(removed or set()),
            "not_found": sorted(missing or set()),
            "period_mismatches": dict(sorted((period_mismatches or {}).items())),
        }
