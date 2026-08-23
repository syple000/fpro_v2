"""订阅生命周期、最新值缓存和顺序缓存。"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from functools import partial
from threading import Lock
from uuid import uuid4

from qmt_agent.gateway import (
    MarketDataGateway,
    MarketQuotePush,
    QmtGatewayError,
    StockQuotePush,
)
from qmt_agent.quote_sequence import QuoteSequenceBuffer
from qmt_agent.subscription_callback import QuoteCallbackGate
from qmt_protocol import (
    DividendFactorsResponse,
    DividendType,
    FinancialDownloadResponse,
    FinancialQueryResponse,
    FinancialReportType,
    FinancialTable,
    HistoryDownloadResponse,
    HistoryQueryResponse,
    LatestQuotesResponse,
    MarketSubscriptionResponse,
    QuotePayload,
    QuoteSequenceResponse,
    QuoteSequenceStatus,
    SnapshotResponse,
    StockSubscriptionResponse,
    SubscriptionStatus,
    TickQuote,
    XtDataPeriod,
)

logger = logging.getLogger(__name__)


class SubscriptionLimitError(ValueError):
    """列表订阅数量超过上限。"""


class SubscriptionPeriodConflictError(ValueError):
    """合约已使用其他周期订阅。"""


@dataclass(slots=True)
class _MarketSubscription:
    subscription_id: int
    callback: QuoteCallbackGate[MarketQuotePush]


@dataclass(slots=True)
class _StockSubscription:
    subscription_id: int
    period: XtDataPeriod
    callback: QuoteCallbackGate[StockQuotePush]


class QmtMarketService:
    """把 XtData 订阅维护成可读取的内存缓存。"""

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

        # 缓存按订阅隔离。XtData 回调中的代码原样保留，退订时只需删除对应订阅，
        # 不需要根据证券代码后缀或请求代码再次过滤。
        self._market_quotes: dict[str, dict[str, TickQuote]] = {}
        self._market_quote_updated_at: dict[str, dict[str, int]] = {}
        self._stock_quotes: dict[str, dict[str, QuotePayload]] = {}
        self._stock_quote_updated_at: dict[str, dict[str, int]] = {}
        self._market_quote_lock = Lock()
        self._stock_quote_lock = Lock()
        self._quote_sequence = QuoteSequenceBuffer(quote_buffer_capacity)

    def subscribe_markets(self, markets: Iterable[str]) -> MarketSubscriptionResponse:
        requested = set(markets)
        with self._market_operation_lock:
            added = requested - self._market_subscriptions.keys()
            for market in sorted(added):
                # 重建同一市场订阅时不能返回上一次订阅留下的缓存。
                self._clear_market_cache(market)
                callback = QuoteCallbackGate(partial(self._on_market_quotes, market=market))
                try:
                    subscription_id = self._gateway.subscribe_market_quote(market, callback)
                except Exception:
                    callback.close()
                    raise
                self._market_subscriptions[market] = _MarketSubscription(subscription_id, callback)
                callback.activate()
            return self._market_subscription_result(added=added)

    def unsubscribe_markets(
        self, markets: Iterable[str] | None = None
    ) -> MarketSubscriptionResponse:
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
                self._clear_market_cache(market)
            return self._market_subscription_result(removed=removed, missing=missing)

    def subscribe_stocks(
        self, stocks: Iterable[str], period: XtDataPeriod
    ) -> StockSubscriptionResponse:
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
                    f"{stock} 当前为 {active_period}"
                    for stock, active_period in sorted(conflicts.items())
                )
                raise SubscriptionPeriodConflictError(
                    f"合约已使用其他周期订阅（{details}）；请先按原周期显式取消，再订阅新周期"
                )

            added = requested - current
            for stock in sorted(added):
                self._clear_stock_cache(stock)
                callback = QuoteCallbackGate(
                    partial(self._on_stock_quotes, stock=stock, period=period)
                )
                try:
                    subscription_id = self._gateway.subscribe_stock_quote(stock, period, callback)
                except Exception:
                    callback.close()
                    raise
                self._stock_subscriptions[stock] = _StockSubscription(
                    subscription_id, period, callback
                )
                callback.activate()
            return self._stock_subscription_result(added=added)

    def unsubscribe_stocks(
        self, stocks: Iterable[str], period: XtDataPeriod
    ) -> StockSubscriptionResponse:
        requested = set(stocks)
        with self._stock_operation_lock:
            current = set(self._stock_subscriptions)
            missing = requested - current
            period_mismatches: dict[str, XtDataPeriod] = {
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
                self._clear_stock_cache(stock)
            return self._stock_subscription_result(
                removed=removed,
                missing=missing,
                period_mismatches=period_mismatches,
            )

    def get_market_snapshot(self, markets: Iterable[str]) -> SnapshotResponse:
        data = self._gateway.get_full_tick(list(markets))
        return SnapshotResponse(data=data, count=len(data))

    def get_stock_snapshot(self, stocks: Iterable[str]) -> SnapshotResponse:
        data = self._gateway.get_full_tick(list(stocks))
        return SnapshotResponse(data=data, count=len(data))

    def get_market_quotes(self) -> LatestQuotesResponse:
        """返回全市场订阅的完整缓存，不做代码或市场过滤。"""
        with self._market_quote_lock:
            data: dict[str, QuotePayload] = {}
            updated_at: dict[str, int] = {}
            for market in self._market_quotes:
                data.update(self._market_quotes[market])
                updated_at.update(self._market_quote_updated_at[market])
        periods: dict[str, XtDataPeriod] = {code: "tick" for code in data}
        return LatestQuotesResponse(
            data=data,
            periods=periods,
            updated_at=updated_at,
        )

    def get_stock_quotes(self) -> LatestQuotesResponse:
        """返回单股订阅的完整缓存，不做代码过滤。"""
        with self._stock_operation_lock, self._stock_quote_lock:
            data: dict[str, QuotePayload] = {}
            periods: dict[str, XtDataPeriod] = {}
            updated_at: dict[str, int] = {}
            for stock, subscription in self._stock_subscriptions.items():
                cached_quotes = self._stock_quotes.get(stock, {})
                data.update(cached_quotes)
                updated_at.update(self._stock_quote_updated_at.get(stock, {}))
                for code in cached_quotes:
                    periods[code] = subscription.period
        return LatestQuotesResponse(data=data, periods=periods, updated_at=updated_at)

    def get_subscribed_quotes(self) -> LatestQuotesResponse:
        """兼容合并读取；同代码的单股订阅覆盖全市场 tick。"""
        market = self.get_market_quotes()
        stock = self.get_stock_quotes()
        data = {**market.data, **stock.data}
        periods: dict[str, XtDataPeriod] = {**market.periods, **stock.periods}
        updated_at = {**market.updated_at, **stock.updated_at}
        return LatestQuotesResponse(data=data, periods=periods, updated_at=updated_at)

    def get_subscribed_quote_sequence(
        self,
        seq: int,
        limit: int,
        wait_ms: int = 0,
    ) -> QuoteSequenceResponse:
        """从指定序号读取连续窗口，不对缓存内容做筛选。"""
        return self._quote_sequence.read(seq, limit, wait_ms=wait_ms)

    def quote_sequence_status(self) -> QuoteSequenceStatus:
        return self._quote_sequence.status()

    def status(self) -> SubscriptionStatus:
        with self._market_operation_lock, self._stock_operation_lock:
            markets = sorted(self._market_subscriptions)
            stocks = sorted(self._stock_subscriptions)
            return SubscriptionStatus(
                instance_id=self._instance_id,
                markets=markets,
                stocks=stocks,
                stock_periods={stock: self._stock_subscriptions[stock].period for stock in stocks},
                stock_count=len(stocks),
                stock_limit=self._max_stock_subscriptions,
                quote_sequence=self.quote_sequence_status(),
            )

    def download_history(
        self,
        stocks: list[str],
        period: XtDataPeriod,
        start_time: str,
        end_time: str,
        incrementally: bool,
    ) -> HistoryDownloadResponse:
        return self._gateway.download_history(stocks, period, start_time, end_time, incrementally)

    def get_history(
        self,
        stocks: list[str],
        fields: list[str],
        period: XtDataPeriod,
        start_time: str,
        end_time: str,
        count: int,
        dividend_type: DividendType,
        fill_data: bool,
    ) -> HistoryQueryResponse:
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

    def download_financial(
        self,
        stocks: list[str],
        tables: list[FinancialTable],
        start_time: str,
        end_time: str,
    ) -> FinancialDownloadResponse:
        return self._gateway.download_financial(stocks, tables, start_time, end_time)

    def get_financial(
        self,
        stocks: list[str],
        tables: list[FinancialTable],
        start_time: str,
        end_time: str,
        report_type: FinancialReportType,
    ) -> FinancialQueryResponse:
        return self._gateway.get_financial(stocks, tables, start_time, end_time, report_type)

    def get_dividend_factors(
        self,
        stocks: list[str],
        start_time: str,
        end_time: str,
    ) -> DividendFactorsResponse:
        return self._gateway.get_dividend_factors(stocks, start_time, end_time)

    def close(self) -> None:
        """进程退出时尽力释放全部订阅。"""
        with self._market_operation_lock, self._stock_operation_lock:
            for market, subscription in list(self._market_subscriptions.items()):
                self._close_subscription(
                    market, subscription.subscription_id, subscription.callback
                )
                del self._market_subscriptions[market]
            for stock, subscription in list(self._stock_subscriptions.items()):
                self._close_subscription(stock, subscription.subscription_id, subscription.callback)
                del self._stock_subscriptions[stock]

    def _close_subscription(
        self,
        name: str,
        subscription_id: int,
        callback: QuoteCallbackGate[MarketQuotePush] | QuoteCallbackGate[StockQuotePush],
    ) -> None:
        callback.suspend()
        try:
            self._gateway.unsubscribe(subscription_id)
        except QmtGatewayError:
            logger.exception("退出时取消 %s 订阅失败，订阅号=%s", name, subscription_id)
        finally:
            callback.close()

    def _clear_market_cache(self, market: str) -> None:
        with self._market_quote_lock:
            self._market_quotes.pop(market, None)
            self._market_quote_updated_at.pop(market, None)

    def _clear_stock_cache(self, stock: str) -> None:
        with self._stock_quote_lock:
            self._stock_quotes.pop(stock, None)
            self._stock_quote_updated_at.pop(stock, None)

    def _on_market_quotes(
        self,
        quotes: MarketQuotePush,
        received_at: int,
        *,
        market: str,
    ) -> None:
        if not quotes:
            return
        # 顺序缓存保留每条回调；最新值缓存保留 XtData 原始映射中的最后状态。
        self._quote_sequence.append(
            quotes.items(),
            source="market",
            subscription=market,
            period="tick",
            received_at=received_at,
        )
        with self._market_quote_lock:
            self._market_quotes.setdefault(market, {}).update(quotes)
            self._market_quote_updated_at.setdefault(market, {}).update(
                dict.fromkeys(quotes, received_at)
            )

    def _on_stock_quotes(
        self,
        quotes: StockQuotePush,
        received_at: int,
        *,
        stock: str,
        period: XtDataPeriod,
    ) -> None:
        items = [(code, quote) for code, rows in quotes.items() for quote in rows]
        if not items:
            return
        self._quote_sequence.append(
            items,
            source="stock",
            subscription=stock,
            period=period,
            received_at=received_at,
        )
        latest = {code: rows[-1] for code, rows in quotes.items() if rows}
        with self._stock_quote_lock:
            self._stock_quotes.setdefault(stock, {}).update(latest)
            self._stock_quote_updated_at.setdefault(stock, {}).update(
                dict.fromkeys(latest, received_at)
            )

    def _market_subscription_result(
        self,
        *,
        added: set[str] | None = None,
        removed: set[str] | None = None,
        missing: set[str] | None = None,
    ) -> MarketSubscriptionResponse:
        return MarketSubscriptionResponse(
            subscribed=sorted(self._market_subscriptions),
            added=sorted(added or set()),
            removed=sorted(removed or set()),
            not_found=sorted(missing or set()),
        )

    def _stock_subscription_result(
        self,
        *,
        added: set[str] | None = None,
        updated: set[str] | None = None,
        removed: set[str] | None = None,
        missing: set[str] | None = None,
        period_mismatches: dict[str, XtDataPeriod] | None = None,
    ) -> StockSubscriptionResponse:
        stocks = sorted(self._stock_subscriptions)
        return StockSubscriptionResponse(
            periods={stock: self._stock_subscriptions[stock].period for stock in stocks},
            subscribed=stocks,
            added=sorted(added or set()),
            updated=sorted(updated or set()),
            removed=sorted(removed or set()),
            not_found=sorted(missing or set()),
            period_mismatches=dict(sorted((period_mismatches or {}).items())),
        )
