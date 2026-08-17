"""可由普通 Python 程序直接调用的强类型 qmt-agent 客户端。"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import TypeVar

import httpx2
from pydantic import ValidationError

from qmt_protocol import (
    DividendType,
    ErrorResponse,
    HealthResponse,
    HistoryDownloadResponse,
    HistoryMode,
    HistoryQueryResponse,
    LatestQuotesResponse,
    MarketSubscriptionResponse,
    ProtocolModel,
    QuoteSequenceErrorResponse,
    QuoteSequenceResponse,
    SnapshotResponse,
    StockSubscriptionResponse,
    SubscriptionStatus,
    XtDataPeriod,
)

logger = logging.getLogger(__name__)
ResponseModel = TypeVar("ResponseModel", bound=ProtocolModel)


class QmtAgentError(RuntimeError):
    """qmt-agent 连接失败，或 HTTP 数据不符合共享协议。"""


class QuoteSequenceOutOfRange(QmtAgentError):
    """请求序号不在 qmt-agent 当前环形缓存中。"""

    def __init__(self, payload: QuoteSequenceErrorResponse) -> None:
        self.requested_seq = payload.requested_seq
        self.oldest_seq = payload.oldest_seq
        self.latest_seq = payload.latest_seq
        super().__init__(payload.detail)


class QmtAgentClient:
    """qmt-agent 全部业务接口的同步、运行时校验 Python 封装。"""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8765",
        *,
        timeout: float = 600,
        client: httpx2.Client | None = None,
    ) -> None:
        self._owns_client = client is None
        self._client = client or httpx2.Client(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            trust_env=False,
        )

    def health(self) -> HealthResponse:
        return self._request("GET", "/health", HealthResponse)

    def subscriptions(self) -> SubscriptionStatus:
        return self._request("GET", "/v1/subscriptions", SubscriptionStatus)

    def subscribe_markets(
        self, markets: Sequence[str] = ("SH", "SZ")
    ) -> MarketSubscriptionResponse:
        return self._request(
            "POST",
            "/v1/subscriptions/markets",
            MarketSubscriptionResponse,
            json={"markets": list(markets)},
        )

    def unsubscribe_markets(
        self, markets: Sequence[str] | None = None
    ) -> MarketSubscriptionResponse:
        body = None if markets is None else {"markets": list(markets)}
        return self._request(
            "DELETE",
            "/v1/subscriptions/markets",
            MarketSubscriptionResponse,
            json=body,
        )

    def subscribe_stocks(
        self, stocks: Sequence[str], period: XtDataPeriod
    ) -> StockSubscriptionResponse:
        return self._request(
            "POST",
            "/v1/subscriptions/stocks",
            StockSubscriptionResponse,
            json={"stocks": list(stocks), "period": period},
        )

    def unsubscribe_stocks(
        self, stocks: Sequence[str], period: XtDataPeriod
    ) -> StockSubscriptionResponse:
        return self._request(
            "DELETE",
            "/v1/subscriptions/stocks",
            StockSubscriptionResponse,
            json={"stocks": list(stocks), "period": period},
        )

    def market_snapshot(self, markets: Sequence[str] = ("SH", "SZ")) -> SnapshotResponse:
        return self._request(
            "POST",
            "/v1/snapshots/markets",
            SnapshotResponse,
            json={"markets": list(markets)},
        )

    def stock_snapshot(self, stocks: Sequence[str]) -> SnapshotResponse:
        return self._request(
            "POST",
            "/v1/snapshots/stocks",
            SnapshotResponse,
            json={"stocks": list(stocks)},
        )

    def market_quotes(self, stocks: Sequence[str] | None = None) -> LatestQuotesResponse:
        return self._optional_stocks_request("/v1/quotes/subscribed/markets", stocks)

    def stock_quotes(self, stocks: Sequence[str] | None = None) -> LatestQuotesResponse:
        return self._optional_stocks_request("/v1/quotes/subscribed/stocks", stocks)

    def quote_sequence(
        self,
        seq: int,
        limit: int = 1_000,
        stocks: Sequence[str] | None = None,
        wait_ms: int = 0,
    ) -> QuoteSequenceResponse:
        body: dict[str, object] = {"seq": seq, "limit": limit, "wait_ms": wait_ms}
        if stocks is not None:
            body["stocks"] = list(stocks)
        response = self._send("POST", "/v1/quotes/subscribed/sequence", json=body)
        if response.status_code == 416:
            raise QuoteSequenceOutOfRange(_decode_response(response, QuoteSequenceErrorResponse))
        return _successful_payload(response, QuoteSequenceResponse)

    def download_history(
        self,
        stocks: Sequence[str],
        period: XtDataPeriod = "1d",
        start_time: str = "",
        end_time: str = "",
        mode: HistoryMode = "incremental",
    ) -> HistoryDownloadResponse:
        return self._request(
            "POST",
            "/v1/history/download",
            HistoryDownloadResponse,
            json={
                "stocks": list(stocks),
                "period": period,
                "start_time": start_time,
                "end_time": end_time,
                "mode": mode,
            },
        )

    def query_history(
        self,
        stocks: Sequence[str],
        fields: Sequence[str] = (),
        period: XtDataPeriod = "1d",
        start_time: str = "",
        end_time: str = "",
        count: int = -1,
        dividend_type: DividendType = "none",
        fill_data: bool = True,
    ) -> HistoryQueryResponse:
        return self._request(
            "POST",
            "/v1/history/query",
            HistoryQueryResponse,
            json={
                "stocks": list(stocks),
                "fields": list(fields),
                "period": period,
                "start_time": start_time,
                "end_time": end_time,
                "count": count,
                "dividend_type": dividend_type,
                "fill_data": fill_data,
            },
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> QmtAgentClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _optional_stocks_request(
        self, path: str, stocks: Sequence[str] | None
    ) -> LatestQuotesResponse:
        body = None if stocks is None else {"stocks": list(stocks)}
        return self._request("POST", path, LatestQuotesResponse, json=body)

    def _request(
        self,
        method: str,
        path: str,
        model: type[ResponseModel],
        *,
        json: Mapping[str, object] | None = None,
    ) -> ResponseModel:
        return _successful_payload(self._send(method, path, json=json), model)

    def _send(
        self,
        method: str,
        path: str,
        *,
        json: Mapping[str, object] | None = None,
    ) -> httpx2.Response:
        try:
            return self._client.request(method, path, json=json)
        except httpx2.RequestError as exc:
            raise QmtAgentError(f"无法连接 qmt-agent：{exc}") from exc


def _successful_payload(response: httpx2.Response, model: type[ResponseModel]) -> ResponseModel:
    if not 200 <= response.status_code < 300:
        try:
            detail = _decode_response(response, ErrorResponse).detail
        except QmtAgentError:
            detail = response.text[:500]
        raise QmtAgentError(f"qmt-agent HTTP {response.status_code}: {detail}")
    return _decode_response(response, model)


def _decode_response(response: httpx2.Response, model: type[ResponseModel]) -> ResponseModel:
    try:
        # 直接从 JSON 字节校验，strict 模式仍可按 JSON 标准解析 datetime/date。
        return model.model_validate_json(response.content)
    except ValidationError as exc:
        extra_paths = [
            ".".join(str(part) for part in error["loc"])
            for error in exc.errors()
            if error["type"] == "extra_forbidden"
        ]
        if extra_paths:
            # 顶层协议的新字段不能被旧 receiver 静默丢弃，必须明确拒绝并记录。
            logger.debug(
                "qmt-agent 响应包含未定义字段，已拒绝而非丢弃：模型=%s，字段=%s",
                model.__name__,
                extra_paths,
            )
        logger.debug(
            "不符合 %s 的 qmt-agent 原始响应：%s",
            model.__name__,
            response.text[:5_000],
        )
        raise QmtAgentError(f"qmt-agent 返回结构不符合 {model.__name__}：{exc}") from exc
