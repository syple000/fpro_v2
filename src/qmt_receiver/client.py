"""可由普通 Python 程序直接调用的 qmt-agent 客户端。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import httpx2


class QmtAgentError(RuntimeError):
    """qmt-agent 连接失败或返回非成功状态。"""


class QuoteSequenceOutOfRange(QmtAgentError):
    """请求序号不在 qmt-agent 当前环形缓存中。"""

    def __init__(self, payload: dict[str, Any]) -> None:
        self.requested_seq = _optional_int(payload.get("requested_seq"))
        self.oldest_seq = _optional_int(payload.get("oldest_seq"))
        self.latest_seq = _optional_int(payload.get("latest_seq"))
        super().__init__(str(payload.get("detail", "行情序号越界")))


class QmtAgentClient:
    """qmt-agent 全部业务接口的同步 Python 封装。"""

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

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/health")

    def subscriptions(self) -> dict[str, Any]:
        return self._request("GET", "/v1/subscriptions")

    def subscribe_markets(self, markets: Sequence[str] = ("SH", "SZ")) -> dict[str, Any]:
        return self._request("POST", "/v1/subscriptions/markets", json={"markets": list(markets)})

    def unsubscribe_markets(self, markets: Sequence[str] | None = None) -> dict[str, Any]:
        kwargs = {} if markets is None else {"json": {"markets": list(markets)}}
        return self._request("DELETE", "/v1/subscriptions/markets", **kwargs)

    def subscribe_stocks(self, stocks: Sequence[str], period: str) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/subscriptions/stocks",
            json={"stocks": list(stocks), "period": period},
        )

    def unsubscribe_stocks(self, stocks: Sequence[str], period: str) -> dict[str, Any]:
        return self._request(
            "DELETE",
            "/v1/subscriptions/stocks",
            json={"stocks": list(stocks), "period": period},
        )

    def market_snapshot(self, markets: Sequence[str] = ("SH", "SZ")) -> dict[str, Any]:
        return self._request("POST", "/v1/snapshots/markets", json={"markets": list(markets)})

    def stock_snapshot(self, stocks: Sequence[str]) -> dict[str, Any]:
        return self._request("POST", "/v1/snapshots/stocks", json={"stocks": list(stocks)})

    def market_quotes(self, stocks: Sequence[str] | None = None) -> dict[str, Any]:
        return self._optional_stocks_request("/v1/quotes/subscribed/markets", stocks)

    def stock_quotes(self, stocks: Sequence[str] | None = None) -> dict[str, Any]:
        return self._optional_stocks_request("/v1/quotes/subscribed/stocks", stocks)

    def quote_sequence(
        self,
        seq: int,
        limit: int = 1_000,
        stocks: Sequence[str] | None = None,
        wait_ms: int = 0,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"seq": seq, "limit": limit, "wait_ms": wait_ms}
        if stocks is not None:
            body["stocks"] = list(stocks)
        response = self._send("POST", "/v1/quotes/subscribed/sequence", json=body)
        if response.status_code == 416:
            raise QuoteSequenceOutOfRange(_response_dict(response))
        return _successful_payload(response)

    def download_history(
        self,
        stocks: Sequence[str],
        period: str = "1d",
        start_time: str = "",
        end_time: str = "",
        mode: str = "incremental",
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/history/download",
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
        period: str = "1d",
        start_time: str = "",
        end_time: str = "",
        count: int = -1,
        dividend_type: str = "none",
        fill_data: bool = True,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/history/query",
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

    def _optional_stocks_request(self, path: str, stocks: Sequence[str] | None) -> dict[str, Any]:
        kwargs = {} if stocks is None else {"json": {"stocks": list(stocks)}}
        return self._request("POST", path, **kwargs)

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        return _successful_payload(self._send(method, path, **kwargs))

    def _send(self, method: str, path: str, **kwargs: Any) -> httpx2.Response:
        try:
            return self._client.request(method, path, **kwargs)
        except httpx2.RequestError as exc:
            raise QmtAgentError(f"无法连接 qmt-agent：{exc}") from exc


def _successful_payload(response: httpx2.Response) -> dict[str, Any]:
    if not 200 <= response.status_code < 300:
        payload = _response_dict(response)
        detail = payload.get("detail", response.text[:500])
        raise QmtAgentError(f"qmt-agent HTTP {response.status_code}: {detail}")
    return _response_dict(response)


def _response_dict(response: httpx2.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise QmtAgentError("qmt-agent 返回的不是 JSON") from exc
    if not isinstance(payload, dict):
        raise QmtAgentError("qmt-agent 返回的 JSON 不是对象")
    return payload


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)
