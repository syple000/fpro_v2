from __future__ import annotations

import httpx2
import pytest

from qmt_protocol import HealthResponse
from qmt_receiver.client import (
    QmtAgentClient,
    QmtAgentError,
    QuoteSequenceOutOfRange,
)

STATUS = {
    "instance_id": "test-instance",
    "markets": [],
    "stocks": [],
    "stock_periods": {},
    "stock_count": 0,
    "stock_limit": 50,
    "quote_sequence": {
        "oldest_seq": None,
        "latest_seq": None,
        "next_seq": 1,
        "size": 0,
        "capacity": 10_000,
    },
}


def response_payload(request: httpx2.Request) -> dict[str, object]:
    path = request.url.path
    if path == "/health":
        return {"status": "ok", "version": "0.1.0", **STATUS}
    if path == "/v1/subscriptions" and request.method == "GET":
        return STATUS
    if path == "/v1/subscriptions/markets":
        return {"subscribed": [], "added": [], "removed": [], "not_found": []}
    if path == "/v1/subscriptions/stocks":
        return {
            "periods": {},
            "subscribed": [],
            "added": [],
            "updated": [],
            "removed": [],
            "not_found": [],
            "period_mismatches": {},
        }
    if path.startswith("/v1/snapshots/"):
        return {"data": {}, "count": 0}
    if path in {
        "/v1/quotes/subscribed/markets",
        "/v1/quotes/subscribed/stocks",
    }:
        return {
            "data": {},
            "updated_at": {},
            "periods": {},
        }
    if path == "/v1/quotes/subscribed/sequence":
        return {
            "data": [],
            "count": 0,
            "requested_seq": 1,
            "next_seq": 2,
            "oldest_seq": 1,
            "latest_seq": 1,
        }
    if path == "/v1/history/download":
        return {"completed": True}
    if path == "/v1/history/query":
        return {"period": "1d", "data": {}}
    if path == "/v1/financial/download":
        return {"completed": True}
    if path == "/v1/financial/query":
        return {"data": {}}
    if path == "/v1/dividend-factors/query":
        return {"data": {}}
    raise AssertionError(f"未处理的测试路径：{request.method} {path}")


def test_default_client_does_not_read_proxy_environment(monkeypatch) -> None:
    options: dict[str, object] = {}
    sentinel = object()

    def create_http_client(**kwargs: object) -> object:
        options.update(kwargs)
        return sentinel

    monkeypatch.setattr(httpx2, "Client", create_http_client)

    client = QmtAgentClient()

    assert client._client is sentinel
    assert options["trust_env"] is False


def test_client_exposes_all_qmt_agent_business_interfaces() -> None:
    requests: list[tuple[str, str]] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append((request.method, request.url.path))
        return httpx2.Response(200, json=response_payload(request))

    http_client = httpx2.Client(
        transport=httpx2.MockTransport(handler), base_url="http://qmt-agent"
    )
    client = QmtAgentClient(client=http_client)
    assert isinstance(client.health(), HealthResponse)
    client.subscriptions()
    client.subscribe_markets()
    client.unsubscribe_markets(("TEST",))
    client.subscribe_stocks(("000001.SZ",), "tick")
    client.unsubscribe_stocks(("000001.SZ",), "tick")
    client.market_snapshot()
    client.stock_snapshot(("000001.SZ",))
    client.market_quotes()
    client.stock_quotes()
    client.quote_sequence(1, wait_ms=500)
    client.download_history(("000001.SZ",))
    client.query_history(("000001.SZ",))
    client.download_financial(("000001.SZ",), ("Balance",))
    client.query_financial(("000001.SZ",), ("Balance",))
    client.query_dividend_factors(("000001.SZ",))
    http_client.close()

    assert requests == [
        ("GET", "/health"),
        ("GET", "/v1/subscriptions"),
        ("POST", "/v1/subscriptions/markets"),
        ("DELETE", "/v1/subscriptions/markets"),
        ("POST", "/v1/subscriptions/stocks"),
        ("DELETE", "/v1/subscriptions/stocks"),
        ("POST", "/v1/snapshots/markets"),
        ("POST", "/v1/snapshots/stocks"),
        ("GET", "/v1/quotes/subscribed/markets"),
        ("GET", "/v1/quotes/subscribed/stocks"),
        ("POST", "/v1/quotes/subscribed/sequence"),
        ("POST", "/v1/history/download"),
        ("POST", "/v1/history/query"),
        ("POST", "/v1/financial/download"),
        ("POST", "/v1/financial/query"),
        ("POST", "/v1/dividend-factors/query"),
    ]


def test_client_preserves_sequence_bounds_on_416() -> None:
    def handler(_: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            416,
            json={
                "detail": "too old",
                "requested_seq": 1,
                "oldest_seq": 100,
                "latest_seq": 200,
            },
        )

    http_client = httpx2.Client(
        transport=httpx2.MockTransport(handler), base_url="http://qmt-agent"
    )
    client = QmtAgentClient(client=http_client)

    with pytest.raises(QuoteSequenceOutOfRange) as error:
        client.quote_sequence(1)

    assert error.value.oldest_seq == 100
    assert error.value.latest_seq == 200
    http_client.close()


def test_client_accepts_successful_empty_sequence_without_bounds() -> None:
    def handler(_: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200,
            json={
                "data": [],
                "count": 0,
                "requested_seq": 1,
                "next_seq": 1,
            },
        )

    http_client = httpx2.Client(
        transport=httpx2.MockTransport(handler), base_url="http://qmt-agent"
    )
    client = QmtAgentClient(client=http_client)

    result = client.quote_sequence(1, wait_ms=30_000)

    assert result.count == 0
    assert result.next_seq == 1
    assert result.oldest_seq is None
    assert result.latest_seq is None
    http_client.close()


def test_client_rejects_unknown_top_level_fields_instead_of_dropping_them() -> None:
    def handler(_: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, json={**STATUS, "unexpected": 1})

    http_client = httpx2.Client(
        transport=httpx2.MockTransport(handler), base_url="http://qmt-agent"
    )
    client = QmtAgentClient(client=http_client)

    with pytest.raises(QmtAgentError, match="extra_forbidden"):
        client.subscriptions()

    http_client.close()
