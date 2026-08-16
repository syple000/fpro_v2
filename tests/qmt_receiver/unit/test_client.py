from __future__ import annotations

import httpx2
import pytest

from qmt_receiver.client import QmtAgentClient, QuoteSequenceOutOfRange


def test_client_exposes_all_qmt_agent_business_interfaces() -> None:
    requests: list[tuple[str, str]] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append((request.method, request.url.path))
        return httpx2.Response(200, json={})

    http_client = httpx2.Client(
        transport=httpx2.MockTransport(handler), base_url="http://qmt-agent"
    )
    client = QmtAgentClient(client=http_client)
    client.health()
    client.subscriptions()
    client.subscribe_markets()
    client.unsubscribe_markets(("TEST",))
    client.subscribe_stocks(("000001.SZ",), "tick")
    client.unsubscribe_stocks(("000001.SZ",), "tick")
    client.market_snapshot()
    client.stock_snapshot(("000001.SZ",))
    client.market_quotes(("000001.SZ",))
    client.stock_quotes(("000001.SZ",))
    client.quote_sequence(1, wait_ms=500)
    client.download_history(("000001.SZ",))
    client.query_history(("000001.SZ",))
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
        ("POST", "/v1/quotes/subscribed/markets"),
        ("POST", "/v1/quotes/subscribed/stocks"),
        ("POST", "/v1/quotes/subscribed/sequence"),
        ("POST", "/v1/history/download"),
        ("POST", "/v1/history/query"),
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
