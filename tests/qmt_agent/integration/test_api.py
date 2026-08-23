from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from qmt_agent.api import create_app
from qmt_agent.config import Settings
from tests.qmt_agent.fakes import FakeGateway

pytestmark = pytest.mark.integration


def make_client(max_subscriptions: int = 50) -> TestClient:
    settings = Settings(max_stock_subscriptions=max_subscriptions)
    return TestClient(create_app(gateway=FakeGateway(), settings=settings))


def test_main_http_flow() -> None:
    with make_client() as client:
        subscribed = client.post(
            "/v1/subscriptions/stocks",
            json={
                "stocks": [" 000001.sz ", "600000.SH", "000001.SZ"],
                "period": "1m",
            },
        )
        status = client.get("/v1/subscriptions")
        snapshot = client.post("/v1/snapshots/markets", json={"markets": ["SH", "SZ"]})
        stock_snapshot = client.post("/v1/snapshots/stocks", json={"stocks": ["000001.SZ"]})
        removed = client.request(
            "DELETE",
            "/v1/subscriptions/stocks",
            json={"stocks": ["000001.SZ"], "period": "1m"},
        )

    assert subscribed.status_code == 200
    assert subscribed.json()["added"] == ["000001.SZ", "600000.SH"]
    assert "subscription_ids" not in subscribed.json()
    assert subscribed.json()["periods"] == {
        "000001.SZ": "1m",
        "600000.SH": "1m",
    }
    assert status.json()["stock_count"] == 2
    assert "market_subscription_ids" not in status.json()
    assert status.json()["stock_periods"] == {
        "000001.SZ": "1m",
        "600000.SH": "1m",
    }
    assert snapshot.json()["count"] == 1
    assert stock_snapshot.json()["count"] == 1
    assert removed.json()["subscribed"] == ["600000.SH"]
    assert "subscription_ids" not in removed.json()


def test_openapi_exposes_explicit_response_and_quote_schemas() -> None:
    with make_client() as client:
        schema = client.get("/openapi.json").json()

    components = schema["components"]["schemas"]
    assert {
        "TickQuote",
        "BarQuote",
        "SnapshotResponse",
        "LatestQuotesResponse",
        "QuoteSequenceResponse",
        "SubscriptionStatus",
        "HistoryQueryResponse",
        "BalanceRecord",
        "FinancialData",
        "FinancialQueryResponse",
        "DividendFactor",
        "DividendFactorsResponse",
    } <= components.keys()
    response_schema = schema["paths"]["/v1/quotes/subscribed/sequence"]["post"]["responses"]["200"][
        "content"
    ]["application/json"]["schema"]
    assert response_schema["$ref"].endswith("/QuoteSequenceResponse")


def test_limit_error_is_http_409() -> None:
    with make_client(max_subscriptions=1) as client:
        response = client.post(
            "/v1/subscriptions/stocks",
            json={"stocks": ["000001.SZ", "600000.SH"], "period": "tick"},
        )

    assert response.status_code == 409
    assert "不能超过 1" in response.json()["detail"]


def test_invalid_stock_code_is_rejected() -> None:
    with make_client() as client:
        response = client.post(
            "/v1/subscriptions/stocks",
            json={"stocks": ["000001"], "period": "tick"},
        )

    assert response.status_code == 422


def test_stock_subscription_requires_supported_period() -> None:
    with make_client() as client:
        missing = client.post("/v1/subscriptions/stocks", json={"stocks": ["000001.SZ"]})
        unsupported = client.post(
            "/v1/subscriptions/stocks",
            json={"stocks": ["000001.SZ"], "period": "2m"},
        )

    assert missing.status_code == 422
    assert unsupported.status_code == 422


def test_request_validation_rejects_unknown_fields_and_type_coercion() -> None:
    gateway = FakeGateway()
    with TestClient(create_app(gateway=gateway, settings=Settings())) as client:
        unknown_field = client.post(
            "/v1/history/download",
            json={
                "stocks": ["000001.SZ"],
                "mod": "full",
            },
        )
        wrong_type = client.post(
            "/v1/quotes/subscribed/sequence",
            json={"seq": "1"},
        )

    assert unknown_field.status_code == 422
    assert wrong_type.status_code == 422
    assert gateway.history_download is None


def test_stock_period_change_requires_explicit_unsubscribe() -> None:
    gateway = FakeGateway()
    with TestClient(create_app(gateway=gateway, settings=Settings())) as client:
        first = client.post(
            "/v1/subscriptions/stocks",
            json={"stocks": ["000001.SZ"], "period": "1m"},
        )
        conflict = client.post(
            "/v1/subscriptions/stocks",
            json={"stocks": ["000001.SZ"], "period": "5m"},
        )
        status = client.get("/v1/subscriptions")
        unsubscribed_before_close = list(gateway.unsubscribed)

    assert first.status_code == 200
    assert conflict.status_code == 409
    assert "显式取消" in conflict.json()["detail"]
    assert status.json()["stock_periods"] == {"000001.SZ": "1m"}
    assert unsubscribed_before_close == []


def test_history_download_mode_is_forwarded() -> None:
    gateway = FakeGateway()
    settings = Settings()
    with TestClient(create_app(gateway=gateway, settings=settings)) as client:
        response = client.post(
            "/v1/history/download",
            json={
                "stocks": ["000001.SZ"],
                "period": "1d",
                "start_time": "20250101",
                "mode": "full",
            },
        )

    assert response.status_code == 200
    assert response.json() == {"completed": True}
    assert gateway.history_download is not None
    assert gateway.history_download["incrementally"] is False


def test_financial_and_dividend_factor_endpoints() -> None:
    gateway = FakeGateway()
    with TestClient(create_app(gateway=gateway, settings=Settings())) as client:
        downloaded = client.post(
            "/v1/financial/download",
            json={
                "stocks": ["000001.SZ"],
                "tables": ["Balance", "Income"],
                "start_time": "20240101",
                "end_time": "20251231",
            },
        )
        financial = client.post(
            "/v1/financial/query",
            json={
                "stocks": ["000001.SZ"],
                "tables": ["Balance"],
                "report_type": "announce_time",
            },
        )
        dividends = client.post(
            "/v1/dividend-factors/query",
            json={"stocks": ["000001.SZ"], "start_time": "20240101"},
        )

    assert downloaded.status_code == 200
    assert downloaded.json()["completed"] is True
    assert gateway.financial_download is not None
    assert gateway.financial_download["tables"] == ["Balance", "Income"]
    assert financial.json()["data"]["000001.SZ"]["Balance"] == [
        {
            "index": 0,
            "m_timetag": "20241231",
            "m_anntime": "20250331",
            "tot_assets": 100.0,
        }
    ]
    assert dividends.json()["data"]["000001.SZ"] == [
        {
            "date": "20240601",
            "time": 1_717_200_000_000.0,
            "interest": 0.1,
            "dr": 0.99,
        }
    ]


def test_market_endpoints_have_useful_defaults() -> None:
    with make_client() as client:
        subscribed = client.post("/v1/subscriptions/markets")
        removed = client.delete("/v1/subscriptions/markets")

    assert subscribed.json()["subscribed"] == ["SH", "SZ"]
    assert "subscription_ids" not in subscribed.json()
    assert removed.json()["subscribed"] == []
    assert removed.json()["removed"] == ["SH", "SZ"]
    assert "subscription_ids" not in removed.json()


def test_sequenced_quote_api_preserves_all_rows_and_reports_range_errors() -> None:
    gateway = FakeGateway()
    settings = Settings(quote_buffer_capacity=2)
    with TestClient(create_app(gateway=gateway, settings=settings)) as client:
        client.post(
            "/v1/subscriptions/stocks",
            json={"stocks": ["000001.SZ"], "period": "1m"},
        )
        subscription_id = gateway.active_subscription_ids()["000001.SZ"]
        gateway.push(
            subscription_id,
            {
                "000001.SZ": [
                    {"close": 10.0},
                    {"close": 10.1},
                    {"close": 10.2},
                ]
            },
        )

        available = client.post(
            "/v1/quotes/subscribed/sequence",
            json={"seq": 2, "limit": 2},
        )
        too_old = client.post(
            "/v1/quotes/subscribed/sequence",
            json={"seq": 1},
        )
        caught_up = client.post(
            "/v1/quotes/subscribed/sequence",
            json={"seq": 4},
        )
        too_new = client.post(
            "/v1/quotes/subscribed/sequence",
            json={"seq": 5},
        )
        status = client.get("/v1/subscriptions")

    assert available.status_code == 200
    assert [item["seq"] for item in available.json()["data"]] == [2, 3]
    assert [item["quote"]["close"] for item in available.json()["data"]] == [
        10.1,
        10.2,
    ]
    assert too_old.status_code == 416
    assert too_old.json()["requested_seq"] == 1
    assert (too_old.json()["oldest_seq"], too_old.json()["latest_seq"]) == (2, 3)
    assert caught_up.status_code == 200
    assert caught_up.json() == {
        "data": [],
        "count": 0,
        "requested_seq": 4,
        "next_seq": 4,
        "oldest_seq": 2,
        "latest_seq": 3,
    }
    assert too_new.status_code == 416
    assert (too_new.json()["oldest_seq"], too_new.json()["latest_seq"]) == (2, 3)
    assert status.json()["quote_sequence"] == {
        "oldest_seq": 2,
        "latest_seq": 3,
        "next_seq": 4,
        "size": 2,
        "capacity": 2,
    }


def test_empty_sequence_long_poll_returns_200_without_advancing_cursor() -> None:
    with make_client() as client:
        first = client.post(
            "/v1/quotes/subscribed/sequence",
            json={"seq": 1, "wait_ms": 1},
        )
        second = client.post(
            "/v1/quotes/subscribed/sequence",
            json={"seq": 1, "wait_ms": 1},
        )

    expected = {
        "data": [],
        "count": 0,
        "requested_seq": 1,
        "next_seq": 1,
    }
    assert first.status_code == 200
    assert first.json() == expected
    assert second.status_code == 200
    assert second.json() == expected


def test_market_and_stock_quote_endpoints_return_separate_caches() -> None:
    gateway = FakeGateway()
    with TestClient(create_app(gateway=gateway, settings=Settings())) as client:
        client.post("/v1/subscriptions/markets", json={"markets": ["SH"]})
        client.post(
            "/v1/subscriptions/stocks",
            json={"stocks": ["600000.SH"], "period": "1m"},
        )
        subscription_ids = gateway.active_subscription_ids()
        gateway.push(
            subscription_ids["SH"],
            {"600000.SH": {"lastPrice": 10.2}},
        )
        gateway.push(
            subscription_ids["600000.SH"],
            {"600000.SH": [{"close": 10.1}]},
        )

        market_quotes = client.post("/v1/quotes/subscribed/markets")
        stock_quotes = client.post("/v1/quotes/subscribed/stocks")
        market_filter = client.post(
            "/v1/quotes/subscribed/markets",
            json={"stocks": ["000001.SZ"]},
        )

    assert market_quotes.status_code == 200
    assert market_quotes.json()["data"]["600000.SH"]["lastPrice"] == 10.2
    assert market_quotes.json()["periods"] == {"600000.SH": "tick"}
    assert stock_quotes.status_code == 200
    assert stock_quotes.json()["data"]["600000.SH"]["close"] == 10.1
    assert stock_quotes.json()["periods"] == {"600000.SH": "1m"}
    assert market_filter.json()["data"] == market_quotes.json()["data"]
