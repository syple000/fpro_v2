from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from qmt_agent.api import create_app
from qmt_agent.config import Settings
from tests.qmt_agent.fakes import FakeGateway

pytestmark = pytest.mark.integration


def make_client(max_subscriptions: int = 300) -> TestClient:
    settings = Settings(max_stock_subscriptions=max_subscriptions)
    return TestClient(create_app(gateway=FakeGateway(), settings=settings))


def test_main_http_flow() -> None:
    with make_client() as client:
        subscribed = client.post(
            "/v1/subscriptions/stocks",
            json={"stocks": [" 000001.sz ", "600000.SH", "000001.SZ"]},
        )
        status = client.get("/v1/subscriptions")
        snapshot = client.post("/v1/snapshots/markets", json={"markets": ["SH", "SZ"]})
        stock_snapshot = client.post(
            "/v1/snapshots/stocks", json={"stocks": ["000001.SZ"]}
        )
        removed = client.request(
            "DELETE",
            "/v1/subscriptions/stocks",
            json={"stocks": ["000001.SZ"]},
        )

    assert subscribed.status_code == 200
    assert subscribed.json()["added"] == ["000001.SZ", "600000.SH"]
    assert status.json()["stock_count"] == 2
    assert snapshot.json()["count"] == 1
    assert stock_snapshot.json()["count"] == 1
    assert removed.json()["subscribed"] == ["600000.SH"]


def test_limit_error_is_http_409() -> None:
    with make_client(max_subscriptions=1) as client:
        response = client.post(
            "/v1/subscriptions/stocks",
            json={"stocks": ["000001.SZ", "600000.SH"]},
        )

    assert response.status_code == 409
    assert "不能超过 1" in response.json()["detail"]


def test_invalid_stock_code_is_rejected() -> None:
    with make_client() as client:
        response = client.post(
            "/v1/subscriptions/stocks", json={"stocks": ["000001"]}
        )

    assert response.status_code == 422


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
    assert gateway.history_download is not None
    assert gateway.history_download["incrementally"] is False


def test_market_endpoints_have_useful_defaults() -> None:
    with make_client() as client:
        subscribed = client.post("/v1/subscriptions/markets")
        removed = client.delete("/v1/subscriptions/markets")

    assert subscribed.json()["subscribed"] == ["SH", "SZ"]
    assert removed.json()["subscribed"] == []
    assert removed.json()["removed"] == ["SH", "SZ"]
