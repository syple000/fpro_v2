"""通过真实 ASGI 路由验证并发请求下的订阅正确性。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from fastapi.testclient import TestClient

from qmt_agent.api import create_app
from qmt_agent.config import Settings
from tests.qmt_agent.fakes import FakeGateway

pytestmark = pytest.mark.stress


def stock_code(number: int) -> str:
    return f"{number:06d}.SH"


def test_concurrent_http_requests_preserve_all_50_subscriptions() -> None:
    gateway = FakeGateway()
    app = create_app(gateway=gateway, settings=Settings())
    batches = [
        [stock_code(number) for number in range(start, start + 10)] for start in range(0, 50, 10)
    ]
    barrier = Barrier(len(batches))

    with TestClient(app) as client:

        def subscribe(batch: list[str]) -> int:
            barrier.wait()
            return client.post(
                "/v1/subscriptions/stocks",
                json={"stocks": batch, "period": "tick"},
            ).status_code

        with ThreadPoolExecutor(max_workers=len(batches)) as executor:
            status_codes = list(executor.map(subscribe, batches))

        status = client.get("/v1/subscriptions").json()
        active_during_run = gateway.active_codes()

    assert status_codes == [200] * len(batches)
    assert status["stock_count"] == 50
    assert status["stocks"] == [stock_code(number) for number in range(50)]
    assert {codes[0] for codes in active_during_run} == set(status["stocks"])
    assert gateway.active_codes() == []
