from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from time import sleep
from typing import Any

import pytest

from tushare_data.client import (
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_REQUESTS_PER_MINUTE,
    RateLimitedProClient,
    RequestLimiter,
)


class RecordingLimiter:
    def __init__(self) -> None:
        self.calls: list[tuple[object, tuple[object, ...], dict[str, object]]] = []

    def call(self, function: Any, *args: object, **kwargs: object) -> Any:
        self.calls.append((function, args, kwargs))
        return function(*args, **kwargs)


class FakeClient:
    version = "test"

    def daily(self, *, ts_code: str) -> str:
        return ts_code


def test_rate_limited_client_routes_every_api_call_through_one_limiter() -> None:
    limiter = RecordingLimiter()
    client = RateLimitedProClient(FakeClient(), limiter)  # type: ignore[arg-type]

    assert client.daily(ts_code="000001.SZ") == "000001.SZ"
    assert client.version == "test"
    assert len(limiter.calls) == 1


def test_request_limiter_uses_conservative_quicksync_defaults() -> None:
    limiter = RequestLimiter()

    assert limiter.requests_per_minute == DEFAULT_REQUESTS_PER_MINUTE == 120
    assert limiter.max_concurrency == DEFAULT_MAX_CONCURRENCY == 1


@pytest.mark.parametrize(
    ("requests_per_minute", "max_concurrency"),
    [(0, 1), (120, 0)],
)
def test_request_limiter_rejects_invalid_limits(
    requests_per_minute: int,
    max_concurrency: int,
) -> None:
    with pytest.raises(ValueError):
        RequestLimiter(requests_per_minute, max_concurrency)


def test_request_limiter_caps_simultaneous_in_flight_calls() -> None:
    limiter = RequestLimiter(requests_per_minute=60_000, max_concurrency=1)
    state_lock = Lock()
    active = 0
    maximum_active = 0

    def request() -> None:
        nonlocal active, maximum_active
        with state_lock:
            active += 1
            maximum_active = max(maximum_active, active)
        sleep(0.01)
        with state_lock:
            active -= 1

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(lambda _: limiter.call(request), range(4)))

    assert maximum_active == 1
