"""Tushare 客户端的请求频率和并发保护。"""

from __future__ import annotations

from collections.abc import Callable
from threading import BoundedSemaphore, Lock
from time import monotonic, sleep
from typing import Any

DEFAULT_REQUESTS_PER_MINUTE = 120
DEFAULT_MAX_CONCURRENCY = 1


class RequestLimiter:
    """限制全客户端的请求启动频率和同时在途请求数。"""

    def __init__(
        self,
        requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    ) -> None:
        if requests_per_minute < 1:
            raise ValueError("requests_per_minute 必须大于等于 1")
        if max_concurrency < 1:
            raise ValueError("max_concurrency 必须大于等于 1")
        self.requests_per_minute = requests_per_minute
        self.max_concurrency = max_concurrency
        self._interval_seconds = 60.0 / requests_per_minute
        self._next_request_at = 0.0
        self._rate_lock = Lock()
        self._slots = BoundedSemaphore(max_concurrency)

    def call(self, function: Callable[..., Any], *args: object, **kwargs: object) -> Any:
        """等待频率和并发额度，然后在额度内完成一次真实请求。"""
        self._slots.acquire()
        try:
            with self._rate_lock:
                now = monotonic()
                delay = self._next_request_at - now
                if delay > 0:
                    sleep(delay)
                started_at = monotonic()
                self._next_request_at = started_at + self._interval_seconds
            return function(*args, **kwargs)
        finally:
            self._slots.release()


class RateLimitedProClient:
    """透明代理 Tushare ProClient，所有可调用接口共用一个限制器。"""

    def __init__(self, client: Any, limiter: RequestLimiter) -> None:
        self._client = client
        self.limiter = limiter

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._client, name)
        if not callable(attribute):
            return attribute

        def limited_call(*args: object, **kwargs: object) -> Any:
            return self.limiter.call(attribute, *args, **kwargs)

        return limited_call

