"""隔离订阅建立、取消和延迟回调之间的竞态。"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from threading import Lock
from typing import Any

logger = logging.getLogger(__name__)

QuoteHandler = Callable[[dict[str, Any], str], None]


class QuoteCallbackGate:
    """订阅确认前暂存回调，订阅关闭后忽略延迟回调。"""

    def __init__(self, handler: QuoteHandler) -> None:
        self._handler = handler
        self._lock = Lock()
        self._state = "pending"
        self._pending: list[tuple[dict[str, Any], str]] = []

    def __call__(self, quotes: dict[str, Any]) -> None:
        received_at = datetime.now(UTC).isoformat()
        with self._lock:
            if self._state == "closed":
                return
            if self._state == "pending":
                self._pending.append((quotes, received_at))
                return
            self._deliver(quotes, received_at)

    def activate(self) -> None:
        """确认订阅成功，并按到达顺序送出此前暂存的回调。"""
        with self._lock:
            if self._state == "closed":
                return
            for quotes, received_at in self._pending:
                self._deliver(quotes, received_at)
            self._pending.clear()
            self._state = "active"

    def suspend(self) -> None:
        """取消订阅前暂停分发；取消失败时可再次 activate。"""
        with self._lock:
            if self._state == "active":
                self._state = "pending"

    def close(self) -> None:
        """永久关闭并丢弃尚未确认的回调。"""
        with self._lock:
            self._state = "closed"
            self._pending.clear()

    def _deliver(self, quotes: dict[str, Any], received_at: str) -> None:
        try:
            self._handler(quotes, received_at)
        except Exception:
            # XtData 在自己的线程中调用回调，业务异常不能终止其回调线程。
            logger.exception("处理 XtData 行情回调失败")
