"""隔离订阅建立、取消和延迟回调之间的竞态。"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from threading import Lock
from typing import Generic, Literal, TypeVar

logger = logging.getLogger(__name__)

QuotePush = TypeVar("QuotePush")
QuoteHandler = Callable[[QuotePush, datetime], None]


class QuoteCallbackGate(Generic[QuotePush]):
    """订阅确认前暂存回调，订阅关闭后忽略延迟回调。"""

    def __init__(self, handler: QuoteHandler) -> None:
        self._handler = handler
        self._lock = Lock()
        self._state: Literal["pending", "active", "closed"] = "pending"
        self._pending: list[tuple[QuotePush, datetime]] = []

    def __call__(self, quotes: QuotePush) -> None:
        received_at = datetime.now(UTC)
        with self._lock:
            if self._state == "closed":
                # 反订阅后到达的延迟回调必须丢弃；留下 DEBUG 记录便于核对竞态。
                logger.debug("丢弃已关闭订阅的延迟 XtData 行情回调：%r", quotes)
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
            if self._pending:
                # 订阅创建失败或取消成功时，暂存数据不能进入已失效订阅的缓存。
                logger.debug("订阅关闭时丢弃 %s 批暂存 XtData 行情", len(self._pending))
            self._state = "closed"
            self._pending.clear()

    def _deliver(self, quotes: QuotePush, received_at: datetime) -> None:
        try:
            self._handler(quotes, received_at)
        except Exception:
            # XtData 在自己的线程中调用回调，业务异常不能终止其回调线程。
            logger.exception("处理 XtData 行情回调失败")
