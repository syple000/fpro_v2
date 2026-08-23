"""带全局递增序号的固定容量行情循环缓存。"""

from __future__ import annotations

from collections.abc import Iterable
from threading import Condition, Lock
from time import monotonic

from qmt_protocol import (
    QuotePayload,
    QuoteSequenceResponse,
    QuoteSequenceStatus,
    QuoteSource,
    SequencedQuote,
    XtDataPeriod,
)


class QuoteSequenceOutOfRangeError(ValueError):
    """请求的行情序号不在当前循环缓存范围内。"""

    def __init__(
        self,
        requested_seq: int,
        oldest_seq: int | None,
        latest_seq: int | None,
    ) -> None:
        self.requested_seq = requested_seq
        self.oldest_seq = oldest_seq
        self.latest_seq = latest_seq

        if oldest_seq is None or latest_seq is None:
            message = f"行情顺序缓存为空，请求序号 {requested_seq}；当前没有最旧或最新序号"
        elif requested_seq < oldest_seq:
            message = (
                f"请求序号 {requested_seq} 过旧；当前最旧序号 {oldest_seq}，最新序号 {latest_seq}"
            )
        else:
            message = (
                f"请求序号 {requested_seq} 过新；当前最旧序号 {oldest_seq}，最新序号 {latest_seq}"
            )
        super().__init__(message)


class QuoteSequenceBuffer:
    """线程安全的环形缓存；写满后只覆盖最旧记录。"""

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("行情顺序缓存容量必须大于等于 1")

        self._capacity = capacity
        self._records: list[SequencedQuote | None] = [None] * capacity
        self._next_seq = 1
        self._size = 0
        self._lock = Lock()
        self._changed = Condition(self._lock)

    def append(
        self,
        items: Iterable[tuple[str, QuotePayload]],
        *,
        source: QuoteSource,
        subscription: str,
        period: XtDataPeriod,
        received_at: int,
    ) -> None:
        with self._changed:
            for code, quote in items:
                record = SequencedQuote(
                    seq=self._next_seq,
                    code=code,
                    period=period,
                    source=source,
                    subscription=subscription,
                    received_at=received_at,
                    quote=quote,
                )
                self._records[self._slot(record.seq)] = record
                self._next_seq += 1
                self._size = min(self._size + 1, self._capacity)
            self._changed.notify_all()

    def read(
        self,
        seq: int,
        limit: int,
        wait_ms: int = 0,
    ) -> QuoteSequenceResponse:
        """读取连续序号窗口；可等待下一个序号到达。"""
        if limit < 1:
            raise ValueError("行情顺序读取条数必须大于等于 1")
        if wait_ms < 0:
            raise ValueError("行情顺序等待时间不能小于 0")

        deadline = monotonic() + wait_ms / 1000
        with self._changed:
            while wait_ms and seq == self._next_seq:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    break
                self._changed.wait(remaining)

            oldest_seq, latest_seq = self._bounds_unlocked()
            if seq == self._next_seq:
                return QuoteSequenceResponse(
                    data=[],
                    count=0,
                    requested_seq=seq,
                    next_seq=seq,
                    oldest_seq=oldest_seq,
                    latest_seq=latest_seq,
                )
            if oldest_seq is None or latest_seq is None or seq < oldest_seq or seq > latest_seq:
                raise QuoteSequenceOutOfRangeError(seq, oldest_seq, latest_seq)

            end_seq = min(seq + limit - 1, latest_seq)
            records: list[SequencedQuote] = []
            for current_seq in range(seq, end_seq + 1):
                record = self._records[self._slot(current_seq)]
                # 在锁内且序号已通过范围校验，因此对应槽位一定是当前记录。
                if record is None or record.seq != current_seq:
                    raise RuntimeError("行情顺序缓存内部状态不一致")
                records.append(record)

        return QuoteSequenceResponse(
            data=records,
            count=len(records),
            requested_seq=seq,
            next_seq=end_seq + 1,
            oldest_seq=oldest_seq,
            latest_seq=latest_seq,
        )

    def status(self) -> QuoteSequenceStatus:
        with self._lock:
            oldest_seq, latest_seq = self._bounds_unlocked()
            return QuoteSequenceStatus(
                oldest_seq=oldest_seq,
                latest_seq=latest_seq,
                next_seq=self._next_seq,
                size=self._size,
                capacity=self._capacity,
            )

    def _slot(self, seq: int) -> int:
        return (seq - 1) % self._capacity

    def _bounds_unlocked(self) -> tuple[int | None, int | None]:
        if self._size == 0:
            return None, None
        return self._next_seq - self._size, self._next_seq - 1
