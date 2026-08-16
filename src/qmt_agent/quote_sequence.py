"""带全局递增序号的固定容量行情循环缓存。"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from threading import Lock
from typing import Any


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
            message = (
                f"行情顺序缓存为空，请求序号 {requested_seq}；"
                "当前没有最旧或最新序号"
            )
        elif requested_seq < oldest_seq:
            message = (
                f"请求序号 {requested_seq} 过旧；"
                f"当前最旧序号 {oldest_seq}，最新序号 {latest_seq}"
            )
        else:
            message = (
                f"请求序号 {requested_seq} 过新；"
                f"当前最旧序号 {oldest_seq}，最新序号 {latest_seq}"
            )
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class SequencedQuote:
    seq: int
    code: str
    period: str
    source: str
    subscription: str
    received_at: str
    quote: Any

    def as_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "code": self.code,
            "period": self.period,
            "source": self.source,
            "subscription": self.subscription,
            "received_at": self.received_at,
            "quote": self.quote,
        }


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

    def append(
        self,
        items: Iterable[tuple[str, Any]],
        *,
        source: str,
        subscription: str,
        period: str,
        received_at: str,
    ) -> None:
        with self._lock:
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

    def read(
        self,
        seq: int,
        limit: int,
        stocks: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        """读取连续序号窗口；股票筛选不改变窗口推进位置。"""
        if limit < 1:
            raise ValueError("行情顺序读取条数必须大于等于 1")

        requested_stocks = None if stocks is None else set(stocks)
        with self._lock:
            oldest_seq, latest_seq = self._bounds_unlocked()
            if (
                oldest_seq is None
                or latest_seq is None
                or seq < oldest_seq
                or seq > latest_seq
            ):
                raise QuoteSequenceOutOfRangeError(seq, oldest_seq, latest_seq)

            end_seq = min(seq + limit - 1, latest_seq)
            records: list[SequencedQuote] = []
            for current_seq in range(seq, end_seq + 1):
                record = self._records[self._slot(current_seq)]
                # 在锁内且序号已通过范围校验，因此对应槽位一定是当前记录。
                if record is None or record.seq != current_seq:
                    raise RuntimeError("行情顺序缓存内部状态不一致")
                if requested_stocks is None or record.code in requested_stocks:
                    records.append(record)

        return {
            "data": [record.as_dict() for record in records],
            "count": len(records),
            "requested_seq": seq,
            "next_seq": end_seq + 1,
            "oldest_seq": oldest_seq,
            "latest_seq": latest_seq,
        }

    def status(self) -> dict[str, int | None]:
        with self._lock:
            oldest_seq, latest_seq = self._bounds_unlocked()
            return {
                "oldest_seq": oldest_seq,
                "latest_seq": latest_seq,
                "next_seq": self._next_seq,
                "size": self._size,
                "capacity": self._capacity,
            }

    def _slot(self, seq: int) -> int:
        return (seq - 1) % self._capacity

    def _bounds_unlocked(self) -> tuple[int | None, int | None]:
        if self._size == 0:
            return None, None
        return self._next_seq - self._size, self._next_seq - 1
