"""由 platform 主动调用的一次一批行情接收器。"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from qmt_protocol import QuoteEvent, QuoteSequenceResponse, SequencedQuote
from qmt_receiver.client import QuoteSequenceOutOfRange


class QuoteSequenceClient(Protocol):
    def quote_sequence(
        self,
        seq: int,
        limit: int = 1_000,
        stocks: Sequence[str] | None = None,
        wait_ms: int = 0,
    ) -> QuoteSequenceResponse: ...


class QuoteWriter(Protocol):
    def append(self, records: Sequence[SequencedQuote]) -> list[QuoteEvent]: ...


class QuoteQueue(Protocol):
    """platform 传入的队列只需提供线程安全的 put。"""

    def put(self, item: QuoteEvent) -> object: ...


@dataclass(frozen=True, slots=True)
class ReceiveResult:
    count: int
    next_seq: int
    probes: int = 0
    skipped: int = 0


class QmtReceiver:
    """不创建线程或队列；platform 每调用一次就处理一个 sequence 批次。"""

    def __init__(
        self,
        client: QuoteSequenceClient,
        writer: QuoteWriter,
        *,
        start_seq: int = 1,
        batch_size: int = 1_000,
        timeout_ms: int = 30_000,
    ) -> None:
        if start_seq < 1:
            raise ValueError("start_seq 必须大于等于 1")
        if not 1 <= batch_size <= 1_000:
            raise ValueError("batch_size 必须在 1 到 1000 之间")
        if not 0 <= timeout_ms <= 30_000:
            raise ValueError("timeout_ms 必须在 0 到 30000 之间")
        self._client = client
        self._writer = writer
        self._next_seq = start_seq
        self._batch_size = batch_size
        self._timeout_ms = timeout_ms

    @property
    def next_seq(self) -> int:
        return self._next_seq

    def receive(self, queue: QuoteQueue) -> ReceiveResult:
        """读取、写入按日存储缓冲区并投递一批；调用节奏由 platform 决定。"""
        requested_seq = self._next_seq
        probes = 0
        try:
            payload = self._client.quote_sequence(
                requested_seq,
                limit=self._batch_size,
                wait_ms=self._timeout_ms,
            )
        except QuoteSequenceOutOfRange as error:
            if _is_caught_up(requested_seq, error):
                return ReceiveResult(0, requested_seq)
            payload, probes = self._probe_available_sequence(error)
            if payload is None:
                return ReceiveResult(0, requested_seq, probes=probes)

        records = payload.data
        next_seq = payload.next_seq
        events = self._writer.append(records)
        for event in events:
            queue.put(event)
        self._next_seq = next_seq

        first_seq = records[0].seq if records else requested_seq
        return ReceiveResult(
            count=len(records),
            next_seq=next_seq,
            probes=probes,
            skipped=max(0, first_seq - requested_seq),
        )

    def _probe_available_sequence(
        self, initial: QuoteSequenceOutOfRange
    ) -> tuple[QuoteSequenceResponse | None, int]:
        """从 oldest-1 开始，以 +1/+2/+4... 向最新序号内试探。"""
        if initial.oldest_seq is None or initial.latest_seq is None:
            return None, 0

        candidate = max(1, initial.oldest_seq - 1)
        offset = 1
        probes = 0
        while True:
            probes += 1
            try:
                return (
                    self._client.quote_sequence(candidate, limit=self._batch_size),
                    probes,
                )
            except QuoteSequenceOutOfRange as error:
                if error.oldest_seq is None or error.latest_seq is None:
                    return None, probes
                candidate = min(error.latest_seq, error.oldest_seq + offset)
                offset *= 2


def _is_caught_up(requested_seq: int, error: QuoteSequenceOutOfRange) -> bool:
    return error.latest_seq is None or requested_seq == error.latest_seq + 1
