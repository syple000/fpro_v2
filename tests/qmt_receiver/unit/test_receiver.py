from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from queue import Queue

from qmt_protocol import (
    QuoteEvent,
    QuoteSequenceErrorResponse,
    QuoteSequenceResponse,
    SequencedQuote,
    TickQuote,
)
from qmt_receiver.client import QuoteSequenceOutOfRange
from qmt_receiver.receiver import QmtReceiver


def out_of_range(requested: int, oldest: int | None, latest: int | None) -> Exception:
    return QuoteSequenceOutOfRange(
        QuoteSequenceErrorResponse(
            detail="行情序号越界",
            requested_seq=requested,
            oldest_seq=oldest,
            latest_seq=latest,
        )
    )


class FakeClient:
    def __init__(self, responses: list[QuoteSequenceResponse | Exception]) -> None:
        self.responses = responses
        self.requested: list[tuple[int, int, int]] = []

    def quote_sequence(
        self,
        seq: int,
        limit: int = 1_000,
        stocks: Sequence[str] | None = None,
        wait_ms: int = 0,
    ) -> QuoteSequenceResponse:
        self.requested.append((seq, limit, wait_ms))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeWriter:
    def __init__(self) -> None:
        self.records: list[SequencedQuote] = []

    def append(self, records: Sequence[SequencedQuote]) -> list[QuoteEvent]:
        self.records.extend(records)
        return [
            QuoteEvent(
                trading_date=date(2026, 8, 16),
                **record.model_dump(mode="python"),
            )
            for record in records
        ]


def quote(seq: int) -> SequencedQuote:
    return SequencedQuote(
        seq=seq,
        code="000001.SZ",
        period="tick",
        source="market",
        subscription="SZ",
        received_at=datetime(2026, 8, 16, 1, tzinfo=UTC),
        quote=TickQuote(lastPrice=10.0),
    )


def batch(records: list[SequencedQuote], *, requested: int, next_seq: int) -> QuoteSequenceResponse:
    latest = max(next_seq - 1, requested)
    return QuoteSequenceResponse(
        data=records,
        count=len(records),
        requested_seq=requested,
        next_seq=next_seq,
        oldest_seq=min(requested, latest),
        latest_seq=latest,
    )


def empty_batch(requested: int, oldest: int | None, latest: int | None) -> QuoteSequenceResponse:
    return QuoteSequenceResponse(
        data=[],
        count=0,
        requested_seq=requested,
        next_seq=requested,
        oldest_seq=oldest,
        latest_seq=latest,
    )


def test_receive_writes_and_publishes_one_batch() -> None:
    records = [quote(1), quote(2)]
    client = FakeClient([batch(records, requested=1, next_seq=3)])
    writer = FakeWriter()
    queue: Queue[QuoteEvent] = Queue()
    receiver = QmtReceiver(client, writer, timeout_ms=123)

    result = receiver.receive(queue)

    assert result.count == 2
    assert result.next_seq == 3
    assert writer.records == records
    assert [queue.get_nowait().seq, queue.get_nowait().seq] == [1, 2]
    assert client.requested == [(1, 1_000, 123)]


def test_receive_returns_empty_after_long_poll_timeout_at_latest() -> None:
    client = FakeClient([empty_batch(11, 1, 10)])
    writer = FakeWriter()
    receiver = QmtReceiver(client, writer, start_seq=11, timeout_ms=500)

    result = receiver.receive(Queue())

    assert result.count == 0
    assert result.next_seq == 11
    assert client.requested == [(11, 1_000, 500)]


def test_outdated_sequence_probes_oldest_minus_one_then_exponential_offsets() -> None:
    available = batch([quote(122)], requested=122, next_seq=123)
    client = FakeClient(
        [
            out_of_range(1, 100, 199),
            out_of_range(99, 110, 209),
            out_of_range(111, 120, 219),
            available,
        ]
    )
    writer = FakeWriter()
    queue: Queue[QuoteEvent] = Queue()
    receiver = QmtReceiver(client, writer, timeout_ms=1_000)

    result = receiver.receive(queue)

    assert [request[0] for request in client.requested] == [1, 99, 111, 122]
    assert [request[2] for request in client.requested] == [1_000, 0, 0, 0]
    assert result.probes == 3
    assert result.skipped == 121
    assert result.next_seq == 123
    assert queue.get_nowait().seq == 122
