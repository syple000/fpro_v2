from __future__ import annotations

from queue import Queue
from typing import Any

from qmt_receiver.client import QuoteSequenceOutOfRange
from qmt_receiver.receiver import QmtReceiver


def out_of_range(requested: int, oldest: int | None, latest: int | None) -> Exception:
    return QuoteSequenceOutOfRange(
        {
            "requested_seq": requested,
            "oldest_seq": oldest,
            "latest_seq": latest,
        }
    )


class FakeClient:
    def __init__(self, responses: list[dict[str, Any] | Exception]) -> None:
        self.responses = responses
        self.requested: list[tuple[int, int, int]] = []

    def quote_sequence(self, seq: int, limit: int, wait_ms: int = 0) -> dict[str, Any]:
        self.requested.append((seq, limit, wait_ms))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeWriter:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def append(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        self.records.extend(records)
        return records


def quote(seq: int) -> dict[str, Any]:
    return {
        "seq": seq,
        "code": "000001.SZ",
        "period": "tick",
        "source": "market",
        "subscription": "SZ",
        "received_at": "2026-08-16T01:00:00+00:00",
        "quote": {"lastPrice": 10.0},
    }


def test_receive_writes_and_publishes_one_batch() -> None:
    client = FakeClient([{"data": [quote(1), quote(2)], "next_seq": 3}])
    writer = FakeWriter()
    queue: Queue[dict[str, Any]] = Queue()
    receiver = QmtReceiver(client, writer, timeout_ms=123)  # type: ignore[arg-type]

    result = receiver.receive(queue)

    assert result.count == 2
    assert result.next_seq == 3
    assert writer.records == [quote(1), quote(2)]
    assert [queue.get_nowait()["seq"], queue.get_nowait()["seq"]] == [1, 2]
    assert client.requested == [(1, 1_000, 123)]


def test_receive_returns_empty_after_long_poll_timeout_at_latest() -> None:
    client = FakeClient([out_of_range(11, 1, 10)])
    writer = FakeWriter()
    receiver = QmtReceiver(  # type: ignore[arg-type]
        client, writer, start_seq=11, timeout_ms=500
    )

    result = receiver.receive(Queue())

    assert result.count == 0
    assert result.next_seq == 11
    assert client.requested == [(11, 1_000, 500)]


def test_outdated_sequence_probes_oldest_minus_one_then_exponential_offsets() -> None:
    client = FakeClient(
        [
            out_of_range(1, 100, 199),
            out_of_range(99, 110, 209),
            out_of_range(111, 120, 219),
            {"data": [quote(122)], "next_seq": 123},
        ]
    )
    writer = FakeWriter()
    queue: Queue[dict[str, Any]] = Queue()
    receiver = QmtReceiver(client, writer, timeout_ms=1_000)  # type: ignore[arg-type]

    result = receiver.receive(queue)

    assert [request[0] for request in client.requested] == [1, 99, 111, 122]
    assert [request[2] for request in client.requested] == [1_000, 0, 0, 0]
    assert result.probes == 3
    assert result.skipped == 121
    assert result.next_seq == 123
    assert queue.get_nowait()["seq"] == 122
