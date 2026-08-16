from __future__ import annotations

from threading import Thread

import pytest

from qmt_agent.quote_sequence import (
    QuoteSequenceBuffer,
    QuoteSequenceOutOfRangeError,
)


def append_values(buffer: QuoteSequenceBuffer, *values: int) -> None:
    buffer.append(
        [(f"{value:06d}.SZ", {"value": value}) for value in values],
        source="stock",
        subscription="000001.SZ",
        period="tick",
        received_at="2026-08-16T00:00:00+00:00",
    )


def test_buffer_assigns_contiguous_sequences_and_reads_only_requested_window() -> None:
    buffer = QuoteSequenceBuffer(capacity=5)
    append_values(buffer, 1, 2, 3, 4)

    result = buffer.read(2, 2)

    assert [item["seq"] for item in result["data"]] == [2, 3]
    assert [item["quote"]["value"] for item in result["data"]] == [2, 3]
    assert result["next_seq"] == 4
    assert buffer.status() == {
        "oldest_seq": 1,
        "latest_seq": 4,
        "next_seq": 5,
        "size": 4,
        "capacity": 5,
    }


def test_buffer_overwrites_only_oldest_records_and_reports_boundaries() -> None:
    buffer = QuoteSequenceBuffer(capacity=3)
    append_values(buffer, 1, 2, 3, 4, 5)

    result = buffer.read(3, 10)

    assert [item["seq"] for item in result["data"]] == [3, 4, 5]
    assert buffer.status()["oldest_seq"] == 3
    with pytest.raises(QuoteSequenceOutOfRangeError) as too_old:
        buffer.read(2, 1)
    with pytest.raises(QuoteSequenceOutOfRangeError) as too_new:
        buffer.read(6, 1)
    assert (too_old.value.oldest_seq, too_old.value.latest_seq) == (3, 5)
    assert (too_new.value.oldest_seq, too_new.value.latest_seq) == (3, 5)


def test_stock_filter_does_not_change_sequence_window_progress() -> None:
    buffer = QuoteSequenceBuffer(capacity=5)
    append_values(buffer, 1, 2, 3)

    result = buffer.read(1, 2, ["000002.SZ"])

    assert [item["seq"] for item in result["data"]] == [2]
    assert result["next_seq"] == 3


def test_buffer_rejects_invalid_capacity() -> None:
    with pytest.raises(ValueError, match="容量"):
        QuoteSequenceBuffer(capacity=0)


def test_buffer_rejects_invalid_read_limit() -> None:
    buffer = QuoteSequenceBuffer(capacity=1)
    append_values(buffer, 1)

    with pytest.raises(ValueError, match="读取条数"):
        buffer.read(1, 0)


def test_buffer_long_poll_wakes_when_requested_sequence_arrives() -> None:
    buffer = QuoteSequenceBuffer(capacity=2)
    result: dict[str, object] = {}

    def read() -> None:
        result.update(buffer.read(1, 1, wait_ms=1_000))

    reader = Thread(target=read)
    reader.start()
    append_values(buffer, 7)
    reader.join(timeout=2)

    assert not reader.is_alive()
    assert [item["seq"] for item in result["data"]] == [1]  # type: ignore[index]
