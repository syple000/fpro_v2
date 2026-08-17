from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq

from qmt_protocol import SequencedQuote, TickQuote
from qmt_receiver.storage import QuoteParquetWriter


def test_writer_partitions_by_quote_date_and_preserves_quote(tmp_path: Path) -> None:
    writer = QuoteParquetWriter(tmp_path)

    events = writer.append(
        [
            SequencedQuote(
                seq=8,
                code="000001.SZ",
                period="tick",
                source="market",
                subscription="SZ",
                received_at=datetime(2026, 8, 17, 1, tzinfo=UTC),
                quote=TickQuote(
                    time=int(
                        datetime(
                            2026,
                            8,
                            16,
                            14,
                            59,
                            59,
                            tzinfo=timezone(timedelta(hours=8)),
                        ).timestamp()
                        * 1_000
                    ),
                    lastPrice=10.5,
                ),
            )
        ]
    )
    writer.close()

    manifests = list((tmp_path / "quotes").rglob("_manifest.json"))
    assert len(manifests) == 1
    manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
    table = pq.ParquetFile(manifests[0].parent / manifest["files"][0]).read()
    row = table.to_pylist()[0]
    assert row["trading_date"].isoformat() == "2026-08-16"
    assert row["seq"] == 8
    assert json.loads(row["quote_json"])["lastPrice"] == 10.5
    assert events[0].trading_date.isoformat() == "2026-08-16"
    assert isinstance(events[0].quote, TickQuote)
    assert events[0].quote.lastPrice == 10.5
