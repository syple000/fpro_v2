from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq

from qmt_receiver.storage import QuoteParquetWriter


def test_writer_partitions_by_quote_date_and_preserves_quote(tmp_path: Path) -> None:
    writer = QuoteParquetWriter(tmp_path)

    events = writer.append(
        [
            {
                "seq": 8,
                "code": "000001.SZ",
                "period": "tick",
                "source": "market",
                "subscription": "SZ",
                "received_at": "2026-08-17T01:00:00+00:00",
                "quote": {"time": "20260816145959", "lastPrice": 10.5},
            }
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
    assert events[0]["trading_date"] == "2026-08-16"
    assert events[0]["quote"]["lastPrice"] == 10.5
