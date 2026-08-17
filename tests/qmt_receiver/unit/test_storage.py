from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from qmt_protocol import BarQuote, SequencedQuote, TickQuote
from qmt_receiver.storage import BAR_SCHEMA, TICK_SCHEMA, QuoteParquetWriter


def _read_only_table(table_root: Path) -> pa.Table:
    manifests = list(table_root.rglob("_manifest.json"))
    assert len(manifests) == 1
    manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
    return pq.ParquetFile(manifests[0].parent / manifest["files"][0]).read()


def test_parquet_quote_columns_cover_protocol_models() -> None:
    tick_quote_type = TICK_SCHEMA.field("quote").type
    bar_quote_type = BAR_SCHEMA.field("quote").type
    assert isinstance(tick_quote_type, pa.StructType)
    assert isinstance(bar_quote_type, pa.StructType)
    assert tick_quote_type.names[:-1] == list(TickQuote.model_fields)
    assert bar_quote_type.names[:-1] == list(BarQuote.model_fields)


def test_writer_separates_tick_and_bar_with_fixed_schemas(tmp_path: Path) -> None:
    quote_time = int(
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
    )
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
                quote=TickQuote.model_validate(
                    {
                        "time": quote_time,
                        "lastPrice": 10.5,
                        "askPrice": [10.6, 10.7],
                        "vendorFlag": "tick-extension",
                    }
                ),
            ),
            SequencedQuote(
                seq=9,
                code="000001.SZ",
                period="1m",
                source="stock",
                subscription="000001.SZ",
                received_at=datetime(2026, 8, 17, 1, tzinfo=UTC),
                quote=BarQuote.model_validate(
                    {
                        "time": quote_time,
                        "open": 10.1,
                        "close": 10.5,
                        "volume": 123,
                        "vendorFlag": "bar-extension",
                    }
                ),
            ),
        ]
    )
    writer.close()

    tick_table = _read_only_table(tmp_path / "ticks")
    bar_table = _read_only_table(tmp_path / "bars")
    assert tick_table.schema == TICK_SCHEMA
    assert bar_table.schema == BAR_SCHEMA

    tick_row = tick_table.to_pylist()[0]
    assert tick_row["trading_date"].isoformat() == "2026-08-16"
    assert tick_row["seq"] == 8
    assert tick_row["period"] == "tick"
    assert tick_row["quote"]["lastPrice"] == 10.5
    assert tick_row["quote"]["askPrice"] == [10.6, 10.7]
    assert json.loads(tick_row["quote"]["extra_json"]) == {
        "vendorFlag": "tick-extension"
    }
    assert "close" not in TICK_SCHEMA.field("quote").type.names
    assert "quote_json" not in tick_table.column_names

    bar_row = bar_table.to_pylist()[0]
    assert bar_row["trading_date"].isoformat() == "2026-08-16"
    assert bar_row["seq"] == 9
    assert bar_row["period"] == "1m"
    assert bar_row["quote"]["open"] == 10.1
    assert bar_row["quote"]["close"] == 10.5
    assert bar_row["quote"]["volume"] == 123
    assert json.loads(bar_row["quote"]["extra_json"]) == {
        "vendorFlag": "bar-extension"
    }
    assert "lastPrice" not in BAR_SCHEMA.field("quote").type.names
    assert "quote_json" not in bar_table.column_names

    assert [event.seq for event in events] == [8, 9]
    assert isinstance(events[0].quote, TickQuote)
    assert isinstance(events[1].quote, BarQuote)


def test_writer_only_creates_manifest_for_received_quote_kind(tmp_path: Path) -> None:
    with QuoteParquetWriter(tmp_path) as writer:
        writer.append(
            [
                SequencedQuote(
                    seq=1,
                    code="000001.SZ",
                    period="tick",
                    source="market",
                    subscription="SZ",
                    received_at=datetime(2026, 8, 17, 1, tzinfo=UTC),
                    quote=TickQuote(lastPrice=10.5),
                )
            ]
        )

    assert len(list((tmp_path / "ticks").rglob("_manifest.json"))) == 1
    assert list((tmp_path / "bars").rglob("_manifest.json")) == []
