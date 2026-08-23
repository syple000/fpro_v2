from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from qmt_protocol import BarQuote, HistoryFrame, SequencedQuote, TickQuote
from qmt_receiver.storage import (
    BAR_SCHEMA,
    DAILY_TABLE,
    TICK_SCHEMA,
    QmtDataStore,
)


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


def test_store_separates_tick_and_bar_with_fixed_schemas(tmp_path: Path) -> None:
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
    received_at = 1_786_928_400_000_000
    store = QmtDataStore(tmp_path)

    events = store.append_quotes(
        [
            SequencedQuote(
                seq=8,
                code="000001.SZ",
                period="tick",
                source="market",
                subscription="SZ",
                received_at=received_at,
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
                received_at=received_at,
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
    store.close()

    tick_table = _read_only_table(tmp_path / "ticks")
    bar_table = _read_only_table(tmp_path / "bars")
    assert tick_table.schema == TICK_SCHEMA
    assert bar_table.schema == BAR_SCHEMA

    tick_row = tick_table.to_pylist()[0]
    assert tick_row["trading_date"].isoformat() == "2026-08-16"
    assert tick_row["seq"] == 8
    assert tick_row["period"] == "tick"
    assert TICK_SCHEMA.field("received_at").type == pa.int64()
    assert TICK_SCHEMA.field("event_at").type == pa.int64()
    assert tick_row["received_at"] == received_at
    assert tick_row["event_at"] == quote_time * 1_000
    assert tick_row["quote"]["lastPrice"] == 10.5
    assert tick_row["quote"]["askPrice"] == [10.6, 10.7]
    assert json.loads(tick_row["quote"]["extra_json"]) == {"vendorFlag": "tick-extension"}
    assert "close" not in TICK_SCHEMA.field("quote").type.names
    assert "quote_json" not in tick_table.column_names

    bar_row = bar_table.to_pylist()[0]
    assert bar_row["trading_date"].isoformat() == "2026-08-16"
    assert bar_row["seq"] == 9
    assert bar_row["period"] == "1m"
    assert BAR_SCHEMA.field("received_at").type == pa.int64()
    assert BAR_SCHEMA.field("event_at").type == pa.int64()
    assert bar_row["event_at"] == quote_time * 1_000
    assert bar_row["quote"]["open"] == 10.1
    assert bar_row["quote"]["close"] == 10.5
    assert bar_row["quote"]["volume"] == 123
    assert json.loads(bar_row["quote"]["extra_json"]) == {"vendorFlag": "bar-extension"}
    assert "lastPrice" not in BAR_SCHEMA.field("quote").type.names
    assert "quote_json" not in bar_table.column_names

    assert [event.seq for event in events] == [8, 9]
    assert isinstance(events[0].quote, TickQuote)
    assert events[0].event_at == quote_time * 1_000
    assert isinstance(events[1].quote, BarQuote)


def test_store_only_creates_manifest_for_received_quote_kind(tmp_path: Path) -> None:
    store = QmtDataStore(tmp_path)
    store.append_quotes(
        [
            SequencedQuote(
                seq=1,
                code="000001.SZ",
                period="tick",
                source="market",
                subscription="SZ",
                received_at=1_786_928_400_000_000,
                quote=TickQuote(lastPrice=10.5),
            )
        ]
    )

    # append 只需进入 store 缓冲区，小批次不会立即生成 Parquet manifest。
    assert list((tmp_path / "ticks").rglob("_manifest.json")) == []
    store.close()

    assert len(list((tmp_path / "ticks").rglob("_manifest.json"))) == 1
    assert list((tmp_path / "bars").rglob("_manifest.json")) == []


def test_download_write_immediately_deduplicates_partition(tmp_path: Path) -> None:
    first = HistoryFrame(index=[20240102], columns=["close"], data=[[10.0]])
    latest = HistoryFrame(index=[20240102], columns=["close"], data=[[11.0]])

    with QmtDataStore(tmp_path) as store:
        store.write_daily({"000001.SZ": first}, "none")
        store.write_daily({"000001.SZ": latest}, "none")

    table = _read_only_table(tmp_path / DAILY_TABLE)
    assert table.num_rows == 1
    assert table.column("close").to_pylist() == [11.0]


def test_store_compacts_and_deduplicates_realtime_files(tmp_path: Path) -> None:
    record = SequencedQuote(
        seq=1,
        code="000001.SZ",
        period="tick",
        source="market",
        subscription="SZ",
        received_at=1_786_928_400_000_000,
        quote=TickQuote(lastPrice=10.5),
    )
    for _ in range(2):
        with QmtDataStore(tmp_path) as store:
            store.append_quotes([record])

    manifest_path = next((tmp_path / "ticks").rglob("_manifest.json"))
    fragmented = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert len(fragmented["files"]) == 2

    with QmtDataStore(tmp_path) as store:
        store.compact_realtime()

    compacted = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert len(compacted["files"]) == 1
    assert _read_only_table(tmp_path / "ticks").num_rows == 1


def test_store_can_explicitly_compact_realtime_files(tmp_path: Path) -> None:
    record = SequencedQuote(
        seq=1,
        code="000001.SZ",
        period="tick",
        source="market",
        subscription="SZ",
        received_at=1_786_928_400_000_000,
        quote=TickQuote(lastPrice=10.5),
    )
    with QmtDataStore(tmp_path) as store:
        store.append_quotes([record])
        assert store.compact_realtime() == {"ticks": 0, "bars": 0}
        store.append_quotes([record])
        assert store.compact_realtime() == {"ticks": 1, "bars": 0}

    assert _read_only_table(tmp_path / "ticks").num_rows == 1
