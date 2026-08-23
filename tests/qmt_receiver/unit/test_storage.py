from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from qmt_protocol import (
    BalanceRecord,
    BarQuote,
    DividendFactor,
    FinancialData,
    HistoryBar,
    SequencedQuote,
    TickQuote,
    XtDataPeriod,
)
from qmt_receiver.storage import (
    BAR_SCHEMA,
    DAILY_TABLE,
    DIVIDEND_FACTOR_TABLE,
    FINANCIAL_TABLE,
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
    assert tick_quote_type.names == list(TickQuote.model_fields)
    assert bar_quote_type.names == list(BarQuote.model_fields)


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
    assert TICK_SCHEMA.field("event_time").type == pa.int64()
    assert TICK_SCHEMA.field("event_time").nullable
    assert tick_row["received_at"] == received_at
    assert tick_row["event_time"] == quote_time * 1_000
    assert tick_row["quote"]["lastPrice"] == 10.5
    assert tick_row["quote"]["askPrice"] == [10.6, 10.7]
    assert "close" not in TICK_SCHEMA.field("quote").type.names
    assert "quote_json" not in tick_table.column_names

    bar_row = bar_table.to_pylist()[0]
    assert bar_row["trading_date"].isoformat() == "2026-08-16"
    assert bar_row["seq"] == 9
    assert bar_row["period"] == "1m"
    assert BAR_SCHEMA.field("received_at").type == pa.int64()
    assert BAR_SCHEMA.field("event_time").type == pa.int64()
    assert BAR_SCHEMA.field("event_time").nullable
    assert bar_row["event_time"] == quote_time * 1_000
    assert bar_row["quote"]["open"] == 10.1
    assert bar_row["quote"]["close"] == 10.5
    assert bar_row["quote"]["volume"] == 123
    assert "lastPrice" not in BAR_SCHEMA.field("quote").type.names
    assert "quote_json" not in bar_table.column_names

    assert [event.seq for event in events] == [8, 9]
    assert isinstance(events[0].quote, TickQuote)
    assert events[0].event_time == quote_time * 1_000
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
                quote=TickQuote(time=1_786_928_400_000, lastPrice=10.5),
            )
        ]
    )

    # append 只需进入 store 缓冲区，小批次不会立即生成 Parquet manifest。
    assert list((tmp_path / "ticks").rglob("_manifest.json")) == []
    store.close()

    assert len(list((tmp_path / "ticks").rglob("_manifest.json"))) == 1
    assert list((tmp_path / "bars").rglob("_manifest.json")) == []
    row = _read_only_table(tmp_path / "ticks").to_pylist()[0]
    assert row["event_time"] == row["received_at"]


def test_download_write_immediately_deduplicates_partition(tmp_path: Path) -> None:
    first = [HistoryBar(index=20240102, close=10.0)]
    latest = [HistoryBar(index=20240102, close=11.0)]

    with QmtDataStore(tmp_path) as store:
        store.write_daily({"000001.SZ": first}, "none")
        store.write_daily({"000001.SZ": latest}, "none")

    table = _read_only_table(tmp_path / DAILY_TABLE)
    assert table.num_rows == 1
    assert table.column("close").to_pylist() == [11.0]


def test_financial_disclosure_date_is_an_attribute_not_a_key(tmp_path: Path) -> None:
    first = FinancialData(
        Balance=[
            BalanceRecord(
                index=0,
                m_anntime="20240401",
                m_timetag="20231231",
                tot_assets=100.0,
            )
        ]
    )
    latest = FinancialData(
        Balance=[
            BalanceRecord(
                index=0,
                m_anntime="20240430",
                m_timetag="20231231",
                tot_assets=110.0,
            )
        ]
    )
    with QmtDataStore(tmp_path) as store:
        store.write_financial({"000001.SZ": latest})
        store.write_financial({"000001.SZ": first})

    table = _read_only_table(tmp_path / FINANCIAL_TABLE)
    assert table.num_rows == 1
    disclosure_date = table.column("disclosure_date").to_pylist()[0]
    data_json = table.column("data_json").to_pylist()[0]
    assert disclosure_date is not None
    assert data_json is not None
    assert disclosure_date.isoformat() == "2024-04-30"
    assert '"tot_assets":110.0' in data_json


def test_dividend_timestamp_uses_china_calendar_date(tmp_path: Path) -> None:
    with QmtDataStore(tmp_path) as store:
        store.write_dividend_factors(
            {
                "000001.SZ": [
                    DividendFactor(
                        date="20230721",
                        time=1_689_868_800_000.0,
                        interest=0.32,
                        dr=1.04507,
                    )
                ]
            }
        )

    table = _read_only_table(tmp_path / DIVIDEND_FACTOR_TABLE)
    ex_date = table.column("ex_date").to_pylist()[0]
    assert ex_date is not None
    assert ex_date.isoformat() == "2023-07-21"
    assert table.column("event_time").to_pylist() == [1_689_868_800_000_000]


def test_store_compacts_and_deduplicates_realtime_files(tmp_path: Path) -> None:
    event_time = 1_786_928_400_000_000
    records = (
        SequencedQuote(
            seq=1,
            code="000001.SZ",
            period="tick",
            source="market",
            subscription="SZ",
            received_at=event_time + 1,
            quote=TickQuote(time=event_time, lastPrice=10.5),
        ),
        SequencedQuote(
            seq=2,
            code="000001.SZ",
            period="tick",
            source="stock",
            subscription="000001.SZ",
            received_at=event_time + 2,
            quote=TickQuote(time=event_time, lastPrice=10.6),
        ),
    )
    for record in records:
        with QmtDataStore(tmp_path) as store:
            store.append_quotes([record])

    manifest_path = next((tmp_path / "ticks").rglob("_manifest.json"))
    fragmented = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert len(fragmented["files"]) == 2

    with QmtDataStore(tmp_path) as store:
        store.compact_realtime()

    compacted = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert len(compacted["files"]) == 1
    table = _read_only_table(tmp_path / "ticks")
    assert table.num_rows == 1
    assert table.column("seq").to_pylist() == [2]


def test_store_deduplicates_realtime_rows_inside_one_file(tmp_path: Path) -> None:
    event_time = 1_786_928_400_000_000
    records = [
        SequencedQuote(
            seq=1,
            code="000001.SZ",
            period="tick",
            source="market",
            subscription="SZ",
            received_at=event_time + 2,
            quote=TickQuote(time=event_time, lastPrice=10.6),
        ),
        SequencedQuote(
            seq=2,
            code="000001.SZ",
            period="tick",
            source="stock",
            subscription="000001.SZ",
            received_at=event_time + 1,
            quote=TickQuote(time=event_time, lastPrice=10.5),
        ),
    ]
    with QmtDataStore(tmp_path) as store:
        store.append_quotes(records)

    with QmtDataStore(tmp_path) as store:
        assert store.compact_realtime() == {"ticks": 1, "bars": 0}

    table = _read_only_table(tmp_path / "ticks")
    assert table.num_rows == 1
    assert table.column("seq").to_pylist() == [1]


def test_store_can_explicitly_compact_realtime_files(tmp_path: Path) -> None:
    record = SequencedQuote(
        seq=1,
        code="000001.SZ",
        period="tick",
        source="market",
        subscription="SZ",
        received_at=1_786_928_400_000_000,
        quote=TickQuote(time=1_786_928_400_000, lastPrice=10.5),
    )
    with QmtDataStore(tmp_path) as store:
        store.append_quotes([record])
        assert store.compact_realtime() == {"ticks": 0, "bars": 0}
        store.append_quotes([record])
        assert store.compact_realtime() == {"ticks": 1, "bars": 0}

    assert _read_only_table(tmp_path / "ticks").num_rows == 1


def test_bar_deduplication_keeps_different_periods(tmp_path: Path) -> None:
    event_time = 1_786_928_400_000_000
    periods: tuple[tuple[int, XtDataPeriod], ...] = ((1, "1m"), (2, "5m"))
    for seq, period in periods:
        with QmtDataStore(tmp_path) as store:
            store.append_quotes(
                [
                    SequencedQuote(
                        seq=seq,
                        code="000001.SZ",
                        period=period,
                        source="stock",
                        subscription="000001.SZ",
                        received_at=event_time + seq,
                        quote=BarQuote(time=event_time, close=10.5),
                    )
                ]
            )

    with QmtDataStore(tmp_path) as store:
        store.compact_realtime()

    table = _read_only_table(tmp_path / "bars")
    assert table.num_rows == 2
    assert set(table.column("period").to_pylist()) == {"1m", "5m"}
