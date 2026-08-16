"""quote sequence 的按日 Parquet 写入。"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa

from parquet_store import ParquetStore, TableConfig

QUOTE_TABLE = "quotes"
QUOTE_SCHEMA = pa.schema(
    [
        pa.field("trading_date", pa.date32(), nullable=False),
        pa.field("seq", pa.int64(), nullable=False),
        pa.field("code", pa.string(), nullable=False),
        pa.field("period", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("subscription", pa.string(), nullable=False),
        pa.field("received_at", pa.string(), nullable=False),
        pa.field("quote_json", pa.large_string(), nullable=False),
    ]
)


class QuoteParquetWriter:
    def __init__(self, root: str | Path, timezone: str = "Asia/Shanghai") -> None:
        self._timezone = ZoneInfo(timezone)
        self._store = ParquetStore(root)
        self._store.register(
            TableConfig(
                name=QUOTE_TABLE,
                schema=QUOTE_SCHEMA,
                partition_by="trading_date",
                sort_by="seq",
            )
        )

    def append(self, records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        for record in records:
            received_at = str(record["received_at"])
            quote = record.get("quote")
            trading_date = _trading_date(quote, received_at, self._timezone)
            rows.append(
                {
                    "trading_date": trading_date,
                    "seq": int(record["seq"]),
                    "code": str(record["code"]),
                    "period": str(record["period"]),
                    "source": str(record["source"]),
                    "subscription": str(record["subscription"]),
                    "received_at": received_at,
                    "quote_json": json.dumps(
                        quote,
                        ensure_ascii=False,
                        allow_nan=False,
                        separators=(",", ":"),
                    ),
                }
            )
            events.append({"trading_date": trading_date.isoformat(), **dict(record)})

        if rows:
            self._store.append(QUOTE_TABLE, pa.Table.from_pylist(rows, schema=QUOTE_SCHEMA))
            self._store.flush(QUOTE_TABLE)
        return events

    def close(self) -> None:
        self._store.close()

    def __enter__(self) -> QuoteParquetWriter:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _trading_date(quote: Any, received_at: str, timezone: ZoneInfo) -> date:
    if isinstance(quote, Mapping):
        parsed = _parse_quote_time(quote.get("time"), timezone)
        if parsed is not None:
            return parsed.date()

    try:
        parsed_received_at = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
    except ValueError:
        parsed_received_at = datetime.now(UTC)
    if parsed_received_at.tzinfo is None:
        parsed_received_at = parsed_received_at.replace(tzinfo=UTC)
    return parsed_received_at.astimezone(timezone).date()


def _parse_quote_time(value: Any, timezone: ZoneInfo) -> datetime | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        timestamp = float(value)
        while abs(timestamp) >= 100_000_000_000:
            timestamp /= 1_000
        try:
            return datetime.fromtimestamp(timestamp, UTC).astimezone(timezone)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str):
        return None

    compact = value.strip()
    if compact.isdigit() and len(compact) not in {8, 14}:
        try:
            return _parse_quote_time(float(compact), timezone)
        except ValueError:
            return None
    if len(compact) >= 8 and compact[:8].isdigit():
        try:
            return datetime.strptime(compact[:8], "%Y%m%d").replace(tzinfo=timezone)
        except ValueError:
            pass
    try:
        parsed = datetime.fromisoformat(compact.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone)
    return parsed.astimezone(timezone)
