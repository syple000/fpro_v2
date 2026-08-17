"""强类型 quote sequence 的按日 Parquet 写入。"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TypedDict
from zoneinfo import ZoneInfo

import pyarrow as pa

from parquet_store import ParquetStore, TableConfig
from qmt_protocol import QuoteEvent, QuotePayload, SequencedQuote

logger = logging.getLogger(__name__)
QUOTE_TABLE = "quotes"
QUOTE_SCHEMA = pa.schema(
    [
        pa.field("trading_date", pa.date32(), nullable=False),
        pa.field("seq", pa.int64(), nullable=False),
        pa.field("code", pa.string(), nullable=False),
        pa.field("period", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("subscription", pa.string(), nullable=False),
        pa.field("received_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("quote_json", pa.large_string(), nullable=False),
    ]
)


class _QuoteParquetRow(TypedDict):
    trading_date: date
    seq: int
    code: str
    period: str
    source: str
    subscription: str
    received_at: datetime
    quote_json: str


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

    def append(self, records: Sequence[SequencedQuote]) -> list[QuoteEvent]:
        rows: list[_QuoteParquetRow] = []
        events: list[QuoteEvent] = []
        for record in records:
            trading_date = _trading_date(record.quote, record.received_at, self._timezone)
            # quote_json 保存行情模型的全部已知字段和 __pydantic_extra__ 扩展字段；
            # Parquet 只把固定信封字段单列，不会静默丢弃动态行情字段。
            quote_data = record.quote.model_dump(mode="json", exclude_none=True)
            rows.append(
                {
                    "trading_date": trading_date,
                    "seq": record.seq,
                    "code": record.code,
                    "period": record.period,
                    "source": record.source,
                    "subscription": record.subscription,
                    "received_at": record.received_at.astimezone(UTC),
                    "quote_json": json.dumps(
                        quote_data,
                        ensure_ascii=False,
                        allow_nan=False,
                        separators=(",", ":"),
                    ),
                }
            )
            events.append(
                QuoteEvent(
                    trading_date=trading_date,
                    seq=record.seq,
                    code=record.code,
                    period=record.period,
                    source=record.source,
                    subscription=record.subscription,
                    received_at=record.received_at,
                    quote=record.quote,
                )
            )

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


def _trading_date(quote: QuotePayload, received_at: datetime, timezone: ZoneInfo) -> date:
    if quote.time is not None:
        parsed = _parse_quote_time(quote.time, timezone)
        if parsed is not None:
            return parsed.date()
        logger.debug(
            "行情 time 无法解析，交易日期回退到 received_at：time=%s，received_at=%s",
            quote.time,
            received_at,
        )
    return received_at.astimezone(timezone).date()


def _parse_quote_time(value: int, timezone: ZoneInfo) -> datetime | None:
    timestamp = float(value)
    while abs(timestamp) >= 100_000_000_000:
        timestamp /= 1_000
    try:
        return datetime.fromtimestamp(timestamp, UTC).astimezone(timezone)
    except (OverflowError, OSError, ValueError):
        return None
