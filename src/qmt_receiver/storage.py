"""Tick 与 bar 的强类型、按交易日 Parquet 写入。"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa

from fpro_common import utc_us_to_datetime
from parquet_store import ParquetStore, TableConfig
from qmt_protocol import BarQuote, QuoteEvent, SequencedQuote, TickQuote

logger = logging.getLogger(__name__)

TICK_TABLE = "ticks"
BAR_TABLE = "bars"

_ENVELOPE_FIELDS = (
    pa.field("trading_date", pa.date32(), nullable=False),
    pa.field("seq", pa.int64(), nullable=False),
    pa.field("code", pa.string(), nullable=False),
    pa.field("period", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
    pa.field("subscription", pa.string(), nullable=False),
    pa.field("received_at", pa.int64(), nullable=False),
    pa.field("event_at", pa.int64()),
)

_TICK_QUOTE_FIELDS = (
    pa.field("time", pa.int64()),
    pa.field("stime", pa.string()),
    pa.field("timetag", pa.string()),
    pa.field("lastPrice", pa.float64()),
    pa.field("open", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    pa.field("lastClose", pa.float64()),
    pa.field("amount", pa.float64()),
    pa.field("volume", pa.int64()),
    pa.field("pvolume", pa.int64()),
    pa.field("stockStatus", pa.int64()),
    pa.field("openInt", pa.int64()),
    pa.field("transactionNum", pa.int64()),
    pa.field("lastSettlementPrice", pa.float64()),
    pa.field("settlementPrice", pa.float64()),
    pa.field("pe", pa.float64()),
    pa.field("askPrice", pa.list_(pa.float64())),
    pa.field("bidPrice", pa.list_(pa.float64())),
    pa.field("askVol", pa.list_(pa.int64())),
    pa.field("bidVol", pa.list_(pa.int64())),
    pa.field("volRatio", pa.float64()),
    pa.field("speed1Min", pa.float64()),
    pa.field("speed5Min", pa.float64()),
)

_BAR_QUOTE_FIELDS = (
    pa.field("time", pa.int64()),
    pa.field("open", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    pa.field("close", pa.float64()),
    pa.field("volume", pa.int64()),
    pa.field("amount", pa.float64()),
    pa.field("settelementPrice", pa.float64()),
    pa.field("settlementPrice", pa.float64()),
    pa.field("openInterest", pa.float64()),
    pa.field("preClose", pa.float64()),
    pa.field("suspendFlag", pa.int64()),
    pa.field("dr", pa.float64()),
    pa.field("totaldr", pa.float64()),
)

# 行情主体保持与协议一致的嵌套结构；仅券商客户端的未知扩展字段进入 extra_json。
_TICK_QUOTE_TYPE = pa.struct([*_TICK_QUOTE_FIELDS, pa.field("extra_json", pa.large_string())])
_BAR_QUOTE_TYPE = pa.struct([*_BAR_QUOTE_FIELDS, pa.field("extra_json", pa.large_string())])
TICK_SCHEMA = pa.schema([*_ENVELOPE_FIELDS, pa.field("quote", _TICK_QUOTE_TYPE, nullable=False)])
BAR_SCHEMA = pa.schema([*_ENVELOPE_FIELDS, pa.field("quote", _BAR_QUOTE_TYPE, nullable=False)])

_TICK_QUOTE_COLUMNS = tuple(field.name for field in _TICK_QUOTE_FIELDS)
_BAR_QUOTE_COLUMNS = tuple(field.name for field in _BAR_QUOTE_FIELDS)


class QuoteParquetWriter:
    """把 sequence 中的 tick 和 bar 写入各自的按交易日分区表。"""

    def __init__(self, root: str | Path, timezone: str = "Asia/Shanghai") -> None:
        self._timezone = ZoneInfo(timezone)
        self._store = ParquetStore(root)
        self._store.register(
            TableConfig(
                name=TICK_TABLE,
                schema=TICK_SCHEMA,
                partition_by="trading_date",
                sort_by="seq",
            )
        )
        self._store.register(
            TableConfig(
                name=BAR_TABLE,
                schema=BAR_SCHEMA,
                partition_by="trading_date",
                sort_by="seq",
            )
        )

    def append(self, records: Sequence[SequencedQuote]) -> list[QuoteEvent]:
        tick_rows: list[dict[str, Any]] = []
        bar_rows: list[dict[str, Any]] = []
        events: list[QuoteEvent] = []

        for record in records:
            trading_date = _trading_date(record.event_at, record.received_at, self._timezone)
            common = _envelope_row(record, trading_date)
            if record.period == "tick":
                if not isinstance(record.quote, TickQuote):
                    raise TypeError("tick 行情必须使用 TickQuote")
                tick_rows.append(_quote_row(common, record.quote, _TICK_QUOTE_COLUMNS))
            else:
                if not isinstance(record.quote, BarQuote):
                    raise TypeError("bar 行情必须使用 BarQuote")
                bar_rows.append(_quote_row(common, record.quote, _BAR_QUOTE_COLUMNS))

            events.append(
                QuoteEvent(
                    trading_date=trading_date,
                    seq=record.seq,
                    code=record.code,
                    period=record.period,
                    source=record.source,
                    subscription=record.subscription,
                    received_at=record.received_at,
                    event_at=record.event_at,
                    quote=record.quote,
                )
            )

        # 先完成两张 Arrow Table 的构造和 schema 校验，再写入 ParquetStore 缓冲区。
        # 是否立即落盘由 store 的缓冲阈值决定，close() 会提交剩余数据。
        tick_table = pa.Table.from_pylist(tick_rows, schema=TICK_SCHEMA) if tick_rows else None
        bar_table = pa.Table.from_pylist(bar_rows, schema=BAR_SCHEMA) if bar_rows else None
        if tick_table is not None:
            self._store.append(TICK_TABLE, tick_table)
        if bar_table is not None:
            self._store.append(BAR_TABLE, bar_table)
        return events

    def close(self) -> None:
        self._store.close()

    def __enter__(self) -> QuoteParquetWriter:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _envelope_row(record: SequencedQuote, trading_date: date) -> dict[str, Any]:
    return {
        "trading_date": trading_date,
        "seq": record.seq,
        "code": record.code,
        "period": record.period,
        "source": record.source,
        "subscription": record.subscription,
        "received_at": record.received_at,
        "event_at": record.event_at,
    }


def _quote_row(
    common: dict[str, Any],
    quote: TickQuote | BarQuote,
    columns: tuple[str, ...],
) -> dict[str, Any]:
    row = dict(common)
    quote_data = quote.model_dump(mode="python", include=set(columns))
    quote_data["extra_json"] = _extra_json(quote)
    row["quote"] = quote_data
    return row


def _extra_json(quote: TickQuote | BarQuote) -> str | None:
    if not quote.model_extra:
        return None
    return json.dumps(
        quote.model_extra,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _trading_date(
    event_at: int | None,
    received_at: int,
    timezone: ZoneInfo,
) -> date:
    if event_at is not None:
        return utc_us_to_datetime(event_at).astimezone(timezone).date()
    logger.debug(
        "行情缺少可解析的 event_at，交易日期回退到 received_at：received_at=%s",
        received_at,
    )
    return utc_us_to_datetime(received_at).astimezone(timezone).date()
