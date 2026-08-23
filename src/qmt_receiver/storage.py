"""QMT 实时与下载数据的统一 Parquet 存储。"""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa

from fpro_common import normalise_unix_timestamp_us, utc_us_to_datetime
from parquet_store import ParquetStore, TableConfig
from qmt_protocol import (
    BarQuote,
    DividendType,
    HistoryFrame,
    QuoteEvent,
    SequencedQuote,
    TickQuote,
)

logger = logging.getLogger(__name__)

TICK_TABLE = "ticks"
BAR_TABLE = "bars"
DAILY_TABLE = "daily"
FINANCIAL_TABLE = "financial"
DIVIDEND_FACTOR_TABLE = "dividend_factors"

_DAILY_FIELDS = (
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "settelementPrice",
    "openInterest",
    "preClose",
    "suspendFlag",
)

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

DAILY_SCHEMA = pa.schema(
    [
        pa.field("trade_date", pa.date32(), nullable=False),
        pa.field("code", pa.string(), nullable=False),
        pa.field("adjustment", pa.string(), nullable=False),
        pa.field("time", pa.int64()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("amount", pa.float64()),
        pa.field("settelementPrice", pa.float64()),
        pa.field("openInterest", pa.float64()),
        pa.field("preClose", pa.float64()),
        pa.field("suspendFlag", pa.int64()),
        pa.field("extra_json", pa.large_string()),
    ]
)
FINANCIAL_SCHEMA = pa.schema(
    [
        pa.field("report_date", pa.date32(), nullable=False),
        pa.field("code", pa.string(), nullable=False),
        pa.field("dataset", pa.string(), nullable=False),
        pa.field("announcement_date", pa.date32()),
        pa.field("data_json", pa.large_string(), nullable=False),
    ]
)
DIVIDEND_FACTOR_SCHEMA = pa.schema(
    [
        pa.field("ex_date", pa.date32(), nullable=False),
        pa.field("code", pa.string(), nullable=False),
        pa.field("interest", pa.float64()),
        pa.field("stockBonus", pa.float64()),
        pa.field("stockGift", pa.float64()),
        pa.field("allotNum", pa.float64()),
        pa.field("allotPrice", pa.float64()),
        pa.field("gugai", pa.float64()),
        pa.field("dr", pa.float64()),
        pa.field("extra_json", pa.large_string()),
    ]
)

_TICK_QUOTE_COLUMNS = tuple(field.name for field in _TICK_QUOTE_FIELDS)
_BAR_QUOTE_COLUMNS = tuple(field.name for field in _BAR_QUOTE_FIELDS)

_DOWNLOAD_PARTITION_BY = {
    DAILY_TABLE: "trade_date",
    FINANCIAL_TABLE: "report_date",
    DIVIDEND_FACTOR_TABLE: "ex_date",
}


class QmtDataStore:
    """QMT 实时和下载数据共用的薄存储层。"""

    def __init__(self, root: str | Path, timezone: str = "Asia/Shanghai") -> None:
        self._timezone = ZoneInfo(timezone)
        self._store = ParquetStore(root)
        for config in (
            TableConfig(
                name=TICK_TABLE,
                schema=TICK_SCHEMA,
                partition_by="trading_date",
                sort_by="seq",
                primary_key=("received_at", "seq"),
            ),
            TableConfig(
                name=BAR_TABLE,
                schema=BAR_SCHEMA,
                partition_by="trading_date",
                sort_by="seq",
                primary_key=("received_at", "seq"),
            ),
            TableConfig(
                name=DAILY_TABLE,
                schema=DAILY_SCHEMA,
                partition_by="trade_date",
                sort_by=("code", "adjustment"),
                primary_key=("code", "adjustment"),
            ),
            TableConfig(
                name=FINANCIAL_TABLE,
                schema=FINANCIAL_SCHEMA,
                partition_by="report_date",
                sort_by=("code", "dataset", "announcement_date"),
                primary_key=("code", "dataset", "announcement_date"),
            ),
            TableConfig(
                name=DIVIDEND_FACTOR_TABLE,
                schema=DIVIDEND_FACTOR_SCHEMA,
                partition_by="ex_date",
                sort_by="code",
                primary_key="code",
            ),
        ):
            self._store.register(config)

    def append_quotes(self, records: Sequence[SequencedQuote]) -> list[QuoteEvent]:
        """追加实时行情，不立即 flush 或整理。"""
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
                    **record.model_dump(mode="python"),
                )
            )

        if tick_rows:
            self._store.append(TICK_TABLE, pa.Table.from_pylist(tick_rows, schema=TICK_SCHEMA))
        if bar_rows:
            self._store.append(BAR_TABLE, pa.Table.from_pylist(bar_rows, schema=BAR_SCHEMA))
        return events

    def write_daily(
        self,
        frames: Mapping[str, HistoryFrame],
        adjustment: DividendType,
    ) -> int:
        """写入一种复权方式的下载日线。"""
        rows: list[dict[str, object]] = []
        for code, frame in frames.items():
            for index, values in _frame_rows(frame):
                row = {
                    "trade_date": _date_value(index),
                    "code": code,
                    "adjustment": adjustment,
                }
                for field in _DAILY_FIELDS:
                    value = values.get(field)
                    row[field] = (
                        _integer(value)
                        if field in {"time", "volume", "suspendFlag"}
                        else _number(value)
                    )
                row["extra_json"] = _extra_fields_json(values, _DAILY_FIELDS)
                rows.append(row)
        return self._write(
            DAILY_TABLE,
            pa.Table.from_pylist(rows, schema=DAILY_SCHEMA),
        )

    def write_financial(self, data: Mapping[str, Mapping[str, HistoryFrame]]) -> int:
        """写入下载财务数据。"""
        rows: list[dict[str, object]] = []
        for code, tables in data.items():
            for table, frame in tables.items():
                for index, values in _frame_rows(frame):
                    rows.append(
                        {
                            "report_date": _date_value(values.get("m_timetag", index)),
                            "code": code,
                            "dataset": table,
                            "announcement_date": _optional_date(values.get("m_anntime")),
                            "data_json": _json(values),
                        }
                    )
        return self._write(
            FINANCIAL_TABLE,
            pa.Table.from_pylist(rows, schema=FINANCIAL_SCHEMA),
        )

    def write_dividend_factors(self, frames: Mapping[str, HistoryFrame]) -> int:
        """写入下载除权因子。"""
        fields = (
            "interest",
            "stockBonus",
            "stockGift",
            "allotNum",
            "allotPrice",
            "gugai",
            "dr",
        )
        rows: list[dict[str, object]] = []
        for code, frame in frames.items():
            for index, values in _frame_rows(frame):
                row: dict[str, object] = {"ex_date": _date_value(index), "code": code}
                row.update({field: _number(values.get(field)) for field in fields})
                row["extra_json"] = _extra_fields_json(values, fields)
                rows.append(row)
        return self._write(
            DIVIDEND_FACTOR_TABLE,
            pa.Table.from_pylist(rows, schema=DIVIDEND_FACTOR_SCHEMA),
        )

    def _write(self, dataset: str, data: pa.Table) -> int:
        partition_by = _DOWNLOAD_PARTITION_BY[dataset]
        partitions: set[date] = set()
        for value in data.column(partition_by).to_pylist():
            if not isinstance(value, date):
                raise ValueError(f"{dataset} 返回了无效 {partition_by}: {value!r}")
            partitions.add(value)

        self._store.append(dataset, data)
        self._store.flush(dataset)
        for partition in sorted(partitions):
            self._store.compact_partition(dataset, partition)
        return data.num_rows

    def compact_realtime(self) -> dict[str, int]:
        """扫描并整理实时表中存在多个活动文件的分区。"""
        return {table: self._store.compact_table(table) for table in (TICK_TABLE, BAR_TABLE)}

    def close(self) -> None:
        self._store.close()

    def __enter__(self) -> QmtDataStore:
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
    quote_data["extra_json"] = _quote_extra_json(quote)
    row["quote"] = quote_data
    return row


def _quote_extra_json(quote: TickQuote | BarQuote) -> str | None:
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


def _frame_rows(frame: HistoryFrame) -> list[tuple[object, dict[str, object]]]:
    return [
        (index, dict(zip(frame.columns, row, strict=True)))
        for index, row in zip(frame.index, frame.data, strict=True)
    ]


def _date_value(value: object) -> date:
    result = _optional_date(value)
    if result is None:
        raise ValueError(f"QMT 返回了无效日期：{value!r}")
    return result


def _optional_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip().replace("-", "")
        if len(text) >= 8 and text[:8].isdigit():
            return datetime.strptime(text[:8], "%Y%m%d").date()
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        integer = int(value)
        if 19000101 <= integer <= 29991231:
            return datetime.strptime(str(integer), "%Y%m%d").date()
        timestamp = normalise_unix_timestamp_us(integer)
        if timestamp is not None:
            return utc_us_to_datetime(timestamp).astimezone(UTC).date()
    return None


def _integer(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(value):
        return int(value)
    return None


def _number(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        result = float(value)
        return result if math.isfinite(result) else None
    return None


def _extra_fields_json(values: Mapping[str, object], fields: Sequence[str]) -> str | None:
    extra = {name: value for name, value in values.items() if name not in fields}
    return _json(extra) if extra else None


def _json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
