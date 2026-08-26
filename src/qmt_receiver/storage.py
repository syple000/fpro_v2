"""QMT 实时与下载数据的统一 Parquet 存储。"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa

import qmt_receiver.schemas as schemas
from fpro_common import normalise_unix_timestamp_us, utc_us_to_datetime
from parquet_store import ParquetStore, TableConfig
from qmt_protocol import (
    BarQuote,
    DividendFactor,
    DividendType,
    FinancialData,
    HistoryBar,
    HistoryQuote,
    QuoteEvent,
    SequencedQuote,
    TickQuote,
)

_QMT_TIMEZONE = ZoneInfo("Asia/Shanghai")


class QmtDataStore:
    """QMT 实时和下载数据共用的薄存储层。"""

    def __init__(self, root: str | Path, timezone: str = "Asia/Shanghai") -> None:
        self._timezone = ZoneInfo(timezone)
        self._store = ParquetStore(root)
        for table_name, schema in schemas.TABLE_SCHEMAS.items():
            self._store.register(
                TableConfig(
                    name=table_name,
                    schema=schema,
                    partition_by=schemas.TABLE_PARTITION_BY[table_name],
                    sort_by=schemas.TABLE_SORT_BY[table_name],
                    primary_key=schemas.TABLE_PRIMARY_KEY[table_name],
                    deduplicate_prefer_by=(
                        schemas.TABLE_DEDUPLICATE_PREFER_BY[table_name] or None
                    ),
                )
            )

    def append_quotes(self, records: Sequence[SequencedQuote]) -> list[QuoteEvent]:
        """追加实时行情，不立即 flush 或整理。"""
        tick_rows: list[dict[str, Any]] = []
        bar_rows: list[dict[str, Any]] = []
        events: list[QuoteEvent] = []
        for record in records:
            event_time = normalise_unix_timestamp_us(record.quote.time)
            trading_date = _trading_date(
                event_time if event_time is not None else record.received_at,
                self._timezone,
            )
            common = _envelope_row(record, trading_date, event_time)
            if record.period == "tick":
                if not isinstance(record.quote, TickQuote):
                    raise TypeError("tick 行情必须使用 TickQuote")
                tick_rows.append(_quote_row(common, record.quote, schemas.TICK_QUOTE_COLUMNS))
            else:
                if not isinstance(record.quote, BarQuote):
                    raise TypeError("bar 行情必须使用 BarQuote")
                bar_rows.append(_quote_row(common, record.quote, schemas.BAR_QUOTE_COLUMNS))
            events.append(
                QuoteEvent(
                    trading_date=trading_date,
                    event_time=event_time,
                    **record.model_dump(mode="python"),
                )
            )

        if tick_rows:
            self._store.append(
                schemas.TICK_TABLE,
                pa.Table.from_pylist(tick_rows, schema=schemas.TICK_SCHEMA),
            )
        if bar_rows:
            self._store.append(
                schemas.BAR_TABLE,
                pa.Table.from_pylist(bar_rows, schema=schemas.BAR_SCHEMA),
            )
        return events

    def write_daily(
        self,
        data: Mapping[str, Sequence[HistoryQuote]],
        adjustment: DividendType,
    ) -> int:
        """写入一种复权方式的下载日线。"""
        rows: list[dict[str, object]] = []
        for code, records in data.items():
            for record in records:
                if not isinstance(record, HistoryBar):
                    raise TypeError("日线存储只接受 HistoryBar")
                values = record.model_dump(mode="python", exclude_unset=True)
                index = values.pop("index")
                row = {
                    "trade_date": _date_value(index),
                    "code": code,
                    "adjustment": adjustment,
                }
                for field in schemas.DAILY_FIELDS:
                    value = values.get(field)
                    row[field] = (
                        _integer(value)
                        if field in {"time", "volume", "suspendFlag"}
                        else _number(value)
                    )
                rows.append(row)
        return self._write(
            schemas.DAILY_TABLE,
            pa.Table.from_pylist(rows, schema=schemas.DAILY_SCHEMA),
        )

    def write_financial(
        self,
        data: Mapping[str, FinancialData],
    ) -> int:
        """写入下载财务数据。"""
        rows: list[dict[str, object]] = []
        for code, tables in data.items():
            for table in FinancialData.model_fields:
                records = getattr(tables, table)
                if records is None:
                    continue
                for record in records:
                    values = record.model_dump(mode="python", exclude_unset=True)
                    index = values.pop("index")
                    rows.append(
                        {
                            "report_date": _date_value(
                                values.get("m_timetag", values.get("endDate", index))
                            ),
                            "code": code,
                            "dataset": table,
                            "disclosure_date": _optional_date(
                                values.get("m_anntime", values.get("declareDate"))
                            ),
                            "data_json": _json(values),
                        }
                    )
        return self._write(
            schemas.FINANCIAL_TABLE,
            pa.Table.from_pylist(rows, schema=schemas.FINANCIAL_SCHEMA),
        )

    def write_dividend_factors(
        self,
        data: Mapping[str, Sequence[DividendFactor]],
    ) -> int:
        """写入下载除权因子。"""
        rows: list[dict[str, object]] = []
        for code, factors in data.items():
            for factor in factors:
                event_time = _xt_timestamp_us(factor.time)
                if event_time is None:
                    raise ValueError(f"{code} 除权数据包含无效 time: {factor.time!r}")
                rows.append(
                    {
                        "ex_date": _date_value(factor.date),
                        "event_time": event_time,
                        "code": code,
                        "interest": factor.interest,
                        "stockBonus": factor.stockBonus,
                        "stockGift": factor.stockGift,
                        "allotNum": factor.allotNum,
                        "allotPrice": factor.allotPrice,
                        "gugai": factor.gugai,
                        "dr": factor.dr,
                    }
                )
        return self._write(
            schemas.DIVIDEND_FACTOR_TABLE,
            pa.Table.from_pylist(rows, schema=schemas.DIVIDEND_FACTOR_SCHEMA),
        )

    def _write(self, dataset: str, data: pa.Table) -> int:
        partition_by = schemas.TABLE_PARTITION_BY[dataset]
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
        """扫描实时表，合并文件并按业务键去重。"""
        return {
            table: self._store.compact_table(table)
            for table in (schemas.TICK_TABLE, schemas.BAR_TABLE)
        }

    def close(self) -> None:
        self._store.close()

    def __enter__(self) -> QmtDataStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _envelope_row(
    record: SequencedQuote,
    trading_date: date,
    event_time: int | None,
) -> dict[str, Any]:
    return {
        "trading_date": trading_date,
        "seq": record.seq,
        "code": record.code,
        "period": record.period,
        "source": record.source,
        "subscription": record.subscription,
        "received_at": record.received_at,
        "event_time": event_time,
    }


def _quote_row(
    common: dict[str, Any],
    quote: TickQuote | BarQuote,
    columns: tuple[str, ...],
) -> dict[str, Any]:
    row = dict(common)
    quote_data = quote.model_dump(mode="python", include=set(columns))
    row["quote"] = quote_data
    return row


def _trading_date(event_time: int, timezone: ZoneInfo) -> date:
    return utc_us_to_datetime(event_time).astimezone(timezone).date()


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
            return utc_us_to_datetime(timestamp).astimezone(_QMT_TIMEZONE).date()
    return None


def _integer(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(value):
        return int(value)
    return None


def _xt_timestamp_us(value: object) -> int | None:
    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            return None
        value = int(value)
    return normalise_unix_timestamp_us(value)


def _number(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        result = float(value)
        return result if math.isfinite(result) else None
    return None


def _json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
