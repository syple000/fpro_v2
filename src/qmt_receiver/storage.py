"""QMT 实时与下载数据的统一 Parquet 存储。"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import pyarrow as pa

import qmt_receiver.schemas as schemas
from fpro_common import (
    datetime_to_utc_us,
    normalise_unix_timestamp_us,
    utc_now_us,
    utc_us_to_datetime,
)
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
    XtDataPeriod,
)

_QMT_TIMEZONE = ZoneInfo("Asia/Shanghai")
_SYNC_META_DATASETS = frozenset(
    {schemas.DAILY_TABLE, schemas.INTRADAY_TABLE, schemas.DIVIDEND_FACTOR_TABLE}
)


@dataclass(frozen=True, slots=True)
class QmtSyncRange:
    """一只证券的一类 QMT 数据已完整同步的日期闭区间。"""

    dataset: str
    code: str
    period: str | None
    start_date: date
    end_date: date


class QmtDataStore:
    """QMT 实时和下载数据共用的薄存储层。"""

    def __init__(self, root: str | Path, timezone: str = "Asia/Shanghai") -> None:
        self._timezone = ZoneInfo(timezone)
        root_path = Path(root).expanduser().resolve()
        self._store = ParquetStore(root_path)
        self._sync_meta_dir = root_path / "_meta" / "sync"
        self._sync_meta_lock = RLock()
        for table_name, schema in schemas.TABLE_SCHEMAS.items():
            self._store.register(
                TableConfig(
                    name=table_name,
                    schema=schema,
                    partition_by=schemas.TABLE_PARTITION_BY[table_name],
                    sort_by=schemas.TABLE_SORT_BY[table_name],
                    primary_key=schemas.TABLE_PRIMARY_KEY[table_name],
                    deduplicate_prefer_by=(schemas.TABLE_DEDUPLICATE_PREFER_BY[table_name] or None),
                )
            )

    def sync_completed_ranges(
        self,
        dataset: str,
        code: str,
        *,
        period: str | None = None,
    ) -> list[tuple[date, date]]:
        """读取指定数据和证券已完整同步的日期闭区间。"""
        _validate_sync_key(dataset, code, period)
        with self._sync_meta_lock:
            ranges = load_sync_ranges(self._sync_meta_dir, dataset=dataset)
        return [
            (item.start_date, item.end_date)
            for item in ranges
            if item.code == code and item.period == period
        ]

    def mark_sync_completed(
        self,
        dataset: str,
        code: str,
        start_date: date,
        end_date: date,
        *,
        period: str | None = None,
    ) -> None:
        """在数据成功落盘后原子提交一个完成区间。"""
        _validate_sync_key(dataset, code, period)
        if start_date > end_date:
            raise ValueError("QMT 同步完成区间起止颠倒")
        with self._sync_meta_lock:
            current = load_sync_ranges(self._sync_meta_dir, dataset=dataset)
            grouped: dict[tuple[str, str | None], list[tuple[date, date]]] = {}
            for item in current:
                grouped.setdefault((item.code, item.period), []).append(
                    (item.start_date, item.end_date)
                )
            grouped.setdefault((code, period), []).append((start_date, end_date))

            completed_ranges = []
            for (item_code, item_period), values in sorted(
                grouped.items(), key=lambda item: (item[0][0], item[0][1] or "")
            ):
                completed_ranges.extend(
                    {
                        "code": item_code,
                        "period": item_period,
                        "start_date": range_start.isoformat(),
                        "end_date": range_end.isoformat(),
                    }
                    for range_start, range_end in _merge_date_ranges(values)
                )
            document = {
                "version": 1,
                "dataset": dataset,
                "updated_at": utc_now_us(),
                "completed_ranges": completed_ranges,
            }
            self._sync_meta_dir.mkdir(parents=True, exist_ok=True)
            path = self._sync_meta_dir / f"{dataset}.json"
            temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
            try:
                with temporary.open("x", encoding="utf-8") as file:
                    json.dump(document, file, ensure_ascii=False, indent=2, sort_keys=True)
                    file.write("\n")
                    file.flush()
                    os.fsync(file.fileno())
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)

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

    def write_intraday(
        self,
        data: Mapping[str, Sequence[HistoryQuote]],
        period: XtDataPeriod,
        adjustment: DividendType,
    ) -> int:
        """写入 QMT 原生不复权或等比前复权历史分钟线。"""
        if period not in {"1m", "5m", "15m", "30m", "1h"}:
            raise ValueError(f"分钟线不支持周期 {period!r}")
        if adjustment not in {"none", "front_ratio"}:
            raise ValueError(f"分钟线不支持复权方式 {adjustment!r}")
        rows: list[dict[str, object]] = []
        for code, records in data.items():
            for record in records:
                if not isinstance(record, HistoryBar):
                    raise TypeError("分钟线存储只接受 HistoryBar")
                values = record.model_dump(mode="python", exclude_unset=True)
                index = values.pop("index")
                event_time = _history_index_timestamp_us(index, self._timezone)
                if event_time is None:
                    event_time = _xt_timestamp_us(values.get("time"))
                if event_time is None:
                    raise ValueError(f"{code} 分钟线包含无效时间: {index!r}")
                row = {
                    "trading_date": _trading_date(event_time, self._timezone),
                    "code": code,
                    "period": period,
                    "adjustment": adjustment,
                    "event_time": event_time,
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
            schemas.INTRADAY_TABLE,
            pa.Table.from_pylist(rows, schema=schemas.INTRADAY_SCHEMA),
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


def _history_index_timestamp_us(value: object, timezone: ZoneInfo) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    text = str(value)
    if len(text) != 14:
        return None
    try:
        local_time = datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=timezone)
    except ValueError:
        return None
    return datetime_to_utc_us(local_time)


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


def load_sync_ranges(
    meta_dir: str | Path,
    *,
    dataset: str | None = None,
) -> list[QmtSyncRange]:
    """读取 QMT 同步元数据；供存储层和 DataCatalog 共用。"""
    root = Path(meta_dir)
    paths = [root / f"{dataset}.json"] if dataset is not None else sorted(root.glob("*.json"))
    result: list[QmtSyncRange] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            document: object = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"无法读取 QMT 同步元数据: {path}") from exc
        if not isinstance(document, dict):
            raise ValueError(f"QMT 同步元数据格式错误: {path}")
        document_dataset = document.get("dataset")
        if (
            document.get("version") != 1
            or not isinstance(document_dataset, str)
            or document_dataset not in _SYNC_META_DATASETS
            or path.stem != document_dataset
        ):
            raise ValueError(f"QMT 同步元数据版本或数据表不匹配: {path}")
        raw_ranges = document.get("completed_ranges")
        if not isinstance(raw_ranges, list):
            raise ValueError(f"QMT 同步元数据缺少 completed_ranges: {path}")
        for raw_range in raw_ranges:
            if not isinstance(raw_range, dict):
                raise ValueError(f"QMT 同步完成区间格式错误: {path}")
            code = raw_range.get("code")
            period = raw_range.get("period")
            raw_start = raw_range.get("start_date")
            raw_end = raw_range.get("end_date")
            if (
                not isinstance(code, str)
                or (period is not None and not isinstance(period, str))
                or not isinstance(raw_start, str)
                or not isinstance(raw_end, str)
            ):
                raise ValueError(f"QMT 同步完成区间字段错误: {path}")
            _validate_sync_key(document_dataset, code, period)
            try:
                start_date = date.fromisoformat(raw_start)
                end_date = date.fromisoformat(raw_end)
            except ValueError as exc:
                raise ValueError(f"QMT 同步完成区间日期无效: {path}") from exc
            if start_date > end_date:
                raise ValueError(f"QMT 同步完成区间起止颠倒: {path}")
            result.append(QmtSyncRange(document_dataset, code, period, start_date, end_date))

    grouped: dict[tuple[str, str, str | None], list[tuple[date, date]]] = {}
    for item in result:
        grouped.setdefault((item.dataset, item.code, item.period), []).append(
            (item.start_date, item.end_date)
        )
    return [
        QmtSyncRange(item_dataset, code, period, start_date, end_date)
        for (item_dataset, code, period), values in sorted(
            grouped.items(), key=lambda item: (item[0][0], item[0][1], item[0][2] or "")
        )
        for start_date, end_date in _merge_date_ranges(values)
    ]


def _validate_sync_key(dataset: str, code: str, period: str | None) -> None:
    if dataset not in _SYNC_META_DATASETS:
        raise ValueError(f"不支持记录同步区间的数据表: {dataset!r}")
    if not code or code != code.strip().upper():
        raise ValueError(f"QMT 同步证券代码必须是规范大写形式: {code!r}")
    if dataset == schemas.INTRADAY_TABLE:
        if period not in {"1m", "5m", "15m", "30m", "1h"}:
            raise ValueError(f"QMT 分钟线同步周期无效: {period!r}")
    elif period is not None:
        raise ValueError(f"{dataset} 同步元数据不接受 period")


def _merge_date_ranges(ranges: Sequence[tuple[date, date]]) -> list[tuple[date, date]]:
    merged: list[tuple[date, date]] = []
    for start_date, end_date in sorted(ranges):
        if not merged or start_date.toordinal() > merged[-1][1].toordinal() + 1:
            merged.append((start_date, end_date))
            continue
        previous_start, previous_end = merged[-1]
        merged[-1] = (previous_start, max(previous_end, end_date))
    return merged
