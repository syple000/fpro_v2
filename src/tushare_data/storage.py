"""Tushare 数据在 :mod:`parquet_store` 上的薄存储层。"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa

from fpro_common import require_utc_us, utc_now_us
from parquet_store import ParquetStore, TableConfig
from tushare_data.schemas import SYNC_RANGES_SCHEMA, TABLE_PARTITION_BY, TABLE_SCHEMAS

SYNC_RANGES_TABLE = "_sync_ranges"


class TushareDataStore:
    """注册固定表、按股票写入，并保存已经成功拉取的日期区间。"""

    def __init__(self, root: str | Path) -> None:
        self._store = ParquetStore(root)
        for table_name, schema in TABLE_SCHEMAS.items():
            self._store.register(
                TableConfig(
                    name=table_name,
                    schema=schema,
                    partition_by=TABLE_PARTITION_BY[table_name],
                    sort_by="visible_at",
                )
            )
        self._store.register(
            TableConfig(
                name=SYNC_RANGES_TABLE,
                schema=SYNC_RANGES_SCHEMA,
                partition_by=("dataset", "ts_code"),
                sort_by="visible_at",
            )
        )

    def upsert(self, dataset: str, partition: str, data: pa.Table) -> int:
        """把一批数据并入目标分区；重复的完整记录只保留一份。"""
        schema = TABLE_SCHEMAS[dataset]
        if not data.schema.equals(schema, check_metadata=True):
            raise ValueError(f"{dataset} 输入 Schema 不匹配")
        partition_by = TABLE_PARTITION_BY[dataset]
        if data.num_rows and set(data.column(partition_by).to_pylist()) != {partition}:
            raise ValueError(f"输入数据必须全部属于同一个 {partition_by} 分区")
        if not data.num_rows:
            return 0

        current = self._store.read(dataset, partitions=partition)
        rows = current.to_pylist() + data.to_pylist()
        unique_rows: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in rows:
            key = tuple(row[name] for name in schema.names)
            unique_rows[key] = row

        merged = pa.Table.from_pylist(list(unique_rows.values()), schema=schema)
        self._store.replace_partition(dataset, partition, merged)
        return merged.num_rows - current.num_rows

    def read(
        self,
        dataset: str,
        partition: str,
        *,
        visible_start: int | None = None,
        visible_end: int | None = None,
        as_of: int | None = None,
    ) -> pa.Table:
        """读取一只股票，并可按可见时间过滤，供无未来函数回测使用。"""
        filters: list[tuple[str, str, object]] = []
        if visible_start is not None:
            filters.append(("visible_at", ">=", require_utc_us(visible_start, "visible_start")))
        if visible_end is not None:
            filters.append(("visible_at", "<=", require_utc_us(visible_end, "visible_end")))
        if as_of is not None:
            filters.append(("visible_at", "<=", require_utc_us(as_of, "as_of")))
        return self._store.read(dataset, partitions=partition, filter=filters or None)

    def missing_ranges(
        self,
        dataset: str,
        ts_code: str,
        start_date: date,
        end_date: date,
    ) -> list[tuple[date, date]]:
        """返回请求区间中尚未成功拉取的所有闭区间。"""
        if start_date > end_date:
            raise ValueError("start_date 不能晚于 end_date")
        coverage = self._store.read(
            SYNC_RANGES_TABLE,
            partitions={"dataset": dataset, "ts_code": ts_code},
            columns=("start_date", "end_date"),
        )
        existing = [
            (row["start_date"], row["end_date"])
            for row in coverage.to_pylist()
            if row["end_date"] >= start_date and row["start_date"] <= end_date
        ]
        return _missing_ranges(existing, start_date, end_date)

    def mark_synced(
        self,
        dataset: str,
        ts_code: str,
        start_date: date,
        end_date: date,
    ) -> None:
        """记录一个成功请求过的区间，空结果也必须调用。"""
        if start_date > end_date:
            raise ValueError("start_date 不能晚于 end_date")
        selector = {"dataset": dataset, "ts_code": ts_code}
        current = self._store.read(SYNC_RANGES_TABLE, partitions=selector)
        ranges = [
            (row["start_date"], row["end_date"])
            for row in current.to_pylist()
        ]
        ranges.append((start_date, end_date))
        now = utc_now_us()
        rows = [
            {
                "dataset": dataset,
                "ts_code": ts_code,
                "visible_at": now,
                "start_date": range_start,
                "end_date": range_end,
            }
            for range_start, range_end in _merge_ranges(ranges)
        ]
        data = pa.Table.from_pylist(rows, schema=SYNC_RANGES_SCHEMA)
        self._store.replace_partition(SYNC_RANGES_TABLE, selector, data)

    def synced_ranges(self, dataset: str, ts_code: str) -> list[tuple[date, date]]:
        """返回已经成功拉取并合并后的日期区间。"""
        data = self._store.read(
            SYNC_RANGES_TABLE,
            partitions={"dataset": dataset, "ts_code": ts_code},
            columns=("start_date", "end_date"),
        )
        return sorted(
            (row["start_date"], row["end_date"])
            for row in data.to_pylist()
        )

    def close(self) -> None:
        self._store.close()

    def __enter__(self) -> TushareDataStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _merge_ranges(ranges: list[tuple[date, date]]) -> list[tuple[date, date]]:
    if not ranges:
        return []
    ordered = sorted(ranges)
    merged = [ordered[0]]
    one_day = timedelta(days=1)
    for start_date, end_date in ordered[1:]:
        previous_start, previous_end = merged[-1]
        if start_date <= previous_end + one_day:
            merged[-1] = (previous_start, max(previous_end, end_date))
        else:
            merged.append((start_date, end_date))
    return merged


def _missing_ranges(
    existing: list[tuple[date, date]],
    start_date: date,
    end_date: date,
) -> list[tuple[date, date]]:
    result: list[tuple[date, date]] = []
    cursor = start_date
    one_day = timedelta(days=1)
    for covered_start, covered_end in _merge_ranges(existing):
        if covered_end < cursor:
            continue
        if covered_start > end_date:
            break
        if covered_start > cursor:
            result.append((cursor, min(end_date, covered_start - one_day)))
        cursor = max(cursor, covered_end + one_day)
        if cursor > end_date:
            break
    if cursor <= end_date:
        result.append((cursor, end_date))
    return result
