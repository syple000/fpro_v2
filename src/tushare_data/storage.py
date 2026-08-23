"""Tushare 数据在 :mod:`parquet_store` 上的薄存储层。"""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from threading import RLock
from uuid import uuid4

import pyarrow as pa

from fpro_common import utc_now_us
from parquet_store import ParquetStore, TableConfig
from tushare_data.schemas import (
    TABLE_DEDUPLICATE_PREFER_BY,
    TABLE_PARTITION_BY,
    TABLE_PRIMARY_KEY,
    TABLE_SCHEMAS,
    TABLE_SORT_BY,
)


class TushareDataStore:
    """注册固定表，并按原始业务日期写入 Tushare 数据。"""

    def __init__(self, root: str | Path) -> None:
        root_path = Path(root).expanduser().resolve()
        self._store = ParquetStore(root_path)
        self._sync_all_meta_dir = root_path / "_meta" / "sync_all"
        self._sync_all_meta_lock = RLock()
        for table_name, schema in TABLE_SCHEMAS.items():
            self._store.register(
                TableConfig(
                    name=table_name,
                    schema=schema,
                    partition_by=TABLE_PARTITION_BY[table_name],
                    sort_by=TABLE_SORT_BY[table_name],
                    primary_key=TABLE_PRIMARY_KEY[table_name],
                    deduplicate_prefer_by=(TABLE_DEDUPLICATE_PREFER_BY[table_name] or None),
                )
            )

    def write(self, dataset: str, data: pa.Table) -> int:
        """按业务日期追加数据，落盘后立即整理所有受影响分区。"""
        if dataset not in TABLE_SCHEMAS:
            raise ValueError(f"未知数据表: {dataset}")
        schema = TABLE_SCHEMAS[dataset]
        if not data.schema.equals(schema, check_metadata=True):
            raise ValueError(f"{dataset} 输入 Schema 不匹配")

        partition_by = TABLE_PARTITION_BY[dataset]
        partitions: set[date] = set()
        partition_values = data.column(partition_by).to_pylist()
        for partition_value in partition_values:
            if not isinstance(partition_value, date):
                raise ValueError(f"{dataset} 返回了无效 {partition_by}: {partition_value!r}")
            partitions.add(partition_value)

        self._store.append(dataset, data)
        self._store.flush(dataset)
        for partition_value in sorted(partitions):
            self._store.compact_partition(dataset, partition_value)
        return data.num_rows

    def read(
        self,
        dataset: str,
        partition: date | Sequence[date],
        *,
        ts_code: str | None = None,
    ) -> pa.Table:
        """读取一个或多个业务日期分区，并可继续过滤股票。"""
        filters: list[tuple[str, str, object]] = []
        if ts_code is not None:
            if "ts_code" not in TABLE_SCHEMAS[dataset].names:
                raise ValueError(f"{dataset} 没有 ts_code 字段")
            filters.append(("ts_code", "=", ts_code))
        result = self._store.read(dataset, partitions=partition, filter=filters or None)
        return result.sort_by([(name, "ascending") for name in TABLE_SORT_BY[dataset]])

    def _sync_all_completed_ranges(self, dataset: str) -> list[tuple[date, date]]:
        """读取 sync_all 已完整拉取的日期闭区间。"""
        if dataset not in TABLE_SCHEMAS:
            raise ValueError(f"未知数据表: {dataset}")
        path = self._sync_all_meta_dir / f"{dataset}.json"
        with self._sync_all_meta_lock:
            if not path.exists():
                return []
            try:
                document: object = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError(f"无法读取 sync_all 元数据: {path}") from exc
        if not isinstance(document, dict):
            raise ValueError(f"sync_all 元数据格式错误: {path}")
        if document.get("version") != 1 or document.get("dataset") != dataset:
            raise ValueError(f"sync_all 元数据版本或数据表不匹配: {path}")
        raw_ranges = document.get("completed_ranges")
        if not isinstance(raw_ranges, list):
            raise ValueError(f"sync_all 元数据缺少 completed_ranges: {path}")

        ranges: list[tuple[date, date]] = []
        for raw_range in raw_ranges:
            if not isinstance(raw_range, dict):
                raise ValueError(f"sync_all 完成区间格式错误: {path}")
            raw_start = raw_range.get("start_date")
            raw_end = raw_range.get("end_date")
            if not isinstance(raw_start, str) or not isinstance(raw_end, str):
                raise ValueError(f"sync_all 完成区间日期格式错误: {path}")
            try:
                start_date = date.fromisoformat(raw_start)
                end_date = date.fromisoformat(raw_end)
            except ValueError as exc:
                raise ValueError(f"sync_all 完成区间日期无效: {path}") from exc
            if start_date > end_date:
                raise ValueError(f"sync_all 完成区间起止颠倒: {path}")
            ranges.append((start_date, end_date))
        return _merge_date_ranges(ranges)

    def _mark_sync_all_completed(
        self,
        dataset: str,
        start_date: date,
        end_date: date,
    ) -> None:
        """在数据落盘成功后原子提交一个 sync_all 完成区间。"""
        if start_date > end_date:
            raise ValueError("sync_all 完成区间起止颠倒")
        with self._sync_all_meta_lock:
            ranges = _merge_date_ranges(
                [*self._sync_all_completed_ranges(dataset), (start_date, end_date)]
            )
            document = {
                "version": 1,
                "dataset": dataset,
                "updated_at": utc_now_us(),
                "completed_ranges": [
                    {
                        "start_date": range_start.isoformat(),
                        "end_date": range_end.isoformat(),
                    }
                    for range_start, range_end in ranges
                ],
            }
            self._sync_all_meta_dir.mkdir(parents=True, exist_ok=True)
            path = self._sync_all_meta_dir / f"{dataset}.json"
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

    def __enter__(self) -> TushareDataStore:
        return self

    def __exit__(self, *_: object) -> None:
        self._store.close()


def _merge_date_ranges(ranges: Sequence[tuple[date, date]]) -> list[tuple[date, date]]:
    """合并重叠或相邻的日期闭区间。"""
    merged: list[tuple[date, date]] = []
    for start_date, end_date in sorted(ranges):
        if not merged or start_date.toordinal() > merged[-1][1].toordinal() + 1:
            merged.append((start_date, end_date))
            continue
        previous_start, previous_end = merged[-1]
        merged[-1] = (previous_start, max(previous_end, end_date))
    return merged
