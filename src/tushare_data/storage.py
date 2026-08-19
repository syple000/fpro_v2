"""Tushare 数据在 :mod:`parquet_store` 上的薄存储层。"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path

import pyarrow as pa

from fpro_common import require_utc_us
from parquet_store import ParquetStore, TableConfig
from tushare_data.schemas import (
    TABLE_PARTITION_BY,
    TABLE_SCHEMAS,
    TABLE_SORT_BY,
)


class TushareDataStore:
    """注册固定表，并按可见日期写入 Tushare 数据。"""

    def __init__(self, root: str | Path) -> None:
        self._store = ParquetStore(root)
        for table_name, schema in TABLE_SCHEMAS.items():
            self._store.register(
                TableConfig(
                    name=table_name,
                    schema=schema,
                    partition_by=TABLE_PARTITION_BY[table_name],
                    sort_by=TABLE_SORT_BY[table_name],
                )
            )

    def write(self, dataset: str, data: pa.Table) -> int:
        """把完整的每日截面按日期拆分，并覆盖对应的非空日期分区。"""
        schema = TABLE_SCHEMAS[dataset]
        if not data.schema.equals(schema, check_metadata=True):
            raise ValueError(f"{dataset} 输入 Schema 不匹配")

        partition_by = TABLE_PARTITION_BY[dataset]
        indices: dict[date, list[int]] = {}
        for index, value in enumerate(data.column(partition_by).to_pylist()):
            if not isinstance(value, date):
                raise ValueError(f"{dataset} 返回了无效 {partition_by}: {value!r}")
            indices.setdefault(value, []).append(index)

        for partition_value, positions in indices.items():
            partition_data = data.take(pa.array(positions, type=pa.int64()))
            self._store.replace_partition(dataset, partition_value, partition_data)
        return data.num_rows

    def read(
        self,
        dataset: str,
        partition: date | Sequence[date],
        *,
        ts_code: str | None = None,
        visible_start: int | None = None,
        visible_end: int | None = None,
        as_of: int | None = None,
    ) -> pa.Table:
        """读取一个或多个日期分区，并可继续过滤股票和可见时间。"""
        filters: list[tuple[str, str, object]] = []
        if ts_code is not None:
            if "ts_code" not in TABLE_SCHEMAS[dataset].names:
                raise ValueError(f"{dataset} 没有 ts_code 字段")
            filters.append(("ts_code", "=", ts_code))
        if visible_start is not None:
            filters.append(("visible_at", ">=", require_utc_us(visible_start, "visible_start")))
        if visible_end is not None:
            filters.append(("visible_at", "<=", require_utc_us(visible_end, "visible_end")))
        if as_of is not None:
            filters.append(("visible_at", "<=", require_utc_us(as_of, "as_of")))
        result = self._store.read(dataset, partitions=partition, filter=filters or None)
        return result.sort_by([(TABLE_SORT_BY[dataset], "ascending")])

    def __enter__(self) -> TushareDataStore:
        return self

    def __exit__(self, *_: object) -> None:
        self._store.close()
