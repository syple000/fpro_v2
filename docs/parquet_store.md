# Parquet 存储

`parquet_store` 是一个不包含业务概念的本地存储模块。它只支持单进程；数据文件不可变，
每个分区的 `_manifest.json` 是该分区当前可见文件的唯一依据。

## 安装依赖

```bash
uv sync --group parquet-store
```

## 最简示例

```python
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds

from parquet_store import ParquetStore, TableConfig

schema = pa.schema(
    [
        ("day", pa.date32()),
        ("id", pa.int64()),
        ("value", pa.float64()),
    ]
)
rows = [
    {"day": date(2026, 8, 16), "id": 2, "value": -1.0},
    {"day": date(2026, 8, 16), "id": 1, "value": 3.5},
]

with ParquetStore(Path("data")) as store:
    store.register(
        TableConfig(
            name="events",
            schema=schema,
            partition_by="day",
            sort_by="id",
            max_buffer_rows=100_000,
            max_buffer_bytes=64 * 1024 * 1024,
            target_rows_per_file=1_000_000,
        )
    )
    store.append("events", pa.Table.from_pylist(rows, schema=schema))
    store.flush("events")

    result = store.read(
        "events",
        partitions=[{"day": rows[0]["day"]}],
        columns=["id", "value"],
        filter=ds.field("value") > 0,
    )
```

`partition_by` 和 `sort_by` 可传单个字段名或字段名序列。分区参数推荐使用字段到值的映射；
单字段分区也可以直接传值。`read(partitions=...)` 接受分区映射序列，传 `None` 时读取所有
已有 Manifest 的分区。`filter` 接受 `pyarrow.dataset.Expression`，也接受 Parquet 风格的
单个三元组、合取三元组列表或析取范式列表。

`append` 只追加新文件；缓冲数据在 `flush` 或 `close` 前不可见。`replace_partition` 会完整
替换一个分区并丢弃该分区此前尚未刷新的缓存，空 Table 表示清空。`compact_partition` 只
重排当前 Manifest 内的物理文件，零或单文件分区不会产生新版本。

Schema 可以在末尾追加可空字段：

```python
new_schema = pa.schema([*schema, pa.field("source", pa.string())])
store.update_schema("events", new_schema)
```

旧文件不重写，读取时新增字段自动补 `null`；之后写入必须使用新 Schema。Schema 配置只保存
在当前实例中，程序重启后需要用新 Schema 重新 `register`。

第一版不提供行级更新、去重、WAL、多进程协调、后台任务、字段删除/改名/改类型或对象存储
支持。
