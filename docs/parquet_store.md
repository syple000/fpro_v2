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
            primary_key=("id",),
            deduplicate_prefer_by=None,
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

`partition_by`、`sort_by`、`primary_key` 和 `deduplicate_prefer_by` 可传单个字段名或字段名序列。`primary_key` 默认
为 `None`，表示分区没有逻辑主键。主键只在单个分区内生效；表级唯一键等价于
`partition_by + primary_key`。分区参数推荐使用字段到值的映射；
单字段分区也可以直接传值。`read(partitions=...)` 接受分区映射序列，传 `None` 时读取所有
已有 Manifest 的分区。`filter` 接受 `pyarrow.dataset.Expression`，也接受 Parquet 风格的
单个三元组、合取三元组列表或析取范式列表。

`append` 只追加新文件；缓冲数据在 `flush` 或 `close` 前不可见。`replace_partition` 会完整
替换一个分区并丢弃该分区此前尚未刷新的缓存，空 Table 表示清空。`compact_partition` 只
处理当前 Manifest 内的物理文件：没有主键时仅重排文件，不去重；配置主键时按文件提交时间、
同次提交的文件顺序和文件内行顺序保留最后一个物理版本，再排序和切分文件。主键按
null-safe 语义分组，多行中相同位置的 null 视为同一键值。配置主键后，即使分区只有一个文件，
也会检查并清理文件内部的重复键。

`deduplicate_prefer_by` 是可选冲突优先字段：按字段升序比较，较大值胜出；完全相同时仍由
后提交的行胜出。该配置不会在 Parquet 中增加字段。

Manifest 同时保存本次更新时间和每个文件的提交时间（UTC Unix Epoch 微秒）：

```json
{
  "version": 2,
  "updated_at": 1787155200000001,
  "files": ["part-old.parquet", "part-new.parquet"],
  "file_committed_at": {
    "part-old.parquet": 1787155200000000,
    "part-new.parquet": 1787155200000001
  }
}
```

读取和整理都会按 `file_committed_at` 排序，而不依赖文件名或 JSON 中对象键的顺序。即使系统时钟
回拨，新的提交时间也至少比当前 Manifest 大 1 微秒。没有时间字段的旧版 Manifest 仍可读取；
它原有的 `files` 数组顺序会被保留，并在下一次提交时自动补齐每文件时间。

Schema 可以在末尾追加可空字段：

```python
new_schema = pa.schema([*schema, pa.field("source", pa.string())])
store.update_schema("events", new_schema)
```

旧文件不重写，读取时新增字段自动补 `null`；之后写入必须使用新 Schema。Schema 配置只保存
在当前实例中，程序重启后需要用新 Schema 重新 `register`。

第一版不提供写入时唯一性约束、行级更新、WAL、多进程协调、后台任务、字段删除/改名/改类型
或对象存储支持；主键去重是显式调用 `compact_partition` 时发生的延迟整理。
