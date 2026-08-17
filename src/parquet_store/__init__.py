"""基于不可变文件和分区 Manifest 的本地 Parquet 存储。"""

from parquet_store.store import ParquetStore, SchemaMismatchError, TableConfig

__all__ = ["ParquetStore", "SchemaMismatchError", "TableConfig"]
