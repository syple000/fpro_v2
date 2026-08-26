"""研究、回测与实盘共用的 PIT 数据读取层。"""

from data.catalog import DataCatalog
from data.config import SourceConfig
from data.errors import (
    DataAdapterError,
    DataCapabilityNotSupportedError,
    DataReaderError,
    DataSourceNotConfiguredError,
    DataSourceUnavailableError,
)
from data.reader import ALL_SYMBOLS, DataReader, DataSnapshot
from models import (
    DataCapability,
    DataSourceAdapter,
    QueryParameter,
    QueryResult,
    SnapshotHandle,
    SourceRequest,
    SourceSnapshot,
)

__all__ = [
    "ALL_SYMBOLS",
    "DataAdapterError",
    "DataCapability",
    "DataCapabilityNotSupportedError",
    "DataCatalog",
    "DataReader",
    "DataReaderError",
    "DataSnapshot",
    "DataSourceAdapter",
    "DataSourceNotConfiguredError",
    "DataSourceUnavailableError",
    "QueryParameter",
    "QueryResult",
    "SnapshotHandle",
    "SourceConfig",
    "SourceRequest",
    "SourceSnapshot",
]
