"""研究、回测与实盘共用的 PIT 数据读取层。"""

from data.catalog import DataCatalog
from data.config import SourceConfig
from data.errors import (
    DataAdapterError,
    DataCapabilityNotSupportedError,
    DataReaderError,
    DataResultTooLargeError,
    DataSourceNotConfiguredError,
    DataSourceUnavailableError,
)
from data.reader import ALL_SYMBOLS, DataReader, DataView
from models import DataCapability, QueryResult

__all__ = [
    "ALL_SYMBOLS",
    "DataAdapterError",
    "DataCapability",
    "DataCapabilityNotSupportedError",
    "DataCatalog",
    "DataReader",
    "DataReaderError",
    "DataResultTooLargeError",
    "DataSourceNotConfiguredError",
    "DataSourceUnavailableError",
    "DataView",
    "QueryResult",
    "SourceConfig",
]
