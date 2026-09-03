"""研究、回测与实盘共用的 PIT 数据读取层。"""

from market_data.catalog import DataCatalog
from market_data.config import SourceConfig
from market_data.errors import (
    DataAdapterError,
    DataCapabilityNotSupportedError,
    DataReaderError,
    DataResultTooLargeError,
    DataSourceNotConfiguredError,
    DataSourceUnavailableError,
)
from market_data.protocols import DataAdapter
from market_data.reader import ALL_SYMBOLS, DataReader, DataView
from models import QueryResult

__all__ = [
    "ALL_SYMBOLS",
    "DataAdapterError",
    "DataAdapter",
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
