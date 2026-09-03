"""数据读取层的稳定错误语义。"""


class DataReaderError(RuntimeError):
    """所有 Reader 运行期错误的基类。"""


class DataSourceNotConfiguredError(DataReaderError):
    """请求需要的逻辑路由没有配置。"""


class DataCapabilityNotSupportedError(DataReaderError):
    """已选择的来源不能完整实现请求语义。"""


class DataSourceUnavailableError(DataReaderError):
    """数据未通过质量门禁或当前不可访问。"""


class DataResultTooLargeError(DataReaderError):
    """查询结果超过 Reader 的内部安全上限。"""


class DataAdapterError(DataReaderError):
    """来源适配器违反了平台字段或类型契约。"""
