"""回测领域错误。"""


class BacktestError(Exception):
    """所有回测错误的基类。"""


class ConfigurationError(BacktestError, ValueError):
    """配置不合法。"""


class DataError(BacktestError):
    """历史数据缺失或时间顺序不合法。"""


class AccountError(BacktestError):
    """账户状态违反现金或持仓约束。"""


class CorporateActionError(BacktestError):
    """公司行动无法被可靠记账。"""
