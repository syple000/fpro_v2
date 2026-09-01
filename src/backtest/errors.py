"""回测模块的明确业务错误。"""


class BacktestError(Exception):
    """所有回测错误的基类。"""


class BacktestConfigurationError(BacktestError, ValueError):
    """回测或策略配置无效。"""


class BacktestDataError(BacktestError):
    """数据缺失、覆盖不足或违反 PIT 约束。"""


class AccountInvariantError(BacktestError):
    """现金、持仓或总资产不再满足账户不变量。"""


class UnsupportedCorporateActionError(BacktestError):
    """持仓遇到了第一版尚不能正确记账的公司行动。"""
