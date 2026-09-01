"""入门回测所需的少量配置。"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

from backtest.errors import BacktestConfigurationError


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    """市场范围和模拟成交参数。

    日期、初始资金决定回测范围；滑点、容量和佣金决定模拟成交成本；最后两个参数决定基础
    股票池。策略本身的排名和持仓参数放在 ``MomentumConfig`` 中。
    """

    # 回测的第一个自然日（包含当天）；真正运行的是区间内存在行情的交易日。
    start_date: date
    # 回测的最后一个自然日（包含当天）。
    end_date: date
    # 初始可用现金，单位为人民币元。默认 1,000 万元。
    initial_cash: float = 10_000_000.0
    # 单边滑点，单位为基点（bps）。1 bps = 0.01%，默认 5 bps = 0.05%。
    # 买入价在市场价上增加滑点，卖出价在市场价上扣除滑点。
    slippage_bps: float = 5.0
    # 一张订单最多使用上一交易日成交量的比例。0.10 表示最多 10%；None 表示不限制。
    max_volume_fraction: float | None = 0.10
    # 券商佣金比例。0.0003 表示成交金额的万分之三。
    commission_rate: float = 0.0003
    # 每笔成交的最低佣金，单位为人民币元。
    minimum_commission: float = 5.0
    # 股票至少上市多少个交易日才能进入候选池。250 大约是一年。
    minimum_listing_sessions: int = 250
    # True 表示候选池排除 ST、*ST 等风险警示股票。
    exclude_st: bool = True

    def __post_init__(self) -> None:
        if self.start_date > self.end_date:
            raise BacktestConfigurationError("start_date 不能晚于 end_date")
        self._positive(self.initial_cash, "initial_cash")
        self._non_negative(self.slippage_bps, "slippage_bps")
        self._non_negative(self.commission_rate, "commission_rate")
        self._non_negative(self.minimum_commission, "minimum_commission")
        if self.max_volume_fraction is not None and not 0 < self.max_volume_fraction <= 1:
            raise BacktestConfigurationError("max_volume_fraction 必须位于 (0, 1] 或为 None")
        if self.minimum_listing_sessions < 0:
            raise BacktestConfigurationError("minimum_listing_sessions 不能为负")

    @staticmethod
    def _positive(value: float, name: str) -> None:
        if not math.isfinite(value) or value <= 0:
            raise BacktestConfigurationError(f"{name} 必须是有限正数")

    @staticmethod
    def _non_negative(value: float, name: str) -> None:
        if not math.isfinite(value) or value < 0:
            raise BacktestConfigurationError(f"{name} 必须是有限非负数")

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["start_date"] = self.start_date.isoformat()
        payload["end_date"] = self.end_date.isoformat()
        return payload


@dataclass(frozen=True, slots=True)
class RunOptions:
    """只控制从哪里读取数据、是否保存结果，不改变策略收益计算。"""

    # Tushare 清洗数据的根目录。
    tushare_root: Path = Path("dataset/tushare")
    # QMT 清洗数据的根目录；当前回测主要行情由数据路由决定。
    qmt_root: Path = Path("dataset/qmt")
    # 为 None 时只返回内存结果；指定目录时写出配置、指标、订单、成交和净值文件。
    output_dir: Path | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "tushare_root", self.tushare_root.expanduser().resolve())
        object.__setattr__(self, "qmt_root", self.qmt_root.expanduser().resolve())
        if self.output_dir is not None:
            object.__setattr__(self, "output_dir", self.output_dir.expanduser().resolve())
