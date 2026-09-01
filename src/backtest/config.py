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
    """只保留会影响基础回测结果的参数。"""

    start_date: date
    end_date: date
    initial_cash: float = 10_000_000.0
    slippage_bps: float = 5.0
    max_volume_fraction: float | None = 0.10
    commission_rate: float = 0.0003
    minimum_commission: float = 5.0
    minimum_listing_sessions: int = 250
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
    """数据路径以及可选的结果输出目录。"""

    tushare_root: Path = Path("dataset/tushare")
    qmt_root: Path = Path("dataset/qmt")
    output_dir: Path | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "tushare_root", self.tushare_root.expanduser().resolve())
        object.__setattr__(self, "qmt_root", self.qmt_root.expanduser().resolve())
        if self.output_dir is not None:
            object.__setattr__(self, "output_dir", self.output_dir.expanduser().resolve())
