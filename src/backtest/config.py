"""回测运行配置。"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Literal

from backtest.clock import MarketHours
from backtest.errors import ConfigurationError

SUPPORTED_FREQUENCIES = frozenset({"1m", "5m", "15m", "30m", "60m", "1d"})


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    """影响回测业务结果的参数；同一配置和同一份数据应得到同一结果。"""

    # 回测交易日范围，首尾日期都包含在内。
    start_date: date
    end_date: date

    # 撮合和估值的 K 线周期；策略调用周期单独由 Strategy.schedule 决定。
    frequency: str = "1d"

    market: MarketHours = field(default_factory=MarketHours)

    # 限定证券池；None 表示让数据层读取全部证券。
    symbols: tuple[str, ...] | None = None

    # 账户初始可用现金，单位为元。
    initial_cash: float = 10_000_000.0

    # 单边成交滑点，单位为基点（1 bps = 0.01%）；买入上浮、卖出下浮。
    slippage_bps: float = 5.0

    # 单笔成交最多使用上一根已完成 K 线成交量的比例；None 表示不限制。
    volume_limit: float | None = 0.10

    # 佣金费率，例如 0.0003 表示成交金额的万分之三。
    commission_rate: float = 0.0003
    # 每笔订单的最低佣金，单位为元。
    minimum_commission: float = 5.0
    # 当前没有完整退市结算模型；只有显式选择 write_off 才按零价值核销。
    delisting_policy: Literal["error", "write_off"] = "error"
    # 当前唯一支持的分红模型；名称随配置输出，不冒充按持有批次精算税费。
    cash_dividend_model: Literal["source_cash_then_gross"] = "source_cash_then_gross"
    fractional_share_model: Literal["floor"] = "floor"

    # historical 按固定快照回测；received 按持久化分钟 Bar 的实际接收时间回放。
    bar_availability: Literal["historical", "received"] = "historical"

    def __post_init__(self) -> None:
        """在程序读取大量历史数据之前尽早拒绝无效配置。"""
        if self.start_date > self.end_date:
            raise ConfigurationError("start_date 不能晚于 end_date")
        if self.delisting_policy not in {"error", "write_off"}:
            raise ConfigurationError("delisting_policy 必须为 error 或 write_off")
        if self.cash_dividend_model != "source_cash_then_gross":
            raise ConfigurationError("当前仅支持 cash_dividend_model='source_cash_then_gross'")
        if self.fractional_share_model != "floor":
            raise ConfigurationError("当前仅支持 fractional_share_model='floor'")
        if self.frequency not in SUPPORTED_FREQUENCIES:
            raise ConfigurationError(f"不支持的 frequency: {self.frequency!r}")
        if self.bar_availability not in {"historical", "received"}:
            raise ConfigurationError("bar_availability 必须为 historical 或 received")
        if self.bar_availability == "received" and self.frequency == "1d":
            raise ConfigurationError("received 模式仅支持带接收时间的分钟 Bar")
        if self.symbols is not None:
            if not self.symbols or any(not symbol for symbol in self.symbols):
                raise ConfigurationError("symbols 必须包含有效证券代码")
            if len(set(self.symbols)) != len(self.symbols):
                raise ConfigurationError("symbols 不能重复")
        self._positive(self.initial_cash, "initial_cash")
        self._non_negative(self.slippage_bps, "slippage_bps")
        if self.slippage_bps >= 10_000:
            raise ConfigurationError("slippage_bps 必须小于 10000")
        self._non_negative(self.commission_rate, "commission_rate")
        self._non_negative(self.minimum_commission, "minimum_commission")
        if self.volume_limit is not None and not 0 < self.volume_limit <= 1:
            raise ConfigurationError("volume_limit 必须位于 (0, 1] 或为 None")

    @staticmethod
    def _positive(value: float, name: str) -> None:
        """校验有限正数。"""
        if not math.isfinite(value) or value <= 0:
            raise ConfigurationError(f"{name} 必须是有限正数")

    @staticmethod
    def _non_negative(value: float, name: str) -> None:
        """校验有限非负数。"""
        if not math.isfinite(value) or value < 0:
            raise ConfigurationError(f"{name} 必须是有限非负数")

    def to_dict(self) -> dict[str, Any]:
        """转换为适合写入 JSON 的字典。"""
        result = asdict(self)
        result["start_date"] = self.start_date.isoformat()
        result["end_date"] = self.end_date.isoformat()
        result["market"] = {
            "exchange": self.market.exchange,
            "timezone": self.market.timezone,
            "session_start": self.market.session_start.isoformat(),
            "session_end": self.market.session_end.isoformat(),
            "daily_bar_at": self.market.daily_bar_at.isoformat(),
            "opening_auction": (
                [value.isoformat() for value in self.market.opening_auction]
                if self.market.opening_auction is not None
                else None
            ),
            "segments": [
                [start.isoformat(), end.isoformat()] for start, end in self.market.segments
            ],
        }
        return result


@dataclass(frozen=True, slots=True)
class RunOptions:
    """只影响程序如何运行、不改变成交结果的路径参数。"""

    # 两个底层数据源的 Parquet 根目录。
    tushare_root: Path = Path("dataset/tushare")
    qmt_root: Path = Path("dataset/qmt")

    # None 表示只返回内存结果，不写结果文件。
    output_dir: Path | None = None

    # 可靠资料维护的持久证券代码历史；None 保持单代码兼容模式。
    security_code_history: Path | None = None

    def __post_init__(self) -> None:
        """统一把外部路径转换成展开后的绝对路径。"""
        object.__setattr__(self, "tushare_root", self.tushare_root.expanduser().resolve())
        object.__setattr__(self, "qmt_root", self.qmt_root.expanduser().resolve())
        if self.security_code_history is not None:
            object.__setattr__(
                self, "security_code_history", self.security_code_history.expanduser().resolve()
            )
        if self.output_dir is not None:
            object.__setattr__(self, "output_dir", self.output_dir.expanduser().resolve())
