"""集中、不可变的回测配置。"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Literal

from backtest.errors import BacktestConfigurationError


def _finite_non_negative(value: float, name: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise BacktestConfigurationError(f"{name} 必须是非负数")


@dataclass(frozen=True, slots=True)
class FeeConfig:
    """A 股费用模型；政策变化按成交日期生效。"""

    commission_rate: float = 0.0003
    minimum_commission: float = 5.0
    stamp_tax_rate_before_2023_08_28: float = 0.001
    stamp_tax_rate_from_2023_08_28: float = 0.0005
    transfer_fee_rate_before_2022_04_29: float = 0.00002
    transfer_fee_rate_from_2022_04_29: float = 0.00001

    def __post_init__(self) -> None:
        for name, value in asdict(self).items():
            _finite_non_negative(value, name)


@dataclass(frozen=True, slots=True)
class ExecutionConfig:
    """开盘撮合、滑点、容量与交易单位。"""

    slippage_bps: float = 5.0
    lot_size: int = 100
    price_tick: float = 0.01
    max_previous_volume_participation: float | None = 0.10
    allow_odd_lot_full_exit: bool = True
    strict_price_limits: bool = True

    def __post_init__(self) -> None:
        _finite_non_negative(self.slippage_bps, "slippage_bps")
        _finite_non_negative(self.price_tick, "price_tick")
        if (
            isinstance(self.lot_size, bool)
            or not isinstance(self.lot_size, int)
            or self.lot_size < 1
        ):
            raise BacktestConfigurationError("lot_size 必须是正整数")
        if self.price_tick <= 0:
            raise BacktestConfigurationError("price_tick 必须大于 0")
        participation = self.max_previous_volume_participation
        if participation is not None and not 0 < participation <= 1:
            raise BacktestConfigurationError(
                "max_previous_volume_participation 必须位于 (0, 1] 或为 None"
            )


@dataclass(frozen=True, slots=True)
class UniverseConfig:
    """PIT 股票池的默认过滤规则。"""

    currency: str = "CNY"
    exchanges: tuple[str, ...] = ("BSE", "SSE", "SZSE")
    minimum_listing_sessions: int = 250
    exclude_st: bool = True

    def __post_init__(self) -> None:
        if not self.currency.strip():
            raise BacktestConfigurationError("currency 不能为空")
        if not self.exchanges or any(not item.strip() for item in self.exchanges):
            raise BacktestConfigurationError("exchanges 不能为空")
        if len(set(self.exchanges)) != len(self.exchanges):
            raise BacktestConfigurationError("exchanges 不能重复")
        if (
            isinstance(self.minimum_listing_sessions, bool)
            or not isinstance(self.minimum_listing_sessions, int)
            or self.minimum_listing_sessions < 0
        ):
            raise BacktestConfigurationError("minimum_listing_sessions 必须是非负整数")


@dataclass(frozen=True, slots=True)
class CorporateActionConfig:
    """分红送转和退市处理口径。"""

    dividend_mode: Literal["after_tax", "before_tax", "disabled"] = "after_tax"
    fixed_dividend_tax_rate: float = 0.0
    write_off_delisted: bool = True
    strict_unknown_actions: bool = True

    def __post_init__(self) -> None:
        if self.dividend_mode not in {"after_tax", "before_tax", "disabled"}:
            raise BacktestConfigurationError("dividend_mode 无效")
        if not 0 <= self.fixed_dividend_tax_rate <= 1:
            raise BacktestConfigurationError("fixed_dividend_tax_rate 必须位于 [0, 1]")


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    """会影响回测结果的业务配置。"""

    start_date: date
    end_date: date
    initial_cash: float = 10_000_000.0
    annualization_sessions: int = 252
    risk_free_rate: float = 0.0
    fee: FeeConfig = field(default_factory=FeeConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    universe: UniverseConfig = field(default_factory=UniverseConfig)
    corporate_actions: CorporateActionConfig = field(default_factory=CorporateActionConfig)

    def __post_init__(self) -> None:
        if not isinstance(self.start_date, date) or not isinstance(self.end_date, date):
            raise BacktestConfigurationError("start_date/end_date 必须是 date")
        if self.start_date > self.end_date:
            raise BacktestConfigurationError("start_date 不能晚于 end_date")
        if not math.isfinite(self.initial_cash) or self.initial_cash <= 0:
            raise BacktestConfigurationError("initial_cash 必须大于 0")
        if self.annualization_sessions < 1:
            raise BacktestConfigurationError("annualization_sessions 必须是正整数")
        if not math.isfinite(self.risk_free_rate) or self.risk_free_rate <= -1:
            raise BacktestConfigurationError("risk_free_rate 必须为有限值且大于 -1")

    def to_dict(self) -> dict[str, Any]:
        """返回稳定、可 JSON 序列化的配置。"""

        payload = asdict(self)
        for name in ("start_date", "end_date"):
            payload[name] = payload[name].isoformat()
        return payload


@dataclass(frozen=True, slots=True)
class RunOptions:
    """不影响策略结果的路径和产物选项。"""

    tushare_root: Path = Path("dataset/tushare")
    qmt_root: Path = Path("dataset/qmt")
    output_root: Path = Path("runs")
    audit_data_hashes: bool = False

    def __post_init__(self) -> None:
        for name in ("tushare_root", "qmt_root", "output_root"):
            value = Path(getattr(self, name)).expanduser().resolve()
            object.__setattr__(self, name, value)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tushare_root": str(self.tushare_root),
            "qmt_root": str(self.qmt_root),
            "output_root": str(self.output_root),
            "audit_data_hashes": self.audit_data_hashes,
        }
