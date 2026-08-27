"""与具体供应商无关的数据读取模型。

金额统一为元，价格和每股金额为元/股，数量为股，百分比使用小数，倍数保留原值。
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

import pandas as pd
import pyarrow as pa

SHANGHAI_TIMESTAMP = pa.timestamp("us", tz="Asia/Shanghai")


BAR_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("interval_start", SHANGHAI_TIMESTAMP, nullable=False),
        pa.field("interval_end", SHANGHAI_TIMESTAMP, nullable=False),
        *(pa.field(name, pa.float64()) for name in ("open", "high", "low", "close", "pre_close")),
        pa.field("volume", pa.float64()),
        pa.field("amount", pa.float64()),
    ]
)

CURRENT_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("event_time", SHANGHAI_TIMESTAMP),
        *(pa.field(name, pa.float64()) for name in ("open", "high", "low", "last", "pre_close")),
        pa.field("volume", pa.float64()),
        pa.field("amount", pa.float64()),
    ]
)

STATUS_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("suspended", pa.bool_()),
        pa.field("up_limit", pa.float64()),
        pa.field("down_limit", pa.float64()),
        pa.field("st_type", pa.string()),
    ]
)
SUSPENSION_SCHEMA = pa.schema([STATUS_SCHEMA.field("symbol"), STATUS_SCHEMA.field("suspended")])
PRICE_LIMIT_SCHEMA = pa.schema(
    [
        STATUS_SCHEMA.field("symbol"),
        STATUS_SCHEMA.field("up_limit"),
        STATUS_SCHEMA.field("down_limit"),
    ]
)
ST_STATUS_SCHEMA = pa.schema([STATUS_SCHEMA.field("symbol"), STATUS_SCHEMA.field("st_type")])

DAILY_METRICS_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("trade_date", pa.date32(), nullable=False),
        pa.field("close", pa.float64()),
        *(pa.field(name, pa.float64()) for name in ("turnover_rate", "turnover_rate_f")),
        *(
            pa.field(name, pa.float64())
            for name in ("volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm")
        ),
        *(pa.field(name, pa.float64()) for name in ("dv_ratio", "dv_ttm")),
        *(pa.field(name, pa.float64()) for name in ("total_share", "float_share", "free_share")),
        *(pa.field(name, pa.float64()) for name in ("total_mv", "circ_mv")),
    ]
)

MONEYFLOW_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("trade_date", pa.date32(), nullable=False),
        *(
            pa.field(name, pa.float64())
            for name in (
                "buy_sm_volume",
                "buy_sm_amount",
                "sell_sm_volume",
                "sell_sm_amount",
                "buy_md_volume",
                "buy_md_amount",
                "sell_md_volume",
                "sell_md_amount",
                "buy_lg_volume",
                "buy_lg_amount",
                "sell_lg_volume",
                "sell_lg_amount",
                "buy_elg_volume",
                "buy_elg_amount",
                "sell_elg_volume",
                "sell_elg_amount",
                "net_volume",
                "net_amount",
            )
        ),
    ]
)

ADJUSTMENT_FACTOR_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("trade_date", pa.date32(), nullable=False),
        pa.field("factor", pa.float64()),
    ]
)

DIVIDEND_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("visible_at", SHANGHAI_TIMESTAMP, nullable=False),
        pa.field("end_date", pa.date32()),
        pa.field("ann_date", pa.date32()),
        pa.field("div_proc", pa.string()),
        pa.field("stock_dividend", pa.float64()),
        pa.field("stock_bonus_rate", pa.float64()),
        pa.field("stock_conversion_rate", pa.float64()),
        pa.field("cash_dividend", pa.float64()),
        pa.field("cash_dividend_before_tax", pa.float64()),
        pa.field("record_date", pa.date32()),
        pa.field("ex_date", pa.date32()),
        pa.field("pay_date", pa.date32()),
        pa.field("listing_date", pa.date32()),
        pa.field("implementation_ann_date", pa.date32()),
        pa.field("base_date", pa.date32()),
        pa.field("base_share", pa.float64()),
    ]
)

INDUSTRY_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("level", pa.int8(), nullable=False),
        pa.field("industry_code", pa.string()),
        pa.field("industry_name", pa.string()),
    ]
)

SESSION_SCHEMA = pa.schema(
    [
        pa.field("cal_date", pa.date32(), nullable=False),
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("is_open", pa.bool_(), nullable=False),
        pa.field("previous_session", pa.date32()),
    ]
)


class DataCapability(StrEnum):
    """稳定的逻辑数据集能力键。"""

    # 市场行情与交易状态。
    # 日线行情：每日开、高、低、收、成交量和成交额。
    DAILY_BARS = "market.daily_bars"
    # 日内 K 线：1 分钟、5 分钟等交易日内周期行情。
    INTRADAY_BARS = "market.intraday_bars"
    # 实时行情：最新价、买卖盘及实时成交信息。
    REALTIME_QUOTES = "market.realtime_quotes"
    # 每日估值指标：PE、PB、换手率和市值等。
    DAILY_METRICS = "market.daily_metrics"
    # 个股资金流向：不同大小订单的买卖金额及净流入。
    MONEYFLOW = "market.moneyflow"
    # 停复牌状态。
    SUSPENSIONS = "market.suspensions"
    # 每日涨停价和跌停价。
    PRICE_LIMITS = "market.price_limits"
    # ST、*ST 等风险警示状态。
    ST_STATUS = "market.st_status"

    # 公司基本面与财务数据。
    # 利润表。
    INCOME = "fundamentals.income"
    # 资产负债表。
    BALANCE_SHEET = "fundamentals.balance_sheet"
    # 现金流量表。
    CASHFLOW = "fundamentals.cashflow"
    # ROE、毛利率、每股收益等财务指标。
    INDICATORS = "fundamentals.indicators"
    # 公司在正式财报前发布的业绩预告区间。
    FORECAST = "fundamentals.forecast"
    # 公司初步核算后发布的业绩快报。
    EXPRESS = "fundamentals.express"
    # 会计师事务所出具的财务审计意见。
    AUDIT = "fundamentals.audit"

    # 公司行为。
    # 现金分红、送股、转增及其实施日期。
    DIVIDENDS = "corporate_actions.dividends"
    # 用于计算前复权、后复权价格的复权因子。
    ADJUSTMENT_FACTORS = "corporate_actions.adjustment_factors"

    # 股票所属的行业分类及层级。
    INDUSTRY = "classification.industry"
    # 交易日历：是否开市及上一交易日；不是登录会话。
    SESSIONS = "calendar.sessions"


@dataclass(frozen=True, slots=True)
class QueryResult:
    """一次数据查询的结果及必要元数据。"""

    table: pa.Table
    as_of: datetime
    sources: tuple[str, ...]
    truncated: bool

    def to_pandas(self) -> pd.DataFrame:
        """显式转换为 pandas DataFrame。"""
        return self.table.to_pandas()

    def iter_batches(self, *, batch_size: int = 65_536) -> Iterator[pa.RecordBatch]:
        """按固定大小遍历 Arrow 批次。"""
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise ValueError("batch_size 必须是正整数")
        yield from self.table.to_batches(max_chunksize=batch_size)
