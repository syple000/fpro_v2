"""QMT 协议的基础类型和业务数据结构。"""

from __future__ import annotations

import math
from datetime import date
from typing import Annotated, Literal, TypeAlias

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, JsonValue, model_validator

from fpro_common import normalise_unix_timestamp_us, require_utc_us

XtDataPeriod: TypeAlias = Literal[
    "tick",
    "1m",
    "5m",
    "15m",
    "30m",
    "1h",
    "1d",
    "1w",
    "1mon",
    "1q",
    "1hy",
    "1y",
]
QuoteSource: TypeAlias = Literal["market", "stock"]
HistoryMode: TypeAlias = Literal["incremental", "full"]
DividendType: TypeAlias = Literal["none", "front", "back", "front_ratio", "back_ratio"]
FinancialTable: TypeAlias = Literal[
    "Balance",
    "Income",
    "CashFlow",
    "Capital",
    "Holdernum",
    "Top10holder",
    "Top10flowholder",
    "Pershareindex",
]
FinancialReportType: TypeAlias = Literal["report_time", "announce_time"]


def _finite_float(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("布尔值不能作为行情数值")
    if isinstance(value, (int, float)):
        converted = float(value)
        return converted if math.isfinite(converted) else None
    return value


def _float_list(value: object) -> object:
    if not isinstance(value, (list, tuple)):
        return value
    return [_finite_float(item) for item in value]


def _utc_timestamp_us(value: object) -> object:
    try:
        return require_utc_us(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(str(exc)) from exc


def _xt_timestamp_us(value: object) -> object:
    converted = normalise_unix_timestamp_us(value)
    if converted is None:
        raise ValueError("XtData time 必须是可转换为 int64 微秒的整数")
    return converted


def unix_timestamp_to_utc_us(value: object) -> int | None:
    """把 XtData 的秒/毫秒/微秒/纳秒时间戳统一为 Unix Epoch 微秒。"""
    return normalise_unix_timestamp_us(value)


OptionalFiniteFloat = Annotated[float | None, BeforeValidator(_finite_float)]
OptionalFloatList = Annotated[list[float] | None, BeforeValidator(_float_list)]
UtcTimestampUs = Annotated[int, BeforeValidator(_utc_timestamp_us)]
XtTimestampUs = Annotated[int, BeforeValidator(_xt_timestamp_us)]


class ProtocolModel(BaseModel):
    """协议信封和确定结构共同使用的严格规则。"""

    model_config = ConfigDict(extra="forbid", strict=True, validate_default=True)


class QuoteModel(BaseModel):
    """允许保留券商客户端扩展字段的行情明细基类。"""

    model_config = ConfigDict(extra="allow", strict=True, validate_default=True)
    __pydantic_extra__: dict[str, JsonValue] = Field(  # pyright: ignore[reportIncompatibleVariableOverride]
        init=False
    )


class TickQuote(QuoteModel):
    """XtData tick / 全推行情。快照接口可能返回不完整字段。"""

    time: XtTimestampUs | None = None
    stime: str | None = None
    timetag: str | None = None
    lastPrice: OptionalFiniteFloat = None
    open: OptionalFiniteFloat = None
    high: OptionalFiniteFloat = None
    low: OptionalFiniteFloat = None
    lastClose: OptionalFiniteFloat = None
    amount: OptionalFiniteFloat = None
    volume: int | None = None
    pvolume: int | None = None
    stockStatus: int | None = None
    openInt: int | None = None
    transactionNum: int | None = None
    lastSettlementPrice: OptionalFiniteFloat = None
    settlementPrice: OptionalFiniteFloat = None
    pe: OptionalFiniteFloat = None
    askPrice: OptionalFloatList = None
    bidPrice: OptionalFloatList = None
    askVol: list[int] | None = None
    bidVol: list[int] | None = None
    volRatio: OptionalFiniteFloat = None
    speed1Min: OptionalFiniteFloat = None
    speed5Min: OptionalFiniteFloat = None


class BarQuote(QuoteModel):
    """XtData 分钟线、日线等 K 线。"""

    time: XtTimestampUs | None = None
    open: OptionalFiniteFloat = None
    high: OptionalFiniteFloat = None
    low: OptionalFiniteFloat = None
    close: OptionalFiniteFloat = None
    volume: int | None = None
    amount: OptionalFiniteFloat = None
    settelementPrice: OptionalFiniteFloat = None
    settlementPrice: OptionalFiniteFloat = None
    openInterest: OptionalFiniteFloat = None
    preClose: OptionalFiniteFloat = None
    suspendFlag: int | None = None
    dr: OptionalFiniteFloat = None
    totaldr: OptionalFiniteFloat = None


QuotePayload: TypeAlias = TickQuote | BarQuote


def quote_model_for_period(period: XtDataPeriod) -> type[TickQuote] | type[BarQuote]:
    return TickQuote if period == "tick" else BarQuote


def validate_quote(period: XtDataPeriod, value: object) -> QuotePayload:
    """按周期选择确定的行情结构，避免联合类型猜测。"""
    model = quote_model_for_period(period)
    if isinstance(value, model):
        return value
    return model.model_validate(value)


class TabularFrame(ProtocolModel):
    """pandas DataFrame 的稳定 split 基础结构。"""

    index: list[JsonValue]
    columns: list[str]
    data: list[list[JsonValue]]

    @model_validator(mode="after")
    def validate_shape(self) -> TabularFrame:
        if len(self.index) != len(self.data):
            raise ValueError("DataFrame index 与 data 行数不一致")
        if len(self.columns) != len(set(self.columns)):
            raise ValueError("DataFrame columns 不能重复")
        width = len(self.columns)
        if any(len(row) != width for row in self.data):
            raise ValueError("DataFrame data 行宽与 columns 不一致")
        return self


class HistoryFrame(TabularFrame):
    """历史行情 DataFrame；列由查询 fields 决定。"""


class FinancialFrame(TabularFrame):
    """单张财务报表 DataFrame；index 语义由 response.report_type 指定。"""


class DividendFactor(ProtocolModel):
    """XtData 时间戳对应的一条 QMT 除权记录。"""

    event_time: XtTimestampUs
    interest: OptionalFiniteFloat = None
    stockBonus: OptionalFiniteFloat = None
    stockGift: OptionalFiniteFloat = None
    allotNum: OptionalFiniteFloat = None
    allotPrice: OptionalFiniteFloat = None
    gugai: OptionalFiniteFloat = None
    dr: OptionalFiniteFloat = None
    extra: dict[str, JsonValue] = Field(default_factory=dict)


class SequencedQuote(ProtocolModel):
    seq: int = Field(ge=1)
    code: str
    period: XtDataPeriod
    source: QuoteSource
    subscription: str
    received_at: UtcTimestampUs
    event_time: UtcTimestampUs = Field(init=False)
    quote: QuotePayload

    @model_validator(mode="before")
    @classmethod
    def select_quote_model(cls, value: object) -> object:
        if not isinstance(value, dict) or "period" not in value or "quote" not in value:
            return value
        converted = dict(value)
        quote = validate_quote(converted["period"], converted["quote"])
        converted["quote"] = quote
        converted["event_time"] = quote.time
        return converted


class QuoteEvent(SequencedQuote):
    """qmt-receiver 写入存储后向 platform 队列投递的结构。"""

    trading_date: date
