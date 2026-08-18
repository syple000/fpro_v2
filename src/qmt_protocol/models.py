"""qmt-agent 与 qmt-receiver 共用的数据协议。

这里的行情字段来自 XtData 官方字段表，并使用本机 miniQMT 实际调用结果校正。
协议外层严格禁止未知字段；行情明细允许并保留未知字段，以兼容券商客户端扩展。
"""

from __future__ import annotations

import math
from datetime import date
from typing import Annotated, Literal, TypeAlias

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    JsonValue,
    model_validator,
)

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


def _finite_float(value: object) -> object:
    """把 XtData 偶尔返回的整型价格统一成有限浮点数。"""
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
    """接入 XtData 时识别其历史版本使用的时间单位，并统一为微秒。"""
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
    """所有非行情明细协议共同使用的严格规则。"""

    model_config = ConfigDict(extra="forbid", strict=True, validate_default=True)


class QuoteModel(BaseModel):
    """行情明细基类。

    XtData 会随客户端版本、市场和行情级别增加字段。未知字段不得静默丢弃，
    因此这里以 JSON 值类型完整保留；qmt-agent 的边界适配器会记录 DEBUG 日志。
    """

    model_config = ConfigDict(extra="allow", strict=True, validate_default=True)
    __pydantic_extra__: dict[str, JsonValue] = Field(  # pyright: ignore[reportIncompatibleVariableOverride]
        init=False
    )


class TickQuote(QuoteModel):
    """XtData tick / 全推快照字段。

    字段设为可选是因为实测 `get_full_tick`、全推回调和不同客户端版本的字段集合
    并不完全相同；已出现的字段仍会严格校验其值类型。
    """

    # XtData 原始值常为毫秒；模型边界统一成 Unix Epoch 微秒整数。
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
    """XtData 分钟线、日线等 K 线字段。"""

    # XtData 原始值常为毫秒；模型边界统一成 Unix Epoch 微秒整数。
    time: XtTimestampUs | None = None
    open: OptionalFiniteFloat = None
    high: OptionalFiniteFloat = None
    low: OptionalFiniteFloat = None
    close: OptionalFiniteFloat = None
    volume: int | None = None
    amount: OptionalFiniteFloat = None
    # 官方历史字段沿用拼写错误 settelementPrice；实时回调实测为 settlementPrice。
    # 两个字段均显式定义并原样保留，不能擅自合并导致调用方无法判断来源。
    settelementPrice: OptionalFiniteFloat = None
    settlementPrice: OptionalFiniteFloat = None
    openInterest: OptionalFiniteFloat = None
    preClose: OptionalFiniteFloat = None
    suspendFlag: int | None = None
    # 当前东北证券 miniQMT 的 K 线回调实测会额外返回这两个复权字段。
    dr: OptionalFiniteFloat = None
    totaldr: OptionalFiniteFloat = None


QuotePayload: TypeAlias = TickQuote | BarQuote


def quote_model_for_period(period: XtDataPeriod) -> type[TickQuote] | type[BarQuote]:
    return TickQuote if period == "tick" else BarQuote


def validate_quote(period: XtDataPeriod, value: object) -> QuotePayload:
    """按周期选择确定的行情结构，避免 Pydantic 对联合类型进行猜测。"""
    model = quote_model_for_period(period)
    if isinstance(value, model):
        return value
    return model.model_validate(value)


class QuoteSequenceStatus(ProtocolModel):
    oldest_seq: int | None
    latest_seq: int | None
    next_seq: int = Field(ge=1)
    size: int = Field(ge=0)
    capacity: int = Field(ge=1)

    @model_validator(mode="after")
    def validate_bounds(self) -> QuoteSequenceStatus:
        if self.size == 0:
            if self.oldest_seq is not None or self.latest_seq is not None:
                raise ValueError("空行情缓存不能包含最旧或最新序号")
            return self
        if self.oldest_seq is None or self.latest_seq is None:
            raise ValueError("非空行情缓存必须包含最旧和最新序号")
        if self.oldest_seq > self.latest_seq or self.latest_seq + 1 != self.next_seq:
            raise ValueError("行情缓存序号边界不一致")
        if self.latest_seq - self.oldest_seq + 1 != self.size:
            raise ValueError("行情缓存大小与序号边界不一致")
        return self


class SubscriptionStatus(ProtocolModel):
    instance_id: str
    markets: list[str]
    stocks: list[str]
    stock_periods: dict[str, XtDataPeriod]
    stock_count: int = Field(ge=0)
    stock_limit: int = Field(ge=1, le=50)
    quote_sequence: QuoteSequenceStatus

    @model_validator(mode="after")
    def validate_stock_state(self) -> SubscriptionStatus:
        if self.stock_count != len(self.stocks):
            raise ValueError("stock_count 与 stocks 数量不一致")
        if set(self.stock_periods) != set(self.stocks):
            raise ValueError("stock_periods 与 stocks 范围不一致")
        return self


class HealthResponse(SubscriptionStatus):
    status: Literal["ok"]
    version: str


class MarketSubscriptionResponse(ProtocolModel):
    subscribed: list[str]
    added: list[str]
    removed: list[str]
    not_found: list[str]


class StockSubscriptionResponse(ProtocolModel):
    periods: dict[str, XtDataPeriod]
    subscribed: list[str]
    added: list[str]
    updated: list[str]
    removed: list[str]
    not_found: list[str]
    period_mismatches: dict[str, XtDataPeriod]

    @model_validator(mode="after")
    def validate_periods(self) -> StockSubscriptionResponse:
        if set(self.periods) != set(self.subscribed):
            raise ValueError("periods 与 subscribed 范围不一致")
        return self


class SnapshotResponse(ProtocolModel):
    data: dict[str, TickQuote]
    count: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_count(self) -> SnapshotResponse:
        if self.count != len(self.data):
            raise ValueError("快照 count 与 data 数量不一致")
        return self


class LatestQuotesResponse(ProtocolModel):
    data: dict[str, QuotePayload]
    updated_at: dict[str, UtcTimestampUs]
    periods: dict[str, XtDataPeriod]
    missing: list[str]
    not_subscribed: list[str]

    @model_validator(mode="before")
    @classmethod
    def select_quote_models(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        data = value.get("data")
        periods = value.get("periods")
        if not isinstance(data, dict) or not isinstance(periods, dict):
            return value
        converted = dict(value)
        converted["data"] = {
            code: validate_quote(periods[code], quote)
            for code, quote in data.items()
            if code in periods
        }
        return converted

    @model_validator(mode="after")
    def validate_keys(self) -> LatestQuotesResponse:
        keys = set(self.data)
        if set(self.updated_at) != keys or set(self.periods) != keys:
            raise ValueError("data、updated_at 和 periods 的代码范围必须一致")
        return self


class SequencedQuote(ProtocolModel):
    seq: int = Field(ge=1)
    code: str
    period: XtDataPeriod
    source: QuoteSource
    subscription: str
    received_at: UtcTimestampUs
    event_at: UtcTimestampUs | None = None
    quote: QuotePayload

    @model_validator(mode="before")
    @classmethod
    def select_quote_model(cls, value: object) -> object:
        if not isinstance(value, dict) or "period" not in value or "quote" not in value:
            return value
        converted = dict(value)
        quote = validate_quote(converted["period"], converted["quote"])
        converted["quote"] = quote
        converted["event_at"] = quote.time
        return converted


class QuoteSequenceResponse(ProtocolModel):
    data: list[SequencedQuote]
    count: int = Field(ge=0)
    requested_seq: int = Field(ge=1)
    next_seq: int = Field(ge=1)
    oldest_seq: int | None = Field(default=None, ge=1)
    latest_seq: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_window(self) -> QuoteSequenceResponse:
        if self.count != len(self.data):
            raise ValueError("顺序行情 count 与 data 数量不一致")
        if self.next_seq < self.requested_seq:
            raise ValueError("顺序行情 next_seq 不能早于 requested_seq")
        if self.next_seq == self.requested_seq and self.count:
            raise ValueError("未推进的顺序行情必须是空批次")
        if (self.oldest_seq is None) != (self.latest_seq is None):
            raise ValueError("顺序行情边界不一致")
        if (
            self.oldest_seq is not None
            and self.latest_seq is not None
            and self.oldest_seq > self.latest_seq
        ):
            raise ValueError("顺序行情边界不一致")
        return self


class HistoryFrame(ProtocolModel):
    """一个 pandas DataFrame 的稳定 JSON split 编码。"""

    index: list[JsonValue]
    columns: list[str]
    data: list[list[JsonValue]]

    @model_validator(mode="after")
    def validate_shape(self) -> HistoryFrame:
        if len(self.index) != len(self.data):
            raise ValueError("历史数据 index 与 data 行数不一致")
        width = len(self.columns)
        if any(len(row) != width for row in self.data):
            raise ValueError("历史数据行宽与 columns 不一致")
        return self


class HistoryQueryResponse(ProtocolModel):
    data: dict[str, HistoryFrame]


class HistoryDownloadResponse(ProtocolModel):
    stocks: list[str]
    period: XtDataPeriod
    mode: HistoryMode
    completed: bool


class ErrorResponse(ProtocolModel):
    detail: str


class QuoteSequenceErrorResponse(ErrorResponse):
    requested_seq: int | None = None
    oldest_seq: int | None = None
    latest_seq: int | None = None


class QuoteEvent(SequencedQuote):
    """qmt-receiver 写入存储缓冲区后向 platform 队列投递的结构。"""

    trading_date: date
