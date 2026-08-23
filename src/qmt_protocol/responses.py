"""qmt-agent 的 HTTP 返回结构。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from qmt_protocol.base import (
    DividendFactor,
    FinancialData,
    HistoryBar,
    HistoryQuote,
    HistoryTick,
    ProtocolModel,
    QuotePayload,
    SequencedQuote,
    TickQuote,
    XtDataPeriod,
    validate_quote,
)


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
                raise ValueError("空缓存不能包含序号边界")
            return self
        if self.oldest_seq is None or self.latest_seq is None:
            raise ValueError("非空缓存必须包含序号边界")
        if self.oldest_seq > self.latest_seq or self.latest_seq + 1 != self.next_seq:
            raise ValueError("缓存序号边界不一致")
        if self.latest_seq - self.oldest_seq + 1 != self.size:
            raise ValueError("缓存大小与序号边界不一致")
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
            raise ValueError("count 与 data 数量不一致")
        return self


class LatestQuotesResponse(ProtocolModel):
    """某类订阅的完整最新值缓存；不接受代码筛选。"""

    data: dict[str, QuotePayload]
    periods: dict[str, XtDataPeriod]
    updated_at: dict[str, int]

    @model_validator(mode="before")
    @classmethod
    def select_quote_models(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        data = value.get("data")
        periods = value.get("periods")
        if not isinstance(data, dict) or not isinstance(periods, dict):
            return value
        if set(data) != set(periods):
            raise ValueError("data 与 periods 的代码范围必须一致")
        converted = dict(value)
        converted["data"] = {
            code: validate_quote(periods[code], quote) for code, quote in data.items()
        }
        return converted

    @model_validator(mode="after")
    def validate_keys(self) -> LatestQuotesResponse:
        keys = set(self.data)
        if set(self.periods) != keys or set(self.updated_at) != keys:
            raise ValueError("data、periods 与 updated_at 的代码范围必须一致")
        return self


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
            raise ValueError("count 与 data 数量不一致")
        if self.next_seq < self.requested_seq:
            raise ValueError("next_seq 不能早于 requested_seq")
        if self.next_seq == self.requested_seq and self.count:
            raise ValueError("未推进的窗口必须为空")
        if (self.oldest_seq is None) != (self.latest_seq is None):
            raise ValueError("序号边界不一致")
        if (
            self.oldest_seq is not None
            and self.latest_seq is not None
            and self.oldest_seq > self.latest_seq
        ):
            raise ValueError("序号边界不一致")
        return self


class HistoryQueryResponse(ProtocolModel):
    period: XtDataPeriod
    data: dict[str, list[HistoryQuote]]

    @model_validator(mode="before")
    @classmethod
    def select_history_model(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        period = value.get("period")
        data = value.get("data")
        if not isinstance(period, str) or not isinstance(data, dict):
            return value
        model = HistoryTick if period == "tick" else HistoryBar
        converted = dict(value)
        converted["data"] = {
            code: [row if isinstance(row, model) else model.model_validate(row) for row in rows]
            for code, rows in data.items()
        }
        return converted


class HistoryDownloadResponse(ProtocolModel):
    completed: bool


class FinancialQueryResponse(ProtocolModel):
    data: dict[str, FinancialData]


class FinancialDownloadResponse(ProtocolModel):
    completed: bool


class DividendFactorsResponse(ProtocolModel):
    data: dict[str, list[DividendFactor]]


class ErrorResponse(ProtocolModel):
    detail: str


class QuoteSequenceErrorResponse(ErrorResponse):
    requested_seq: int | None = None
    oldest_seq: int | None = None
    latest_seq: int | None = None
