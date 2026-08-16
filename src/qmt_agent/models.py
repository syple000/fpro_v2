"""HTTP 请求模型和基础参数清洗。"""

from __future__ import annotations

import re
from typing import Literal, TypeAlias

from pydantic import BaseModel, Field, field_validator

_STOCK_PATTERN = re.compile(r"^[A-Z0-9_]+\.[A-Z0-9_]+$")
_MARKET_PATTERN = re.compile(r"^[A-Z0-9_]+$")
_TIME_PATTERN = re.compile(r"^(?:\d{8}|\d{14})?$")

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


def _unique_upper(values: list[str], pattern: re.Pattern[str], name: str) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        value = raw_value.strip().upper()
        if not pattern.fullmatch(value):
            raise ValueError(f"无效的{name}：{raw_value!r}")
        if value not in seen:
            seen.add(value)
            result.append(value)
    if not result:
        raise ValueError(f"{name}列表不能为空")
    return result


class MarketRequest(BaseModel):
    markets: list[str] = Field(default_factory=lambda: ["SH", "SZ"])

    @field_validator("markets")
    @classmethod
    def validate_markets(cls, values: list[str]) -> list[str]:
        return _unique_upper(values, _MARKET_PATTERN, "市场代码")


class MarketUnsubscribeRequest(BaseModel):
    markets: list[str] | None = None

    @field_validator("markets")
    @classmethod
    def validate_markets(cls, values: list[str] | None) -> list[str] | None:
        if values is None:
            return None
        return _unique_upper(values, _MARKET_PATTERN, "市场代码")


class StockRequest(BaseModel):
    stocks: list[str]

    @field_validator("stocks")
    @classmethod
    def validate_stocks(cls, values: list[str]) -> list[str]:
        return _unique_upper(values, _STOCK_PATTERN, "合约代码")


class StockSubscriptionRequest(StockRequest):
    period: XtDataPeriod


class SubscribedQuoteRequest(BaseModel):
    stocks: list[str] | None = None

    @field_validator("stocks")
    @classmethod
    def validate_stocks(cls, values: list[str] | None) -> list[str] | None:
        if values is None:
            return None
        return _unique_upper(values, _STOCK_PATTERN, "合约代码")


class SequencedQuoteRequest(SubscribedQuoteRequest):
    seq: int = Field(ge=1)
    limit: int = Field(default=100, ge=1, le=1_000)


class HistoryDownloadRequest(StockRequest):
    period: XtDataPeriod = "1d"
    start_time: str = ""
    end_time: str = ""
    mode: Literal["incremental", "full"] = "incremental"

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_time(cls, value: str) -> str:
        if not _TIME_PATTERN.fullmatch(value):
            raise ValueError("时间必须为空、YYYYMMDD 或 YYYYMMDDhhmmss")
        return value


class HistoryQueryRequest(StockRequest):
    fields: list[str] = Field(default_factory=list, max_length=100)
    period: XtDataPeriod = "1d"
    start_time: str = ""
    end_time: str = ""
    count: int = Field(default=-1, ge=-1)
    dividend_type: Literal[
        "none", "front", "back", "front_ratio", "back_ratio"
    ] = "none"
    fill_data: bool = True

    @field_validator("fields")
    @classmethod
    def normalize_fields(cls, values: list[str]) -> list[str]:
        # 空字段没有含义；重复字段直接去重，不把负担传给 xtdata。
        return list(dict.fromkeys(value.strip() for value in values if value.strip()))

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_time(cls, value: str) -> str:
        return HistoryDownloadRequest.validate_time(value)
