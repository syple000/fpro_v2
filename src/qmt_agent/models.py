"""HTTP 请求模型和基础参数清洗。"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from fpro_common import datetime_to_utc_us
from qmt_protocol import (
    DividendType,
    FinancialReportType,
    FinancialTable,
    HistoryMode,
    XtDataPeriod,
)

logger = logging.getLogger(__name__)

_STOCK_PATTERN = re.compile(r"^[A-Z0-9_]+\.[A-Z0-9_]+$")
_MARKET_PATTERN = re.compile(r"^[A-Z0-9_]+$")
_TIME_PATTERN = re.compile(r"^(?:\d{8}|\d{14})?$")
_QMT_TIMEZONE = timezone(timedelta(hours=8), name="Asia/Shanghai")


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
        else:
            # 请求中的重复项不会传给 XtData；明确记录，避免清洗行为不可见。
            logger.debug("丢弃重复的%s值：原始值=%r，规范值=%s", name, raw_value, value)
    if not result:
        raise ValueError(f"{name}列表不能为空")
    return result


def _validate_xt_time(value: str) -> str:
    if not _TIME_PATTERN.fullmatch(value):
        raise ValueError("时间必须为空、YYYYMMDD 或 YYYYMMDDhhmmss")
    if not value:
        return value

    try:
        _time_boundary(value, end_of_day=False)
    except ValueError as exc:
        raise ValueError(f"时间不是有效日期或时刻：{value}") from exc
    return value


def _time_boundary(value: str, *, end_of_day: bool) -> int | None:
    if not value:
        return None
    if len(value) == 14:
        local_time = datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=_QMT_TIMEZONE)
        return datetime_to_utc_us(local_time)

    parsed = datetime.strptime(value, "%Y%m%d")
    if end_of_day:
        parsed = parsed.replace(hour=23, minute=59, second=59)
    return datetime_to_utc_us(parsed.replace(tzinfo=_QMT_TIMEZONE))


def _validate_time_range(start_time: str, end_time: str) -> None:
    start = _time_boundary(start_time, end_of_day=False)
    end = _time_boundary(end_time, end_of_day=True)
    if start is not None and end is not None and start > end:
        raise ValueError("start_time 不能晚于 end_time")


class StrictRequestModel(BaseModel):
    """所有 HTTP 请求共同使用的严格校验规则。"""

    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        validate_default=True,
    )


class MarketRequest(StrictRequestModel):
    markets: list[str] = Field(default_factory=lambda: ["SH", "SZ"])

    @field_validator("markets")
    @classmethod
    def validate_markets(cls, values: list[str]) -> list[str]:
        return _unique_upper(values, _MARKET_PATTERN, "市场代码")


class MarketUnsubscribeRequest(StrictRequestModel):
    markets: list[str] | None = None

    @field_validator("markets")
    @classmethod
    def validate_markets(cls, values: list[str] | None) -> list[str] | None:
        if values is None:
            return None
        return _unique_upper(values, _MARKET_PATTERN, "市场代码")


class StockRequest(StrictRequestModel):
    stocks: list[str]

    @field_validator("stocks")
    @classmethod
    def validate_stocks(cls, values: list[str]) -> list[str]:
        return _unique_upper(values, _STOCK_PATTERN, "合约代码")


class StockSubscriptionRequest(StockRequest):
    period: XtDataPeriod


class SubscribedQuoteRequest(StrictRequestModel):
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
    wait_ms: int = Field(default=0, ge=0, le=30_000)


class HistoryDownloadRequest(StockRequest):
    period: XtDataPeriod = "1d"
    start_time: str = ""
    end_time: str = ""
    mode: HistoryMode = "incremental"

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_time(cls, value: str) -> str:
        return _validate_xt_time(value)

    @model_validator(mode="after")
    def validate_time_range(self) -> Self:
        _validate_time_range(self.start_time, self.end_time)
        return self


class HistoryQueryRequest(StockRequest):
    fields: list[str] = Field(default_factory=list, max_length=100)
    period: XtDataPeriod = "1d"
    start_time: str = ""
    end_time: str = ""
    count: int = Field(default=-1, ge=-1)
    dividend_type: DividendType = "none"
    fill_data: bool = True

    @field_validator("fields")
    @classmethod
    def normalize_fields(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_value in values:
            value = raw_value.strip()
            if not value:
                # 空字段没有含义且不会传给 XtData；按要求留下可追踪的 DEBUG 记录。
                logger.debug("丢弃历史查询中的空字段名：原始值=%r", raw_value)
                continue
            if value in seen:
                # 重复字段直接去重，避免 XtData 返回重复列。
                logger.debug("丢弃历史查询中的重复字段名：%s", value)
                continue
            seen.add(value)
            normalized.append(value)
        return normalized

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_time(cls, value: str) -> str:
        return _validate_xt_time(value)

    @model_validator(mode="after")
    def validate_time_range(self) -> Self:
        _validate_time_range(self.start_time, self.end_time)
        return self


class FinancialRequest(StockRequest):
    tables: list[FinancialTable] = Field(default_factory=list)
    start_time: str = ""
    end_time: str = ""

    @field_validator("tables")
    @classmethod
    def unique_tables(cls, values: list[FinancialTable]) -> list[FinancialTable]:
        return list(dict.fromkeys(values))

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_time(cls, value: str) -> str:
        return _validate_xt_time(value)

    @model_validator(mode="after")
    def validate_time_range(self) -> Self:
        _validate_time_range(self.start_time, self.end_time)
        return self


class FinancialQueryRequest(FinancialRequest):
    report_type: FinancialReportType = "report_time"


class DividendFactorsRequest(StockRequest):
    start_time: str = ""
    end_time: str = ""

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_time(cls, value: str) -> str:
        return _validate_xt_time(value)

    @model_validator(mode="after")
    def validate_time_range(self) -> Self:
        _validate_time_range(self.start_time, self.end_time)
        return self
