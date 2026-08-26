"""QMT 落盘字段、Arrow Schema 与表配置。"""

from __future__ import annotations

import pyarrow as pa

TICK_TABLE = "ticks"
BAR_TABLE = "bars"
DAILY_TABLE = "daily"
FINANCIAL_TABLE = "financial"
DIVIDEND_FACTOR_TABLE = "dividend_factors"

# 下载日线时显式请求并落盘的固定字段，避免上游字段变化悄悄改变本地 Schema。
DAILY_FIELDS = (
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "settelementPrice",
    "settlementPrice",
    "openInterest",
    "preClose",
    "suspendFlag",
    "dr",
    "totaldr",
)

_ENVELOPE_FIELDS = (
    pa.field("trading_date", pa.date32(), nullable=False),
    pa.field("seq", pa.int64(), nullable=False),
    pa.field("code", pa.string(), nullable=False),
    pa.field("period", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
    pa.field("subscription", pa.string(), nullable=False),
    pa.field("received_at", pa.int64(), nullable=False),
    pa.field("event_time", pa.int64()),
)

_TICK_QUOTE_FIELDS = (
    pa.field("time", pa.int64()),
    pa.field("stime", pa.string()),
    pa.field("timetag", pa.string()),
    pa.field("lastPrice", pa.float64()),
    pa.field("open", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    pa.field("lastClose", pa.float64()),
    pa.field("amount", pa.float64()),
    pa.field("volume", pa.int64()),
    pa.field("pvolume", pa.int64()),
    pa.field("stockStatus", pa.int64()),
    pa.field("openInt", pa.int64()),
    pa.field("transactionNum", pa.int64()),
    pa.field("lastSettlementPrice", pa.float64()),
    pa.field("settlementPrice", pa.float64()),
    pa.field("pe", pa.float64()),
    pa.field("askPrice", pa.list_(pa.float64())),
    pa.field("bidPrice", pa.list_(pa.float64())),
    pa.field("askVol", pa.list_(pa.int64())),
    pa.field("bidVol", pa.list_(pa.int64())),
    pa.field("volRatio", pa.float64()),
    pa.field("speed1Min", pa.float64()),
    pa.field("speed5Min", pa.float64()),
)

_BAR_QUOTE_FIELDS = (
    pa.field("time", pa.int64()),
    pa.field("open", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    pa.field("close", pa.float64()),
    pa.field("volume", pa.int64()),
    pa.field("amount", pa.float64()),
    pa.field("settelementPrice", pa.float64()),
    pa.field("settlementPrice", pa.float64()),
    pa.field("openInterest", pa.float64()),
    pa.field("preClose", pa.float64()),
    pa.field("suspendFlag", pa.int64()),
    pa.field("dr", pa.float64()),
    pa.field("totaldr", pa.float64()),
)

# 行情主体与协议模型逐字段一致，不保留通用 JSON 扩展槽。
TICK_QUOTE_COLUMNS = tuple(field.name for field in _TICK_QUOTE_FIELDS)
BAR_QUOTE_COLUMNS = tuple(field.name for field in _BAR_QUOTE_FIELDS)
TICK_SCHEMA = pa.schema(
    [*_ENVELOPE_FIELDS, pa.field("quote", pa.struct(_TICK_QUOTE_FIELDS), nullable=False)]
)
BAR_SCHEMA = pa.schema(
    [*_ENVELOPE_FIELDS, pa.field("quote", pa.struct(_BAR_QUOTE_FIELDS), nullable=False)]
)

DAILY_SCHEMA = pa.schema(
    [
        pa.field("trade_date", pa.date32(), nullable=False),
        pa.field("code", pa.string(), nullable=False),
        pa.field("adjustment", pa.string(), nullable=False),
        pa.field("time", pa.int64()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("amount", pa.float64()),
        pa.field("settelementPrice", pa.float64()),
        pa.field("openInterest", pa.float64()),
        pa.field("preClose", pa.float64()),
        pa.field("suspendFlag", pa.int64()),
        pa.field("dr", pa.float64()),
        pa.field("totaldr", pa.float64()),
    ]
)
FINANCIAL_SCHEMA = pa.schema(
    [
        pa.field("report_date", pa.date32(), nullable=False),
        pa.field("code", pa.string(), nullable=False),
        pa.field("dataset", pa.string(), nullable=False),
        pa.field("disclosure_date", pa.date32()),
        pa.field("data_json", pa.large_string(), nullable=False),
    ]
)
DIVIDEND_FACTOR_SCHEMA = pa.schema(
    [
        pa.field("ex_date", pa.date32(), nullable=False),
        pa.field("event_time", pa.int64(), nullable=False),
        pa.field("code", pa.string(), nullable=False),
        pa.field("interest", pa.float64()),
        pa.field("stockBonus", pa.float64()),
        pa.field("stockGift", pa.float64()),
        pa.field("allotNum", pa.float64()),
        pa.field("allotPrice", pa.float64()),
        pa.field("gugai", pa.float64()),
        pa.field("dr", pa.float64()),
    ]
)

TABLE_SCHEMAS = {
    TICK_TABLE: TICK_SCHEMA,
    BAR_TABLE: BAR_SCHEMA,
    DAILY_TABLE: DAILY_SCHEMA,
    FINANCIAL_TABLE: FINANCIAL_SCHEMA,
    DIVIDEND_FACTOR_TABLE: DIVIDEND_FACTOR_SCHEMA,
}

TABLE_PARTITION_BY = {
    TICK_TABLE: "trading_date",
    BAR_TABLE: "trading_date",
    DAILY_TABLE: "trade_date",
    FINANCIAL_TABLE: "report_date",
    DIVIDEND_FACTOR_TABLE: "ex_date",
}

TABLE_SORT_BY = {
    TICK_TABLE: ("event_time",),
    BAR_TABLE: ("event_time",),
    DAILY_TABLE: ("code", "adjustment"),
    FINANCIAL_TABLE: ("code", "dataset"),
    DIVIDEND_FACTOR_TABLE: ("code",),
}

TABLE_PRIMARY_KEY = {
    TICK_TABLE: ("code", "event_time"),
    BAR_TABLE: ("code", "period", "event_time"),
    DAILY_TABLE: ("code", "adjustment"),
    FINANCIAL_TABLE: ("code", "dataset"),
    DIVIDEND_FACTOR_TABLE: ("code",),
}

TABLE_DEDUPLICATE_PREFER_BY = {
    TICK_TABLE: ("received_at",),
    BAR_TABLE: ("received_at",),
    DAILY_TABLE: (),
    FINANCIAL_TABLE: ("disclosure_date",),
    DIVIDEND_FACTOR_TABLE: (),
}
