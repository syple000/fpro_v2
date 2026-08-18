"""按功能拆分的 Tushare 历史数据拉取函数。"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa

from fpro_common import datetime_to_utc_us, utc_us_to_datetime
from tushare_data.client import (
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_REQUESTS_PER_MINUTE,
    RateLimitedProClient,
    RequestLimiter,
)
from tushare_data.schemas import DATE_FIELDS, SOURCE_FIELDS, TABLE_SCHEMAS
from tushare_data.storage import TushareDataStore

DEFAULT_API_URL = "http://api.quicksync.cn"
SHANGHAI = ZoneInfo("Asia/Shanghai")
ANNOUNCEMENT_VISIBLE_TIME = time.max

# 时间均取数据在交易/研究程序中可以安全使用的时刻，而不是数据所属日期的零点。
# 日期型公告接口没有精确发布时间，保守地在公告日结束后才允许回测使用。
VISIBILITY_RULES: dict[str, tuple[tuple[str, ...], time]] = {
    "daily": (("trade_date",), time(16, 0)),
    "daily_basic": (("trade_date",), time(17, 0)),
    "adj_factor": (("trade_date",), time(9, 20)),
    "suspend_d": (("trade_date",), time(9, 30)),
    "stk_limit": (("trade_date",), time(8, 45)),
    "stock_st": (("trade_date",), time(9, 20)),
    "moneyflow": (("trade_date",), time(19, 0)),
    "dividend": (("ann_date", "imp_ann_date", "ex_date"), ANNOUNCEMENT_VISIBLE_TIME),
    "forecast": (("ann_date",), ANNOUNCEMENT_VISIBLE_TIME),
    "express": (("ann_date",), ANNOUNCEMENT_VISIBLE_TIME),
    "fina_audit": (("ann_date",), ANNOUNCEMENT_VISIBLE_TIME),
    "income": (("f_ann_date", "ann_date"), ANNOUNCEMENT_VISIBLE_TIME),
    "balancesheet": (("f_ann_date", "ann_date"), ANNOUNCEMENT_VISIBLE_TIME),
    "cashflow": (("f_ann_date", "ann_date"), ANNOUNCEMENT_VISIBLE_TIME),
    "fina_indicator": (("ann_date",), ANNOUNCEMENT_VISIBLE_TIME),
    "trade_cal": (("cal_date",), time(0, 0)),
}

MAX_DAYS_PER_REQUEST = {
    "daily": 3650,
    "daily_basic": 3650,
    "adj_factor": 3650,
    "suspend_d": 3650,
    "stk_limit": 3650,
    "stock_st": 730,
    "moneyflow": 3650,
    "forecast": 730,
    "express": 730,
    "fina_audit": 730,
    "income": 730,
    "balancesheet": 730,
    "cashflow": 730,
    "fina_indicator": 730,
    "trade_cal": 3650,
}


def create_pro_client(
    token: str,
    api_url: str = DEFAULT_API_URL,
    *,
    requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
) -> RateLimitedProClient:
    """按 quicksync 要求初始化客户端，并为全部接口应用同一个请求限制器。"""
    if not token.strip():
        raise ValueError("Tushare token 不能为空")
    import tushare as ts
    import tushare.pro.client as client

    # DataApi 没有公开代理地址配置项，quicksync 的接入方式要求修改该类属性。
    http_url_attribute = "_DataApi__http_url"
    setattr(client.DataApi, http_url_attribute, api_url.rstrip("/"))
    pro = ts.pro_api(token)
    limiter = RequestLimiter(requests_per_minute, max_concurrency)
    return RateLimitedProClient(pro, limiter)


def sync_daily(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """增量拉取未复权 A 股日 K 线。"""
    return _sync_ranged_api(pro, store, "daily", ts_code, start_date, end_date)


def sync_daily_basic(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """增量拉取估值、换手率、股本和市值等每日指标。"""
    return _sync_ranged_api(pro, store, "daily_basic", ts_code, start_date, end_date)


def sync_adj_factor(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """增量拉取复权因子。"""
    return _sync_ranged_api(pro, store, "adj_factor", ts_code, start_date, end_date)


def sync_suspend_d(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """增量拉取每日停牌和复牌事件。"""
    return _sync_ranged_api(pro, store, "suspend_d", ts_code, start_date, end_date)


def sync_stk_limit(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """增量拉取每日涨停价和跌停价，用于判断委托是否可成交。"""
    return _sync_ranged_api(pro, store, "stk_limit", ts_code, start_date, end_date)


def sync_stock_st(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """增量拉取每日 ST/风险警示状态；Tushare 从 2016 年开始提供。"""
    return _sync_ranged_api(pro, store, "stock_st", ts_code, start_date, end_date)


def sync_moneyflow(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """增量拉取按小、中、大、特大单统计的个股资金流向。"""
    return _sync_ranged_api(pro, store, "moneyflow", ts_code, start_date, end_date)


def sync_income(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按实际公告日期增量拉取利润表，保留 Tushare 的修订版本行。"""
    return _sync_ranged_api(pro, store, "income", ts_code, start_date, end_date)


def sync_forecast(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按公告日期增量拉取业绩预告及其更新版本。"""
    return _sync_ranged_api(pro, store, "forecast", ts_code, start_date, end_date)


def sync_express(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按公告日期增量拉取业绩快报。"""
    return _sync_ranged_api(pro, store, "express", ts_code, start_date, end_date)


def sync_fina_audit(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按公告日期增量拉取财务审计意见。"""
    return _sync_ranged_api(pro, store, "fina_audit", ts_code, start_date, end_date)


def sync_balancesheet(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按实际公告日期增量拉取资产负债表。"""
    return _sync_ranged_api(pro, store, "balancesheet", ts_code, start_date, end_date)


def sync_cashflow(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按实际公告日期增量拉取现金流量表。"""
    return _sync_ranged_api(pro, store, "cashflow", ts_code, start_date, end_date)


def sync_fina_indicator(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按公告日期增量拉取财务分析指标。"""
    return _sync_ranged_api(pro, store, "fina_indicator", ts_code, start_date, end_date)


def sync_dividend(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按公告可见时间增量保存分红送转和除权实施信息。

    Tushare dividend 接口不支持 start_date/end_date，因此仅在确有未覆盖区间时
    请求一次该股票全部记录，再只写入本地缺失区间。
    """
    return _sync_unbounded_api(
        store,
        "dividend",
        ts_code,
        start_date,
        end_date,
        lambda: pro.dividend(
            ts_code=ts_code,
            fields=",".join(SOURCE_FIELDS["dividend"]),
        ),
    )


def sync_sw_industry(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """增量保存申万一级、二级、三级行业的纳入/移出事件。

    上游当前记录会同时带 in_date 和未来 out_date。这里拆成两条事件，避免在
    in_date 时点提前看到未来的移出日期。
    """
    requested = _historical_range("sw_industry", start_date, end_date)
    if requested is None:
        return 0
    start, end = requested
    missing = store.missing_ranges("sw_industry", ts_code, start, end)
    if not missing:
        return 0

    fields = ",".join(SOURCE_FIELDS["sw_industry"])
    current = pro.index_member_all(ts_code=ts_code, is_new="Y", fields=fields)
    history = pro.index_member_all(ts_code=ts_code, is_new="N", fields=fields)
    frames = [frame for frame in (current, history) if not frame.empty]
    source = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=pd.Index(fields.split(",")))
    )
    _validate_columns(source, SOURCE_FIELDS["sw_industry"], "index_member_all")

    rows: list[dict[str, object]] = []
    seen: set[tuple[object, ...]] = set()
    for raw_row in source.to_dict("records"):
        cleaned = {name: _clean_scalar(raw_row.get(name)) for name in source.columns}
        raw_key = tuple(cleaned[name] for name in SOURCE_FIELDS["sw_industry"])
        if raw_key in seen:
            continue
        seen.add(raw_key)
        out_date = (
            _parse_date(cleaned["out_date"])
            if cleaned["out_date"] is not None
            else None
        )
        for event_type, source_date in (("IN", cleaned["in_date"]), ("OUT", out_date)):
            if source_date is None:
                continue
            event_date = _parse_date(source_date)
            is_baseline = event_type == "IN" and any(
                event_date <= range_start
                and (out_date is None or out_date >= range_start)
                for range_start, _ in missing
            )
            if not is_baseline and not _in_ranges(event_date, missing):
                continue
            rows.append(
                {
                    "ts_code": str(cleaned["ts_code"]),
                    "visible_at": _at_time(event_date, time(9, 0)),
                    "event_date": event_date,
                    "event_type": event_type,
                    "l1_code": cleaned["l1_code"],
                    "l1_name": cleaned["l1_name"],
                    "l2_code": cleaned["l2_code"],
                    "l2_name": cleaned["l2_name"],
                    "l3_code": cleaned["l3_code"],
                    "l3_name": cleaned["l3_name"],
                    "stock_name": cleaned["name"],
                }
            )

    data = pa.Table.from_pylist(rows, schema=TABLE_SCHEMAS["sw_industry"])
    store.upsert("sw_industry", ts_code, data)
    for range_start, range_end in missing:
        store.mark_synced("sw_industry", ts_code, range_start, range_end)
    return len(rows)


def sync_trade_cal(
    pro: Any,
    store: TushareDataStore,
    exchange: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按交易所增量拉取交易日历。"""
    exchange = exchange.upper()
    requested = _historical_range("trade_cal", start_date, end_date)
    if requested is None:
        return 0
    start, end = requested
    missing = store.missing_ranges("trade_cal", exchange, start, end)
    fields = ",".join(SOURCE_FIELDS["trade_cal"])
    total = 0
    for missing_start, missing_end in missing:
        for range_start, range_end in _split_range(
            missing_start,
            missing_end,
            MAX_DAYS_PER_REQUEST["trade_cal"],
        ):
            frame = pro.trade_cal(
                exchange=exchange,
                start_date=_format_date(range_start),
                end_date=_format_date(range_end),
                fields=fields,
            )
            data = _normalise_trade_cal(frame)
            store.upsert("trade_cal", exchange, data)
            store.mark_synced("trade_cal", exchange, range_start, range_end)
            total += data.num_rows
    return total


def sync_all(
    pro: Any,
    store: TushareDataStore,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> dict[str, int]:
    """依次同步一只股票常用的全部量化分析数据。"""
    functions: tuple[tuple[str, Callable[..., int]], ...] = (
        ("daily", sync_daily),
        ("daily_basic", sync_daily_basic),
        ("stk_limit", sync_stk_limit),
        ("stock_st", sync_stock_st),
        ("adj_factor", sync_adj_factor),
        ("suspend_d", sync_suspend_d),
        ("moneyflow", sync_moneyflow),
        ("dividend", sync_dividend),
        ("forecast", sync_forecast),
        ("express", sync_express),
        ("fina_audit", sync_fina_audit),
        ("income", sync_income),
        ("balancesheet", sync_balancesheet),
        ("cashflow", sync_cashflow),
        ("fina_indicator", sync_fina_indicator),
        ("sw_industry", sync_sw_industry),
    )
    result = {
        dataset: function(pro, store, ts_code, start_date, end_date)
        for dataset, function in functions
    }
    exchange = exchange_for_ts_code(ts_code)
    result["trade_cal"] = sync_trade_cal(pro, store, exchange, start_date, end_date)
    return result


def _sync_ranged_api(
    pro: Any,
    store: TushareDataStore,
    dataset: str,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
) -> int:
    requested = _historical_range(dataset, start_date, end_date)
    if requested is None:
        return 0
    start, end = requested
    missing = store.missing_ranges(dataset, ts_code, start, end)
    total = 0
    api = getattr(pro, dataset)
    fields = ",".join(SOURCE_FIELDS[dataset])
    request_ranges = [
        part
        for missing_start, missing_end in missing
        for part in _split_range(
            missing_start,
            missing_end,
            MAX_DAYS_PER_REQUEST[dataset],
        )
    ]
    for range_start, range_end in request_ranges:
        # fina_indicator 的 start/end 是报告期，不是公告日。向前多取 550 天覆盖
        # 上一年年报和可能延迟披露的报告，再按公告可见时间裁回请求区间。
        api_start = (
            range_start - timedelta(days=550)
            if dataset == "fina_indicator"
            else range_start
        )
        frame = api(
            ts_code=ts_code,
            start_date=_format_date(api_start),
            end_date=_format_date(range_end),
            fields=fields,
        )
        data = _normalise_frame(dataset, frame)
        if dataset == "fina_indicator":
            data = _filter_visible_dates(data, range_start, range_end)
        store.upsert(dataset, ts_code, data)
        store.mark_synced(dataset, ts_code, range_start, range_end)
        total += data.num_rows
    return total


def _sync_unbounded_api(
    store: TushareDataStore,
    dataset: str,
    ts_code: str,
    start_date: str | date,
    end_date: str | date,
    request: Callable[[], pd.DataFrame],
) -> int:
    requested = _historical_range(dataset, start_date, end_date)
    if requested is None:
        return 0
    start, end = requested
    missing = store.missing_ranges(dataset, ts_code, start, end)
    if not missing:
        return 0
    data = _normalise_frame(dataset, request())
    rows = [
        row
        for row in data.to_pylist()
        if _in_ranges(_market_date(row["visible_at"]), missing)
    ]
    filtered = pa.Table.from_pylist(rows, schema=TABLE_SCHEMAS[dataset])
    store.upsert(dataset, ts_code, filtered)
    for range_start, range_end in missing:
        store.mark_synced(dataset, ts_code, range_start, range_end)
    return filtered.num_rows


def _normalise_frame(dataset: str, frame: pd.DataFrame) -> pa.Table:
    source_fields = SOURCE_FIELDS[dataset]
    _validate_columns(frame, source_fields, dataset)
    visible_fields, visible_time = VISIBILITY_RULES[dataset]
    schema = TABLE_SCHEMAS[dataset]
    rows: list[dict[str, object]] = []
    for raw_row in frame.to_dict("records"):
        cleaned = {name: _clean_scalar(raw_row.get(name)) for name in source_fields}
        visible_date = _first_date(cleaned, visible_fields)
        if visible_date is None:
            raise ValueError(f"{dataset} 返回记录缺少可用的可见日期: {cleaned}")
        row: dict[str, object] = {
            "ts_code": str(cleaned["ts_code"]),
            "visible_at": _at_time(visible_date, visible_time),
        }
        for field in schema:
            name = field.name
            if name in row:
                continue
            value = cleaned.get(name)
            if name in DATE_FIELDS and value is not None:
                value = _parse_date(value)
            elif pa.types.is_floating(field.type) and value is not None:
                value = float(str(value))
            elif pa.types.is_string(field.type) and value is not None:
                value = str(value)
            row[name] = value
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=schema)


def _normalise_trade_cal(frame: pd.DataFrame) -> pa.Table:
    fields = SOURCE_FIELDS["trade_cal"]
    _validate_columns(frame, fields, "trade_cal")
    rows: list[dict[str, object]] = []
    for raw_row in frame.to_dict("records"):
        cleaned = {name: _clean_scalar(raw_row.get(name)) for name in fields}
        cal_date = _parse_date(cleaned["cal_date"])
        is_open = cleaned["is_open"]
        if is_open is None:
            raise ValueError("trade_cal 返回记录缺少 is_open")
        pretrade_date = cleaned["pretrade_date"]
        rows.append(
            {
                "exchange": str(cleaned["exchange"]),
                "visible_at": _at_time(cal_date, time(0, 0)),
                "cal_date": cal_date,
                "is_open": int(str(is_open)),
                "pretrade_date": (
                    _parse_date(pretrade_date)
                    if pretrade_date is not None
                    else None
                ),
            }
        )
    return pa.Table.from_pylist(rows, schema=TABLE_SCHEMAS["trade_cal"])


def _validate_columns(
    frame: pd.DataFrame,
    expected: Iterable[str],
    api_name: str,
) -> None:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"{api_name} 必须返回 pandas.DataFrame")
    missing = [name for name in expected if name not in frame.columns]
    if missing:
        raise ValueError(f"{api_name} 返回缺少已定义字段: {missing}")


def _first_date(row: dict[str, object], names: tuple[str, ...]) -> date | None:
    for name in names:
        value = row.get(name)
        if value is not None:
            return _parse_date(value)
    return None


def _clean_scalar(value: object) -> object | None:
    if value is None:
        return None
    if bool(pd.isna(value)):
        return None
    return value


def _parse_date(value: str | date | object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip().replace("-", "")
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"日期必须是 YYYYMMDD、YYYY-MM-DD 或 date，实际为 {value!r}") from exc


def _format_date(value: date) -> str:
    return value.strftime("%Y%m%d")


def _historical_range(
    dataset: str,
    start_date: str | date,
    end_date: str | date,
) -> tuple[date, date] | None:
    start = _parse_date(start_date)
    requested_end = _parse_date(end_date)
    if start > requested_end:
        raise ValueError("start_date 不能晚于 end_date")
    now = datetime.now(UTC).astimezone(SHANGHAI)
    complete_time = (
        time(9, 0)
        if dataset == "sw_industry"
        else VISIBILITY_RULES[dataset][1]
    )
    last_complete_date = now.date()
    if now.timetz().replace(tzinfo=None) < complete_time:
        last_complete_date -= timedelta(days=1)
    end = min(requested_end, last_complete_date)
    return None if start > end else (start, end)


def _split_range(
    start_date: date,
    end_date: date,
    max_days: int,
) -> list[tuple[date, date]]:
    result: list[tuple[date, date]] = []
    cursor = start_date
    while cursor <= end_date:
        part_end = min(end_date, cursor + timedelta(days=max_days - 1))
        result.append((cursor, part_end))
        cursor = part_end + timedelta(days=1)
    return result


def _filter_visible_dates(data: pa.Table, start_date: date, end_date: date) -> pa.Table:
    rows = [
        row
        for row in data.to_pylist()
        if start_date <= _market_date(row["visible_at"]) <= end_date
    ]
    return pa.Table.from_pylist(rows, schema=data.schema)


def _at_time(value: date, value_time: time) -> int:
    local_time = datetime.combine(value, value_time, tzinfo=SHANGHAI)
    return datetime_to_utc_us(local_time)


def _market_date(value: int) -> date:
    """把微秒时间戳转换成 Tushare 接口使用的中国市场日期。"""
    return utc_us_to_datetime(value).astimezone(SHANGHAI).date()


def _in_ranges(value: date, ranges: list[tuple[date, date]]) -> bool:
    return any(start_date <= value <= end_date for start_date, end_date in ranges)


def exchange_for_ts_code(ts_code: str) -> str:
    """把股票代码后缀映射为 Tushare 交易日历的交易所代码。"""
    suffix = ts_code.rsplit(".", maxsplit=1)[-1].upper()
    try:
        return {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}[suffix]
    except KeyError:
        raise ValueError(f"无法从股票代码识别交易所: {ts_code!r}") from None
