"""按全市场截面拉取 Tushare 历史数据，再由存储层按业务日期分区。"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from math import isnan
from numbers import Real
from threading import local
from typing import Any

import pandas as pd
import pyarrow as pa
import requests

from tushare_data.client import (
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_MAX_RETRIES,
    DEFAULT_REQUESTS_PER_MINUTE,
    DEFAULT_RETRY_BACKOFF_SECONDS,
    RequestLimiter,
    TushareProClient,
    require_tushare_data_api,
)
from tushare_data.schemas import DATE_FIELDS, SOURCE_FIELDS, TABLE_SCHEMAS
from tushare_data.storage import TushareDataStore

logger = logging.getLogger(__name__)

DEFAULT_API_URL = "http://api.quicksync.cn"
_REQUEST_TIMEOUT_SECONDS = 120
MARKET_EXCHANGES = ("SSE", "SZSE", "BSE")
PAGE_SIZE = 5_000
INDEX_MEMBER_PAGE_SIZE = 2_000
CALENDAR_REQUEST_DAYS = 3_650
MARKET_WRITE_CHUNK_DAYS = 31
INC_STABLE_TRADING_DAYS = 5
INC_FACTOR_TRADING_DAYS = 10
INC_FINANCIAL_DAILY_DAYS = 10
INC_FINANCIAL_YEARS = 3
INC_EVENT_DAYS = 180
INC_DIVIDEND_DAILY_DAYS = 30
INC_DIVIDEND_YEARS = 2
INC_CALENDAR_PAST_DAYS = 60
INC_CALENDAR_FUTURE_DAYS = 366
INC_WEEKLY_REFRESH_WEEKDAY = 0
INC_MONTHLY_REFRESH_DAY = 1

PagedRequest = Callable[[int, int], pd.DataFrame]
TradeDateRequest = Callable[[str, int, int], pd.DataFrame]
DateRangeRequest = Callable[[str, str, int, int], pd.DataFrame]
MarketSyncFunction = Callable[
    [TushareProClient, TushareDataStore, str | date, str | date],
    int,
]


class _DirectRequests:
    """为每个工作线程复用一个不读取系统和环境代理的 Session。"""

    def __init__(self) -> None:
        self._local = local()

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.trust_env = False
            self._local.session = session
        return session

    def post(self, url: str, **kwargs: Any) -> requests.Response:
        return self._session().post(url, **kwargs)


def create_pro_client(
    token: str,
    api_url: str = DEFAULT_API_URL,
    *,
    requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
) -> TushareProClient:
    """按 quicksync 要求初始化客户端，并为所有分页请求应用同一个限制器。"""
    if not token.strip():
        raise ValueError("Tushare token 不能为空")
    import tushare as ts
    import tushare.pro.client as client

    client.DataApi._DataApi__http_url = api_url.rstrip("/")  # pyright: ignore[reportAttributeAccessIssue]
    # DataApi.query 固定调用其模块级 requests.post；替换成相同窄接口，避免
    # HTTP_PROXY/HTTPS_PROXY/ALL_PROXY 以及 Windows 系统代理影响 Tushare。
    client.requests = _DirectRequests()  # pyright: ignore[reportAttributeAccessIssue]
    raw_pro: object = ts.pro_api(token, timeout=_REQUEST_TIMEOUT_SECONDS)
    limiter = RequestLimiter(requests_per_minute, max_concurrency)
    return TushareProClient(
        require_tushare_data_api(raw_pro),
        limiter,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )


def sync_daily(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按交易日刷新全市场未复权日 K 线。"""
    fields = ",".join(SOURCE_FIELDS["daily"])
    return _sync_trade_date_dataset(
        pro,
        store,
        "daily",
        start_date,
        end_date,
        lambda day, limit, offset: pro.daily(
            trade_date=day, fields=fields, limit=limit, offset=offset
        ),
    )


def sync_daily_basic(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按交易日刷新全市场估值、换手率、股本和市值。"""
    fields = ",".join(SOURCE_FIELDS["daily_basic"])
    return _sync_trade_date_dataset(
        pro,
        store,
        "daily_basic",
        start_date,
        end_date,
        lambda day, limit, offset: pro.daily_basic(
            trade_date=day, fields=fields, limit=limit, offset=offset
        ),
    )


def sync_adj_factor(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按交易日刷新全市场复权因子。"""
    fields = ",".join(SOURCE_FIELDS["adj_factor"])
    return _sync_trade_date_dataset(
        pro,
        store,
        "adj_factor",
        start_date,
        end_date,
        lambda day, limit, offset: pro.adj_factor(
            trade_date=day, fields=fields, limit=limit, offset=offset
        ),
    )


def sync_suspend_d(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按交易日刷新全市场停牌和复牌事件。"""
    fields = ",".join(SOURCE_FIELDS["suspend_d"])
    return _sync_trade_date_dataset(
        pro,
        store,
        "suspend_d",
        start_date,
        end_date,
        lambda day, limit, offset: pro.suspend_d(
            trade_date=day, fields=fields, limit=limit, offset=offset
        ),
    )


def sync_stk_limit(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按交易日刷新全市场涨跌停价格。"""
    fields = ",".join(SOURCE_FIELDS["stk_limit"])
    return _sync_trade_date_dataset(
        pro,
        store,
        "stk_limit",
        start_date,
        end_date,
        lambda day, limit, offset: pro.stk_limit(
            trade_date=day, fields=fields, limit=limit, offset=offset
        ),
    )


def sync_stock_st(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按交易日刷新全市场 ST/风险警示状态。"""
    fields = ",".join(SOURCE_FIELDS["stock_st"])
    return _sync_trade_date_dataset(
        pro,
        store,
        "stock_st",
        start_date,
        end_date,
        lambda day, limit, offset: pro.stock_st(
            trade_date=day, fields=fields, limit=limit, offset=offset
        ),
    )


def sync_moneyflow(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按交易日刷新全市场个股资金流向。"""
    fields = ",".join(SOURCE_FIELDS["moneyflow"])
    return _sync_trade_date_dataset(
        pro,
        store,
        "moneyflow",
        start_date,
        end_date,
        lambda day, limit, offset: pro.moneyflow(
            trade_date=day, fields=fields, limit=limit, offset=offset
        ),
    )


def sync_forecast(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """通过 forecast_vip 按公告区间拉取全市场业绩预告。"""
    fields = ",".join(SOURCE_FIELDS["forecast"])
    return _sync_announcement_range_dataset(
        store,
        "forecast",
        start_date,
        end_date,
        lambda start, end, limit, offset: pro.forecast_vip(
            start_date=start,
            end_date=end,
            fields=fields,
            limit=limit,
            offset=offset,
        ),
    )


def sync_express(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """通过 express_vip 按公告区间拉取全市场业绩快报。"""
    fields = ",".join(SOURCE_FIELDS["express"])
    return _sync_announcement_range_dataset(
        store,
        "express",
        start_date,
        end_date,
        lambda start, end, limit, offset: pro.express_vip(
            start_date=start,
            end_date=end,
            fields=fields,
            limit=limit,
            offset=offset,
        ),
    )


def sync_fina_indicator(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """通过 fina_indicator_vip 按公告区间拉取全市场财务指标。"""
    fields = ",".join(SOURCE_FIELDS["fina_indicator"])
    return _sync_announcement_range_dataset(
        store,
        "fina_indicator",
        start_date,
        end_date,
        lambda start, end, limit, offset: pro.fina_indicator_vip(
            start_date=start,
            end_date=end,
            fields=fields,
            limit=limit,
            offset=offset,
        ),
    )


def sync_income(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """通过 income_vip 按公告区间拉取全市场利润表。"""
    fields = ",".join(SOURCE_FIELDS["income"])
    return _sync_announcement_range_dataset(
        store,
        "income",
        start_date,
        end_date,
        lambda start, end, limit, offset: pro.income_vip(
            start_date=start,
            end_date=end,
            fields=fields,
            limit=limit,
            offset=offset,
        ),
    )


def sync_balancesheet(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """通过 balancesheet_vip 按公告区间拉取全市场资产负债表。"""
    fields = ",".join(SOURCE_FIELDS["balancesheet"])
    return _sync_announcement_range_dataset(
        store,
        "balancesheet",
        start_date,
        end_date,
        lambda start, end, limit, offset: pro.balancesheet_vip(
            start_date=start,
            end_date=end,
            fields=fields,
            limit=limit,
            offset=offset,
        ),
    )


def sync_cashflow(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """通过 cashflow_vip 按公告区间拉取全市场现金流量表。"""
    fields = ",".join(SOURCE_FIELDS["cashflow"])
    return _sync_announcement_range_dataset(
        store,
        "cashflow",
        start_date,
        end_date,
        lambda start, end, limit, offset: pro.cashflow_vip(
            start_date=start,
            end_date=end,
            fields=fields,
            limit=limit,
            offset=offset,
        ),
    )


def sync_dividend(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """按预案公告日和实施公告日拉取全市场分红数据，并以实施公告日控制可见性。"""
    requested = (_parse_date(start_date), _parse_date(end_date))
    if requested[0] > requested[1]:
        raise ValueError("start_date 不能晚于 end_date")
    fields = ",".join(SOURCE_FIELDS["dividend"])
    total = 0
    for chunk_start, chunk_end in _split_range(*requested, MARKET_WRITE_CHUNK_DAYS):
        tables: list[pa.Table] = []
        for day in _dates(chunk_start, chunk_end):
            day_text = _format_date(day)
            for by_implementation in (False, True):

                def request_dividend(
                    limit: int,
                    offset: int,
                    *,
                    day_text: str = day_text,
                    by_implementation: bool = by_implementation,
                ) -> pd.DataFrame:
                    return pro.dividend(
                        ann_date=None if by_implementation else day_text,
                        imp_ann_date=day_text if by_implementation else None,
                        fields=fields,
                        limit=limit,
                        offset=offset,
                    )

                frame = _fetch_pages(request_dividend)
                tables.append(_normalise_frame("dividend", frame))
        data = _deduplicate_table(_concat_tables(tables, TABLE_SCHEMAS["dividend"]))
        total += store.write("dividend", data)
    return total


def sync_fina_audit(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """全市场同步审计意见；Tushare 无全量接口，只在这里按股票清单回退。"""
    requested = (_parse_date(start_date), _parse_date(end_date))
    if requested[0] > requested[1]:
        raise ValueError("start_date 不能晚于 end_date")
    fields = ",".join(SOURCE_FIELDS["fina_audit"])
    total = 0
    stock_codes = _stock_codes(pro)
    for range_start, range_end in _split_range(*requested, 366):
        tables: list[pa.Table] = []
        for ts_code in stock_codes:

            def request_audit(
                limit: int,
                offset: int,
                *,
                ts_code: str = ts_code,
                range_start: date = range_start,
                range_end: date = range_end,
            ) -> pd.DataFrame:
                return pro.fina_audit(
                    ts_code=ts_code,
                    start_date=_format_date(range_start),
                    end_date=_format_date(range_end),
                    fields=fields,
                    limit=limit,
                    offset=offset,
                )

            frame = _fetch_pages(request_audit)
            data = _normalise_frame("fina_audit", frame)
            tables.append(data)
        combined = _concat_tables(tables, TABLE_SCHEMAS["fina_audit"])
        total += store.write("fina_audit", combined)
    return total


def sync_sw_industry(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """分页获取全部申万三级行业成员原始区间。"""
    requested_start = _parse_date(start_date)
    requested_end = _parse_date(end_date)
    if requested_start > requested_end:
        raise ValueError("start_date 不能晚于 end_date")
    fields = ",".join(SOURCE_FIELDS["sw_industry"])
    frames = [
        _fetch_pages(
            lambda limit, offset, is_new=is_new: pro.index_member_all(
                is_new=is_new,
                fields=fields,
                limit=limit,
                offset=offset,
            ),
            page_size=INDEX_MEMBER_PAGE_SIZE,
        )
        for is_new in ("Y", "N")
    ]
    source = pd.concat(frames, ignore_index=True)
    _validate_columns(source, SOURCE_FIELDS["sw_industry"], "index_member_all")
    data = _deduplicate_table(_normalise_frame("sw_industry", source))
    return store.write("sw_industry", data)


def sync_trade_cal(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> int:
    """合并三家交易所后，按日期刷新完整交易日历。"""
    requested = (_parse_date(start_date), _parse_date(end_date))
    if requested[0] > requested[1]:
        raise ValueError("start_date 不能晚于 end_date")
    fields = ",".join(SOURCE_FIELDS["trade_cal"])
    total = 0
    for range_start, range_end in _split_range(*requested, CALENDAR_REQUEST_DAYS):
        tables: list[pa.Table] = []
        for exchange in MARKET_EXCHANGES:

            def request_calendar(
                limit: int,
                offset: int,
                *,
                exchange: str = exchange,
                range_start: date = range_start,
                range_end: date = range_end,
            ) -> pd.DataFrame:
                return pro.trade_cal(
                    exchange=exchange,
                    start_date=_format_date(range_start),
                    end_date=_format_date(range_end),
                    fields=fields,
                    limit=limit,
                    offset=offset,
                )

            tables.append(_normalise_trade_cal(_fetch_pages(request_calendar)))
        data = _concat_tables(tables, TABLE_SCHEMAS["trade_cal"])
        total += store.write("trade_cal", data)
    return total


def sync_all(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: str | date,
    end_date: str | date,
) -> dict[str, int]:
    """按日期升序完成一次性历史回填，并跳过已经成功提交的区间。"""
    requested_start = _parse_date(start_date)
    requested_end = _parse_date(end_date)
    if requested_start > requested_end:
        raise ValueError("start_date 不能晚于 end_date")

    functions: tuple[tuple[str, MarketSyncFunction, int], ...] = (
        ("trade_cal", sync_trade_cal, CALENDAR_REQUEST_DAYS),
        ("daily", sync_daily, MARKET_WRITE_CHUNK_DAYS),
        ("daily_basic", sync_daily_basic, MARKET_WRITE_CHUNK_DAYS),
        ("stk_limit", sync_stk_limit, MARKET_WRITE_CHUNK_DAYS),
        ("stock_st", sync_stock_st, MARKET_WRITE_CHUNK_DAYS),
        ("adj_factor", sync_adj_factor, MARKET_WRITE_CHUNK_DAYS),
        ("suspend_d", sync_suspend_d, MARKET_WRITE_CHUNK_DAYS),
        ("moneyflow", sync_moneyflow, MARKET_WRITE_CHUNK_DAYS),
        ("dividend", sync_dividend, MARKET_WRITE_CHUNK_DAYS),
        ("forecast", sync_forecast, 366),
        ("express", sync_express, 366),
        ("income", sync_income, 366),
        ("balancesheet", sync_balancesheet, 366),
        ("cashflow", sync_cashflow, 366),
        ("fina_indicator", sync_fina_indicator, 366),
        ("sw_industry", sync_sw_industry, CALENDAR_REQUEST_DAYS),
        ("fina_audit", sync_fina_audit, 366),
    )

    # 交易日历是日频接口的共同依赖，先完整提交；其余数据集再并行回填。
    result = {
        "trade_cal": _sync_all_dataset(
            pro,
            store,
            requested_start,
            requested_end,
            functions[0],
        )
    }
    dataset_specs = functions[1:]
    with ThreadPoolExecutor(
        max_workers=len(dataset_specs),
        thread_name_prefix="tushare-dataset",
    ) as executor:
        futures = {
            spec[0]: executor.submit(
                _sync_all_dataset,
                pro,
                store,
                requested_start,
                requested_end,
                spec,
            )
            for spec in dataset_specs
        }
        for dataset, future in futures.items():
            result[dataset] = future.result()
    return result


def _sync_all_dataset(
    pro: TushareProClient,
    store: TushareDataStore,
    requested_start: date,
    requested_end: date,
    spec: tuple[str, MarketSyncFunction, int],
) -> int:
    dataset, function, checkpoint_days = spec
    completed = store._sync_all_completed_ranges(dataset)
    missing = _missing_date_ranges(requested_start, requested_end, completed)
    total = 0

    # 申万接口始终返回完整成员快照，一次调用即可覆盖全部缺失历史区间。
    if dataset == "sw_industry" and missing:
        total = function(pro, store, requested_start, requested_end)
        for range_start, range_end in missing:
            store._mark_sync_all_completed(dataset, range_start, range_end)
        logger.info(
            "sync_all %s 已完成 %s 至 %s，写入 %d 行",
            dataset,
            requested_start,
            requested_end,
            total,
        )
        return total

    for range_start, range_end in missing:
        for chunk_start, chunk_end in _split_range(
            range_start,
            range_end,
            checkpoint_days,
        ):
            written = function(pro, store, chunk_start, chunk_end)
            total += written
            store._mark_sync_all_completed(dataset, chunk_start, chunk_end)
            logger.info(
                "sync_all %s 已完成 %s 至 %s，写入 %d 行",
                dataset,
                chunk_start,
                chunk_end,
                written,
            )
    return total


def sync_inc(
    pro: TushareProClient,
    store: TushareDataStore,
    current_date: str | date,
) -> dict[str, int]:
    """以给定日期为基准，按各类数据的稳定性自动滚动刷新。"""
    current = _parse_date(current_date)
    calendar_start = current - timedelta(days=INC_CALENDAR_PAST_DAYS)
    calendar_end = current + timedelta(days=INC_CALENDAR_FUTURE_DAYS)

    result: dict[str, int] = {"trade_cal": sync_trade_cal(pro, store, calendar_start, calendar_end)}
    jobs: list[tuple[str, MarketSyncFunction, date, date]] = []
    open_dates = _market_open_dates(store, calendar_start, current)
    if open_dates:
        stable_start = open_dates[-min(len(open_dates), INC_STABLE_TRADING_DAYS)]
        factor_start = open_dates[-min(len(open_dates), INC_FACTOR_TRADING_DAYS)]
        for dataset, function in (
            ("daily", sync_daily),
            ("stk_limit", sync_stk_limit),
            ("suspend_d", sync_suspend_d),
            ("stock_st", sync_stock_st),
        ):
            jobs.append((dataset, function, stable_start, current))
        for dataset, function in (
            ("daily_basic", sync_daily_basic),
            ("moneyflow", sync_moneyflow),
            ("adj_factor", sync_adj_factor),
        ):
            jobs.append((dataset, function, factor_start, current))
    else:
        for dataset in (
            "daily",
            "stk_limit",
            "suspend_d",
            "stock_st",
            "daily_basic",
            "moneyflow",
            "adj_factor",
        ):
            result[dataset] = 0

    financial_start = (
        _years_before(current, INC_FINANCIAL_YEARS)
        if current.day == INC_MONTHLY_REFRESH_DAY
        else current - timedelta(days=INC_FINANCIAL_DAILY_DAYS - 1)
    )
    for dataset, function in (
        ("income", sync_income),
        ("balancesheet", sync_balancesheet),
        ("cashflow", sync_cashflow),
        ("fina_indicator", sync_fina_indicator),
    ):
        jobs.append((dataset, function, financial_start, current))

    event_start = current - timedelta(days=INC_EVENT_DAYS - 1)
    for dataset, function in (
        ("forecast", sync_forecast),
        ("express", sync_express),
    ):
        jobs.append((dataset, function, event_start, current))

    if current.weekday() == INC_WEEKLY_REFRESH_WEEKDAY:
        jobs.extend(
            (
                ("fina_audit", sync_fina_audit, event_start, current),
                ("sw_industry", sync_sw_industry, current, current),
            )
        )
    else:
        result["fina_audit"] = 0
        result["sw_industry"] = 0

    dividend_start = (
        _years_before(current, INC_DIVIDEND_YEARS)
        if current.day == INC_MONTHLY_REFRESH_DAY
        else current - timedelta(days=INC_DIVIDEND_DAILY_DAYS - 1)
    )
    jobs.append(("dividend", sync_dividend, dividend_start, current))
    with ThreadPoolExecutor(
        max_workers=len(jobs),
        thread_name_prefix="tushare-dataset",
    ) as executor:
        futures = {
            dataset: executor.submit(function, pro, store, start_date, end_date)
            for dataset, function, start_date, end_date in jobs
        }
        for dataset, future in futures.items():
            result[dataset] = future.result()
    return result


def _sync_trade_date_dataset(
    pro: TushareProClient,
    store: TushareDataStore,
    dataset: str,
    start_date: str | date,
    end_date: str | date,
    request: TradeDateRequest,
) -> int:
    requested = (_parse_date(start_date), _parse_date(end_date))
    if requested[0] > requested[1]:
        raise ValueError("start_date 不能晚于 end_date")
    _ensure_trade_calendars(pro, store, *requested)
    total = 0
    for chunk_start, chunk_end in _split_range(*requested, MARKET_WRITE_CHUNK_DAYS):
        tables = [
            _normalise_frame(
                dataset,
                _fetch_pages(
                    lambda limit, offset, day=day: request(_format_date(day), limit, offset)
                ),
            )
            for day in _market_open_dates(store, chunk_start, chunk_end)
        ]
        data = _concat_tables(tables, TABLE_SCHEMAS[dataset])
        total += store.write(dataset, data)
    return total


def _sync_announcement_range_dataset(
    store: TushareDataStore,
    dataset: str,
    start_date: str | date,
    end_date: str | date,
    request: DateRangeRequest,
) -> int:
    requested = (_parse_date(start_date), _parse_date(end_date))
    if requested[0] > requested[1]:
        raise ValueError("start_date 不能晚于 end_date")
    total = 0
    for chunk_start, chunk_end in _split_range(*requested, 366):
        frame = _fetch_pages(
            lambda limit, offset, chunk_start=chunk_start, chunk_end=chunk_end: request(
                _format_date(chunk_start),
                _format_date(chunk_end),
                limit,
                offset,
            )
        )
        data = _normalise_frame(dataset, frame)
        total += store.write(dataset, data)
    return total


def _fetch_pages(request: PagedRequest, page_size: int = PAGE_SIZE) -> pd.DataFrame:
    """持续请求 offset，直到最后一页；整页返回时绝不能假定数据已经完整。"""
    frames: list[pd.DataFrame] = []
    empty_frame: pd.DataFrame | None = None
    offset = 0
    previous: pd.DataFrame | None = None
    while True:
        frame = request(page_size, offset)
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("Tushare 接口必须返回 pandas.DataFrame")
        if frame.empty:
            empty_frame = frame
            break
        if previous is not None and frame.equals(previous):
            raise RuntimeError("Tushare 分页接口忽略了 offset，已停止以避免无限循环")
        frames.append(frame)
        offset += len(frame)
        if len(frame) < page_size:
            break
        previous = frame
    if not frames:
        return empty_frame if empty_frame is not None else pd.DataFrame()
    if len(frames) == 1:
        return frames[0].reset_index(drop=True)
    records = [row for frame in frames for row in frame.to_dict("records")]
    return pd.DataFrame.from_records(records, columns=frames[0].columns)


def _stock_codes(pro: TushareProClient) -> list[str]:
    codes: set[str] = set()
    for list_status in ("L", "D", "P"):
        frame = _fetch_pages(
            lambda limit, offset, list_status=list_status: pro.stock_basic(
                list_status=list_status,
                fields="ts_code",
                limit=limit,
                offset=offset,
            )
        )
        if "ts_code" not in frame.columns:
            raise ValueError("stock_basic 返回缺少 ts_code")
        codes.update(str(value) for value in frame["ts_code"] if pd.notna(value))
    return sorted(codes)


def _ensure_trade_calendars(
    pro: TushareProClient,
    store: TushareDataStore,
    start_date: date,
    end_date: date,
) -> None:
    dates = _dates(start_date, end_date)
    calendar = store.read("trade_cal", dates)
    existing = {(str(row["exchange"]), row["cal_date"]) for row in calendar.to_pylist()}
    missing = [
        day
        for day in dates
        if any((exchange, day) not in existing for exchange in MARKET_EXCHANGES)
    ]
    for missing_start, missing_end in _contiguous_ranges(missing):
        sync_trade_cal(pro, store, missing_start, missing_end)


def _market_open_dates(
    store: TushareDataStore,
    start_date: date,
    end_date: date,
) -> list[date]:
    calendar = store.read("trade_cal", _dates(start_date, end_date))
    return sorted({row["cal_date"] for row in calendar.to_pylist() if row["is_open"] == 1})


def _normalise_frame(dataset: str, frame: pd.DataFrame) -> pa.Table:
    source_fields = SOURCE_FIELDS[dataset]
    if frame.empty and not len(frame.columns):
        frame = pd.DataFrame(columns=pd.Index(source_fields))
    _validate_columns(frame, source_fields, dataset)
    schema = TABLE_SCHEMAS[dataset]
    rows: list[dict[str, object]] = []
    for raw_row in frame.to_dict("records"):
        cleaned = {name: _clean_scalar(raw_row.get(name)) for name in source_fields}
        row: dict[str, object] = {}
        for field in schema:
            name = field.name
            value = cleaned.get(name)
            if name in DATE_FIELDS and value is not None:
                value = _parse_date(value)
            elif pa.types.is_floating(field.type) and value is not None:
                value = float(str(value))
            elif pa.types.is_integer(field.type) and value is not None:
                value = int(float(str(value)))
            elif pa.types.is_string(field.type) and value is not None:
                value = str(value)
            if not field.nullable and value is None:
                raise ValueError(f"{dataset} 返回记录缺少必填字段 {name}")
            row[name] = value
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=schema)


def _normalise_trade_cal(frame: pd.DataFrame) -> pa.Table:
    fields = SOURCE_FIELDS["trade_cal"]
    if frame.empty and not len(frame.columns):
        frame = pd.DataFrame(columns=pd.Index(fields))
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
                "cal_date": cal_date,
                "is_open": int(str(is_open)),
                "pretrade_date": (
                    _parse_date(pretrade_date) if pretrade_date is not None else None
                ),
            }
        )
    return pa.Table.from_pylist(rows, schema=TABLE_SCHEMAS["trade_cal"])


def _validate_columns(frame: pd.DataFrame, expected: Iterable[str], api_name: str) -> None:
    missing = [name for name in expected if name not in frame.columns]
    if missing:
        raise ValueError(f"{api_name} 返回缺少已定义字段: {missing}")


def _concat_tables(tables: list[pa.Table], schema: pa.Schema) -> pa.Table:
    populated = [table for table in tables if table.num_rows]
    return pa.concat_tables(populated) if populated else pa.Table.from_pylist([], schema=schema)


def _deduplicate_table(data: pa.Table) -> pa.Table:
    unique: dict[tuple[object, ...], dict[str, object]] = {}
    for row in data.to_pylist():
        unique[tuple(row[name] for name in data.schema.names)] = row
    return pa.Table.from_pylist(list(unique.values()), schema=data.schema)


def _clean_scalar(value: object) -> object | None:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, Real) and isnan(float(value)):
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


def _years_before(value: date, years: int) -> date:
    """按自然年回退；闰日回退到目标年份的 2 月 28 日。"""
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def _split_range(start_date: date, end_date: date, max_days: int) -> list[tuple[date, date]]:
    result: list[tuple[date, date]] = []
    cursor = start_date
    while cursor <= end_date:
        part_end = min(end_date, cursor + timedelta(days=max_days - 1))
        result.append((cursor, part_end))
        cursor = part_end + timedelta(days=1)
    return result


def _missing_date_ranges(
    requested_start: date,
    requested_end: date,
    completed: list[tuple[date, date]],
) -> list[tuple[date, date]]:
    """返回请求区间中尚未完成的日期闭区间，并保持日期升序。"""
    missing: list[tuple[date, date]] = []
    cursor = requested_start
    for completed_start, completed_end in completed:
        if completed_end < cursor:
            continue
        if completed_start > requested_end:
            break
        if completed_start > cursor:
            missing.append((cursor, min(requested_end, completed_start - timedelta(days=1))))
        cursor = max(cursor, completed_end + timedelta(days=1))
        if cursor > requested_end:
            break
    if cursor <= requested_end:
        missing.append((cursor, requested_end))
    return missing


def _contiguous_ranges(values: list[date]) -> list[tuple[date, date]]:
    """把缺失日期合成连续区间，减少交易日历请求次数。"""
    if not values:
        return []
    result: list[tuple[date, date]] = []
    start = previous = values[0]
    for value in values[1:]:
        if value != previous + timedelta(days=1):
            result.append((start, previous))
            start = value
        previous = value
    result.append((start, previous))
    return result


def _dates(start_date: date, end_date: date) -> list[date]:
    return [
        start_date + timedelta(days=offset) for offset in range((end_date - start_date).days + 1)
    ]


def exchange_for_ts_code(ts_code: str) -> str:
    """把股票代码后缀映射为 Tushare 交易日历的交易所代码。"""
    suffix = ts_code.rsplit(".", maxsplit=1)[-1].upper()
    try:
        return {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}[suffix]
    except KeyError:
        raise ValueError(f"无法从股票代码识别交易所: {ts_code!r}") from None
