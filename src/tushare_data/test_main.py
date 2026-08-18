"""实际调用 quicksync/Tushare 并验证全部历史数据接口。"""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fpro_common import configure_beijing_logging
from tushare_data import (
    DEFAULT_API_URL,
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_REQUESTS_PER_MINUTE,
    TABLE_SCHEMAS,
    TushareDataStore,
    create_pro_client,
    exchange_for_ts_code,
    sync_adj_factor,
    sync_balancesheet,
    sync_cashflow,
    sync_daily,
    sync_daily_basic,
    sync_dividend,
    sync_express,
    sync_fina_audit,
    sync_fina_indicator,
    sync_forecast,
    sync_income,
    sync_moneyflow,
    sync_stk_limit,
    sync_stock_st,
    sync_suspend_d,
    sync_sw_industry,
    sync_trade_cal,
)

logger = logging.getLogger("tushare_data.test_main")

SYNC_FUNCTIONS: dict[str, Callable[..., int]] = {
    "daily": sync_daily,
    "daily_basic": sync_daily_basic,
    "stk_limit": sync_stk_limit,
    "stock_st": sync_stock_st,
    "adj_factor": sync_adj_factor,
    "suspend_d": sync_suspend_d,
    "moneyflow": sync_moneyflow,
    "dividend": sync_dividend,
    "forecast": sync_forecast,
    "express": sync_express,
    "fina_audit": sync_fina_audit,
    "income": sync_income,
    "balancesheet": sync_balancesheet,
    "cashflow": sync_cashflow,
    "fina_indicator": sync_fina_indicator,
    "sw_industry": sync_sw_industry,
    "trade_cal": lambda pro, store, ts_code, start_date, end_date: sync_trade_cal(
        pro,
        store,
        exchange_for_ts_code(ts_code),
        start_date,
        end_date,
    ),
}


def run(
    token: str,
    api_url: str,
    data_dir: Path,
    ts_code: str,
    start_date: str,
    end_date: str,
    datasets: list[str],
    requests_per_minute: int,
    max_concurrency: int,
) -> None:
    """逐个调用接口；单个接口失败时继续验证其余接口，最后统一报错。"""
    pro: Any = create_pro_client(
        token,
        api_url,
        requests_per_minute=requests_per_minute,
        max_concurrency=max_concurrency,
    )
    failures: list[str] = []
    with TushareDataStore(data_dir) as store:
        for dataset in datasets:
            try:
                fetched = SYNC_FUNCTIONS[dataset](
                    pro,
                    store,
                    ts_code,
                    start_date,
                    end_date,
                )
                partition = exchange_for_ts_code(ts_code) if dataset == "trade_cal" else ts_code
                saved = store.read(dataset, partition)
                ranges = store.synced_ranges(dataset, partition)
                visible = [
                    value
                    for value in saved.column("visible_at").to_pylist()
                    if isinstance(value, int) and not isinstance(value, bool)
                ]
                logger.info(
                    "%s 完成：本次拉取=%d，总行数=%d，覆盖区间=%s，可见时间=%s ~ %s，字段数=%d",
                    dataset,
                    fetched,
                    saved.num_rows,
                    ranges,
                    min(visible) if visible else None,
                    max(visible) if visible else None,
                    len(TABLE_SCHEMAS[dataset]),
                )
            except Exception:
                failures.append(dataset)
                logger.exception("%s 验证失败", dataset)
    if failures:
        raise RuntimeError(f"接口验证失败：{', '.join(failures)}")


def main() -> None:
    today = datetime.now(UTC).astimezone(ZoneInfo("Asia/Shanghai")).date()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token", default=os.environ.get("TUSHARE_TOKEN"))
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    parser.add_argument("--data-dir", type=Path, default=Path("data/tushare"))
    parser.add_argument("--ts-code", default="000001.SZ")
    default_start = (today - timedelta(days=365 * 3)).strftime("%Y%m%d")
    parser.add_argument("--start-date", default=default_start)
    parser.add_argument("--end-date", default=today.strftime("%Y%m%d"))
    parser.add_argument(
        "--requests-per-minute",
        type=int,
        default=DEFAULT_REQUESTS_PER_MINUTE,
        help="quicksync 限速：基础版120、标准版600、极速版1200；默认按基础版",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=DEFAULT_MAX_CONCURRENCY,
        help="同时在途请求数，默认1",
    )
    parser.add_argument(
        "--datasets",
        default="all",
        help=f"all 或逗号分隔的表名：{','.join(SYNC_FUNCTIONS)}",
    )
    args = parser.parse_args()
    if not args.token:
        parser.error("请通过环境变量 TUSHARE_TOKEN 或 --token 提供 Token")
    datasets = (
        list(SYNC_FUNCTIONS)
        if args.datasets == "all"
        else [item.strip() for item in args.datasets.split(",") if item.strip()]
    )
    unknown = sorted(set(datasets) - SYNC_FUNCTIONS.keys())
    if unknown:
        parser.error(f"未知数据表: {', '.join(unknown)}")

    configure_beijing_logging(logging.INFO)
    run(
        args.token,
        args.api_url,
        args.data_dir,
        args.ts_code,
        args.start_date,
        args.end_date,
        datasets,
        args.requests_per_minute,
        args.max_concurrency,
    )


if __name__ == "__main__":
    main()
