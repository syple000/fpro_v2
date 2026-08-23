"""实际调用 quicksync/Tushare，验证完整区间或自动增量刷新。"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from fpro_common import configure_beijing_logging
from tushare_data import (
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_REQUESTS_PER_MINUTE,
    TABLE_SCHEMAS,
    TushareDataStore,
    create_pro_client,
    sync_all,
    sync_inc,
)

logger = logging.getLogger("tushare_data.test_main")


def run(
    token: str,
    data_dir: Path,
    mode: str,
    start_date: str,
    end_date: str,
    current_date: str,
    requests_per_minute: int,
    max_concurrency: int,
) -> None:
    """按命令行选择完整区间同步或自动滚动增量同步。"""
    pro = create_pro_client(
        token,
        requests_per_minute=requests_per_minute,
        max_concurrency=max_concurrency,
    )
    with TushareDataStore(data_dir) as store:
        result = (
            sync_all(pro, store, start_date, end_date)
            if mode == "sync_all"
            else sync_inc(pro, store, current_date)
        )
    for dataset, fetched in result.items():
        logger.info(
            "%s 完成：本次返回并写入=%d，字段数=%d",
            dataset,
            fetched,
            len(TABLE_SCHEMAS[dataset]),
        )


def main() -> None:
    today = datetime.now(UTC).astimezone(ZoneInfo("Asia/Shanghai")).date()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token", default=os.environ.get("TUSHARE_TOKEN"))
    parser.add_argument("--data-dir", type=Path, default=Path("data/tushare"))
    parser.add_argument(
        "--mode",
        choices=("sync_all", "sync_inc"),
        default="sync_all",
        help="sync_all 使用起止区间；sync_inc 根据当前日期自动选择回看区间",
    )
    default_start = (today - timedelta(days=365 * 3)).strftime("%Y%m%d")
    parser.add_argument("--start-date", default=default_start)
    parser.add_argument("--end-date", default=today.strftime("%Y%m%d"))
    parser.add_argument("--current-date", default=today.strftime("%Y%m%d"))
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
    args = parser.parse_args()
    if not args.token:
        parser.error("请通过环境变量 TUSHARE_TOKEN 或 --token 提供 Token")

    configure_beijing_logging(logging.INFO)
    run(
        args.token,
        args.data_dir,
        args.mode,
        args.start_date,
        args.end_date,
        args.current_date,
        args.requests_per_minute,
        args.max_concurrency,
    )


if __name__ == "__main__":
    main()
