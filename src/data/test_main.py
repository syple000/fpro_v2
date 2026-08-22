"""直接运行 DuckDB 读取层，查看 Tushare 财务 PIT 和 QMT Tick。"""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from data import DataCatalog


def run(
    tushare_dir: Path,
    qmt_dir: Path,
    as_of: date,
    qmt_as_of_us: int,
    ts_code: str,
    limit: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """查询指定股票的财务 PIT 和实时 Tick 样例。"""
    with DataCatalog(tushare_root=tushare_dir, qmt_root=qmt_dir) as catalog:
        connection = catalog.connection
        cashflow = connection.execute(
            """
            SELECT
                ts_code,
                end_date,
                ann_date,
                f_ann_date,
                report_type,
                comp_type,
                update_flag,
                free_cashflow
            FROM tushare.cashflow_as_of(CAST(? AS DATE))
            WHERE ts_code = ?
            ORDER BY end_date DESC
            LIMIT ?
            """,
            [as_of, ts_code, limit],
        ).fetch_df()
        ticks = connection.execute(
            """
            SELECT
                trading_date,
                seq,
                code,
                received_at,
                quote.lastPrice AS last_price
            FROM qmt.ticks_as_of(?)
            WHERE code = ?
            ORDER BY received_at DESC, seq DESC
            LIMIT ?
            """,
            [qmt_as_of_us, ts_code, limit],
        ).fetch_df()
    return cashflow, ticks


def main() -> None:
    now = datetime.now(UTC)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tushare-dir", type=Path, default=Path("data/tushare"))
    parser.add_argument("--qmt-dir", type=Path, default=Path("data/qmt"))
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=now.date(),
        help="Tushare PIT 日期，格式 YYYY-MM-DD",
    )
    parser.add_argument(
        "--qmt-as-of-us",
        type=int,
        default=int(now.timestamp() * 1_000_000),
        help="QMT received_at 截止时间，UTC Unix Epoch 微秒",
    )
    parser.add_argument("--ts-code", default="000001.SZ")
    parser.add_argument("--limit", type=int, default=10)
    arguments = parser.parse_args()
    if arguments.limit < 1:
        parser.error("--limit 必须大于等于 1")

    cashflow, ticks = run(
        arguments.tushare_dir,
        arguments.qmt_dir,
        arguments.as_of,
        arguments.qmt_as_of_us,
        arguments.ts_code,
        arguments.limit,
    )
    print("Tushare cashflow as_of:")
    print(cashflow.to_string(index=False))
    print("\nQMT ticks as_of:")
    print(ticks.to_string(index=False))


if __name__ == "__main__":
    main()
