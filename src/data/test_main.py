"""直接运行统一 Reader，查看 Tushare 财务和 QMT 当前行情。"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from data import DataCatalog, DataReader, QueryResult, SourceConfig

SHANGHAI = ZoneInfo("Asia/Shanghai")


def run(
    tushare_dir: Path,
    qmt_dir: Path,
    as_of: datetime,
    symbol: str,
    periods: int,
) -> tuple[QueryResult, QueryResult]:
    """通过公共 Reader 查询指定股票的现金流和当前行情。"""
    sources = SourceConfig(
        routes={
            "fundamentals.cashflow": "tushare",
            "market.realtime_quotes": "qmt",
        }
    )
    with DataCatalog(tushare_root=tushare_dir, qmt_root=qmt_dir) as catalog:
        reader = DataReader(catalog, sources=sources)
        data = reader.at(as_of)
        cashflow = data.fundamentals.statements(
            kind="cash_flow",
            symbols=(symbol,),
            periods=periods,
            fields=("free_cash_flow",),
        )
        current = data.market.current(
            symbols=(symbol,),
            fields=("last",),
        )
    return cashflow, current


def main() -> None:
    now = datetime.now(SHANGHAI)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tushare-dir", type=Path, default=Path("data/tushare"))
    parser.add_argument("--qmt-dir", type=Path, default=Path("data/qmt"))
    parser.add_argument(
        "--as-of",
        type=datetime.fromisoformat,
        default=now,
        help="带时区的 PIT 时间，例如 2024-04-30T16:05:00+08:00",
    )
    parser.add_argument("--symbol", default="000001.SZ")
    parser.add_argument("--periods", type=int, default=10)
    arguments = parser.parse_args()
    if arguments.periods < 1:
        parser.error("--periods 必须大于等于 1")

    cashflow, current = run(
        arguments.tushare_dir,
        arguments.qmt_dir,
        arguments.as_of,
        arguments.symbol,
        arguments.periods,
    )
    print("Cashflow:")
    print(cashflow.to_pandas().to_string(index=False))
    print("\nCurrent quote:")
    print(current.to_pandas().to_string(index=False))


if __name__ == "__main__":
    main()
