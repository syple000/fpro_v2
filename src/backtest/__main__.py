"""月度中期动量回测命令行入口。"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from backtest.config import BacktestConfig, RunOptions
from backtest.runner import run_monthly_momentum
from strategies import MomentumConfig


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("日期必须为 YYYY-MM-DD") from exc


def main() -> None:
    parser = argparse.ArgumentParser(
        description="运行 A 股月度中期动量回测",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--start", type=_date, default=date(2017, 1, 1), help="回测开始日期")
    parser.add_argument("--end", type=_date, default=date(2026, 8, 22), help="回测结束日期")
    parser.add_argument(
        "--initial-cash",
        type=float,
        default=10_000_000.0,
        help="初始现金，单位为人民币元",
    )
    parser.add_argument(
        "--tushare-dir",
        type=Path,
        default=Path("dataset/tushare"),
        help="Tushare 数据根目录",
    )
    parser.add_argument(
        "--qmt-dir",
        type=Path,
        default=Path("dataset/qmt"),
        help="QMT 数据根目录",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="结果输出目录；不指定时不写文件",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=120,
        help="动量区间较早端点距当前的交易日数",
    )
    parser.add_argument(
        "--skip",
        type=int,
        default=20,
        help="动量计算跳过最近多少个交易日",
    )
    parser.add_argument(
        "--top-fraction",
        type=float,
        default=0.10,
        help="选择动量排名靠前的比例，0.10 表示前 10%%",
    )
    parser.add_argument(
        "--max-positions",
        type=int,
        default=30,
        help="最大持仓股票数量",
    )
    args = parser.parse_args()
    config = BacktestConfig(
        start_date=args.start,
        end_date=args.end,
        initial_cash=args.initial_cash,
    )
    strategy = MomentumConfig(
        lookback_sessions=args.lookback,
        skip_sessions=args.skip,
        top_fraction=args.top_fraction,
        max_positions=args.max_positions,
    )
    completed = run_monthly_momentum(
        config,
        strategy,
        RunOptions(
            tushare_root=args.tushare_dir,
            qmt_root=args.qmt_dir,
            output_dir=args.output_dir,
        ),
    )
    summary = {
        "output_dir": str(completed.output_dir) if completed.output_dir else None,
        "total_return": completed.metrics["total_return"],
        "annualized_return": completed.metrics["annualized_return"],
        "max_drawdown": completed.metrics["max_drawdown"],
        "sharpe": completed.metrics["sharpe"],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
