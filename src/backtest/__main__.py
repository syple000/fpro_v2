"""月度中期动量回测命令行入口。"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from backtest.config import BacktestConfig, UniverseConfig
from backtest.runner import run_monthly_momentum
from strategies import MomentumConfig


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("日期必须为 YYYY-MM-DD") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="运行 A 股月度中期动量回测")
    parser.add_argument("--start", type=_date, default=date(2017, 1, 1))
    parser.add_argument("--end", type=_date, default=date(2026, 8, 22))
    parser.add_argument("--initial-cash", type=float, default=10_000_000.0)
    parser.add_argument("--tushare-dir", type=Path, default=Path("dataset/tushare"))
    parser.add_argument("--qmt-dir", type=Path, default=Path("dataset/qmt"))
    parser.add_argument("--output-dir", type=Path, default=Path("runs"))
    parser.add_argument("--lookback", type=int, default=120)
    parser.add_argument("--skip", type=int, default=20)
    parser.add_argument("--top-fraction", type=float, default=0.10)
    parser.add_argument("--max-positions", type=int, default=30)
    args = parser.parse_args()
    config = BacktestConfig(
        start_date=args.start,
        end_date=args.end,
        initial_cash=args.initial_cash,
        tushare_root=args.tushare_dir,
        qmt_root=args.qmt_dir,
        output_root=args.output_dir,
        universe=UniverseConfig(minimum_listing_sessions=250, exclude_st=True),
    )
    strategy = MomentumConfig(
        lookback_sessions=args.lookback,
        skip_sessions=args.skip,
        top_fraction=args.top_fraction,
        max_positions=args.max_positions,
    )
    completed = run_monthly_momentum(config, strategy)
    summary = {
        "output_dir": str(completed.output_dir),
        "total_return": completed.metrics["total_return"],
        "annualized_return": completed.metrics["annualized_return"],
        "max_drawdown": completed.metrics["max_drawdown"],
        "sharpe": completed.metrics["sharpe"],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
