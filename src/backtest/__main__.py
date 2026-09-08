"""内置月度动量策略的命令行入口。"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from backtest.config import BacktestConfig, RunOptions
from strategies import MomentumConfig, run_monthly_momentum


def main() -> None:
    """解析命令行，运行内置策略，并把汇总指标打印到终端。"""
    arguments = _parser().parse_args()
    config = BacktestConfig(
        start_date=arguments.start,
        end_date=arguments.end,
        frequency="1d",
        symbols=tuple(arguments.symbols) if arguments.symbols else None,
        initial_cash=arguments.initial_cash,
        slippage_bps=arguments.slippage_bps,
        volume_limit=arguments.volume_limit,
    )
    completed = run_monthly_momentum(
        config=config,
        strategy_config=MomentumConfig(
            lookback_sessions=arguments.lookback_sessions,
            selection_count=arguments.selection_count,
        ),
        options=RunOptions(
            tushare_root=arguments.tushare_dir,
            qmt_root=arguments.qmt_dir,
            security_code_history=arguments.security_code_history,
            output_dir=arguments.output_dir,
        ),
    )
    print(json.dumps(completed.metrics, ensure_ascii=False, indent=2))


def _parser() -> argparse.ArgumentParser:
    """集中声明 CLI 参数，保持 main 只负责组装配置。"""
    parser = argparse.ArgumentParser(description="运行月度动量日线回测")
    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        default=date(2017, 1, 1),
        help="回测开始日期（含）",
    )
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=date(2026, 8, 31),
        help="回测结束日期（含）",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        help="可选证券代码列表；不传时使用全市场",
    )
    parser.add_argument(
        "--initial-cash",
        type=float,
        default=10_000_000.0,
        help="初始现金，单位元",
    )
    parser.add_argument(
        "--slippage-bps",
        type=float,
        default=5.0,
        help="单边滑点，单位基点",
    )
    parser.add_argument(
        "--volume-limit",
        type=float,
        default=0.10,
        help="单笔成交占上一根 K 线成交量的最大比例",
    )
    parser.add_argument(
        "--lookback-sessions",
        type=int,
        default=20,
        help="动量回看交易日数",
    )
    parser.add_argument(
        "--selection-count",
        type=int,
        default=10,
        help="每次选择的最大证券数量",
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
        "--security-code-history",
        type=Path,
        help="可选 security_code_history.parquet；启用持久 sid 和历史交易代码",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="回测结果输出目录",
    )
    return parser


if __name__ == "__main__":
    main()
