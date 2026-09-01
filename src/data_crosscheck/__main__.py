"""运行一批 Tushare/QMT 数据交叉检查。"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date

from data_crosscheck import crosscheck_sample
from qmt_receiver import QmtAgentClient


def main() -> None:
    parser = argparse.ArgumentParser(description="随机抽样比较 Tushare 与 QMT，报告跨源数据差异")
    parser.add_argument("--tushare-dir", default="dataset/tushare")
    parser.add_argument("--qmt-dir", default="dataset/qmt")
    parser.add_argument("--qmt-url", default="http://127.0.0.1:8765")
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--sample-size", type=int, default=20)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--output", default="./data_crosscheck.json")
    args = parser.parse_args()

    if args.start_date > args.end_date:
        parser.error("--start-date 不能晚于 --end-date")

    with QmtAgentClient(args.qmt_url) as client:
        report = crosscheck_sample(
            client,
            tushare_root=args.tushare_dir,
            qmt_root=args.qmt_dir,
            start_date=args.start_date,
            end_date=args.end_date,
            sample_size=args.sample_size,
            seed=args.seed,
        )
    with open(args.output, "w") as f:
        json.dump(asdict(report), f, ensure_ascii=False, indent=2)
    raise SystemExit(0 if report.passed else 1)


if __name__ == "__main__":
    main()
