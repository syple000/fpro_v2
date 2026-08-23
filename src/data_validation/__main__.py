"""运行一批 Tushare/QMT 随机抽样复核。"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date

from data_validation import validate_sample
from qmt_receiver import QmtAgentClient


def main() -> None:
    parser = argparse.ArgumentParser(description="随机抽样复核 Tushare 与 QMT 数据")
    parser.add_argument("--tushare-dir", default="data/tushare")
    parser.add_argument("--qmt-dir", default="data/qmt_receiver")
    parser.add_argument("--qmt-url", default="http://127.0.0.1:8765")
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--sample-size", type=int, default=20)
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    if args.start_date > args.end_date:
        parser.error("--start-date 不能晚于 --end-date")

    with QmtAgentClient(args.qmt_url) as client:
        report = validate_sample(
            client,
            tushare_root=args.tushare_dir,
            qmt_root=args.qmt_dir,
            start_date=args.start_date,
            end_date=args.end_date,
            sample_size=args.sample_size,
            seed=args.seed,
        )
    print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
    raise SystemExit(0 if report.passed else 1)


if __name__ == "__main__":
    main()
