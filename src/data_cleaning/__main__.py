"""检测、定向修复并发布 Tushare 离线数据。"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

from data_cleaning import detect, publish, read_report, repair, write_report
from tushare_data import (
    DEFAULT_API_URL,
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_REQUESTS_PER_MINUTE,
    TABLE_SCHEMAS,
    TushareDataStore,
    create_pro_client,
    sync_datasets,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    _detect_parser(subparsers)
    _repair_parser(subparsers)
    _publish_parser(subparsers)
    args = parser.parse_args()

    if args.command == "detect":
        report = detect(
            args.input,
            through=args.through,
            start=args.start,
            datasets=args.datasets,
        )
        write_report(report, args.output)
        _print_report(report, args.output)
        raise SystemExit(0 if report.passed else 1)

    if args.command == "repair":
        if not args.token:
            parser.error("请通过环境变量 TUSHARE_TOKEN 或 --token 提供 Token")
        pro = create_pro_client(
            args.token,
            args.api_url,
            requests_per_minute=args.requests_per_minute,
            max_concurrency=args.max_concurrency,
        )
        with TushareDataStore(args.input) as store:

            def refetch(dataset: str, start: date, end: date) -> int:
                return sync_datasets(
                    pro,
                    store,
                    (dataset,),
                    start,
                    end,
                    force=True,
                )[dataset]

            report = repair(
                args.input,
                through=args.through,
                start=args.start,
                datasets=args.datasets,
                refetch=refetch,
                max_rounds=args.max_rounds,
            )
        write_report(report, args.output)
        _print_report(report, args.output)
        raise SystemExit(0 if report.passed else 1)

    report = read_report(args.issues)
    release_path = publish(
        args.input,
        args.output_root,
        report=report,
        decisions_path=args.decisions,
        release_id=args.release,
    )
    print(json.dumps({"release": str(release_path)}, ensure_ascii=False))


def _detect_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("detect", help="只读检测并输出问题报告")
    _scope_arguments(parser)
    parser.add_argument("--output", type=Path, required=True)


def _repair_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("repair", help="对可定位问题定向重拉并复检")
    _scope_arguments(parser)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-rounds", type=int, default=2)
    parser.add_argument("--token", default=os.environ.get("TUSHARE_TOKEN"))
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    parser.add_argument("--requests-per-minute", type=int, default=DEFAULT_REQUESTS_PER_MINUTE)
    parser.add_argument("--max-concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY)


def _publish_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("publish", help="应用决策、复检并原子发布")
    parser.add_argument("--input", type=Path, default=Path("dataset/tushare"))
    parser.add_argument("--output-root", type=Path, default=Path("dataset/tushare_published"))
    parser.add_argument("--issues", type=Path, required=True)
    parser.add_argument("--decisions", type=Path)
    parser.add_argument("--release", required=True)


def _scope_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--input", type=Path, default=Path("dataset/tushare"))
    parser.add_argument("--through", type=date.fromisoformat, required=True)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--datasets", nargs="+", choices=tuple(TABLE_SCHEMAS))


def _print_report(report, output: Path) -> None:
    print(
        json.dumps(
            {
                "datasets": list(report.datasets),
                "issues": len(report.issues),
                "manual": sum(issue.fix_mode == "MANUAL" for issue in report.issues),
                "auto_fix": sum(issue.fix_mode == "AUTO_FIX" for issue in report.issues),
                "output": str(output),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
