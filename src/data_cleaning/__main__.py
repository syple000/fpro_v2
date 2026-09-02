"""检测、定向修复并发布 Tushare 离线数据。"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

from data_cleaning import (
    DetectionReport,
    detect,
    publish,
    read_report,
    refetch_ranges,
    repair,
    repair_instructions,
    write_report,
)
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
        source_report = read_report(args.issues)
        ranges = refetch_ranges(source_report)
        if ranges and not args.token:
            parser.error("请通过环境变量 TUSHARE_TOKEN 或 --token 提供 Token")
        _print_repair_plan(source_report)
        if ranges:
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
                    report=source_report,
                    refetch=refetch,
                    max_rounds=args.max_rounds,
                )
        else:
            report = repair(args.input, report=source_report, max_rounds=args.max_rounds)
        write_report(report, args.output)
        _print_report(report, args.output)
        _print_manual_work(report)
        raise SystemExit(0 if report.passed else 1)

    report = read_report(args.issues)
    release_path = publish(
        args.input,
        args.output_root,
        report=report,
        decisions_path=args.decisions,
        release_id=args.release,
    )
    _print_release(release_path)


def _detect_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("detect", help="只读检测并输出问题报告")
    _scope_arguments(parser)
    parser.add_argument("--output", type=Path, required=True)


def _repair_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("repair", help="读取检测报告并逐项修复")
    parser.add_argument("--input", type=Path, default=Path("dataset/tushare"))
    parser.add_argument("--issues", type=Path, required=True)
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


def _print_report(report: DetectionReport, output: Path) -> None:
    print("检测结果：")
    for dataset in report.datasets:
        print(f"{dataset} ({report.row_counts.get(dataset, 0)} 行)")
        dataset_issues = [issue for issue in report.issues if issue.dataset == dataset]
        for check in (item for item in report.checks if item.dataset == dataset):
            modes = [issue.fix_mode for issue in dataset_issues if issue.rule_id == check.check_id]
            suffix = ""
            if modes:
                suffix = f"：{modes.count('AUTO_FIX')} 个自动，{modes.count('MANUAL')} 个人工"
            label = {"PASS": "通过", "WARN": "告警", "FAIL": "失败"}[check.status]
            print(f"  [{label}] {check.check_id} - {check.description}{suffix}")
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


def _print_repair_plan(report: DetectionReport) -> None:
    print("修复计划：")
    instructions = repair_instructions(report)
    if not instructions:
        print("  没有需要修复的问题")
        return
    labels = {"PATCH": "自动补丁", "REFETCH": "自动重拉", "MANUAL": "待人工"}
    for instruction in instructions:
        print(
            f"  [{labels[instruction.action]}] {instruction.dataset} "
            f"{instruction.rule_id} - {instruction.message}"
        )


def _print_manual_work(report: DetectionReport) -> None:
    remaining = [
        instruction for instruction in repair_instructions(report) if instruction.action != "PATCH"
    ]
    patches = [
        instruction for instruction in repair_instructions(report) if instruction.action == "PATCH"
    ]
    if patches:
        print(f"已生成 {len(patches)} 个确定性补丁，publish 时按数据集合并。")
    if not remaining:
        print("待人工干预：0")
        return
    print(f"待人工干预：{len(remaining)}")
    for instruction in remaining:
        print(
            f"  {instruction.issue_id} | {instruction.dataset} | "
            f"{instruction.rule_id} | {instruction.message}"
        )


def _print_release(release_path: Path) -> None:
    payload = json.loads((release_path / "release.json").read_text(encoding="utf-8"))
    print("发布结果：")
    for dataset, state in payload["datasets"].items():
        print(
            f"  [{state['status']}] {dataset}: {state['row_count']} 行，"
            f"未解决 {len(state['open_issue_ids'])} 个"
        )
    print(json.dumps({"release": str(release_path)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
