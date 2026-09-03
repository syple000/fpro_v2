"""检测并以可回滚事务原位修复 Tushare 离线数据。"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from collections.abc import Mapping
from datetime import date
from pathlib import Path

from data_cleaning import (
    Decision,
    DetectionReport,
    RepairInstruction,
    detect,
    read_decisions,
    read_report,
    record_detection,
    refetch_ranges,
    repair,
    repair_instructions,
    rollback,
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
    _rollback_parser(subparsers)
    args = parser.parse_args()

    if args.command == "detect":
        report = detect(
            args.input,
            through=args.through,
            start=args.start,
            datasets=args.datasets,
        )
        report_path = record_detection(args.input, report)
        _print_report(report, report_path)
        raise SystemExit(0 if report.passed else 1)

    if args.command == "repair":
        source_report = read_report(args.issues)
        decisions = read_decisions(args.decisions)
        ranges = refetch_ranges(source_report, decisions)
        if ranges and not args.token:
            parser.error("请通过环境变量 TUSHARE_TOKEN 或 --token 提供 Token")
        _print_repair_plan(source_report, decisions)
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

                result = repair(
                    args.input,
                    report=source_report,
                    refetch=refetch,
                    max_rounds=args.max_rounds,
                    decisions_path=args.decisions,
                )
        else:
            result = repair(
                args.input,
                report=source_report,
                max_rounds=args.max_rounds,
                decisions_path=args.decisions,
            )
        _print_report(result.report, result.journal_path)
        print(f"修复记录：{result.journal_path}")
        print(f"回滚命令：data-cleaning rollback --repair-id {result.repair_id}")
        _print_manual_work(result.report)
        raise SystemExit(0 if result.report.passed else 1)

    journal = rollback(args.input, args.repair_id)
    print(f"已回滚修复：{journal}")
    print("数据已恢复到修复前状态；请重新执行全量 detect 更新质量状态。")


def _detect_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("detect", help="检测并把报告记录在数据目录")
    _scope_arguments(parser)


def _repair_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("repair", help="读取检测报告并逐项修复")
    parser.add_argument("--input", type=Path, default=Path("dataset/tushare"))
    parser.add_argument("--issues", type=Path, required=True)
    parser.add_argument("--decisions", type=Path)
    parser.add_argument("--max-rounds", type=int, default=2)
    parser.add_argument("--token", default=os.environ.get("TUSHARE_TOKEN"))
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    parser.add_argument("--requests-per-minute", type=int, default=DEFAULT_REQUESTS_PER_MINUTE)
    parser.add_argument("--max-concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY)


def _rollback_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("rollback", help="恢复一次原位修复")
    parser.add_argument("--input", type=Path, default=Path("dataset/tushare"))
    parser.add_argument("--repair-id", required=True)


def _scope_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--input", type=Path, default=Path("dataset/tushare"))
    parser.add_argument("--through", type=date.fromisoformat, required=True)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--datasets", nargs="+", choices=tuple(TABLE_SCHEMAS))


def _print_report(report: DetectionReport, output: Path) -> None:
    print("检测结果：")
    for dataset in report.datasets:
        dataset_issues = [issue for issue in report.issues if issue.dataset == dataset]
        errors = sum(issue.severity == "ERROR" for issue in dataset_issues)
        warnings = sum(issue.severity == "WARNING" for issue in dataset_issues)
        checks = sum(check.dataset == dataset for check in report.checks)
        label = "失败" if errors else "告警" if warnings else "通过"
        print(
            f"  [{label}] {dataset}: {report.row_counts.get(dataset, 0)} 行，"
            f"{checks} 项检查，{errors} 错误，{warnings} 告警"
        )
    print(
        json.dumps(
            {
                "datasets": list(report.datasets),
                "issues": len(report.issues),
                "errors": sum(issue.severity == "ERROR" for issue in report.issues),
                "warnings": sum(issue.severity == "WARNING" for issue in report.issues),
                "manual": sum(issue.fix_mode == "MANUAL" for issue in report.issues),
                "auto_fix": sum(issue.fix_mode == "AUTO_FIX" for issue in report.issues),
                "output": str(output),
            },
            ensure_ascii=False,
        )
    )


def _print_repair_plan(
    report: DetectionReport,
    decisions: Mapping[str, Decision] | None = None,
) -> None:
    print("修复计划：")
    instructions = repair_instructions(report)
    if not instructions:
        print("  没有需要修复的问题")
        return
    labels = {"PATCH": "自动补丁", "REFETCH": "自动重拉", "MANUAL": "待人工"}
    groups: Counter[tuple[str, str, str, str]] = Counter()
    for instruction in instructions:
        decision = (decisions or {}).get(instruction.issue_id)
        if decision is None:
            label = labels[instruction.action]
            message = instruction.message
        else:
            label = "人工补丁"
            message = decision.reason
        groups[(label, instruction.dataset, instruction.rule_id, message)] += 1
    for (label, dataset, rule_id, message), count in groups.items():
        suffix = f"（{count} 项）" if count > 1 else ""
        print(f"  [{label}] {dataset} {rule_id}{suffix} - {message}")


def _print_manual_work(report: DetectionReport) -> None:
    instructions = repair_instructions(report)
    severity_by_id = {issue.issue_id: issue.severity for issue in report.issues}
    blocking = [
        instruction
        for instruction in instructions
        if instruction.action != "PATCH" and severity_by_id[instruction.issue_id] == "ERROR"
    ]
    warnings = [
        instruction
        for instruction in instructions
        if instruction.action != "PATCH" and severity_by_id[instruction.issue_id] == "WARNING"
    ]
    if not blocking:
        print("待人工干预：0")
    else:
        print(f"待人工干预（阻止使用）：{len(blocking)}")
    _print_instruction_groups(blocking)
    if warnings:
        print(f"待复核告警（不阻止使用）：{len(warnings)}")
    _print_instruction_groups(warnings)


def _print_instruction_groups(instructions: list[RepairInstruction]) -> None:
    groups = Counter((item.dataset, item.rule_id, item.message) for item in instructions)
    for (dataset, rule_id, message), count in groups.items():
        suffix = f"（{count} 项）" if count > 1 else ""
        print(f"  {dataset} | {rule_id}{suffix} | {message}")


if __name__ == "__main__":
    main()
