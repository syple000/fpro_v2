"""将可定位的问题合并为最小重拉区间并重复检测。"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable
from datetime import date, timedelta
from pathlib import Path

from data_cleaning.detector import detect
from data_cleaning.models import DetectionReport

Refetch = Callable[[str, date, date], int]


def repair(
    root: str | Path,
    *,
    through: date,
    refetch: Refetch,
    datasets: Iterable[str] | None = None,
    start: date | None = None,
    max_rounds: int = 2,
) -> DetectionReport:
    """定向重拉可自动处理的问题，直到通过、无可重拉问题或不再改变。"""
    if max_rounds < 1:
        raise ValueError("max_rounds 必须大于等于 1")
    selected = None if datasets is None else tuple(datasets)
    report = detect(root, through=through, datasets=selected, start=start)
    for _ in range(max_rounds):
        ranges = refetch_ranges(report)
        if not ranges:
            break
        previous = tuple(issue.issue_id for issue in report.issues)
        for dataset, range_start, range_end in ranges:
            refetch(dataset, range_start, range_end)
        report = detect(root, through=through, datasets=selected, start=start)
        if tuple(issue.issue_id for issue in report.issues) == previous:
            break
    return report


def refetch_ranges(report: DetectionReport) -> tuple[tuple[str, date, date], ...]:
    """从人工问题的 REFETCH 建议中提取并合并连续日期区间。"""
    days: dict[str, set[date]] = defaultdict(set)
    for issue in report.issues:
        suggested = issue.suggested
        if issue.fix_mode != "MANUAL" or not suggested or suggested.get("action") != "REFETCH":
            continue
        try:
            start = date.fromisoformat(str(suggested["start_date"]))
            end = date.fromisoformat(str(suggested["end_date"]))
        except (KeyError, ValueError):
            continue
        current = start
        while current <= end:
            days[issue.dataset].add(current)
            current += timedelta(days=1)

    ranges: list[tuple[str, date, date]] = []
    dataset_order = {dataset: index for index, dataset in enumerate(report.datasets)}
    for dataset, values in days.items():
        ordered = sorted(values)
        range_start = range_end = ordered[0]
        for current in ordered[1:]:
            if current == range_end + timedelta(days=1):
                range_end = current
                continue
            ranges.append((dataset, range_start, range_end))
            range_start = range_end = current
        ranges.append((dataset, range_start, range_end))
    ranges.sort(key=lambda item: (dataset_order.get(item[0], len(dataset_order)), item[1]))
    return tuple(ranges)
