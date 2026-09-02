"""应用确定修正和人工决策，生成带质量门禁的不可变发布版本。"""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Iterable, Mapping
from datetime import date
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from data_cleaning.detector import active_partitions, detect, source_fingerprint
from data_cleaning.models import Decision, DetectionReport, Issue, read_decisions
from tushare_data.schemas import TABLE_SCHEMAS


class PatchMismatchError(ValueError):
    """人工补丁声明的原值与当前数据不一致。"""


_HARD_RULES = frozenset(
    {
        "manifest_v1",
        "manifest_file_missing_v1",
        "parquet_read_v1",
        "schema_v1",
        "partition_value_v1",
        "duplicate_key_v1",
        "required_value_v1",
        "finite_float_v1",
        "dataset_empty_v1",
        "missing_market_partition_v1",
        "calendar_exchange_coverage_v1",
        "stk_limit_partition_missing_v1",
    }
)


def publish(
    input_root: str | Path,
    output_root: str | Path,
    *,
    report: DetectionReport,
    release_id: str,
    decisions_path: str | Path | None = None,
) -> Path:
    """物化、复检并原子发布一个数据版本。"""
    source = Path(input_root).expanduser().resolve()
    destination = Path(output_root).expanduser().resolve()
    if not release_id or Path(release_id).name != release_id or release_id in {".", ".."}:
        raise ValueError("release_id 必须是非空的单个路径段")
    if report.start is not None:
        raise ValueError("发布必须使用不带 start 限制的全量检测报告")
    current_fingerprint = source_fingerprint(source, report.datasets)
    if current_fingerprint != report.input_fingerprint:
        raise ValueError("问题报告与当前原始 Manifest 不匹配，请重新执行 detect")

    decisions = read_decisions(decisions_path)
    release_path = destination / "releases" / release_id
    if release_path.exists():
        raise FileExistsError(f"发布版本已存在: {release_path}")
    temporary = destination / "releases" / f".{release_id}.{uuid4().hex}.tmp"
    temporary.mkdir(parents=True)
    try:
        states = _build_release(source, temporary, report, decisions)
        payload = {
            "release_id": release_id,
            "validated_through": report.through.isoformat(),
            "ruleset_version": 2,
            "input_fingerprint": report.input_fingerprint,
            "datasets": states,
        }
        _write_json(temporary / "release.json", payload)
        os.rename(temporary, release_path)
        _switch_current(destination, release_id)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return release_path


def _build_release(
    source: Path,
    candidate: Path,
    report: DetectionReport,
    decisions: Mapping[str, Decision],
) -> dict[str, object]:
    issues_by_dataset: dict[str, list[Issue]] = {dataset: [] for dataset in TABLE_SCHEMAS}
    for issue in report.issues:
        issues_by_dataset[issue.dataset].append(issue)
    selected = set(report.datasets)
    states: dict[str, object] = {}

    for dataset in TABLE_SCHEMAS:
        states[dataset] = _publish_dataset(
            source,
            candidate,
            dataset,
            selected=dataset in selected,
            through=report.through,
            row_count=report.row_counts.get(dataset, 0),
            issues=issues_by_dataset[dataset],
            decisions=decisions,
        )
    return states


def _publish_dataset(
    source: Path,
    candidate: Path,
    dataset: str,
    *,
    selected: bool,
    through: date,
    row_count: int,
    issues: list[Issue],
    decisions: Mapping[str, Decision],
) -> dict[str, object]:
    """合并一个数据集的修复，复检后返回该数据集的发布状态。"""
    if not selected:
        return {
            "status": "UNAVAILABLE",
            "row_count": 0,
            "auto_fixes": 0,
            "manual_patches": 0,
            "open_issue_ids": [f"{dataset}:not_validated"],
        }

    unresolved = [
        issue.issue_id
        for issue in issues
        if issue.fix_mode == "MANUAL" and not _manual_resolved(issue, decisions.get(issue.issue_id))
    ]
    if unresolved:
        return {
            "status": "UNAVAILABLE",
            "row_count": row_count,
            "auto_fixes": 0,
            "manual_patches": 0,
            "open_issue_ids": sorted(unresolved),
        }

    auto_fixes = sum(issue.fix_mode == "AUTO_FIX" for issue in issues)
    manual_patches = sum(
        decisions.get(issue.issue_id) is not None and decisions[issue.issue_id].action == "PATCH"
        for issue in issues
    )
    try:
        _merge_dataset_fixes(
            source,
            candidate,
            dataset,
            through=through,
            issues=issues,
            decisions=decisions,
        )
    except PatchMismatchError as exc:
        shutil.rmtree(candidate / dataset, ignore_errors=True)
        return {
            "status": "UNAVAILABLE",
            "row_count": row_count,
            "auto_fixes": 0,
            "manual_patches": 0,
            "open_issue_ids": [f"{dataset}:patch_mismatch:{exc}"],
        }

    post = detect(candidate, through=through, datasets=(dataset,))
    accepted = {issue_id for issue_id, decision in decisions.items() if decision.action == "ACCEPT"}
    open_post = sorted(
        issue.issue_id
        for issue in post.issues
        if issue.fix_mode == "MANUAL" and issue.issue_id not in accepted
    )
    if open_post:
        shutil.rmtree(candidate / dataset, ignore_errors=True)
        return {
            "status": "UNAVAILABLE",
            "row_count": post.row_counts.get(dataset, 0),
            "auto_fixes": 0,
            "manual_patches": 0,
            "open_issue_ids": open_post,
        }
    return {
        "status": "AVAILABLE",
        "row_count": post.row_counts.get(dataset, 0),
        "auto_fixes": auto_fixes,
        "manual_patches": manual_patches,
        "open_issue_ids": [],
    }


def _merge_dataset_fixes(
    source: Path,
    candidate: Path,
    dataset: str,
    *,
    through: date,
    issues: Iterable[Issue],
    decisions: Mapping[str, Decision],
) -> None:
    """把一个数据集的自动补丁和人工 PATCH 合并到候选发布目录。"""
    modifications: dict[str, list[tuple[Issue, Decision | None]]] = {}
    for issue in issues:
        decision = decisions.get(issue.issue_id)
        if issue.fix_mode == "AUTO_FIX" or (decision and decision.action == "PATCH"):
            if issue.partition is None:
                raise PatchMismatchError(f"{issue.issue_id} 缺少分区")
            modifications.setdefault(issue.partition, []).append((issue, decision))

    for partition in active_partitions(source, dataset):
        if partition.value is None:
            raise PatchMismatchError(f"{partition.label} 分区日期无法解析")
        if partition.value > through:
            continue
        target_directory = candidate / dataset / partition.label
        target_directory.mkdir(parents=True, exist_ok=True)
        partition_modifications = modifications.get(partition.label, [])
        if not partition_modifications:
            for path in partition.files:
                _link_or_copy(path, target_directory / path.name)
            shutil.copy2(partition.manifest, target_directory / partition.manifest.name)
            continue

        tables = [pq.ParquetFile(path).read() for path in partition.files]
        table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        rows = table.to_pylist()
        for issue, decision in partition_modifications:
            matches = [row for row in rows if _matches(row, issue.key)]
            if len(matches) != 1:
                raise PatchMismatchError(f"{issue.issue_id} 匹配到 {len(matches)} 行")
            row = matches[0]
            if issue.fix_mode == "AUTO_FIX":
                values = issue.suggested.get("values") if issue.suggested else None
                if not isinstance(values, Mapping):
                    raise PatchMismatchError(f"{issue.issue_id} 没有确定修正值")
            else:
                assert decision is not None
                expected = decision.expected or {}
                if any(not _equal(row.get(name), value) for name, value in expected.items()):
                    raise PatchMismatchError(issue.issue_id)
                values = decision.values or {}
            unknown = set(values) - set(TABLE_SCHEMAS[dataset].names)
            if unknown:
                raise PatchMismatchError(f"{issue.issue_id} 包含未知字段 {sorted(unknown)}")
            row.update(values)

        cleaned = pa.Table.from_pylist(rows, schema=TABLE_SCHEMAS[dataset])
        filename = "part-clean.parquet"
        pq.write_table(cleaned, target_directory / filename)
        _write_json(
            target_directory / "_manifest.json",
            {
                "version": 1,
                "updated_at": 1,
                "files": [filename],
                "file_committed_at": {filename: 1},
            },
        )


def _matches(row: Mapping[str, object], key: Mapping[str, object]) -> bool:
    return all(_equal(row.get(name), value) for name, value in key.items())


def _manual_resolved(issue: Issue, decision: Decision | None) -> bool:
    if decision is None or decision.action == "REFETCH":
        return False
    return not (decision.action == "ACCEPT" and issue.rule_id in _HARD_RULES)


def _equal(actual: object, expected: object) -> bool:
    if isinstance(actual, date):
        return actual.isoformat() == expected
    return actual == expected


def _link_or_copy(source: Path, destination: Path) -> None:
    try:
        os.link(source, destination)
    except OSError:
        shutil.copy2(source, destination)


def _write_json(path: Path, payload: object) -> None:
    with path.open("x", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2, sort_keys=True)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())


def _switch_current(root: Path, release_id: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    current = root / "current"
    if current.exists() and not current.is_symlink():
        raise FileExistsError(f"current 存在且不是符号链接: {current}")
    temporary = root / f".current.{uuid4().hex}.tmp"
    try:
        os.symlink(Path("releases") / release_id, temporary)
        os.replace(temporary, current)
    finally:
        temporary.unlink(missing_ok=True)
