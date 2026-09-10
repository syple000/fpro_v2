"""在原数据目录中事务式修复问题，并保留可回滚记录。"""

from __future__ import annotations

import json
import os
import shutil
from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal
from urllib.parse import quote
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from data_cleaning.detector import active_partitions, detect, source_fingerprint
from data_cleaning.models import (
    Decision,
    DetectionReport,
    Issue,
    read_decisions,
    record_detection,
)
from tushare_data import TushareDataStore
from tushare_data.schemas import TABLE_PARTITION_BY, TABLE_SCHEMAS

Refetch = Callable[[str, date, date], int]
RepairAction = Literal["PATCH", "REFETCH", "MANUAL"]
_FULL_SNAPSHOT_REFETCH = frozenset(
    {
        "stock_basic",
        "dividend",
        "forecast",
        "express",
        "fina_audit",
        "income",
        "balancesheet",
        "cashflow",
        "fina_indicator",
        "sw_industry",
    }
)
_PARTITION_DATE_REFETCH = frozenset(TABLE_SCHEMAS) - frozenset(
    {
        "dividend",
        "forecast",
        "express",
        "fina_audit",
        "income",
        "balancesheet",
        "cashflow",
        "fina_indicator",
    }
)
_RESET_BEFORE_REFETCH = frozenset(
    {
        "manifest_v1",
        "manifest_file_missing_v1",
        "parquet_read_v1",
        "schema_v1",
        "partition_value_v1",
    }
)


@dataclass(frozen=True, slots=True)
class RepairInstruction:
    """一个检测问题的明确修复方式。"""

    issue_id: str
    dataset: str
    rule_id: str
    action: RepairAction
    details: Mapping[str, object]
    message: str


IssueRepairer = Callable[[Issue], RepairInstruction]


@dataclass(frozen=True, slots=True)
class RepairResult:
    """一次原位修复的最终报告和回滚凭据。"""

    report: DetectionReport
    repair_id: str
    journal_path: Path


class PatchMismatchError(ValueError):
    """补丁目标、原值或字段与检测报告不一致。"""


def repair(
    root: str | Path,
    *,
    report: DetectionReport,
    refetch: Refetch | None = None,
    max_rounds: int = 2,
    decisions_path: str | Path | None = None,
) -> RepairResult:
    """备份受影响分区后原位补丁/重拉，失败时自动恢复。"""
    if max_rounds < 1:
        raise ValueError("max_rounds 必须大于等于 1")
    source = Path(root).expanduser().resolve()
    current_fingerprint = source_fingerprint(source, report.datasets)
    if current_fingerprint != report.input_fingerprint:
        raise ValueError("检测报告与当前 Manifest 不匹配，请重新执行 detect")

    decisions = read_decisions(decisions_path)
    unknown = sorted(set(decisions) - {issue.issue_id for issue in report.issues})
    if unknown:
        raise ValueError(f"人工决策包含当前报告不存在的问题: {unknown}")

    transaction = _RepairTransaction(source, report)
    current = report
    try:
        _apply_patches(source, transaction, current, decisions)
        detected_after_change = False
        previous_refetch_issues = _refetch_issue_ids(current, decisions)
        report_path: Path | None = None
        for _ in range(max_rounds):
            ranges = refetch_ranges(current, decisions)
            if not ranges:
                break
            if refetch is None:
                raise ValueError("检测报告包含需要重拉的问题，但没有提供 refetch")
            for dataset, range_start, range_end in ranges:
                transaction.backup_refetch(dataset, range_start, range_end)
                _reset_broken_partitions(source, current, dataset, range_start, range_end)
                rows = refetch(dataset, range_start, range_end)
                transaction.record_refetch(dataset, range_start, range_end, rows)
            current = detect(
                source,
                through=current.through,
                datasets=current.datasets,
                start=current.start,
            )
            report_path = record_detection(source, current)
            detected_after_change = True
            current_refetch_issues = _refetch_issue_ids(current, decisions)
            if current_refetch_issues == previous_refetch_issues:
                break
            previous_refetch_issues = current_refetch_issues

        late_patch = detected_after_change and _apply_patches(
            source, transaction, current, decisions
        )
        if not detected_after_change or late_patch:
            current = detect(
                source,
                through=current.through,
                datasets=current.datasets,
                start=current.start,
            )
            report_path = record_detection(source, current)
        assert report_path is not None
        transaction.commit(current.input_fingerprint, report_path)
    except BaseException:
        transaction.restore("ROLLED_BACK")
        raise
    return RepairResult(
        report=current,
        repair_id=transaction.repair_id,
        journal_path=transaction.journal_path,
    )


def rollback(root: str | Path, repair_id: str) -> Path:
    """恢复一次已提交修复；若数据后来又变过则拒绝覆盖。"""
    if not repair_id or Path(repair_id).name != repair_id or repair_id in {".", ".."}:
        raise ValueError("repair_id 必须是单个路径段")
    source = Path(root).expanduser().resolve()
    transaction = _RepairTransaction.load(source, repair_id)
    if transaction.status != "COMMITTED":
        raise ValueError(f"修复 {repair_id} 当前状态不是 COMMITTED")
    if source_fingerprint(source, transaction.datasets) != transaction.output_fingerprint:
        raise ValueError("修复后数据又发生了变化，为避免覆盖后续修改，拒绝回滚")
    transaction.restore("ROLLED_BACK")
    return transaction.journal_path


def repair_instructions(report: DetectionReport) -> tuple[RepairInstruction, ...]:
    """按规则将检测问题逐一转换为修复方式。"""
    return tuple(
        _REPAIRERS.get(issue.rule_id, repair_unknown_issue)(issue) for issue in report.issues
    )


def refetch_ranges(
    report: DetectionReport,
    decisions: Mapping[str, Decision] | None = None,
) -> tuple[tuple[str, date, date], ...]:
    """从逐项修复方式中提取重拉日期，并合并连续区间。"""
    days: dict[str, set[date]] = defaultdict(set)
    for instruction in repair_instructions(report):
        decision = (decisions or {}).get(instruction.issue_id)
        if decision is not None:
            continue
        if instruction.action != "REFETCH":
            continue
        try:
            start = date.fromisoformat(str(instruction.details["start_date"]))
            end = date.fromisoformat(str(instruction.details["end_date"]))
        except (KeyError, ValueError):
            continue
        current = start
        while current <= end:
            days[instruction.dataset].add(current)
            current += timedelta(days=1)

    ranges: list[tuple[str, date, date]] = []
    dataset_order = {dataset: index for index, dataset in enumerate(report.datasets)}
    for dataset, values in days.items():
        ordered = sorted(values)
        if dataset in _FULL_SNAPSHOT_REFETCH:
            ranges.append((dataset, ordered[0], ordered[-1]))
            continue
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


def _refetch_issue_ids(
    report: DetectionReport,
    decisions: Mapping[str, Decision],
) -> tuple[str, ...]:
    return tuple(
        instruction.issue_id
        for instruction in repair_instructions(report)
        if instruction.action == "REFETCH" and instruction.issue_id not in decisions
    )


class _RepairTransaction:
    """只备份会改动的路径；备份文件优先使用硬链接，不复制整库。"""

    def __init__(self, root: Path, report: DetectionReport) -> None:
        now = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        self.root = root
        self.repair_id = f"{now}-{uuid4().hex[:8]}"
        self.directory = root / "_quality" / "repairs" / self.repair_id
        self.journal_path = self.directory / "journal.json"
        self.datasets = report.datasets
        self.input_fingerprint = report.input_fingerprint
        self.output_fingerprint: str | None = None
        self.status = "PREPARED"
        self._entries: dict[str, dict[str, object]] = {}
        self._dataset_partitions_before: dict[str, list[str]] = {}
        self._operations: list[dict[str, object]] = []
        self.directory.mkdir(parents=True)
        self._write_journal()

    @classmethod
    def load(cls, root: Path, repair_id: str) -> _RepairTransaction:
        journal_path = root / "_quality" / "repairs" / repair_id / "journal.json"
        try:
            payload = json.loads(journal_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"无法读取修复记录: {journal_path}") from exc
        if not isinstance(payload, dict) or payload.get("version") != 1:
            raise ValueError(f"修复记录格式无效: {journal_path}")
        instance = cls.__new__(cls)
        instance.root = root
        instance.repair_id = repair_id
        instance.directory = journal_path.parent
        instance.journal_path = journal_path
        instance.datasets = tuple(payload["datasets"])
        instance.input_fingerprint = str(payload["input_fingerprint"])
        output = payload.get("output_fingerprint")
        instance.output_fingerprint = str(output) if output else None
        instance.status = str(payload["status"])
        instance._entries = {str(item["path"]): dict(item) for item in payload.get("entries", [])}
        raw_before = payload.get("dataset_partitions_before", {})
        instance._dataset_partitions_before = {
            str(dataset): [str(path) for path in paths] for dataset, paths in raw_before.items()
        }
        instance._operations = [dict(item) for item in payload.get("operations", [])]
        return instance

    def backup_refetch(self, dataset: str, start: date, end: date) -> None:
        self._backup(self.root / "_meta" / "sync_all" / f"{dataset}.json", "file")
        if dataset in _FULL_SNAPSHOT_REFETCH:
            self._backup_dataset(dataset)
            return
        current = start
        while current <= end:
            self._backup(_partition_path(self.root, dataset, current), "directory")
            current += timedelta(days=1)

    def backup_partition(self, dataset: str, label: str) -> None:
        target = (self.root / dataset / label).resolve()
        table_root = (self.root / dataset).resolve()
        if target == table_root or table_root not in target.parents:
            raise PatchMismatchError(f"非法分区路径: {dataset}/{label}")
        self._backup(target, "directory")

    def record_refetch(self, dataset: str, start: date, end: date, rows: int) -> None:
        self._operations.append(
            {
                "kind": "REFETCH",
                "dataset": dataset,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "rows": rows,
            }
        )
        self._write_journal()

    def record_patch(
        self,
        issue: Issue,
        *,
        after: Mapping[str, object],
        reason: str,
    ) -> None:
        self._operations.append(
            {
                "kind": "PATCH",
                "issue_id": issue.issue_id,
                "dataset": issue.dataset,
                "partition": issue.partition,
                "key": dict(issue.key),
                "before": dict(issue.observed),
                "after": dict(after),
                "reason": reason,
            }
        )
        self._write_journal()

    def commit(self, output_fingerprint: str, report_path: Path) -> None:
        self.output_fingerprint = output_fingerprint
        self.status = "COMMITTED"
        self._write_journal(report_path=report_path)

    def restore(self, status: str) -> None:
        for dataset, before in self._dataset_partitions_before.items():
            before_set = set(before)
            for partition in _partition_directories(self.root, dataset):
                relative = partition.relative_to(self.root).as_posix()
                if relative not in before_set:
                    shutil.rmtree(partition)
        for relative, entry in reversed(tuple(self._entries.items())):
            target = self.root / relative
            kind = str(entry["kind"])
            if target.exists():
                if kind == "directory":
                    shutil.rmtree(target)
                else:
                    target.unlink()
            if not entry["existed"]:
                continue
            backup = self.directory / "backup" / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            if kind == "directory":
                shutil.copytree(backup, target, copy_function=_link_or_copy)
            else:
                _link_or_copy(backup, target)
        self.status = status
        self.output_fingerprint = None
        self._write_journal()

    def _backup_dataset(self, dataset: str) -> None:
        if dataset in self._dataset_partitions_before:
            return
        partitions = _partition_directories(self.root, dataset)
        self._dataset_partitions_before[dataset] = [
            path.relative_to(self.root).as_posix() for path in partitions
        ]
        for path in partitions:
            self._backup(path, "directory")
        self._write_journal()

    def _backup(self, target: Path, kind: Literal["file", "directory"]) -> None:
        relative = target.relative_to(self.root).as_posix()
        if relative in self._entries:
            return
        existed = target.is_file() if kind == "file" else target.is_dir()
        entry = {"path": relative, "kind": kind, "existed": existed}
        if existed:
            backup = self.directory / "backup" / relative
            backup.parent.mkdir(parents=True, exist_ok=True)
            if kind == "directory":
                shutil.copytree(target, backup, copy_function=_link_or_copy)
            else:
                _link_or_copy(target, backup)
        self._entries[relative] = entry
        self._write_journal()

    def _write_journal(self, *, report_path: Path | None = None) -> None:
        payload: dict[str, object] = {
            "version": 1,
            "repair_id": self.repair_id,
            "status": self.status,
            "datasets": list(self.datasets),
            "input_fingerprint": self.input_fingerprint,
            "output_fingerprint": self.output_fingerprint,
            "entries": list(self._entries.values()),
            "dataset_partitions_before": self._dataset_partitions_before,
            "operations": self._operations,
        }
        if report_path is not None:
            payload["report"] = report_path.relative_to(self.root).as_posix()
        _atomic_json(self.journal_path, payload)


def _apply_patches(
    root: Path,
    transaction: _RepairTransaction,
    report: DetectionReport,
    decisions: Mapping[str, Decision],
) -> bool:
    modifications: dict[tuple[str, str], list[tuple[Issue, Decision | None]]] = {}
    for issue in report.issues:
        decision = decisions.get(issue.issue_id)
        if issue.fix_mode != "AUTO_FIX" and decision is None:
            continue
        if issue.partition is None:
            raise PatchMismatchError(f"{issue.issue_id} 缺少分区")
        modifications.setdefault((issue.dataset, issue.partition), []).append((issue, decision))

    if not modifications:
        return False
    partition_maps = {
        dataset: {partition.label: partition for partition in active_partitions(root, dataset)}
        for dataset in {dataset for dataset, _ in modifications}
    }
    with TushareDataStore(root) as store:
        for (dataset, label), changes in modifications.items():
            partition = partition_maps[dataset].get(label)
            if partition is None or partition.value is None:
                raise PatchMismatchError(f"找不到可修改分区: {dataset}/{label}")
            transaction.backup_partition(dataset, label)
            rows = [
                row for path in partition.files for row in pq.ParquetFile(path).read().to_pylist()
            ]
            applied: list[tuple[Issue, Mapping[str, object], str]] = []
            for issue, decision in changes:
                matches = [row for row in rows if _matches(row, issue.key)]
                if len(matches) != 1:
                    raise PatchMismatchError(f"{issue.issue_id} 匹配到 {len(matches)} 行")
                row = matches[0]
                delete = False
                if issue.fix_mode == "AUTO_FIX":
                    suggested = issue.suggested or {}
                    delete = suggested.get("delete") is True
                    values = suggested.get("values")
                    if not delete and not isinstance(values, Mapping):
                        raise PatchMismatchError(f"{issue.issue_id} 没有确定修正值")
                else:
                    assert decision is not None
                    expected = decision.expected or {}
                    if any(not _equal(row.get(name), value) for name, value in expected.items()):
                        raise PatchMismatchError(f"{issue.issue_id} 原值已变化")
                    values = decision.values or {}
                if delete:
                    rows.remove(row)
                    applied.append((issue, {"delete": True}, issue.message))
                    continue
                assert isinstance(values, Mapping)
                unknown = set(values) - set(TABLE_SCHEMAS[dataset].names)
                if unknown:
                    raise PatchMismatchError(f"{issue.issue_id} 包含未知字段 {sorted(unknown)}")
                row.update(values)
                reason = issue.message if decision is None else decision.reason
                applied.append((issue, {"values": dict(values)}, reason))
            cleaned = pa.Table.from_pylist(rows, schema=TABLE_SCHEMAS[dataset])
            store._replace_partition(dataset, partition.value, cleaned)
            for issue, after, reason in applied:
                transaction.record_patch(issue, after=after, reason=reason)
    return True


def _reset_broken_partitions(
    root: Path,
    report: DetectionReport,
    dataset: str,
    start: date,
    end: date,
) -> None:
    for issue in report.issues:
        if issue.dataset != dataset or issue.rule_id not in _RESET_BEFORE_REFETCH:
            continue
        instruction = _REPAIRERS[issue.rule_id](issue)
        if instruction.action != "REFETCH" or issue.partition is None:
            continue
        issue_start = date.fromisoformat(str(instruction.details["start_date"]))
        if not start <= issue_start <= end:
            continue
        target = (root / dataset / issue.partition).resolve()
        table_root = (root / dataset).resolve()
        if target == table_root or table_root not in target.parents:
            raise ValueError(f"非法分区路径: {dataset}/{issue.partition}")
        if target.is_dir():
            shutil.rmtree(target)


def _partition_path(root: Path, dataset: str, value: date) -> Path:
    field = TABLE_PARTITION_BY[dataset]
    label = f"{quote(field, safe='')}={quote(f'value:{value.isoformat()}', safe='')}"
    return root / dataset / label


def _partition_directories(root: Path, dataset: str) -> list[Path]:
    return sorted({manifest.parent for manifest in (root / dataset).rglob("_manifest.json")})


def _matches(row: Mapping[str, object], key: Mapping[str, object]) -> bool:
    return all(_equal(row.get(name), value) for name, value in key.items())


def _equal(actual: object, expected: object) -> bool:
    return actual.isoformat() == expected if isinstance(actual, date) else actual == expected


def _link_or_copy(source: str | Path, destination: str | Path) -> str:
    source_path = Path(source)
    destination_path = Path(destination)
    try:
        os.link(source_path, destination_path)
    except OSError:
        shutil.copy2(source_path, destination_path)
    return str(destination_path)


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2, sort_keys=True)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def repair_dataset_empty(issue: Issue) -> RepairInstruction:
    """空数据集没有可推导的重拉范围，交给人工。"""
    return _manual(issue, "数据集为空，需人工确认采集范围后补数")


def repair_manifest(issue: Issue) -> RepairInstruction:
    """日期可定位时丢弃损坏分区并重拉，否则交给人工。"""
    return _structural_refetch_or_manual(issue, "备份并重建 Manifest 损坏的分区")


def repair_missing_manifest_file(issue: Issue) -> RepairInstruction:
    """日期可定位时丢弃不完整分区并重拉。"""
    return _structural_refetch_or_manual(issue, "备份并重建文件缺失的分区")


def repair_unreadable_parquet(issue: Issue) -> RepairInstruction:
    """日期可定位时丢弃不可读分区并重拉。"""
    return _structural_refetch_or_manual(issue, "备份并重建 Parquet 损坏的分区")


def repair_schema(issue: Issue) -> RepairInstruction:
    """不猜字段转换，直接备份并重拉可定位的分区。"""
    return _structural_refetch_or_manual(issue, "备份并重建 Schema 不一致的分区")


def repair_partition_value(issue: Issue) -> RepairInstruction:
    """不移动可疑记录，直接备份并重拉路径日期对应分区。"""
    return _structural_refetch_or_manual(issue, "备份并重建分区值冲突的分区")


def repair_duplicate_key(issue: Issue) -> RepairInstruction:
    """主键重复时按问题建议定向重拉。"""
    return _refetch_or_manual(issue, "重拉该数据集与日期，由采集层按主键去重")


def repair_required_value(issue: Issue) -> RepairInstruction:
    """必填字段缺失时定向重拉。"""
    return _refetch_or_manual(issue, "重拉该数据集与日期")


def repair_finite_float(issue: Issue) -> RepairInstruction:
    """非关键浮点转为 null；关键浮点定向重拉。"""
    if issue.fix_mode == "AUTO_FIX":
        return _patch(issue, "原位将非有限的可空浮点值写为 null")
    return _refetch_or_manual(issue, "关键浮点字段定向重拉")


def repair_missing_market_partition(issue: Issue) -> RepairInstruction:
    """交易日分区缺失时定向重拉该日。"""
    return _refetch_or_manual(issue, "重拉缺失的交易日分区")


def repair_daily_missing(issue: Issue) -> RepairInstruction:
    """daily 关键字段缺失时定向重拉。"""
    return _refetch_or_manual(issue, "重拉 daily 对应交易日")


def repair_daily_range(issue: Issue) -> RepairInstruction:
    """daily 数值越界时定向重拉。"""
    return _refetch_or_manual(issue, "重拉 daily 对应交易日")


def repair_daily_ohlc(issue: Issue) -> RepairInstruction:
    """daily OHLC 关系错误时定向重拉。"""
    return _refetch_or_manual(issue, "重拉 daily 对应交易日")


def repair_adj_factor(issue: Issue) -> RepairInstruction:
    """复权因子非正时定向重拉。"""
    return _refetch_or_manual(issue, "重拉 adj_factor 对应交易日")


def repair_stk_limit_partition(issue: Issue) -> RepairInstruction:
    """涨跌停整个分区的关键值为空时定向重拉。"""
    return _refetch_or_manual(issue, "重拉 stk_limit 对应交易日")


def repair_stk_limit_missing(issue: Issue) -> RepairInstruction:
    """涨跌停行的关键值缺失时定向重拉。"""
    return _refetch_or_manual(issue, "重拉 stk_limit 对应交易日")


def repair_stk_limit_order(issue: Issue) -> RepairInstruction:
    """涨跌停顺序错误时定向重拉。"""
    return _refetch_or_manual(issue, "重拉 stk_limit 对应交易日")


def repair_trade_calendar_value(issue: Issue) -> RepairInstruction:
    """交易日历值非法时定向重拉。"""
    return _refetch_or_manual(issue, "重拉 trade_cal 对应日期")


def repair_trade_calendar_coverage(issue: Issue) -> RepairInstruction:
    """交易所覆盖不全时定向重拉。"""
    return _refetch_or_manual(issue, "重拉 trade_cal 对应日期")


def repair_business_value(issue: Issue) -> RepairInstruction:
    """业务值或跨表关系异常时，优先按检测结果定位日期重拉。"""
    if issue.fix_mode == "AUTO_FIX":
        return _patch(issue, "原位应用检测器给出的确定性修正")
    if issue.severity == "WARNING":
        return _manual(issue, f"{_BUSINESS_REPAIR_MESSAGES[issue.rule_id]}；告警不自动改值")
    return _refetch_or_manual(issue, _BUSINESS_REPAIR_MESSAGES[issue.rule_id])


def repair_unknown_issue(issue: Issue) -> RepairInstruction:
    """未登记规则不自动处理。"""
    return _manual(issue, f"规则 {issue.rule_id} 没有登记自动修复方式")


def _patch(issue: Issue, message: str) -> RepairInstruction:
    suggested = issue.suggested or {}
    values = suggested.get("values")
    delete = suggested.get("delete") is True
    if not delete and (not isinstance(values, Mapping) or not values):
        return _manual(issue, "检测结果没有提供确定修正值")
    if delete:
        details: dict[str, object] = {"delete": True}
    else:
        assert isinstance(values, Mapping)
        details = {"values": dict(values)}
    return RepairInstruction(
        issue_id=issue.issue_id,
        dataset=issue.dataset,
        rule_id=issue.rule_id,
        action="PATCH",
        details=details,
        message=message,
    )


def _refetch_or_manual(issue: Issue, message: str) -> RepairInstruction:
    suggested = issue.suggested or {}
    if suggested.get("action") != "REFETCH":
        return _manual(issue, f"{message}，但问题中没有可定位日期")
    try:
        start = date.fromisoformat(str(suggested["start_date"]))
        end = date.fromisoformat(str(suggested["end_date"]))
    except (KeyError, ValueError):
        return _manual(issue, f"{message}，但重拉日期无效")
    return RepairInstruction(
        issue_id=issue.issue_id,
        dataset=issue.dataset,
        rule_id=issue.rule_id,
        action="REFETCH",
        details={"start_date": start.isoformat(), "end_date": end.isoformat()},
        message=message,
    )


def _structural_refetch_or_manual(issue: Issue, message: str) -> RepairInstruction:
    if issue.dataset not in _PARTITION_DATE_REFETCH:
        return _manual(issue, f"{message}，但分区日期不是接口查询日期")
    return _refetch_or_manual(issue, message)


def _manual(issue: Issue, message: str) -> RepairInstruction:
    return RepairInstruction(
        issue_id=issue.issue_id,
        dataset=issue.dataset,
        rule_id=issue.rule_id,
        action="MANUAL",
        details={},
        message=message,
    )


_REPAIRERS: dict[str, IssueRepairer] = {
    "dataset_empty_v1": repair_dataset_empty,
    "manifest_v1": repair_manifest,
    "manifest_file_missing_v1": repair_missing_manifest_file,
    "parquet_read_v1": repair_unreadable_parquet,
    "schema_v1": repair_schema,
    "partition_value_v1": repair_partition_value,
    "duplicate_key_v1": repair_duplicate_key,
    "required_value_v1": repair_required_value,
    "finite_float_v1": repair_finite_float,
    "missing_market_partition_v1": repair_missing_market_partition,
    "daily_missing_v1": repair_daily_missing,
    "daily_range_v1": repair_daily_range,
    "daily_ohlc_v1": repair_daily_ohlc,
    "adj_factor_positive_v1": repair_adj_factor,
    "stk_limit_partition_missing_v1": repair_stk_limit_partition,
    "stk_limit_missing_v1": repair_stk_limit_missing,
    "stk_limit_order_v1": repair_stk_limit_order,
    "trade_calendar_value_v1": repair_trade_calendar_value,
    "calendar_exchange_coverage_v1": repair_trade_calendar_coverage,
}

_BUSINESS_REPAIR_MESSAGES = {
    "closed_market_partition_v1": "重拉该日期；若上游仍返回数据，人工核对交易日历",
    "stock_basic_identity_v1": "重拉 stock_basic 对应上市日期",
    "stock_basic_lifecycle_v1": "重拉 stock_basic 对应上市日期",
    "daily_arithmetic_v1": "重拉 daily 对应交易日",
    "daily_volume_amount_v1": "重拉 daily 对应交易日",
    "daily_basic_range_v1": "重拉 daily_basic 对应交易日",
    "adj_factor_daily_coverage_v1": "重拉 adj_factor 对应交易日",
    "suspend_value_v1": "重拉 suspend_d 对应交易日",
    "stock_st_value_v1": "重拉 stock_st 对应交易日",
    "moneyflow_range_v1": "重拉 moneyflow 对应交易日",
    "dividend_value_v1": "重拉 dividend 对应公告日期",
    "dividend_stock_ratio_v1": "重拉 dividend 对应公告日期",
    "forecast_value_v1": "重拉 forecast 对应公告日期",
    "forecast_range_v1": "重拉 forecast 对应公告日期",
    "express_value_v1": "重拉 express 对应公告日期",
    "fina_audit_value_v1": "重拉 fina_audit 对应公告日期后复核审计信息",
    "income_value_v1": "重拉 income 对应公告日期",
    "balancesheet_value_v1": "重拉 balancesheet 对应公告日期",
    "cashflow_value_v1": "重拉 cashflow 对应公告日期",
    "fina_indicator_value_v1": "重拉 fina_indicator 对应公告日期",
    "sw_industry_value_v1": "重拉 sw_industry 对应纳入日期后复核行业历史",
    "calendar_date_coverage_v1": "重拉 trade_cal 缺失自然日",
    "calendar_pretrade_v1": "重拉 trade_cal 对应日期后复核上一交易日",
}

for _rule_id in _BUSINESS_REPAIR_MESSAGES:
    _REPAIRERS[_rule_id] = repair_business_value
