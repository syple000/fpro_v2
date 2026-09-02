"""数据清洗的稳定问题、决策和检测报告模型。"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

FixMode = Literal["AUTO_FIX", "MANUAL"]
DecisionAction = Literal["PATCH", "REFETCH", "ACCEPT"]
IssueSeverity = Literal["ERROR", "WARNING"]
CheckStatus = Literal["PASS", "WARN", "FAIL"]


@dataclass(frozen=True, slots=True)
class Issue:
    issue_id: str
    dataset: str
    partition: str | None
    key: Mapping[str, object]
    rule_id: str
    severity: IssueSeverity
    fix_mode: FixMode
    observed: Mapping[str, object]
    suggested: Mapping[str, object] | None
    message: str

    @classmethod
    def create(
        cls,
        *,
        dataset: str,
        partition: str | None,
        key: Mapping[str, object],
        rule_id: str,
        severity: IssueSeverity = "ERROR",
        fix_mode: FixMode,
        observed: Mapping[str, object],
        suggested: Mapping[str, object] | None,
        message: str,
    ) -> Issue:
        identity = {
            "dataset": dataset,
            "key": _json_value(key),
            "observed": _json_value(observed),
            "rule_id": rule_id,
        }
        digest = sha256(_json_text(identity).encode()).hexdigest()[:20]
        return cls(
            issue_id=f"{dataset}:{rule_id}:{digest}",
            dataset=dataset,
            partition=partition,
            key=dict(_json_value(key)),
            rule_id=rule_id,
            severity=severity,
            fix_mode=fix_mode,
            observed=dict(_json_value(observed)),
            suggested=None if suggested is None else dict(_json_value(suggested)),
            message=message,
        )


@dataclass(frozen=True, slots=True)
class Decision:
    issue_id: str
    action: DecisionAction
    expected: Mapping[str, object] | None
    values: Mapping[str, object] | None
    reason: str


@dataclass(frozen=True, slots=True)
class CheckResult:
    """一个明确检查项在一个数据集上的执行结果。"""

    dataset: str
    check_id: str
    description: str
    status: CheckStatus
    issue_count: int


@dataclass(frozen=True, slots=True)
class DetectionReport:
    input_fingerprint: str
    through: date
    start: date | None
    datasets: tuple[str, ...]
    row_counts: Mapping[str, int]
    checks: tuple[CheckResult, ...]
    issues: tuple[Issue, ...]

    @property
    def passed(self) -> bool:
        return not any(
            issue.severity == "ERROR" and issue.fix_mode == "MANUAL" for issue in self.issues
        )


def write_report(report: DetectionReport, path: str | Path) -> None:
    destination = Path(path)
    lines = [
        _json_text(
            {
                "kind": "report",
                "version": 3,
                "input_fingerprint": report.input_fingerprint,
                "through": report.through,
                "start": report.start,
                "datasets": report.datasets,
                "row_counts": report.row_counts,
            }
        )
    ]
    lines.extend(_json_text({"kind": "check", **asdict(check)}) for check in report.checks)
    lines.extend(_json_text({"kind": "issue", **asdict(issue)}) for issue in report.issues)
    _atomic_write(destination, "\n".join(lines) + "\n")


def read_report(path: str | Path) -> DetectionReport:
    source = Path(path)
    records = [json.loads(line) for line in source.read_text(encoding="utf-8").splitlines() if line]
    if (
        not records
        or records[0].get("kind") != "report"
        or records[0].get("version") not in {1, 2, 3}
    ):
        raise ValueError(f"问题报告格式无效: {source}")
    header = records[0]
    checks = tuple(_check_from_json(item) for item in records[1:] if item.get("kind") == "check")
    issues = tuple(_issue_from_json(item) for item in records[1:] if item.get("kind") == "issue")
    try:
        return DetectionReport(
            input_fingerprint=str(header["input_fingerprint"]),
            through=date.fromisoformat(header["through"]),
            start=(date.fromisoformat(header["start"]) if header.get("start") else None),
            datasets=tuple(header["datasets"]),
            row_counts={str(key): int(value) for key, value in header["row_counts"].items()},
            checks=checks,
            issues=issues,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"问题报告格式无效: {source}") from exc


def read_decisions(path: str | Path | None) -> dict[str, Decision]:
    if path is None or not Path(path).exists():
        return {}
    decisions: dict[str, Decision] = {}
    for line_number, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not line:
            continue
        try:
            payload = json.loads(line)
            decision = Decision(
                issue_id=payload["issue_id"],
                action=payload["action"],
                expected=payload.get("expected"),
                values=payload.get("values"),
                reason=payload["reason"],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"人工决策第 {line_number} 行格式无效") from exc
        if decision.action not in {"PATCH", "REFETCH", "ACCEPT"}:
            raise ValueError(f"人工决策第 {line_number} 行 action 无效")
        if not decision.reason.strip():
            raise ValueError(f"人工决策第 {line_number} 行 reason 不能为空")
        if decision.action == "PATCH" and (not decision.expected or not decision.values):
            raise ValueError("PATCH 必须同时提供 expected 和 values")
        if decision.issue_id in decisions:
            raise ValueError(f"人工决策包含重复 issue_id: {decision.issue_id}")
        decisions[decision.issue_id] = decision
    return decisions


def _issue_from_json(payload: Mapping[str, Any]) -> Issue:
    if payload.get("kind") != "issue":
        raise ValueError("问题报告包含未知记录")
    return Issue(
        issue_id=str(payload["issue_id"]),
        dataset=str(payload["dataset"]),
        partition=payload.get("partition"),
        key=dict(payload["key"]),
        rule_id=str(payload["rule_id"]),
        severity=payload.get("severity", "ERROR"),
        fix_mode=payload["fix_mode"],
        observed=dict(payload["observed"]),
        suggested=None if payload.get("suggested") is None else dict(payload["suggested"]),
        message=str(payload["message"]),
    )


def _check_from_json(payload: Mapping[str, Any]) -> CheckResult:
    try:
        status = payload["status"]
        if status not in {"PASS", "WARN", "FAIL"}:
            raise ValueError("status 无效")
        return CheckResult(
            dataset=str(payload["dataset"]),
            check_id=str(payload["check_id"]),
            description=str(payload["description"]),
            status=status,
            issue_count=int(payload["issue_count"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("问题报告包含无效检查结果") from exc


def _json_text(value: object) -> str:
    return json.dumps(
        _json_value(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_value(value: object) -> Any:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            return "NaN"
        return "Infinity" if value > 0 else "-Infinity"
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as file:
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
