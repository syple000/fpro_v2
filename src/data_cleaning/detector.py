"""对 Tushare Manifest 当前引用的数据执行简单、确定的检查。"""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from urllib.parse import unquote

import pyarrow as pa
import pyarrow.parquet as pq

from data_cleaning.models import DetectionReport, Issue
from tushare_data.schemas import TABLE_PARTITION_BY, TABLE_PRIMARY_KEY, TABLE_SCHEMAS

_DENSE_MARKET_DATASETS = frozenset(
    {"daily", "daily_basic", "adj_factor", "stk_limit", "stock_st", "moneyflow"}
)
_MARKET_DATE_DATASETS = _DENSE_MARKET_DATASETS | {"suspend_d", "trade_cal"}
_ANNOUNCEMENT_DATASETS = frozenset(
    {"forecast", "express", "fina_audit", "fina_indicator", "income", "balancesheet", "cashflow"}
)
_SNAPSHOT_DATASETS = frozenset({"stock_basic", "sw_industry"})
_EXCHANGES = frozenset({"SSE", "SZSE", "BSE"})
_CRITICAL_FLOAT_FIELDS = {
    "daily": frozenset({"open", "high", "low", "close", "pre_close", "vol", "amount"}),
    "adj_factor": frozenset({"adj_factor"}),
    "stk_limit": frozenset({"pre_close", "up_limit", "down_limit"}),
}


@dataclass(frozen=True, slots=True)
class _Partition:
    manifest: Path
    label: str
    value: date | None
    files: tuple[Path, ...]


def detect(
    root: str | Path,
    *,
    through: date,
    datasets: Iterable[str] | None = None,
    start: date | None = None,
) -> DetectionReport:
    """检查指定数据集，并返回可重复的问题报告。"""
    source = Path(root).expanduser().resolve()
    selected = _datasets(datasets)
    if start is not None and start > through:
        raise ValueError("start 不能晚于 through")

    issues: list[Issue] = []
    row_counts: dict[str, int] = {}
    observed_dates: dict[str, set[date]] = {}
    for dataset in selected:
        dataset_issues, row_count, dates = _detect_dataset(
            source,
            dataset,
            through=through,
            start=start,
        )
        issues.extend(dataset_issues)
        row_counts[dataset] = row_count
        observed_dates[dataset] = dates
        if start is None and row_count == 0 and not dataset_issues:
            issues.append(
                Issue.create(
                    dataset=dataset,
                    partition=None,
                    key={"dataset": dataset},
                    rule_id="dataset_empty_v1",
                    fix_mode="MANUAL",
                    observed={"row_count": 0},
                    suggested=None,
                    message=f"{dataset} 在发布范围内没有数据",
                )
            )

    open_dates = _calendar_open_dates(source, through=through, start=start)
    for dataset in selected:
        if dataset not in _DENSE_MARKET_DATASETS or not open_dates:
            continue
        actual = observed_dates[dataset]
        if actual:
            lower = start or min(actual)
            upper = min(through, max(actual))
            expected = {day for day in open_dates if lower <= day <= upper}
        elif start is not None:
            expected = {day for day in open_dates if start <= day <= through}
        else:
            expected = set()
        for missing in sorted(expected - actual):
            issues.append(
                Issue.create(
                    dataset=dataset,
                    partition=missing.isoformat(),
                    key={TABLE_PARTITION_BY[dataset]: missing},
                    rule_id="missing_market_partition_v1",
                    fix_mode="MANUAL",
                    observed={"partition": "missing"},
                    suggested=_refetch(missing),
                    message=f"{dataset} 缺少交易日 {missing.isoformat()} 分区",
                )
            )

    issues.sort(key=_issue_sort_key)
    return DetectionReport(
        input_fingerprint=source_fingerprint(source, selected),
        through=through,
        start=start,
        datasets=selected,
        row_counts=dict(sorted(row_counts.items())),
        issues=tuple(issues),
    )


def source_fingerprint(root: str | Path, datasets: Iterable[str]) -> str:
    """对 Manifest 内容、活跃文件名和大小生成快速输入指纹。"""
    source = Path(root).expanduser().resolve()
    digest = sha256()
    for dataset in sorted(datasets):
        table_root = source / dataset
        for manifest in sorted(table_root.rglob("_manifest.json")):
            relative = manifest.relative_to(source).as_posix()
            digest.update(relative.encode())
            try:
                content = manifest.read_bytes()
            except OSError as exc:
                digest.update(f"ERROR:{exc.__class__.__name__}".encode())
                continue
            digest.update(content)
            try:
                payload = json.loads(content)
                names = payload.get("files", []) if isinstance(payload, dict) else []
            except json.JSONDecodeError:
                names = []
            if isinstance(names, list):
                for name in names:
                    if not isinstance(name, str):
                        continue
                    path = manifest.parent / name
                    digest.update(name.encode())
                    try:
                        digest.update(str(path.stat().st_size).encode())
                    except OSError:
                        digest.update(b"MISSING")
    return digest.hexdigest()


def active_partitions(root: str | Path, dataset: str) -> tuple[_Partition, ...]:
    """返回可正常解析的活跃分区；格式问题由 ``detect`` 转换为 Issue。"""
    table_root = Path(root).expanduser().resolve() / dataset
    partitions: list[_Partition] = []
    for manifest in sorted(table_root.rglob("_manifest.json")):
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        names = payload["files"]
        partitions.append(
            _Partition(
                manifest=manifest,
                label=_partition_label(table_root, manifest),
                value=_partition_date(table_root, manifest),
                files=tuple(manifest.parent / name for name in names),
            )
        )
    return tuple(partitions)


def _detect_dataset(
    root: Path,
    dataset: str,
    *,
    through: date,
    start: date | None,
) -> tuple[list[Issue], int, set[date]]:
    schema = TABLE_SCHEMAS[dataset]
    table_root = root / dataset
    issues: list[Issue] = []
    row_count = 0
    observed_dates: set[date] = set()
    for manifest in sorted(table_root.rglob("_manifest.json")):
        label = _partition_label(table_root, manifest)
        partition_date = _partition_date(table_root, manifest)
        if partition_date is not None and (
            partition_date > through or (start is not None and partition_date < start)
        ):
            continue
        try:
            partition = _load_partition(manifest, table_root)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            issues.append(
                Issue.create(
                    dataset=dataset,
                    partition=label,
                    key={"partition": label},
                    rule_id="manifest_v1",
                    fix_mode="MANUAL",
                    observed={"error": str(exc)},
                    suggested=None,
                    message=f"{dataset} 分区 Manifest 无法读取",
                )
            )
            continue
        missing_files = [path for path in partition.files if not path.is_file()]
        if missing_files:
            issues.append(
                Issue.create(
                    dataset=dataset,
                    partition=label,
                    key={"partition": label},
                    rule_id="manifest_file_missing_v1",
                    fix_mode="MANUAL",
                    observed={"files": [path.name for path in missing_files]},
                    suggested=None,
                    message=f"{dataset} 分区 Manifest 引用的文件不存在",
                )
            )
            continue
        tables: list[pa.Table] = []
        schema_invalid = False
        for path in partition.files:
            try:
                file_schema = pq.ParquetFile(path).schema_arrow
            except (OSError, pa.ArrowException) as exc:
                issues.append(
                    Issue.create(
                        dataset=dataset,
                        partition=label,
                        key={"partition": label, "file": path.name},
                        rule_id="parquet_read_v1",
                        fix_mode="MANUAL",
                        observed={"error": str(exc)},
                        suggested=None,
                        message=f"{dataset} Parquet 文件无法读取",
                    )
                )
                schema_invalid = True
                continue
            if not file_schema.equals(schema, check_metadata=True):
                issues.append(
                    Issue.create(
                        dataset=dataset,
                        partition=label,
                        key={"partition": label, "file": path.name},
                        rule_id="schema_v1",
                        fix_mode="MANUAL",
                        observed={"schema": str(file_schema)},
                        suggested=None,
                        message=f"{dataset} Parquet Schema 与登记 Schema 不一致",
                    )
                )
                schema_invalid = True
                continue
            tables.append(pq.ParquetFile(path).read())
        if schema_invalid or not tables:
            continue
        table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        row_count += table.num_rows
        if partition.value is not None:
            observed_dates.add(partition.value)
        issues.extend(_check_table(dataset, label, partition.value, table))
    return issues, row_count, observed_dates


def _load_partition(manifest: Path, table_root: Path) -> _Partition:
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Manifest 必须是 JSON 对象")
    names = payload.get("files")
    if (
        not isinstance(names, list)
        or len(names) != len(set(names))
        or any(
            not isinstance(name, str) or Path(name).name != name or not name.endswith(".parquet")
            for name in names
        )
    ):
        raise ValueError("Manifest files 格式无效")
    return _Partition(
        manifest=manifest,
        label=_partition_label(table_root, manifest),
        value=_partition_date(table_root, manifest),
        files=tuple(manifest.parent / name for name in names),
    )


def _check_table(
    dataset: str,
    partition: str,
    partition_date: date | None,
    table: pa.Table,
) -> list[Issue]:
    schema = TABLE_SCHEMAS[dataset]
    partition_field = TABLE_PARTITION_BY[dataset]
    primary_key = (partition_field, *TABLE_PRIMARY_KEY[dataset])
    rows = table.to_pylist()
    issues: list[Issue] = []
    skip_stk_limit_rows = False

    partition_values = {row[partition_field] for row in rows}
    if None in partition_values or (
        partition_date is not None and partition_values != {partition_date}
    ):
        issues.append(
            Issue.create(
                dataset=dataset,
                partition=partition,
                key={"partition": partition},
                rule_id="partition_value_v1",
                fix_mode="MANUAL",
                observed={partition_field: sorted(str(value) for value in partition_values)},
                suggested=None,
                message=f"{dataset} 分区路径与记录 {partition_field} 不一致",
            )
        )

    counts = Counter(tuple(row[name] for name in primary_key) for row in rows)
    for values, count in sorted(counts.items(), key=lambda item: str(item[0])):
        if count <= 1:
            continue
        key = dict(zip(primary_key, values, strict=True))
        issues.append(
            Issue.create(
                dataset=dataset,
                partition=partition,
                key=key,
                rule_id="duplicate_key_v1",
                fix_mode="MANUAL",
                observed={"count": count},
                suggested=_suggested_refetch(dataset, key, partition_date),
                message=f"{dataset} 业务主键重复",
            )
        )

    if dataset == "stk_limit" and table.num_rows:
        all_null = [
            name
            for name in ("pre_close", "up_limit", "down_limit")
            if table.column(name).null_count == table.num_rows
        ]
        if all_null:
            skip_stk_limit_rows = True
            issues.append(
                Issue.create(
                    dataset=dataset,
                    partition=partition,
                    key={partition_field: partition_date},
                    rule_id="stk_limit_partition_missing_v1",
                    fix_mode="MANUAL",
                    observed={"all_null_fields": all_null, "row_count": table.num_rows},
                    suggested=_refetch(partition_date) if partition_date else None,
                    message=f"stk_limit 整个分区的关键字段为空: {all_null}",
                )
            )

    required = [field.name for field in schema if not field.nullable]
    for row in rows:
        key = _row_key(dataset, row)
        missing = [name for name in required if row[name] is None]
        if missing:
            issues.append(
                Issue.create(
                    dataset=dataset,
                    partition=partition,
                    key=key,
                    rule_id="required_value_v1",
                    fix_mode="MANUAL",
                    observed={name: None for name in missing},
                    suggested=_suggested_refetch(dataset, row, partition_date),
                    message=f"{dataset} 必填字段为空: {missing}",
                )
            )
        for field in schema:
            value = row[field.name]
            if (
                pa.types.is_floating(field.type)
                and isinstance(value, float)
                and not math.isfinite(value)
            ):
                critical = field.name in _CRITICAL_FLOAT_FIELDS.get(dataset, frozenset())
                issues.append(
                    Issue.create(
                        dataset=dataset,
                        partition=partition,
                        key=key,
                        rule_id="finite_float_v1",
                        fix_mode="MANUAL" if critical else "AUTO_FIX",
                        observed={field.name: value},
                        suggested=(
                            _suggested_refetch(dataset, row, partition_date)
                            if critical
                            else {"values": {field.name: None}}
                        ),
                        message=f"{dataset}.{field.name} 包含非有限浮点数",
                    )
                )
        if not (dataset == "stk_limit" and skip_stk_limit_rows):
            issues.extend(_business_rules(dataset, partition, partition_date, row, key))

    if dataset == "trade_cal" and partition_date is not None:
        exchanges = {row["exchange"] for row in rows}
        missing = sorted(_EXCHANGES - exchanges)
        if missing:
            issues.append(
                Issue.create(
                    dataset=dataset,
                    partition=partition,
                    key={"cal_date": partition_date},
                    rule_id="calendar_exchange_coverage_v1",
                    fix_mode="MANUAL",
                    observed={"missing_exchanges": missing},
                    suggested=_refetch(partition_date),
                    message=f"交易日历缺少交易所: {missing}",
                )
            )
    return issues


def _business_rules(
    dataset: str,
    partition: str,
    partition_date: date | None,
    row: Mapping[str, object],
    key: Mapping[str, object],
) -> list[Issue]:
    issues: list[Issue] = []
    refetch = _suggested_refetch(dataset, row, partition_date)
    if dataset == "daily":
        required = ("open", "high", "low", "close", "pre_close", "vol", "amount")
        missing = [name for name in required if row[name] is None]
        if missing:
            issues.append(
                _manual(dataset, partition, key, "daily_missing_v1", row, missing, refetch)
            )
        elif all(_finite_number(row[name]) for name in required):
            open_, high, low, close, pre_close, volume, amount = (
                _number(row[name]) for name in required
            )
            if min(open_, high, low, close, pre_close) <= 0 or volume < 0 or amount < 0:
                issues.append(
                    _manual(dataset, partition, key, "daily_range_v1", row, required, refetch)
                )
            elif high < max(open_, low, close) or low > min(open_, high, close):
                issues.append(
                    _manual(dataset, partition, key, "daily_ohlc_v1", row, required[:4], refetch)
                )
            change = row["change"]
            pct_chg = row["pct_chg"]
            if _finite_number(change) and _finite_number(pct_chg):
                from_change = round(pre_close + _number(change), 2)
                from_pct = pre_close * (1 + _number(pct_chg) / 100)
                if (
                    math.isclose(from_change, from_pct, abs_tol=0.0051)
                    and not math.isclose(close, from_change, abs_tol=0.0051)
                    and low <= from_change <= high
                ):
                    issues.append(
                        Issue.create(
                            dataset=dataset,
                            partition=partition,
                            key=key,
                            rule_id="daily_close_consistency_v1",
                            fix_mode="AUTO_FIX",
                            observed={
                                "close": close,
                                "pre_close": pre_close,
                                "change": change,
                                "pct_chg": pct_chg,
                            },
                            suggested={"values": {"close": from_change}},
                            message="close 与 change/pct_chg 不一致，两个独立字段指向同一修正值",
                        )
                    )
    elif dataset == "adj_factor":
        value = row["adj_factor"]
        if value is None or (_finite_number(value) and _number(value) <= 0):
            issues.append(
                _manual(
                    dataset, partition, key, "adj_factor_positive_v1", row, ("adj_factor",), refetch
                )
            )
    elif dataset == "stk_limit":
        fields = ("pre_close", "up_limit", "down_limit")
        if any(row[name] is None for name in fields):
            issues.append(
                _manual(dataset, partition, key, "stk_limit_missing_v1", row, fields, refetch)
            )
        elif all(_finite_number(row[name]) for name in fields):
            pre_close, up_limit, down_limit = (_number(row[name]) for name in fields)
            if down_limit <= 0 or not down_limit <= pre_close <= up_limit:
                issues.append(
                    _manual(dataset, partition, key, "stk_limit_order_v1", row, fields, refetch)
                )
    elif dataset == "trade_cal":
        if row["exchange"] not in _EXCHANGES or row["is_open"] not in {0, 1}:
            issues.append(
                _manual(
                    dataset,
                    partition,
                    key,
                    "trade_calendar_value_v1",
                    row,
                    ("exchange", "is_open"),
                    refetch,
                )
            )
    return issues


def _manual(
    dataset: str,
    partition: str,
    key: Mapping[str, object],
    rule_id: str,
    row: Mapping[str, object],
    fields: Iterable[str],
    suggested: Mapping[str, object] | None,
) -> Issue:
    names = tuple(fields)
    return Issue.create(
        dataset=dataset,
        partition=partition,
        key=key,
        rule_id=rule_id,
        fix_mode="MANUAL",
        observed={name: row.get(name) for name in names},
        suggested=suggested,
        message=f"{dataset} 未通过 {rule_id} 检查",
    )


def _row_key(dataset: str, row: Mapping[str, object]) -> dict[str, object]:
    names = (TABLE_PARTITION_BY[dataset], *TABLE_PRIMARY_KEY[dataset])
    return {name: row[name] for name in names}


def _suggested_refetch(
    dataset: str,
    row: Mapping[str, object],
    partition_date: date | None,
) -> Mapping[str, object] | None:
    target: date | None = None
    if dataset in _MARKET_DATE_DATASETS or dataset in _SNAPSHOT_DATASETS:
        target = partition_date
    elif dataset in _ANNOUNCEMENT_DATASETS:
        value = row.get("ann_date") or row.get("f_ann_date")
        target = value if isinstance(value, date) else None
    elif dataset == "dividend":
        value = row.get("imp_ann_date") or row.get("ann_date")
        target = value if isinstance(value, date) else None
    if target is None:
        return None
    return _refetch(target)


def _refetch(day: date) -> dict[str, object]:
    return {"action": "REFETCH", "start_date": day, "end_date": day}


def _calendar_open_dates(root: Path, *, through: date, start: date | None) -> set[date]:
    dates: set[date] = set()
    try:
        partitions = active_partitions(root, "trade_cal")
    except (OSError, ValueError, json.JSONDecodeError):
        return dates
    for partition in partitions:
        if partition.value is not None and (
            partition.value > through or (start is not None and partition.value < start)
        ):
            continue
        if any(not path.is_file() for path in partition.files):
            continue
        try:
            rows = [
                row for path in partition.files for row in pq.ParquetFile(path).read().to_pylist()
            ]
        except (OSError, pa.ArrowException):
            continue
        if any(row["is_open"] == 1 for row in rows):
            value = partition.value or next(
                (row["cal_date"] for row in rows if isinstance(row["cal_date"], date)), None
            )
            if value is not None:
                dates.add(value)
    return dates


def _partition_label(table_root: Path, manifest: Path) -> str:
    return manifest.parent.relative_to(table_root).as_posix()


def _partition_date(table_root: Path, manifest: Path) -> date | None:
    label = _partition_label(table_root, manifest)
    component = label.rsplit("/", 1)[-1]
    if "=" not in component:
        return None
    raw = unquote(component.split("=", 1)[1])
    if not raw.startswith("value:"):
        return None
    try:
        return date.fromisoformat(raw.removeprefix("value:"))
    except ValueError:
        return None


def _finite_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _number(value: object) -> float:
    if not _finite_number(value):
        raise TypeError(f"{value!r} 不是有限数值")
    assert isinstance(value, (int, float)) and not isinstance(value, bool)
    return float(value)


def _datasets(values: Iterable[str] | None) -> tuple[str, ...]:
    if values is None:
        return tuple(TABLE_SCHEMAS)
    selected = tuple(dict.fromkeys(values))
    if not selected:
        raise ValueError("datasets 不能为空")
    unknown = [value for value in selected if value not in TABLE_SCHEMAS]
    if unknown:
        raise ValueError(f"未知数据集: {unknown}")
    return selected


def _issue_sort_key(issue: Issue) -> tuple[str, str, str, str]:
    return (
        issue.dataset,
        issue.partition or "",
        json.dumps(issue.key, sort_keys=True),
        issue.rule_id,
    )
