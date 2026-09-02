"""对 Tushare Manifest 当前引用的数据执行简单、确定的检查。"""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from urllib.parse import unquote

import pyarrow as pa
import pyarrow.parquet as pq

from data_cleaning.models import CheckResult, DetectionReport, Issue
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

_COMMON_CHECKS = {
    "dataset_empty_v1": "数据集不为空",
    "manifest_v1": "Manifest 格式正确",
    "manifest_file_missing_v1": "Manifest 引用的文件都存在",
    "parquet_read_v1": "Parquet 文件可读",
    "schema_v1": "Parquet Schema 与登记 Schema 一致",
    "partition_value_v1": "分区路径与记录分区值一致",
    "duplicate_key_v1": "业务主键不重复",
    "required_value_v1": "Schema 必填字段不为空",
    "finite_float_v1": "浮点数不含 NaN 或无穷值",
}

_MISSING_PARTITION_CHECK = {"missing_market_partition_v1": "不缺已知交易日分区"}

_DATASET_CHECKS = {
    "daily": {
        "daily_missing_v1": "行情价格、成交量和成交额完整",
        "daily_range_v1": "价格为正且成交量额非负",
        "daily_ohlc_v1": "开高低收关系正确",
        "daily_close_consistency_v1": "收盘价与涨跌额、涨跌幅一致",
    },
    "adj_factor": {"adj_factor_positive_v1": "复权因子为正数"},
    "stk_limit": {
        "stk_limit_partition_missing_v1": "整个分区的关键价格不全为空",
        "stk_limit_missing_v1": "昨收、涨停价和跌停价完整",
        "stk_limit_order_v1": "跌停价 ≤ 昨收价 ≤ 涨停价",
    },
    "trade_cal": {
        "trade_calendar_value_v1": "交易所代码和开市标记合法",
        "calendar_exchange_coverage_v1": "每日覆盖 SSE、SZSE 和 BSE",
    },
}


@dataclass(frozen=True, slots=True)
class _Partition:
    manifest: Path
    label: str
    value: date | None
    files: tuple[Path, ...]


DatasetChecker = Callable[[str, date | None, pa.Table], list[Issue]]


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
        checks=_check_results(selected, issues, full_history=start is None),
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
        partition, manifest_issues = _check_manifest(dataset, manifest, table_root)
        issues.extend(manifest_issues)
        if partition is None:
            continue
        missing_file_issues = _check_manifest_files_exist(dataset, partition)
        issues.extend(missing_file_issues)
        if missing_file_issues:
            continue
        tables, parquet_issues = _check_parquet_files(dataset, partition, schema)
        issues.extend(parquet_issues)
        if parquet_issues or not tables:
            continue
        table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        row_count += table.num_rows
        if partition.value is not None:
            observed_dates.add(partition.value)
        issues.extend(_check_table(dataset, label, partition.value, table))
    return issues, row_count, observed_dates


def _check_manifest(
    dataset: str,
    manifest: Path,
    table_root: Path,
) -> tuple[_Partition | None, list[Issue]]:
    """检查并解析一个 Manifest。"""
    label = _partition_label(table_root, manifest)
    try:
        return _load_partition(manifest, table_root), []
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return None, [
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
        ]


def _check_manifest_files_exist(dataset: str, partition: _Partition) -> list[Issue]:
    """检查 Manifest 引用的文件是否全部存在。"""
    missing_files = [path for path in partition.files if not path.is_file()]
    if not missing_files:
        return []
    return [
        Issue.create(
            dataset=dataset,
            partition=partition.label,
            key={"partition": partition.label},
            rule_id="manifest_file_missing_v1",
            fix_mode="MANUAL",
            observed={"files": [path.name for path in missing_files]},
            suggested=None,
            message=f"{dataset} 分区 Manifest 引用的文件不存在",
        )
    ]


def _check_parquet_files(
    dataset: str,
    partition: _Partition,
    expected_schema: pa.Schema,
) -> tuple[list[pa.Table], list[Issue]]:
    """逐文件检查 Parquet 可读性和 Schema。"""
    tables: list[pa.Table] = []
    issues: list[Issue] = []
    for path in partition.files:
        try:
            parquet = pq.ParquetFile(path)
            table = parquet.read()
        except (OSError, pa.ArrowException) as exc:
            issues.append(
                Issue.create(
                    dataset=dataset,
                    partition=partition.label,
                    key={"partition": partition.label, "file": path.name},
                    rule_id="parquet_read_v1",
                    fix_mode="MANUAL",
                    observed={"error": str(exc)},
                    suggested=None,
                    message=f"{dataset} Parquet 文件无法读取",
                )
            )
            continue
        schema_issue = _check_parquet_schema(dataset, partition, path, parquet, expected_schema)
        if schema_issue is not None:
            issues.append(schema_issue)
            continue
        tables.append(table)
    return tables, issues


def _check_parquet_schema(
    dataset: str,
    partition: _Partition,
    path: Path,
    parquet: pq.ParquetFile,
    expected_schema: pa.Schema,
) -> Issue | None:
    """检查一个 Parquet 文件的 Schema。"""
    if parquet.schema_arrow.equals(expected_schema, check_metadata=True):
        return None
    return Issue.create(
        dataset=dataset,
        partition=partition.label,
        key={"partition": partition.label, "file": path.name},
        rule_id="schema_v1",
        fix_mode="MANUAL",
        observed={"schema": str(parquet.schema_arrow)},
        suggested=None,
        message=f"{dataset} Parquet Schema 与登记 Schema 不一致",
    )


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
    """按固定顺序执行通用检查，再执行该数据集的专有检查。"""
    issues = _check_partition_values(dataset, partition, partition_date, table)
    issues.extend(_check_duplicate_primary_keys(dataset, partition, partition_date, table))
    issues.extend(_check_required_values(dataset, partition, partition_date, table))
    issues.extend(_check_finite_floats(dataset, partition, partition_date, table))
    issues.extend(_DATASET_CHECKERS[dataset](partition, partition_date, table))
    return issues


def _check_partition_values(
    dataset: str,
    partition: str,
    partition_date: date | None,
    table: pa.Table,
) -> list[Issue]:
    """检查分区路径与每行的分区字段是否一致。"""
    partition_field = TABLE_PARTITION_BY[dataset]
    partition_values = {row[partition_field] for row in table.to_pylist()}
    if None in partition_values or (
        partition_date is not None and partition_values != {partition_date}
    ):
        return [
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
        ]
    return []


def _check_duplicate_primary_keys(
    dataset: str,
    partition: str,
    partition_date: date | None,
    table: pa.Table,
) -> list[Issue]:
    """检查分区内的业务主键是否重复。"""
    primary_key = (TABLE_PARTITION_BY[dataset], *TABLE_PRIMARY_KEY[dataset])
    rows = table.to_pylist()
    counts = Counter(tuple(row[name] for name in primary_key) for row in rows)
    issues: list[Issue] = []
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
    return issues


def _check_required_values(
    dataset: str,
    partition: str,
    partition_date: date | None,
    table: pa.Table,
) -> list[Issue]:
    """检查 Schema 中标记为必填的字段。"""
    schema = TABLE_SCHEMAS[dataset]
    required = [field.name for field in schema if not field.nullable]
    issues: list[Issue] = []
    for row in table.to_pylist():
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
    return issues


def _check_finite_floats(
    dataset: str,
    partition: str,
    partition_date: date | None,
    table: pa.Table,
) -> list[Issue]:
    """检查所有浮点字段是否包含 NaN 或无穷值。"""
    schema = TABLE_SCHEMAS[dataset]
    issues: list[Issue] = []
    for row in table.to_pylist():
        key = _row_key(dataset, row)
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
    return issues


def check_daily(
    partition: str,
    partition_date: date | None,
    table: pa.Table,
) -> list[Issue]:
    """检查 daily：完整性、数值范围、OHLC 关系和收盘价一致性。"""
    issues: list[Issue] = []
    for row in table.to_pylist():
        key = _row_key("daily", row)
        refetch = _suggested_refetch("daily", row, partition_date)
        required = ("open", "high", "low", "close", "pre_close", "vol", "amount")
        missing = [name for name in required if row[name] is None]
        if missing:
            issues.append(
                _manual("daily", partition, key, "daily_missing_v1", row, missing, refetch)
            )
            continue
        if not all(_finite_number(row[name]) for name in required):
            continue
        open_, high, low, close, pre_close, volume, amount = (
            _number(row[name]) for name in required
        )
        range_invalid = min(open_, high, low, close, pre_close) <= 0 or volume < 0 or amount < 0
        if range_invalid:
            issues.append(
                _manual("daily", partition, key, "daily_range_v1", row, required, refetch)
            )
        elif high < max(open_, low, close) or low > min(open_, high, close):
            issues.append(
                _manual("daily", partition, key, "daily_ohlc_v1", row, required[:4], refetch)
            )
        change = row["change"]
        pct_chg = row["pct_chg"]
        if not (_finite_number(change) and _finite_number(pct_chg)):
            continue
        from_change = round(pre_close + _number(change), 2)
        from_pct = pre_close * (1 + _number(pct_chg) / 100)
        if (
            math.isclose(from_change, from_pct, abs_tol=0.0051)
            and not math.isclose(close, from_change, abs_tol=0.0051)
            and low <= from_change <= high
        ):
            issues.append(
                Issue.create(
                    dataset="daily",
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
    return issues


def check_adj_factor(
    partition: str,
    partition_date: date | None,
    table: pa.Table,
) -> list[Issue]:
    """检查 adj_factor：复权因子必须为正数。"""
    issues: list[Issue] = []
    for row in table.to_pylist():
        value = row["adj_factor"]
        if value is None or (_finite_number(value) and _number(value) <= 0):
            issues.append(
                _manual(
                    "adj_factor",
                    partition,
                    _row_key("adj_factor", row),
                    "adj_factor_positive_v1",
                    row,
                    ("adj_factor",),
                    _suggested_refetch("adj_factor", row, partition_date),
                )
            )
    return issues


def check_stk_limit(
    partition: str,
    partition_date: date | None,
    table: pa.Table,
) -> list[Issue]:
    """检查 stk_limit：关键价格完整且涨跌停顺序正确。"""
    fields = ("pre_close", "up_limit", "down_limit")
    all_null = [
        name
        for name in fields
        if table.num_rows and table.column(name).null_count == table.num_rows
    ]
    if all_null:
        return [
            Issue.create(
                dataset="stk_limit",
                partition=partition,
                key={"trade_date": partition_date},
                rule_id="stk_limit_partition_missing_v1",
                fix_mode="MANUAL",
                observed={"all_null_fields": all_null, "row_count": table.num_rows},
                suggested=_refetch(partition_date) if partition_date else None,
                message=f"stk_limit 整个分区的关键字段为空: {all_null}",
            )
        ]
    issues: list[Issue] = []
    for row in table.to_pylist():
        key = _row_key("stk_limit", row)
        refetch = _suggested_refetch("stk_limit", row, partition_date)
        if any(row[name] is None for name in fields):
            issues.append(
                _manual("stk_limit", partition, key, "stk_limit_missing_v1", row, fields, refetch)
            )
        elif all(_finite_number(row[name]) for name in fields):
            pre_close, up_limit, down_limit = (_number(row[name]) for name in fields)
            if down_limit <= 0 or not down_limit <= pre_close <= up_limit:
                issues.append(
                    _manual("stk_limit", partition, key, "stk_limit_order_v1", row, fields, refetch)
                )
    return issues


def check_trade_cal(
    partition: str,
    partition_date: date | None,
    table: pa.Table,
) -> list[Issue]:
    """检查 trade_cal：交易所、开市标记和三市覆盖完整。"""
    issues: list[Issue] = []
    rows = table.to_pylist()
    for row in rows:
        if row["exchange"] not in _EXCHANGES or row["is_open"] not in {0, 1}:
            issues.append(
                _manual(
                    "trade_cal",
                    partition,
                    _row_key("trade_cal", row),
                    "trade_calendar_value_v1",
                    row,
                    ("exchange", "is_open"),
                    _suggested_refetch("trade_cal", row, partition_date),
                )
            )
    if partition_date is not None:
        missing = sorted(_EXCHANGES - {row["exchange"] for row in rows})
        if missing:
            issues.append(
                Issue.create(
                    dataset="trade_cal",
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


def check_stock_basic(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 stock_basic：当前只执行通用检查。"""
    return []


def check_daily_basic(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 daily_basic：当前只执行通用检查。"""
    return []


def check_suspend_d(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 suspend_d：当前只执行通用检查。"""
    return []


def check_stock_st(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 stock_st：当前只执行通用检查。"""
    return []


def check_moneyflow(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 moneyflow：当前只执行通用检查。"""
    return []


def check_dividend(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 dividend：当前只执行通用检查。"""
    return []


def check_forecast(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 forecast：当前只执行通用检查。"""
    return []


def check_express(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 express：当前只执行通用检查。"""
    return []


def check_fina_audit(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 fina_audit：当前只执行通用检查。"""
    return []


def check_income(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 income：当前只执行通用检查。"""
    return []


def check_balancesheet(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 balancesheet：当前只执行通用检查。"""
    return []


def check_cashflow(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 cashflow：当前只执行通用检查。"""
    return []


def check_fina_indicator(
    partition: str, partition_date: date | None, table: pa.Table
) -> list[Issue]:
    """检查 fina_indicator：当前只执行通用检查。"""
    return []


def check_sw_industry(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 sw_industry：当前只执行通用检查。"""
    return []


_DATASET_CHECKERS: dict[str, DatasetChecker] = {
    "stock_basic": check_stock_basic,
    "daily": check_daily,
    "daily_basic": check_daily_basic,
    "adj_factor": check_adj_factor,
    "suspend_d": check_suspend_d,
    "stk_limit": check_stk_limit,
    "stock_st": check_stock_st,
    "moneyflow": check_moneyflow,
    "dividend": check_dividend,
    "forecast": check_forecast,
    "express": check_express,
    "fina_audit": check_fina_audit,
    "income": check_income,
    "balancesheet": check_balancesheet,
    "cashflow": check_cashflow,
    "fina_indicator": check_fina_indicator,
    "sw_industry": check_sw_industry,
    "trade_cal": check_trade_cal,
}


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


def _check_results(
    datasets: Iterable[str],
    issues: Iterable[Issue],
    *,
    full_history: bool,
) -> tuple[CheckResult, ...]:
    """为每个数据集的每个检查项生成 PASS/FAIL 结果。"""
    counts = Counter((issue.dataset, issue.rule_id) for issue in issues)
    results: list[CheckResult] = []
    for dataset in datasets:
        definitions = dict(_COMMON_CHECKS)
        if not full_history:
            definitions.pop("dataset_empty_v1")
        if dataset in _DENSE_MARKET_DATASETS:
            definitions.update(_MISSING_PARTITION_CHECK)
        definitions.update(_DATASET_CHECKS.get(dataset, {}))
        for check_id, description in definitions.items():
            issue_count = counts[(dataset, check_id)]
            results.append(
                CheckResult(
                    dataset=dataset,
                    check_id=check_id,
                    description=description,
                    status="FAIL" if issue_count else "PASS",
                    issue_count=issue_count,
                )
            )
    return tuple(results)
