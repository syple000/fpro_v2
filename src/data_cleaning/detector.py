"""对 Tushare Manifest 当前引用的数据执行简单、确定的检查。"""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Literal
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
_EXCHANGES = frozenset({"SSE", "SZSE"})
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
_CLOSED_MARKET_CHECK = {"closed_market_partition_v1": "日级市场数据只出现在开市日"}

_DATASET_CHECKS = {
    "stock_basic": {
        "stock_basic_identity_v1": "代码、证券代码和交易所一致",
        "stock_basic_lifecycle_v1": "上市状态和上市退市日期合理",
    },
    "daily": {
        "daily_missing_v1": "行情价格、成交量和成交额完整",
        "daily_range_v1": "价格为正且成交量额非负",
        "daily_ohlc_v1": "开高低收关系正确",
        "daily_close_consistency_v1": "收盘价与涨跌额、涨跌幅一致",
        "daily_arithmetic_v1": "涨跌额和涨跌幅计算正确",
        "daily_volume_amount_v1": "成交量和成交额的零值关系正确",
    },
    "daily_basic": {
        "daily_basic_range_v1": "股本、市值和非负指标不小于零",
        "daily_basic_share_order_v1": "总股本、流通股本和自由流通股本顺序合理",
        "daily_basic_market_value_v1": "市值等于收盘价乘以对应股本",
        "daily_basic_daily_match_v1": "每日指标与日线收盘价及覆盖一致",
    },
    "adj_factor": {
        "adj_factor_positive_v1": "复权因子为正数",
        "adj_factor_daily_coverage_v1": "每条日线都有同日复权因子",
        "adj_factor_continuity_v1": "复权后昨日收盘与今日昨收连续",
        "adj_factor_decrease_v1": "复权因子下降需要复核",
        "adj_factor_without_daily_v1": "无对应日线的复权因子需要复核",
    },
    "suspend_d": {
        "suspend_value_v1": "停复牌类型和日内时间段合法",
        "suspend_daily_conflict_v1": "全日停牌记录与日线不冲突",
    },
    "stk_limit": {
        "stk_limit_partition_missing_v1": "整个分区的关键价格不全为空",
        "stk_limit_missing_v1": "昨收、涨停价和跌停价完整",
        "stk_limit_order_v1": "跌停价 ≤ 昨收价 ≤ 涨停价",
        "stk_limit_daily_match_v1": "涨跌停昨收与日线一致且行情不越界",
    },
    "stock_st": {
        "stock_st_value_v1": "ST 类型、类型名称和证券名称完整",
    },
    "moneyflow": {
        "moneyflow_range_v1": "买卖分档成交量和成交额非负",
        "moneyflow_daily_coverage_v1": "资金流向存在对应日线",
    },
    "dividend": {
        "dividend_value_v1": "分红送转数值和实施进度合法",
        "dividend_stock_ratio_v1": "每股送转等于送股与转增之和",
        "dividend_date_order_v1": "实施阶段的登记、除权和派发日期合理",
    },
    "forecast": {
        "forecast_value_v1": "业绩预告类型和版本标识合法",
        "forecast_range_v1": "预告比例和利润上下限顺序正确",
        "forecast_date_order_v1": "首次公告、公告日和报告期顺序合理",
    },
    "express": {
        "express_value_v1": "业绩快报日期和版本标识合法",
        "express_audit_flag_v1": "业绩快报审计标识异常需要复核",
        "express_growth_v1": "业绩快报同比指标与同期值一致",
    },
    "fina_audit": {
        "fina_audit_value_v1": "审计公告日期、费用和审计信息合理",
    },
    "income": {
        "income_value_v1": "利润表公告日期、报告类型和版本合法",
        "income_equation_v1": "利润表核心科目勾稽一致",
    },
    "balancesheet": {
        "balancesheet_value_v1": "资产负债表公告日期、报告类型和版本合法",
        "balancesheet_equation_v1": "资产、负债和权益核心科目勾稽一致",
    },
    "cashflow": {
        "cashflow_value_v1": "现金流量表公告日期、报告类型和版本合法",
        "cashflow_equation_v1": "现金流入、流出、净额和余额勾稽一致",
    },
    "fina_indicator": {
        "fina_indicator_value_v1": "财务指标公告日期、报告期和版本合法",
    },
    "sw_industry": {
        "sw_industry_value_v1": "申万行业层级、日期和最新标识合法",
        "sw_industry_mapping_v1": "申万行业层级映射和有效区间不冲突",
    },
    "trade_cal": {
        "trade_calendar_value_v1": "交易所代码和开市标记合法",
        "calendar_exchange_coverage_v1": "每日覆盖 SSE 和 SZSE",
        "calendar_date_coverage_v1": "请求范围内每个自然日都有交易日历",
        "calendar_pretrade_v1": "上一个交易日指向此前最近开市日",
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

    if "trade_cal" in selected:
        issues.extend(
            check_trade_calendar_series(
                source,
                through=through,
                start=start,
                observed_dates=observed_dates["trade_cal"],
            )
        )

    open_dates = _calendar_open_dates(source, through=through, start=start)
    for dataset in selected:
        if dataset not in _DENSE_MARKET_DATASETS or not open_dates:
            continue
        actual = observed_dates[dataset]
        for closed in sorted(actual - open_dates):
            issues.append(
                Issue.create(
                    dataset=dataset,
                    partition=closed.isoformat(),
                    key={TABLE_PARTITION_BY[dataset]: closed},
                    rule_id="closed_market_partition_v1",
                    fix_mode="MANUAL",
                    observed={"calendar": "closed", "partition": "present"},
                    suggested=_refetch(closed),
                    message=f"{dataset} 在休市日 {closed.isoformat()} 存在分区",
                )
            )
        if actual:
            lower = start or min(actual)
            upper = through
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

    issues.extend(
        check_cross_dataset_consistency(
            source,
            datasets=selected,
            through=through,
            start=start,
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
        can_fix_close = (
            math.isclose(from_change, from_pct, abs_tol=0.0051)
            and not math.isclose(close, from_change, abs_tol=0.0051)
            and low <= from_change <= high
        )
        if can_fix_close:
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
        elif not math.isclose(close, from_change, abs_tol=0.0051) or not math.isclose(
            _number(pct_chg), _number(change) / pre_close * 100, abs_tol=0.011
        ):
            issues.append(
                _manual(
                    "daily",
                    partition,
                    key,
                    "daily_arithmetic_v1",
                    row,
                    ("close", "pre_close", "change", "pct_chg"),
                    refetch,
                )
            )
        if (volume == 0) != (amount == 0):
            issues.append(
                _manual(
                    "daily",
                    partition,
                    key,
                    "daily_volume_amount_v1",
                    row,
                    ("vol", "amount"),
                    refetch,
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
    fields = ("up_limit", "down_limit")
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
            up_limit, down_limit = (_number(row[name]) for name in fields)
            pre_close = row["pre_close"]
            if (
                down_limit < 0
                or up_limit < down_limit
                or (_finite_number(pre_close) and not down_limit <= _number(pre_close) <= up_limit)
            ):
                issues.append(
                    _manual(
                        "stk_limit",
                        partition,
                        key,
                        "stk_limit_order_v1",
                        row,
                        ("pre_close", *fields),
                        refetch,
                    )
                )
    return issues


def check_trade_cal(
    partition: str,
    partition_date: date | None,
    table: pa.Table,
) -> list[Issue]:
    """检查 trade_cal：交易所、开市标记和沪深市场覆盖完整。"""
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
    """检查 stock_basic：证券身份和上市生命周期。"""
    issues: list[Issue] = []
    suffix_by_exchange = {"SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}
    for row in table.to_pylist():
        key = _row_key("stock_basic", row)
        code = row["ts_code"]
        symbol = row["symbol"]
        exchange = row["exchange"]
        suffix = suffix_by_exchange.get(exchange)
        identity_ok = (
            isinstance(code, str)
            and isinstance(symbol, str)
            and suffix is not None
            and code == f"{symbol}.{suffix}"
        )
        if not identity_ok:
            issues.append(
                _manual(
                    "stock_basic",
                    partition,
                    key,
                    "stock_basic_identity_v1",
                    row,
                    ("ts_code", "symbol", "exchange"),
                    _suggested_refetch("stock_basic", row, partition_date),
                )
            )
        status = row["list_status"]
        listed = row["list_date"]
        delisted = row["delist_date"]
        lifecycle_ok = (
            status in {"L", "D", "P"}
            and isinstance(listed, date)
            and not (isinstance(delisted, date) and listed > delisted)
            and not (status == "D" and not isinstance(delisted, date))
            and not (status == "L" and isinstance(delisted, date))
        )
        if not lifecycle_ok:
            issues.append(
                _manual(
                    "stock_basic",
                    partition,
                    key,
                    "stock_basic_lifecycle_v1",
                    row,
                    ("list_status", "list_date", "delist_date"),
                    _suggested_refetch("stock_basic", row, partition_date),
                )
            )
    return issues


def check_daily_basic(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 daily_basic：非负范围、股本顺序和市值恒等式。"""
    issues: list[Issue] = []
    nonnegative = (
        "close",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "dv_ratio",
        "dv_ttm",
        "total_share",
        "float_share",
        "free_share",
        "total_mv",
        "circ_mv",
    )
    for row in table.to_pylist():
        key = _row_key("daily_basic", row)
        refetch = _suggested_refetch("daily_basic", row, partition_date)
        invalid = [
            name for name in nonnegative if _finite_number(row[name]) and _number(row[name]) < 0
        ]
        if invalid:
            issues.append(
                _manual(
                    "daily_basic",
                    partition,
                    key,
                    "daily_basic_range_v1",
                    row,
                    invalid,
                    refetch,
                )
            )
        total, floating, free = (
            row["total_share"],
            row["float_share"],
            row["free_share"],
        )
        share_bad = (
            _finite_number(total)
            and _finite_number(floating)
            and _number(floating) > _number(total) + 0.001
        ) or (
            _finite_number(floating)
            and _finite_number(free)
            and _number(free) > _number(floating) + 0.001
        )
        if share_bad:
            issues.append(
                _manual(
                    "daily_basic",
                    partition,
                    key,
                    "daily_basic_share_order_v1",
                    row,
                    ("total_share", "float_share", "free_share"),
                    refetch,
                    severity="WARNING",
                )
            )
        market_value_bad = _product_mismatch(row, "total_mv", "close", "total_share") or (
            _product_mismatch(row, "circ_mv", "close", "float_share")
        )
        if market_value_bad:
            issues.append(
                _manual(
                    "daily_basic",
                    partition,
                    key,
                    "daily_basic_market_value_v1",
                    row,
                    ("close", "total_share", "float_share", "total_mv", "circ_mv"),
                    refetch,
                    severity="WARNING",
                )
            )
    return issues


def check_suspend_d(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 suspend_d：停复牌类型和时间段。"""
    issues: list[Issue] = []
    for row in table.to_pylist():
        timing = row["suspend_timing"]
        invalid = row["suspend_type"] not in {"S", "R"} or (
            row["suspend_type"] == "R" and timing is not None
        )
        if invalid:
            issues.append(
                _manual(
                    "suspend_d",
                    partition,
                    _row_key("suspend_d", row),
                    "suspend_value_v1",
                    row,
                    ("suspend_type", "suspend_timing"),
                    _suggested_refetch("suspend_d", row, partition_date),
                )
            )
    return issues


def check_stock_st(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 stock_st：状态和名称完整。"""
    issues: list[Issue] = []
    for row in table.to_pylist():
        fields = ("name", "type", "type_name")
        if any(not isinstance(row[name], str) or not row[name].strip() for name in fields):
            issues.append(
                _manual(
                    "stock_st",
                    partition,
                    _row_key("stock_st", row),
                    "stock_st_value_v1",
                    row,
                    fields,
                    _suggested_refetch("stock_st", row, partition_date),
                )
            )
    return issues


def check_moneyflow(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 moneyflow：买卖分档量额必须非负。"""
    issues: list[Issue] = []
    fields = tuple(
        name for name in TABLE_SCHEMAS["moneyflow"].names if name.startswith(("buy_", "sell_"))
    )
    for row in table.to_pylist():
        invalid = [name for name in fields if _finite_number(row[name]) and _number(row[name]) < 0]
        if invalid:
            issues.append(
                _manual(
                    "moneyflow",
                    partition,
                    _row_key("moneyflow", row),
                    "moneyflow_range_v1",
                    row,
                    invalid,
                    _suggested_refetch("moneyflow", row, partition_date),
                )
            )
    return issues


def check_dividend(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 dividend：数值、送转合计和实施日期。"""
    issues: list[Issue] = []
    processes = {
        "预案",
        "预披露",
        "股东大会通过",
        "股东大会未通过",
        "实施",
        "停止实施",
        "未通过",
        "其他",
    }
    numeric = (
        "stk_div",
        "stk_bo_rate",
        "stk_co_rate",
        "cash_div",
        "cash_div_tax",
        "base_share",
    )
    for row in table.to_pylist():
        key = _row_key("dividend", row)
        refetch = _suggested_refetch("dividend", row, partition_date)
        process = row["div_proc"]
        invalid = [name for name in numeric if _finite_number(row[name]) and _number(row[name]) < 0]
        if not isinstance(process, str) or process not in processes:
            invalid.append("div_proc")
        if (
            _finite_number(row["cash_div"])
            and _finite_number(row["cash_div_tax"])
            and _number(row["cash_div"]) > _number(row["cash_div_tax"]) + 1e-8
        ):
            invalid.extend(("cash_div", "cash_div_tax"))
        if invalid:
            issues.append(
                _manual(
                    "dividend",
                    partition,
                    key,
                    "dividend_value_v1",
                    row,
                    dict.fromkeys(invalid),
                    refetch,
                )
            )
        parts = (row["stk_div"], row["stk_bo_rate"], row["stk_co_rate"])
        if all(_finite_number(value) for value in parts) and not math.isclose(
            _number(parts[0]), _number(parts[1]) + _number(parts[2]), abs_tol=1.1e-6
        ):
            issues.append(
                _manual(
                    "dividend",
                    partition,
                    key,
                    "dividend_stock_ratio_v1",
                    row,
                    ("stk_div", "stk_bo_rate", "stk_co_rate"),
                    refetch,
                )
            )
        if process == "实施" and not _implemented_dividend_dates_valid(row):
            issues.append(
                _manual(
                    "dividend",
                    partition,
                    key,
                    "dividend_date_order_v1",
                    row,
                    ("imp_ann_date", "record_date", "ex_date", "pay_date", "div_listdate"),
                    refetch,
                    severity="WARNING",
                )
            )
    return issues


def check_forecast(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 forecast：类型、上下限和公告日期。"""
    issues: list[Issue] = []
    types = {
        "预增",
        "预减",
        "扭亏",
        "首亏",
        "续亏",
        "续盈",
        "略增",
        "略减",
        "不确定",
        "增亏",
        "减亏",
        "其他",
    }
    for row in table.to_pylist():
        key = _row_key("forecast", row)
        refetch = _suggested_refetch("forecast", row, partition_date)
        if row["type"] not in types or row["update_flag"] not in {None, "0", "1"}:
            issues.append(
                _manual(
                    "forecast",
                    partition,
                    key,
                    "forecast_value_v1",
                    row,
                    ("type", "update_flag"),
                    refetch,
                )
            )
        if _bounds_reversed(row["p_change_min"], row["p_change_max"]) or _bounds_reversed(
            row["net_profit_min"], row["net_profit_max"]
        ):
            issues.append(
                _manual(
                    "forecast",
                    partition,
                    key,
                    "forecast_range_v1",
                    row,
                    ("p_change_min", "p_change_max", "net_profit_min", "net_profit_max"),
                    refetch,
                )
            )
        first, announced = row["first_ann_date"], row["ann_date"]
        if not _quarter_end(row["end_date"]) or (
            isinstance(first, date) and isinstance(announced, date) and first > announced
        ):
            issues.append(
                _manual(
                    "forecast",
                    partition,
                    key,
                    "forecast_date_order_v1",
                    row,
                    ("end_date", "first_ann_date", "ann_date"),
                    refetch,
                    severity="WARNING",
                )
            )
    return issues


def check_express(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 express：公告字段和可重算同比指标。"""
    issues: list[Issue] = []
    comparisons = (
        ("revenue", "or_last_year", "yoy_sales"),
        ("operate_profit", "op_last_year", "yoy_op"),
        ("total_profit", "tp_last_year", "yoy_tp"),
        ("diluted_eps", "eps_last_year", "yoy_eps"),
    )
    for row in table.to_pylist():
        key = _row_key("express", row)
        refetch = _suggested_refetch("express", row, partition_date)
        if not _formal_announcement_valid(row, actual=False) or row["update_flag"] not in {
            None,
            "0",
            "1",
        }:
            issues.append(
                _manual(
                    "express",
                    partition,
                    key,
                    "express_value_v1",
                    row,
                    ("end_date", "ann_date", "update_flag"),
                    refetch,
                )
            )
        if row["is_audit"] not in {None, 0, 1}:
            issues.append(
                _manual(
                    "express",
                    partition,
                    key,
                    "express_audit_flag_v1",
                    row,
                    ("is_audit",),
                    refetch,
                    severity="WARNING",
                )
            )
        bad = [names for names in comparisons if _growth_mismatch(row, *names)]
        if bad:
            fields = tuple(dict.fromkeys(name for names in bad for name in names))
            issues.append(
                _manual(
                    "express",
                    partition,
                    key,
                    "express_growth_v1",
                    row,
                    fields,
                    refetch,
                    severity="WARNING",
                )
            )
    return issues


def check_fina_audit(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 fina_audit：日期、费用和年度审计信息。"""
    issues: list[Issue] = []
    for row in table.to_pylist():
        invalid = not _formal_announcement_valid(row, actual=False) or (
            _finite_number(row["audit_fees"]) and _number(row["audit_fees"]) < 0
        )
        if row["end_date"] and row["end_date"].month == 12:
            invalid = invalid or any(
                not isinstance(row[name], str) or not row[name].strip()
                for name in ("audit_result", "audit_agency")
            )
        if invalid:
            issues.append(
                _manual(
                    "fina_audit",
                    partition,
                    _row_key("fina_audit", row),
                    "fina_audit_value_v1",
                    row,
                    ("end_date", "ann_date", "audit_result", "audit_fees", "audit_agency"),
                    _suggested_refetch("fina_audit", row, partition_date),
                    severity="WARNING",
                )
            )
    return issues


def check_income(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 income：公告字段和利润勾稽关系。"""
    equations = (
        ("total_profit", (("operate_profit", 1), ("non_oper_income", 1), ("non_oper_exp", -1))),
        ("n_income", (("total_profit", 1), ("income_tax", -1))),
        ("n_income", (("n_income_attr_p", 1), ("minority_gain", 1))),
        ("t_compr_income", (("n_income", 1), ("oth_compr_income", 1))),
        ("t_compr_income", (("compr_inc_attr_p", 1), ("compr_inc_attr_m_s", 1))),
    )
    return _check_financial_statement(
        "income",
        partition,
        partition_date,
        table,
        "income_value_v1",
        "income_equation_v1",
        equations,
    )


def check_balancesheet(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 balancesheet：公告字段和资产负债权益勾稽。"""
    equations = (
        ("total_assets", (("total_liab_hldr_eqy", 1),)),
        ("total_liab_hldr_eqy", (("total_liab", 1), ("total_hldr_eqy_inc_min_int", 1))),
        (
            "total_hldr_eqy_inc_min_int",
            (("total_hldr_eqy_exc_min_int", 1), ("minority_int", 1)),
        ),
    )
    return _check_financial_statement(
        "balancesheet",
        partition,
        partition_date,
        table,
        "balancesheet_value_v1",
        "balancesheet_equation_v1",
        equations,
    )


def check_cashflow(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 cashflow：公告字段、流量净额和现金余额勾稽。"""
    equations = (
        ("n_cashflow_act", (("c_inf_fr_operate_a", 1), ("st_cash_out_act", -1))),
        ("n_cashflow_inv_act", (("stot_inflows_inv_act", 1), ("stot_out_inv_act", -1))),
        ("n_cash_flows_fnc_act", (("stot_cash_in_fnc_act", 1), ("stot_cashout_fnc_act", -1))),
        (
            "n_incr_cash_cash_equ",
            (
                ("n_cashflow_act", 1),
                ("n_cashflow_inv_act", 1),
                ("n_cash_flows_fnc_act", 1),
                ("eff_fx_flu_cash", 1),
            ),
        ),
        ("c_cash_equ_end_period", (("c_cash_equ_beg_period", 1), ("n_incr_cash_cash_equ", 1))),
        ("im_net_cashflow_oper_act", (("n_cashflow_act", 1),)),
        ("im_n_incr_cash_equ", (("n_incr_cash_cash_equ", 1),)),
    )
    return _check_financial_statement(
        "cashflow",
        partition,
        partition_date,
        table,
        "cashflow_value_v1",
        "cashflow_equation_v1",
        equations,
    )


def check_fina_indicator(
    partition: str, partition_date: date | None, table: pa.Table
) -> list[Issue]:
    """检查 fina_indicator：公告、报告期和版本标识。"""
    issues: list[Issue] = []
    for row in table.to_pylist():
        if not _formal_announcement_valid(row, actual=False) or row["update_flag"] not in {
            None,
            "0",
            "1",
        }:
            issues.append(
                _manual(
                    "fina_indicator",
                    partition,
                    _row_key("fina_indicator", row),
                    "fina_indicator_value_v1",
                    row,
                    ("end_date", "ann_date", "update_flag"),
                    _suggested_refetch("fina_indicator", row, partition_date),
                )
            )
    return issues


def check_sw_industry(partition: str, partition_date: date | None, table: pa.Table) -> list[Issue]:
    """检查 sw_industry：层级字段、有效区间和最新标识。"""
    issues: list[Issue] = []
    hierarchy = ("l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name")
    for row in table.to_pylist():
        invalid = (
            any(not isinstance(row[name], str) or not row[name].strip() for name in hierarchy)
            or row["is_new"] not in {"Y", "N"}
            or (
                isinstance(row["out_date"], date)
                and isinstance(row["in_date"], date)
                and row["out_date"] <= row["in_date"]
            )
            or (row["is_new"] == "Y" and row["out_date"] is not None)
        )
        if invalid:
            issues.append(
                _manual(
                    "sw_industry",
                    partition,
                    _row_key("sw_industry", row),
                    "sw_industry_value_v1",
                    row,
                    (*hierarchy, "in_date", "out_date", "is_new"),
                    _suggested_refetch("sw_industry", row, partition_date),
                    severity="WARNING",
                )
            )
    return issues


def check_trade_calendar_series(
    root: Path,
    *,
    through: date,
    start: date | None,
    observed_dates: set[date],
) -> list[Issue]:
    """检查交易日历自然日连续性和 pretrade_date。"""
    issues: list[Issue] = []
    if observed_dates:
        lower = start or min(observed_dates)
        current = lower
        while current <= through:
            if current not in observed_dates:
                issues.append(
                    Issue.create(
                        dataset="trade_cal",
                        partition=current.isoformat(),
                        key={"cal_date": current},
                        rule_id="calendar_date_coverage_v1",
                        fix_mode="MANUAL",
                        observed={"partition": "missing"},
                        suggested=_refetch(current),
                        message=f"交易日历缺少自然日 {current.isoformat()}",
                    )
                )
            current += timedelta(days=1)

    index = _partition_index(root, "trade_cal", through=through, start=None)
    previous_open: dict[str, date] = {}
    for current in sorted(index):
        partition = index[current]
        for row in _read_partition_rows(partition):
            exchange = row["exchange"]
            if not isinstance(exchange, str) or exchange not in _EXCHANGES:
                continue
            expected = previous_open.get(exchange)
            if row["pretrade_date"] != expected and (start is None or current >= start):
                issues.append(
                    _manual(
                        "trade_cal",
                        partition.label,
                        _row_key("trade_cal", row),
                        "calendar_pretrade_v1",
                        row,
                        ("is_open", "pretrade_date"),
                        _suggested_refetch("trade_cal", row, current),
                        severity="WARNING",
                        message=(
                            f"{exchange} {current.isoformat()} 的 pretrade_date 应为 "
                            f"{expected.isoformat() if expected else None}"
                        ),
                    )
                )
            if row["is_open"] == 1:
                previous_open[exchange] = current
    return issues


def check_cross_dataset_consistency(
    root: Path,
    *,
    datasets: tuple[str, ...],
    through: date,
    start: date | None,
) -> list[Issue]:
    """只在相关数据集同时被选择时执行明确的跨表检查。"""
    selected = set(datasets)
    issues: list[Issue] = []
    if {"daily", "adj_factor"} <= selected:
        issues.extend(_check_daily_and_adj_factor(root, through=through, start=start))
    if {"daily", "daily_basic"} <= selected:
        issues.extend(_check_daily_and_daily_basic(root, through=through, start=start))
    if {"daily", "stk_limit"} <= selected:
        issues.extend(_check_daily_and_stk_limit(root, through=through, start=start))
    if {"daily", "suspend_d"} <= selected:
        issues.extend(_check_daily_and_suspend(root, through=through, start=start))
    if {"daily", "moneyflow"} <= selected:
        issues.extend(_check_daily_and_moneyflow(root, through=through, start=start))
    if "sw_industry" in selected:
        issues.extend(_check_sw_industry_series(root, through=through, start=start))
    return issues


def _check_daily_and_adj_factor(root: Path, *, through: date, start: date | None) -> list[Issue]:
    daily = _partition_index(root, "daily", through=through, start=start)
    factors = _partition_index(root, "adj_factor", through=through, start=start)
    issues: list[Issue] = []
    previous_matched: dict[str, tuple[float, float]] = {}
    previous_factor: dict[str, float] = {}
    decrease_count = 0
    decrease_samples: list[dict[str, object]] = []
    extra_count = 0
    extra_samples: list[dict[str, object]] = []

    for current in sorted(set(daily) | set(factors)):
        daily_rows = _rows_by_code(daily.get(current))
        factor_rows = _rows_by_code(factors.get(current))
        missing = sorted(daily_rows.keys() - factor_rows.keys())
        if missing:
            issues.append(
                _cross_partition_issue(
                    dataset="adj_factor",
                    partition=factors.get(current),
                    day=current,
                    rule_id="adj_factor_daily_coverage_v1",
                    count=len(missing),
                    samples=missing[:10],
                    severity="ERROR",
                    message="日线缺少同代码同日期的复权因子",
                )
            )
            for code in missing:
                previous_matched.pop(code, None)

        extras = sorted(factor_rows.keys() - daily_rows.keys())
        extra_count += len(extras)
        extra_samples.extend(
            {"ts_code": code, "trade_date": current}
            for code in extras[: max(0, 10 - len(extra_samples))]
        )

        continuity_bad: list[dict[str, object]] = []
        for code, factor_row in factor_rows.items():
            value = factor_row["adj_factor"]
            if not _finite_number(value) or _number(value) <= 0:
                continue
            factor = _number(value)
            if code in previous_factor and factor < previous_factor[code]:
                decrease_count += 1
                if len(decrease_samples) < 10:
                    decrease_samples.append(
                        {
                            "ts_code": code,
                            "trade_date": current,
                            "previous": previous_factor[code],
                            "current": factor,
                        }
                    )
            previous_factor[code] = factor
            daily_row = daily_rows.get(code)
            if daily_row is None or not _finite_number(daily_row["close"]):
                continue
            previous = previous_matched.get(code)
            if previous is not None and _finite_number(daily_row["pre_close"]):
                previous_close, previous_adj = previous
                expected = round(previous_close * previous_adj / factor, 2)
                actual = _number(daily_row["pre_close"])
                if not math.isclose(actual, expected, abs_tol=0.011):
                    continuity_bad.append(
                        {
                            "ts_code": code,
                            "expected_pre_close": expected,
                            "actual_pre_close": actual,
                        }
                    )
            previous_matched[code] = (_number(daily_row["close"]), factor)
        if continuity_bad:
            issues.append(
                _cross_partition_issue(
                    dataset="adj_factor",
                    partition=factors.get(current),
                    day=current,
                    rule_id="adj_factor_continuity_v1",
                    count=len(continuity_bad),
                    samples=continuity_bad[:10],
                    severity="WARNING",
                    message="复权因子与相邻日线无法形成连续复权价格",
                )
            )

    if decrease_count:
        issues.append(
            _dataset_series_issue(
                "adj_factor",
                "adj_factor_decrease_v1",
                decrease_count,
                decrease_samples,
                "复权因子出现下降；这不是硬错误，但需要核对公司行动或历史修订",
            )
        )
    if extra_count:
        issues.append(
            _dataset_series_issue(
                "adj_factor",
                "adj_factor_without_daily_v1",
                extra_count,
                extra_samples,
                "存在没有同日行情的复权因子；需要区分停牌、退市和历史代码",
            )
        )
    return issues


def _check_daily_and_daily_basic(root: Path, *, through: date, start: date | None) -> list[Issue]:
    daily = _partition_index(root, "daily", through=through, start=start)
    basics = _partition_index(root, "daily_basic", through=through, start=start)
    issues: list[Issue] = []
    for current in sorted(set(daily) | set(basics)):
        daily_rows = _rows_by_code(daily.get(current))
        basic_rows = _rows_by_code(basics.get(current))
        bad: list[dict[str, object]] = []
        for code, row in basic_rows.items():
            market = daily_rows.get(code)
            if market is None:
                bad.append({"ts_code": code, "reason": "没有同日日线"})
            elif (
                _finite_number(row["close"])
                and _finite_number(market["close"])
                and not math.isclose(
                    _number(row["close"]), _number(market["close"]), abs_tol=0.0011
                )
            ):
                bad.append(
                    {
                        "ts_code": code,
                        "daily_basic_close": row["close"],
                        "daily_close": market["close"],
                    }
                )
        if bad:
            issues.append(
                _cross_partition_issue(
                    dataset="daily_basic",
                    partition=basics.get(current),
                    day=current,
                    rule_id="daily_basic_daily_match_v1",
                    count=len(bad),
                    samples=bad[:10],
                    severity="WARNING",
                    message="每日指标与同日日线覆盖或收盘价不一致",
                )
            )
    return issues


def _check_daily_and_stk_limit(root: Path, *, through: date, start: date | None) -> list[Issue]:
    daily = _partition_index(root, "daily", through=through, start=start)
    limits = _partition_index(root, "stk_limit", through=through, start=start)
    issues: list[Issue] = []
    for current in sorted(set(daily) & set(limits)):
        daily_rows = _rows_by_code(daily[current])
        limit_rows = _rows_by_code(limits[current])
        bad: list[dict[str, object]] = []
        for code in daily_rows.keys() & limit_rows.keys():
            market, limit = daily_rows[code], limit_rows[code]
            required = (
                market["pre_close"],
                market["high"],
                market["low"],
                limit["pre_close"],
                limit["up_limit"],
                limit["down_limit"],
            )
            if not all(_finite_number(value) for value in required):
                continue
            if (
                not math.isclose(
                    _number(market["pre_close"]), _number(limit["pre_close"]), abs_tol=0.011
                )
                or _number(market["high"]) > _number(limit["up_limit"]) + 0.011
                or _number(market["low"]) < _number(limit["down_limit"]) - 0.011
            ):
                bad.append(
                    {
                        "ts_code": code,
                        "daily": {
                            "pre_close": market["pre_close"],
                            "high": market["high"],
                            "low": market["low"],
                        },
                        "stk_limit": {
                            "pre_close": limit["pre_close"],
                            "up_limit": limit["up_limit"],
                            "down_limit": limit["down_limit"],
                        },
                    }
                )
        if bad:
            issues.append(
                _cross_partition_issue(
                    dataset="stk_limit",
                    partition=limits.get(current),
                    day=current,
                    rule_id="stk_limit_daily_match_v1",
                    count=len(bad),
                    samples=bad[:10],
                    severity="WARNING",
                    message="涨跌停价格与同日日线不一致",
                )
            )
    return issues


def _check_daily_and_suspend(root: Path, *, through: date, start: date | None) -> list[Issue]:
    daily = _partition_index(root, "daily", through=through, start=start)
    suspends = _partition_index(root, "suspend_d", through=through, start=start)
    issues: list[Issue] = []
    for current in sorted(set(daily) & set(suspends)):
        daily_codes = set(_rows_by_code(daily[current]))
        conflicts = [
            row["ts_code"]
            for row in _read_partition_rows(suspends[current])
            if row["suspend_type"] == "S"
            and row["suspend_timing"] is None
            and row["ts_code"] in daily_codes
        ]
        if conflicts:
            issues.append(
                _cross_partition_issue(
                    dataset="suspend_d",
                    partition=suspends.get(current),
                    day=current,
                    rule_id="suspend_daily_conflict_v1",
                    count=len(conflicts),
                    samples=conflicts[:10],
                    severity="ERROR",
                    message="全日停牌证券仍存在同日日线",
                )
            )
    return issues


def _check_daily_and_moneyflow(root: Path, *, through: date, start: date | None) -> list[Issue]:
    daily = _partition_index(root, "daily", through=through, start=start)
    flows = _partition_index(root, "moneyflow", through=through, start=start)
    issues: list[Issue] = []
    for current in sorted(set(flows)):
        daily_codes = set(_rows_by_code(daily.get(current)))
        missing = sorted(set(_rows_by_code(flows[current])) - daily_codes)
        if missing:
            issues.append(
                _cross_partition_issue(
                    dataset="moneyflow",
                    partition=flows.get(current),
                    day=current,
                    rule_id="moneyflow_daily_coverage_v1",
                    count=len(missing),
                    samples=missing[:10],
                    severity="WARNING",
                    message="资金流向没有对应同日日线",
                )
            )
    return issues


def _check_sw_industry_series(root: Path, *, through: date, start: date | None) -> list[Issue]:
    index = _partition_index(root, "sw_industry", through=through, start=start)
    rows = [row for partition in index.values() for row in _read_partition_rows(partition)]
    mappings: dict[str, dict[str, set[str]]] = {level: {} for level in ("l1", "l2", "l3")}
    by_stock: dict[str, list[Mapping[str, object]]] = {}
    for row in rows:
        for level in mappings:
            code = row[f"{level}_code"]
            name = row[f"{level}_name"]
            if isinstance(code, str) and isinstance(name, str):
                mappings[level].setdefault(code, set()).add(name)
        by_stock.setdefault(str(row["ts_code"]), []).append(row)

    bad: list[dict[str, object]] = []
    for level, values in mappings.items():
        for code, names in values.items():
            if len(names) > 1:
                bad.append({"level": level, "code": code, "names": sorted(names)})
    for code, members in by_stock.items():
        current = [row for row in members if row["is_new"] == "Y"]
        if len(current) > 1:
            bad.append({"ts_code": code, "reason": "存在多个当前行业", "count": len(current)})
        ordered = sorted(members, key=lambda row: str(row["in_date"]))
        for previous, following in zip(ordered, ordered[1:], strict=False):
            previous_out = previous["out_date"]
            following_in = following["in_date"]
            if previous_out is None or (
                isinstance(previous_out, date)
                and isinstance(following_in, date)
                and following_in < previous_out
            ):
                bad.append(
                    {
                        "ts_code": code,
                        "reason": "行业有效区间重叠",
                        "previous_in": previous["in_date"],
                        "previous_out": previous_out,
                        "following_in": following_in,
                    }
                )
                break
    if not bad:
        return []
    return [
        _dataset_series_issue(
            "sw_industry",
            "sw_industry_mapping_v1",
            len(bad),
            bad[:10],
            "申万行业代码名称映射、当前成员或有效区间存在冲突",
        )
    ]


def _check_financial_statement(
    dataset: str,
    partition: str,
    partition_date: date | None,
    table: pa.Table,
    value_rule: str,
    equation_rule: str,
    equations: tuple[tuple[str, tuple[tuple[str, int], ...]], ...],
) -> list[Issue]:
    issues: list[Issue] = []
    for row in table.to_pylist():
        key = _row_key(dataset, row)
        refetch = _suggested_refetch(dataset, row, partition_date)
        if (
            not _formal_announcement_valid(row, actual=True)
            or row["update_flag"]
            not in {
                None,
                "0",
                "1",
            }
            or not _report_type_valid(row)
        ):
            issues.append(
                _manual(
                    dataset,
                    partition,
                    key,
                    value_rule,
                    row,
                    (
                        "end_date",
                        "ann_date",
                        "f_ann_date",
                        "report_type",
                        "comp_type",
                        "update_flag",
                    ),
                    refetch,
                )
            )
        failed = [target for target, terms in equations if _equation_mismatch(row, target, terms)]
        if failed:
            fields = tuple(
                dict.fromkeys(
                    name
                    for target, terms in equations
                    if target in failed
                    for name in (target, *(term[0] for term in terms))
                )
            )
            issues.append(
                _manual(
                    dataset,
                    partition,
                    key,
                    equation_rule,
                    row,
                    fields,
                    refetch,
                    severity="WARNING",
                    message=f"{dataset} 核心勾稽关系不一致: {failed}",
                )
            )
    return issues


def _partition_index(
    root: Path, dataset: str, *, through: date, start: date | None
) -> dict[date, _Partition]:
    try:
        partitions = active_partitions(root, dataset)
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return {
        partition.value: partition
        for partition in partitions
        if partition.value is not None
        and partition.value <= through
        and (start is None or partition.value >= start)
        and all(path.is_file() for path in partition.files)
    }


def _read_partition_rows(partition: _Partition) -> list[dict[str, object]]:
    try:
        return [row for path in partition.files for row in pq.ParquetFile(path).read().to_pylist()]
    except (OSError, pa.ArrowException):
        return []


def _rows_by_code(partition: _Partition | None) -> dict[str, dict[str, object]]:
    if partition is None:
        return {}
    return {
        str(row["ts_code"]): row
        for row in _read_partition_rows(partition)
        if row.get("ts_code") is not None
    }


def _cross_partition_issue(
    *,
    dataset: str,
    partition: _Partition | None,
    day: date,
    rule_id: str,
    count: int,
    samples: Sequence[object],
    severity: Literal["ERROR", "WARNING"],
    message: str,
) -> Issue:
    return Issue.create(
        dataset=dataset,
        partition=partition.label if partition else day.isoformat(),
        key={TABLE_PARTITION_BY[dataset]: day},
        rule_id=rule_id,
        severity=severity,
        fix_mode="MANUAL",
        observed={"count": count, "samples": samples},
        suggested=_refetch(day),
        message=message,
    )


def _dataset_series_issue(
    dataset: str,
    rule_id: str,
    count: int,
    samples: list[dict[str, object]],
    message: str,
) -> Issue:
    return Issue.create(
        dataset=dataset,
        partition=None,
        key={"dataset": dataset},
        rule_id=rule_id,
        severity="WARNING",
        fix_mode="MANUAL",
        observed={"count": count, "samples": samples},
        suggested=None,
        message=message,
    )


def _product_mismatch(row: Mapping[str, object], result: str, left: str, right: str) -> bool:
    values = (row[result], row[left], row[right])
    if not all(_finite_number(value) for value in values):
        return False
    actual = _number(values[0])
    expected = _number(values[1]) * _number(values[2])
    return not math.isclose(actual, expected, rel_tol=0.001, abs_tol=1.0)


def _bounds_reversed(lower: object, upper: object) -> bool:
    return _finite_number(lower) and _finite_number(upper) and _number(lower) > _number(upper)


def _quarter_end(value: object) -> bool:
    return isinstance(value, date) and (value.month, value.day) in {
        (3, 31),
        (6, 30),
        (9, 30),
        (12, 31),
    }


def _implemented_dividend_dates_valid(row: Mapping[str, object]) -> bool:
    announced = row["imp_ann_date"]
    record = row["record_date"]
    ex_date = row["ex_date"]
    if not all(isinstance(value, date) for value in (announced, record, ex_date)):
        return False
    assert isinstance(announced, date) and isinstance(record, date) and isinstance(ex_date, date)
    if announced > record or record > ex_date:
        return False
    for name in ("pay_date", "div_listdate"):
        value = row[name]
        if isinstance(value, date) and value < record:
            return False
    return True


def _formal_announcement_valid(row: Mapping[str, object], *, actual: bool) -> bool:
    end = row.get("end_date")
    announced = row.get("f_ann_date") if actual else row.get("ann_date")
    return (
        _quarter_end(end)
        and isinstance(announced, date)
        and isinstance(end, date)
        and announced >= end
    )


def _report_type_valid(row: Mapping[str, object]) -> bool:
    report_type = row.get("report_type")
    comp_type = row.get("comp_type")
    return (
        isinstance(report_type, str)
        and report_type.isdigit()
        and 1 <= int(report_type) <= 12
        and isinstance(comp_type, str)
        and bool(comp_type.strip())
    )


def _growth_mismatch(row: Mapping[str, object], current: str, previous: str, growth: str) -> bool:
    values = (row[current], row[previous], row[growth])
    if not all(_finite_number(value) for value in values) or math.isclose(
        _number(values[1]), 0.0, abs_tol=1e-12
    ):
        return False
    expected = (_number(values[0]) / _number(values[1]) - 1) * 100
    return not math.isclose(_number(values[2]), expected, abs_tol=0.1)


def _equation_mismatch(
    row: Mapping[str, object],
    target: str,
    terms: tuple[tuple[str, int], ...],
) -> bool:
    values = (row[target], *(row[name] for name, _ in terms))
    if not all(_finite_number(value) for value in values):
        return False
    actual = _number(row[target])
    expected = sum(_number(row[name]) * sign for name, sign in terms)
    return not math.isclose(actual, expected, rel_tol=1e-6, abs_tol=1.0)


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
    *,
    severity: Literal["ERROR", "WARNING"] = "ERROR",
    message: str | None = None,
) -> Issue:
    names = tuple(fields)
    return Issue.create(
        dataset=dataset,
        partition=partition,
        key=key,
        rule_id=rule_id,
        severity=severity,
        fix_mode="MANUAL",
        observed={name: row.get(name) for name in names},
        suggested=suggested,
        message=message or f"{dataset} 未通过 {rule_id} 检查",
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
    """为每个数据集的每个检查项生成 PASS/WARN/FAIL 结果。"""
    issue_list = tuple(issues)
    counts = Counter((issue.dataset, issue.rule_id) for issue in issue_list)
    errors = {(issue.dataset, issue.rule_id) for issue in issue_list if issue.severity == "ERROR"}
    results: list[CheckResult] = []
    for dataset in datasets:
        definitions = dict(_COMMON_CHECKS)
        if not full_history:
            definitions.pop("dataset_empty_v1")
        if dataset in _DENSE_MARKET_DATASETS:
            definitions.update(_MISSING_PARTITION_CHECK)
            definitions.update(_CLOSED_MARKET_CHECK)
        definitions.update(_DATASET_CHECKS.get(dataset, {}))
        for check_id, description in definitions.items():
            issue_count = counts[(dataset, check_id)]
            results.append(
                CheckResult(
                    dataset=dataset,
                    check_id=check_id,
                    description=description,
                    status=(
                        "FAIL"
                        if (dataset, check_id) in errors
                        else "WARN"
                        if issue_count
                        else "PASS"
                    ),
                    issue_count=issue_count,
                )
            )
    return tuple(results)
