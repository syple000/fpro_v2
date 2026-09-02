"""读取检测报告，将每个检查问题映射为补丁、重拉或人工处理。"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Literal

from data_cleaning.detector import detect, source_fingerprint
from data_cleaning.models import DetectionReport, Issue

Refetch = Callable[[str, date, date], int]
RepairAction = Literal["PATCH", "REFETCH", "MANUAL"]


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


def repair(
    root: str | Path,
    *,
    report: DetectionReport,
    refetch: Refetch | None = None,
    max_rounds: int = 2,
) -> DetectionReport:
    """读取一份检测报告，重拉可恢复问题，并按原范围复检。"""
    if max_rounds < 1:
        raise ValueError("max_rounds 必须大于等于 1")
    current_fingerprint = source_fingerprint(root, report.datasets)
    if current_fingerprint != report.input_fingerprint:
        raise ValueError("检测报告与当前 Manifest 不匹配，请重新执行 detect")

    current = report
    for _ in range(max_rounds):
        ranges = refetch_ranges(current)
        if not ranges:
            break
        if refetch is None:
            raise ValueError("检测报告包含需要重拉的问题，但没有提供 refetch")
        previous = tuple(issue.issue_id for issue in current.issues)
        for dataset, range_start, range_end in ranges:
            refetch(dataset, range_start, range_end)
        current = detect(
            root,
            through=current.through,
            datasets=current.datasets,
            start=current.start,
        )
        if tuple(issue.issue_id for issue in current.issues) == previous:
            break
    return current


def repair_instructions(report: DetectionReport) -> tuple[RepairInstruction, ...]:
    """按规则将检测问题逐一转换为修复方式。"""
    return tuple(
        _REPAIRERS.get(issue.rule_id, repair_unknown_issue)(issue) for issue in report.issues
    )


def refetch_ranges(report: DetectionReport) -> tuple[tuple[str, date, date], ...]:
    """从逐项修复方式中提取重拉日期，并合并连续区间。"""
    days: dict[str, set[date]] = defaultdict(set)
    for instruction in repair_instructions(report):
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


def repair_dataset_empty(issue: Issue) -> RepairInstruction:
    """空数据集没有可推导的重拉范围，交给人工。"""
    return _manual(issue, "数据集为空，需人工确认采集范围后补数")


def repair_manifest(issue: Issue) -> RepairInstruction:
    """Manifest 损坏时不猜测文件归属，交给人工。"""
    return _manual(issue, "Manifest 格式损坏，需人工恢复或重建分区")


def repair_missing_manifest_file(issue: Issue) -> RepairInstruction:
    """Manifest 引用文件丢失时交给人工。"""
    return _manual(issue, "Manifest 引用文件丢失，需恢复文件或重建分区")


def repair_unreadable_parquet(issue: Issue) -> RepairInstruction:
    """Parquet 无法读取时交给人工。"""
    return _manual(issue, "Parquet 损坏，需从原始数据恢复或重建分区")


def repair_schema(issue: Issue) -> RepairInstruction:
    """Schema 不一致时交给人工，避免猜测字段转换。"""
    return _manual(issue, "Schema 不一致，需核对版本并重建分区")


def repair_partition_value(issue: Issue) -> RepairInstruction:
    """分区值冲突时交给人工，避免把记录移到错误日期。"""
    return _manual(issue, "分区路径与记录值冲突，需核对后重建分区")


def repair_duplicate_key(issue: Issue) -> RepairInstruction:
    """主键重复时按问题建议定向重拉。"""
    return _refetch_or_manual(issue, "重拉该数据集与日期，由采集层按主键去重")


def repair_required_value(issue: Issue) -> RepairInstruction:
    """必填字段缺失时定向重拉。"""
    return _refetch_or_manual(issue, "重拉该数据集与日期")


def repair_finite_float(issue: Issue) -> RepairInstruction:
    """非关键浮点转为 null；关键浮点定向重拉。"""
    if issue.fix_mode == "AUTO_FIX":
        return _patch(issue, "发布时将非有限的可空浮点值写为 null")
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


def repair_daily_close(issue: Issue) -> RepairInstruction:
    """涨跌额和涨跌幅指向同一值时，记录确定性收盘价补丁。"""
    return _patch(issue, "发布时使用涨跌额和涨跌幅共同推导的收盘价")


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
    if issue.severity == "WARNING":
        return _manual(issue, f"{_BUSINESS_REPAIR_MESSAGES[issue.rule_id]}；告警不自动改值")
    return _refetch_or_manual(issue, _BUSINESS_REPAIR_MESSAGES[issue.rule_id])


def repair_review_warning(issue: Issue) -> RepairInstruction:
    """没有唯一正确值和安全重拉区间的告警交给人工复核。"""
    return _manual(issue, _BUSINESS_REPAIR_MESSAGES[issue.rule_id])


def repair_unknown_issue(issue: Issue) -> RepairInstruction:
    """未登记规则不自动处理。"""
    return _manual(issue, f"规则 {issue.rule_id} 没有登记自动修复方式")


def _patch(issue: Issue, message: str) -> RepairInstruction:
    suggested = issue.suggested or {}
    values = suggested.get("values")
    if not isinstance(values, Mapping) or not values:
        return _manual(issue, "检测结果没有提供确定修正值")
    return RepairInstruction(
        issue_id=issue.issue_id,
        dataset=issue.dataset,
        rule_id=issue.rule_id,
        action="PATCH",
        details={"values": dict(values)},
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
    "daily_close_consistency_v1": repair_daily_close,
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
    "daily_basic_share_order_v1": "重拉 daily_basic 对应交易日后复核股本口径",
    "daily_basic_market_value_v1": "重拉 daily_basic 对应交易日后复核单位与舍入",
    "daily_basic_daily_match_v1": "重拉 daily_basic 对应交易日后与 daily 复核",
    "adj_factor_daily_coverage_v1": "重拉 adj_factor 对应交易日",
    "adj_factor_continuity_v1": "重拉 adj_factor 对应交易日后复核公司行动",
    "suspend_value_v1": "重拉 suspend_d 对应交易日",
    "suspend_daily_conflict_v1": "重拉 suspend_d 对应交易日后与 daily 复核",
    "stk_limit_daily_match_v1": "重拉 stk_limit 对应交易日后与 daily 复核",
    "stock_st_value_v1": "重拉 stock_st 对应交易日",
    "moneyflow_range_v1": "重拉 moneyflow 对应交易日",
    "moneyflow_daily_coverage_v1": "重拉 moneyflow 对应交易日后与 daily 复核",
    "dividend_value_v1": "重拉 dividend 对应公告日期",
    "dividend_stock_ratio_v1": "重拉 dividend 对应公告日期",
    "dividend_date_order_v1": "重拉 dividend 对应公告日期后复核实施公告",
    "forecast_value_v1": "重拉 forecast 对应公告日期",
    "forecast_range_v1": "重拉 forecast 对应公告日期",
    "forecast_date_order_v1": "重拉 forecast 对应公告日期",
    "express_value_v1": "重拉 express 对应公告日期",
    "express_audit_flag_v1": "重拉 express 对应公告日期后复核审计标识口径",
    "express_growth_v1": "重拉 express 对应公告日期后复核修订口径",
    "fina_audit_value_v1": "重拉 fina_audit 对应公告日期后复核审计信息",
    "income_value_v1": "重拉 income 对应公告日期",
    "income_equation_v1": "重拉 income 对应公告日期后复核报表口径",
    "balancesheet_value_v1": "重拉 balancesheet 对应公告日期",
    "balancesheet_equation_v1": "重拉 balancesheet 对应公告日期后复核报表口径",
    "cashflow_value_v1": "重拉 cashflow 对应公告日期",
    "cashflow_equation_v1": "重拉 cashflow 对应公告日期后复核报表口径",
    "fina_indicator_value_v1": "重拉 fina_indicator 对应公告日期",
    "sw_industry_value_v1": "重拉 sw_industry 对应纳入日期后复核行业历史",
    "calendar_date_coverage_v1": "重拉 trade_cal 缺失自然日",
    "calendar_pretrade_v1": "重拉 trade_cal 对应日期后复核上一交易日",
    "adj_factor_decrease_v1": "人工核对公司行动、历史代码或因子历史修订",
    "adj_factor_without_daily_v1": "人工区分停牌、退市、历史代码和异常冗余",
    "sw_industry_mapping_v1": "人工核对行业更名、换版和有效区间",
}

for _rule_id in _BUSINESS_REPAIR_MESSAGES:
    _REPAIRERS[_rule_id] = (
        repair_review_warning
        if _rule_id
        in {"adj_factor_decrease_v1", "adj_factor_without_daily_v1", "sw_industry_mapping_v1"}
        else repair_business_value
    )
