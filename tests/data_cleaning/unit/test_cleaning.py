from __future__ import annotations

import json
import math
import sys
from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

from data_cleaning import (
    Decision,
    DetectionReport,
    detect,
    read_report,
    record_detection,
    repair,
    repair_instructions,
    rollback,
    source_fingerprint,
    write_report,
)
from data_cleaning.__main__ import _print_repair_plan, main
from market_data import DataCatalog, DataSourceUnavailableError
from tushare_data import TABLE_SCHEMAS, TushareDataStore


def _table(dataset: str, *rows: dict[str, object]) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=TABLE_SCHEMAS[dataset])


def _daily(
    *,
    close: float = 10.5,
    high: float = 11.0,
    amount: float = 100.0,
    ah_amount: float | None = None,
):
    return {
        "ts_code": "000001.SZ",
        "trade_date": date(2024, 1, 2),
        "open": 10.0,
        "high": high,
        "low": 9.5,
        "close": close,
        "pre_close": 10.0,
        "change": close - 10.0,
        "pct_chg": (close / 10.0 - 1) * 100,
        "vol": 1000.0,
        "amount": amount,
        "ah_amount": ah_amount,
    }


def test_detect_reports_refetchable_daily_business_error(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", _daily(close=12.0, high=11.0)))

    report = detect(tmp_path, through=date(2024, 1, 2), datasets=("daily",))

    assert [issue.rule_id for issue in report.issues] == ["daily_ohlc_v1"]
    assert report.issues[0].suggested == {
        "action": "REFETCH",
        "start_date": "2024-01-02",
        "end_date": "2024-01-02",
    }
    checks = {check.check_id: check for check in report.checks}
    assert checks["daily_ohlc_v1"].status == "FAIL"
    assert checks["daily_ohlc_v1"].issue_count == 1
    assert checks["daily_range_v1"].status == "PASS"
    assert checks["schema_v1"].status == "PASS"


def test_detection_report_preserves_each_check_result(tmp_path: Path) -> None:
    data = tmp_path / "data"
    output = tmp_path / "issues.jsonl"
    with TushareDataStore(data) as store:
        store.write("daily", _table("daily", _daily()))

    original = detect(
        data,
        through=date(2024, 1, 2),
        start=date(2024, 1, 2),
        datasets=("daily",),
    )
    write_report(original, output)
    restored = read_report(output)

    assert restored.checks == original.checks
    records = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert any(record["kind"] == "check" for record in records)


def test_detect_cli_prints_a_concise_dataset_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with TushareDataStore(tmp_path / "data") as store:
        store.write("daily", _table("daily", _daily()))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "data-cleaning",
            "detect",
            "--input",
            str(tmp_path / "data"),
            "--datasets",
            "daily",
            "--start",
            "2024-01-02",
            "--through",
            "2024-01-02",
        ],
    )

    with pytest.raises(SystemExit) as stopped:
        main()

    assert stopped.value.code == 0
    printed = capsys.readouterr().out
    assert "[通过] daily: 1 行" in printed
    assert "0 错误，0 告警" in printed
    assert "manifest_v1" not in printed
    assert len(list((tmp_path / "data" / "_quality" / "detections").glob("*.jsonl"))) == 1


def test_detect_refetches_inconsistent_close_instead_of_rewriting_price(tmp_path: Path) -> None:
    row = _daily(close=86.80, high=94.88)
    row.update(
        {
            "open": 93.0,
            "low": 85.91,
            "pre_close": 91.89,
            "change": -5.03,
            "pct_chg": -5.4739,
        }
    )
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", row))

    report = detect(tmp_path, through=date(2024, 1, 2), datasets=("daily",))

    assert [(issue.rule_id, issue.fix_mode) for issue in report.issues] == [
        ("daily_arithmetic_v1", "MANUAL")
    ]
    assert repair_instructions(report)[0].action == "REFETCH"
    assert not report.passed


def test_missing_daily_pre_close_is_allowed_for_the_first_local_bar(tmp_path: Path) -> None:
    row = _daily()
    row["pre_close"] = None
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", row))

    report = detect(tmp_path, through=date(2024, 1, 2), datasets=("daily",))

    assert report.issues == ()
    assert report.passed


def test_detect_groups_partition_wide_stk_limit_gap(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        store.write(
            "stk_limit",
            _table(
                "stk_limit",
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 7, 23),
                    "pre_close": 10.0,
                    "up_limit": None,
                    "down_limit": 9.0,
                },
                {
                    "ts_code": "000002.SZ",
                    "trade_date": date(2024, 7, 23),
                    "pre_close": 20.0,
                    "up_limit": None,
                    "down_limit": 18.0,
                },
            ),
        )

    report = detect(tmp_path, through=date(2024, 7, 23), datasets=("stk_limit",))

    assert [issue.rule_id for issue in report.issues] == ["stk_limit_partition_missing_v1"]
    assert report.issues[0].suggested == {
        "action": "REFETCH",
        "start_date": "2024-07-23",
        "end_date": "2024-07-23",
    }


def test_stk_limit_allows_historical_zero_pre_close_sentinel(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        store.write(
            "stk_limit",
            _table(
                "stk_limit",
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 7, 23),
                    "pre_close": 0.0,
                    "up_limit": 11.0,
                    "down_limit": 9.0,
                },
            ),
        )

    report = detect(tmp_path, through=date(2024, 7, 23), datasets=("stk_limit",))

    assert report.issues == ()


def test_trade_calendar_accepts_bse_requested_by_the_collector(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        store.write(
            "trade_cal",
            _table(
                "trade_cal",
                *(
                    {
                        "exchange": exchange,
                        "cal_date": date(2024, 1, 1),
                        "is_open": 0,
                        "pretrade_date": None,
                    }
                    for exchange in ("SSE", "SZSE", "BSE")
                ),
            ),
        )

    report = detect(
        tmp_path,
        start=date(2024, 1, 1),
        through=date(2024, 1, 1),
        datasets=("trade_cal",),
    )

    assert report.issues == ()


def test_repair_refetches_only_the_affected_dataset_and_day(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", _daily(close=12.0, high=11.0)))
    calls: list[tuple[str, date, date]] = []

    def refetch(dataset: str, start: date, end: date) -> int:
        calls.append((dataset, start, end))
        with TushareDataStore(tmp_path) as store:
            return store.write("daily", _table("daily", _daily()))

    detected = detect(tmp_path, through=date(2024, 1, 2), datasets=("daily",))
    result = repair(
        tmp_path,
        report=detected,
        refetch=refetch,
    )

    assert calls == [("daily", date(2024, 1, 2), date(2024, 1, 2))]
    assert result.report.issues == ()
    assert result.journal_path.is_file()
    journal = json.loads(result.journal_path.read_text(encoding="utf-8"))
    assert journal["operations"] == [
        {
            "kind": "REFETCH",
            "dataset": "daily",
            "start_date": "2024-01-02",
            "end_date": "2024-01-02",
            "rows": 1,
        }
    ]


def test_repair_maps_each_issue_to_an_explicit_action(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", _daily(close=12.0, high=11.0)))
    report = detect(tmp_path, through=date(2024, 1, 2), datasets=("daily",))

    instructions = repair_instructions(report)

    assert [(item.rule_id, item.action) for item in instructions] == [("daily_ohlc_v1", "REFETCH")]


def test_repair_plan_shows_a_manual_decision_as_a_patch(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", _daily(close=12.0, high=11.0)))
    report = detect(tmp_path, through=date(2024, 1, 2), datasets=("daily",))
    issue = report.issues[0]

    _print_repair_plan(
        report,
        {
            issue.issue_id: Decision(
                issue_id=issue.issue_id,
                action="PATCH",
                expected={"close": 12.0},
                values={"close": 10.5},
                reason="交易所历史行情核对",
            )
        },
    )

    printed = capsys.readouterr().out
    assert "[人工补丁] daily daily_ohlc_v1 - 交易所历史行情核对" in printed
    assert "[自动重拉]" not in printed


def test_repair_cli_reads_report_and_prints_automatic_patch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data = tmp_path / "data"
    detected_path = tmp_path / "detected.jsonl"
    row = _daily(ah_amount=math.nan)
    with TushareDataStore(data) as store:
        store.write("daily", _table("daily", row))
    write_report(detect(data, through=date(2024, 1, 2), datasets=("daily",)), detected_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "data-cleaning",
            "repair",
            "--input",
            str(data),
            "--issues",
            str(detected_path),
        ],
    )

    with pytest.raises(SystemExit) as stopped:
        main()

    assert stopped.value.code == 0
    printed = capsys.readouterr().out
    assert "[自动补丁] daily finite_float_v1" in printed
    assert "待人工干预：0" in printed
    with DataCatalog(tushare_root=data, qmt_root=tmp_path / "qmt") as catalog:
        assert catalog.connection.execute("SELECT ah_amount FROM tushare.daily").fetchone() == (
            None,
        )


def test_repair_rejects_a_stale_detection_report(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", _daily(close=12.0, high=11.0)))
    report = detect(tmp_path, through=date(2024, 1, 2), datasets=("daily",))
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", _daily()))

    with pytest.raises(ValueError, match="Manifest 不匹配"):
        repair(tmp_path, report=report)


def test_repair_rebuilds_a_partition_with_a_missing_manifest_file(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", _daily()))
    manifest = next((tmp_path / "daily").rglob("_manifest.json"))
    active_name = json.loads(manifest.read_text(encoding="utf-8"))["files"][0]
    (manifest.parent / active_name).unlink()
    detected = detect(tmp_path, through=date(2024, 1, 2), datasets=("daily",))

    def refetch(_dataset: str, _start: date, _end: date) -> int:
        with TushareDataStore(tmp_path) as store:
            return store.write("daily", _table("daily", _daily()))

    result = repair(tmp_path, report=detected, refetch=refetch)

    assert result.report.issues == ()


def test_repair_automatically_rolls_back_when_refetch_fails(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", _daily(close=12.0, high=11.0)))
    detected = detect(tmp_path, through=date(2024, 1, 2), datasets=("daily",))

    def failing_refetch(_dataset: str, _start: date, _end: date) -> int:
        with TushareDataStore(tmp_path) as store:
            store.write("daily", _table("daily", _daily()))
        raise RuntimeError("upstream failed")

    with pytest.raises(RuntimeError, match="upstream failed"):
        repair(tmp_path, report=detected, refetch=failing_refetch)

    restored = detect(tmp_path, through=date(2024, 1, 2), datasets=("daily",))
    assert [issue.rule_id for issue in restored.issues] == ["daily_ohlc_v1"]
    journal = next((tmp_path / "_quality" / "repairs").glob("*/journal.json"))
    assert json.loads(journal.read_text(encoding="utf-8"))["status"] == "ROLLED_BACK"


def test_repair_applies_finite_float_auto_fix_in_place(tmp_path: Path) -> None:
    source = tmp_path / "source"
    with TushareDataStore(source) as store:
        store.write("daily", _table("daily", _daily(ah_amount=math.nan)))
    report = detect(source, through=date(2024, 1, 2), datasets=("daily",))

    result = repair(source, report=report)

    assert result.report.issues == ()
    with DataCatalog(tushare_root=source, qmt_root=tmp_path / "qmt") as catalog:
        row = catalog.connection.execute("SELECT ah_amount FROM tushare.daily").fetchone()
    assert row == (None,)


def test_rollback_restores_the_partition_before_repair(tmp_path: Path) -> None:
    source = tmp_path / "source"
    with TushareDataStore(source) as store:
        store.write("daily", _table("daily", _daily(ah_amount=math.nan)))
    report = detect(source, through=date(2024, 1, 2), datasets=("daily",))

    result = repair(source, report=report)
    rollback(source, result.repair_id)

    restored = detect(source, through=date(2024, 1, 2), datasets=("daily",))
    assert [(issue.rule_id, issue.fix_mode) for issue in restored.issues] == [
        ("finite_float_v1", "AUTO_FIX")
    ]


def test_repair_applies_manual_patch_only_when_expected_value_matches(tmp_path: Path) -> None:
    source = tmp_path / "source"
    decisions = tmp_path / "decisions.jsonl"
    with TushareDataStore(source) as store:
        store.write("daily", _table("daily", _daily(close=12.0, high=11.0)))
    report = detect(source, through=date(2024, 1, 2), datasets=("daily",))
    issue = report.issues[0]
    decisions.write_text(
        json.dumps(
            {
                "issue_id": issue.issue_id,
                "action": "PATCH",
                "expected": {"close": 12.0},
                "values": {"close": 10.5, "change": 0.5, "pct_chg": 5.0},
                "reason": "交易所历史行情核对",
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    result = repair(
        source,
        report=report,
        decisions_path=decisions,
    )

    assert result.report.issues == ()
    with DataCatalog(tushare_root=source, qmt_root=tmp_path / "qmt") as catalog:
        assert catalog.connection.execute("SELECT close FROM tushare.daily").fetchone() == (10.5,)
    operation = json.loads(result.journal_path.read_text(encoding="utf-8"))["operations"][0]
    assert operation["kind"] == "PATCH"
    assert operation["reason"] == "交易所历史行情核对"


def test_repair_does_not_allow_accept_to_bypass_an_error(tmp_path: Path) -> None:
    source = tmp_path / "source"
    decisions = tmp_path / "decisions.jsonl"
    with TushareDataStore(source) as store:
        store.write("daily", _table("daily", _daily(close=12.0, high=11.0)))
    report = detect(source, through=date(2024, 1, 2), datasets=("daily",))
    decisions.write_text(
        json.dumps(
            {
                "issue_id": report.issues[0].issue_id,
                "action": "ACCEPT",
                "reason": "仅用于证明 ERROR 不能被接受后绕过",
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="只允许 PATCH"):
        repair(source, report=report, decisions_path=decisions)


def test_adj_factor_decrease_is_not_treated_as_corruption(tmp_path: Path) -> None:
    first = _daily(close=10.0)
    second = {
        **_daily(close=21.0, high=21.5),
        "trade_date": date(2024, 1, 3),
        "open": 20.0,
        "low": 19.5,
        "pre_close": 20.0,
        "change": 1.0,
        "pct_chg": 5.0,
    }
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", first, second))
        store.write(
            "adj_factor",
            _table(
                "adj_factor",
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 2),
                    "adj_factor": 2.0,
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 3),
                    "adj_factor": 1.0,
                },
            ),
        )

    report = detect(
        tmp_path,
        through=date(2024, 1, 3),
        datasets=("daily", "adj_factor"),
    )

    assert report.issues == ()
    assert report.passed


def test_market_data_rejects_quality_state_after_manifests_change(tmp_path: Path) -> None:
    datasets = tuple(TABLE_SCHEMAS)
    clean = DetectionReport(
        input_fingerprint=source_fingerprint(tmp_path, datasets),
        through=date(2024, 1, 1),
        start=None,
        datasets=datasets,
        row_counts={dataset: 0 for dataset in datasets},
        checks=(),
        issues=(),
    )
    record_detection(tmp_path, clean)
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", _daily()))

    with (
        DataCatalog(tushare_root=tmp_path, qmt_root=tmp_path / "qmt") as catalog,
        pytest.raises(DataSourceUnavailableError, match="质量检查"),
    ):
        catalog.require_available("tushare", "daily")


def test_missing_adj_factor_is_refetchable_error(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", _daily()))
        store.write(
            "adj_factor",
            _table(
                "adj_factor",
                {
                    "ts_code": "000002.SZ",
                    "trade_date": date(2024, 1, 2),
                    "adj_factor": 1.0,
                },
            ),
        )

    report = detect(
        tmp_path,
        through=date(2024, 1, 2),
        datasets=("daily", "adj_factor"),
    )

    issue = next(
        issue for issue in report.issues if issue.rule_id == "adj_factor_daily_coverage_v1"
    )
    instruction = next(
        item for item in repair_instructions(report) if item.rule_id == issue.rule_id
    )
    assert issue.severity == "ERROR"
    assert instruction.action == "REFETCH"
    assert instruction.dataset == "adj_factor"
    assert not report.passed


def test_daily_basic_close_difference_is_not_treated_as_corruption(tmp_path: Path) -> None:
    basic = {
        "ts_code": "000001.SZ",
        "trade_date": date(2024, 1, 2),
        "close": 10.6,
        "total_share": 100.0,
        "float_share": 80.0,
        "free_share": 60.0,
        "total_mv": 1060.0,
        "circ_mv": 848.0,
    }
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", _daily()))
        store.write("daily_basic", _table("daily_basic", basic))

    report = detect(
        tmp_path,
        through=date(2024, 1, 2),
        datasets=("daily", "daily_basic"),
    )

    assert report.issues == ()
    assert report.passed


def test_income_simplified_equation_is_not_assumed_for_every_report_type(tmp_path: Path) -> None:
    row = {
        "ts_code": "000001.SZ",
        "ann_date": date(2024, 4, 20),
        "f_ann_date": date(2024, 4, 20),
        "end_date": date(2024, 3, 31),
        "report_type": "1",
        "comp_type": "1",
        "end_type": "1",
        "operate_profit": 100.0,
        "non_oper_income": 10.0,
        "non_oper_exp": 5.0,
        "total_profit": 120.0,
        "update_flag": "1",
    }
    with TushareDataStore(tmp_path) as store:
        store.write("income", _table("income", row))

    report = detect(tmp_path, through=date(2024, 12, 31), datasets=("income",))

    assert report.issues == ()
    assert report.passed


def test_fina_indicator_removes_only_an_impossible_superseded_version(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    end_date = date(2026, 6, 30)
    early = {
        "ts_code": "603400.SH",
        "ann_date": date(2026, 4, 22),
        "end_date": end_date,
        "update_flag": "0",
    }
    formal = {
        "ts_code": "603400.SH",
        "ann_date": date(2026, 8, 3),
        "end_date": end_date,
        "update_flag": "1",
    }
    with TushareDataStore(source) as store:
        store.write("fina_indicator", _table("fina_indicator", early, formal))

    report = detect(source, through=end_date, datasets=("fina_indicator",))

    assert len(report.issues) == 1
    assert report.issues[0].fix_mode == "AUTO_FIX"
    assert report.issues[0].suggested == {"delete": True}
    result = repair(source, report=report)
    assert result.report.issues == ()
    with TushareDataStore(source) as store:
        rows = store.read("fina_indicator", end_date).to_pylist()
    assert [(row["ann_date"], row["update_flag"]) for row in rows] == [(date(2026, 8, 3), "1")]

    rollback(source, result.repair_id)
    with TushareDataStore(source) as store:
        restored = store.read("fina_indicator", end_date).to_pylist()
    assert len(restored) == 2


def test_fina_indicator_does_not_delete_an_early_row_without_a_formal_version(
    tmp_path: Path,
) -> None:
    end_date = date(2026, 6, 30)
    with TushareDataStore(tmp_path) as store:
        store.write(
            "fina_indicator",
            _table(
                "fina_indicator",
                {
                    "ts_code": "603400.SH",
                    "ann_date": date(2026, 4, 22),
                    "end_date": end_date,
                    "update_flag": "0",
                },
            ),
        )

    report = detect(tmp_path, through=end_date, datasets=("fina_indicator",))

    assert len(report.issues) == 1
    assert report.issues[0].fix_mode == "MANUAL"
    assert repair_instructions(report)[0].action == "REFETCH"


def test_trade_calendar_detects_a_whole_missing_date(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        for current, previous in (
            (date(2024, 1, 1), None),
            (date(2024, 1, 3), date(2024, 1, 1)),
        ):
            store.write(
                "trade_cal",
                _table(
                    "trade_cal",
                    *(
                        {
                            "exchange": exchange,
                            "cal_date": current,
                            "is_open": 1,
                            "pretrade_date": previous,
                        }
                        for exchange in ("SSE", "SZSE")
                    ),
                ),
            )

    report = detect(
        tmp_path,
        start=date(2024, 1, 1),
        through=date(2024, 1, 3),
        datasets=("trade_cal",),
    )

    issue = next(issue for issue in report.issues if issue.rule_id == "calendar_date_coverage_v1")
    assert issue.key == {"cal_date": "2024-01-02"}
    assert issue.severity == "ERROR"
