from __future__ import annotations

import json
import math
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from data_cleaning import (
    detect,
    publish,
    read_report,
    repair,
    repair_instructions,
    write_report,
)
from data_cleaning.__main__ import main
from market_data import DataCatalog, DataReader, DataSourceUnavailableError, SourceConfig
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


def test_detect_cli_prints_every_check_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output = tmp_path / "issues.jsonl"
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
            "--output",
            str(output),
        ],
    )

    with pytest.raises(SystemExit) as stopped:
        main()

    assert stopped.value.code == 0
    printed = capsys.readouterr().out
    assert "[通过] manifest_v1 - Manifest 格式正确" in printed
    assert "[通过] daily_ohlc_v1 - 开高低收关系正确" in printed


def test_detect_auto_fixes_close_when_change_and_pct_agree(tmp_path: Path) -> None:
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
        ("daily_close_consistency_v1", "AUTO_FIX")
    ]
    assert report.issues[0].suggested == {"values": {"close": 86.86}}
    assert repair_instructions(report)[0].action == "PATCH"


def test_detect_groups_partition_wide_stk_limit_gap(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        store.write(
            "stk_limit",
            _table(
                "stk_limit",
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 7, 23),
                    "pre_close": None,
                    "up_limit": 11.0,
                    "down_limit": 9.0,
                },
                {
                    "ts_code": "000002.SZ",
                    "trade_date": date(2024, 7, 23),
                    "pre_close": None,
                    "up_limit": 22.0,
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


def test_repair_refetches_only_the_affected_dataset_and_day(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", _daily(close=12.0, high=11.0)))
    calls: list[tuple[str, date, date]] = []

    def refetch(dataset: str, start: date, end: date) -> int:
        calls.append((dataset, start, end))
        with TushareDataStore(tmp_path) as store:
            return store.write("daily", _table("daily", _daily()))

    detected = detect(tmp_path, through=date(2024, 1, 2), datasets=("daily",))
    report = repair(
        tmp_path,
        report=detected,
        refetch=refetch,
    )

    assert calls == [("daily", date(2024, 1, 2), date(2024, 1, 2))]
    assert report.issues == ()


def test_repair_maps_each_issue_to_an_explicit_action(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", _daily(close=12.0, high=11.0)))
    report = detect(tmp_path, through=date(2024, 1, 2), datasets=("daily",))

    instructions = repair_instructions(report)

    assert [(item.rule_id, item.action) for item in instructions] == [("daily_ohlc_v1", "REFETCH")]


def test_repair_cli_reads_report_and_prints_automatic_patch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data = tmp_path / "data"
    detected_path = tmp_path / "detected.jsonl"
    repaired_path = tmp_path / "repaired.jsonl"
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
            "--output",
            str(repaired_path),
        ],
    )

    with pytest.raises(SystemExit) as stopped:
        main()

    assert stopped.value.code == 0
    printed = capsys.readouterr().out
    assert "[自动补丁] daily daily_close_consistency_v1" in printed
    assert "待人工干预：0" in printed
    assert read_report(repaired_path).issues[0].fix_mode == "AUTO_FIX"


def test_repair_rejects_a_stale_detection_report(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", _daily(close=12.0, high=11.0)))
    report = detect(tmp_path, through=date(2024, 1, 2), datasets=("daily",))
    with TushareDataStore(tmp_path) as store:
        store.write("daily", _table("daily", _daily()))

    with pytest.raises(ValueError, match="Manifest 不匹配"):
        repair(tmp_path, report=report)


def test_publish_applies_finite_float_auto_fix(tmp_path: Path) -> None:
    source = tmp_path / "source"
    output = tmp_path / "published"
    with TushareDataStore(source) as store:
        store.write("daily", _table("daily", _daily(ah_amount=math.nan)))
    report = detect(source, through=date(2024, 1, 2), datasets=("daily",))

    release = publish(source, output, report=report, release_id="v1")

    state = json.loads((release / "release.json").read_text(encoding="utf-8"))
    assert state["datasets"]["daily"]["status"] == "AVAILABLE"
    assert state["datasets"]["daily"]["auto_fixes"] == 1
    with DataCatalog(tushare_root=output / "current", qmt_root=tmp_path / "qmt") as catalog:
        row = catalog.connection.execute("SELECT ah_amount FROM tushare.daily").fetchone()
    assert row == (None,)


def test_publish_blocks_only_dataset_with_unresolved_manual_issue(tmp_path: Path) -> None:
    source = tmp_path / "source"
    output = tmp_path / "published"
    with TushareDataStore(source) as store:
        store.write("daily", _table("daily", _daily()))
        store.write(
            "adj_factor",
            _table(
                "adj_factor",
                {"ts_code": "000001.SZ", "trade_date": date(2024, 1, 2), "adj_factor": 0.0},
            ),
        )
    report = detect(
        source,
        through=date(2024, 1, 2),
        datasets=("daily", "adj_factor"),
    )

    release = publish(source, output, report=report, release_id="v1")
    state = json.loads((release / "release.json").read_text(encoding="utf-8"))

    assert state["datasets"]["daily"]["status"] == "AVAILABLE"
    assert state["datasets"]["adj_factor"]["status"] == "UNAVAILABLE"
    with DataCatalog(tushare_root=output / "current", qmt_root=tmp_path / "qmt") as catalog:
        catalog.require_available("tushare", "daily")
        with pytest.raises(DataSourceUnavailableError, match="adj_factor"):
            catalog.require_available("tushare", "adj_factor")
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"corporate_actions.adjustment_factors": "tushare"}),
        )
        with pytest.raises(DataSourceUnavailableError, match="adj_factor"):
            reader.at(
                datetime(2024, 1, 3, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
            ).corporate_actions.adjustment_factors(symbols=("000001.SZ",))


def test_publish_applies_patch_only_when_expected_value_matches(tmp_path: Path) -> None:
    source = tmp_path / "source"
    output = tmp_path / "published"
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
                "values": {"close": 10.5},
                "reason": "交易所历史行情核对",
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    release = publish(
        source,
        output,
        report=report,
        decisions_path=decisions,
        release_id="v1",
    )

    state = json.loads((release / "release.json").read_text(encoding="utf-8"))
    assert state["datasets"]["daily"]["status"] == "AVAILABLE"
    assert state["datasets"]["daily"]["manual_patches"] == 1
    with DataCatalog(tushare_root=output / "current", qmt_root=tmp_path / "qmt") as catalog:
        assert catalog.connection.execute("SELECT close FROM tushare.daily").fetchone() == (10.5,)
