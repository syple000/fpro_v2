from __future__ import annotations

import json
import math
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from data_cleaning import detect, publish, repair
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

    report = repair(
        tmp_path,
        through=date(2024, 1, 2),
        datasets=("daily",),
        refetch=refetch,
    )

    assert calls == [("daily", date(2024, 1, 2), date(2024, 1, 2))]
    assert report.issues == ()


def test_publish_applies_finite_float_auto_fix(tmp_path: Path) -> None:
    source = tmp_path / "source"
    output = tmp_path / "published"
    with TushareDataStore(source) as store:
        store.write("daily", _table("daily", _daily(ah_amount=math.nan)))
    report = detect(source, through=date(2024, 1, 2), datasets=("daily",))

    release = publish(source, output, report=report, release_id="v1")

    state = json.loads((release / "release.json").read_text(encoding="utf-8"))
    assert state["datasets"]["daily"]["status"] == "AVAILABLE"
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
    with DataCatalog(tushare_root=output / "current", qmt_root=tmp_path / "qmt") as catalog:
        assert catalog.connection.execute("SELECT close FROM tushare.daily").fetchone() == (10.5,)
