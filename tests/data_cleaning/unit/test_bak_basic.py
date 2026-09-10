from __future__ import annotations

from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path
from typing import ParamSpec, TypeVar

import pandas as pd
import pyarrow as pa
import pytest

from data_cleaning import detect, refetch_ranges, repair, repair_instructions, rollback
from data_cleaning.detector import check_bak_basic
from tushare_data import TABLE_SCHEMAS, TushareDataStore, TushareProClient, sync_datasets

DAY = date(2024, 1, 2)
COVERAGE_RULE = "bak_basic_stock_coverage_v1"
_P = ParamSpec("_P")
_R = TypeVar("_R")


def _write(store: TushareDataStore, dataset: str, *rows: dict[str, object]) -> None:
    store.write(dataset, pa.Table.from_pylist(list(rows), schema=TABLE_SCHEMAS[dataset]))


def _stock(code: str = "000001.SZ", **values: object) -> dict[str, object]:
    return {
        "ts_code": code,
        "symbol": code[:6],
        "exchange": {"SZ": "SZSE", "SH": "SSE", "BJ": "BSE"}[code[-2:]],
        "list_date": date(1991, 4, 3),
        "delist_date": None,
        "list_status": "L",
        **values,
    }


def _basic(code: str = "000001.SZ", day: date = DAY, **values: object) -> dict[str, object]:
    return {
        "ts_code": code,
        "trade_date": day,
        "name": "测试股票",
        "float_share": 1.0,
        "total_share": 2.0,
        "total_assets": 3.0,
        "liquid_assets": 1.0,
        "fixed_assets": 1.0,
        "holder_num": 100,
        **values,
    }


def _calendar(store: TushareDataStore, day: date = DAY, **open_flags: int) -> None:
    _write(
        store,
        "trade_cal",
        *(
            {
                "exchange": exchange,
                "cal_date": day,
                "is_open": open_flags.get(exchange, 1),
                "pretrade_date": day - timedelta(days=1),
            }
            for exchange in ("SSE", "SZSE", "BSE")
        ),
    )


def test_coverage_uses_lifecycle_exchange_and_same_day_suspensions(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        _calendar(store, BSE=0)
        _write(
            store,
            "stock_basic",
            _stock(),  # 老股票缺行。
            _stock("000002.SZ", list_date=DAY),  # 上市当日也需要记录。
            _stock("000003.SZ", list_date=DAY + timedelta(days=1)),
            _stock("000004.SZ", delist_date=DAY, list_status="D"),
            _stock("000005.SZ", delist_date=DAY + timedelta(days=1), list_status="D"),
            _stock("000006.SZ", list_status="P"),  # 当前暂停上市不豁免历史。
            _stock("000007.SZ"),
            _stock("000008.SZ"),
            _stock("000009.SZ"),
            _stock("600000.SH"),
            _stock("920001.BJ"),
        )
        _write(store, "bak_basic", _basic("600000.SH"))
        _write(
            store,
            "suspend_d",
            {"ts_code": "000007.SZ", "trade_date": DAY, "suspend_type": "S"},
            {
                "ts_code": "000008.SZ",
                "trade_date": DAY,
                "suspend_type": "S",
                "suspend_timing": "09:30-10:00",
            },
            {"ts_code": "000009.SZ", "trade_date": DAY, "suspend_type": "R"},
        )
        # 更早的 S 不会被当成当前日期仍停牌。
        _write(
            store,
            "suspend_d",
            {"ts_code": "000001.SZ", "trade_date": date(2023, 12, 29), "suspend_type": "S"},
        )

    report = detect(tmp_path, datasets=("bak_basic",), start=DAY, through=DAY)

    assert len(report.issues) == 1
    issue = report.issues[0]
    assert issue.rule_id == COVERAGE_RULE
    assert issue.severity == "ERROR"
    assert issue.observed == {
        "count": 5,
        "samples": ["000001.SZ", "000002.SZ", "000005.SZ", "000006.SZ", "000009.SZ"],
    }
    assert refetch_ranges(report) == (("bak_basic", DAY, DAY),)
    assert repair_instructions(report)[0].action == "REFETCH"
    assert (
        next(check for check in report.checks if check.check_id == COVERAGE_RULE).status == "FAIL"
    )


@pytest.mark.parametrize("present", [False, True])
def test_whole_missing_day_is_checked_even_when_no_bak_basic_exists(
    tmp_path: Path,
    present: bool,
) -> None:
    with TushareDataStore(tmp_path) as store:
        _calendar(store)
        _write(store, "stock_basic", _stock())
        if present:
            _write(store, "bak_basic", _basic(day=DAY + timedelta(days=1)))
    report = detect(tmp_path, datasets=("bak_basic",), start=DAY, through=DAY)
    assert [issue.rule_id for issue in report.issues] == [COVERAGE_RULE]
    assert report.row_counts == {"bak_basic": 0}
    assert refetch_ranges(report) == (("bak_basic", DAY, DAY),)


@pytest.mark.parametrize("closed,suspended", [(True, False), (False, True)])
def test_closed_or_suspended_day_does_not_require_a_partition(
    tmp_path: Path,
    closed: bool,
    suspended: bool,
) -> None:
    with TushareDataStore(tmp_path) as store:
        _calendar(store, SZSE=int(not closed))
        _write(store, "stock_basic", _stock())
        if suspended:
            _write(
                store, "suspend_d", {"ts_code": "000001.SZ", "trade_date": DAY, "suspend_type": "S"}
            )
    report = detect(tmp_path, datasets=("bak_basic",), start=DAY, through=DAY)
    assert report.passed
    assert report.issues == ()


def test_coverage_checks_missing_days_inside_the_requested_range(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        _write(store, "stock_basic", _stock())
        for offset in range(5):
            _calendar(store, DAY + timedelta(days=offset))
        _write(store, "bak_basic", _basic())
    report = detect(
        tmp_path,
        datasets=("bak_basic",),
        start=DAY + timedelta(days=1),
        through=DAY + timedelta(days=2),
    )
    assert len(report.issues) == 2
    assert refetch_ranges(report) == (
        ("bak_basic", DAY + timedelta(days=1), DAY + timedelta(days=2)),
    )


@pytest.mark.parametrize(
    "field",
    ["float_share", "total_share", "total_assets", "liquid_assets", "fixed_assets", "holder_num"],
)
def test_negative_bak_basic_counts_and_assets_require_refetch(field: str) -> None:
    issues = check_bak_basic("trade_date=value%3A2024-01-02", DAY, [{**_basic(), field: -1}])
    assert [issue.rule_id for issue in issues] == ["bak_basic_range_v1"]
    assert issues[0].suggested == {
        "action": "REFETCH",
        "start_date": "2024-01-02",
        "end_date": "2024-01-02",
    }


@pytest.mark.parametrize("values", [{"name": None}, {"name": " "}, {"ts_code": "invalid"}])
def test_bak_basic_rejects_missing_identity(values: dict[str, object]) -> None:
    issues = check_bak_basic("trade_date=value%3A2024-01-02", DAY, [{**_basic(), **values}])
    assert [issue.rule_id for issue in issues] == ["bak_basic_value_v1"]


@pytest.mark.parametrize("listing", [None, date(2025, 1, 1)])
def test_bak_basic_allows_losses_zero_values_and_unknown_or_future_listing(
    listing: date | None,
) -> None:
    row = _basic(
        list_date=listing,
        pe=-1.0,
        pb=-1.0,
        eps=-1.0,
        bvps=-1.0,
        reserved=-1.0,
        undp=-1.0,
        rev_yoy=-200.0,
        gpr=-200.0,
        total_assets=0.0,
        holder_num=None,
    )
    assert check_bak_basic("trade_date=value%3A2024-01-02", DAY, [row]) == []


def test_missing_references_do_not_silently_pass_coverage(tmp_path: Path) -> None:
    with TushareDataStore(tmp_path) as store:
        _write(store, "bak_basic", _basic())
    report = detect(tmp_path, datasets=("bak_basic",), start=DAY, through=DAY)
    assert [issue.rule_id for issue in report.issues] == ["bak_basic_reference_v1"]
    assert report.issues[0].observed == {"missing_references": ["stock_basic", "trade_cal"]}
    assert not report.passed
    assert repair_instructions(report)[0].action == "MANUAL"


@pytest.mark.parametrize("reference", ["stock_basic", "trade_cal", "suspend_d"])
def test_reference_change_invalidates_bak_basic_only_report(tmp_path: Path, reference: str) -> None:
    with TushareDataStore(tmp_path) as store:
        _calendar(store)
        _write(store, "stock_basic", _stock())
        report = detect(tmp_path, datasets=("bak_basic",), start=DAY, through=DAY)
        if reference == "stock_basic":
            _write(store, reference, _stock("000002.SZ"))
        elif reference == "trade_cal":
            _calendar(store, SZSE=0)
        else:
            _write(
                store, reference, {"ts_code": "000001.SZ", "trade_date": DAY, "suspend_type": "S"}
            )
    with pytest.raises(ValueError, match="Manifest 不匹配"):
        repair(tmp_path, report=report, refetch=lambda *_: 0)


class _ImmediateExecutor:
    def call(self, function: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs) -> _R:
        return function(*args, **kwargs)


@pytest.mark.parametrize("failure", [None, "empty", "exception"])
def test_missing_history_repair_forces_sync_rechecks_and_can_rollback(
    tmp_path: Path,
    failure: str | None,
) -> None:
    with TushareDataStore(tmp_path) as store:
        _calendar(store)
        _write(store, "stock_basic", _stock(), _stock("000002.SZ"))
        _write(store, "bak_basic", _basic("000002.SZ"))
        store._mark_sync_all_completed("bak_basic", DAY, DAY)
    report = detect(tmp_path, datasets=("bak_basic",), start=DAY, through=DAY)
    calls: list[tuple[str, str]] = []

    class Api:
        def query(self, api_name: str, fields: str = "", **kwargs: object) -> pd.DataFrame:
            assert api_name == "bak_basic"
            assert kwargs["trade_date"] == "20240102"
            calls.append((api_name, str(kwargs["trade_date"])))
            if failure == "exception":
                raise RuntimeError("upstream failure")
            rows = [] if failure == "empty" else [_basic(), _basic("000002.SZ")]
            return pd.DataFrame(rows, columns=pd.Index(fields.split(",")))

    client = TushareProClient(Api(), _ImmediateExecutor())
    with TushareDataStore(tmp_path) as store:

        def refetch(dataset: str, start: date, end: date) -> int:
            return sync_datasets(client, store, (dataset,), start, end, force=True)[dataset]

        if failure == "exception":
            with pytest.raises(RuntimeError, match="upstream failure"):
                repair(tmp_path, report=report, refetch=refetch)
        else:
            result = repair(tmp_path, report=report, refetch=refetch)
            assert result.report.passed == (failure is None)
            if failure is None:
                assert store.read("bak_basic", DAY).num_rows == 2
            else:
                assert [issue.rule_id for issue in result.report.issues] == [COVERAGE_RULE]
            rollback(tmp_path, result.repair_id)
    with TushareDataStore(tmp_path) as store:
        assert store.read("bak_basic", DAY).column("ts_code").to_pylist() == ["000002.SZ"]
        assert store._sync_all_completed_ranges("bak_basic") == [(DAY, DAY)]
    assert calls == [("bak_basic", "20240102")]
