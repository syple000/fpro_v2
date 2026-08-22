from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import ParamSpec, TypeVar, cast
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pytest

import tushare_data.sync as sync_module
from fpro_common import datetime_to_utc_us, utc_us_to_datetime
from tushare_data import (
    SOURCE_FIELDS,
    TABLE_SCHEMAS,
    TushareDataStore,
    TushareProClient,
    sync_daily,
    sync_dividend,
    sync_fina_audit,
    sync_fina_indicator,
    sync_income,
    sync_suspend_d,
    sync_sw_industry,
)
from tushare_data.schemas import DATE_FIELDS, TABLE_PARTITION_BY, TABLE_SORT_BY

_P = ParamSpec("_P")
_R = TypeVar("_R")
QueryResponder = Callable[[str, str, dict[str, object]], pd.DataFrame]


class ImmediateExecutor:
    def call(
        self,
        function: Callable[_P, _R],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _R:
        return function(*args, **kwargs)


class RecordingDataApi:
    def __init__(self, responder: QueryResponder) -> None:
        self.responder = responder
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    def query(
        self,
        api_name: str,
        fields: str = "",
        **kwargs: object,
    ) -> pd.DataFrame:
        arguments = dict(kwargs)
        self.calls.append((api_name, fields, arguments))
        return self.responder(api_name, fields, arguments)


def _client(responder: QueryResponder) -> tuple[TushareProClient, RecordingDataApi]:
    api = RecordingDataApi(responder)
    return TushareProClient(api, ImmediateExecutor()), api


def _empty(fields: str) -> pd.DataFrame:
    return pd.DataFrame(columns=pd.Index(fields.split(",")))


def _calendar_frame(fields: str, arguments: dict[str, object]) -> pd.DataFrame:
    start = str(arguments["start_date"])
    end = str(arguments["end_date"])
    exchange = str(arguments["exchange"])
    rows = [
        {
            "exchange": exchange,
            "cal_date": day.strftime("%Y%m%d"),
            "is_open": 1,
            "pretrade_date": day.strftime("%Y%m%d"),
        }
        for day in pd.date_range(start, end, freq="D")
    ]
    return pd.DataFrame(rows, columns=pd.Index(fields.split(",")))


def _record(dataset: str, ts_code: str, visible_date: str) -> dict[str, object]:
    schema = TABLE_SCHEMAS[dataset]
    result: dict[str, object] = {}
    for name in SOURCE_FIELDS[dataset]:
        if name == "ts_code":
            result[name] = ts_code
        elif name in DATE_FIELDS:
            result[name] = "20231231"
        elif name in schema.names and pa.types.is_string(schema.field(name).type):
            result[name] = "1"
        else:
            result[name] = 1.0

    visible_fields = sync_module.VISIBILITY_RULES[dataset][0]
    result[visible_fields[0]] = visible_date
    return result


def _market_responder(
    api_name: str,
    fields: str,
    arguments: dict[str, object],
) -> pd.DataFrame:
    if api_name == "trade_cal":
        return _calendar_frame(fields, arguments)
    if api_name == "daily":
        trade_date = str(arguments["trade_date"])
        rows = [
            _record("daily", "000001.SZ", trade_date),
            _record("daily", "600000.SH", trade_date),
        ]
        return pd.DataFrame(rows, columns=pd.Index(fields.split(",")))
    return _empty(fields)


def test_daily_fetches_full_market_and_partitions_by_visible_date(
    tmp_path: Path,
) -> None:
    pro, api = _client(_market_responder)
    with TushareDataStore(tmp_path) as store:
        assert sync_daily(pro, store, "20240102", "20240103") == 4
        calendar = store.read("trade_cal", date(2024, 1, 2)).to_pylist()
        first_day = store.read("daily", date(2024, 1, 2)).to_pylist()
        first_stock = store.read(
            "daily",
            [date(2024, 1, 2), date(2024, 1, 3)],
            ts_code="000001.SZ",
        ).to_pylist()

    daily_calls = [arguments for name, _, arguments in api.calls if name == "daily"]
    calendar_calls = [arguments for name, _, arguments in api.calls if name == "trade_cal"]
    assert [call["trade_date"] for call in daily_calls] == ["20240102", "20240103"]
    assert [call["exchange"] for call in calendar_calls] == ["SSE", "SZSE", "BSE"]
    assert all("ts_code" not in call for call in daily_calls)
    assert [row["ts_code"] for row in first_day] == ["000001.SZ", "600000.SH"]
    assert len(first_stock) == 2
    assert {row["partition_date"] for row in first_stock} == {
        date(2024, 1, 2),
        date(2024, 1, 3),
    }
    partition_directories = [
        manifest.parent.relative_to(tmp_path / "daily").as_posix()
        for manifest in (tmp_path / "daily").rglob("_manifest.json")
    ]
    assert len(partition_directories) == 2
    assert all(path.startswith("partition_date=") for path in partition_directories)
    assert all("ts_code=" not in path for path in partition_directories)
    assert {utc_us_to_datetime(row["visible_at"]).hour for row in first_stock} == {8}
    assert {row["exchange"] for row in calendar} == {"SSE", "SZSE", "BSE"}
    assert {row["partition_date"] for row in calendar} == {date(2024, 1, 2)}
    calendar_partition_directories = [
        manifest.parent.relative_to(tmp_path / "trade_cal").as_posix()
        for manifest in (tmp_path / "trade_cal").rglob("_manifest.json")
    ]
    assert calendar_partition_directories == [
        "partition_date=value%3A2024-01-02",
        "partition_date=value%3A2024-01-03",
    ]


def test_data_store_public_business_methods_are_only_read_and_write() -> None:
    public_methods = {
        name
        for name, value in vars(TushareDataStore).items()
        if callable(value) and not name.startswith("_")
    }
    assert public_methods == {"read", "write"}


def test_requested_range_is_refetched_and_partitions_are_not_duplicated(
    tmp_path: Path,
) -> None:
    pro, api = _client(_market_responder)
    with TushareDataStore(tmp_path) as store:
        assert sync_daily(pro, store, "20240103", "20240105") == 6
        assert sync_daily(pro, store, "20240101", "20240107") == 14
        assert sync_daily(pro, store, "20240101", "20240107") == 14
        rows = store.read(
            "daily",
            [date(2024, 1, day) for day in range(1, 8)],
            ts_code="000001.SZ",
        ).to_pylist()

    requested_dates = [
        str(arguments["trade_date"]) for name, _, arguments in api.calls if name == "daily"
    ]
    assert requested_dates == [
        "20240103",
        "20240104",
        "20240105",
        "20240101",
        "20240102",
        "20240103",
        "20240104",
        "20240105",
        "20240106",
        "20240107",
        "20240101",
        "20240102",
        "20240103",
        "20240104",
        "20240105",
        "20240106",
        "20240107",
    ]
    assert [row["trade_date"] for row in rows] == [date(2024, 1, day) for day in range(1, 8)]


def test_sync_inc_uses_planned_windows_and_ignores_sync_all_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = date(2024, 7, 1)
    calls: dict[str, tuple[date, date]] = {}

    def record(dataset: str) -> Callable[..., int]:
        def sync_dataset(
            _pro: TushareProClient,
            _store: TushareDataStore,
            start_date: str | date,
            end_date: str | date,
        ) -> int:
            calls[dataset] = (
                sync_module._parse_date(start_date),
                sync_module._parse_date(end_date),
            )
            return 1

        return sync_dataset

    datasets = (
        "daily",
        "stk_limit",
        "suspend_d",
        "stock_st",
        "daily_basic",
        "moneyflow",
        "adj_factor",
        "income",
        "balancesheet",
        "cashflow",
        "fina_indicator",
        "forecast",
        "express",
        "fina_audit",
        "dividend",
        "sw_industry",
    )
    for dataset in datasets:
        monkeypatch.setattr(sync_module, f"sync_{dataset}", record(dataset))

    pro, api = _client(_market_responder)
    with TushareDataStore(tmp_path) as store:
        for dataset in TABLE_SCHEMAS:
            store._mark_sync_all_completed(
                dataset,
                date(2000, 1, 1),
                date(2030, 12, 31),
            )
        result = sync_module.sync_inc(pro, store, current)

    assert calls["daily"] == (date(2024, 6, 27), current)
    assert calls["stk_limit"] == calls["suspend_d"] == calls["stock_st"] == calls["daily"]
    assert calls["daily_basic"] == (date(2024, 6, 22), current)
    assert calls["moneyflow"] == calls["adj_factor"] == calls["daily_basic"]
    assert calls["income"] == (date(2021, 7, 1), current)
    assert calls["balancesheet"] == calls["cashflow"] == calls["income"]
    assert calls["fina_indicator"] == calls["income"]
    assert calls["forecast"] == (current - timedelta(days=179), current)
    assert calls["express"] == calls["fina_audit"] == calls["forecast"]
    assert calls["dividend"] == (date(2022, 7, 1), current)
    assert calls["sw_industry"] == (current, current)
    calendar_calls = [arguments for name, _, arguments in api.calls if name == "trade_cal"]
    assert [call["exchange"] for call in calendar_calls] == ["SSE", "SZSE", "BSE"]
    assert {call["start_date"] for call in calendar_calls} == {"20240502"}
    assert {call["end_date"] for call in calendar_calls} == {"20250702"}
    assert set(result) == {"trade_cal", *datasets}

    calls.clear()
    ordinary = date(2024, 4, 30)
    ordinary_pro, _ = _client(_market_responder)
    with TushareDataStore(tmp_path / "ordinary") as store:
        ordinary_result = sync_module.sync_inc(ordinary_pro, store, ordinary)

    assert calls["income"] == (date(2024, 4, 21), ordinary)
    assert calls["dividend"] == (date(2024, 4, 1), ordinary)
    assert "fina_audit" not in calls
    assert "sw_industry" not in calls
    assert ordinary_result["fina_audit"] == 0
    assert ordinary_result["sw_industry"] == 0


def test_sync_all_resumes_failed_chunk_and_then_skips_completed_range(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    datasets = (
        "trade_cal",
        "daily",
        "daily_basic",
        "stk_limit",
        "stock_st",
        "adj_factor",
        "suspend_d",
        "moneyflow",
        "dividend",
        "forecast",
        "express",
        "income",
        "balancesheet",
        "cashflow",
        "fina_indicator",
        "sw_industry",
        "fina_audit",
    )
    calls: dict[str, list[tuple[date, date]]] = {dataset: [] for dataset in datasets}
    daily_failed = False

    def record(dataset: str) -> Callable[..., int]:
        def sync_dataset(
            _pro: TushareProClient,
            _store: TushareDataStore,
            start_date: str | date,
            end_date: str | date,
        ) -> int:
            nonlocal daily_failed
            requested = (
                sync_module._parse_date(start_date),
                sync_module._parse_date(end_date),
            )
            calls[dataset].append(requested)
            if dataset == "daily" and len(calls[dataset]) == 2 and not daily_failed:
                daily_failed = True
                raise RuntimeError("simulated interruption")
            return 1

        return sync_dataset

    for dataset in datasets:
        monkeypatch.setattr(sync_module, f"sync_{dataset}", record(dataset))

    pro, _ = _client(_market_responder)
    requested_start = date(2024, 1, 1)
    requested_end = date(2024, 3, 10)
    with TushareDataStore(tmp_path) as store:
        with pytest.raises(RuntimeError, match="simulated interruption"):
            sync_module.sync_all(pro, store, requested_start, requested_end)

        assert store._sync_all_completed_ranges("daily") == [(date(2024, 1, 1), date(2024, 1, 31))]
        resumed = sync_module.sync_all(pro, store, requested_start, requested_end)
        calls_after_resume = {dataset: len(values) for dataset, values in calls.items()}
        repeated = sync_module.sync_all(pro, store, requested_start, requested_end)

    assert calls["trade_cal"] == [(requested_start, requested_end)]
    assert calls["daily"] == [
        (date(2024, 1, 1), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 3, 2)),
        (date(2024, 2, 1), date(2024, 3, 2)),
        (date(2024, 3, 3), date(2024, 3, 10)),
    ]
    assert resumed["trade_cal"] == 0
    assert resumed["daily"] == 2
    assert all(value == 0 for value in repeated.values())
    assert calls_after_resume == {dataset: len(values) for dataset, values in calls.items()}

    document = json.loads(
        (tmp_path / "_meta" / "sync_all" / "daily.json").read_text(encoding="utf-8")
    )
    assert document["dataset"] == "daily"
    assert document["completed_ranges"] == [{"start_date": "2024-01-01", "end_date": "2024-03-10"}]
    assert isinstance(document["updated_at"], int)


def test_sync_all_expands_both_sides_in_date_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    datasets = tuple(TABLE_SCHEMAS)
    calls: dict[str, list[tuple[date, date]]] = {dataset: [] for dataset in datasets}

    def record(dataset: str) -> Callable[..., int]:
        def sync_dataset(
            _pro: TushareProClient,
            _store: TushareDataStore,
            start_date: str | date,
            end_date: str | date,
        ) -> int:
            calls[dataset].append(
                (
                    sync_module._parse_date(start_date),
                    sync_module._parse_date(end_date),
                )
            )
            return 1

        return sync_dataset

    for dataset in datasets:
        monkeypatch.setattr(sync_module, f"sync_{dataset}", record(dataset))

    pro, _ = _client(_market_responder)
    with TushareDataStore(tmp_path) as store:
        sync_module.sync_all(pro, store, "20240110", "20240120")
        sync_module.sync_all(pro, store, "20240101", "20240131")
        completed = store._sync_all_completed_ranges("daily")

    assert calls["daily"] == [
        (date(2024, 1, 10), date(2024, 1, 20)),
        (date(2024, 1, 1), date(2024, 1, 9)),
        (date(2024, 1, 21), date(2024, 1, 31)),
    ]
    assert completed == [(date(2024, 1, 1), date(2024, 1, 31))]


def test_daily_refresh_replaces_old_market_partition(tmp_path: Path) -> None:
    request_count = 0

    def responder(
        api_name: str,
        fields: str,
        arguments: dict[str, object],
    ) -> pd.DataFrame:
        nonlocal request_count
        if api_name == "trade_cal":
            return _calendar_frame(fields, arguments)
        if api_name != "daily":
            return _empty(fields)
        request_count += 1
        row = _record("daily", "000001.SZ", str(arguments["trade_date"]))
        row["close"] = float(request_count)
        return pd.DataFrame([row], columns=pd.Index(fields.split(",")))

    pro, _ = _client(responder)
    with TushareDataStore(tmp_path) as store:
        assert sync_daily(pro, store, "20240102", "20240102") == 1
        assert sync_daily(pro, store, "20240102", "20240102") == 1
        rows = store.read("daily", date(2024, 1, 2)).to_pylist()

    assert len(rows) == 1
    assert rows[0]["close"] == 2.0


def test_store_rejects_partition_date_that_disagrees_with_visible_at(tmp_path: Path) -> None:
    row = _record("daily", "000001.SZ", "20240102")
    frame = pd.DataFrame([row], columns=pd.Index(SOURCE_FIELDS["daily"]))
    data = sync_module._normalise_frame("daily", frame)
    invalid = data.set_column(
        0,
        data.schema.field("partition_date"),
        pa.array([date(2024, 1, 3)], type=pa.date32()),
    )

    with (
        TushareDataStore(tmp_path) as store,
        pytest.raises(ValueError, match="与 visible_at 对应的北京时间日期"),
    ):
        store.write("daily", invalid)

    assert not list((tmp_path / "daily").rglob("_manifest.json"))


def test_empty_cross_section_is_requested_again_on_the_next_refresh(
    tmp_path: Path,
) -> None:
    pro, api = _client(_market_responder)
    with TushareDataStore(tmp_path) as store:
        assert sync_suspend_d(pro, store, "20240102", "20240103") == 0
        first_count = len([call for call in api.calls if call[0] == "suspend_d"])
        assert sync_suspend_d(pro, store, "20240102", "20240103") == 0

    assert first_count == 2
    assert len([call for call in api.calls if call[0] == "suspend_d"]) == 4


def test_statement_uses_full_market_vip_api_and_actual_announcement_time(
    tmp_path: Path,
) -> None:
    def responder(
        api_name: str,
        fields: str,
        arguments: dict[str, object],
    ) -> pd.DataFrame:
        if api_name != "income_vip":
            return _empty(fields)
        day = str(arguments["end_date"])
        rows = [
            _record("income", "000001.SZ", day),
            _record("income", "600000.SH", day),
        ]
        return pd.DataFrame(rows, columns=pd.Index(fields.split(",")))

    pro, api = _client(responder)
    with TushareDataStore(tmp_path) as store:
        assert sync_income(pro, store, "20240502", "20240502") == 2
        row = store.read("income", date(2024, 5, 2)).to_pylist()[0]

    call = api.calls[0]
    assert call[0] == "income_vip"
    assert call[2]["start_date"] == "20240502"
    assert call[2]["end_date"] == "20240502"
    assert "ts_code" not in call[2]
    assert row["visible_at"] == datetime_to_utc_us(
        datetime(2024, 5, 2, 23, 59, 59, 999999, tzinfo=ZoneInfo("Asia/Shanghai"))
    )


def test_indicator_uses_full_market_vip_announcement_range(tmp_path: Path) -> None:
    def responder(
        api_name: str,
        fields: str,
        arguments: dict[str, object],
    ) -> pd.DataFrame:
        if api_name != "fina_indicator_vip":
            return _empty(fields)
        row = _record("fina_indicator", "000001.SZ", str(arguments["start_date"]))
        return pd.DataFrame([row], columns=pd.Index(fields.split(",")))

    pro, api = _client(responder)
    with TushareDataStore(tmp_path) as store:
        assert sync_fina_indicator(pro, store, "20240401", "20240430") == 1

    assert api.calls[0][0] == "fina_indicator_vip"
    assert api.calls[0][2]["start_date"] == "20240401"
    assert api.calls[0][2]["end_date"] == "20240430"
    assert "ts_code" not in api.calls[0][2]


def test_dividend_waits_until_implementation_announcement_for_future_fields(
    tmp_path: Path,
) -> None:
    def responder(
        api_name: str,
        fields: str,
        arguments: dict[str, object],
    ) -> pd.DataFrame:
        if api_name != "dividend" or "imp_ann_date" not in arguments:
            return _empty(fields)
        row = _record("dividend", "000001.SZ", "20240430")
        row["ann_date"] = "20240301"
        row["imp_ann_date"] = arguments["imp_ann_date"]
        return pd.DataFrame([row], columns=pd.Index(fields.split(",")))

    pro, api = _client(responder)
    with TushareDataStore(tmp_path) as store:
        assert sync_dividend(pro, store, "20240430", "20240430") == 1
        row = store.read("dividend", date(2024, 4, 30)).to_pylist()[0]

    calls = [arguments for name, _, arguments in api.calls if name == "dividend"]
    assert calls == [
        {"limit": 5_000, "offset": 0, "ann_date": "20240430"},
        {"limit": 5_000, "offset": 0, "imp_ann_date": "20240430"},
    ]
    assert utc_us_to_datetime(row["visible_at"]).astimezone(
        ZoneInfo("Asia/Shanghai")
    ).date() == date(2024, 4, 30)


def test_dividend_does_not_treat_ex_date_as_a_visibility_date() -> None:
    row = _record("dividend", "000001.SZ", "20240430")
    row["imp_ann_date"] = None
    row["ann_date"] = None
    row["ex_date"] = "20240430"
    frame = pd.DataFrame([row], columns=pd.Index(SOURCE_FIELDS["dividend"]))

    with pytest.raises(ValueError, match="缺少可用的可见日期"):
        sync_module._normalise_frame("dividend", frame)


def test_sw_industry_full_market_api_is_paginated(tmp_path: Path) -> None:
    columns = SOURCE_FIELDS["sw_industry"]

    def industry_row(in_date: str) -> dict[str, object]:
        return {
            "l1_code": "801000.SI",
            "l1_name": "一级",
            "l2_code": "801001.SI",
            "l2_name": "二级",
            "l3_code": "850001.SI",
            "l3_name": "三级",
            "ts_code": "000001.SZ",
            "name": "测试股票",
            "in_date": in_date,
            "out_date": None,
            "is_new": "Y",
        }

    def responder(
        api_name: str,
        fields: str,
        arguments: dict[str, object],
    ) -> pd.DataFrame:
        if api_name != "index_member_all" or arguments["is_new"] == "N":
            return _empty(fields)
        if arguments["offset"] == 0:
            rows = [industry_row("20200101") for _ in range(2_000)]
        elif arguments["offset"] == 2_000:
            rows = [industry_row("20210101")]
        else:
            rows = []
        return pd.DataFrame(rows, columns=pd.Index(columns))

    pro, api = _client(responder)
    with TushareDataStore(tmp_path) as store:
        assert sync_sw_industry(pro, store, "20190101", "20211231") == 2
        rows = store.read(
            "sw_industry",
            [date(2020, 1, 1), date(2021, 1, 1)],
            ts_code="000001.SZ",
        ).to_pylist()

    calls = [arguments for name, _, arguments in api.calls if name == "index_member_all"]
    assert [(call["is_new"], call["offset"]) for call in calls] == [
        ("Y", 0),
        ("Y", 2_000),
        ("N", 0),
    ]
    assert [(row["event_date"], row["event_type"]) for row in rows] == [
        (date(2020, 1, 1), "IN"),
        (date(2021, 1, 1), "IN"),
    ]


def test_audit_is_the_only_per_stock_fallback(tmp_path: Path) -> None:
    def responder(
        api_name: str,
        fields: str,
        arguments: dict[str, object],
    ) -> pd.DataFrame:
        if api_name == "stock_basic":
            rows = (
                [{"ts_code": "000001.SZ"}, {"ts_code": "600000.SH"}]
                if arguments["list_status"] == "L"
                else []
            )
            return pd.DataFrame(rows, columns=pd.Index(fields.split(",")))
        if api_name == "fina_audit":
            row = _record("fina_audit", str(arguments["ts_code"]), "20240430")
            return pd.DataFrame([row], columns=pd.Index(fields.split(",")))
        return _empty(fields)

    pro, api = _client(responder)
    with TushareDataStore(tmp_path) as store:
        assert sync_fina_audit(pro, store, "20240401", "20240430") == 2

    audit_calls = [arguments for name, _, arguments in api.calls if name == "fina_audit"]
    assert [call["ts_code"] for call in audit_calls] == ["000001.SZ", "600000.SH"]


def test_pagination_continues_after_full_page_and_preserves_empty_columns() -> None:
    offsets: list[int] = []

    def request(limit: int, offset: int) -> pd.DataFrame:
        offsets.append(offset)
        if offset == 0:
            return pd.DataFrame([{"value": 1}, {"value": 2}])
        return pd.DataFrame([{"value": 3}])

    result = sync_module._fetch_pages(request, page_size=2)
    empty = sync_module._fetch_pages(
        lambda _limit, _offset: pd.DataFrame(columns=pd.Index(["value"])),
        page_size=2,
    )

    assert offsets == [0, 2]
    assert result["value"].tolist() == [1, 2, 3]
    assert empty.columns.tolist() == ["value"]


@pytest.mark.parametrize("value", [None, pd.NA, pd.NaT, float("nan")])
def test_clean_scalar_recognises_common_missing_values(value: object) -> None:
    assert sync_module._clean_scalar(value) is None


def test_clean_scalar_keeps_normal_scalar() -> None:
    assert sync_module._clean_scalar(1.5) == 1.5
    assert sync_module._clean_scalar("20240101") == "20240101"


def test_empty_requested_range_is_allowed(tmp_path: Path) -> None:
    def responder(
        api_name: str,
        fields: str,
        arguments: dict[str, object],
    ) -> pd.DataFrame:
        if api_name == "trade_cal":
            return _calendar_frame(fields, arguments)
        return _empty(fields)

    pro, api = _client(responder)
    with TushareDataStore(tmp_path) as store:
        assert sync_daily(pro, store, "20990101", "20990102") == 0

    requested = [arguments["trade_date"] for name, _, arguments in api.calls if name == "daily"]
    assert requested == ["20990101", "20990102"]


def test_every_data_table_has_date_partition_and_market_code_sort() -> None:
    assert set(sync_module.VISIBILITY_RULES) == set(TABLE_SCHEMAS) - {"sw_industry"}
    for dataset, schema in TABLE_SCHEMAS.items():
        assert schema.names[0] == "partition_date"
        assert schema.field("partition_date").type == pa.date32()
        assert TABLE_PARTITION_BY[dataset] == "partition_date"
        if dataset == "trade_cal":
            assert schema.names[:3] == ["partition_date", "exchange", "visible_at"]
            assert TABLE_SORT_BY[dataset] == "exchange"
        else:
            assert schema.names[:3] == ["partition_date", "ts_code", "visible_at"]
            assert TABLE_SORT_BY[dataset] == "ts_code"
        assert schema.field("visible_at").type == pa.int64(), dataset
        assert SOURCE_FIELDS[dataset], dataset


@pytest.mark.parametrize(
    "dataset",
    sorted(set(TABLE_SCHEMAS) - {"sw_industry", "trade_cal"}),
)
def test_every_standard_dataset_derives_partition_from_visible_date(
    dataset: str,
    tmp_path: Path,
) -> None:
    visible_date = date(2024, 1, 2)
    row = _record(dataset, "000001.SZ", "20240102")
    frame = pd.DataFrame([row], columns=pd.Index(SOURCE_FIELDS[dataset]))
    data = sync_module._normalise_frame(dataset, frame)

    assert data.column("partition_date").to_pylist() == [visible_date]
    assert sync_module._market_date(data.column("visible_at")[0].as_py()) == visible_date
    with TushareDataStore(tmp_path) as store:
        assert store.write(dataset, data) == 1
        assert store.read(dataset, visible_date).num_rows == 1


def test_schema_contains_current_documented_and_proxy_supported_fields() -> None:
    assert SOURCE_FIELDS["daily"][-2:] == ("ah_vol", "ah_amount")
    assert "pre_close" in SOURCE_FIELDS["stk_limit"]
    assert "yoy_sales" in SOURCE_FIELDS["express"]
    assert "is_audit" in SOURCE_FIELDS["express"]
    assert "remark" in SOURCE_FIELDS["express"]
    assert len(SOURCE_FIELDS["fina_indicator"]) == 167
    assert "q_netprofit_qoq" in SOURCE_FIELDS["fina_indicator"]
    assert "update_flag" in SOURCE_FIELDS["fina_indicator"]


def test_official_integer_fields_use_int64() -> None:
    for name in (
        "buy_sm_vol",
        "sell_sm_vol",
        "net_mf_vol",
    ):
        assert TABLE_SCHEMAS["moneyflow"].field(name).type == pa.int64()
    assert TABLE_SCHEMAS["express"].field("is_audit").type == pa.int64()


def test_read_accepts_only_int64_microsecond_filter(tmp_path: Path) -> None:
    pro, _ = _client(_market_responder)
    with TushareDataStore(tmp_path) as store:
        sync_daily(pro, store, "20240101", "20240101")
        visible_end = datetime_to_utc_us(
            datetime(2024, 1, 1, 16, tzinfo=ZoneInfo("Asia/Shanghai"))
        )
        assert (
            store.read(
                "daily",
                date(2024, 1, 1),
                ts_code="000001.SZ",
                visible_end=visible_end,
            ).num_rows
            == 1
        )
        with pytest.raises(TypeError, match="Unix Epoch 微秒整数"):
            store.read(
                "daily",
                date(2024, 1, 1),
                ts_code="000001.SZ",
                visible_end=cast(int, datetime(2024, 1, 1, 8)),
            )
