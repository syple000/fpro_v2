from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
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
    sync_daily,
    sync_dividend,
    sync_fina_indicator,
    sync_income,
    sync_suspend_d,
    sync_sw_industry,
)


class FakePro:
    def __init__(self) -> None:
        self.daily_calls: list[dict[str, Any]] = []
        self.suspend_calls: list[dict[str, Any]] = []
        self.income_calls: list[dict[str, Any]] = []
        self.fina_indicator_calls: list[dict[str, Any]] = []
        self.industry_calls: list[dict[str, Any]] = []
        self.dividend_calls: list[dict[str, Any]] = []

    def daily(self, **kwargs: Any) -> pd.DataFrame:
        self.daily_calls.append(kwargs)
        fields = kwargs["fields"].split(",")
        days = pd.date_range(kwargs["start_date"], kwargs["end_date"], freq="D")
        rows = []
        for day in days:
            row = {name: 1.0 for name in fields}
            row["ts_code"] = kwargs["ts_code"]
            row["trade_date"] = day.strftime("%Y%m%d")
            rows.append(row)
        return pd.DataFrame(rows, columns=fields)

    def suspend_d(self, **kwargs: Any) -> pd.DataFrame:
        self.suspend_calls.append(kwargs)
        return pd.DataFrame(columns=kwargs["fields"].split(","))

    def income(self, **kwargs: Any) -> pd.DataFrame:
        self.income_calls.append(kwargs)
        fields = kwargs["fields"].split(",")
        row: dict[str, object] = {name: 1.0 for name in fields}
        row.update(
            {
                "ts_code": kwargs["ts_code"],
                "ann_date": "20240501",
                "f_ann_date": "20240502",
                "end_date": "20231231",
                "report_type": "1",
                "comp_type": "1",
                "end_type": "4",
                "update_flag": "1",
            }
        )
        return pd.DataFrame([row], columns=fields)

    def fina_indicator(self, **kwargs: Any) -> pd.DataFrame:
        self.fina_indicator_calls.append(kwargs)
        fields = kwargs["fields"].split(",")
        row: dict[str, object] = {name: 1.0 for name in fields}
        row.update(
            {
                "ts_code": kwargs["ts_code"],
                "ann_date": "20240420",
                "end_date": "20231231",
            }
        )
        return pd.DataFrame([row], columns=fields)

    def index_member_all(self, **kwargs: Any) -> pd.DataFrame:
        self.industry_calls.append(kwargs)
        fields = kwargs["fields"].split(",")
        common = {
            "l1_code": "801000.SI",
            "l1_name": "一级",
            "l2_code": "801001.SI",
            "l2_name": "二级",
            "l3_code": "850001.SI",
            "l3_name": "三级",
            "ts_code": kwargs["ts_code"],
            "name": "测试股票",
        }
        if kwargs["is_new"] == "Y":
            rows = [{**common, "in_date": "20200101", "out_date": None, "is_new": "Y"}]
        else:
            rows = [
                {**common, "in_date": "20180101", "out_date": "20191231", "is_new": "N"}
            ]
        return pd.DataFrame(rows, columns=fields)

    def dividend(self, **kwargs: Any) -> pd.DataFrame:
        self.dividend_calls.append(kwargs)
        fields = kwargs["fields"].split(",")
        rows = []
        for ann_date in ("20180601", "20210601", "20240601"):
            row: dict[str, Any] = {name: None for name in fields}
            row.update(
                {
                    "ts_code": kwargs["ts_code"],
                    "end_date": f"{int(ann_date[:4]) - 1}1231",
                    "ann_date": ann_date,
                    "div_proc": "实施",
                }
            )
            rows.append(row)
        return pd.DataFrame(rows, columns=fields)


class EmptyRecordingPro:
    def __init__(self) -> None:
        self.calls: dict[str, list[dict[str, Any]]] = {}

    def __getattr__(self, api_name: str) -> Any:
        def request(**kwargs: Any) -> pd.DataFrame:
            self.calls.setdefault(api_name, []).append(kwargs)
            return pd.DataFrame(columns=kwargs["fields"].split(","))

        return request


def test_incremental_sync_requests_only_uncovered_ranges(tmp_path: Path) -> None:
    pro = FakePro()
    with TushareDataStore(tmp_path) as store:
        assert sync_daily(pro, store, "000001.SZ", "20240103", "20240105") == 3
        assert sync_daily(pro, store, "000001.SZ", "20240101", "20240107") == 4

        assert [(call["start_date"], call["end_date"]) for call in pro.daily_calls] == [
            ("20240103", "20240105"),
            ("20240101", "20240102"),
            ("20240106", "20240107"),
        ]
        assert store.synced_ranges("daily", "000001.SZ") == [
            (date(2024, 1, 1), date(2024, 1, 7))
        ]
        rows = store.read("daily", "000001.SZ").to_pylist()

    assert len(rows) == 7
    assert [row["trade_date"] for row in rows] == sorted(row["trade_date"] for row in rows)
    assert all(isinstance(row["visible_at"], int) for row in rows)
    assert {utc_us_to_datetime(row["visible_at"]).hour for row in rows} == {8}


def test_empty_result_is_also_covered(tmp_path: Path) -> None:
    pro = FakePro()
    with TushareDataStore(tmp_path) as store:
        assert sync_suspend_d(pro, store, "000001.SZ", "20240101", "20240131") == 0
        assert sync_suspend_d(pro, store, "000001.SZ", "20240101", "20240131") == 0
        assert store.read("suspend_d", "000001.SZ").num_rows == 0

    assert len(pro.suspend_calls) == 1


def test_future_range_is_not_requested_or_marked_as_synced(tmp_path: Path) -> None:
    pro = FakePro()
    future = datetime.now(UTC).date() + timedelta(days=2)
    with TushareDataStore(tmp_path) as store:
        assert sync_daily(pro, store, "000001.SZ", future, future + timedelta(days=10)) == 0
        assert store.synced_ranges("daily", "000001.SZ") == []

    assert pro.daily_calls == []


def test_long_daily_range_is_split_below_single_request_limit(tmp_path: Path) -> None:
    pro = FakePro()
    with TushareDataStore(tmp_path) as store:
        sync_daily(pro, store, "000001.SZ", "20000101", "20111231")

    assert len(pro.daily_calls) == 2
    first_end = datetime.strptime(pro.daily_calls[0]["end_date"], "%Y%m%d").date()
    second_start = datetime.strptime(pro.daily_calls[1]["start_date"], "%Y%m%d").date()
    assert second_start == first_end + timedelta(days=1)


def test_financial_visible_time_uses_actual_announcement_date(tmp_path: Path) -> None:
    pro = FakePro()
    with TushareDataStore(tmp_path) as store:
        assert sync_income(pro, store, "000001.SZ", "20240501", "20240531") == 1
        row = store.read("income", "000001.SZ").to_pylist()[0]

    assert row["end_date"] == date(2023, 12, 31)
    assert row["visible_at"] == datetime_to_utc_us(
        datetime(
            2024,
            5,
            2,
            15,
            59,
            59,
            999999,
            tzinfo=UTC,
        )
    )


def test_fina_indicator_overfetches_report_period_then_filters_by_announcement(
    tmp_path: Path,
) -> None:
    pro = FakePro()
    with TushareDataStore(tmp_path) as store:
        assert sync_fina_indicator(
            pro,
            store,
            "000001.SZ",
            "20240401",
            "20240430",
        ) == 1
        row = store.read("fina_indicator", "000001.SZ").to_pylist()[0]

    call = pro.fina_indicator_calls[0]
    assert call["start_date"] < "20231231"
    assert row["end_date"] == date(2023, 12, 31)
    assert utc_us_to_datetime(row["visible_at"]).date() == date(2024, 4, 20)


def test_sw_industry_splits_future_out_date_into_separate_event(tmp_path: Path) -> None:
    pro = FakePro()
    with TushareDataStore(tmp_path) as store:
        assert sync_sw_industry(pro, store, "000001.SZ", "20170101", "20210101") == 3
        rows = store.read("sw_industry", "000001.SZ").to_pylist()

    assert [(row["event_date"], row["event_type"]) for row in rows] == [
        (date(2018, 1, 1), "IN"),
        (date(2019, 12, 31), "OUT"),
        (date(2020, 1, 1), "IN"),
    ]
    assert "out_date" not in TABLE_SCHEMAS["sw_industry"].names
    assert "is_new" not in TABLE_SCHEMAS["sw_industry"].names
    assert [call["is_new"] for call in pro.industry_calls] == ["Y", "N"]


def test_every_data_table_has_partition_key_then_visible_time() -> None:
    for dataset, schema in TABLE_SCHEMAS.items():
        partition_key = "exchange" if dataset == "trade_cal" else "ts_code"
        assert schema.names[:2] == [partition_key, "visible_at"], dataset
        assert schema.field("visible_at").type == pa.int64(), dataset
        assert SOURCE_FIELDS[dataset], dataset


def test_read_accepts_only_int64_microsecond_filter(tmp_path: Path) -> None:
    pro = FakePro()
    with TushareDataStore(tmp_path) as store:
        sync_daily(pro, store, "000001.SZ", "20240101", "20240101")
        as_of = datetime_to_utc_us(
            datetime(2024, 1, 1, 16, tzinfo=ZoneInfo("Asia/Shanghai"))
        )
        result = store.read(
            "daily",
            "000001.SZ",
            as_of=as_of,
        )
        assert result.num_rows == 1
        with pytest.raises(TypeError, match="Unix Epoch 微秒整数"):
            store.read(
                "daily",
                "000001.SZ",
                as_of=datetime(2024, 1, 1, 8),  # type: ignore[arg-type]
            )


def test_two_stage_range_sync_is_complete_without_refetching_middle_range(
    tmp_path: Path,
) -> None:
    bounded_datasets = (
        "daily",
        "daily_basic",
        "stk_limit",
        "stock_st",
        "adj_factor",
        "suspend_d",
        "moneyflow",
        "forecast",
        "express",
        "fina_audit",
        "income",
        "balancesheet",
        "cashflow",
        "fina_indicator",
    )
    pro = EmptyRecordingPro()
    with TushareDataStore(tmp_path) as store:
        for dataset in bounded_datasets:
            sync_function = getattr(sync_module, f"sync_{dataset}")
            sync_function(pro, store, "000001.SZ", "20210101", "20221231")
            first_call_count = len(pro.calls[dataset])

            sync_function(pro, store, "000001.SZ", "20170101", "20260817")
            second_calls = pro.calls[dataset][first_call_count:]
            assert second_calls, dataset

            # fina_indicator 为弥补“报告期参数/公告日可见”的口径差异会向前多取；
            # 其他有日期范围的接口不应再次请求已经覆盖的 2021-2022。
            if dataset != "fina_indicator":
                for call in second_calls:
                    assert call["end_date"] <= "20201231" or call["start_date"] >= "20230101"

            call_count = len(pro.calls[dataset])
            assert sync_function(
                pro,
                store,
                "000001.SZ",
                "20170101",
                "20260817",
            ) == 0
            assert len(pro.calls[dataset]) == call_count
            assert store.synced_ranges(dataset, "000001.SZ") == [
                (date(2017, 1, 1), date(2026, 8, 17))
            ]


def test_trade_calendar_two_stage_sync_uses_only_uncovered_ranges(tmp_path: Path) -> None:
    pro = EmptyRecordingPro()
    with TushareDataStore(tmp_path) as store:
        sync_module.sync_trade_cal(pro, store, "SZSE", "20210101", "20221231")
        first_call_count = len(pro.calls["trade_cal"])
        sync_module.sync_trade_cal(pro, store, "SZSE", "20170101", "20260817")

        for call in pro.calls["trade_cal"][first_call_count:]:
            assert call["end_date"] <= "20201231" or call["start_date"] >= "20230101"
        assert store.synced_ranges("trade_cal", "SZSE") == [
            (date(2017, 1, 1), date(2026, 8, 17))
        ]


def test_unbounded_apis_two_stage_sync_deduplicates_and_keeps_all_events(
    tmp_path: Path,
) -> None:
    pro = FakePro()
    with TushareDataStore(tmp_path) as store:
        assert sync_dividend(pro, store, "000001.SZ", "20210101", "20221231") == 1
        assert sync_dividend(pro, store, "000001.SZ", "20170101", "20260817") == 2
        assert sync_dividend(pro, store, "000001.SZ", "20170101", "20260817") == 0

        sync_sw_industry(pro, store, "000001.SZ", "20210101", "20221231")
        sync_sw_industry(pro, store, "000001.SZ", "20170101", "20260817")
        assert sync_sw_industry(pro, store, "000001.SZ", "20170101", "20260817") == 0

        dividends = store.read("dividend", "000001.SZ")
        industry = store.read("sw_industry", "000001.SZ")

    assert dividends.num_rows == 3
    assert len({tuple(row.values()) for row in dividends.to_pylist()}) == 3
    assert industry.num_rows == 3
    assert len({tuple(row.values()) for row in industry.to_pylist()}) == 3
    assert len(pro.dividend_calls) == 2
    assert len(pro.industry_calls) == 4
