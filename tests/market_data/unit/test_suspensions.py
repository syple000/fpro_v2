from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from market_data import (
    CodeInterval,
    DataCatalog,
    DataReader,
    DataSourceUnavailableError,
    SecurityCodeHistory,
    SourceConfig,
)
from market_data.adapters import TushareAdapter
from tushare_data import TABLE_SCHEMAS, TushareDataStore


@pytest.mark.parametrize(
    "timing,clock,expected",
    [
        ("09:30-09:40,09:40-09:50", "09:29", None),
        ("09:30-09:40,09:40-09:50", "09:30", True),
        ("09:30-09:40,09:40-09:50", "09:40", True),
        ("09:30-09:40,09:40-09:50", "09:45", True),
        ("09:30-09:40,09:40-09:50", "09:50", False),
        ("9:30-9:40", "09:35", True),
        ("9:30-9:40", "09:40", False),
        ("09:30-09:40,10:00-10:10", "09:45", False),
        ("09:30-09:40,10:00-10:10", "10:00", True),
        ("09:30-09:40,10:00-10:10", "10:10", False),
        ("09:30-11:00,09:40-09:45", "09:50", True),
        ("09:40-09:45,09:30-11:00", "09:50", True),
        (" 9:30 - 9:40 , 09:40-09:50 ", "09:45", True),
        ("09:34:21-09:44:21,09:46:15-09:56:15", "09:34:20", None),
        ("09:34:21-09:44:21,09:46:15-09:56:15", "09:34:21", True),
        ("09:34:21-09:44:21,09:46:15-09:56:15", "09:44:20", True),
        ("09:34:21-09:44:21,09:46:15-09:56:15", "09:44:21", False),
        ("09:34:21-09:44:21,09:46:15-09:56:15", "09:46:14", False),
        ("09:34:21-09:44:21,09:46:15-09:56:15", "09:46:15", True),
        ("09:34:21-09:44:21,09:46:15-09:56:15", "09:56:15", False),
    ],
)
def test_all_suspension_intervals_are_used(
    tmp_path: Path, timing: str, clock: str, expected: bool | None,
) -> None:
    root = tmp_path / "tushare"
    day = date(2021, 7, 19)
    with TushareDataStore(root) as store:
        store.write(
            "suspend_d",
            pa.Table.from_pylist(
                [{"ts_code": "301025.SZ", "trade_date": day,
                  "suspend_type": "S", "suspend_timing": timing}],
                schema=TABLE_SCHEMAS["suspend_d"],
            ),
        )
    with DataCatalog(tushare_root=root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(catalog, sources=SourceConfig(routes={"market.suspensions": "tushare"}))
        result = reader.at(datetime.fromisoformat(f"{day}T{clock}+08:00")).market.status(
            symbols=("301025.SZ",), fields=("suspended",),
        )
        assert result.table.to_pylist() == [{"symbol": "301025.SZ", "suspended": expected}]


@pytest.mark.parametrize("timing", ["09:30-09:40,bad", "09:30-09:40,", "09:30-09:40extra"])
def test_invalid_suffix_is_rejected_even_when_first_interval_is_valid(
    tmp_path: Path, timing: str,
) -> None:
    with TushareDataStore(tmp_path / "tushare") as store:
        store.write(
            "suspend_d",
            pa.Table.from_pylist(
                [{"ts_code": "301025.SZ", "trade_date": date(2021, 7, 19),
                  "suspend_type": "S", "suspend_timing": timing}],
                schema=TABLE_SCHEMAS["suspend_d"],
            ),
        )
    with (
        DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog,
        pytest.raises(DataSourceUnavailableError, match="停牌时段格式无效"),
    ):
        TushareAdapter(catalog).suspensions(
            as_of=datetime(2021, 7, 19, 9, 35, tzinfo=ZoneInfo("Asia/Shanghai")),
            symbols=("301025.SZ",), fetch_limit=1, columns=("symbol",),
        )


def test_untimed_resumption_does_not_override_active_intraday_suspension(tmp_path: Path) -> None:
    day = date(2026, 6, 1)
    with TushareDataStore(tmp_path / "tushare") as store:
        store.write(
            "suspend_d",
            pa.Table.from_pylist(
                [
                    {"ts_code": "600421.SH", "trade_date": day,
                     "suspend_type": "S", "suspend_timing": "9:30-9:40"},
                    {"ts_code": "600421.SH", "trade_date": day, "suspend_type": "R"},
                ],
                schema=TABLE_SCHEMAS["suspend_d"],
            ),
        )
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        adapter = TushareAdapter(catalog)
        for clock, expected in [("09:25", False), ("09:35", True), ("09:40", False)]:
            result = adapter.suspensions(
                as_of=datetime.fromisoformat(f"{day}T{clock}+08:00"),
                symbols=("600421.SH",), fetch_limit=1,
            )
            assert result.to_pylist() == [{"symbol": "600421.SH", "suspended": expected}]


def test_direct_adapter_resolves_aliases_before_python_status_lookup(tmp_path: Path) -> None:
    # 构造代码历史，日期不代表真实证券的更码日。
    old, new = "430047.BJ", "920047.BJ"
    day = date(2026, 1, 8)
    identities = SecurityCodeHistory([
        CodeInterval(1001, old, date(2020, 1, 1), date(2026, 1, 7)),
        CodeInterval(1001, new, date(2026, 1, 7)),
    ])
    with TushareDataStore(tmp_path / "tushare") as store:
        store._mark_sync_all_completed("suspend_d", day, day)
        store.write(
            "suspend_d",
            pa.Table.from_pylist(
                [{"ts_code": new, "trade_date": day,
                  "suspend_type": "S", "suspend_timing": "09:30-10:30"}],
                schema=TABLE_SCHEMAS["suspend_d"],
            ),
        )
    with DataCatalog(
        tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt", identities=identities,
    ) as catalog:
        for symbols in [(old,), (new,), (old, new)]:
            result = TushareAdapter(catalog).suspensions(
                as_of=datetime.fromisoformat(f"{day}T09:45+08:00"),
                symbols=symbols, fetch_limit=1,
            )
            assert result.to_pylist() == [{"symbol": old, "suspended": True}]
