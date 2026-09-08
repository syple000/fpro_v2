from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pytest

from market_data import (
    ALL_SYMBOLS,
    DataAdapter,
    DataCatalog,
    DataReader,
    DataSourceUnavailableError,
    SourceConfig,
)
from models import INDUSTRY_SCHEMA
from tushare_data import TABLE_SCHEMAS, TushareDataStore


@pytest.fixture
def industry_reader(tmp_path: Path) -> Iterator[DataReader]:
    # 当前 Manifest 中的真实字段；旧 in_date 不代表 2021 版分类当时已经存在。
    rows = [
        {
            "ts_code": "000035.SZ",
            "name": "中国天楹",
            "l1_code": "801970.SI",
            "l1_name": "环保",
            "l2_code": "801971.SI",
            "l2_name": "环境治理",
            "l3_code": "859713.SI",
            "l3_name": "固废治理",
            "in_date": date(1994, 4, 8),
            "is_new": "Y",
        },
        {
            "ts_code": "000523.SZ",
            "name": "红棉股份",
            "l1_code": "801980.SI",
            "l1_name": "美容护理",
            "l2_code": "801981.SI",
            "l2_name": "个护用品",
            "l3_code": "859812.SI",
            "l3_name": "洗护用品",
            "in_date": date(1993, 11, 8),
            "out_date": date(2024, 7, 29),
            "is_new": "N",
        },
        {
            "ts_code": "000523.SZ",
            "name": "红棉股份",
            "l1_code": "801230.SI",
            "l1_name": "综合",
            "l2_code": "801231.SI",
            "l2_name": "综合Ⅱ",
            "l3_code": "852311.SI",
            "l3_name": "综合Ⅲ",
            "in_date": date(2024, 7, 30),
            "is_new": "Y",
        },
    ]
    tushare_root = tmp_path / "tushare"
    with TushareDataStore(tushare_root) as store:
        store.write("sw_industry", pa.Table.from_pylist(rows, schema=TABLE_SCHEMAS["sw_industry"]))
    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        yield DataReader(
            catalog,
            sources=SourceConfig(routes={"classification.industry": "tushare"}),
        )


@pytest.mark.parametrize("level", [1, 2, 3])
def test_current_taxonomy_cannot_be_queried_in_2010(
    industry_reader: DataReader, level: Literal[1, 2, 3],
) -> None:
    with pytest.raises(DataSourceUnavailableError, match="旧版历史成员分类"):
        industry_reader.at(datetime.fromisoformat("2010-01-04T10:00:00+08:00")).classification.industry(
            symbols=("000035.SZ", "000523.SZ"), level=level,
        )


@pytest.mark.parametrize(
    "as_of",
    [
        "2021-07-30T09:25:00+08:00",
        "2021-07-31T23:59:59+08:00",
        "2021-08-02T09:24:59+08:00",
        "2021-08-02T01:24:59+00:00",
    ],
)
def test_tushare_industry_rejects_times_before_supported_boundary(
    industry_reader: DataReader, as_of: str,
) -> None:
    with pytest.raises(DataSourceUnavailableError, match="旧版历史成员分类"):
        industry_reader.at(datetime.fromisoformat(as_of)).classification.industry(symbols=ALL_SYMBOLS)


@pytest.mark.parametrize("as_of", ["2021-08-02T09:25:00+08:00", "2021-08-02T01:25:00+00:00"])
@pytest.mark.parametrize(
    "level,expected",
    [
        (1, [("801970.SI", "环保"), ("801980.SI", "美容护理")]),
        (2, [("801971.SI", "环境治理"), ("801981.SI", "个护用品")]),
        (3, [("859713.SI", "固废治理"), ("859812.SI", "洗护用品")]),
    ],
)
def test_supported_boundary_keeps_longstanding_members(
    industry_reader: DataReader,
    as_of: str,
    level: Literal[1, 2, 3],
    expected: list[tuple[str, str]],
) -> None:
    result = industry_reader.at(datetime.fromisoformat(as_of)).classification.industry(
        symbols=("000035.SZ", "000523.SZ"), level=level,
    )
    assert result.table.to_pylist() == [
        {"symbol": symbol, "level": level, "industry_code": code, "industry_name": name}
        for symbol, (code, name) in zip(("000035.SZ", "000523.SZ"), expected, strict=True)
    ]


def test_supported_history_does_not_expose_future_membership(industry_reader: DataReader) -> None:
    result = industry_reader.at(
        datetime.fromisoformat("2024-07-28T10:00:00+08:00")
    ).classification.industry(symbols=("000523.SZ",))
    assert result.table.to_pylist() == [
        {"symbol": "000523.SZ", "level": 1,
         "industry_code": "801980.SI", "industry_name": "美容护理"}
    ]


@pytest.mark.parametrize(
    "as_of,expected_names",
    [
        ("2024-07-29T09:25:00+08:00", []),
        ("2024-07-30T09:24:59+08:00", []),
        ("2024-07-30T09:25:00+08:00", ["综合"]),
    ],
)
def test_membership_switch_keeps_existing_date_visibility_rules(
    industry_reader: DataReader, as_of: str, expected_names: list[str],
) -> None:
    # 保留项目已有约定：out_date 不含，in_date 当日 09:25 起可见。
    result = industry_reader.at(datetime.fromisoformat(as_of)).classification.industry(
        symbols=("000523.SZ",),
    )
    assert result.table.column("industry_name").to_pylist() == expected_names


class _HistoricalIndustryAdapter(DataAdapter):
    def industry(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        level: Literal[1, 2, 3],
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        # 构造独立历史来源，不将测试返回值冒充真实旧版分类。
        assert as_of == datetime.fromisoformat("2010-01-04T10:00:00+08:00")
        assert symbols == ("000523.SZ",)
        table = pa.Table.from_pylist(
            [{"symbol": "000523.SZ", "level": level,
              "industry_code": "historical-test", "industry_name": "测试旧版行业"}],
            schema=INDUSTRY_SCHEMA,
        )
        return table if columns is None else table.select(columns)


@pytest.mark.parametrize("level", [1, 2, 3])
def test_custom_historical_industry_source_is_not_limited_by_tushare(
    tmp_path: Path, level: Literal[1, 2, 3],
) -> None:
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"classification.industry": "historical"}),
            adapters={"historical": _HistoricalIndustryAdapter()},
        )
        view = reader.at(datetime.fromisoformat("2010-01-04T10:00:00+08:00"))
        result = view.classification.industry(symbols=("000523.SZ",), level=level)
    assert result.sources == ("historical",)
    assert result.table.to_pylist() == [
        {"symbol": "000523.SZ", "level": level,
         "industry_code": "historical-test", "industry_name": "测试旧版行业"}
    ]
