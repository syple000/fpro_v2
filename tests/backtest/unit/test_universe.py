from collections.abc import Iterator
from datetime import date, time
from pathlib import Path

import pyarrow as pa
import pytest

from backtest.clock import at_time
from backtest.errors import DataError
from backtest.universe import select_stock_universe
from market_data import DataCatalog, DataReader, SourceConfig
from tushare_data import TABLE_SCHEMAS, TushareDataStore


@pytest.fixture
def universe_reader(tmp_path: Path) -> Iterator[DataReader]:
    """日历只有 2024 年元旦起的数据，包含老股和两只新股。"""
    root = tmp_path / "tushare"
    with TushareDataStore(root) as store:
        store.write(
            "trade_cal",
            pa.Table.from_pylist(
                [
                    {
                        "exchange": "SSE",
                        "cal_date": date(2024, 1, day),
                        "is_open": int(day in (2, 3, 4, 5, 8)),
                    }
                    for day in range(1, 9)
                ],
                schema=TABLE_SCHEMAS["trade_cal"],
            ),
        )
        store.write(
            "stock_basic",
            pa.Table.from_pylist(
                [
                    {
                        "ts_code": symbol,
                        "exchange": "SZSE",
                        "curr_type": "CNY",
                        "list_date": listing_date,
                    }
                    for symbol, listing_date in (
                        ("000001.SZ", date(1991, 4, 3)),
                        ("000002.SZ", date(2024, 1, 3)),
                        ("000003.SZ", date(2024, 1, 4)),
                    )
                ],
                schema=TABLE_SCHEMAS["stock_basic"],
            ),
        )
    with DataCatalog(tushare_root=root, qmt_root=tmp_path / "qmt") as catalog:
        yield DataReader(
            catalog,
            sources=SourceConfig(
                routes={
                    "calendar.sessions": "tushare",
                    "reference.stocks": "tushare",
                }
            ),
        )


@pytest.mark.parametrize("as_of", [date(2024, 1, 5), date(2024, 1, 7)])
def test_listing_threshold_counts_trading_days_including_listing_day(
    universe_reader: DataReader, as_of: date
) -> None:
    """老股、恰好满三交易日的新股入选；周末和未来交易日不计数。"""
    symbols = select_stock_universe(
        universe_reader.at(at_time(as_of, time(16, 5))),
        minimum_listing_sessions=3,
        exclude_st=False,
    )

    assert symbols == ("000001.SZ", "000002.SZ")


@pytest.mark.parametrize("as_of", [date(2023, 12, 29), date(2024, 1, 1), date(2024, 1, 5)])
def test_missing_listing_history_raises_instead_of_excluding_old_stock(
    universe_reader: DataReader, as_of: date
) -> None:
    with pytest.raises(DataError, match="日历覆盖不足.*250"):
        select_stock_universe(
            universe_reader.at(at_time(as_of, time(16, 5))),
            minimum_listing_sessions=250,
            exclude_st=False,
            allowed_symbols=("000001.SZ",),
        )


def test_new_stock_with_complete_listing_history_is_excluded_without_error(
    universe_reader: DataReader,
) -> None:
    symbols = select_stock_universe(
        universe_reader.at(at_time(date(2024, 1, 5), time(16, 5))),
        minimum_listing_sessions=250,
        exclude_st=False,
        allowed_symbols=("000003.SZ",),
    )

    assert symbols == ()


def test_disabled_listing_filter_does_not_require_calendar_history(
    universe_reader: DataReader,
) -> None:
    symbols = select_stock_universe(
        universe_reader.at(at_time(date(2023, 12, 29), time(16, 5))),
        minimum_listing_sessions=0,
        exclude_st=False,
    )

    assert symbols == ("000001.SZ",)
