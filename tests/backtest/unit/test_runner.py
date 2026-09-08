from contextlib import contextmanager
from datetime import date, datetime
from typing import cast

import pyarrow as pa
import pytest

from backtest.config import BacktestConfig
from backtest.errors import DataError
from backtest.runner import _load_sessions, _validate_calendar, default_source_config
from market_data import DataCatalog, DataReader, SourceConfig
from models import ROUTE_SCHEMAS
from tests.backtest.conftest import MemoryDataReader, MemoryDataView, bar_table
from tushare_data import TABLE_SCHEMAS, TushareDataStore


def test_default_source_config_registers_every_logical_route() -> None:
    config = default_source_config()

    assert set(config.routes) == set(ROUTE_SCHEMAS)
    assert config.routes["market.intraday_bars"] == "qmt"
    assert config.routes["market.realtime_quotes"] == "qmt"
    assert all(
        source == "tushare"
        for route, source in config.routes.items()
        if route not in {"market.intraday_bars", "market.realtime_quotes"}
    )


def test_default_schedule_only_loads_requested_calendar_dates() -> None:
    day = date(2026, 1, 30)

    class CurrentCalendarReader(MemoryDataReader):
        def at(self, as_of: datetime) -> MemoryDataView:
            assert as_of.date() == day
            return super().at(as_of)

    reader = CurrentCalendarReader(bar_table([]), (day, date(2026, 2, 2)))
    sessions, calendar = _load_sessions(cast(DataReader, reader), BacktestConfig(day, day))

    assert sessions == calendar == (day,)


def test_period_schedule_loads_one_following_session(tmp_path) -> None:
    day, following = date(2026, 1, 30), date(2026, 2, 2)
    with calendar_reader(tmp_path, {
        day: 1, date(2026, 1, 31): 0, date(2026, 2, 1): 0,
        following: 1, date(2026, 2, 3): 1,
    }) as reader:
        sessions, calendar = _load_sessions(
            reader, BacktestConfig(day, day), needs_next_session=True
        )

    assert sessions == (day,)
    assert calendar == (day, following)


def test_missing_following_session_is_an_explicit_error() -> None:
    day = date(2026, 1, 30)
    reader = MemoryDataReader(bar_table([]), (day,))

    with pytest.raises(DataError, match="缺少下一交易日"):
        _load_sessions(cast(DataReader, reader), BacktestConfig(day, day), needs_next_session=True)


@contextmanager
def calendar_reader(tmp_path, states):
    root = tmp_path / "tushare"
    with TushareDataStore(root) as store:
        store.write("trade_cal", pa.Table.from_pylist(
            [
                {"exchange": "SSE", "cal_date": day, "is_open": state}
                for day, state in states.items()
            ],
            schema=TABLE_SCHEMAS["trade_cal"],
        ))
    with DataCatalog(tushare_root=root, qmt_root=tmp_path / "qmt") as catalog:
        yield DataReader(catalog, sources=SourceConfig(routes={"calendar.sessions": "tushare"}))


@pytest.mark.parametrize("missing_day", [5, 7, 9])
def test_calendar_requires_start_interior_and_end_dates(tmp_path, missing_day: int) -> None:
    states = {date(2026, 1, day): 1 for day in range(5, 10) if day != missing_day}
    with (
        calendar_reader(tmp_path, states) as reader,
        pytest.raises(DataError, match=f"日历覆盖不足.*2026-01-{missing_day:02}"),
    ):
        _load_sessions(reader, BacktestConfig(date(2026, 1, 5), date(2026, 1, 9)))


def test_calendar_distinguishes_weekend_from_missing_data(tmp_path) -> None:
    states = {date(2026, 1, day): int(day in (9, 12)) for day in range(9, 13)}
    with calendar_reader(tmp_path, states) as reader:
        sessions, calendar = _load_sessions(
            reader, BacktestConfig(date(2026, 1, 9), date(2026, 1, 12))
        )
    assert sessions == calendar == (date(2026, 1, 9), date(2026, 1, 12))


def test_calendar_rejects_unknown_open_state() -> None:
    day = date(2026, 1, 5)
    with pytest.raises(DataError, match="开休市状态未知"):
        _validate_calendar([{"cal_date": day, "is_open": None}], day, day, "SSE")


def test_period_boundary_requires_calendar_through_next_session(tmp_path) -> None:
    day = date(2026, 1, 30)
    with (
        calendar_reader(tmp_path, {day: 1, date(2026, 2, 2): 1}) as reader,
        pytest.raises(DataError, match="日历覆盖不足.*2026-01-31"),
    ):
        _load_sessions(reader, BacktestConfig(day, day), needs_next_session=True)
