from datetime import date, datetime
from typing import cast

import pytest

from backtest.config import BacktestConfig
from backtest.errors import DataError
from backtest.runner import _load_sessions, default_source_config
from market_data import DataReader
from models import ROUTE_SCHEMAS
from tests.backtest.conftest import MemoryDataReader, MemoryDataView, bar_table


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


def test_period_schedule_loads_one_following_session() -> None:
    day, following = date(2026, 1, 30), date(2026, 2, 2)
    reader = MemoryDataReader(bar_table([]), (day, following, date(2026, 2, 3)))
    sessions, calendar = _load_sessions(
        cast(DataReader, reader), BacktestConfig(day, day), needs_next_session=True
    )

    assert sessions == (day,)
    assert calendar == (day, following)


def test_missing_following_session_is_an_explicit_error() -> None:
    day = date(2026, 1, 30)
    reader = MemoryDataReader(bar_table([]), (day,))

    with pytest.raises(DataError, match="缺少下一交易日"):
        _load_sessions(cast(DataReader, reader), BacktestConfig(day, day), needs_next_session=True)
