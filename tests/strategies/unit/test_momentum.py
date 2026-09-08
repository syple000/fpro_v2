from datetime import date, time
from typing import cast

import pytest

from backtest.clock import Event, at_time
from backtest.errors import DataError
from market_data import DataView
from strategies import MomentumConfig, momentum_return
from strategies.momentum import select_momentum_targets
from tests.backtest.conftest import MemoryDataReader, bar_table, daily_bar


def test_momentum_uses_first_and_last_adjusted_close() -> None:
    assert momentum_return([1.0, 1.1, 1.2]) == pytest.approx(0.2)


def test_momentum_rejects_incomplete_or_invalid_window() -> None:
    assert momentum_return([1.0]) is None
    assert momentum_return([0.0, 1.0]) is None
    assert momentum_return([1.0, None]) is None
    assert momentum_return([1.0, None, 1.1]) is None
    assert momentum_return([1.0, float("nan"), 1.1]) is None
    assert momentum_return([1.0, 0.0]) is None


def test_momentum_config_rejects_non_positive_values() -> None:
    with pytest.raises(ValueError):
        MomentumConfig(lookback_sessions=0)


@pytest.mark.parametrize("missing_index", [0, 1, 2])
def test_momentum_does_not_extend_window_to_replace_missing_prices(missing_index: int) -> None:
    sessions = (date(2026, 1, 28), date(2026, 1, 29), date(2026, 1, 30))
    rows = [daily_bar(date(2025, 1, 2), 1.0)]
    rows.extend(
        daily_bar(day, 10.0) for index, day in enumerate(sessions) if index != missing_index
    )
    reader = MemoryDataReader(bar_table(rows), sessions)
    at = at_time(sessions[-1], time(16, 5))

    targets = select_momentum_targets(
        cast(DataView, reader.at(at)),
        Event(at, sessions[-1], at, "1d", "strategy"),
        MomentumConfig(lookback_sessions=2, minimum_listing_sessions=0),
    )

    assert targets == {}


def test_momentum_window_counts_trading_days_across_weekend() -> None:
    sessions = (date(2026, 1, 29), date(2026, 1, 30), date(2026, 2, 2))
    reader = MemoryDataReader(bar_table(daily_bar(day, 10.0) for day in sessions), sessions)
    at = at_time(sessions[-1], time(16, 5))

    targets = select_momentum_targets(
        cast(DataView, reader.at(at)),
        Event(at, sessions[-1], at, "1d", "strategy"),
        MomentumConfig(lookback_sessions=2, minimum_listing_sessions=0),
    )

    assert targets == {"000001.SZ": 1.0}


def test_momentum_reports_insufficient_calendar() -> None:
    session = date(2026, 1, 30)
    reader = MemoryDataReader(bar_table([daily_bar(session, 10.0)]), (session,))
    at = at_time(session, time(16, 5))

    with pytest.raises(DataError, match="交易日历不足"):
        select_momentum_targets(
            cast(DataView, reader.at(at)),
            Event(at, session, at, "1d", "strategy"),
            MomentumConfig(lookback_sessions=2, minimum_listing_sessions=0),
        )
