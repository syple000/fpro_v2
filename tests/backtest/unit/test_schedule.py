from datetime import date, datetime, time

import pytest

from backtest.clock import MarketHours, at_time, market_timeline
from backtest.errors import DataError
from backtest.schedule import Schedule


@pytest.mark.parametrize("every", ["week", "month"])
def test_period_end_uses_next_trading_day_not_backtest_end(every: str) -> None:
    # 回测在周四结束，周五仍是同一周、同一月，不应提前调仓。
    day = date(2026, 1, 29)
    market = MarketHours()
    events = Schedule(every).events(market_timeline((day,), "1d"), (day, date(2026, 1, 30)), market)

    assert events == ()


@pytest.mark.parametrize("every", ["week", "month"])
def test_period_end_respects_holidays(every: str) -> None:
    day = date(2026, 4, 30)
    market = MarketHours()
    events = Schedule(every).events(market_timeline((day,), "1d"), (day, date(2026, 5, 6)), market)

    assert [event.at for event in events] == [at_time(day, time(16, 5))]
    assert events[0].kind == "strategy"


def test_monthly_strategy_can_run_after_minute_bars_when_daily_data_becomes_visible() -> None:
    day = date(2026, 1, 30)
    events = Schedule("month", at=time(16, 5)).events(
        market_timeline((day,), "1m"), (day, date(2026, 2, 2)), MarketHours()
    )

    assert [event.at for event in events] == [at_time(day, time(16, 5))]


def test_seven_minute_schedule_is_independent_of_one_minute_bars() -> None:
    day = date(2026, 1, 5)
    events = Schedule("7m").events(market_timeline((day,), "1m"), (), MarketHours())

    assert len(events) == 34
    assert events[0].at.time() == time(9, 37)
    assert events[16].at.time() == time(11, 29)
    assert events[17].at.time() == time(13, 7)
    assert events[-1].at.time() == time(14, 59)


def test_fixed_time_cannot_trade_inside_an_unfinished_bar() -> None:
    day = date(2026, 1, 5)
    with pytest.raises(DataError, match="更细的撮合周期"):
        Schedule("day", at=time(10, 3)).events(market_timeline((day,), "5m"), (), MarketHours())


def test_explicit_times_are_deduplicated_and_sorted() -> None:
    day = date(2026, 1, 5)
    early, late = at_time(day, time(9, 25)), at_time(day, time(16, 5))
    events = Schedule(times=(late, early, late)).events(
        market_timeline((day,), "1d"), (), MarketHours()
    )

    assert [event.at for event in events] == [early, late]


def test_period_end_requires_calendar_coverage() -> None:
    day = date(2026, 1, 30)
    with pytest.raises(DataError, match="缺少下一交易日"):
        Schedule("month").events(market_timeline((day,), "1d"), (day,), MarketHours())


def test_explicit_times_require_timezone() -> None:
    with pytest.raises(ValueError, match="带时区"):
        Schedule(times=(datetime(2026, 1, 5, 10),))
