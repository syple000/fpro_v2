from datetime import date, time

import pytest

from backtest.clock import Clock, at_time, market_timeline


def test_daily_timeline_comes_only_from_calendar_and_frequency() -> None:
    session = date(2026, 1, 30)
    events = list(market_timeline((session,), (session, date(2026, 2, 2)), "1d"))

    assert len(events) == 1
    assert events[0].at == at_time(session, time(16, 5))
    assert events[0].interval_start == at_time(session, time(9, 30))
    assert events[0].is_month_end is True


def test_sixty_minute_timeline_respects_lunch_break() -> None:
    session = date(2026, 1, 5)
    closes = [
        event.at.time()
        for event in market_timeline((session,), (session, date(2026, 1, 6)), "60m")
    ]

    assert closes == [time(10, 30), time(11, 30), time(14), time(15)]


def test_clock_rejects_time_travel() -> None:
    clock = Clock()
    clock.move_to(at_time(date(2026, 1, 5), time(10)))

    with pytest.raises(ValueError, match="不能倒退"):
        clock.move_to(at_time(date(2026, 1, 5), time(9, 59)))
