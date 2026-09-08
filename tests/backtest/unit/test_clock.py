from datetime import date, time

import pytest

from backtest.clock import Clock, MarketHours, at_time, market_timeline


def test_daily_timeline_comes_only_from_calendar_and_frequency() -> None:
    session = date(2026, 1, 30)
    events = list(market_timeline((session,), "1d"))

    assert [event.kind for event in events] == ["session_start", "bar", "session_end"]
    assert events[0].at == at_time(session, time(9, 25))
    assert events[1].at == at_time(session, time(16, 5))
    assert events[1].interval_start == at_time(session, time(9, 30))
    assert events[2].at == at_time(session, time(16, 5))


def test_sixty_minute_timeline_respects_lunch_break() -> None:
    session = date(2026, 1, 5)
    closes = [
        event.at.time() for event in market_timeline((session,), "60m") if event.kind == "bar"
    ]

    assert closes == [time(10, 30), time(11, 30), time(14), time(15)]


def test_market_hours_control_timeline() -> None:
    session = date(2026, 1, 5)
    market = MarketHours(
        session_start=time(8, 55),
        segments=((time(9), time(11)),),
        daily_bar_at=time(11, 5),
        session_end=time(11, 5),
    )
    events = market_timeline((session,), "60m", market)

    assert [event.at.time() for event in events] == [time(8, 55), time(10), time(11), time(11, 5)]


def test_partial_bar_is_rejected_instead_of_crossing_session_end() -> None:
    market = MarketHours(segments=((time(9, 30), time(10, 15)),))
    with pytest.raises(ValueError, match="不能被 30m 整除"):
        market_timeline((date(2026, 1, 5),), "30m", market)


def test_clock_rejects_time_travel() -> None:
    clock = Clock()
    clock.move_to(at_time(date(2026, 1, 5), time(10)))

    with pytest.raises(ValueError, match="不能倒退"):
        clock.move_to(at_time(date(2026, 1, 5), time(9, 59)))
