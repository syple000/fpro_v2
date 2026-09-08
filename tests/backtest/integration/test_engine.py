from __future__ import annotations

from datetime import date, time
from typing import cast

import pytest

from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.domain import BacktestResult, CorporateAction, OrderStatus, Side
from backtest.engine import BacktestEngine
from backtest.errors import CorporateActionError
from backtest.schedule import Schedule
from backtest.strategy import Strategy, StrategyContext
from market_data import DataReader
from tests.backtest.conftest import (
    MemoryDataReader,
    bar_table,
    daily_bar,
    timestamp,
)


class OneShotStrategy(Strategy):
    def __init__(self) -> None:
        self.calls = 0
        self.events: list[str] = []
        self.quantities: list[int] = []

    def on_event(self, context: StrategyContext) -> dict[str, float] | None:
        event, account = context.event, context.account
        self.events.append(f"bar:{event.at:%Y-%m-%d %H:%M}")
        holding = account.holding("000001.SZ")
        self.quantities.append(holding.quantity if holding is not None else 0)
        self.calls += 1
        return {"000001.SZ": 0.5} if self.calls == 1 else None


def _run(
    config: BacktestConfig,
    reader: MemoryDataReader,
    sessions: tuple[date, ...],
    calendar: tuple[date, ...],
    strategy: Strategy,
) -> BacktestResult:
    return BacktestEngine(
        reader=cast(DataReader, reader),
        config=config,
        sessions=sessions,
        calendar=calendar,
        strategy=strategy,
    ).run()


@pytest.mark.parametrize("invalid_record", [False, True])
def test_engine_rejects_non_session_dividend_dates(invalid_record: bool) -> None:
    """持仓登记或派息落在回测末尾周日时，交易日回放也必须明确报错。"""
    sessions = (date(2026, 1, 8), date(2026, 1, 9))
    action = CorporateAction(
        action_id="SUNDAY",
        symbol="000001.SZ",
        record_date=date(2026, 1, 11) if invalid_record else date(2026, 1, 9),
        ex_date=None,
        pay_date=date(2026, 1, 12) if invalid_record else date(2026, 1, 11),
        listing_date=None,
        cash_dividend=0.5,
        cash_dividend_before_tax=None,
        stock_dividend=0,
    )
    reader = MemoryDataReader(bar_table([daily_bar(day, 10) for day in sessions]), sessions)
    engine = BacktestEngine(
        reader=cast(DataReader, reader),
        config=BacktestConfig(sessions[0], date(2026, 1, 11), volume_limit=None),
        sessions=sessions,
        strategy=OneShotStrategy(),
        actions=CorporateActionProcessor([action]),
    )
    with pytest.raises(CorporateActionError, match="2026-01-11 不在回放交易日历中"):
        engine.run()


def test_daily_signal_fills_at_next_session_open() -> None:
    sessions = (date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7))
    calendar = (*sessions, date(2026, 1, 8))
    config = BacktestConfig(
        sessions[0],
        sessions[-1],
        frequency="1d",
        symbols=("000001.SZ",),
        initial_cash=100_000,
        slippage_bps=0,
        volume_limit=None,
        commission_rate=0,
        minimum_commission=0,
    )
    strategy = OneShotStrategy()
    reader = MemoryDataReader(
        bar_table(
            [
                daily_bar(sessions[0], 10),
                daily_bar(sessions[1], 12, open_price=11),
                daily_bar(sessions[2], 13),
            ]
        ),
        calendar,
    )

    result = _run(config, reader, sessions, calendar, strategy)

    assert strategy.events[:2] == [
        "bar:2026-01-05 16:05",
        "bar:2026-01-06 16:05",
    ]
    assert strategy.quantities[:2] == [0, 5_000]
    assert [update.status for update in result.order_updates] == [
        OrderStatus.SUBMITTED,
        OrderStatus.FILLED,
    ]
    assert result.fills[0].filled_at == timestamp(sessions[1], time(9, 30))
    assert result.fills[0].execution_price == 11
    assert result.fills[0].quantity == 5_000
    assert result.order_updates[-1].status is OrderStatus.FILLED
    assert result.equity[-1].total_equity == pytest.approx(109_999.45)


def test_minute_signal_fills_on_next_bar_before_strategy_callback() -> None:
    session = date(2026, 1, 5)
    next_session = date(2026, 1, 6)
    config = BacktestConfig(
        session,
        session,
        frequency="1m",
        symbols=("000001.SZ",),
        initial_cash=100_000,
        slippage_bps=0,
        volume_limit=None,
        commission_rate=0,
        minimum_commission=0,
    )
    rows = [
        {
            "symbol": "000001.SZ",
            "interval_start": timestamp(session, time(9, 30)),
            "interval_end": timestamp(session, time(9, 31)),
            "open": 9.5,
            "high": 10.0,
            "low": 9.5,
            "close": 10.0,
        },
        {
            "symbol": "000001.SZ",
            "interval_start": timestamp(session, time(9, 31)),
            "interval_end": timestamp(session, time(9, 32)),
            "open": 11.0,
            "high": 12.0,
            "low": 11.0,
            "close": 12.0,
        },
    ]
    strategy = OneShotStrategy()
    reader = MemoryDataReader(bar_table(rows), (session, next_session))

    result = _run(
        config,
        reader,
        (session,),
        (session, next_session),
        strategy,
    )

    assert strategy.events[:2] == [
        "bar:2026-01-05 09:31",
        "bar:2026-01-05 09:32",
    ]
    assert strategy.quantities[:2] == [0, 5_000]
    assert result.fills[0].filled_at == timestamp(session, time(9, 31))
    assert result.fills[0].execution_price == 11
    assert result.equity[-1].total_equity == pytest.approx(104_999.45)


@pytest.mark.parametrize("period", ["week", "month"])
def test_slow_strategy_still_matches_and_values_every_day(period: str) -> None:
    sessions = tuple(date(2026, 1, day) for day in (28, 29, 30)) + (date(2026, 2, 2),)
    calendar = (*sessions, date(2026, 2, 3))
    strategy = OneShotStrategy()
    strategy.schedule = Schedule(period)
    reader = MemoryDataReader(bar_table([daily_bar(day, 10) for day in sessions]), calendar)
    result = _run(
        BacktestConfig(sessions[0], sessions[-1], volume_limit=None),
        reader,
        sessions,
        calendar,
        strategy,
    )

    assert strategy.events == ["bar:2026-01-30 16:05"]
    assert len(result.equity) == 4
    assert len(result.fills) == 1
    assert result.fills[0].filled_at == timestamp(date(2026, 2, 2), time(9, 30))


def test_direct_orders_cancellation_and_dividends_between_strategy_calls() -> None:
    sessions = tuple(date(2026, 1, day) for day in (5, 6, 7, 8))

    class DirectStrategy(Strategy):
        schedule = Schedule(
            times=tuple(timestamp(day, time(16, 5)) for day in (sessions[0], sessions[-1]))
        )

        def __init__(self) -> None:
            self.accounts = []

        def on_event(self, context: StrategyContext) -> None:
            self.accounts.append(context.account)
            if len(self.accounts) == 1:
                canceled = context.order("000001.SZ", Side.BUY, 200)
                assert context.cancel(canceled.order_id) is True
                assert context.cancel(canceled.order_id) is False
                context.order("000001.SZ", Side.BUY, 100)
                assert len(context.pending_orders) == 1

    action = CorporateAction("CASH", "000001.SZ", sessions[1], None, sessions[2], None, 1, None, 0)
    strategy = DirectStrategy()
    config = BacktestConfig(
        sessions[0],
        sessions[-1],
        initial_cash=10_000,
        volume_limit=None,
        slippage_bps=0,
        commission_rate=0,
        minimum_commission=0,
    )
    reader = MemoryDataReader(bar_table([daily_bar(day, 10) for day in sessions]), sessions)
    result = BacktestEngine(
        reader=cast(DataReader, reader),
        config=config,
        sessions=sessions,
        strategy=strategy,
        actions=CorporateActionProcessor((action,)),
    ).run()

    assert len(strategy.accounts) == 2
    assert len(result.equity) == 4
    assert len(result.fills) == 1
    assert result.fills[0].quantity == 100
    assert result.order_updates[1].status is OrderStatus.CANCELED
    assert strategy.accounts[-1].cash == pytest.approx(9_099.99)
    assert strategy.accounts[-1].holdings[0].sellable_quantity == 100
    assert strategy.accounts[-1].dividend_receivable == 0


def test_daily_schedule_on_minute_bars_keeps_next_bar_execution_and_t1() -> None:
    sessions = (date(2026, 1, 5), date(2026, 1, 6))

    class DailyStrategy(Strategy):
        schedule = Schedule("day", at=time(9, 31))

        def __init__(self) -> None:
            self.contexts: list[StrategyContext] = []

        def on_event(self, context: StrategyContext) -> None:
            self.contexts.append(context)
            if len(self.contexts) == 1:
                context.order("000001.SZ", Side.BUY, 100)

    rows = [
        {
            "symbol": "000001.SZ",
            "interval_start": timestamp(day, time(9, minute)),
            "interval_end": timestamp(day, time(9, minute + 1)),
            "open": 10,
            "close": 10,
            "high": 10,
            "low": 10,
        }
        for day in sessions
        for minute in (30, 31)
    ]
    strategy = DailyStrategy()
    reader = MemoryDataReader(bar_table(rows), sessions)
    result = _run(
        BacktestConfig(sessions[0], sessions[-1], frequency="1m", volume_limit=None),
        reader,
        sessions,
        sessions,
        strategy,
    )

    assert [context.data.as_of for context in strategy.contexts] == [
        timestamp(day, time(9, 31)) for day in sessions
    ]
    assert len(result.equity) == 2
    assert result.fills[0].filled_at == timestamp(sessions[0], time(9, 31))
    assert strategy.contexts[0].account.holdings == ()
    assert strategy.contexts[1].account.holdings[0].sellable_quantity == 100
