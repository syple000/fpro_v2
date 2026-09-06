from __future__ import annotations

from datetime import date, time
from typing import cast

import pytest

from backtest.clock import Event
from backtest.config import BacktestConfig
from backtest.domain import AccountSnapshot, BacktestResult, OrderStatus
from backtest.engine import BacktestEngine
from backtest.strategy import Strategy
from market_data import DataReader, DataView
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

    def on_bar(
        self,
        data: DataView,
        event: Event,
        account: AccountSnapshot,
    ) -> dict[str, float] | None:
        del data
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
