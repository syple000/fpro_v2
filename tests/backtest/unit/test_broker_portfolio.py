from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from backtest.config import ExecutionConfig, FeeConfig
from backtest.execution import ExecutionEngine, FeeModel
from backtest.portfolio import Portfolio
from backtest.types import DailyBar, MarketStatus, OrderReason, OrderSide, OrderStatus

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _time(day: int, hour: int = 9, minute: int = 30) -> datetime:
    return datetime(2024, 1, day, hour, minute, tzinfo=SHANGHAI)


def _bar(day: int, price: float = 10.0) -> DailyBar:
    return DailyBar(
        symbol="000001.SZ",
        session=date(2024, 1, day),
        open=price,
        close=price,
        pre_close=price,
        volume=100_000.0,
    )


def _status(
    *,
    suspended: bool | None = None,
    up: float = 11.0,
    down: float = 9.0,
) -> MarketStatus:
    return MarketStatus("000001.SZ", suspended, up, down, None)


def _execute(
    execution: ExecutionEngine,
    portfolio: Portfolio,
    day: int,
    *,
    status: MarketStatus | None = None,
) -> None:
    fills = execution.execute_open(
        event_time=_time(day),
        bars={"000001.SZ": _bar(day)},
        statuses={"000001.SZ": status or _status()},
        previous_volumes={"000001.SZ": 100_000.0},
        cash=portfolio.cash,
        total_quantities={
            symbol: position.total_quantity for symbol, position in portfolio.positions.items()
        },
        sellable_quantities={
            symbol: position.sellable_quantity for symbol, position in portfolio.positions.items()
        },
    )
    for fill in fills:
        portfolio.apply_fill(fill)


def test_buy_is_t1_locked_and_can_only_be_sold_next_session() -> None:
    portfolio = Portfolio(100_000.0)
    execution = ExecutionEngine(
        execution=ExecutionConfig(slippage_bps=0, max_previous_volume_participation=None),
        fees=FeeConfig(commission_rate=0, minimum_commission=0),
    )
    buy = execution.submit_order(
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=100,
        submitted_at=_time(2, 16, 5),
        earliest_fill_at=_time(3),
    )
    _execute(execution, portfolio, 3)

    assert execution.results[-1].order == buy
    assert execution.results[-1].status is OrderStatus.FILLED
    assert portfolio.position("000001.SZ").sellable_quantity == 0

    same_day_sell = execution.submit_order(
        symbol="000001.SZ",
        side=OrderSide.SELL,
        quantity=100,
        submitted_at=_time(3, 16, 5),
        earliest_fill_at=_time(3),
    )
    _execute(execution, portfolio, 3)
    assert execution.results[-1].order == same_day_sell
    assert execution.results[-1].status is OrderStatus.NOT_FILLED
    assert execution.results[-1].reason is OrderReason.INSUFFICIENT_SELLABLE

    portfolio.unlock_t1()
    execution.submit_order(
        symbol="000001.SZ",
        side=OrderSide.SELL,
        quantity=100,
        submitted_at=_time(4, 9, 25),
        earliest_fill_at=_time(4),
    )
    _execute(execution, portfolio, 4)
    assert execution.results[-1].status is OrderStatus.FILLED
    assert portfolio.position("000001.SZ").total_quantity == 0


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (_status(suspended=True), OrderReason.SUSPENDED),
        (_status(up=10.0), OrderReason.LIMIT_UP),
    ],
)
def test_untradeable_open_has_explicit_reason(
    status: MarketStatus,
    reason: OrderReason,
) -> None:
    portfolio = Portfolio(100_000.0)
    execution = ExecutionEngine(
        execution=ExecutionConfig(slippage_bps=0, max_previous_volume_participation=None),
        fees=FeeConfig(),
    )
    order = execution.submit_order(
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=100,
        submitted_at=_time(2, 16, 5),
        earliest_fill_at=_time(3),
    )
    _execute(execution, portfolio, 3, status=status)
    result = execution.results[-1]
    assert result.order == order
    assert result.status is OrderStatus.NOT_FILLED
    assert result.reason is reason
    assert not execution.fills


def test_execution_returns_fills_before_account_applies_them() -> None:
    portfolio = Portfolio(100_000.0)
    execution = ExecutionEngine(
        execution=ExecutionConfig(slippage_bps=0, max_previous_volume_participation=None),
        fees=FeeConfig(commission_rate=0, minimum_commission=0),
    )
    execution.submit_order(
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=100,
        submitted_at=_time(2, 16, 5),
        earliest_fill_at=_time(3),
    )
    fills = execution.execute_open(
        event_time=_time(3),
        bars={"000001.SZ": _bar(3)},
        statuses={"000001.SZ": _status()},
        previous_volumes={"000001.SZ": 100_000.0},
        cash=portfolio.cash,
        total_quantities={},
        sellable_quantities={},
    )
    assert portfolio.cash == 100_000.0
    portfolio.apply_fill(fills[0])
    assert portfolio.cash < 100_000.0


def test_fee_policy_changes_on_effective_dates() -> None:
    model = FeeModel(FeeConfig(commission_rate=0, minimum_commission=0))
    _, old_stamp, old_transfer = model.calculate(
        side=OrderSide.SELL,
        notional=1_000_000,
        session=date(2022, 4, 28),
    )
    _, middle_stamp, new_transfer = model.calculate(
        side=OrderSide.SELL,
        notional=1_000_000,
        session=date(2022, 4, 29),
    )
    _, new_stamp, _ = model.calculate(
        side=OrderSide.SELL,
        notional=1_000_000,
        session=date(2023, 8, 28),
    )
    assert (old_stamp, middle_stamp, new_stamp) == (1000.0, 1000.0, 500.0)
    assert (old_transfer, new_transfer) == (20.0, 10.0)
