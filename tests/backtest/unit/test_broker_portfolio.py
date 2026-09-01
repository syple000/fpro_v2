from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from backtest.broker import FeeModel, SimBroker
from backtest.config import ExecutionConfig, FeeConfig
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
        amount=price * 100_000,
    )


def _status(*, suspended: bool | None = None, up: float = 11.0, down: float = 9.0) -> MarketStatus:
    return MarketStatus("000001.SZ", suspended, up, down, None)


def test_buy_is_t1_locked_and_can_only_be_sold_next_session() -> None:
    portfolio = Portfolio(100_000.0)
    broker = SimBroker(
        run_id="run",
        strategy_id="test",
        execution=ExecutionConfig(slippage_bps=0, max_previous_volume_participation=None),
        fees=FeeConfig(commission_rate=0, minimum_commission=0),
    )
    order = broker.submit_market_order(
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=100,
        submitted_at=_time(2, 16, 5),
        earliest_fill_at=_time(3),
        portfolio=portfolio,
    )
    broker.match_open(
        event_time=_time(3),
        bars={"000001.SZ": _bar(3)},
        statuses={"000001.SZ": _status()},
        previous_volumes={"000001.SZ": 100_000.0},
        portfolio=portfolio,
    )

    assert order.status is OrderStatus.FILLED
    assert portfolio.position("000001.SZ").total_quantity == 100
    assert portfolio.position("000001.SZ").sellable_quantity == 0
    rejected = broker.submit_market_order(
        symbol="000001.SZ",
        side=OrderSide.SELL,
        quantity=100,
        submitted_at=_time(3, 16, 5),
        earliest_fill_at=_time(4),
        portfolio=portfolio,
    )
    assert rejected.status is OrderStatus.REJECTED
    assert rejected.reason is OrderReason.INSUFFICIENT_SELLABLE

    portfolio.unlock_t1()
    accepted = broker.submit_market_order(
        symbol="000001.SZ",
        side=OrderSide.SELL,
        quantity=100,
        submitted_at=_time(4, 9, 25),
        earliest_fill_at=_time(4),
        portfolio=portfolio,
    )
    broker.match_open(
        event_time=_time(4),
        bars={"000001.SZ": _bar(4)},
        statuses={"000001.SZ": _status()},
        previous_volumes={"000001.SZ": 100_000.0},
        portfolio=portfolio,
    )
    assert accepted.status is OrderStatus.FILLED
    assert portfolio.position("000001.SZ").total_quantity == 0
    portfolio.assert_invariants()


@pytest.mark.parametrize(
    ("status", "side", "reason"),
    [
        (_status(suspended=True), OrderSide.BUY, OrderReason.SUSPENDED),
        (_status(up=10.0), OrderSide.BUY, OrderReason.LIMIT_UP),
    ],
)
def test_untradeable_open_expires_with_explicit_reason(
    status: MarketStatus,
    side: OrderSide,
    reason: OrderReason,
) -> None:
    portfolio = Portfolio(100_000.0)
    broker = SimBroker(
        run_id="run",
        strategy_id="test",
        execution=ExecutionConfig(slippage_bps=0, max_previous_volume_participation=None),
        fees=FeeConfig(),
    )
    order = broker.submit_market_order(
        symbol="000001.SZ",
        side=side,
        quantity=100,
        submitted_at=_time(2, 16, 5),
        earliest_fill_at=_time(3),
        portfolio=portfolio,
    )
    broker.match_open(
        event_time=_time(3),
        bars={"000001.SZ": _bar(3)},
        statuses={"000001.SZ": status},
        previous_volumes={"000001.SZ": 100_000.0},
        portfolio=portfolio,
    )
    assert order.status is OrderStatus.EXPIRED
    assert order.reason is reason
    assert not broker.fills


def test_fee_policy_changes_on_effective_dates() -> None:
    model = FeeModel(FeeConfig(commission_rate=0, minimum_commission=0))
    _, old_stamp, old_transfer = model.calculate(
        side=OrderSide.SELL, notional=1_000_000, session=date(2022, 4, 28)
    )
    _, middle_stamp, new_transfer = model.calculate(
        side=OrderSide.SELL, notional=1_000_000, session=date(2022, 4, 29)
    )
    _, new_stamp, _ = model.calculate(
        side=OrderSide.SELL, notional=1_000_000, session=date(2023, 8, 28)
    )
    assert (old_stamp, middle_stamp, new_stamp) == (1000.0, 1000.0, 500.0)
    assert (old_transfer, new_transfer) == (20.0, 10.0)
