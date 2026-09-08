from datetime import date, time
from typing import cast

import pytest

from backtest.broker import SimulatedBroker
from backtest.clock import Event, at_time
from backtest.config import BacktestConfig
from backtest.domain import (
    Bar,
    Fill,
    OrderReason,
    OrderRequest,
    OrderStatus,
    Side,
)
from backtest.errors import AccountError
from backtest.portfolio import Portfolio
from market_data import DataView
from tests.backtest.conftest import MemoryDataReader, bar_table, daily_bar


def _fill(side: Side, quantity: int, price: float) -> Fill:
    return Fill(
        fill_id="F",
        order_id="O",
        symbol="000001.SZ",
        side=side,
        filled_at=at_time(date(2026, 1, 5), time(9, 30)),
        quantity=quantity,
        market_price=price,
        execution_price=price,
        notional=quantity * price,
        commission=0,
        stamp_tax=0,
        transfer_fee=0,
        slippage_cost=0,
    )


def _bar(at_date: date, price: float) -> Bar:
    return Bar(
        symbol="000001.SZ",
        interval_start=at_time(at_date, time(9, 30)),
        open=price,
        close=price,
    )


def _data(
    config: BacktestConfig,
    *,
    up_limit: float | None = None,
    down_limit: float | None = None,
) -> DataView:
    session = config.start_date
    reader = MemoryDataReader(
        bar_table([daily_bar(session, 10)]),
        (session,),
        statuses={"000001.SZ": {"up_limit": up_limit, "down_limit": down_limit}},
    )
    return cast(DataView, reader.at(at_time(session, time(16, 5))))


def _event(session: date) -> Event:
    return Event(
        at=at_time(session, time(16, 5)),
        session=session,
        interval_start=at_time(session, time(9, 30)),
        frequency="1d",
    )


def test_bought_shares_are_not_sellable_until_next_session() -> None:
    config = BacktestConfig(
        date(2026, 1, 5),
        date(2026, 1, 6),
        initial_cash=100_000,
        slippage_bps=0,
        volume_limit=None,
        commission_rate=0,
        minimum_commission=0,
    )
    portfolio = Portfolio(config.initial_cash)
    portfolio.apply_fill(_fill(Side.BUY, 1_000, 10))
    broker = SimulatedBroker(config)
    open_at = at_time(date(2026, 1, 5), time(9, 30))
    broker.submit(
        OrderRequest("000001.SZ", Side.SELL, 1_000, 0),
        submitted_at=open_at,
    )

    fills = broker.match_bar(
        event=_event(open_at.date()),
        bars={"000001.SZ": _bar(open_at.date(), 10)},
        account=portfolio.account_snapshot(),
        data=_data(config),
    )

    assert not fills
    assert broker.updates[-1].status is OrderStatus.NOT_FILLED
    assert broker.updates[-1].reason is OrderReason.INSUFFICIENT_SELLABLE
    portfolio.unlock_t1()
    assert portfolio.position("000001.SZ").sellable_quantity == 1_000


def test_limit_up_blocks_buy() -> None:
    config = BacktestConfig(date(2026, 1, 5), date(2026, 1, 5), volume_limit=None)
    broker = SimulatedBroker(config)
    open_at = at_time(date(2026, 1, 5), time(9, 30))
    broker.submit(
        OrderRequest("000001.SZ", Side.BUY, 100, 0.1),
        submitted_at=open_at,
    )
    fills = broker.match_bar(
        event=_event(open_at.date()),
        bars={"000001.SZ": _bar(open_at.date(), 11)},
        account=Portfolio(100_000).account_snapshot(),
        data=_data(config, up_limit=11),
    )

    assert broker.updates[-1].reason is OrderReason.LIMIT_UP
    assert not fills


def test_portfolio_rejects_overspending_fill() -> None:
    with pytest.raises(AccountError, match="超过可用现金"):
        Portfolio(1_000).apply_fill(_fill(Side.BUY, 1_000, 10))


@pytest.mark.parametrize("side,expected", [(Side.BUY, 100.01), (Side.SELL, 99.99)])
def test_slippage_is_bounded_before_quantity_and_fee_calculation(
    side: Side, expected: float
) -> None:
    day = date(2026, 1, 5)
    config = BacktestConfig(day, day, volume_limit=None, slippage_bps=5)
    portfolio = Portfolio(100_000)
    if side is Side.SELL:
        portfolio.apply_fill(_fill(Side.BUY, 100, 100))
        portfolio.unlock_t1()
    broker = SimulatedBroker(config)
    broker.submit(OrderRequest("000001.SZ", side, 100), at_time(day, time(9, 30)))
    (fill,) = broker.match_bar(
        event=_event(day),
        bars={"000001.SZ": _bar(day, 100)},
        account=portfolio.account_snapshot(),
        data=_data(config, up_limit=100.01, down_limit=99.99),
    )
    assert fill.execution_price == expected
    assert fill.notional == pytest.approx(expected * 100)
    assert fill.slippage_cost == pytest.approx(1)
    portfolio.apply_fill(fill)
    portfolio.assert_valid()


def test_affordability_uses_bounded_execution_price() -> None:
    day = date(2026, 1, 5)
    config = BacktestConfig(day, day, volume_limit=None, slippage_bps=5)
    broker = SimulatedBroker(config)
    broker.submit(OrderRequest("000001.SZ", Side.BUY, 100), at_time(day, time(9, 30)))
    (fill,) = broker.match_bar(
        event=_event(day),
        bars={"000001.SZ": _bar(day, 100)},
        account=Portfolio(10_006.2).account_snapshot(),
        data=_data(config, up_limit=100.01),
    )
    assert fill.quantity == 100
    assert fill.notional + fill.total_fee <= 10_006.2


def test_order_submitted_after_bar_open_waits_for_next_bar() -> None:
    day, following = date(2026, 1, 5), date(2026, 1, 6)
    config = BacktestConfig(day, following, volume_limit=None)
    broker = SimulatedBroker(config)
    order = broker.submit(OrderRequest("000001.SZ", Side.BUY, 100), at_time(day, time(10)))
    portfolio = Portfolio(100_000)

    assert (
        broker.match_bar(
            event=_event(day),
            bars={"000001.SZ": _bar(day, 10)},
            account=portfolio.account_snapshot(),
            data=_data(config),
        )
        == ()
    )
    assert broker.pending_orders == (order,)
    fills = broker.match_bar(
        event=_event(following),
        bars={"000001.SZ": _bar(following, 10)},
        account=portfolio.account_snapshot(),
        data=_data(config),
    )
    assert fills[0].filled_at == at_time(following, time(9, 30))
