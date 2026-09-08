from dataclasses import replace
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
    limited = up_limit is not None or down_limit is not None
    reader = MemoryDataReader(
        bar_table([daily_bar(session, 10)]),
        (session,),
        statuses={
            "000001.SZ": {
                "up_limit": up_limit if up_limit is not None else (1e9 if limited else None),
                "down_limit": (
                    down_limit if down_limit is not None else (0.001 if limited else None)
                ),
                "price_limit_status": "limited" if limited else "unlimited",
            }
        },
    )
    return cast(DataView, reader.at(at_time(session, time(16, 5))))


def _event(session: date) -> Event:
    return Event(
        at=at_time(session, time(16, 5)),
        session=session,
        interval_start=at_time(session, time(9, 30)),
        frequency="1d",
    )


@pytest.mark.parametrize(
    "status,expected_fill",
    [
        ({"suspended": None, "price_limit_status": "unlimited"}, False),
        ({"suspended": False, "price_limit_status": "unknown"}, False),
        ({"suspended": False, "price_limit_status": None}, False),
        ({"price_limit_status": "limited", "up_limit": 11}, False),
        ({"price_limit_status": "limited", "up_limit": 11, "down_limit": 12}, False),
        ({"price_limit_status": "limited", "up_limit": 11, "down_limit": 0}, False),
        ({"price_limit_status": "unlimited", "up_limit": 11}, False),
        ({"suspended": False, "price_limit_status": "unlimited"}, True),
        ({"price_limit_status": "limited", "up_limit": 11, "down_limit": 9}, True),
    ],
)
def test_execution_requires_known_suspension_and_price_limit_semantics(
    status: dict[str, object], expected_fill: bool
) -> None:
    """未知与显式无限制区别处理，矛盾或不完整的限价不能放行。"""
    session = date(2026, 1, 5)
    config = BacktestConfig(session, session, volume_limit=None)
    reader = MemoryDataReader(
        bar_table([daily_bar(session, 10)]),
        (session,),
        statuses={"000001.SZ": status},
    )
    broker = SimulatedBroker(config)
    broker.submit(OrderRequest("000001.SZ", Side.BUY, 100), at_time(session, time(9, 25)))
    fills = broker.match_bar(
        event=_event(session),
        bars={"000001.SZ": _bar(session, 10)},
        account=Portfolio(100_000).account_snapshot(),
        data=cast(DataView, reader.at(at_time(session, time(9, 30)))),
    )
    assert bool(fills) is expected_fill
    if not expected_fill:
        assert broker.updates[-1].reason is OrderReason.UNKNOWN_MARKET_STATUS
        assert broker.fills == ()


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
    portfolio = Portfolio(1_000)
    with pytest.raises(AccountError, match="超过可用现金"):
        portfolio.apply_fill(_fill(Side.BUY, 1_000, 10))
    assert portfolio.cash == 1_000
    assert portfolio.positions == {}


def test_sale_that_cannot_pay_fees_leaves_broker_and_account_without_a_fill() -> None:
    day = date(2026, 1, 5)
    portfolio = Portfolio(1.1)
    portfolio.apply_fill(_fill(Side.BUY, 100, 0.01))
    portfolio.unlock_t1()
    before = portfolio.account_snapshot()
    broker = SimulatedBroker(BacktestConfig(day, day, volume_limit=None, slippage_bps=0))
    broker.submit(OrderRequest("000001.SZ", Side.SELL, 100), at_time(day, time(9, 30)))
    assert (
        broker.match_bar(
            event=_event(day),
            bars={"000001.SZ": _bar(day, 0.01)},
            account=before,
            data=_data(broker.config),
        )
        == ()
    )
    assert broker.fills == ()
    assert broker.updates[-1].reason is OrderReason.INSUFFICIENT_CASH
    assert portfolio.account_snapshot() == before
    with pytest.raises(AccountError, match="不足以支付费用"):
        portfolio.apply_fill(replace(_fill(Side.SELL, 100, 0.01), commission=5))
    assert portfolio.account_snapshot() == before
    assert portfolio.position("000001.SZ").quantity == 100


def test_small_sale_can_pay_fees_from_existing_cash() -> None:
    portfolio = Portfolio(10)
    portfolio.apply_fill(_fill(Side.BUY, 100, 0.01))
    portfolio.unlock_t1()
    portfolio.apply_fill(replace(_fill(Side.SELL, 100, 0.01), commission=5))
    assert portfolio.cash == 5
    assert portfolio.position("000001.SZ").quantity == 0


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


@pytest.mark.parametrize("side,opening_price", [(Side.BUY, 8), (Side.SELL, 12)])
def test_invalid_open_is_not_made_valid_by_slippage_clamping(
    side: Side, opening_price: float
) -> None:
    day = date(2026, 1, 5)
    config = BacktestConfig(day, day, volume_limit=None)
    broker = SimulatedBroker(config)
    portfolio = Portfolio(100_000)
    if side is Side.SELL:
        portfolio.apply_fill(_fill(Side.BUY, 100, 10))
        portfolio.unlock_t1()
    broker.submit(OrderRequest("000001.SZ", side, 100), at_time(day, time(9, 30)))
    before = portfolio.account_snapshot()
    assert (
        broker.match_bar(
            event=_event(day),
            bars={"000001.SZ": _bar(day, opening_price)},
            account=before,
            data=_data(config, up_limit=11, down_limit=9),
        )
        == ()
    )
    assert broker.fills == ()
    assert broker.updates[-1].reason is OrderReason.INVALID_OPEN
    assert portfolio.account_snapshot() == before


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
