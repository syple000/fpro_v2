from datetime import date, time
from typing import cast

import pytest

from backtest.broker import SimulatedBroker
from backtest.clock import Event, at_time
from backtest.config import BacktestConfig
from backtest.domain import (
    AccountSnapshot,
    Bar,
    Holding,
    OrderReason,
    OrderRequest,
    OrderStatus,
    Side,
)
from backtest.orders import create_orders
from backtest.portfolio import Portfolio
from backtest.trading_rules import quantity_rule
from market_data import DataView
from tests.backtest.conftest import MemoryDataReader, bar_table, daily_bar

DAY = date(2026, 1, 5)


@pytest.mark.parametrize(
    "symbol,quantity", [("688001.SH", 100), ("000001.SZ", 101), ("920047.BJ", 99)]
)
def test_invalid_buy_is_rejected_before_submission(symbol: str, quantity: int) -> None:
    broker = SimulatedBroker(BacktestConfig(DAY, DAY))
    with pytest.raises(ValueError, match="申报规则"):
        broker.submit(OrderRequest(symbol, Side.BUY, quantity), at_time(DAY, time(9, 30)))
    assert broker.orders == ()


@pytest.mark.parametrize("symbol,quantity", [("688001.SH", 201), ("920047.BJ", 101)])
def test_one_share_increment_reaches_broker_fill(symbol: str, quantity: int) -> None:
    config = BacktestConfig(DAY, DAY, volume_limit=None)
    broker = SimulatedBroker(config)
    opening = at_time(DAY, time(9, 30))
    broker.submit(OrderRequest(symbol, Side.BUY, quantity), opening)
    data = MemoryDataReader(bar_table([]), (DAY,)).at(at_time(DAY, time(16, 5)))
    (fill,) = broker.match_bar(
        event=Event(data.as_of, DAY, opening, "1d"),
        bars={symbol: Bar(symbol, opening, 10, 10)},
        account=Portfolio(100_000).account_snapshot(),
        data=cast(DataView, data),
    )
    assert fill.quantity == quantity


def test_board_rules_use_date_and_order_type() -> None:
    assert quantity_rule("688001.SH", DAY).maximum == 50_000
    assert quantity_rule("688001.SH", DAY, "limit").maximum == 100_000
    assert quantity_rule("300001.SZ", date(2020, 8, 21)).maximum == 1_000_000
    assert quantity_rule("300001.SZ", date(2020, 8, 24)).maximum == 150_000
    assert quantity_rule("300001.SZ", DAY, "limit").maximum == 300_000


def test_target_orders_split_at_board_maximum_and_keep_odd_lot_liquidation() -> None:
    account = AccountSnapshot(
        1_000_000, 0, 1_990, 1_001_990, (Holding("688002.SH", 199, 199, 1_990),)
    )
    requests = create_orders({"688001.SH": 1}, account, {"688001.SH": 10}, trading_date=DAY)
    assert [(r.symbol, r.quantity) for r in requests] == [
        ("688001.SH", 50_000),
        ("688001.SH", 50_000),
        ("688002.SH", 199),
    ]


@pytest.mark.parametrize(
    "symbol,requested,capacity",
    [("688001.SH", 201, 199), ("920047.BJ", 101, 99), ("000001.SZ", 100, 99)],
)
@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_legal_orders_can_partially_fill_below_submission_minimum(
    symbol: str, requested: int, capacity: int, side: Side
) -> None:
    """申报合法后，部分成交不受申报最低量或整手递增限制。"""
    config = BacktestConfig(DAY, DAY, volume_limit=0.1)
    broker = SimulatedBroker(config)
    opening = at_time(DAY, time(9, 30))
    broker.submit(OrderRequest(symbol, side, requested), opening)
    previous = {
        **daily_bar(date(2026, 1, 2), 10), "symbol": symbol, "volume": capacity * 10
    }
    data = MemoryDataReader(bar_table([previous]), (DAY,)).at(at_time(DAY, time(16, 5)))
    portfolio = Portfolio(100_000)
    if side is Side.SELL:
        position = portfolio.position(symbol)
        position.quantity = position.sellable_quantity = requested
        position.last_price = 10
    (fill,) = broker.match_bar(
        event=Event(data.as_of, DAY, opening, "1d"),
        bars={symbol: Bar(symbol, opening, 10, 10)},
        account=portfolio.account_snapshot(),
        data=cast(DataView, data),
    )
    assert fill.quantity == capacity
    assert broker.updates[-1].status is OrderStatus.PARTIALLY_FILLED
    assert broker.updates[-1].reason is OrderReason.VOLUME_LIMIT
    portfolio.apply_fill(fill)
    expected = capacity if side is Side.BUY else requested - capacity
    assert portfolio.position(symbol).quantity == expected


@pytest.mark.parametrize(
    "symbol,requested,affordable",
    [("688001.SH", 201, 199), ("920047.BJ", 101, 99), ("000001.SZ", 100, 99)],
)
def test_cash_can_fund_partial_execution_below_submission_minimum(
    symbol: str, requested: int, affordable: int
) -> None:
    """最低佣金和过户费计入现金约束，但不能把不足最低申报量的成交归零。"""
    config = BacktestConfig(DAY, DAY, volume_limit=None, slippage_bps=0)
    broker = SimulatedBroker(config)
    opening = at_time(DAY, time(9, 30))
    broker.submit(OrderRequest(symbol, Side.BUY, requested), opening)
    data = MemoryDataReader(bar_table([]), (DAY,)).at(opening)
    cash = affordable * 10 + 5.1
    portfolio = Portfolio(cash)
    (fill,) = broker.match_bar(
        event=Event(at_time(DAY, time(16, 5)), DAY, opening, "1d"),
        bars={symbol: Bar(symbol, opening, 10, 10)},
        account=portfolio.account_snapshot(),
        data=cast(DataView, data),
    )
    assert fill.quantity == affordable
    assert broker.updates[-1].status is OrderStatus.PARTIALLY_FILLED
    assert broker.updates[-1].reason is OrderReason.INSUFFICIENT_CASH
    portfolio.apply_fill(fill)
    assert portfolio.position(symbol).quantity == affordable
    assert portfolio.cash == pytest.approx(cash - fill.notional - fill.total_fee)
    assert 0 <= portfolio.cash < 10
