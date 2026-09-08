from datetime import date, time
from typing import cast

import pytest

from backtest.broker import SimulatedBroker
from backtest.clock import Event, at_time
from backtest.config import BacktestConfig
from backtest.domain import AccountSnapshot, Bar, Holding, OrderReason, OrderRequest, Side
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


def test_capacity_below_star_minimum_does_not_create_invalid_fill() -> None:
    config = BacktestConfig(DAY, DAY, volume_limit=0.1)
    broker = SimulatedBroker(config)
    opening = at_time(DAY, time(9, 30))
    broker.submit(OrderRequest("688001.SH", Side.BUY, 201), opening)
    previous = {**daily_bar(date(2026, 1, 2), 10), "symbol": "688001.SH", "volume": 1_990}
    data = MemoryDataReader(bar_table([previous]), (DAY,)).at(at_time(DAY, time(16, 5)))
    assert (
        broker.match_bar(
            event=Event(data.as_of, DAY, opening, "1d"),
            bars={"688001.SH": Bar("688001.SH", opening, 10, 10)},
            account=Portfolio(100_000).account_snapshot(),
            data=cast(DataView, data),
        )
        == ()
    )
    assert broker.updates[-1].reason is OrderReason.VOLUME_LIMIT
