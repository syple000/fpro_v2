from datetime import date, time

import pytest

from backtest.broker import SimulatedBroker
from backtest.clock import at_time
from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.domain import CorporateAction, Fill, Side
from backtest.portfolio import Portfolio


def test_cash_and_stock_dividend_follow_record_ex_pay_and_listing_dates() -> None:
    portfolio = Portfolio(100_000)
    portfolio.apply_fill(
        Fill(
            "F",
            "O",
            "000001.SZ",
            Side.BUY,
            at_time(date(2026, 1, 2), time(9, 30)),
            1_000,
            10,
            10,
            10_000,
            0,
            0,
            0,
            0,
        )
    )
    portfolio.unlock_t1()
    action = CorporateAction(
        action_id="CA",
        symbol="000001.SZ",
        visible_at=at_time(date(2026, 1, 2), time(9)),
        record_date=date(2026, 1, 2),
        ex_date=date(2026, 1, 5),
        pay_date=date(2026, 1, 6),
        listing_date=date(2026, 1, 7),
        cash_dividend=0.5,
        cash_dividend_before_tax=None,
        stock_dividend=0.1,
    )
    processor = CorporateActionProcessor([action])
    broker = SimulatedBroker(
        BacktestConfig(date(2026, 1, 2), date(2026, 1, 7))
    )

    processor.on_session_end(
        at_time(date(2026, 1, 2), time(16, 5)), portfolio
    )
    processor.on_session_start(
        at_time(date(2026, 1, 5), time(9, 25)), portfolio, broker
    )
    position = portfolio.position("000001.SZ")
    assert portfolio.dividend_receivable == pytest.approx(500)
    assert (position.quantity, position.pending_listing_quantity) == (1_100, 100)

    processor.on_session_start(
        at_time(date(2026, 1, 6), time(9, 25)), portfolio, broker
    )
    assert portfolio.cash == pytest.approx(90_500)
    processor.on_session_start(
        at_time(date(2026, 1, 7), time(9, 25)), portfolio, broker
    )
    assert position.sellable_quantity == 1_100
