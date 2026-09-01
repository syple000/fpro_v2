from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.execution import ExecutionEngine
from backtest.portfolio import Portfolio
from backtest.types import CorporateAction

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _at(day: int, hour: int = 9, minute: int = 25) -> datetime:
    return datetime(2024, 1, day, hour, minute, tzinfo=SHANGHAI)


def test_dividend_receivable_and_stock_listing_are_separate_events() -> None:
    portfolio = Portfolio(10_000.0)
    position = portfolio.position("000001.SZ")
    position.total_quantity = 100
    position.sellable_quantity = 100
    position.average_cost = 10.0
    position.last_price = 10.0
    action = CorporateAction(
        action_id="CA1",
        symbol="000001.SZ",
        visible_at=_at(1),
        record_date=date(2024, 1, 2),
        ex_date=date(2024, 1, 3),
        pay_date=date(2024, 1, 4),
        listing_date=date(2024, 1, 5),
        cash_dividend=0.5,
        cash_dividend_before_tax=0.6,
        stock_dividend=0.1,
    )
    processor = CorporateActionProcessor((action,))
    execution = ExecutionEngine(
        BacktestConfig(start_date=date(2024, 1, 1), end_date=date(2024, 1, 5))
    )

    processor.capture_record_date(_at(2, 16, 5), portfolio)
    processor.pre_open(_at(3), portfolio=portfolio, execution=execution)
    assert portfolio.dividend_receivable == 50.0
    assert position.total_quantity == 110
    assert position.sellable_quantity == 100
    assert position.pending_listing_quantity == 10

    processor.pre_open(_at(4), portfolio=portfolio, execution=execution)
    assert portfolio.dividend_receivable == 0.0
    assert portfolio.cash == 10_050.0
    processor.pre_open(_at(5), portfolio=portfolio, execution=execution)
    assert position.pending_listing_quantity == 0
    assert position.sellable_quantity == 110
    portfolio.assert_invariants()
