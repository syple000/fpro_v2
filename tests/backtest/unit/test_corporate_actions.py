from datetime import date, datetime, time
from types import SimpleNamespace
from typing import cast

import pytest

from backtest.broker import SimulatedBroker
from backtest.clock import at_time
from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.domain import CorporateAction, Fill, Side
from backtest.errors import CorporateActionError
from backtest.portfolio import Portfolio
from market_data import DataReader


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


def test_cash_dividend_without_ex_date_is_paid_on_pay_date() -> None:
    """特殊现金分配不猜除权日，按登记数量在派息日直接到账。"""
    portfolio = _portfolio_with_shares(1_000)
    action = CorporateAction(
        action_id="CASH",
        symbol="000001.SZ",
        record_date=date(2026, 1, 2),
        ex_date=None,
        pay_date=date(2026, 1, 6),
        listing_date=None,
        cash_dividend=0.5,
        cash_dividend_before_tax=None,
        stock_dividend=0,
    )
    processor = CorporateActionProcessor([action])
    broker = _broker()

    processor.on_session_end(at_time(date(2026, 1, 2), time(16, 5)), portfolio)
    assert portfolio.dividend_receivable == 0

    processor.on_session_start(
        at_time(date(2026, 1, 6), time(9, 25)), portfolio, broker
    )

    assert portfolio.cash == pytest.approx(90_500)
    assert portfolio.dividend_receivable == 0


def test_stock_dividend_without_ex_date_is_sellable_on_listing_date() -> None:
    """特殊送股不猜除权日，在上市日直接增加为可卖数量。"""
    portfolio = _portfolio_with_shares(1_000)
    action = CorporateAction(
        action_id="STOCK",
        symbol="000001.SZ",
        record_date=date(2026, 1, 2),
        ex_date=None,
        pay_date=None,
        listing_date=date(2026, 1, 7),
        cash_dividend=0,
        cash_dividend_before_tax=None,
        stock_dividend=0.1,
    )
    processor = CorporateActionProcessor([action])

    processor.on_session_end(at_time(date(2026, 1, 2), time(16, 5)), portfolio)
    processor.on_session_start(
        at_time(date(2026, 1, 7), time(9, 25)), portfolio, _broker()
    )

    position = portfolio.position("000001.SZ")
    assert (position.quantity, position.sellable_quantity) == (1_100, 1_100)
    assert position.pending_listing_quantity == 0


def test_unheld_invalid_action_does_not_interrupt_backtest() -> None:
    """没有登记权益时，缺少派息日等字段不会影响账户。"""
    portfolio = Portfolio(100_000)
    action = CorporateAction(
        action_id="UNHELD",
        symbol="000001.SZ",
        record_date=date(2026, 1, 2),
        ex_date=date(2026, 1, 5),
        pay_date=None,
        listing_date=None,
        cash_dividend=0.5,
        cash_dividend_before_tax=None,
        stock_dividend=0,
    )
    processor = CorporateActionProcessor([action])

    processor.on_session_end(at_time(date(2026, 1, 2), time(16, 5)), portfolio)
    processor.on_session_start(
        at_time(date(2026, 1, 5), time(9, 25)), portfolio, _broker()
    )

    assert portfolio.cash == 100_000


def test_held_action_requires_a_settlement_date() -> None:
    """只有实际持有权益时，无法完成结算的记录才成为业务错误。"""
    portfolio = _portfolio_with_shares(1_000)
    action = CorporateAction(
        action_id="INVALID",
        symbol="000001.SZ",
        record_date=date(2026, 1, 2),
        ex_date=date(2026, 1, 5),
        pay_date=None,
        listing_date=None,
        cash_dividend=0.5,
        cash_dividend_before_tax=None,
        stock_dividend=0,
    )
    processor = CorporateActionProcessor([action])

    with pytest.raises(CorporateActionError, match="缺少派息日"):
        processor.on_session_end(
            at_time(date(2026, 1, 2), time(16, 5)), portfolio
        )


def test_load_uses_final_implemented_facts_and_merges_business_duplicates() -> None:
    """账户加载最终实施事实；晚可见和来源报告期差异不造成漏发或重发。"""
    implemented = {
        "symbol": "000001.SZ",
        "visible_at": at_time(date(2026, 1, 8), time(9, 25)),
        "end_date": date(2025, 12, 31),
        "div_proc": "实施",
        "record_date": date(2026, 1, 2),
        "ex_date": None,
        "pay_date": date(2026, 1, 6),
        "listing_date": None,
        "cash_dividend": 0.5,
        "cash_dividend_before_tax": 0.5,
        "stock_dividend": 0.0,
        "base_date": date(2026, 1, 2),
        "base_share": 1_000_000.0,
    }
    rows = [
        implemented,
        {**implemented, "end_date": date(2026, 1, 1)},
        {**implemented, "div_proc": "预案", "cash_dividend": 9.0},
    ]
    reader = _ActionReader(rows)
    config = BacktestConfig(
        date(2026, 1, 2),
        date(2026, 1, 7),
        symbols=("000001.SZ",),
    )
    processor = CorporateActionProcessor.load(cast(DataReader, reader), config)
    portfolio = _portfolio_with_shares(1_000)

    processor.on_session_end(at_time(date(2026, 1, 2), time(16, 5)), portfolio)
    processor.on_session_start(
        at_time(date(2026, 1, 6), time(9, 25)), portfolio, _broker()
    )

    assert reader.requested_at is not None
    assert reader.requested_at.date() == date.max
    assert portfolio.cash == pytest.approx(90_500)


def _portfolio_with_shares(quantity: int) -> Portfolio:
    """创建一个已经持有且可卖指定数量股票的测试账户。"""
    portfolio = Portfolio(100_000)
    portfolio.apply_fill(
        Fill(
            "F",
            "O",
            "000001.SZ",
            Side.BUY,
            at_time(date(2026, 1, 2), time(9, 30)),
            quantity,
            10,
            10,
            quantity * 10,
            0,
            0,
            0,
            0,
        )
    )
    portfolio.unlock_t1()
    return portfolio


def _broker() -> SimulatedBroker:
    """创建不参与成交、只供公司行动撤单使用的测试 Broker。"""
    return SimulatedBroker(BacktestConfig(date(2026, 1, 2), date(2026, 1, 7)))


class _ActionReader:
    """只实现 CorporateActionProcessor.load 所需形状的测试读取器。"""

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.requested_at: datetime | None = None
        table = SimpleNamespace(to_pylist=lambda: rows)
        corporate_actions = SimpleNamespace(
            dividends=lambda **_: SimpleNamespace(table=table)
        )
        self._view = SimpleNamespace(corporate_actions=corporate_actions)

    def at(self, as_of: datetime) -> object:
        self.requested_at = as_of
        return self._view
