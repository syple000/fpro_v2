from dataclasses import replace
from datetime import date, time
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from backtest.broker import SimulatedBroker
from backtest.clock import at_time
from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.domain import CorporateAction, Fill, Side
from backtest.errors import CorporateActionError
from backtest.portfolio import Portfolio
from market_data import DataCatalog, DataReader, SourceConfig
from models import IMPLEMENTED_DIVIDEND_SCHEMA
from tushare_data import TABLE_SCHEMAS, TushareDataStore


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


@pytest.mark.parametrize("kind", ["cash", "stock"])
@pytest.mark.parametrize("settlement", [date(2026, 1, 2), date(2026, 1, 5)])
@pytest.mark.parametrize("held", [False, True])
def test_settlement_cannot_precede_recognition(
    kind: str, settlement: date, held: bool
) -> None:
    """有权益时在登记日就拒绝不可处理的顺序；无权益仍忽略异常记录。"""
    portfolio = _portfolio_with_shares(1_000) if held else Portfolio(100_000)
    action = CorporateAction(
        action_id="EARLY",
        symbol="000001.SZ",
        record_date=date(2026, 1, 2),
        ex_date=date(2026, 1, 6),
        pay_date=settlement if kind == "cash" else None,
        listing_date=settlement if kind == "stock" else None,
        cash_dividend=0.5 if kind == "cash" else 0,
        cash_dividend_before_tax=None,
        stock_dividend=0.1 if kind == "stock" else 0,
    )
    processor = CorporateActionProcessor([action])
    if held:
        with pytest.raises(CorporateActionError, match="不晚于股权登记日|早于除权日"):
            processor.on_session_end(at_time(date(2026, 1, 2), time(16, 5)), portfolio)
    else:
        processor.on_session_end(at_time(date(2026, 1, 2), time(16, 5)), portfolio)
    assert portfolio.dividend_receivable == 0
    assert portfolio.cash == (90_000 if held else 100_000)


def test_cash_and_stock_can_settle_on_ex_date() -> None:
    """除权日先确认应收及红股，再在同日日初完成派息和上市。"""
    portfolio = _portfolio_with_shares(1_000)
    action = CorporateAction(
        action_id="SAME_DAY",
        symbol="000001.SZ",
        record_date=date(2026, 1, 2),
        ex_date=date(2026, 1, 5),
        pay_date=date(2026, 1, 5),
        listing_date=date(2026, 1, 5),
        cash_dividend=0.5,
        cash_dividend_before_tax=None,
        stock_dividend=0.1,
    )
    processor = CorporateActionProcessor([action])
    processor.on_session_end(at_time(date(2026, 1, 2), time(16, 5)), portfolio)
    processor.on_session_start(at_time(date(2026, 1, 5), time(9, 25)), portfolio, _broker())
    assert portfolio.cash == pytest.approx(90_500)
    assert portfolio.dividend_receivable == 0
    assert portfolio.position("000001.SZ").sellable_quantity == 1_100


@pytest.mark.parametrize("field", ["record_date", "ex_date", "pay_date", "listing_date"])
@pytest.mark.parametrize("held", [False, True])
def test_non_session_business_date_is_rejected_only_with_entitlement(
    field: str, held: bool
) -> None:
    """周日业务日期不能静默错过；无持仓记录仍不阻断回放。"""
    action = CorporateAction(
        action_id="OFF_CALENDAR",
        symbol="000001.SZ",
        record_date=date(2026, 1, 2),
        ex_date=None,
        pay_date=date(2026, 1, 5),
        listing_date=date(2026, 1, 5),
        cash_dividend=0.5,
        cash_dividend_before_tax=None,
        stock_dividend=0.1,
    )
    action = replace(action, **{field: date(2026, 1, 4)})
    processor = CorporateActionProcessor([action])
    processor.set_sessions(
        [date(2026, 1, 2), date(2026, 1, 5)],
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 5),
    )
    portfolio = _portfolio_with_shares(1_000) if held else Portfolio(100_000)
    if held:
        with pytest.raises(CorporateActionError, match="2026-01-04 不在回放交易日历中"):
            processor.on_session_end(at_time(date(2026, 1, 2), time(16, 5)), portfolio)
    else:
        processor.on_session_end(at_time(date(2026, 1, 2), time(16, 5)), portfolio)
        assert portfolio.entitlement("OFF_CALENDAR") == 0


def test_future_settlement_outside_run_can_remain_receivable() -> None:
    action = CorporateAction(
        action_id="FUTURE",
        symbol="000001.SZ",
        record_date=date(2026, 1, 2),
        ex_date=date(2026, 1, 5),
        pay_date=date(2026, 1, 6),
        listing_date=None,
        cash_dividend=0.5,
        cash_dividend_before_tax=None,
        stock_dividend=0,
    )
    processor = CorporateActionProcessor([action])
    processor.set_sessions(
        [date(2026, 1, 2), date(2026, 1, 5)],
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 5),
    )
    portfolio = _portfolio_with_shares(1_000)
    processor.on_session_end(at_time(date(2026, 1, 2), time(16, 5)), portfolio)
    processor.on_session_start(at_time(date(2026, 1, 5), time(9, 25)), portfolio, _broker())
    assert portfolio.dividend_receivable == 500


@pytest.mark.parametrize(
    "changed",
    [
        {"cash_dividend": 0.2},
        {"cash_dividend": 0.2, "end_date": date(2024, 12, 31)},
        {"stock_dividend": 0.2, "listing_date": date(2026, 1, 6)},
        {"pay_date": date(2026, 1, 7)},
        {"record_date": date(2026, 1, 5)},
        {"record_date": date(2026, 1, 8), "pay_date": date(2026, 1, 9)},
    ],
)
@pytest.mark.parametrize("held", [False, True])
def test_conflicting_implementation_versions_are_never_added_together(
    changed: dict[str, object], held: bool
) -> None:
    """金额、比例和日期更正都不能重复执行；区间外修订也参与冲突识别。"""
    original = _implemented_row()
    rows = [original, {**original, **changed}]
    processor = CorporateActionProcessor.load(
        cast(DataReader, _ActionReader(rows)),
        BacktestConfig(date(2026, 1, 2), date(2026, 1, 7)),
    )
    portfolio = _portfolio_with_shares(1_000) if held else Portfolio(100_000)
    if held:
        with pytest.raises(CorporateActionError, match="实施版本冲突"):
            processor.on_session_end(at_time(date(2026, 1, 2), time(16, 5)), portfolio)
    else:
        for day in (2, 5, 6, 7):
            at = at_time(date(2026, 1, day), time(9, 25))
            processor.on_session_start(at, portfolio, _broker())
            processor.on_session_end(at_time(at.date(), time(16, 5)), portfolio)
    assert portfolio.cash == (90_000 if held else 100_000)
    assert portfolio.dividend_receivable == 0


@pytest.mark.parametrize("same_period", [False, True])
def test_distinct_announcements_and_record_dates_execute_independently(same_period: bool) -> None:
    first = _implemented_row()
    second = {
        **first,
        "end_date": first["end_date"] if same_period else date(2026, 1, 1),
        "ann_date": date(2026, 1, 1),
        "record_date": date(2026, 1, 5),
        "pay_date": date(2026, 1, 7),
        "cash_dividend": 0.2,
    }
    processor = CorporateActionProcessor.load(
        cast(DataReader, _ActionReader([first, second])),
        BacktestConfig(date(2026, 1, 2), date(2026, 1, 7)),
    )
    portfolio = _portfolio_with_shares(1_000)
    for day in (2, 5, 6, 7):
        processor.on_session_start(at_time(date(2026, 1, day), time(9, 25)), portfolio, _broker())
        processor.on_session_end(at_time(date(2026, 1, day), time(16, 5)), portfolio)
    assert portfolio.cash == pytest.approx(90_300)


def _implemented_row() -> dict[str, object]:
    return {
        "symbol": "000001.SZ",
        "end_date": date(2025, 12, 31),
        "ann_date": date(2025, 12, 30),
        "div_proc": "实施",
        "record_date": date(2026, 1, 2),
        "pay_date": date(2026, 1, 6),
        "cash_dividend": 0.1,
        "stock_dividend": 0,
    }


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

    assert portfolio.cash == pytest.approx(90_500)


def test_implemented_fact_without_announcement_is_paid(tmp_path: Path) -> None:
    """真实适配路径不依赖公告日期或公告可见性日历，且不把预案入账。"""
    rows = [
        {
            "ts_code": "000001.SZ",
            "end_date": date(2025, 12, 31),
            "div_proc": status,
            "record_date": date(2026, 1, 2),
            "ex_date": date(2026, 1, 5),
            "pay_date": date(2026, 1, 6),
            "cash_div": amount,
        }
        for status, amount in (("实施", 0.5), ("预案", 9.0))
    ]
    with TushareDataStore(tmp_path / "tushare") as store:
        store.write("dividend", pa.Table.from_pylist(rows, schema=TABLE_SCHEMAS["dividend"]))
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog, sources=SourceConfig(routes={"corporate_actions.dividends": "tushare"})
        )
        processor = CorporateActionProcessor.load(
            reader, BacktestConfig(date(2026, 1, 2), date(2026, 1, 7))
        )
    portfolio = _portfolio_with_shares(1_000)
    processor.on_session_end(at_time(date(2026, 1, 2), time(16, 5)), portfolio)
    processor.on_session_start(at_time(date(2026, 1, 5), time(9, 25)), portfolio, _broker())
    assert portfolio.dividend_receivable == 500
    processor.on_session_start(at_time(date(2026, 1, 6), time(9, 25)), portfolio, _broker())
    assert portfolio.cash == pytest.approx(90_500)
    assert portfolio.dividend_receivable == 0


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
        self._table = pa.Table.from_pylist(rows, schema=IMPLEMENTED_DIVIDEND_SCHEMA)

    def implemented_dividends(self, *, symbols: object) -> pa.Table:
        return self._table
