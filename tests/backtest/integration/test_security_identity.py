"""使用构造代码历史验证纯更码；日期不代表任何真实证券的更码日。"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backtest import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.domain import OrderReason, OrderRequest, Side
from backtest.engine import BacktestEngine
from backtest.errors import CorporateActionError
from backtest.runner import default_source_config
from backtest.strategy import Strategy
from market_data import CodeInterval, DataCatalog, DataReader, SecurityCodeHistory
from market_data.identity import SecurityMappingError
from qmt_protocol import DividendFactor, HistoryBar
from qmt_receiver import QmtDataStore
from tushare_data import TABLE_SCHEMAS, TushareDataStore

OLD = "430047.BJ"
NEW = "920047.BJ"
SID = 1001
DAYS = tuple(date(2026, 1, day) for day in (5, 6, 7, 8))
TZ = ZoneInfo("Asia/Shanghai")


def at(day: date, hour: int = 17) -> datetime:
    return datetime.combine(day, time(hour), TZ)


def history() -> SecurityCodeHistory:
    return SecurityCodeHistory(
        [
            CodeInterval(SID, OLD, date(2020, 1, 1), DAYS[2]),
            CodeInterval(SID, NEW, DAYS[2]),
        ]
    )


def write_data(root: Path, mode: str, *, dividend: bool = False) -> None:
    def source(day: date) -> str:
        return NEW if mode == "new" or (mode == "mixed" and day >= DAYS[2]) else OLD

    datasets = {
        "daily": [
            {"ts_code": source(day), "trade_date": day, "open": 10.0, "close": 10.0, "vol": 10000.0}
            for day in DAYS
        ],
        # 因子故意使用与行情不同的源别名，验证先解析身份再连接。
        "adj_factor": [
            {
                "ts_code": OLD if source(day) == NEW else NEW,
                "trade_date": day,
                "adj_factor": 1.0 if day < DAYS[2] else 2.0,
            }
            for day in DAYS
        ],
        "stock_basic": [
            {
                "ts_code": NEW,
                "symbol": "920047",
                "list_date": date(2020, 1, 1),
                "exchange": "BSE",
                "curr_type": "CNY",
                "market": "北交所",
            }
        ],
        "stk_limit": [
            {"ts_code": source(day), "trade_date": day, "up_limit": 13.0, "down_limit": 7.0}
            for day in DAYS
        ],
        "trade_cal": [{"exchange": "SSE", "cal_date": day, "is_open": 1} for day in DAYS],
        "income": [
            {
                "ts_code": OLD,
                "end_date": date(2025, 9, 30),
                "f_ann_date": DAYS[0],
                "ann_date": DAYS[0],
                "report_type": "1",
                "comp_type": "1",
                "revenue": 100.0,
            },
            {
                "ts_code": NEW,
                "end_date": date(2025, 9, 30),
                "f_ann_date": DAYS[1],
                "ann_date": DAYS[1],
                "report_type": "4",
                "comp_type": "1",
                "revenue": 120.0,
            },
        ],
    }
    if dividend:
        datasets["dividend"] = [
            {
                "ts_code": code,
                "end_date": date(2025, 12, 31),
                "div_proc": "实施",
                "ann_date": DAYS[0],
                "imp_ann_date": DAYS[0],
                "record_date": DAYS[1],
                "ex_date": DAYS[2],
                "pay_date": DAYS[3],
                "div_listdate": DAYS[3],
                "cash_div": 0.1,
                "stk_div": 0.1,
            }
            for code in (OLD, NEW)
        ]
    with TushareDataStore(root) as store:
        for name, rows in datasets.items():
            store.write(name, pa.Table.from_pylist(rows, schema=TABLE_SCHEMAS[name]))
        store._mark_sync_all_completed("suspend_d", DAYS[0], DAYS[-1])


class Idle(Strategy):
    def on_event(self, context):
        return None


class BuyOnce(Strategy):
    def on_event(self, context):
        if context.event.session == DAYS[0]:
            return {OLD: 0.1, NEW: 0.1}
        return None


def config() -> BacktestConfig:
    return BacktestConfig(
        start_date=DAYS[0],
        end_date=DAYS[-1],
        symbols=(OLD, NEW),
        initial_cash=100000,
        volume_limit=None,
        slippage_bps=0,
        commission_rate=0,
        minimum_commission=0,
    )


@pytest.mark.parametrize("mode", ("old", "new", "mixed"))
def test_holdings_and_orders_survive_code_change(tmp_path: Path, mode: str) -> None:
    write_data(tmp_path / "tushare", mode)
    with DataCatalog(
        tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt", identities=history()
    ) as catalog:
        reader = DataReader(catalog, sources=default_source_config())
        engine = BacktestEngine(
            reader=reader,
            config=config(),
            sessions=DAYS,
            strategy=Idle(),
            actions=CorporateActionProcessor(()),
        )
        position = engine.portfolio.position(OLD)
        position.quantity = position.sellable_quantity = 1000
        position.last_price = 10
        engine.broker.submit(OrderRequest(OLD, Side.BUY, 100), at(DAYS[1]))
        result = engine.run()
    assert len(engine.portfolio.positions) == 1
    assert list(engine.portfolio.positions) == [SID]
    assert engine.portfolio.position(NEW) is position
    assert position.quantity == position.sellable_quantity == 1100
    assert position.symbol == NEW
    assert result.orders[0].symbol == OLD
    assert result.orders[0].sid == result.fills[0].sid == SID
    assert result.fills[0].symbol == NEW
    assert not any(update.reason == OrderReason.DELISTED for update in result.order_updates)
    assert [snapshot.total_equity for snapshot in result.equity] == pytest.approx(
        [110000, 110000, 109999.99, 109999.99]
    )


@pytest.mark.parametrize("mode", ("old", "new", "mixed"))
def test_alias_targets_and_dividends_execute_once(tmp_path: Path, mode: str) -> None:
    write_data(tmp_path / "tushare", mode, dividend=True)
    with DataCatalog(
        tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt", identities=history()
    ) as catalog:
        reader = DataReader(catalog, sources=default_source_config())
        actions = CorporateActionProcessor.load(reader, config())
        engine = BacktestEngine(
            reader=reader, config=config(), sessions=DAYS, strategy=BuyOnce(), actions=actions
        )
        result = engine.run()
    assert len(result.orders) == len(result.fills) == 1
    assert result.fills[0].quantity == 1000
    assert engine.portfolio.position(SID).quantity == 1100
    assert engine.portfolio.position(SID).sellable_quantity == 1100
    assert engine.portfolio.cash == pytest.approx(90099.9)
    assert engine.portfolio.dividend_receivable == 0
    assert len(engine.portfolio._entitlements) == 1
    assert next(iter(engine.portfolio._entitlements)).startswith(f"CA:{SID}:")


def test_backfilled_history_adjustment_and_financial_versions(tmp_path: Path) -> None:
    write_data(tmp_path / "tushare", "new")
    with DataCatalog(
        tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt", identities=history()
    ) as catalog:
        reader = DataReader(catalog, sources=default_source_config())
        before = reader.at(at(DAYS[1]))
        rows = before.market.bars(symbols=(OLD, NEW), frequency="1d", count=2).table.to_pylist()
        assert [row["symbol"] for row in rows] == [OLD, OLD]
        assert [row["sid"] for row in rows] == [SID, SID]
        assert before.reference.stocks(fields=()).table.to_pylist() == [{"symbol": OLD, "sid": SID}]
        original = catalog.connection.execute(
            "SELECT DISTINCT ts_code FROM tushare.daily"
        ).fetchall()
        assert original == [(NEW,)]
        latest = reader.at(at(DAYS[-1]))
        adjusted = latest.market.bars(
            symbols=(OLD,), frequency="1d", count=4, adjustment="forward", fields=("close",)
        ).table.to_pylist()
        assert [row["close"] for row in adjusted] == [5, 5, 10, 10]
        assert {row["symbol"] for row in adjusted} == {NEW}
        for view, expected in ((before, 100), (latest, 120)):
            financials = view.fundamentals.statements(
                kind="income", symbols=(OLD, NEW), periods=1, fields=("operating_revenue",)
            ).table.to_pylist()
            assert len(financials) == 1
            assert financials[0]["operating_revenue"] == expected
            assert financials[0]["sid"] == SID


def test_mapping_snapshot_and_errors(tmp_path: Path) -> None:
    mapping = history()
    path = tmp_path / "security_code_history.parquet"
    pq.write_table(mapping.table(), path)
    loaded = SecurityCodeHistory.load(path)
    reordered = SecurityCodeHistory(
        reversed(
            [CodeInterval(SID, OLD, date(2020, 1, 1), DAYS[2]), CodeInterval(SID, NEW, DAYS[2])]
        )
    )
    assert loaded.snapshot_id == mapping.snapshot_id == reordered.snapshot_id
    assert loaded.code_at(SID, DAYS[1]) == OLD
    assert loaded.code_at(SID, DAYS[2]) == NEW
    with pytest.raises(SecurityMappingError, match="缺少映射"):
        loaded.sid("000001.SZ")
    with pytest.raises(SecurityMappingError, match="缺少有效交易代码"):
        loaded.code_at(SID, date(2019, 1, 1))
    with pytest.raises(SecurityMappingError, match="归属冲突"):
        SecurityCodeHistory([CodeInterval(1, OLD, DAYS[0]), CodeInterval(2, OLD, DAYS[0])])
    with pytest.raises(SecurityMappingError, match="有效期冲突"):
        SecurityCodeHistory([CodeInterval(1, OLD, DAYS[0]), CodeInterval(1, NEW, DAYS[1])])
    # 没有可靠资料时两个代码是不同 sid，不因后缀或相似数字被自动合并。
    separate = SecurityCodeHistory([CodeInterval(1, OLD, DAYS[0]), CodeInterval(2, NEW, DAYS[0])])
    assert separate.sid(OLD) != separate.sid(NEW)


def test_missing_mapping_fails_before_trading(tmp_path: Path) -> None:
    with DataCatalog(
        tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt", identities=history()
    ) as catalog:
        reader = DataReader(catalog, sources=default_source_config())
        with pytest.raises(SecurityMappingError, match="缺少映射"):
            reader.at(at(DAYS[0])).market.bars(symbols=("000001.SZ",), frequency="1d", count=1)


def test_pure_code_change_preserves_cash_quantity_and_equity(tmp_path: Path) -> None:
    write_data(tmp_path / "tushare", "mixed")
    with DataCatalog(
        tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt", identities=history()
    ) as catalog:
        reader = DataReader(catalog, sources=default_source_config())
        engine = BacktestEngine(
            reader=reader,
            config=config(),
            sessions=DAYS,
            strategy=Idle(),
            actions=CorporateActionProcessor(()),
        )
        position = engine.portfolio.position(OLD)
        position.quantity = position.sellable_quantity = 1000
        position.last_price = 10
        result = engine.run()
        latest = (
            reader.at(at(DAYS[-1]))
            .market.bars(symbols=(OLD, NEW), frequency="1d", count=2)
            .table.to_pylist()
        )
    assert [row["interval_start"].date() for row in latest] == list(DAYS[-2:])
    assert all(snapshot.cash == 100000 for snapshot in result.equity)
    assert all(snapshot.total_equity == 110000 for snapshot in result.equity)
    assert position.quantity == position.sellable_quantity == 1000
    assert not result.order_updates


def test_conflicting_alias_prices_are_not_hidden_by_count(tmp_path: Path) -> None:
    write_data(tmp_path / "tushare", "new")
    with TushareDataStore(tmp_path / "tushare") as store:
        store.write(
            "daily",
            pa.Table.from_pylist(
                [{"ts_code": OLD, "trade_date": DAYS[-1], "open": 1000.0, "close": 1000.0}],
                schema=TABLE_SCHEMAS["daily"],
            ),
        )
    with DataCatalog(
        tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt", identities=history()
    ) as catalog:
        reader = DataReader(catalog, sources=default_source_config())
        with pytest.raises(SecurityMappingError, match="别名存在冲突"):
            reader.at(at(DAYS[-1])).market.bars(symbols=(NEW,), frequency="1d", count=1)


def test_qmt_minute_history_joins_alias_factor_and_sync_coverage(tmp_path: Path) -> None:
    with QmtDataStore(tmp_path / "qmt") as store:
        store.write_intraday({NEW: [HistoryBar(index=20260105093100, close=10.0)]}, "1m", "none")
        store.write_dividend_factors(
            {OLD: [DividendFactor(date="20260107", time=at(DAYS[2], 0).timestamp() * 1000, dr=2.0)]}
        )
        store.mark_sync_completed("dividend_factors", OLD, DAYS[0], DAYS[-1])
    with DataCatalog(
        tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt", identities=history()
    ) as catalog:
        reader = DataReader(catalog, sources=default_source_config())
        before = (
            reader.at(at(DAYS[0]))
            .market.bars(symbols=(OLD,), frequency="1m", count=1)
            .table.to_pylist()
        )
        after = (
            reader.at(at(DAYS[-1]))
            .market.bars(symbols=(NEW,), frequency="1m", count=1, adjustment="forward")
            .table.to_pylist()
        )
    assert before[0]["symbol"] == OLD
    assert after[0]["symbol"] == NEW
    assert before[0]["sid"] == after[0]["sid"] == SID
    assert after[0]["close"] == 5.0


@pytest.mark.parametrize("policy", ("error", "write_off"))
def test_delisting_is_distinct_from_code_change(
    tmp_path: Path, policy: Literal["error", "write_off"]
) -> None:
    identities = SecurityCodeHistory(
        [
            CodeInterval(SID, OLD, date(2020, 1, 1), DAYS[1]),
            CodeInterval(SID, NEW, DAYS[1], DAYS[2]),
        ]
    )
    with TushareDataStore(tmp_path / "tushare") as store:
        store.write(
            "stock_basic",
            pa.Table.from_pylist(
                [
                    {
                        "ts_code": NEW,
                        "list_date": date(2020, 1, 1),
                        "delist_date": DAYS[2],
                        "curr_type": "CNY",
                        "exchange": "BSE",
                    }
                ],
                schema=TABLE_SCHEMAS["stock_basic"],
            ),
        )
        store.write(
            "daily",
            pa.Table.from_pylist(
                [
                    {"ts_code": NEW, "trade_date": day, "open": 10.0, "close": 10.0}
                    for day in DAYS[:2]
                ],
                schema=TABLE_SCHEMAS["daily"],
            ),
        )
    with DataCatalog(
        tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt", identities=identities
    ) as catalog:
        reader = DataReader(catalog, sources=default_source_config())
        lifecycle = reader.security_lifecycles(symbols=(OLD, NEW)).to_pylist()
        assert len(lifecycle) == 1
        assert lifecycle[0]["sid"] == SID
        assert lifecycle[0]["delisting_date"] == DAYS[2]
        engine = BacktestEngine(
            reader=reader,
            config=replace(config(), delisting_policy=policy),
            sessions=DAYS,
            strategy=Idle(),
            actions=CorporateActionProcessor(()),
        )
        position = engine.portfolio.position(OLD)
        position.quantity = position.sellable_quantity = 1000
        position.last_price = 10.0
        engine.broker.submit(OrderRequest(OLD, Side.BUY, 100), at(DAYS[1]))
        if policy == "error":
            with pytest.raises(CorporateActionError, match="退市经济结算"):
                engine.run()
            assert position.quantity == 1000
        else:
            result = engine.run()
            assert position.quantity == 0
            assert result.equity[-1].total_equity == 100000
            assert result.order_updates[-1].reason == OrderReason.DELISTED


def test_action_ids_ignore_alias_and_input_order(tmp_path: Path, monkeypatch) -> None:
    write_data(tmp_path / "tushare", "new", dividend=True)
    with DataCatalog(
        tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt", identities=history()
    ) as catalog:
        reader = DataReader(catalog, sources=default_source_config())
        rows = reader.implemented_dividends(symbols=(OLD, NEW)).to_pylist()
        assert len(rows) == 1
        # 在业务边界再提供相同 sid 的两份来源别名副本，验证业务事件身份稳定。
        rows = [{**rows[0], "symbol": OLD}, {**rows[0], "symbol": NEW}]
        action_ids = []
        for input_rows in (rows, list(reversed(rows)), rows[:1], rows[1:]):
            table = pa.Table.from_pylist(input_rows)
            monkeypatch.setattr(reader, "implemented_dividends", lambda table=table, **_: table)
            processor = CorporateActionProcessor.load(reader, config())
            actions = processor._record[DAYS[1]]
            assert len(actions) == 1
            action_ids.append(actions[0].action_id)
        assert len(set(action_ids)) == 1


@pytest.mark.parametrize("dataset", ("daily", "adj_factor"))
def test_future_alias_conflicts_do_not_break_earlier_pit_queries(
    tmp_path: Path, dataset: str
) -> None:
    write_data(tmp_path / "tushare", "new")
    conflicting = (
        {"ts_code": OLD, "trade_date": DAYS[-1], "open": 1000.0, "close": 1000.0}
        if dataset == "daily"
        else {"ts_code": NEW, "trade_date": DAYS[-1], "adj_factor": 1000.0}
    )
    with TushareDataStore(tmp_path / "tushare") as store:
        store.write(dataset, pa.Table.from_pylist([conflicting], schema=TABLE_SCHEMAS[dataset]))
    with DataCatalog(
        tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt", identities=history()
    ) as catalog:
        reader = DataReader(catalog, sources=default_source_config())
        earlier = (
            reader.at(at(DAYS[1]))
            .market.bars(symbols=(OLD,), frequency="1d", count=2, adjustment="forward")
            .table.to_pylist()
        )
        assert [row["close"] for row in earlier] == [10, 10]
        with pytest.raises(SecurityMappingError, match="别名存在冲突"):
            reader.at(at(DAYS[-1])).market.bars(
                symbols=(NEW,), frequency="1d", count=4, adjustment="forward"
            )


def test_master_aliases_with_different_unused_names_share_one_lifecycle(tmp_path: Path) -> None:
    write_data(tmp_path / "tushare", "new")
    with TushareDataStore(tmp_path / "tushare") as store:
        store.write(
            "stock_basic",
            pa.Table.from_pylist(
                [
                    {
                        "ts_code": OLD,
                        "symbol": "430047",
                        "name": "源中旧简称",
                        "list_date": date(2020, 1, 1),
                        "exchange": "BSE",
                        "curr_type": "CNY",
                        "market": "北交所",
                    }
                ],
                schema=TABLE_SCHEMAS["stock_basic"],
            ),
        )
    with DataCatalog(
        tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt", identities=history()
    ) as catalog:
        reader = DataReader(catalog, sources=default_source_config())
        lifecycles = reader.security_lifecycles(symbols=(OLD, NEW)).to_pylist()
        assert len(lifecycles) == 1
        assert lifecycles[0]["sid"] == SID
        stocks = reader.at(at(DAYS[0])).reference.stocks(fields=()).table.to_pylist()
        assert stocks == [{"sid": SID, "symbol": OLD}]
