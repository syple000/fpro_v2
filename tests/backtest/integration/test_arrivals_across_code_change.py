"""构造跨更码日的接收回放；日期仅用于测试，不代表真实更码事实。"""

import json
from datetime import date, time
from pathlib import Path

import pytest

from backtest.clock import at_time
from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.domain import OrderReason, Side
from backtest.engine import BacktestEngine
from backtest.metadata import capture_run_metadata
from backtest.metrics import calculate_metrics
from backtest.output import write_results
from backtest.runner import default_source_config
from backtest.strategy import Strategy, StrategyContext
from market_data import DataCatalog, DataReader
from qmt_protocol import BarQuote, SequencedQuote
from qmt_receiver import QmtDataStore
from tests.backtest.integration.test_bar_arrivals import HOURS
from tests.backtest.integration.test_security_identity import (
    DAYS,
    NEW,
    OLD,
    SID,
    history,
    write_data,
)

BEFORE, AFTER = DAYS[1:3]


class ObserveAndOrder(Strategy):
    def __init__(self) -> None:
        self.holdings: list[tuple[str, float]] = []

    def on_event(self, context: StrategyContext) -> None:
        holding = context.account.holdings[0]
        self.holdings.append((holding.symbol, holding.market_value))
        if context.event.at == at_time(BEFORE, time(9, 33)):
            context.order(OLD, Side.BUY, 100)


def received_bar(
    seq: int,
    code: str,
    day: date,
    start: time,
    receipt_day: date,
    receipt: time,
    close: float,
) -> SequencedQuote:
    return SequencedQuote(
        seq=seq,
        code=code,
        period="1m",
        source="market",
        subscription="BJ",
        received_at=int(at_time(receipt_day, receipt).timestamp() * 1_000_000),
        quote=BarQuote(
            time=int(at_time(day, start).timestamp() * 1_000_000),
            open=10,
            high=max(10, close),
            low=min(10, close),
            close=close,
            volume=1000,
        ),
    )


@pytest.mark.parametrize("has_new_day_bar", [False, True])
def test_received_bars_keep_identity_price_order_and_metadata_across_rename(
    tmp_path: Path,
    has_new_day_bar: bool,
) -> None:
    identities = history()
    write_data(tmp_path / "tushare", "mixed")
    records = [
        received_bar(1, OLD, BEFORE, time(9, 32), BEFORE, time(9, 33), 11),
        # 更早的旧代码 Bar 到更码后才到达，不能覆盖已经见过的后续价格。
        received_bar(2, OLD, BEFORE, time(9, 30), AFTER, time(9, 32, 30), 9),
    ]
    if has_new_day_bar:
        records.append(received_bar(3, NEW, AFTER, time(9, 31), AFTER, time(9, 32), 12))
    with QmtDataStore(tmp_path / "qmt") as store:
        store.append_quotes(records)
    config = BacktestConfig(
        BEFORE,
        AFTER,
        frequency="1m",
        market=HOURS,
        symbols=(NEW,),
        volume_limit=None,
        bar_availability="received",
    )
    strategy = ObserveAndOrder()
    with DataCatalog(
        tushare_root=tmp_path / "tushare",
        qmt_root=tmp_path / "qmt",
        identities=identities,
    ) as catalog:
        reader = DataReader(
            catalog,
            sources=default_source_config(),
            bar_availability="received",
            qmt_history_time_label="start",
            qmt_realtime_time_label="start",
        )
        metadata = capture_run_metadata(reader, strategy)
        engine = BacktestEngine(
            reader=reader,
            config=config,
            sessions=(BEFORE, AFTER),
            strategy=strategy,
            actions=CorporateActionProcessor(()),
        )
        position = engine.portfolio.position(SID)
        position.quantity = position.sellable_quantity = 1000
        position.last_price = 10
        result = engine.run()
        write_results(
            tmp_path / "run", config, result, calculate_metrics(result, config), metadata=metadata
        )

    latest_value = 12_000 if has_new_day_bar else 11_000
    assert strategy.holdings[:3] == [(OLD, 10_000), (OLD, 10_000), (OLD, 11_000)]
    assert strategy.holdings[3:] == [(NEW, 11_000), (NEW, latest_value), (NEW, latest_value)]
    assert [row.market_value for row in result.equity] == [11_000, latest_value]
    assert result.equity[-1].stale_position_count == 1
    assert result.fills == ()
    assert result.order_updates[-1].reason is OrderReason.MISSING_OPEN
    assert set(engine.portfolio.positions) == {SID}
    assert engine.portfolio.positions[SID].symbol == NEW
    coverage = result.market_data_coverage
    assert coverage is not None
    assert coverage.bar_count == (2 if has_new_day_bar else 1)
    assert coverage.requested_symbols_without_bars == ()
    assert coverage.sessions_without_bars == (() if has_new_day_bar else (AFTER,))
    saved = json.loads((tmp_path / "run" / "run_metadata.json").read_text())
    assert saved["security_code_history"]["snapshot_id"] == identities.snapshot_id
    assert [row["code"] for row in saved["security_code_history"]["rows"]] == [OLD, NEW]
    assert saved["data"]["qmt"] == {
        "bar_availability": "received",
        "history_time_label": "start",
        "realtime_time_label": "start",
    }
