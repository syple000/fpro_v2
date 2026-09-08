from datetime import date, time
from pathlib import Path
from typing import Literal

import pytest

from backtest.clock import market_timeline
from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.engine import BacktestEngine
from backtest.strategy import Strategy
from market_data import DataCatalog, DataReader, SourceConfig
from market_data.adapters import QmtAdapter
from qmt_protocol import HistoryBar
from qmt_receiver import QmtDataStore
from tests.backtest.conftest import timestamp


class IdleStrategy(Strategy):
    def on_event(self, context):
        return None


@pytest.mark.parametrize("frequency,period,first_end", [("1m", "1m", 93100), ("5m", "5m", 93500)])
def test_qmt_end_labels_match_auction_lunch_and_close(
    tmp_path: Path,
    frequency: str,
    period: Literal["1m", "5m"],
    first_end: int,
) -> None:
    session = date(2026, 1, 5)
    with QmtDataStore(tmp_path / "qmt") as store:
        store.write_intraday(
            {
                "000001.SZ": [
                    HistoryBar(index=20260105000000 + label, open=price, close=price)
                    for label, price in [(93000, 10), (first_end, 11), (113000, 12), (150000, 13)]
                ]
            },
            period,
            "none",
        )
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(catalog, sources=SourceConfig(routes={"market.intraday_bars": "qmt"}))
        engine = BacktestEngine(
            reader=reader,
            config=BacktestConfig(session, session, frequency=frequency),
            sessions=(session,),
            strategy=IdleStrategy(),
            actions=CorporateActionProcessor(()),
        )
        matched = []
        for event in market_timeline((session,), frequency):
            if event.kind in {"auction", "bar"}:
                matched.extend(engine._read_bars(event, reader.at(event.at)).values())
        assert [bar.close for bar in matched] == [10, 11, 12, 13]
        assert matched[0].interval_start == timestamp(session, time(9, 15))
        assert matched[1].interval_start == timestamp(session, time(9, 30))


def test_qmt_start_label_dataset_remains_explicitly_supported(tmp_path: Path) -> None:
    session = date(2026, 1, 5)
    with QmtDataStore(tmp_path / "qmt") as store:
        store.write_intraday(
            {"000001.SZ": [HistoryBar(index=20260105145900, close=13)]}, "1m", "none"
        )
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"market.intraday_bars": "qmt_start"}),
            adapters={"qmt_start": QmtAdapter(catalog, history_time_label="start")},
        )
        row = (
            reader.at(timestamp(session, time(15)))
            .market.bars(
                symbols=("000001.SZ",),
                frequency="1m",
                start=timestamp(session, time(14, 59)),
            )
            .table.to_pylist()[0]
        )
        assert row["interval_start"] == timestamp(session, time(14, 59))
        assert row["interval_end"] == timestamp(session, time(15))
