from datetime import date, time, timedelta
from pathlib import Path
from typing import Literal

import pytest

from backtest.clock import market_timeline
from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.engine import BacktestEngine
from backtest.strategy import Strategy
from market_data import DataCatalog, DataReader, SourceConfig
from qmt_protocol import HistoryBar
from qmt_receiver import QmtDataStore
from tests.backtest.conftest import timestamp


class IdleStrategy(Strategy):
    def on_event(self, context):
        return None


@pytest.mark.parametrize(
    "frequency,period,minutes,first_end",
    [
        ("1m", "1m", 1, 93100),
        ("5m", "5m", 5, 93500),
        ("15m", "15m", 15, 94500),
        ("30m", "30m", 30, 100000),
        ("60m", "1h", 60, 103000),
    ],
)
def test_qmt_end_labels_match_auction_lunch_and_close(
    tmp_path: Path,
    frequency: str,
    period: Literal["1m", "5m", "15m", "30m", "1h"],
    minutes: int,
    first_end: int,
) -> None:
    session = date(2026, 1, 5)
    rows = [
        HistoryBar(index=20260105000000 + label, open=price, close=price)
        for label, price in [(93000, 10), (first_end, 11), (113000, 12), (150000, 13)]
    ]
    with QmtDataStore(tmp_path / "qmt") as store:
        store.write_intraday({"000001.SZ": rows}, period, "none")
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"market.intraday_bars": "qmt"}),
        )
        assert reader.snapshot_metadata()["qmt"] == {
            "intraday_bars": "downloaded",
        }
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
                matched.extend(
                    bar
                    for bar in engine._read_bars(event, reader.at(event.at)).values()
                    if bar.interval_start == event.interval_start
                )
        assert [bar.close for bar in matched] == [10, 11, 12, 13]
        assert matched[0].interval_start == timestamp(session, time(9, 15))
        assert matched[1].interval_start == timestamp(session, time(9, 30))
        duration = timedelta(minutes=minutes)
        assert matched[-2].interval_start == timestamp(session, time(11, 30)) - duration
        assert matched[-1].interval_start == timestamp(session, time(15)) - duration
