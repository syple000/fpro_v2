from datetime import date, datetime, time, timedelta
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
from qmt_protocol import BarQuote, HistoryBar, SequencedQuote
from qmt_receiver import QmtDataStore
from tests.backtest.conftest import timestamp


class IdleStrategy(Strategy):
    def on_event(self, context):
        return None


@pytest.mark.parametrize("frequency,period,first_end", [("1m", "1m", 93100), ("5m", "5m", 93500)])
@pytest.mark.parametrize(
    "source,availability",
    [("downloaded", "historical"), ("subscription", "historical"), ("subscription", "received")],
)
def test_qmt_end_labels_match_auction_lunch_and_close(
    tmp_path: Path,
    frequency: str,
    period: Literal["1m", "5m"],
    first_end: int,
    source: str,
    availability: Literal["historical", "received"],
) -> None:
    session = date(2026, 1, 5)
    rows = [
        HistoryBar(index=20260105000000 + label, open=price, close=price)
        for label, price in [(93000, 10), (first_end, 11), (113000, 12), (150000, 13)]
    ]
    with QmtDataStore(tmp_path / "qmt") as store:
        if source == "downloaded":
            store.write_intraday({"000001.SZ": rows}, period, "none")
        else:
            # 构造结束标签的协议输入；不是实机推送样本。
            for seq, row in enumerate(rows, 1):
                label = datetime.strptime(str(row.index), "%Y%m%d%H%M%S").time()
                end = timestamp(session, label)
                store.append_quotes(
                    [
                        SequencedQuote(
                            seq=seq,
                            code="000001.SZ",
                            period=period,
                            source="stock",
                            subscription="000001.SZ",
                            received_at=int(end.timestamp() * 1_000_000),
                            quote=BarQuote(
                                time=int(end.timestamp() * 1_000), open=row.open, close=row.close
                            ),
                        )
                    ]
                )
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        assert QmtAdapter(catalog).realtime_time_label == "end"
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"market.intraday_bars": "qmt"}),
            bar_availability=availability,
        )
        assert reader.snapshot_metadata()["qmt"] == {
            "bar_availability": availability,
            "history_time_label": "end",
            "realtime_time_label": "end",
        }
        engine = BacktestEngine(
            reader=reader,
            config=BacktestConfig(
                session, session, frequency=frequency, bar_availability=availability
            ),
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
        duration = timedelta(minutes=1 if frequency == "1m" else 5)
        assert matched[-2].interval_start == timestamp(session, time(11, 30)) - duration
        assert matched[-1].interval_start == timestamp(session, time(15)) - duration


@pytest.mark.parametrize("frequency,minutes", [("1m", 1), ("5m", 5)])
def test_received_end_label_is_hidden_until_both_end_and_arrival(
    tmp_path: Path, frequency: Literal["1m", "5m"], minutes: int
) -> None:
    session = date(2026, 1, 5)
    start = timestamp(session, time(10))
    end = start + timedelta(minutes=minutes)
    with QmtDataStore(tmp_path / "qmt") as store:
        store.append_quotes(
            [
                SequencedQuote(
                    seq=seq,
                    code=symbol,
                    period=frequency,
                    source="stock",
                    subscription=symbol,
                    received_at=int(received.timestamp() * 1_000_000),
                    quote=BarQuote(time=int(end.timestamp() * 1_000), close=11),
                )
                for seq, symbol, received in [
                    (1, "000001.SZ", end - timedelta(seconds=20)),
                    (2, "600000.SH", end + timedelta(seconds=20)),
                ]
            ]
        )
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"market.intraday_bars": "qmt"}),
            bar_availability="received",
        )
        for as_of, expected_symbols in [
            (end - timedelta(seconds=1), []),
            (end, ["000001.SZ"]),
            (end + timedelta(seconds=20), ["000001.SZ", "600000.SH"]),
        ]:
            rows = (
                reader.at(as_of)
                .market.bars(symbols=("000001.SZ", "600000.SH"), frequency=frequency, count=1)
                .table.to_pylist()
            )
            assert [row["symbol"] for row in rows] == expected_symbols
            assert all(
                row["interval_start"] == start and row["interval_end"] == end for row in rows
            )


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
