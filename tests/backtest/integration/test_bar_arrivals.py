from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import date, time
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pytest

from backtest.clock import MarketHours, at_time
from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.domain import OrderReason, OrderRequest, Side
from backtest.engine import BacktestEngine
from backtest.errors import ConfigurationError
from backtest.runner import default_source_config
from backtest.strategy import Strategy, StrategyContext
from market_data import DataCatalog, DataReader
from qmt_protocol import BarQuote, HistoryBar, SequencedQuote
from qmt_receiver import QmtDataStore
from tushare_data import TABLE_SCHEMAS, TushareDataStore

DAY = date(2026, 1, 5)
SYMBOL = "000001.SZ"
HOURS = MarketHours(
    segments=((time(9, 30), time(9, 33)),), daily_bar_at=time(9, 33), session_end=time(9, 34)
)


class ObservePrices(Strategy):
    def __init__(self) -> None:
        self.values: list[float] = []

    def on_event(self, context: StrategyContext) -> None:
        self.values.append(context.account.market_value)


def quote(seq: int, start: time, received: time, close: float) -> SequencedQuote:
    return SequencedQuote(
        seq=seq, code=SYMBOL, period="1m", source="market", subscription="SZ",
        received_at=int(at_time(DAY, received).timestamp() * 1_000_000),
        quote=BarQuote(
            time=int(at_time(DAY, start).timestamp() * 1_000_000),
            open=10, high=max(10, close), low=min(10, close), close=close, volume=1000,
        ),
    )


@contextmanager
def data_reader(
    tmp_path: Path, quotes: Sequence[SequencedQuote], mode: Literal["historical", "received"]
) -> Iterator[DataReader]:
    with TushareDataStore(tmp_path / "tushare") as store:
        store.write("stock_basic", pa.Table.from_pylist([{
            "ts_code": SYMBOL, "exchange": "SZSE", "curr_type": "CNY",
            "list_date": date(2000, 1, 1),
        }], schema=TABLE_SCHEMAS["stock_basic"]))
        store.write("stk_limit", pa.Table.from_pylist([{
            "ts_code": SYMBOL, "trade_date": DAY, "up_limit": 20, "down_limit": 1,
        }], schema=TABLE_SCHEMAS["stk_limit"]))
        store._mark_sync_all_completed("suspend_d", DAY, DAY)
    with QmtDataStore(tmp_path / "qmt") as store:
        store.append_quotes(quotes)
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        yield DataReader(catalog, sources=default_source_config(), bar_availability=mode)


def make_engine(reader: DataReader, *, holding: bool = False) -> BacktestEngine:
    config = BacktestConfig(
        DAY, DAY, frequency="1m", symbols=(SYMBOL,), market=HOURS,
        volume_limit=None, slippage_bps=0, bar_availability=reader.bar_availability,
    )
    engine = BacktestEngine(
        reader=reader, config=config, sessions=(DAY,), strategy=ObservePrices(),
        actions=CorporateActionProcessor(()),
    )
    if holding:
        position = engine.portfolio.position(SYMBOL)
        position.quantity = position.sellable_quantity = 1000
        position.last_price = 10
    return engine


def test_historical_mode_uses_complete_bar_at_end_even_if_received_later(tmp_path: Path) -> None:
    record = quote(1, time(9, 30), time(9, 31, 30), 11)
    with data_reader(tmp_path, [record], "historical") as reader:
        before = reader.at(at_time(DAY, time(9, 30, 30))).market.bars(
            symbols=(SYMBOL,), frequency="1m", count=1,
        )
        assert before.table.num_rows == 0
        engine = make_engine(reader)
        engine.broker.submit(OrderRequest(SYMBOL, Side.BUY, 100), at_time(DAY, time(9, 30)))
        result = engine.run()

    assert len(result.fills) == 1
    assert result.fills[0].filled_at == at_time(DAY, time(9, 30))
    assert result.fills[0].execution_price == 10


def test_late_bar_updates_current_valuation_without_historical_fill(tmp_path: Path) -> None:
    record = quote(1, time(9, 30), time(9, 31, 30), 11)
    with data_reader(tmp_path, [record], "received") as reader:
        engine = make_engine(reader, holding=True)
        engine.broker.submit(OrderRequest(SYMBOL, Side.BUY, 100), at_time(DAY, time(9, 30)))
        result = engine.run()
        assert isinstance(engine.strategy, ObservePrices)
        assert engine.strategy.values == [10_000, 11_000, 11_000]

    assert result.fills == ()
    assert result.order_updates[-1].reason is OrderReason.MISSING_OPEN
    assert result.equity[-1].market_value == 11_000
    assert result.equity[-1].stale_position_count == 1
    assert result.market_data_coverage is not None
    assert result.market_data_coverage.bar_count == 1


def test_received_older_arrival_never_overwrites_newer_market_price(tmp_path: Path) -> None:
    records = [
        quote(1, time(9, 30), time(9, 32, 30), 9),
        quote(2, time(9, 31), time(9, 32), 12),
    ]
    with data_reader(tmp_path, records, "received") as reader:
        engine = make_engine(reader, holding=True)
        result = engine.run()
        assert isinstance(engine.strategy, ObservePrices)
        assert engine.strategy.values == [10_000, 12_000, 12_000]
    assert result.equity[-1].market_value == 12_000


def test_received_last_bar_can_arrive_between_market_close_and_session_end(tmp_path: Path) -> None:
    record = quote(1, time(9, 32), time(9, 33, 30), 13)
    with data_reader(tmp_path, [record], "received") as reader:
        engine = make_engine(reader, holding=True)
        result = engine.run()
        assert isinstance(engine.strategy, ObservePrices)
        assert engine.strategy.values == [10_000, 10_000, 10_000]
    assert result.equity[-1].market_value == 13_000
    assert result.equity[-1].stale_position_count == 0


def test_received_mode_excludes_downloaded_bars_without_receive_times(tmp_path: Path) -> None:
    with QmtDataStore(tmp_path / "qmt") as store:
        store.write_intraday({SYMBOL: [HistoryBar(
            index=20260105093100, open=10, high=11, low=10, close=11, volume=100,
        )]}, "1m", "none")
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(catalog, sources=default_source_config(), bar_availability="received")
        table = reader.at(at_time(DAY, time(9, 32))).market.bars(
            symbols=(SYMBOL,), frequency="1m", count=1,
        ).table
    assert table.num_rows == 0


def test_engine_rejects_mismatched_reader_availability(tmp_path: Path) -> None:
    with (
        data_reader(tmp_path, [], "received") as reader,
        pytest.raises(ConfigurationError, match="bar_availability 必须一致"),
    ):
        BacktestEngine(
            reader=reader, config=BacktestConfig(DAY, DAY, frequency="1m"),
            sessions=(DAY,), strategy=ObservePrices(), actions=CorporateActionProcessor(()),
        )


def test_latest_bar_uses_normalized_end_time_across_qmt_time_labels(tmp_path: Path) -> None:
    with QmtDataStore(tmp_path / "qmt") as store:
        store.write_intraday({SYMBOL: [HistoryBar(
            index=20260105093100, open=10, high=11, low=10, close=11, volume=100,
        )]}, "1m", "none")
    record = quote(1, time(9, 31), time(9, 32), 12)
    with data_reader(tmp_path, [record], "historical") as reader:
        table = reader.at(at_time(DAY, time(9, 32))).market.bars(
            symbols=(SYMBOL,), frequency="1m", count=1,
        ).table
        assert reader.snapshot_metadata()["qmt"] == {
            "bar_availability": "historical",
            "history_time_label": "end",
            "realtime_time_label": "start",
        }
    assert table.to_pylist()[0]["interval_end"] == at_time(DAY, time(9, 32))
    assert table.to_pylist()[0]["close"] == 12


def test_next_day_arrival_does_not_rewrite_previous_equity_snapshot(tmp_path: Path) -> None:
    following = date(2026, 1, 6)
    record = quote(1, time(9, 32), time(9, 33), 11).model_copy(update={
        "received_at": int(at_time(following, time(9, 30)).timestamp() * 1_000_000),
    })
    with data_reader(tmp_path, [record], "received") as reader:
        config = BacktestConfig(
            DAY, following, frequency="1m", symbols=(SYMBOL,), market=HOURS,
            volume_limit=None, bar_availability="received",
        )
        engine = BacktestEngine(
            reader=reader, config=config, sessions=(DAY, following), strategy=ObservePrices(),
            actions=CorporateActionProcessor(()),
        )
        position = engine.portfolio.position(SYMBOL)
        position.quantity = position.sellable_quantity = 1000
        position.last_price = 10
        result = engine.run()

    assert [row.market_value for row in result.equity] == [10_000, 11_000]
