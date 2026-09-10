from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import date, time
from pathlib import Path

import pyarrow as pa
import pytest

from backtest.clock import MarketHours, at_time
from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.domain import OrderReason, OrderRequest, Side
from backtest.engine import BacktestEngine
from backtest.errors import DataError
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


def quote(seq: int, end: time, received: time, close: float) -> SequencedQuote:
    """用区间结束时间构造 QMT 推送。"""
    return SequencedQuote(
        seq=seq,
        code=SYMBOL,
        period="1m",
        source="market",
        subscription="SZ",
        received_at=int(at_time(DAY, received).timestamp() * 1_000_000),
        quote=BarQuote(
            time=int(at_time(DAY, end).timestamp() * 1_000_000),
            open=10,
            high=max(10, close),
            low=min(10, close),
            close=close,
            volume=1000,
        ),
    )


@contextmanager
def data_reader(
    tmp_path: Path,
    quotes: Sequence[SequencedQuote],
    bars: Sequence[HistoryBar] = (),
) -> Iterator[DataReader]:
    with TushareDataStore(tmp_path / "tushare") as store:
        store.write(
            "stock_basic",
            pa.Table.from_pylist(
                [
                    {
                        "ts_code": SYMBOL,
                        "exchange": "SZSE",
                        "curr_type": "CNY",
                        "list_date": date(2000, 1, 1),
                    }
                ],
                schema=TABLE_SCHEMAS["stock_basic"],
            ),
        )
        store.write(
            "stk_limit",
            pa.Table.from_pylist(
                [
                    {
                        "ts_code": SYMBOL,
                        "trade_date": DAY,
                        "up_limit": 20,
                        "down_limit": 1,
                    }
                ],
                schema=TABLE_SCHEMAS["stk_limit"],
            ),
        )
        store._mark_sync_all_completed("suspend_d", DAY, DAY)
    with QmtDataStore(tmp_path / "qmt") as store:
        store.append_quotes(quotes)
        store.write_intraday({SYMBOL: bars}, "1m", "none")
    with DataCatalog(
        tushare_root=tmp_path / "tushare",
        qmt_root=tmp_path / "qmt",
    ) as catalog:
        yield DataReader(
            catalog,
            sources=default_source_config(),
        )


def make_engine(reader: DataReader, *, holding: bool = False) -> BacktestEngine:
    config = BacktestConfig(
        DAY,
        DAY,
        frequency="1m",
        symbols=(SYMBOL,),
        market=HOURS,
        volume_limit=None,
        slippage_bps=0,
    )
    engine = BacktestEngine(
        reader=reader,
        config=config,
        sessions=(DAY,),
        strategy=ObservePrices(),
        actions=CorporateActionProcessor(()),
    )
    if holding:
        position = engine.portfolio.position(SYMBOL)
        position.quantity = position.sellable_quantity = 1000
        position.last_price = 10
    return engine


def test_backtest_uses_downloaded_prices_and_leaves_push_only_intervals_missing(
    tmp_path: Path,
) -> None:
    bars = [HistoryBar(index=20260105093100, open=10, close=11, volume=100)]
    records = [
        quote(1, time(9, 31), time(9, 30, 20), 9),
        quote(2, time(9, 32), time(9, 32), 12),
        quote(3, time(9, 33), time(9, 33, 30), 13),
    ]
    with data_reader(tmp_path, records, bars) as reader:
        engine = make_engine(reader, holding=True)
        engine.broker.submit(OrderRequest(SYMBOL, Side.BUY, 100), at_time(DAY, time(9, 30)))
        result = engine.run()
        assert isinstance(engine.strategy, ObservePrices)
        assert engine.strategy.values == [12_100, 12_100, 12_100]
    assert len(result.fills) == 1
    assert result.fills[0].execution_price == 10
    assert result.fills[0].filled_at == at_time(DAY, time(9, 30))
    assert result.equity[-1].market_value == 12_100
    assert result.equity[-1].stale_position_count == 1
    assert result.market_data_coverage is not None
    assert result.market_data_coverage.bar_count == 1


def test_backtest_rejects_coverage_from_pushes_alone(tmp_path: Path) -> None:
    with data_reader(tmp_path, [quote(1, time(9, 31), time(9, 31), 11)]) as reader:
        engine = make_engine(reader)
        engine.broker.submit(OrderRequest(SYMBOL, Side.BUY, 100), at_time(DAY, time(9, 30)))
        with pytest.raises(DataError, match="未读取到任何 1m 行情"):
            engine.run()
        assert engine.broker.fills == ()
        assert engine.broker.updates[-1].reason is OrderReason.MISSING_OPEN
