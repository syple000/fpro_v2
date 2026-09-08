from datetime import date, time
from pathlib import Path

import pyarrow as pa
import pytest

from backtest.clock import Event
from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.domain import OrderReason, OrderRequest, Side
from backtest.engine import BacktestEngine
from backtest.strategy import Strategy, StrategyContext
from market_data import DataCatalog, DataReader, SourceConfig
from qmt_protocol import HistoryBar
from qmt_receiver import QmtDataStore
from tests.backtest.conftest import timestamp
from tushare_data import TABLE_SCHEMAS, TushareDataStore


class IdleStrategy(Strategy):
    def on_event(self, context: StrategyContext) -> None:
        pass


@pytest.mark.parametrize("timing,expected_fills", [("09:31-10:00", 1), ("09:30-09:31", 0)])
def test_matching_uses_opening_status_even_when_it_changes_before_bar_end(
    tmp_path: Path,
    timing: str,
    expected_fills: int,
) -> None:
    day = date(2026, 1, 5)
    with TushareDataStore(tmp_path / "tushare") as store:
        store._mark_sync_all_completed("suspend_d", day, day)
        store.write(
            "suspend_d",
            pa.Table.from_pylist(
                [
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": day,
                        "suspend_timing": timing,
                        "suspend_type": "S",
                    }
                ],
                schema=TABLE_SCHEMAS["suspend_d"],
            ),
        )
        store.write(
            "stk_limit",
            pa.Table.from_pylist(
                [
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": day,
                        "up_limit": 11,
                        "down_limit": 9,
                    }
                ],
                schema=TABLE_SCHEMAS["stk_limit"],
            ),
        )
    with QmtDataStore(tmp_path / "qmt") as store:
        store.write_intraday(
            {
                "000001.SZ": [
                    HistoryBar(
                        index=20260105093100,
                        open=10,
                        close=10,
                    )
                ]
            },
            "1m",
            "none",
        )
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(
                routes={
                    "market.intraday_bars": "qmt",
                    "market.suspensions": "tushare",
                    "market.price_limits": "tushare",
                }
            ),
        )
        engine = BacktestEngine(
            reader=reader,
            config=BacktestConfig(day, day, frequency="1m", volume_limit=None),
            sessions=(day,),
            strategy=IdleStrategy(),
            actions=CorporateActionProcessor(()),
        )
        engine.broker.submit(OrderRequest("000001.SZ", Side.BUY, 100), timestamp(day, time(9, 30)))
        event = Event(timestamp(day, time(9, 31)), day, timestamp(day, time(9, 30)), "1m")
        engine._process_bar(event, reader.at(event.at))
        assert len(engine.broker.fills) == expected_fills
        if expected_fills:
            assert engine.broker.fills[0].filled_at == timestamp(day, time(9, 30))
        else:
            assert engine.broker.updates[-1].reason is OrderReason.SUSPENDED
        # 策略时点仍能看见刚发生的停牌/复牌。
        status = (
            reader.at(event.at)
            .market.status(
                symbols=("000001.SZ",),
                fields=("suspended",),
            )
            .table.to_pylist()[0]
        )
        assert status["suspended"] is bool(expected_fills)
