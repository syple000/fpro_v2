from datetime import date, time
from pathlib import Path

import pyarrow as pa
import pytest

from backtest.clock import at_time
from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.domain import OrderReason, OrderRequest, Side
from backtest.engine import BacktestEngine
from backtest.errors import DataError
from backtest.strategy import Strategy, StrategyContext
from market_data import DataCatalog, DataReader, SourceConfig
from tushare_data import TABLE_SCHEMAS, TushareDataStore


class IdleStrategy(Strategy):
    def on_event(self, context: StrategyContext) -> None:
        pass


@pytest.mark.parametrize("source_code,delisted", [("920047.BJ", False), ("430047.BJ", True)])
def test_only_explicit_delisting_can_remove_a_position(
    tmp_path: Path,
    source_code: str,
    delisted: bool,
) -> None:
    day = date(2026, 1, 5)
    with TushareDataStore(tmp_path / "tushare") as store:
        store.write(
            "stock_basic",
            pa.Table.from_pylist(
                [
                    {
                        "ts_code": source_code,
                        "list_date": date(2020, 1, 1),
                        "delist_date": day if delisted else None,
                        "curr_type": "CNY",
                        "exchange": "BSE",
                    }
                ],
                schema=TABLE_SCHEMAS["stock_basic"],
            ),
        )
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(catalog, sources=SourceConfig(routes={"reference.stocks": "tushare"}))
        engine = BacktestEngine(
            reader=reader,
            config=BacktestConfig(day, day),
            sessions=(day,),
            strategy=IdleStrategy(),
            actions=CorporateActionProcessor(()),
        )
        position = engine.portfolio.position("430047.BJ")
        position.quantity = position.sellable_quantity = 1_000
        position.last_price = 10
        at = at_time(day, time(9, 25))
        engine.broker.submit(OrderRequest("430047.BJ", Side.SELL, 1_000), at)
        if delisted:
            engine._start_session(at, reader.at(at))
            assert position.quantity == 0
            assert engine.broker.updates[-1].reason is OrderReason.DELISTED
        else:
            before = engine.portfolio.total_equity
            with pytest.raises(DataError, match="主数据缺失.*代码映射"):
                engine._start_session(at, reader.at(at))
            assert position.quantity == 1_000
            assert engine.portfolio.total_equity == before
            assert len(engine.broker.pending_orders) == 1
        assert not hasattr(reader.at(at), "security_lifecycles")
