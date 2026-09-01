from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa

from backtest.config import BacktestConfig, ExecutionConfig, FeeConfig
from backtest.data import DataPortal
from backtest.engine import BacktestEngine
from backtest.strategy import BacktestData, StrategyContext
from market_data import DataCatalog, DataReader, SourceConfig
from tushare_data import TABLE_SCHEMAS, TushareDataStore


def _table(dataset: str, *rows: dict[str, object]) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=TABLE_SCHEMAS[dataset])


class OneShotStrategy:
    strategy_id = "one_shot"

    def __init__(self) -> None:
        self.ordered = False

    def initialize(self, context: StrategyContext) -> None:
        del context

    def on_pre_open(self, context: StrategyContext, data: BacktestData) -> None:
        del context, data

    def on_close(self, context: StrategyContext, data: BacktestData) -> None:
        if not self.ordered:
            context.rebalance({"000001.SZ": 0.5})
            self.ordered = True
        del data


def test_close_signal_only_fills_at_next_open(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    sessions = (date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4))
    with TushareDataStore(tushare_root) as store:
        store.write(
            "stock_basic",
            _table(
                "stock_basic",
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "exchange": "SZSE",
                    "curr_type": "CNY",
                    "list_status": "L",
                    "list_date": date(1991, 4, 3),
                },
            ),
        )
        for index, session in enumerate(sessions):
            store.write(
                "trade_cal",
                _table(
                    "trade_cal",
                    {
                        "exchange": "SSE",
                        "cal_date": session,
                        "is_open": 1,
                        "pretrade_date": sessions[index - 1] if index else None,
                    },
                ),
            )
            store.write(
                "daily",
                _table(
                    "daily",
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": session,
                        "open": 10.0 + index,
                        "high": 10.5 + index,
                        "low": 9.5 + index,
                        "close": 10.0 + index,
                        "pre_close": 9.0 + index,
                        "vol": 10_000.0,
                        "amount": 100_000.0,
                    },
                ),
            )
            store.write(
                "stk_limit",
                _table(
                    "stk_limit",
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": session,
                        "pre_close": 9.0 + index,
                        "up_limit": 20.0,
                        "down_limit": 1.0,
                    },
                ),
            )

    routes = {
        route: "tushare"
        for route in (
            "calendar.sessions",
            "corporate_actions.dividends",
            "market.daily_bars",
            "market.price_limits",
            "market.st_status",
            "market.suspensions",
            "reference.stocks",
        )
    }
    config = BacktestConfig(
        start_date=sessions[0],
        end_date=sessions[-1],
        initial_cash=100_000.0,
        tushare_root=tushare_root,
        qmt_root=tmp_path / "qmt",
        output_root=tmp_path / "runs",
        fee=FeeConfig(commission_rate=0, minimum_commission=0),
        execution=ExecutionConfig(slippage_bps=0, max_previous_volume_participation=None),
    )
    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        portal = DataPortal(
            DataReader(catalog, sources=SourceConfig(routes)),
            config,
        )
        result = BacktestEngine(
            run_id="run",
            config=config,
            portal=portal,
            strategy=OneShotStrategy(),
        ).run()

    assert len(result.fills) == 1
    assert result.orders[0].submitted_at.date() == sessions[0]
    assert result.fills[0].filled_at.date() == sessions[1]
    assert result.fills[0].market_price == 11.0
    assert result.equity[0].total_equity == 100_000.0
