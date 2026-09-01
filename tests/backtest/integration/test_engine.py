from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa

from backtest.config import BacktestConfig
from backtest.data import MarketData, SessionData
from backtest.engine import BacktestEngine
from market_data import DataCatalog, DataReader, SourceConfig
from tushare_data import TABLE_SCHEMAS, TushareDataStore


def _table(dataset: str, *rows: dict[str, object]) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=TABLE_SCHEMAS[dataset])


class OneShotStrategy:
    def __init__(self) -> None:
        self.ordered = False
        self.visible_history: list[tuple[int, ...]] = []

    def __call__(self, data: SessionData) -> dict[str, float] | None:
        self.visible_history.append(
            tuple(point.session_index for point in data.history("000001.SZ"))
        )
        if not self.ordered:
            self.ordered = True
            return {"000001.SZ": 0.5}
        return None


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
        commission_rate=0,
        minimum_commission=0,
        slippage_bps=0,
        max_volume_fraction=None,
    )
    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        data = MarketData(
            DataReader(catalog, sources=SourceConfig(routes)),
            config,
        )
        strategy = OneShotStrategy()
        result = BacktestEngine(
            config=config,
            data=data,
            strategy=strategy,
        ).run()

    assert len(result.fills) == 1
    assert result.orders[0].order.submitted_at.date() == sessions[0]
    assert result.fills[0].filled_at.date() == sessions[1]
    assert result.fills[0].market_price == 11.0
    assert result.equity[0].total_equity == 100_000.0
    assert strategy.visible_history == [(0,), (0, 1), (0, 1, 2)]
