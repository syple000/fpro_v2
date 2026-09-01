from __future__ import annotations

from datetime import date
from pathlib import Path

from backtest.config import BacktestConfig
from backtest.engine import BacktestResult
from backtest.metrics import calculate_metrics
from backtest.output import write_results
from backtest.types import EquitySnapshot


def test_write_minimal_results(tmp_path: Path) -> None:
    session = date(2024, 1, 2)
    config = BacktestConfig(start_date=session, end_date=session, initial_cash=100.0)
    result = BacktestResult(
        sessions=(session,),
        orders=(),
        fills=(),
        equity=(
            EquitySnapshot(
                session=session,
                cash=100.0,
                dividend_receivable=0.0,
                market_value=0.0,
                total_equity=100.0,
                daily_return=None,
                holding_count=0,
                stale_position_count=0,
            ),
        ),
    )
    output = write_results(
        tmp_path / "result",
        config,
        result,
        calculate_metrics(result, config),
    )

    assert {path.name for path in output.iterdir()} == {
        "config.json",
        "metrics.json",
        "orders.parquet",
        "fills.parquet",
        "equity.parquet",
    }
