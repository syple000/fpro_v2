import math
import statistics
from datetime import date

import pyarrow.parquet as pq
import pytest

from backtest.config import BacktestConfig
from backtest.domain import BacktestResult, EquitySnapshot
from backtest.metrics import calculate_metrics
from backtest.output import write_results
from backtest.portfolio import Portfolio


def test_daily_returns_and_risk_metrics_include_first_session() -> None:
    sessions = (date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7))
    config = BacktestConfig(sessions[0], sessions[-1], initial_cash=100_000)
    portfolio = Portfolio(config.initial_cash)
    snapshots = []
    for session, equity in zip(sessions, (99_000, 100_000, 99_000), strict=True):
        portfolio.cash = equity
        snapshots.append(portfolio.equity_snapshot(session))
    result = BacktestResult(sessions, (), (), (), tuple(snapshots))

    expected_returns = [-0.01, 100_000 / 99_000 - 1, -0.01]
    assert [row.daily_return for row in result.equity] == pytest.approx(expected_returns)
    metrics = calculate_metrics(result, config)
    daily_volatility = statistics.stdev(expected_returns)
    assert metrics["annualized_volatility"] == pytest.approx(daily_volatility * math.sqrt(252))
    assert metrics["sharpe"] == pytest.approx(
        statistics.fmean(expected_returns) / daily_volatility * math.sqrt(252)
    )
    assert math.prod(1 + value for value in expected_returns) - 1 == pytest.approx(
        metrics["total_return"]
    )


def test_first_session_without_profit_has_zero_return() -> None:
    assert Portfolio(100_000).equity_snapshot(date(2026, 1, 5)).daily_return == 0.0


def test_metrics_and_output_use_explicit_result_tables(tmp_path) -> None:
    config = BacktestConfig(
        date(2026, 1, 5),
        date(2026, 1, 6),
        initial_cash=100_000,
    )
    result = BacktestResult(
        sessions=(date(2026, 1, 5), date(2026, 1, 6)),
        orders=(),
        order_updates=(),
        fills=(),
        equity=(
            EquitySnapshot(date(2026, 1, 5), 100_000, 0, 0, 100_000, None, 0, 0),
            EquitySnapshot(date(2026, 1, 6), 101_000, 0, 0, 101_000, 0.01, 0, 0),
        ),
    )

    metrics = calculate_metrics(result, config)
    output = write_results(tmp_path / "run", config, result, metrics)

    assert metrics["total_return"] == pytest.approx(0.01)
    assert (output / "metrics.json").is_file()
    assert pq.read_table(output / "equity.parquet").num_rows == 2
    assert pq.read_table(output / "orders.parquet").num_rows == 0
