from datetime import date

import pyarrow.parquet as pq
import pytest

from backtest.config import BacktestConfig
from backtest.domain import BacktestResult, EquitySnapshot
from backtest.metrics import calculate_metrics
from backtest.output import write_results


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
