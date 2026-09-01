from __future__ import annotations

from datetime import date

from backtest.config import BacktestConfig
from backtest.engine import BacktestResult
from backtest.metrics import calculate_metrics
from backtest.types import EquitySnapshot


def test_metrics_return_none_for_zero_volatility_denominators() -> None:
    sessions = (date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4))
    equity = tuple(
        EquitySnapshot(
            session=session,
            cash=100.0,
            dividend_receivable=0.0,
            market_value=0.0,
            total_equity=100.0,
            daily_return=None if index == 0 else 0.0,
            holding_count=0,
            stale_position_count=0,
        )
        for index, session in enumerate(sessions)
    )
    result = BacktestResult(
        sessions=sessions,
        orders=(),
        fills=(),
        equity=equity,
    )
    metrics = calculate_metrics(
        result,
        BacktestConfig(start_date=sessions[0], end_date=sessions[-1], initial_cash=100.0),
    )
    assert metrics["total_return"] == 0.0
    assert metrics["sharpe"] is None
