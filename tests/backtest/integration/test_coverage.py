from datetime import date
from typing import cast

import pytest

from backtest.config import BacktestConfig
from backtest.corporate_actions import CorporateActionProcessor
from backtest.engine import BacktestEngine
from backtest.errors import DataError
from backtest.metrics import calculate_metrics
from backtest.strategy import Strategy, StrategyContext
from market_data import DataReader
from tests.backtest.conftest import MemoryDataReader, bar_table, daily_bar


class NoTrades(Strategy):
    def on_event(self, context: StrategyContext) -> None:
        pass


@pytest.mark.parametrize("has_other_symbol", [False, True])
def test_no_bars_in_requested_scope_cannot_report_zero_return(has_other_symbol: bool) -> None:
    day = date(2026, 1, 5)
    bars = [daily_bar(day, 10)] if has_other_symbol else []
    config = BacktestConfig(day, day, symbols=("000002.SZ",))
    engine = BacktestEngine(
        reader=cast(DataReader, MemoryDataReader(bar_table(bars), (day,))),
        config=config, sessions=(day,), strategy=NoTrades(), actions=CorporateActionProcessor(()),
    )

    with pytest.raises(DataError, match="未读取到任何 1d 行情"):
        engine.run()


def test_partial_coverage_and_empty_portfolio_are_reported_without_failing() -> None:
    sessions = (date(2026, 1, 5), date(2026, 1, 6))
    config = BacktestConfig(*sessions, symbols=("000001.SZ", "000002.SZ"))
    reader = MemoryDataReader(bar_table([daily_bar(sessions[0], 10)]), sessions)
    result = BacktestEngine(
        reader=cast(DataReader, reader), config=config, sessions=sessions,
        strategy=NoTrades(), actions=CorporateActionProcessor(()),
    ).run()
    metrics = calculate_metrics(result, config)

    assert result.fills == ()
    assert metrics["total_return"] == 0
    assert metrics["market_data_coverage"] == {
        "expected_events": 2,
        "events_with_bars": 1,
        "bar_count": 1,
        "symbols_with_bars": ["000001.SZ"],
        "sessions_without_bars": ["2026-01-06"],
        "requested_symbols_without_bars": ["000002.SZ"],
    }
