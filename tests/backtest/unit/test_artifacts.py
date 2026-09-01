from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

from backtest.artifacts import build_data_snapshot, write_artifacts
from backtest.config import BacktestConfig, RunOptions
from backtest.engine import BacktestResult
from backtest.metrics import calculate_metrics
from backtest.types import EquitySnapshot, Order, OrderReason, OrderResult, OrderSide


def test_data_file_hashes_are_opt_in(tmp_path: Path) -> None:
    dataset = tmp_path / "daily"
    dataset.mkdir()
    data_file = dataset / "part.parquet"
    data_file.write_bytes(b"test-data")
    (dataset / "_manifest.json").write_text(
        json.dumps({"files": [data_file.name]}),
        encoding="utf-8",
    )

    default = build_data_snapshot(tmp_path)
    audited = build_data_snapshot(tmp_path, hash_files=True)

    assert default["snapshot_id"] == audited["snapshot_id"]
    assert default["content_snapshot_id"] is None
    assert "sha256" not in default["files"][0]
    assert audited["content_snapshot_id"] is not None
    assert "sha256" in audited["files"][0]


def test_default_artifacts_only_write_final_results(tmp_path: Path) -> None:
    session = date(2024, 1, 2)
    config = BacktestConfig(start_date=session, end_date=session, initial_cash=100.0)
    order = Order(
        order_id="O00000001",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=100,
        submitted_at=datetime(2024, 1, 2, 16, 5),
        earliest_fill_at=datetime(2024, 1, 3, 9, 30),
    )
    result = BacktestResult(
        run_id="test-run",
        strategy_id="test",
        sessions=(session,),
        orders=(OrderResult(order, 0, OrderReason.END_OF_BACKTEST),),
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
        corporate_actions=(),
        warnings=(),
    )
    output = write_artifacts(
        config=config,
        options=RunOptions(output_root=tmp_path / "runs"),
        result=result,
        metrics=calculate_metrics(result, config),
        strategy={"strategy_id": "test"},
        data_snapshot={"snapshot_id": "snapshot"},
        environment={},
    )

    assert (output / "orders.parquet").is_file()
    assert (output / "equity.parquet").is_file()
    assert not (output / "events.parquet").exists()
    assert not (output / "order_events.parquet").exists()
    assert not (output / "positions.parquet").exists()
