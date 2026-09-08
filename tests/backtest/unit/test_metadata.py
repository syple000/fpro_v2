import hashlib
import json
import subprocess
from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

from backtest.config import BacktestConfig
from backtest.metadata import capture_run_metadata
from backtest.runner import default_source_config, run_backtest
from backtest.strategy import Strategy, StrategyContext
from market_data import DataCatalog, DataReader, SourceConfig
from market_data.errors import DataCapabilityNotSupportedError
from market_data.protocols import DataAdapter
from strategies.momentum import MomentumConfig, MonthlyMomentumStrategy
from tushare_data import TABLE_SCHEMAS, TushareDataStore


class MutableStrategy(Strategy):
    def __init__(self) -> None:
        self.lookback = 20

    def on_event(self, context: StrategyContext) -> None:
        self.lookback = 99


def test_runner_writes_initial_strategy_parameters_and_loaded_snapshot(tmp_path: Path) -> None:
    day = date(2026, 1, 5)
    root = tmp_path / "tushare"
    with TushareDataStore(root) as store:
        rows = {
            "trade_cal": [{"exchange": "SSE", "cal_date": day, "is_open": 1}],
            "stock_basic": [{
                "ts_code": "000001.SZ", "exchange": "SZSE", "curr_type": "CNY",
                "list_date": date(2000, 1, 1),
            }],
            "daily": [{"ts_code": "000001.SZ", "trade_date": day, "open": 10, "close": 10}],
        }
        for dataset, values in rows.items():
            store.write(dataset, pa.Table.from_pylist(values, schema=TABLE_SCHEMAS[dataset]))
    strategy = MutableStrategy()
    with DataCatalog(tushare_root=root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(catalog, sources=default_source_config())
        snapshot_id = catalog.snapshot_metadata()["snapshot_id"]
        run_backtest(
            reader=reader, config=BacktestConfig(day, day), strategy=strategy,
            output_dir=tmp_path / "run",
        )

    metadata = json.loads((tmp_path / "run" / "run_metadata.json").read_text())
    assert strategy.lookback == 99
    assert metadata["strategy"]["parameters"] == {"lookback": 20}
    assert metadata["strategy"]["type"].endswith(".MutableStrategy")
    assert metadata["strategy"]["schedule"]["every"] == "bar"
    assert metadata["strategy"]["source_sha256"] == hashlib.sha256(
        Path(__file__).read_bytes()
    ).hexdigest()
    assert metadata["data"]["routes"] == dict(default_source_config().routes)
    assert metadata["data"]["catalog"]["snapshot_id"] == snapshot_id
    assert metadata["data"]["catalog"]["sources"]["tushare"]["root"] == str(root)
    assert metadata["security_code_history"] is None
    assert metadata["code"]["python"]
    assert metadata["code"]["dependencies"]["pyarrow"] == pa.__version__
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True,
    ).stdout.strip()
    assert metadata["code"]["git_commit"] == commit
    assert isinstance(metadata["code"]["git_dirty"], bool)


def test_momentum_metadata_keeps_every_strategy_parameter(tmp_path: Path) -> None:
    strategy = MonthlyMomentumStrategy(
        MomentumConfig(lookback_sessions=7, selection_count=3), allowed_symbols=("000001.SZ",)
    )
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(catalog, sources=default_source_config())
        metadata = capture_run_metadata(reader, strategy)

    assert metadata["strategy"]["parameters"] == {
        "config": {
            "lookback_sessions": 7, "selection_count": 3,
            "minimum_listing_sessions": 250, "exclude_st": True,
        },
        "allowed_symbols": ["000001.SZ"],
    }
    assert metadata["strategy"]["schedule"] == {"every": "month", "at": "16:05:00", "times": None}


def test_custom_adapter_must_describe_data_version_for_output(tmp_path: Path) -> None:
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog, sources=SourceConfig(routes={"market.daily_bars": "custom"}),
            adapters={"custom": DataAdapter()},
        )
        with pytest.raises(DataCapabilityNotSupportedError, match="snapshot_metadata"):
            capture_run_metadata(reader, MutableStrategy())


def test_custom_adapter_snapshot_is_saved_with_its_source_type(tmp_path: Path) -> None:
    class VersionedAdapter(DataAdapter):
        def snapshot_metadata(self) -> dict[str, object]:
            return {"snapshot_id": "test-daily-v3", "location": "memory-fixture"}

    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog, sources=SourceConfig(routes={"market.daily_bars": "custom"}),
            adapters={"custom": VersionedAdapter()},
        )
        metadata = capture_run_metadata(reader, MutableStrategy())

    custom = metadata["data"]["custom_adapters"]["custom"]
    assert custom["type"].endswith(".VersionedAdapter")
    assert custom["snapshot"] == {"snapshot_id": "test-daily-v3", "location": "memory-fixture"}
