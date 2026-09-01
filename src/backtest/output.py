"""可选地保存最直接的回测结果。"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from backtest.config import BacktestConfig
from backtest.engine import BacktestResult


def _parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    table = pa.Table.from_pylist(rows) if rows else pa.table({"empty": []})
    pq.write_table(table, path, compression="zstd")


def write_results(
    output_dir: Path,
    config: BacktestConfig,
    result: BacktestResult,
    metrics: dict[str, Any],
) -> Path:
    """写入一个新目录；为避免混淆，不覆盖已有运行。"""

    output_dir.mkdir(parents=True)
    (output_dir / "config.json").write_text(
        json.dumps(config.to_dict(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (output_dir / "metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    _parquet(
        output_dir / "orders.parquet",
        [
            {
                **asdict(row.order),
                "side": row.order.side.value,
                "filled_quantity": row.filled_quantity,
                "remaining_quantity": row.remaining_quantity,
                "status": row.status.value,
                "reason": row.reason.value,
            }
            for row in result.orders
        ],
    )
    _parquet(
        output_dir / "fills.parquet",
        [{**asdict(fill), "side": fill.side.value} for fill in result.fills],
    )
    _parquet(output_dir / "equity.parquet", [asdict(row) for row in result.equity])
    return output_dir
