"""将配置、指标和明细结果写到磁盘。"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from backtest.config import BacktestConfig
from backtest.domain import BacktestResult


def write_results(
    output_dir: Path,
    config: BacktestConfig,
    result: BacktestResult,
    metrics: dict[str, Any],
) -> Path:
    """将一次回测拆成便于人读的 JSON 和便于分析的 Parquet。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "config.json", config.to_dict())
    _write_json(output_dir / "metrics.json", metrics)
    _write_parquet(
        output_dir / "orders.parquet",
        [
            {
                **asdict(order),
                "side": order.side.value,
            }
            for order in result.orders
        ],
    )
    _write_parquet(
        output_dir / "order_updates.parquet",
        [
            {
                "order_id": update.order.order_id,
                "updated_at": update.updated_at,
                "status": update.status.value,
                "filled_quantity": update.filled_quantity,
                "remaining_quantity": update.remaining_quantity,
                "reason": update.reason.value,
            }
            for update in result.order_updates
        ],
    )
    _write_parquet(
        output_dir / "fills.parquet",
        [{**asdict(fill), "side": fill.side.value} for fill in result.fills],
    )
    _write_parquet(
        output_dir / "equity.parquet",
        [asdict(snapshot) for snapshot in result.equity],
    )
    return output_dir


def _write_json(path: Path, value: object) -> None:
    """使用统一 UTF-8 和缩进格式写 JSON。"""
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    """写 zstd 压缩 Parquet；空结果仍生成一个可读取的空表。"""
    table = pa.Table.from_pylist(rows) if rows else pa.table({"empty": pa.array([], pa.null())})
    pq.write_table(table, path, compression="zstd")
