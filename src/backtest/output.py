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

_TIMESTAMP = pa.timestamp("us", tz="UTC")
_ORDER_SCHEMA = pa.schema(
    [
        pa.field("order_id", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("side", pa.string(), nullable=False),
        pa.field("quantity", pa.int64(), nullable=False),
        pa.field("submitted_at", _TIMESTAMP, nullable=False),
        pa.field("target_weight", pa.float64()),
    ]
)
_ORDER_UPDATE_SCHEMA = pa.schema(
    [
        pa.field("order_id", pa.string(), nullable=False),
        pa.field("updated_at", _TIMESTAMP, nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("filled_quantity", pa.int64(), nullable=False),
        pa.field("remaining_quantity", pa.int64(), nullable=False),
        pa.field("reason", pa.string(), nullable=False),
    ]
)
_FILL_SCHEMA = pa.schema(
    [
        pa.field("fill_id", pa.string(), nullable=False),
        pa.field("order_id", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("side", pa.string(), nullable=False),
        pa.field("filled_at", _TIMESTAMP, nullable=False),
        pa.field("quantity", pa.int64(), nullable=False),
        pa.field("market_price", pa.float64(), nullable=False),
        pa.field("execution_price", pa.float64(), nullable=False),
        pa.field("notional", pa.float64(), nullable=False),
        pa.field("commission", pa.float64(), nullable=False),
        pa.field("stamp_tax", pa.float64(), nullable=False),
        pa.field("transfer_fee", pa.float64(), nullable=False),
        pa.field("slippage_cost", pa.float64(), nullable=False),
    ]
)
_EQUITY_SCHEMA = pa.schema(
    [
        pa.field("session", pa.date32(), nullable=False),
        pa.field("cash", pa.float64(), nullable=False),
        pa.field("dividend_receivable", pa.float64(), nullable=False),
        pa.field("market_value", pa.float64(), nullable=False),
        pa.field("total_equity", pa.float64(), nullable=False),
        pa.field("daily_return", pa.float64()),
        pa.field("holding_count", pa.int64(), nullable=False),
        pa.field("stale_position_count", pa.int64(), nullable=False),
    ]
)


def write_results(
    output_dir: Path,
    config: BacktestConfig,
    result: BacktestResult,
    metrics: dict[str, Any],
    *,
    metadata: dict[str, Any] | None = None,
) -> Path:
    """将一次回测拆成便于人读的 JSON 和便于分析的 Parquet。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "config.json", config.to_dict())
    _write_json(output_dir / "metrics.json", metrics)
    if metadata is not None:
        _write_json(output_dir / "run_metadata.json", metadata)
    _write_parquet(
        output_dir / "orders.parquet",
        [
            {
                **asdict(order),
                "side": order.side.value,
            }
            for order in result.orders
        ],
        _ORDER_SCHEMA,
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
        _ORDER_UPDATE_SCHEMA,
    )
    _write_parquet(
        output_dir / "fills.parquet",
        [{**asdict(fill), "side": fill.side.value} for fill in result.fills],
        _FILL_SCHEMA,
    )
    _write_parquet(
        output_dir / "equity.parquet",
        [asdict(snapshot) for snapshot in result.equity],
        _EQUITY_SCHEMA,
    )
    return output_dir


def _write_json(path: Path, value: object) -> None:
    """使用统一 UTF-8 和缩进格式写 JSON。"""
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_parquet(path: Path, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
    """空表与非空表采用同一字段和类型，时间戳统一保存为 UTC。"""
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, compression="zstd")
