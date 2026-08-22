"""把 quote Parquet 转换为临时 CSV 文件。"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import date, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

import pyarrow as pa
import pyarrow.parquet as pq


def parquet_to_temp_csv(
    path: str | Path,
    limit: int | None = None,
    *,
    batch_size: int = 65_536,
) -> Path:
    """流式转换 Parquet，展开 struct 列并返回临时 CSV 路径。"""
    source = Path(path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"Parquet 文件不存在: {source}")
    if limit is not None and limit < 1:
        raise ValueError("limit 必须大于等于 1")
    if batch_size < 1:
        raise ValueError("batch_size 必须大于等于 1")

    parquet = pq.ParquetFile(source)
    empty = pa.Table.from_batches([], schema=parquet.schema_arrow).flatten()
    output: Path | None = None
    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            prefix="quote-parquet-",
            suffix=".csv",
            delete=False,
        ) as temporary:
            output = Path(temporary.name)
            writer = csv.writer(temporary)
            writer.writerow(empty.schema.names)

            remaining = limit
            for batch in parquet.iter_batches(batch_size=batch_size):
                table = pa.Table.from_batches([batch]).flatten()
                if remaining is not None:
                    table = table.slice(0, remaining)
                columns = [column.to_pylist() for column in table.columns]
                for row in zip(*columns, strict=True):
                    writer.writerow([_csv_value(value) for value in row])
                if remaining is not None:
                    remaining -= table.num_rows
                    if remaining == 0:
                        break
    except BaseException:
        if output is not None:
            output.unlink(missing_ok=True)
        raise

    if output is None:
        raise RuntimeError("未能创建临时 CSV 文件")
    return output


def _csv_value(value: object) -> object:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="把 quote Parquet 转换为临时 CSV")
    parser.add_argument("parquet_file", type=Path, help="Parquet 文件路径")
    parser.add_argument("limit", nargs="?", type=int, help="可选：最多转换的行数")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = _parse_args()
    csv_path = parquet_to_temp_csv(arguments.parquet_file, arguments.limit)
    print(csv_path)
