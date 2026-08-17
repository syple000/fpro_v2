"""读取单个 quote Parquet 文件的前几行。"""

import json
import sys
from pathlib import Path

import pyarrow.parquet as pq


def read_parquet_file(path: str | Path, limit: int = 5) -> list[dict]:
    table = pq.ParquetFile(path).read().slice(0, limit)
    rows = table.to_pylist()
    for row in rows:
        row["trading_date"] = row["trading_date"].isoformat()
        row["quote"] = json.loads(row.pop("quote_json"))
    return rows


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("用法: python test_read_quote_parquet.py <parquet文件> [行数]")
    result = read_parquet_file(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 5)
    print(json.dumps(result, ensure_ascii=False, indent=2))

