"""把 Tushare 与 QMT 的有效 Parquet 文件映射为 DuckDB 视图。"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

import duckdb
import pyarrow as pa

from qmt_receiver.schemas import TABLE_SCHEMAS as QMT_TABLE_SCHEMAS
from qmt_receiver.storage import load_sync_ranges
from tushare_data.schemas import TABLE_SCHEMAS

_QMT_SYNC_RANGE_SCHEMA = pa.schema(
    [
        pa.field("dataset", pa.string(), nullable=False),
        pa.field("code", pa.string(), nullable=False),
        pa.field("period", pa.string()),
        pa.field("start_date", pa.date32(), nullable=False),
        pa.field("end_date", pa.date32(), nullable=False),
    ]
)


class DataCatalog:
    """把 Tushare/QMT 当前有效的 Parquet 文件注册为 DuckDB 视图。"""

    def __init__(
        self,
        *,
        tushare_root: str | Path,
        qmt_root: str | Path,
    ) -> None:
        self._sources: dict[str, tuple[Path, Mapping[str, pa.Schema]]] = {
            "tushare": (Path(tushare_root).expanduser().resolve(), TABLE_SCHEMAS),
            "qmt": (Path(qmt_root).expanduser().resolve(), QMT_TABLE_SCHEMAS),
        }
        self._connection = duckdb.connect(":memory:")
        self._connection.execute("SET parquet_metadata_cache = true")
        self.refresh()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """返回已注册 `tushare` 和 `qmt` schema 的 DuckDB 连接。"""
        return self._connection

    def refresh(self) -> None:
        """根据最新 Manifest 重新注册原始视图和小型参考表。"""
        for source, (root, schemas) in self._sources.items():
            self._connection.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(source)}")
            for table, schema in schemas.items():
                _register_parquet_view(
                    self._connection,
                    source=source,
                    table=table,
                    files=_active_files(root / table),
                    schema=schema,
                )
        _refresh_reference_tables(self._connection)
        _refresh_qmt_sync_ranges(self._connection, self._sources["qmt"][0])

    def close(self) -> None:
        """关闭 DuckDB 连接。"""
        self._connection.close()

    def __enter__(self) -> DataCatalog:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _active_files(table_root: Path) -> list[Path]:
    """仅返回每个分区 Manifest 当前引用的文件。"""
    files: list[Path] = []
    for manifest_path in sorted(table_root.rglob("_manifest.json")):
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"无法读取 Parquet Manifest: {manifest_path}") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"Parquet Manifest 格式无效: {manifest_path}")
        names = payload.get("files")
        if (
            not isinstance(names, list)
            or any(
                not isinstance(name, str)
                or Path(name).name != name
                or not name.endswith(".parquet")
                for name in names
            )
            or len(names) != len(set(names))
        ):
            raise ValueError(f"Parquet Manifest 格式无效: {manifest_path}")
        committed = payload.get("file_committed_at")
        if committed is not None and (
            not isinstance(committed, dict)
            or set(committed) != set(names)
            or any(
                not isinstance(value, int) or isinstance(value, bool) or value < 1
                for value in committed.values()
            )
        ):
            raise ValueError(f"Parquet Manifest 格式无效: {manifest_path}")
        if isinstance(committed, dict):
            commit_times = committed
            names.sort(key=lambda name: commit_times[name])
        for name in names:
            path = manifest_path.parent / name
            if not path.is_file():
                raise ValueError(f"Manifest 引用的 Parquet 文件不存在: {path}")
            files.append(path)
    return files


def _register_parquet_view(
    connection: duckdb.DuckDBPyConnection,
    *,
    source: str,
    table: str,
    files: list[Path],
    schema: pa.Schema,
) -> None:
    """把一个确定的 Parquet 文件集合注册为视图。"""
    qualified = f"{_quote_identifier(source)}.{_quote_identifier(table)}"
    if files:
        paths = ", ".join(_quote_string(str(path)) for path in files)
        select = (
            f"SELECT * FROM read_parquet([{paths}], "
            "union_by_name = true, hive_partitioning = false)"
        )
    else:
        registration = f"__empty_{source}_{table}"
        connection.register(registration, schema.empty_table())
        select = f"SELECT * FROM {_quote_identifier(registration)}"
    connection.execute(f"CREATE OR REPLACE VIEW {qualified} AS {select}")


def _refresh_reference_tables(connection: duckdb.DuckDBPyConnection) -> None:
    """把高频读取的小表物化一次，避免每次查询重新打开数千个小文件。"""
    connection.execute("CREATE SCHEMA IF NOT EXISTS data_internal")
    connection.execute(
        "CREATE OR REPLACE TABLE data_internal.trade_cal AS SELECT * FROM tushare.trade_cal"
    )
    connection.execute(
        "CREATE OR REPLACE TABLE data_internal.sw_industry AS SELECT * FROM tushare.sw_industry"
    )


def _refresh_qmt_sync_ranges(
    connection: duckdb.DuckDBPyConnection,
    qmt_root: Path,
) -> None:
    """把 QMT 小型同步区间元数据物化，供复权覆盖检查使用。"""
    rows = [
        {
            "dataset": item.dataset,
            "code": item.code,
            "period": item.period,
            "start_date": item.start_date,
            "end_date": item.end_date,
        }
        for item in load_sync_ranges(qmt_root / "_meta" / "sync")
    ]
    connection.register(
        "__qmt_sync_ranges",
        pa.Table.from_pylist(rows, schema=_QMT_SYNC_RANGE_SCHEMA),
    )
    connection.execute(
        "CREATE OR REPLACE TABLE data_internal.qmt_sync_ranges AS SELECT * FROM __qmt_sync_ranges"
    )


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
