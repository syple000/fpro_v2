"""把 Tushare 与 QMT 的有效 Parquet 文件映射为 DuckDB 视图。"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

import duckdb
import pyarrow as pa

from qmt_receiver import (
    BAR_SCHEMA,
    BAR_TABLE,
    DAILY_SCHEMA,
    DAILY_TABLE,
    DIVIDEND_FACTOR_SCHEMA,
    DIVIDEND_FACTOR_TABLE,
    FINANCIAL_SCHEMA,
    FINANCIAL_TABLE,
    TICK_SCHEMA,
    TICK_TABLE,
)
from tushare_data.schemas import TABLE_SCHEMAS

_QMT_SCHEMAS = {
    TICK_TABLE: TICK_SCHEMA,
    BAR_TABLE: BAR_SCHEMA,
    DAILY_TABLE: DAILY_SCHEMA,
    FINANCIAL_TABLE: FINANCIAL_SCHEMA,
    DIVIDEND_FACTOR_TABLE: DIVIDEND_FACTOR_SCHEMA,
}


@dataclass(slots=True)
class CatalogSnapshot:
    """显式绑定 Manifest 文件集合的只读 DuckDB 快照。"""

    connection: duckdb.DuckDBPyConnection
    snapshot_id: str
    _lease: tempfile.TemporaryDirectory[str]

    def close(self) -> None:
        try:
            self.connection.close()
        finally:
            self._lease.cleanup()

    def __enter__(self) -> CatalogSnapshot:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class DataCatalog:
    """注册 Tushare/QMT 原始视图并固定 Manifest 文件快照。"""

    def __init__(
        self,
        *,
        tushare_root: str | Path,
        qmt_root: str | Path,
    ) -> None:
        self._sources: dict[str, tuple[Path, Mapping[str, pa.Schema]]] = {
            "tushare": (Path(tushare_root).expanduser().resolve(), TABLE_SCHEMAS),
            "qmt": (Path(qmt_root).expanduser().resolve(), _QMT_SCHEMAS),
        }
        self._connection = duckdb.connect(":memory:")
        self.refresh()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """返回已注册 `tushare` 和 `qmt` schema 的 DuckDB 连接。"""
        return self._connection

    def refresh(self) -> None:
        """根据最新 Manifest 重新注册原始视图。"""
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

    def open_snapshot(self, source: str) -> CatalogSnapshot:
        """按当前 Manifest 打开一个不随 `refresh()` 改变的来源快照。"""
        try:
            root, schemas = self._sources[source]
        except KeyError:
            raise ValueError(f"DataCatalog 不支持来源: {source!r}") from None

        connection = duckdb.connect(":memory:")
        connection.execute("SET TimeZone = 'Asia/Shanghai'")
        lease = tempfile.TemporaryDirectory(prefix="fpro-data-snapshot-")
        lease_root = Path(lease.name)
        digest = sha256()
        try:
            connection.execute(f"CREATE SCHEMA {_quote_identifier(source)}")
            for table, schema in schemas.items():
                files = _active_files(root / table)
                digest.update(source.encode())
                digest.update(table.encode())
                for path in files:
                    digest.update(str(path).encode())
                    stat = path.stat()
                    digest.update(str(stat.st_size).encode())
                    digest.update(str(stat.st_mtime_ns).encode())
                leased_files = _lease_files(files, lease_root / table)
                _register_parquet_view(
                    connection,
                    source=source,
                    table=table,
                    files=leased_files,
                    schema=schema,
                )
        except BaseException:
            connection.close()
            lease.cleanup()
            raise
        return CatalogSnapshot(
            connection=connection,
            snapshot_id=digest.hexdigest()[:24],
            _lease=lease,
        )

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


def _lease_files(files: list[Path], target_root: Path) -> list[Path]:
    """用硬链接保留快照文件；跨文件系统时退化为复制。"""
    if not files:
        return []
    target_root.mkdir(parents=True, exist_ok=True)
    leased: list[Path] = []
    for index, source in enumerate(files):
        target = target_root / f"{index:08d}-{source.name}"
        try:
            os.link(source, target)
        except OSError:
            shutil.copy2(source, target)
        leased.append(target)
    return leased


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
