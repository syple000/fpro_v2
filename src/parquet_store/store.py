"""轻量的本地、单进程 Parquet 存储。"""

from __future__ import annotations

import json
import os
import warnings
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from threading import RLock
from typing import Any, TypeAlias, TypeGuard, cast
from urllib.parse import quote
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

MANIFEST_NAME = "_manifest.json"
_PROCESS_LOCKS: dict[tuple[str, str, str], RLock] = {}
_PROCESS_LOCKS_GUARD = RLock()

PartitionSelector: TypeAlias = Mapping[str, Any] | Sequence[Any] | Any
FilterPredicate: TypeAlias = tuple[str, str, Any]
FilterSpec: TypeAlias = (
    ds.Expression
    | FilterPredicate
    | Sequence[FilterPredicate]
    | Sequence[Sequence[FilterPredicate]]
)


class SchemaMismatchError(ValueError):
    """输入 Arrow Schema 与注册 Schema 不完全一致。"""


@dataclass(frozen=True, slots=True)
class TableConfig:
    """一张逻辑表的固定配置。"""

    name: str
    schema: pa.Schema
    partition_by: str | Sequence[str]
    sort_by: str | Sequence[str] | None = None
    max_buffer_rows: int = 100_000
    max_buffer_bytes: int = 64 * 1024 * 1024
    target_rows_per_file: int = 1_000_000
    primary_key: str | Sequence[str] | None = None

    def __post_init__(self) -> None:
        if not self.name or Path(self.name).name != self.name or self.name in {".", ".."}:
            raise ValueError("表名必须是非空的单个路径段")
        if not isinstance(self.schema, pa.Schema):
            raise TypeError("schema 必须是 pyarrow.Schema")
        if len(self.schema.names) != len(set(self.schema.names)):
            raise ValueError("Schema 不能包含重复字段名")

        partition_by = _column_tuple(self.partition_by, "partition_by")
        sort_by = () if self.sort_by is None else _column_tuple(self.sort_by, "sort_by")
        primary_key = (
            () if self.primary_key is None else _column_tuple(self.primary_key, "primary_key")
        )
        if not partition_by:
            raise ValueError("partition_by 至少需要一个字段")
        if self.primary_key is not None and not primary_key:
            raise ValueError("primary_key 至少需要一个字段")
        if len(set(partition_by)) != len(partition_by):
            raise ValueError("partition_by 不能包含重复字段")
        if len(set(sort_by)) != len(sort_by):
            raise ValueError("sort_by 不能包含重复字段")
        if len(set(primary_key)) != len(primary_key):
            raise ValueError("primary_key 不能包含重复字段")

        missing = [
            name
            for name in (*partition_by, *sort_by, *primary_key)
            if name not in self.schema.names
        ]
        if missing:
            raise ValueError(f"Schema 中不存在配置字段: {missing}")
        if self.max_buffer_rows < 1:
            raise ValueError("max_buffer_rows 必须大于等于 1")
        if self.max_buffer_bytes < 1:
            raise ValueError("max_buffer_bytes 必须大于等于 1")
        if self.target_rows_per_file < 1:
            raise ValueError("target_rows_per_file 必须大于等于 1")

        object.__setattr__(self, "partition_by", partition_by)
        object.__setattr__(self, "sort_by", sort_by)
        object.__setattr__(
            self,
            "primary_key",
            None if self.primary_key is None else primary_key,
        )


@dataclass(slots=True)
class _Buffer:
    tables: list[pa.Table] = field(default_factory=list)
    rows: int = 0
    bytes: int = 0

    def append(self, table: pa.Table) -> None:
        self.tables.append(table)
        self.rows += table.num_rows
        self.bytes += table.nbytes


@dataclass(frozen=True, slots=True)
class _Manifest:
    version: int = 0
    files: tuple[str, ...] = ()


class ParquetStore:
    """以分区 Manifest 为提交点的本地 Parquet 存储。"""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self._root = Path(root).expanduser().resolve()
        self._root.mkdir(parents=True, exist_ok=True)
        self._configs: dict[str, TableConfig] = {}
        self._buffers: dict[tuple[str, str], _Buffer] = {}
        self._state_lock = RLock()
        self._closed = False

    def register(self, config: TableConfig) -> None:
        """注册固定表配置；相同配置可重复注册。"""
        with self._state_lock:
            self._ensure_open()
            current = self._configs.get(config.name)
            if current is not None and current != config:
                raise ValueError(f"表 {config.name!r} 已使用不同配置注册")
            self._configs[config.name] = config
        self._table_path(config.name).mkdir(parents=True, exist_ok=True)

    def update_schema(self, table: str, schema: pa.Schema) -> None:
        """在现有 Schema 末尾追加可空字段。"""
        current = self._config(table)
        updated = replace(current, schema=schema)
        if current.schema.equals(updated.schema, check_metadata=True):
            return
        _validate_schema_extension(current.schema, updated.schema)

        # 旧缓冲先按旧 Schema 落盘；读取时 PyArrow 会为新增字段补 null。
        self.flush(table)
        with self._state_lock:
            self._ensure_open()
            self._configs[table] = updated

    def append(self, table: str, data: pa.Table) -> None:
        """按分区缓存数据，达到任一阈值时提交一个新文件。"""
        config = self._config(table)
        self._validate_schema(config, data)
        for partition_id, indices in self._group_rows(config, data).items():
            partition_data = data.take(pa.array(indices, type=pa.int64()))
            lock = self._partition_lock(table, partition_id)
            with lock:
                key = (table, partition_id)
                with self._state_lock:
                    buffer = self._buffers.setdefault(key, _Buffer())
                    buffer.append(partition_data)
                    should_flush = (
                        buffer.rows >= config.max_buffer_rows
                        or buffer.bytes >= config.max_buffer_bytes
                    )
                if should_flush:
                    self._flush_partition(config, partition_id)

    def flush(self, table: str | None = None) -> None:
        """提交指定表或所有表当前已缓存的数据。"""
        if table is not None:
            self._config(table)
        else:
            with self._state_lock:
                self._ensure_open()

        with self._state_lock:
            keys = (
                sorted(
                    key for key, buffer in self._buffers.items() if buffer.rows and key[0] == table
                )
                if table is not None
                else sorted(key for key, buffer in self._buffers.items() if buffer.rows)
            )

        for table_name, partition_id in keys:
            config = self._config(table_name)
            with self._partition_lock(table_name, partition_id):
                self._flush_partition(config, partition_id)

    def read(
        self,
        table: str,
        partitions: object | None = None,
        columns: Sequence[str] | None = None,
        filter: FilterSpec | None = None,
    ) -> pa.Table:
        """只从当前 Manifest 列出的文件读取 Arrow Table。"""
        config = self._config(table)
        expression = _filter_expression(filter)
        selected_columns = None if columns is None else list(columns)
        partition_ids = self._read_partition_ids(config, partitions)
        results: list[pa.Table] = []

        for partition_id in partition_ids:
            with self._partition_lock(table, partition_id):
                directory = self._partition_path(table, partition_id)
                manifest = self._load_manifest(directory)
                paths = [directory / name for name in manifest.files]
                results.extend(
                    _read_parquet_files(paths, config.schema, selected_columns, expression)
                )

        if results:
            return pa.concat_tables(results)
        return _empty_result(config.schema, selected_columns, expression)

    def replace_partition(
        self,
        table: str,
        partition: PartitionSelector,
        data: pa.Table,
    ) -> None:
        """以输入数据完整替换一个分区；空 Table 表示清空。"""
        config = self._config(table)
        self._validate_schema(config, data)
        partition_id = self._partition_id_from_selector(config, partition)
        if data.num_rows:
            actual_partitions = set(self._group_rows(config, data))
            if actual_partitions != {partition_id}:
                raise ValueError("替换数据必须全部属于指定分区")

        with self._partition_lock(table, partition_id):
            prepared = self._sort(config, data)
            chunks = _split_table(prepared, config.target_rows_per_file)
            self._commit(config, partition_id, chunks, keep_current=False)
            with self._state_lock:
                self._buffers.pop((table, partition_id), None)

    def compact_partition(self, table: str, partition: PartitionSelector) -> None:
        """重新组织分区；配置主键时先按主键保留最后一个物理版本。"""
        config = self._config(table)
        partition_id = self._partition_id_from_selector(config, partition)
        with self._partition_lock(table, partition_id):
            directory = self._partition_path(table, partition_id)
            manifest = self._load_manifest(directory)
            if not manifest.files or (len(manifest.files) == 1 and not config.primary_key):
                return
            paths = [directory / name for name in manifest.files]
            tables = _read_parquet_files(paths, config.schema, None, None)
            data = pa.concat_tables(tables) if tables else _empty_table(config.schema)
            deduplicated = self._deduplicate_primary_key(config, data)
            if len(manifest.files) == 1 and deduplicated.num_rows == data.num_rows:
                return
            prepared = self._sort(config, deduplicated)
            chunks = _split_table(prepared, config.target_rows_per_file)
            self._commit(config, partition_id, chunks, keep_current=False)

    def close(self) -> None:
        """刷新全部缓存并关闭实例；可重复调用。"""
        with self._state_lock:
            if self._closed:
                return
        self.flush()
        with self._state_lock:
            self._closed = True

    def __enter__(self) -> ParquetStore:
        with self._state_lock:
            self._ensure_open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _config(self, table: str) -> TableConfig:
        with self._state_lock:
            self._ensure_open()
            try:
                return self._configs[table]
            except KeyError:
                raise KeyError(f"表 {table!r} 尚未注册") from None

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("ParquetStore 已关闭")

    def _validate_schema(self, config: TableConfig, data: pa.Table) -> None:
        if not isinstance(data, pa.Table):
            raise TypeError("data 必须是 pyarrow.Table")
        if not data.schema.equals(config.schema, check_metadata=True):
            raise SchemaMismatchError(
                f"表 {config.name!r} 的输入 Schema 不匹配；期望 {config.schema}，实际 {data.schema}"
            )

    def _group_rows(self, config: TableConfig, data: pa.Table) -> dict[str, list[int]]:
        groups: dict[str, list[int]] = {}
        names = tuple(config.partition_by)
        for index, row in enumerate(data.select(names).to_pylist()):
            values = tuple(row[name] for name in names)
            partition_id = self._partition_id(config, values)
            groups.setdefault(partition_id, []).append(index)
        return groups

    def _partition_id_from_selector(
        self,
        config: TableConfig,
        selector: PartitionSelector,
    ) -> str:
        names = tuple(config.partition_by)
        if isinstance(selector, Mapping):
            if set(selector) != set(names):
                raise ValueError(f"分区必须且只能包含字段 {names}")
            raw_values = tuple(selector[name] for name in names)
        elif len(names) == 1 and not _is_non_string_sequence(selector):
            raw_values = (selector,)
        elif _is_non_string_sequence(selector) and len(selector) == len(names):
            raw_values = tuple(selector)
        else:
            raise ValueError(f"分区值数量必须与 partition_by {names} 一致")

        values = tuple(
            _coerce_scalar(raw, config.schema.field(name).type)
            for name, raw in zip(names, raw_values, strict=True)
        )
        return self._partition_id(config, values)

    def _partition_id(self, config: TableConfig, values: Sequence[Any]) -> str:
        components = []
        for name, value in zip(config.partition_by, values, strict=True):
            field = config.schema.field(name)
            scalar = pa.scalar(value, type=field.type)
            encoded = "null" if not scalar.is_valid else f"value:{scalar}"
            components.append(f"{quote(name, safe='')}={quote(encoded, safe='')}")
        return "/".join(components)

    def _read_partition_ids(self, config: TableConfig, partitions: object | None) -> list[str]:
        if partitions is None:
            table_path = self._table_path(config.name)
            manifests = sorted(table_path.rglob(MANIFEST_NAME))
            return [manifest.parent.relative_to(table_path).as_posix() for manifest in manifests]

        selectors = _partition_selector_list(tuple(config.partition_by), partitions)
        return sorted({self._partition_id_from_selector(config, item) for item in selectors})

    def _flush_partition(self, config: TableConfig, partition_id: str) -> None:
        key = (config.name, partition_id)
        with self._state_lock:
            buffer = self._buffers.get(key)
            if buffer is None or not buffer.rows:
                return
            data = pa.concat_tables(buffer.tables)

        prepared = self._sort(config, data)
        self._commit(config, partition_id, [prepared], keep_current=True)
        with self._state_lock:
            if self._buffers.get(key) is buffer:
                del self._buffers[key]

    def _sort(self, config: TableConfig, data: pa.Table) -> pa.Table:
        sort_by = tuple(config.sort_by or ())
        if data.num_rows <= 1 or not sort_by:
            return data
        return data.sort_by([(name, "ascending") for name in sort_by])

    def _deduplicate_primary_key(self, config: TableConfig, data: pa.Table) -> pa.Table:
        primary_key = tuple(config.primary_key or ())
        if not primary_key:
            return data
        invalid = [name for name in primary_key if data.column(name).null_count]
        if invalid:
            raise ValueError(f"主键字段不能包含 null: {invalid}")
        if data.num_rows <= 1:
            return data

        position_name = "__parquet_store_row_position__"
        while position_name in data.schema.names:
            position_name = f"_{position_name}"
        positions = pa.array(range(data.num_rows), type=pa.int64())
        keyed = data.select(primary_key).append_column(position_name, positions)
        grouped = keyed.group_by(list(primary_key), use_threads=False).aggregate(
            [(position_name, "max")]
        )
        selected = grouped.column(grouped.num_columns - 1).combine_chunks()
        if len(selected) == data.num_rows:
            return data
        selected_order = cast(pa.Int64Array, pc.sort_indices(selected).cast(pa.int64()))
        selected_positions = cast(pa.Int64Array, pc.take(selected, selected_order))
        return data.take(selected_positions)

    def _commit(
        self,
        config: TableConfig,
        partition_id: str,
        chunks: Sequence[pa.Table],
        *,
        keep_current: bool,
    ) -> None:
        directory = self._partition_path(config.name, partition_id)
        directory.mkdir(parents=True, exist_ok=True)
        current = self._load_manifest(directory)
        created = self._write_files(directory, chunks)
        files = (*current.files, *created) if keep_current else tuple(created)
        manifest = _Manifest(version=current.version + 1, files=tuple(files))
        try:
            self._write_manifest(directory, manifest)
        except BaseException:
            _unlink_files(directory / name for name in created)
            raise
        self._cleanup_unreferenced_files(directory, set(manifest.files))

    def _write_files(self, directory: Path, chunks: Sequence[pa.Table]) -> list[str]:
        created: list[str] = []
        temporary: Path | None = None
        try:
            for chunk in chunks:
                filename = f"part-{uuid4().hex}.parquet"
                final_path = directory / filename
                temporary = directory / f".{filename}.{uuid4().hex}.tmp"
                if final_path.exists() or temporary.exists():
                    raise FileExistsError("生成了重复的 Parquet 文件名")
                pq.write_table(chunk, temporary)
                _fsync_file(temporary)
                os.rename(temporary, final_path)
                temporary = None
                created.append(filename)
        except BaseException:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            _unlink_files(directory / name for name in created)
            raise
        return created

    def _write_manifest(self, directory: Path, manifest: _Manifest) -> None:
        temporary = directory / f".{MANIFEST_NAME}.{uuid4().hex}.tmp"
        payload = {"version": manifest.version, "files": list(manifest.files)}
        try:
            with temporary.open("x", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False, indent=2, sort_keys=True)
                file.write("\n")
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, directory / MANIFEST_NAME)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    def _load_manifest(self, directory: Path) -> _Manifest:
        path = directory / MANIFEST_NAME
        if not path.exists():
            return _Manifest()
        with path.open(encoding="utf-8") as file:
            payload = json.load(file)
        version = payload.get("version")
        files = payload.get("files")
        if (
            not isinstance(version, int)
            or version < 1
            or not isinstance(files, list)
            or any(
                not isinstance(name, str)
                or Path(name).name != name
                or not name.endswith(".parquet")
                for name in files
            )
            or len(files) != len(set(files))
        ):
            raise ValueError(f"Manifest 格式无效: {path}")
        return _Manifest(version=version, files=tuple(files))

    def _cleanup_unreferenced_files(self, directory: Path, active: set[str]) -> None:
        try:
            _unlink_files(path for path in directory.glob("*.parquet") if path.name not in active)
        except OSError as exc:
            warnings.warn(f"清理失效 Parquet 文件失败: {exc}", RuntimeWarning, stacklevel=2)

    def _partition_lock(self, table: str, partition_id: str) -> RLock:
        key = (str(self._root), table, partition_id)
        with _PROCESS_LOCKS_GUARD:
            return _PROCESS_LOCKS.setdefault(key, RLock())

    def _table_path(self, table: str) -> Path:
        return self._root / table

    def _partition_path(self, table: str, partition_id: str) -> Path:
        return self._table_path(table) / partition_id


def _column_tuple(value: str | Sequence[str], name: str) -> tuple[str, ...]:
    result = (value,) if isinstance(value, str) else tuple(value)
    if any(not isinstance(item, str) or not item for item in result):
        raise ValueError(f"{name} 只能包含非空字段名")
    return result


def _validate_schema_extension(current: pa.Schema, updated: pa.Schema) -> None:
    if len(updated) <= len(current):
        raise SchemaMismatchError("更新 Schema 只能在末尾新增字段")
    if any(
        not current.field(index).equals(updated.field(index), check_metadata=True)
        for index in range(len(current))
    ):
        raise SchemaMismatchError("更新 Schema 不能修改、删除或重排已有字段")
    if any(not updated.field(index).nullable for index in range(len(current), len(updated))):
        raise SchemaMismatchError("新增字段必须允许 null")


def _coerce_scalar(value: Any, data_type: pa.DataType) -> Any:
    if isinstance(value, pa.Scalar):
        value = value.as_py()
    try:
        return pa.array([value], type=data_type)[0].as_py()
    except (pa.ArrowException, TypeError, ValueError) as exc:
        raise ValueError(f"分区值 {value!r} 无法转换为 {data_type}") from exc


def _is_non_string_sequence(value: object) -> TypeGuard[Sequence[Any]]:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _partition_selector_list(names: tuple[str, ...], partitions: object) -> list[Any]:
    if isinstance(partitions, Mapping):
        return [partitions]
    if len(names) == 1:
        if _is_non_string_sequence(partitions):
            return list(partitions)
        return [partitions]
    if not _is_non_string_sequence(partitions):
        raise ValueError("多字段分区必须使用映射、值元组或它们的序列")
    if len(partitions) == len(names) and not any(
        isinstance(item, Mapping) or _is_non_string_sequence(item) for item in partitions
    ):
        return [partitions]
    return list(partitions)


def _split_table(data: pa.Table, target_rows: int) -> list[pa.Table]:
    return [
        data.slice(offset, min(target_rows, data.num_rows - offset))
        for offset in range(0, data.num_rows, target_rows)
    ]


def _read_parquet_files(
    paths: Iterable[Path],
    schema: pa.Schema,
    columns: list[str] | None,
    expression: ds.Expression | None,
) -> list[pa.Table]:
    results = []
    for path in paths:
        dataset = ds.dataset(path, schema=schema, format="parquet")
        results.append(dataset.to_table(columns=columns, filter=expression))
    return results


def _empty_result(
    schema: pa.Schema,
    columns: list[str] | None,
    expression: ds.Expression | None,
) -> pa.Table:
    dataset = ds.dataset(_empty_table(schema))
    return dataset.to_table(columns=columns, filter=expression)


def _empty_table(schema: pa.Schema) -> pa.Table:
    return pa.Table.from_batches([], schema=schema)


def _filter_expression(filter: FilterSpec | None) -> ds.Expression | None:
    if filter is None or isinstance(filter, ds.Expression):
        return filter
    if _is_filter_predicate(filter):
        return _predicate_expression(filter)

    groups = list(filter)
    if not groups:
        return None

    predicates: list[FilterPredicate] = []
    for item in groups:
        if not _is_filter_predicate(item):
            break
        predicates.append(item)
    else:
        return _combine_expressions(_predicate_expression(item) for item in predicates)

    disjunction: ds.Expression | None = None
    for group in groups:
        if not _is_non_string_sequence(group):
            raise ValueError("析取过滤条件必须由三元组列表组成")
        predicates = []
        for item in group:
            if not _is_filter_predicate(item):
                raise ValueError("过滤条件必须是 (字段, 操作符, 值) 三元组")
            predicates.append(item)
        conjunction = _combine_expressions(_predicate_expression(item) for item in predicates)
        disjunction = conjunction if disjunction is None else disjunction | conjunction
    return disjunction


def _is_filter_predicate(value: object) -> TypeGuard[FilterPredicate]:
    return (
        isinstance(value, tuple)
        and len(value) == 3
        and isinstance(value[0], str)
        and isinstance(value[1], str)
    )


def _predicate_expression(predicate: FilterPredicate) -> ds.Expression:
    name, operator, value = predicate
    field = ds.field(name)
    if operator in {"=", "=="}:
        return field.is_null() if value is None else field == value
    if operator == "!=":
        return ~field.is_null() if value is None else field != value
    if operator == "<":
        return field < value
    if operator == "<=":
        return field <= value
    if operator == ">":
        return field > value
    if operator == ">=":
        return field >= value
    if operator == "in":
        return field.isin(value)
    if operator == "not in":
        return ~field.isin(value)
    raise ValueError(f"不支持的过滤操作符: {operator!r}")


def _combine_expressions(expressions: Iterable[ds.Expression]) -> ds.Expression:
    result: ds.Expression | None = None
    for expression in expressions:
        result = expression if result is None else result & expression
    if result is None:
        raise ValueError("过滤条件组不能为空")
    return result


def _fsync_file(path: Path) -> None:
    # Windows 的 CRT 不允许对只读文件描述符执行 fsync；r+b 不会截断或改写文件，
    # 只为刷新刚完成的 Parquet 写入提供一个可同步的文件描述符。
    with path.open("r+b") as file:
        os.fsync(file.fileno())


def _unlink_files(paths: Iterable[Path]) -> None:
    for path in paths:
        path.unlink(missing_ok=True)


def main() -> None:
    """运行一个不会在项目中留下数据文件的最小示例。"""
    from tempfile import TemporaryDirectory

    schema = pa.schema(
        [
            ("group", pa.string()),
            ("id", pa.int64()),
            ("value", pa.float64()),
        ]
    )
    data = pa.Table.from_pylist(
        [
            {"group": "a", "id": 2, "value": -1.0},
            {"group": "a", "id": 1, "value": 3.5},
            {"group": "b", "id": 3, "value": 8.0},
        ],
        schema=schema,
    )

    with TemporaryDirectory(prefix="parquet-store-") as root, ParquetStore(root) as store:
        store.register(
            TableConfig(
                name="events",
                schema=schema,
                partition_by="group",
                sort_by="id",
                max_buffer_rows=10,
                max_buffer_bytes=1024 * 1024,
                target_rows_per_file=10,
            )
        )
        store.append("events", data)
        store.flush("events")
        result = store.read(
            "events",
            partitions=[{"group": "a"}],
            columns=["id", "value"],
            filter=ds.field("value") > 0,
        )
        print(result)


if __name__ == "__main__":
    main()
