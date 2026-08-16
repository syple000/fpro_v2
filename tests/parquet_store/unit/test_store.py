from __future__ import annotations

import json
from pathlib import Path
from typing import TypedDict, cast

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from parquet_store import ParquetStore, SchemaMismatchError, TableConfig


class ManifestPayload(TypedDict):
    version: int
    files: list[str]


SCHEMA = pa.schema(
    [
        pa.field("day", pa.string(), nullable=False),
        pa.field("id", pa.int64(), nullable=False),
        pa.field("value", pa.float64()),
    ],
    metadata={b"schema-version": b"1"},
)


def make_table(*rows: tuple[str, int, float]) -> pa.Table:
    return pa.Table.from_pylist(
        [{"day": day, "id": identifier, "value": value} for day, identifier, value in rows],
        schema=SCHEMA,
    )


def empty_table() -> pa.Table:
    return pa.Table.from_batches([], schema=SCHEMA)


def make_store(
    root: Path,
    *,
    max_buffer_rows: int = 100,
    max_buffer_bytes: int = 1024 * 1024,
    target_rows_per_file: int = 100,
) -> ParquetStore:
    store = ParquetStore(root)
    store.register(
        TableConfig(
            name="events",
            schema=SCHEMA,
            partition_by="day",
            sort_by="id",
            max_buffer_rows=max_buffer_rows,
            max_buffer_bytes=max_buffer_bytes,
            target_rows_per_file=target_rows_per_file,
        )
    )
    return store


def read_manifest(path: Path) -> ManifestPayload:
    return cast(ManifestPayload, json.loads(path.read_text(encoding="utf-8")))


def manifest_for(root: Path, day: str) -> tuple[Path, ManifestPayload]:
    manifests = list((root / "events").rglob("_manifest.json"))
    for path in manifests:
        payload = read_manifest(path)
        table = (
            pq.ParquetFile(path.parent / payload["files"][0]).read()
            if payload["files"]
            else None
        )
        if table is not None and table.column("day")[0].as_py() == day:
            return path, payload
    raise AssertionError(f"没有找到分区 {day!r} 的 Manifest")


def only_manifest(root: Path) -> tuple[Path, ManifestPayload]:
    paths = list((root / "events").rglob("_manifest.json"))
    assert len(paths) == 1
    return paths[0], read_manifest(paths[0])


def test_append_buffers_by_partition_and_flush_creates_immutable_files(tmp_path: Path) -> None:
    store = make_store(tmp_path, max_buffer_rows=3)

    store.append("events", make_table(("a", 2, 2.0), ("a", 1, 1.0)))
    assert store.read("events").num_rows == 0

    store.flush("events")
    assert store.read("events").to_pylist() == [
        {"day": "a", "id": 1, "value": 1.0},
        {"day": "a", "id": 2, "value": 2.0},
    ]
    manifest_path, first_manifest = only_manifest(tmp_path)
    first_file = first_manifest["files"][0]

    store.append("events", make_table(("a", 3, 3.0), ("a", 4, 4.0), ("a", 5, 5.0)))
    second_manifest = read_manifest(manifest_path)

    assert second_manifest["version"] == 2
    assert len(second_manifest["files"]) == 2
    assert first_file in second_manifest["files"]
    assert (manifest_path.parent / first_file).exists()
    assert store.read("events").num_rows == 5


def test_append_flushes_when_byte_threshold_is_reached(tmp_path: Path) -> None:
    store = make_store(tmp_path, max_buffer_rows=100, max_buffer_bytes=1)

    store.append("events", make_table(("a", 1, 1.0)))

    assert store.read("events").num_rows == 1


def test_partition_buffers_and_reads_are_isolated(tmp_path: Path) -> None:
    store = make_store(tmp_path, max_buffer_rows=2)

    store.append("events", make_table(("a", 1, 1.0), ("a", 2, 2.0), ("b", 3, 3.0)))

    assert store.read("events", partitions=[{"day": "a"}]).num_rows == 2
    assert store.read("events", partitions=[{"day": "b"}]).num_rows == 0
    store.flush()
    assert store.read("events", partitions=["b"]).to_pylist() == [
        {"day": "b", "id": 3, "value": 3.0}
    ]


def test_read_supports_projection_and_filtering(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    store.append(
        "events",
        make_table(("a", 1, 1.0), ("a", 2, 2.0), ("b", 3, 3.0), ("b", 4, 4.0)),
    )
    store.flush()

    result = store.read(
        "events",
        columns=["id"],
        filter=(ds.field("value") >= 2.0) & (ds.field("id") < 4),
    )
    tuple_filter_result = store.read("events", filter=[("id", ">", 2), ("id", "<", 4)])

    assert result.to_pylist() == [{"id": 2}, {"id": 3}]
    assert tuple_filter_result.column("id").to_pylist() == [3]


def test_replace_partition_replaces_only_target_and_splits_files(tmp_path: Path) -> None:
    store = make_store(tmp_path, target_rows_per_file=2)
    store.append("events", make_table(("a", 1, 1.0), ("a", 2, 2.0), ("b", 9, 9.0)))
    store.flush()
    manifest_path, old_manifest = manifest_for(tmp_path, "a")
    old_files = set(old_manifest["files"])

    replacement = make_table(
        ("a", 15, 15.0),
        ("a", 11, 11.0),
        ("a", 14, 14.0),
        ("a", 12, 12.0),
        ("a", 13, 13.0),
    )
    store.replace_partition("events", {"day": "a"}, replacement)

    new_manifest = read_manifest(manifest_path)
    assert new_manifest["version"] == 2
    assert len(new_manifest["files"]) == 3
    assert [
        pq.ParquetFile(manifest_path.parent / name).metadata.num_rows
        for name in new_manifest["files"]
    ] == [2, 2, 1]
    assert store.read("events", partitions="a").column("id").to_pylist() == [11, 12, 13, 14, 15]
    assert store.read("events", partitions="b").column("id").to_pylist() == [9]
    assert all(not (manifest_path.parent / name).exists() for name in old_files)


def test_empty_replace_clears_partition_and_discards_its_buffer(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    store.append("events", make_table(("a", 1, 1.0), ("b", 2, 2.0)))
    store.flush()
    store.append("events", make_table(("a", 3, 3.0)))

    store.replace_partition("events", "a", empty_table())
    store.flush()

    assert store.read("events", partitions="a").num_rows == 0
    assert store.read("events", partitions="b").num_rows == 1
    _, manifest = next(
        (path, read_manifest(path))
        for path in (tmp_path / "events").rglob("_manifest.json")
        if not read_manifest(path)["files"]
    )
    assert manifest["files"] == []


def test_compact_merges_files_without_changing_records(tmp_path: Path) -> None:
    store = make_store(tmp_path, max_buffer_rows=1, target_rows_per_file=10)
    for identifier in (3, 1, 2):
        store.append("events", make_table(("a", identifier, float(identifier))))
    manifest_path, old_manifest = only_manifest(tmp_path)
    old_files = set(old_manifest["files"])

    store.compact_partition("events", "a")

    new_manifest = read_manifest(manifest_path)
    assert new_manifest["version"] == old_manifest["version"] + 1
    assert len(new_manifest["files"]) == 1
    assert store.read("events").column("id").to_pylist() == [1, 2, 3]
    assert all(not (manifest_path.parent / name).exists() for name in old_files)


def test_compact_single_file_is_no_op(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    store.append("events", make_table(("a", 1, 1.0), ("a", 2, 2.0)))
    store.flush()
    manifest_path, manifest = only_manifest(tmp_path)
    original_bytes = manifest_path.read_bytes()

    store.compact_partition("events", {"day": "a"})

    assert manifest_path.read_bytes() == original_bytes
    assert read_manifest(manifest_path) == manifest


def test_schema_must_match_fields_order_nullability_and_metadata(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    wrong_schema = pa.schema([("day", pa.string()), ("id", pa.int64()), ("value", pa.float64())])
    wrong_data = pa.Table.from_pylist(
        [{"day": "a", "id": 1, "value": 1.0}],
        schema=wrong_schema,
    )

    with pytest.raises(SchemaMismatchError, match="Schema 不匹配"):
        store.append("events", wrong_data)
    with pytest.raises(SchemaMismatchError, match="Schema 不匹配"):
        store.replace_partition("events", "a", wrong_data)


def test_replace_rejects_rows_from_another_partition(tmp_path: Path) -> None:
    store = make_store(tmp_path)

    with pytest.raises(ValueError, match="全部属于指定分区"):
        store.replace_partition(
            "events",
            "a",
            make_table(("a", 1, 1.0), ("b", 2, 2.0)),
        )

    assert not list((tmp_path / "events").rglob("_manifest.json"))


def test_reader_ignores_temporary_and_unlisted_parquet_files(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    store.append("events", make_table(("a", 1, 1.0)))
    store.flush()
    manifest_path, _ = only_manifest(tmp_path)
    invisible = make_table(("a", 99, 99.0))
    pq.write_table(invisible, manifest_path.parent / ".write-in-progress.tmp")
    pq.write_table(invisible, manifest_path.parent / "part-unlisted.parquet")

    result = store.read("events")

    assert result.column("id").to_pylist() == [1]


def test_close_flushes_buffers_and_rejects_further_operations(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    store.append("events", make_table(("a", 1, 1.0)))

    store.close()

    with pytest.raises(RuntimeError, match="已关闭"):
        store.read("events")
    reopened = make_store(tmp_path)
    assert reopened.read("events").num_rows == 1
