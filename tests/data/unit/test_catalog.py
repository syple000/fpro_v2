from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa

from data import DataCatalog
from tushare_data import TABLE_SCHEMAS, TushareDataStore


def _table(dataset: str, *rows: dict[str, object]) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=TABLE_SCHEMAS[dataset])


def test_catalog_exposes_only_raw_source_fields(tmp_path: Path) -> None:
    with DataCatalog(
        tushare_root=tmp_path / "tushare",
        qmt_root=tmp_path / "qmt",
    ) as catalog:
        columns = {
            row[0] for row in catalog.connection.execute("DESCRIBE tushare.cashflow").fetchall()
        }

    assert columns == set(TABLE_SCHEMAS["cashflow"].names)
    assert {"partition_date", "visible_at", "observed_at"}.isdisjoint(columns)


def test_open_snapshot_uses_the_current_manifest_file_set(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    with TushareDataStore(tushare_root) as store:
        store.write(
            "daily",
            _table("daily", {"ts_code": "000001.SZ", "trade_date": date(2024, 1, 2)}),
        )

    with (
        DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog,
        catalog.open_snapshot("tushare") as snapshot,
    ):
        rows = snapshot.connection.execute(
            "SELECT ts_code, trade_date FROM tushare.daily"
        ).fetchall()

    assert rows == [("000001.SZ", date(2024, 1, 2))]


def test_refresh_reloads_exact_manifest_file_set(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    with TushareDataStore(tushare_root) as store:
        store.write(
            "daily",
            _table("daily", {"ts_code": "000001.SZ", "trade_date": date(2024, 1, 2)}),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        assert catalog.connection.execute("SELECT count(*) FROM tushare.daily").fetchone() == (1,)
        with TushareDataStore(tushare_root) as store:
            store.write(
                "daily",
                _table(
                    "daily",
                    {"ts_code": "000001.SZ", "trade_date": date(2024, 1, 3)},
                ),
            )
        catalog.refresh()
        count = catalog.connection.execute("SELECT count(*) FROM tushare.daily").fetchone()

    assert count == (2,)
