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


def test_refresh_rebuilds_reference_table_cache(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    with TushareDataStore(tushare_root) as store:
        store.write(
            "trade_cal",
            _table(
                "trade_cal",
                {"exchange": "SSE", "cal_date": date(2024, 1, 2), "is_open": 1},
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        assert catalog.connection.execute(
            "SELECT count(*) FROM data_internal.trade_cal"
        ).fetchone() == (1,)
        with TushareDataStore(tushare_root) as store:
            store.write(
                "trade_cal",
                _table(
                    "trade_cal",
                    {"exchange": "SSE", "cal_date": date(2024, 1, 3), "is_open": 1},
                ),
            )
        catalog.refresh()
        count = catalog.connection.execute(
            "SELECT count(*) FROM data_internal.trade_cal"
        ).fetchone()

    assert count == (2,)
