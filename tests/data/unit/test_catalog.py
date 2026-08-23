from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa

from data import DataCatalog
from qmt_protocol import SequencedQuote, TickQuote
from qmt_receiver import QmtDataStore
from tushare_data import TABLE_SCHEMAS, TushareDataStore


def _table(dataset: str, *rows: dict[str, object]) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=TABLE_SCHEMAS[dataset])


def _cashflow(
    *,
    f_ann_date: date,
    update_flag: str,
    free_cashflow: float,
) -> dict[str, object]:
    return {
        "ts_code": "000001.SZ",
        "ann_date": date(2024, 4, 18),
        "f_ann_date": f_ann_date,
        "end_date": date(2023, 12, 31),
        "report_type": "1",
        "comp_type": "1",
        "update_flag": update_flag,
        "free_cashflow": free_cashflow,
    }


def test_tushare_statement_as_of_selects_source_declared_revision(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    with TushareDataStore(tushare_root) as store:
        store.write(
            "cashflow",
            _table(
                "cashflow",
                _cashflow(
                    f_ann_date=date(2024, 4, 18),
                    update_flag="0",
                    free_cashflow=1.0,
                ),
                _cashflow(
                    f_ann_date=date(2025, 4, 29),
                    update_flag="1",
                    free_cashflow=2.0,
                ),
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        raw_columns = {
            row[0] for row in catalog.connection.execute("DESCRIBE tushare.cashflow").fetchall()
        }
        before = catalog.connection.execute(
            "SELECT f_ann_date, free_cashflow FROM tushare.cashflow_as_of(DATE '2024-12-31')"
        ).fetchall()
        after = catalog.connection.execute(
            "SELECT f_ann_date, free_cashflow FROM tushare.cashflow_as_of(DATE '2025-04-29')"
        ).fetchall()

    assert {"partition_date", "visible_at", "observed_at"}.isdisjoint(raw_columns)
    assert before == [(date(2024, 4, 18), 1.0)]
    assert after == [(date(2025, 4, 29), 2.0)]


def test_dividend_as_of_keeps_lifecycle_rows_until_their_announcement(
    tmp_path: Path,
) -> None:
    tushare_root = tmp_path / "tushare"
    common = {
        "ts_code": "000001.SZ",
        "end_date": date(2023, 12, 31),
        "ann_date": date(2024, 3, 1),
    }
    with TushareDataStore(tushare_root) as store:
        store.write(
            "dividend",
            _table(
                "dividend",
                {**common, "div_proc": "预案"},
                {
                    **common,
                    "div_proc": "实施",
                    "imp_ann_date": date(2024, 4, 30),
                    "record_date": date(2024, 5, 8),
                    "ex_date": date(2024, 5, 9),
                },
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        before = catalog.connection.execute(
            "SELECT div_proc FROM tushare.dividend_as_of(DATE '2024-04-29')"
        ).fetchall()
        after = catalog.connection.execute(
            "SELECT div_proc FROM tushare.dividend_as_of(DATE '2024-04-30') ORDER BY div_proc"
        ).fetchall()

    assert before == [("预案",)]
    assert after == [("实施",), ("预案",)]


def test_sw_industry_as_of_returns_active_membership_without_future_state(
    tmp_path: Path,
) -> None:
    tushare_root = tmp_path / "tushare"
    common = {
        "l1_code": "801000.SI",
        "l1_name": "一级",
        "l2_code": "801001.SI",
        "l2_name": "二级",
        "ts_code": "000001.SZ",
        "name": "测试股票",
    }
    with TushareDataStore(tushare_root) as store:
        store.write(
            "sw_industry",
            _table(
                "sw_industry",
                {
                    **common,
                    "l3_code": "850001.SI",
                    "l3_name": "旧行业",
                    "in_date": date(2020, 1, 1),
                    "out_date": date(2024, 1, 1),
                    "is_new": "N",
                },
                {
                    **common,
                    "l3_code": "850002.SI",
                    "l3_name": "新行业",
                    "in_date": date(2024, 1, 1),
                    "out_date": None,
                    "is_new": "Y",
                },
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        old = catalog.connection.execute(
            "SELECT l3_code, out_date, is_new FROM tushare.sw_industry_as_of(DATE '2023-12-31')"
        ).fetchall()
        new = catalog.connection.execute(
            "SELECT l3_code, out_date, is_new FROM tushare.sw_industry_as_of(DATE '2024-01-01')"
        ).fetchall()

    assert old == [("850001.SI", None, None)]
    assert new == [("850002.SI", None, None)]


def test_qmt_as_of_uses_receiver_timestamp(tmp_path: Path) -> None:
    qmt_root = tmp_path / "qmt"
    first = int(datetime(2024, 1, 2, tzinfo=UTC).timestamp() * 1_000_000)
    with QmtDataStore(qmt_root) as store:
        store.append_quotes(
            [
                SequencedQuote(
                    seq=1,
                    code="000001.SZ",
                    period="tick",
                    source="market",
                    subscription="SZ",
                    received_at=first,
                    quote=TickQuote(lastPrice=10.0),
                ),
                SequencedQuote(
                    seq=2,
                    code="000001.SZ",
                    period="tick",
                    source="market",
                    subscription="SZ",
                    received_at=first + 10,
                    quote=TickQuote(lastPrice=11.0),
                ),
            ]
        )

    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=qmt_root) as catalog:
        rows = catalog.connection.execute(
            f"SELECT seq FROM qmt.ticks_as_of({first + 5}) ORDER BY seq"
        ).fetchall()

    assert rows == [(1,)]


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
