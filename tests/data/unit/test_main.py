from __future__ import annotations

from datetime import date

from data.test_main import run


def test_run_queries_empty_tushare_and_qmt_catalogs(tmp_path) -> None:
    cashflow, ticks = run(
        tmp_path / "tushare",
        tmp_path / "qmt",
        date(2024, 4, 30),
        1_714_464_000_000_000,
        "000001.SZ",
        10,
    )

    assert cashflow.empty
    assert ticks.empty
