from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from data.test_main import run


def test_run_uses_reader_on_empty_tushare_and_qmt_catalogs(tmp_path) -> None:
    as_of = datetime(2024, 4, 30, 16, 5, tzinfo=ZoneInfo("Asia/Shanghai"))
    cashflow, current = run(
        tmp_path / "tushare",
        tmp_path / "qmt",
        as_of,
        "000001.SZ",
        10,
    )

    assert cashflow.table.num_rows == 0
    assert current.table.num_rows == 0
    assert cashflow.as_of == current.as_of == as_of
    assert cashflow.sources == ("tushare",)
    assert current.sources == ("qmt",)
    assert cashflow.to_pandas().empty
    assert current.to_pandas().empty
