from __future__ import annotations

import json
from csv import reader as csv_reader
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from data.test_main import run

QUERY_NAMES = {
    "market.bars.daily",
    "market.bars.intraday",
    "market.current",
    "market.status",
    "market.daily_metrics",
    "market.moneyflow",
    "fundamentals.statements.income",
    "fundamentals.statements.balance_sheet",
    "fundamentals.statements.cash_flow",
    "fundamentals.indicators",
    "fundamentals.disclosures.forecast",
    "fundamentals.disclosures.express",
    "fundamentals.disclosures.audit",
    "corporate_actions.dividends",
    "corporate_actions.adjustment_factors",
    "classification.industry",
    "reference.stocks",
    "calendar.sessions",
}


def test_run_calls_every_reader_method_and_writes_test_outputs(tmp_path: Path) -> None:
    as_of = datetime(2024, 4, 30, 16, 5, tzinfo=ZoneInfo("Asia/Shanghai"))
    output_dir = tmp_path / "test-output"
    result = run(
        tmp_path / "tushare",
        tmp_path / "qmt",
        as_of,
        "000001.SZ",
        10,
        output_dir=output_dir,
    )

    assert set(result.queries) == QUERY_NAMES
    assert set(result.files) == {*QUERY_NAMES, "calendar.previous_session"}
    assert result.errors == {}
    assert result.previous_session is None
    assert result.manifest_path == output_dir / "manifest.json"
    assert all(query.as_of == as_of for query in result.queries.values())
    assert result.queries["market.bars.daily"].sources == ("tushare",)
    assert result.queries["market.bars.intraday"].sources == ("qmt",)
    assert result.queries["market.current"].sources == ("qmt",)
    assert result.queries["market.current"].table.num_rows == 0
    assert result.queries["market.status"].table.num_rows == 1

    for name, query in result.queries.items():
        with result.files[name].open(encoding="utf-8", newline="") as file:
            rows = list(csv_reader(file))
        assert rows[0] == query.table.schema.names
        assert len(rows) - 1 == query.table.num_rows

    with result.files["calendar.previous_session"].open(encoding="utf-8", newline="") as file:
        assert list(csv_reader(file)) == [["exchange", "previous_session"], ["SSE", ""]]

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["version"] == 1
    assert manifest["as_of"] == as_of.isoformat()
    assert manifest["parameters"]["symbol"] == "000001.SZ"
    assert set(manifest["datasets"]) == set(result.files)
    assert manifest["errors"] == {}
    assert len(tuple(output_dir.glob("*.csv"))) == len(result.files)
