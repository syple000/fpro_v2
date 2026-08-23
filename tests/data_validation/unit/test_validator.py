from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa

from data import DataCatalog
from data_validation import (
    compare_daily,
    compare_dividends,
    compare_financial,
    sample_stocks,
)
from qmt_protocol import HistoryFrame
from qmt_receiver import QmtDataStore
from tushare_data import TABLE_SCHEMAS, TushareDataStore


def _table(dataset: str, *rows: dict[str, object]) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=TABLE_SCHEMAS[dataset])


def test_sampled_qmt_data_matches_tushare(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    qmt_root = tmp_path / "qmt"
    day = date(2024, 1, 2)
    report_day = date(2023, 12, 31)
    ex_day = date(2024, 6, 1)
    with TushareDataStore(tushare_root) as store:
        store.write(
            "daily",
            _table(
                "daily",
                {
                    "ts_code": "000001.SZ",
                    "trade_date": day,
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.0,
                    "close": 10.5,
                    "vol": 10.0,
                    "amount": 100.0,
                },
            ),
        )
        store.write(
            "adj_factor",
            _table(
                "adj_factor",
                {"ts_code": "000001.SZ", "trade_date": day, "adj_factor": 2.0},
            ),
        )
        store.write(
            "income",
            _table(
                "income",
                {
                    "ts_code": "000001.SZ",
                    "ann_date": date(2024, 4, 30),
                    "f_ann_date": date(2024, 4, 30),
                    "end_date": report_day,
                    "report_type": "1",
                    "comp_type": "1",
                    "revenue": 100.0,
                    "total_revenue": 120.0,
                    "update_flag": "1",
                },
            ),
        )
        store.write(
            "dividend",
            _table(
                "dividend",
                {
                    "ts_code": "000001.SZ",
                    "end_date": report_day,
                    "ann_date": date(2024, 3, 1),
                    "imp_ann_date": date(2024, 5, 20),
                    "div_proc": "实施",
                    "ex_date": ex_day,
                    "cash_div_tax": 0.1,
                    "stk_bo_rate": 0.2,
                    "stk_co_rate": 0.3,
                    "stk_div": 0.5,
                },
            ),
        )

    daily = HistoryFrame(
        index=[20240102],
        columns=["open", "high", "low", "close", "volume", "amount"],
        data=[[10.0, 11.0, 9.0, 10.5, 1000, 100000.0]],
    )
    with QmtDataStore(qmt_root) as store:
        store.write_daily({"000001.SZ": daily}, "none")
        store.write_daily({"000001.SZ": daily}, "front")
        store.write_financial(
            {
                "000001.SZ": {
                    "Income": HistoryFrame(
                        index=[20231231],
                        columns=["m_anntime", "m_timetag", "revenue_inc", "revenue"],
                        data=[[20240430, 20231231, 100.0, 120.0]],
                    )
                }
            }
        )
        store.write_dividend_factors(
            {
                "000001.SZ": HistoryFrame(
                    index=[20240601],
                    columns=["interest", "stockBonus", "stockGift"],
                    data=[[0.1, 0.2, 0.3]],
                )
            }
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=qmt_root) as catalog:
        stocks = sample_stocks(
            catalog.connection,
            start_date=day,
            end_date=day,
            sample_size=10,
            seed=7,
        )
        daily_result = compare_daily(catalog.connection, stocks, day, day)
        financial_result = compare_financial(
            catalog.connection,
            stocks,
            report_day,
            report_day,
        )
        dividend_result = compare_dividends(
            catalog.connection,
            stocks,
            ex_day,
            ex_day,
        )

    assert stocks == ["000001.SZ"]
    assert daily_result.passed
    assert daily_result.compared == 10
    assert financial_result.passed
    assert financial_result.compared == 2
    assert dividend_result.passed
    assert dividend_result.compared == 4
