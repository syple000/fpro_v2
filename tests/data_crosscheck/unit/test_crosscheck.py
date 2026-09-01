from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa

from data_crosscheck import (
    compare_daily,
    compare_dividends,
    compare_financial,
    compare_qmt_front_ratio,
    sample_stocks,
)
from market_data import DataCatalog
from qmt_protocol import (
    BalanceRecord,
    CashFlowRecord,
    DividendFactor,
    FinancialData,
    HistoryBar,
    IncomeRecord,
    PerShareIndexRecord,
)
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
                    "vol": 10.49,
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
                    "cash_div_tax": 0.06,
                    "stk_bo_rate": 0.1,
                    "stk_co_rate": 0.2,
                    "stk_div": 0.3,
                },
                {
                    "ts_code": "000001.SZ",
                    "end_date": date(2023, 6, 30),
                    "ann_date": date(2024, 3, 2),
                    "imp_ann_date": date(2024, 5, 21),
                    "div_proc": "实施",
                    "ex_date": ex_day,
                    "cash_div_tax": 0.04,
                    "stk_bo_rate": 0.1,
                    "stk_co_rate": 0.1,
                    "stk_div": 0.2,
                },
            ),
        )

    daily = [
        HistoryBar(
            index=20240102,
            open=10.0,
            high=11.0,
            low=9.0,
            close=10.5,
            volume=10,
            amount=100000.0,
        )
    ]
    with QmtDataStore(qmt_root) as store:
        store.write_daily({"000001.SZ": daily}, "none")
        store.write_daily({"000001.SZ": daily}, "front_ratio")
        store.write_financial(
            {
                "000001.SZ": FinancialData(
                    Income=[
                        IncomeRecord(
                            index=0,
                            m_anntime="20240430",
                            m_timetag="20231231",
                            revenue_inc=100.0,
                            revenue=120.0,
                        )
                    ]
                )
            }
        )
        store.write_dividend_factors(
            {
                "000001.SZ": [
                    DividendFactor(
                        date="20240601",
                        time=1_717_200_000_000.0,
                        interest=0.1,
                        stockBonus=0.2,
                        stockGift=0.3,
                    )
                ]
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


def test_daily_comparison_accepts_raw_only_qmt_sync(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    qmt_root = tmp_path / "qmt"
    day = date(2024, 1, 2)
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
    with QmtDataStore(qmt_root) as store:
        store.write_daily(
            {
                "000001.SZ": [
                    HistoryBar(
                        index=20240102,
                        open=10.0,
                        high=11.0,
                        low=9.0,
                        close=10.5,
                        volume=10,
                        amount=100_000.0,
                    )
                ]
            },
            "none",
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=qmt_root) as catalog:
        result = compare_daily(catalog.connection, ["000001.SZ"], day, day)

    assert result.passed
    assert result.compared == 6


def test_qmt_front_ratio_is_reproduced_from_raw_and_event_dr(tmp_path: Path) -> None:
    qmt_root = tmp_path / "qmt"
    day = date(2024, 1, 2)
    raw = HistoryBar(
        index=20240102,
        open=10.0,
        high=12.0,
        low=9.0,
        close=11.0,
        preClose=9.5,
        volume=100,
        amount=1_000.0,
    )
    with QmtDataStore(qmt_root) as store:
        store.write_daily({"000001.SZ": [raw]}, "none")

    native = HistoryBar(
        index=20240102,
        open=5.0,
        high=6.0,
        low=4.5,
        close=5.5,
        preClose=4.75,
        volume=100,
        amount=1_000.0,
    )
    factor = DividendFactor(
        date="20240103",
        time=1_704_211_200_000.0,
        dr=2.0,
    )
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=qmt_root) as catalog:
        result = compare_qmt_front_ratio(
            catalog.connection,
            ["000001.SZ"],
            day,
            day,
            {"000001.SZ": [native]},
            {"000001.SZ": [factor]},
        )

    assert result.passed
    assert result.compared == 7


def test_financial_comparison_normalises_vendor_aliases_and_precision(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    qmt_root = tmp_path / "qmt"
    report_day = date(2025, 9, 30)
    common = {
        "ts_code": "000001.SZ",
        "ann_date": date(2025, 10, 31),
        "end_date": report_day,
        "update_flag": "1",
    }
    with TushareDataStore(tushare_root) as store:
        store.write(
            "balancesheet",
            _table(
                "balancesheet",
                {
                    **common,
                    "f_ann_date": date(2025, 10, 31),
                    "report_type": "1",
                    "comp_type": "1",
                    "int_receiv": 0.0,
                    "contract_liab": 10.0,
                    "fix_assets_total": 20.0,
                    "cip_total": 30.0,
                    "oth_rcv_total": 40.0,
                    "oth_pay_total": 50.0,
                },
            ),
        )
        store.write(
            "cashflow",
            _table(
                "cashflow",
                {
                    **common,
                    "f_ann_date": date(2025, 10, 31),
                    "report_type": "1",
                    "comp_type": "1",
                    "c_paid_invest": 60.0,
                },
            ),
        )
        store.write(
            "fina_indicator",
            _table(
                "fina_indicator",
                {
                    **common,
                    "eps": 0.0844,
                    "dt_eps": 0.0844,
                    "bps": 0.842,
                },
            ),
        )

    with QmtDataStore(qmt_root) as store:
        store.write_financial(
            {
                "000001.SZ": FinancialData(
                    Balance=[
                        BalanceRecord(
                            index=0,
                            m_anntime="20251031",
                            m_timetag="20250930",
                            int_rcv=None,
                            advance_peceipts=10.0,
                            fix_assets=20.0,
                            constru_in_process=30.0,
                            other_receivable=40.0,
                            other_payable=50.0,
                        )
                    ],
                    CashFlow=[
                        CashFlowRecord(
                            index=0,
                            m_anntime="20251031",
                            m_timetag="20250930",
                            cash_paid_for_investments=60.0,
                            cash_paid_invest=None,
                            other_cash_pay_ral_inv_act=0.0,
                        )
                    ],
                    Pershareindex=[
                        PerShareIndexRecord(
                            index=0,
                            m_anntime="20251031",
                            m_timetag="20250930",
                            s_fa_eps_basic=0.08,
                            s_fa_eps_diluted=0.08,
                            s_fa_bps=0.842,
                        )
                    ],
                )
            }
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=qmt_root) as catalog:
        result = compare_financial(
            catalog.connection,
            ["000001.SZ"],
            report_day,
            report_day,
        )

    assert result.passed
