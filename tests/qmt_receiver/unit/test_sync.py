from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path

from market_data import DataCatalog
from qmt_protocol import (
    BalanceRecord,
    DividendFactor,
    DividendFactorsResponse,
    DividendType,
    FinancialData,
    FinancialDownloadResponse,
    FinancialQueryResponse,
    FinancialReportType,
    FinancialTable,
    HistoryBar,
    HistoryDownloadResponse,
    HistoryMode,
    HistoryQueryResponse,
    XtDataPeriod,
)
from qmt_receiver import QmtDataStore, sync_all, sync_daily, sync_intraday


class FakeSyncClient:
    def __init__(self) -> None:
        self.adjustments: list[DividendType] = []
        self.history_modes: list[HistoryMode] = []
        self.history_periods: list[XtDataPeriod] = []

    def download_history(
        self,
        stocks: Sequence[str],
        period: XtDataPeriod = "1d",
        start_time: str = "",
        end_time: str = "",
        mode: HistoryMode = "incremental",
    ) -> HistoryDownloadResponse:
        self.history_modes.append(mode)
        self.history_periods.append(period)
        return HistoryDownloadResponse(completed=True)

    def query_history(
        self,
        stocks: Sequence[str],
        fields: Sequence[str] = (),
        period: XtDataPeriod = "1d",
        start_time: str = "",
        end_time: str = "",
        count: int = -1,
        dividend_type: DividendType = "none",
        fill_data: bool = True,
    ) -> HistoryQueryResponse:
        self.adjustments.append(dividend_type)
        close = 10.0 if dividend_type == "none" else 8.0
        index = 20240102 if period == "1d" else 20240102093000
        return HistoryQueryResponse(
            period=period,
            data={
                stocks[0]: [
                    HistoryBar(
                        index=index,
                        time=1_704_153_600_000,
                        open=close,
                        high=close,
                        low=close,
                        close=close,
                        volume=1000,
                        amount=10000.0,
                    )
                ]
            },
        )

    def download_financial(
        self,
        stocks: Sequence[str],
        tables: Sequence[FinancialTable] = (),
        start_time: str = "",
        end_time: str = "",
    ) -> FinancialDownloadResponse:
        return FinancialDownloadResponse(completed=True)

    def query_financial(
        self,
        stocks: Sequence[str],
        tables: Sequence[FinancialTable] = (),
        start_time: str = "",
        end_time: str = "",
        report_type: FinancialReportType = "report_time",
    ) -> FinancialQueryResponse:
        return FinancialQueryResponse(
            data={
                stocks[0]: FinancialData(
                    Balance=[
                        BalanceRecord(
                            index=0,
                            m_anntime="20240430",
                            m_timetag="20231231",
                            tot_assets=100.0,
                        )
                    ]
                )
            },
        )

    def query_dividend_factors(
        self,
        stocks: Sequence[str],
        start_time: str = "",
        end_time: str = "",
    ) -> DividendFactorsResponse:
        return DividendFactorsResponse(
            data={
                stocks[0]: [
                    DividendFactor(
                        date="20240601",
                        time=1_717_200_000_000.0,
                        interest=0.1,
                        stockBonus=0.2,
                        stockGift=0.3,
                        dr=0.9,
                    )
                ]
            }
        )


def test_sync_all_downloads_and_writes_all_sources(tmp_path: Path) -> None:
    client = FakeSyncClient()
    qmt_root = tmp_path / "qmt"
    with QmtDataStore(qmt_root) as store:
        result = sync_all(client, store, ["000001.SZ"], "20240101", "20241231")

    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=qmt_root) as catalog:
        daily = catalog.connection.execute(
            "SELECT adjustment, close FROM qmt.daily ORDER BY adjustment"
        ).fetchall()
        intraday = catalog.connection.execute(
            "SELECT period, adjustment, close FROM qmt.intraday ORDER BY adjustment"
        ).fetchall()
        financial = catalog.connection.execute(
            "SELECT code, dataset, report_date, disclosure_date, data_json FROM qmt.financial"
        ).fetchone()
        dividend = catalog.connection.execute(
            "SELECT code, ex_date, interest, stockBonus, stockGift, dr FROM qmt.dividend_factors"
        ).fetchone()

    assert result.daily_rows == 1
    assert result.intraday_rows == 1
    assert result.financial_rows == 1
    assert result.dividend_factor_rows == 1
    assert client.history_modes == ["incremental", "incremental"]
    assert client.history_periods == ["1d", "1m"]
    assert client.adjustments == ["none", "none"]
    assert daily == [("none", 10.0)]
    assert intraday == [("1m", "none", 10.0)]
    assert financial is not None
    assert dividend is not None
    assert financial[:2] == ("000001.SZ", "Balance")
    assert financial[2].isoformat() == "2023-12-31"
    assert financial[3].isoformat() == "2024-04-30"
    assert '"tot_assets":100.0' in financial[4]
    assert dividend[0] == "000001.SZ"
    assert dividend[1].isoformat() == "2024-06-01"
    assert dividend[2:] == (0.1, 0.2, 0.3, 0.9)


def test_sync_all_force_uses_full_history_download(tmp_path: Path) -> None:
    client = FakeSyncClient()
    with QmtDataStore(tmp_path / "qmt") as store:
        sync_all(
            client,
            store,
            ["000001.SZ"],
            "20240101",
            "20241231",
            force=True,
        )

    assert client.history_modes == ["full", "full"]
    assert client.history_periods == ["1d", "1m"]


def test_sync_intraday_persists_only_raw_bars(tmp_path: Path) -> None:
    client = FakeSyncClient()
    qmt_root = tmp_path / "qmt"
    with QmtDataStore(qmt_root) as store:
        rows = sync_intraday(
            client,
            store,
            ["000001.SZ"],
            "20240101",
            "20240131",
            period="1m",
        )

    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=qmt_root) as catalog:
        bars = catalog.connection.execute(
            "SELECT period, adjustment, close FROM qmt.intraday ORDER BY adjustment"
        ).fetchall()

    assert rows == 1
    assert client.history_modes == ["incremental"]
    assert client.history_periods == ["1m"]
    assert client.adjustments == ["none"]
    assert bars == [("1m", "none", 10.0)]


def test_sync_daily_skips_completed_ranges_and_force_refetches(tmp_path: Path) -> None:
    client = FakeSyncClient()
    with QmtDataStore(tmp_path / "qmt") as store:
        first = sync_daily(
            client,
            store,
            ["000001.SZ"],
            "20240101",
            "20240131",
        )
        skipped = sync_daily(
            client,
            store,
            ["000001.SZ"],
            "20240101",
            "20240131",
        )
        forced = sync_daily(
            client,
            store,
            ["000001.SZ"],
            "20240101",
            "20240131",
            force=True,
        )
        completed = store.sync_completed_ranges("daily", "000001.SZ")

    assert (first, skipped, forced) == (1, 0, 1)
    assert client.history_modes == ["incremental", "full"]
    assert completed == [(date(2024, 1, 1), date(2024, 1, 31))]
