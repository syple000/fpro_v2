from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from data import (
    DataCapability,
    DataCatalog,
    DataReader,
    DataSourceNotConfiguredError,
    DataView,
    SourceConfig,
)
from models import CAPABILITY_SCHEMAS, CASH_FLOW_STATEMENT_SCHEMA
from qmt_protocol import BarQuote, HistoryBar, SequencedQuote, TickQuote
from qmt_receiver import QmtDataStore
from tushare_data import TABLE_SCHEMAS, TushareDataStore

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _table(dataset: str, *rows: dict[str, object]) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=TABLE_SCHEMAS[dataset])


def _as_of(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 1, day, hour, minute, tzinfo=SHANGHAI)


def _us(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000)


def test_every_capability_has_a_platform_schema() -> None:
    assert set(CAPABILITY_SCHEMAS) == set(DataCapability)
    assert all(isinstance(schema, pa.Schema) for schema in CAPABILITY_SCHEMAS.values())


def test_daily_bars_obey_pit_projection_and_limit(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    with TushareDataStore(tushare_root) as store:
        store.write(
            "daily",
            _table(
                "daily",
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 2),
                    "open": 10.0,
                    "high": 12.0,
                    "low": 9.0,
                    "close": 11.0,
                    "pre_close": 9.5,
                    "vol": 100.0,
                    "amount": 200.0,
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 3),
                    "open": 11.0,
                    "close": 12.0,
                },
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"market.daily_bars": "tushare"}),
        )
        before = reader.at(_as_of(3, 16, 4)).market.bars(
            symbols=("000001.SZ",), frequency="1d", count=2
        )
        after = reader.at(_as_of(3, 16, 6)).market.bars(
            symbols=("000001.SZ",),
            frequency="1d",
            count=2,
            fields=("close", "volume", "amount"),
            order="desc",
            limit=1,
        )

    assert [row["close"] for row in before.table.to_pylist()] == [11.0]
    assert after.table.schema.names == [
        "symbol",
        "interval_start",
        "interval_end",
        "close",
        "volume",
        "amount",
    ]
    assert after.table.to_pylist()[0]["close"] == 12.0
    assert after.truncated is True
    assert before.table.to_pylist()[0]["volume"] == 10_000.0
    assert before.table.to_pylist()[0]["amount"] == 200_000.0


def test_daily_metrics_normalize_percentages_shares_and_currency(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    with TushareDataStore(tushare_root) as store:
        store.write(
            "daily_basic",
            _table(
                "daily_basic",
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 2),
                    "close": 10.0,
                    "turnover_rate": 2.5,
                    "turnover_rate_f": 5.0,
                    "dv_ratio": 3.0,
                    "dv_ttm": 4.0,
                    "total_share": 1.0,
                    "total_mv": 2.0,
                },
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"market.daily_metrics": "tushare"}),
        )
        result = reader.at(_as_of(2, 18)).market.daily_metrics(
            symbols=("000001.SZ",),
            start=date(2024, 1, 2),
            end=date(2024, 1, 3),
            fields=(
                "turnover_rate",
                "turnover_rate_f",
                "dv_ratio",
                "dv_ttm",
                "total_share",
                "total_mv",
            ),
        )

    row = result.table.to_pylist()[0]
    assert row["turnover_rate"] == 0.025
    assert row["turnover_rate_f"] == 0.05
    assert row["dv_ratio"] == 0.03
    assert row["dv_ttm"] == 0.04
    assert row["total_share"] == 10_000.0
    assert row["total_mv"] == 20_000.0
    assert result.to_pandas().attrs == {}


def test_reader_uses_refreshed_catalog_without_reopening_snapshots(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    common = {
        "ts_code": "000001.SZ",
        "ann_date": date(2024, 1, 2),
        "end_date": date(2023, 12, 31),
        "report_type": "1",
        "comp_type": "1",
    }
    with TushareDataStore(tushare_root) as store:
        store.write(
            "cashflow",
            _table(
                "cashflow",
                {
                    **common,
                    "f_ann_date": date(2024, 1, 2),
                    "free_cashflow": 1.0,
                    "update_flag": "0",
                },
            ),
        )
        store.write(
            "trade_cal",
            _table(
                "trade_cal",
                {
                    "exchange": "SSE",
                    "cal_date": date(2024, 1, 3),
                    "is_open": 1,
                },
                {
                    "exchange": "SSE",
                    "cal_date": date(2024, 1, 4),
                    "is_open": 1,
                    "pretrade_date": date(2024, 1, 3),
                },
            ),
        )

    config = SourceConfig(routes={"fundamentals.cashflow": "tushare"})
    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(catalog, sources=config)
        data = reader.at(_as_of(3, 10))
        assert _cashflow_value(data) == 1.0
        platform = data.fundamentals.statements(
            kind="cash_flow",
            symbols=("000001.SZ",),
            periods=1,
            company_type="industrial",
        )
        assert platform.table.schema.equals(CASH_FLOW_STATEMENT_SCHEMA)
        assert "free_cash_flow" in platform.table.schema.names
        assert "free_cashflow" not in platform.table.schema.names
        assert platform.table.to_pylist()[0]["company_type"] == "industrial"

        with TushareDataStore(tushare_root) as store:
            store.write(
                "cashflow",
                _table(
                    "cashflow",
                    {
                        **common,
                        "f_ann_date": date(2024, 1, 2),
                        "free_cashflow": 2.0,
                        "update_flag": "1",
                    },
                ),
            )
        catalog.refresh()

        assert _cashflow_value(data) == 2.0


def test_qmt_current_and_completed_intraday_bar_use_received_boundary(tmp_path: Path) -> None:
    qmt_root = tmp_path / "qmt"
    interval_start = _as_of(2, 9, 30)
    received_at = _as_of(2, 9, 31)
    bar_received_at = datetime(2024, 1, 2, 9, 31, 30, tzinfo=SHANGHAI)
    with QmtDataStore(qmt_root) as store:
        store.write_daily(
            {
                "000001.SZ": [
                    HistoryBar(
                        index=20240102,
                        open=10.0,
                        high=10.3,
                        low=9.9,
                        close=10.2,
                        volume=100,
                        amount=1_000.0,
                    )
                ]
            },
            "none",
        )
        store.append_quotes(
            [
                SequencedQuote(
                    seq=1,
                    code="000001.SZ",
                    period="tick",
                    source="market",
                    subscription="SZ",
                    received_at=_us(received_at),
                    quote=TickQuote(
                        time=_us(received_at),
                        lastPrice=10.2,
                        open=10.0,
                        volume=100,
                        pvolume=10_000,
                        amount=1_000.0,
                    ),
                ),
                SequencedQuote(
                    seq=2,
                    code="000001.SZ",
                    period="1m",
                    source="market",
                    subscription="SZ",
                    received_at=_us(bar_received_at),
                    quote=BarQuote(
                        time=_us(interval_start),
                        open=10.0,
                        high=10.3,
                        low=9.9,
                        close=10.2,
                        volume=100,
                        amount=1_000.0,
                    ),
                ),
            ]
        )

    config = SourceConfig(
        routes={
            "market.daily_bars": "qmt",
            "market.realtime_quotes": "qmt",
            "market.intraday_bars": "qmt",
        }
    )
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=qmt_root) as catalog:
        reader = DataReader(catalog, sources=config)
        before = reader.at(datetime(2024, 1, 2, 9, 30, 30, tzinfo=SHANGHAI)).market.bars(
            symbols=("000001.SZ",),
            frequency="1m",
            start=interval_start,
        )
        tick_current = reader.at(datetime(2024, 1, 2, 9, 31, 10, tzinfo=SHANGHAI)).market.current(
            symbols=("000001.SZ",), fields=("last", "volume")
        )
        after_data = reader.at(_as_of(2, 9, 32))
        after = after_data.market.bars(
            symbols=("000001.SZ",),
            frequency="1m",
            start=interval_start,
        )
        current = after_data.market.current(symbols=("000001.SZ",), fields=("last", "volume"))
        daily = reader.at(_as_of(2, 17)).market.bars(
            symbols=("000001.SZ",),
            frequency="1d",
            count=1,
            fields=("volume", "amount"),
        )

    assert before.table.num_rows == 0
    assert after.table.to_pylist()[0]["interval_end"] == _as_of(2, 9, 31)
    assert after.table.to_pylist()[0]["volume"] == 10_000.0
    assert tick_current.table.to_pylist() == [
        {"symbol": "000001.SZ", "last": 10.2, "volume": 10_000.0}
    ]
    assert current.table.to_pylist() == [{"symbol": "000001.SZ", "last": 10.2, "volume": 10_000.0}]
    assert daily.table.to_pylist()[0]["volume"] == 10_000.0
    assert daily.table.to_pylist()[0]["amount"] == 1_000.0


def test_qmt_intraday_bars_can_be_forward_adjusted(tmp_path: Path) -> None:
    qmt_root = tmp_path / "qmt"
    tushare_root = tmp_path / "tushare"
    interval_start = _as_of(2, 9, 30)
    with QmtDataStore(qmt_root) as store:
        store.append_quotes(
            [
                SequencedQuote(
                    seq=1,
                    code="000001.SZ",
                    period="1m",
                    source="market",
                    subscription="SZ",
                    received_at=_us(_as_of(2, 9, 31)),
                    quote=BarQuote(
                        time=_us(interval_start),
                        open=10.0,
                        high=12.0,
                        low=9.0,
                        close=11.0,
                        preClose=9.5,
                        volume=100,
                        amount=1_000.0,
                    ),
                )
            ]
        )
    with TushareDataStore(tushare_root) as store:
        store.write(
            "adj_factor",
            _table(
                "adj_factor",
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 2),
                    "adj_factor": 1.0,
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 3),
                    "adj_factor": 2.0,
                },
            ),
        )

    config = SourceConfig(
        routes={
            "market.intraday_bars": "qmt",
            "corporate_actions.adjustment_factors": "tushare",
        }
    )
    with DataCatalog(tushare_root=tushare_root, qmt_root=qmt_root) as catalog:
        result = DataReader(catalog, sources=config).at(_as_of(3, 10)).market.bars(
            symbols=("000001.SZ",),
            frequency="1m",
            start=interval_start,
            end=_as_of(2, 9, 31),
            adjustment="forward",
        )

    row = result.table.to_pylist()[0]
    assert row["open"] == 5.0
    assert row["high"] == 6.0
    assert row["low"] == 4.5
    assert row["close"] == 5.5
    assert row["pre_close"] == 4.75
    assert row["volume"] == 10_000.0
    assert row["amount"] == 1_000.0
    assert result.sources == ("qmt", "tushare")


def test_statement_without_actual_announcement_date_stays_invisible(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    with TushareDataStore(tushare_root) as store:
        store.write(
            "cashflow",
            _table(
                "cashflow",
                {
                    "ts_code": "000001.SZ",
                    "ann_date": date(2024, 1, 2),
                    "f_ann_date": None,
                    "end_date": date(2023, 12, 31),
                    "report_type": "1",
                    "comp_type": "1",
                    "update_flag": "1",
                    "free_cashflow": 1.0,
                },
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"fundamentals.cashflow": "tushare"}),
        )
        result = reader.at(_as_of(4, 10)).fundamentals.statements(
            kind="cash_flow",
            symbols=("000001.SZ",),
            periods=1,
            fields=("free_cash_flow",),
        )

    assert result.table.num_rows == 0


def test_financial_units_are_normalized_at_adapter_boundary(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    common = {
        "ts_code": "000001.SZ",
        "ann_date": date(2024, 1, 2),
        "end_date": date(2023, 12, 31),
    }
    with TushareDataStore(tushare_root) as store:
        store.write(
            "fina_indicator",
            _table(
                "fina_indicator",
                {
                    **common,
                    "eps": 1.2,
                    "roe": 12.5,
                    "grossprofit_margin": 30.0,
                    "update_flag": "1",
                },
            ),
        )
        store.write(
            "forecast",
            _table(
                "forecast",
                {
                    **common,
                    "type": "预增",
                    "p_change_min": 10.0,
                    "net_profit_min": 2.0,
                },
            ),
        )

    config = SourceConfig(
        routes={
            "fundamentals.indicators": "tushare",
            "fundamentals.forecast": "tushare",
        }
    )
    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(catalog, sources=config)
        data = reader.at(_as_of(3, 10))
        indicator = data.fundamentals.indicators(
            symbols=("000001.SZ",),
            periods=1,
            fields=(
                "basic_earnings_per_share",
                "return_on_equity",
                "gross_margin",
            ),
        )
        forecast = data.fundamentals.disclosures(
            kind="forecast",
            symbols=("000001.SZ",),
            fields=("net_income_change_lower_bound", "net_income_lower_bound"),
        )

    assert indicator.table.to_pylist()[0]["basic_earnings_per_share"] == 1.2
    assert indicator.table.to_pylist()[0]["return_on_equity"] == 0.125
    assert indicator.table.to_pylist()[0]["gross_margin"] == 0.3
    assert forecast.table.to_pylist()[0]["net_income_change_lower_bound"] == 0.1
    assert forecast.table.to_pylist()[0]["net_income_lower_bound"] == 20_000.0


def test_dividend_uses_reader_visibility_policy(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    common = {
        "ts_code": "000001.SZ",
        "end_date": date(2023, 12, 31),
        "ann_date": date(2024, 1, 1),
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
                    "imp_ann_date": date(2024, 1, 2),
                    "ex_date": date(2024, 1, 8),
                },
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"corporate_actions.dividends": "tushare"}),
        )
        before = reader.at(_as_of(3, 9, 24)).corporate_actions.dividends(symbols=("000001.SZ",))
        after = reader.at(_as_of(3, 9, 25)).corporate_actions.dividends(symbols=("000001.SZ",))

    assert before.table.num_rows == 0
    assert after.table.to_pylist()[0]["div_proc"] == "实施"


def test_industry_reader_does_not_expose_future_membership_state(tmp_path: Path) -> None:
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
                    "out_date": date(2024, 1, 3),
                    "is_new": "N",
                },
                {
                    **common,
                    "l3_code": "850002.SI",
                    "l3_name": "新行业",
                    "in_date": date(2024, 1, 3),
                    "is_new": "Y",
                },
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"classification.industry": "tushare"}),
        )
        old = reader.at(_as_of(2, 10)).classification.industry(symbols=("000001.SZ",), level=3)
        new = reader.at(_as_of(3, 10)).classification.industry(symbols=("000001.SZ",), level=3)

    assert old.table.to_pylist()[0]["industry_code"] == "850001.SI"
    assert new.table.to_pylist()[0]["industry_code"] == "850002.SI"
    assert "out_date" not in old.table.schema.names
    assert "is_new" not in old.table.schema.names


def test_pit_view_time_is_aware_normalized_and_read_only(tmp_path: Path) -> None:
    with DataCatalog(
            tushare_root=tmp_path / "tushare",
            qmt_root=tmp_path / "qmt",
        ) as catalog:
        reader = DataReader(catalog, sources=SourceConfig(routes={}))
        with pytest.raises(ValueError, match="时区"):
            reader.at(datetime(2024, 1, 1))
        data = reader.at(datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC")))
        assert data.as_of == _as_of(1, 8)
        with pytest.raises(AttributeError):
            data.as_of = _as_of(2, 8)  # type: ignore[misc]
        with pytest.raises(DataSourceNotConfiguredError):
            data.market.current(symbols=("000001.SZ",))


def _cashflow_value(data: DataView) -> float:
    result = data.fundamentals.statements(
        kind="cash_flow",
        symbols=("000001.SZ",),
        periods=1,
        fields=("free_cash_flow",),
    )
    return result.table.to_pylist()[0]["free_cash_flow"]
