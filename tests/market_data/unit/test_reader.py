from __future__ import annotations

import inspect
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from market_data import (
    ALL_SYMBOLS,
    DataAdapter,
    DataCapability,
    DataCapabilityNotSupportedError,
    DataCatalog,
    DataReader,
    DataResultTooLargeError,
    DataSourceNotConfiguredError,
    DataSourceUnavailableError,
    DataView,
    SourceConfig,
)
from market_data.adapters import QmtAdapter, TushareAdapter
from market_data.reader import (
    _CAPABILITY_METHODS,
    CalendarReader,
    ClassificationReader,
    CorporateActionsReader,
    FundamentalsReader,
    MarketReader,
    ReferenceReader,
)
from models import CAPABILITY_SCHEMAS, CASH_FLOW_STATEMENT_SCHEMA, DAILY_METRICS_SCHEMA
from qmt_protocol import BarQuote, DividendFactor, HistoryBar, SequencedQuote, TickQuote
from qmt_receiver import QmtDataStore
from tushare_data import TABLE_SCHEMAS, TushareDataStore

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _table(dataset: str, *rows: dict[str, object]) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=TABLE_SCHEMAS[dataset])


def _as_of(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 1, day, hour, minute, tzinfo=SHANGHAI)


def _us(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000)


class _CustomDailyMetricsAdapter:
    capabilities = frozenset({DataCapability.DAILY_METRICS})

    def daily_metrics(
        self,
        *,
        as_of: datetime,
        symbols: tuple[str, ...] | None,
        start: date,
        end: date,
        order: str,
        fetch_limit: int | None,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        del as_of, symbols, start, end, order, fetch_limit
        table = pa.Table.from_pylist(
            [{"symbol": "000001.SZ", "trade_date": date(2024, 1, 2), "close": 10.0}],
            schema=DAILY_METRICS_SCHEMA,
        )
        return table if columns is None else table.select(columns)


class _IncompleteDailyMetricsAdapter:
    capabilities = frozenset({DataCapability.DAILY_METRICS})


def test_every_capability_has_a_platform_schema() -> None:
    assert set(CAPABILITY_SCHEMAS) == set(DataCapability)
    assert set(_CAPABILITY_METHODS) == set(DataCapability)
    assert all(isinstance(schema, pa.Schema) for schema in CAPABILITY_SCHEMAS.values())


def test_adapters_expose_explicit_capability_parameters() -> None:
    assert not hasattr(TushareAdapter, "read")
    assert not hasattr(QmtAdapter, "read")
    assert tuple(inspect.signature(TushareAdapter.daily_bars).parameters) == (
        "self",
        "as_of",
        "symbols",
        "start",
        "end",
        "count",
        "adjustment",
        "order",
        "fetch_limit",
        "columns",
    )
    assert tuple(inspect.signature(TushareAdapter.previous_session).parameters) == (
        "self",
        "end",
        "exchange",
    )
    assert tuple(inspect.signature(QmtAdapter.intraday_bars).parameters) == (
        "self",
        "as_of",
        "symbols",
        "frequency",
        "start",
        "end",
        "count",
        "adjustment",
        "order",
        "fetch_limit",
        "columns",
    )


@pytest.mark.parametrize(
    "query",
    (
        MarketReader.bars,
        MarketReader.current,
        MarketReader.status,
        MarketReader.daily_metrics,
        MarketReader.moneyflow,
        FundamentalsReader.statements,
        FundamentalsReader.indicators,
        FundamentalsReader.disclosures,
        CorporateActionsReader.dividends,
        CorporateActionsReader.adjustment_factors,
        ClassificationReader.industry,
        ReferenceReader.stocks,
        CalendarReader.sessions,
    ),
)
def test_public_queries_do_not_expose_generic_limit(query: Callable[..., object]) -> None:
    assert "limit" not in inspect.signature(query).parameters


@pytest.mark.parametrize("max_result_rows", (0, -1, True, 1.5))
def test_reader_rejects_invalid_internal_result_limit(
    tmp_path: Path, max_result_rows: object
) -> None:
    with (
        DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog,
        pytest.raises(ValueError, match="max_result_rows"),
    ):
        DataReader(
            catalog,
            sources=SourceConfig(routes={}),
            max_result_rows=max_result_rows,  # type: ignore[arg-type]
        )


def test_reader_routes_injected_adapter_by_declared_capability(tmp_path: Path) -> None:
    custom: DataAdapter = _CustomDailyMetricsAdapter()
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"market.daily_metrics": "custom"}),
            adapters={"custom": custom},
        )
        result = reader.at(_as_of(2, 18)).market.daily_metrics(
            symbols=("000001.SZ",),
            start=date(2024, 1, 1),
            fields=("close",),
        )

    assert result.sources == ("custom",)
    assert result.table.to_pylist() == [
        {"symbol": "000001.SZ", "trade_date": date(2024, 1, 2), "close": 10.0}
    ]


def test_reader_rejects_declared_capability_without_required_method(tmp_path: Path) -> None:
    incomplete: DataAdapter = _IncompleteDailyMetricsAdapter()
    with (
        DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=tmp_path / "qmt") as catalog,
        pytest.raises(DataCapabilityNotSupportedError, match="缺少适配器方法.*daily_metrics"),
    ):
        DataReader(
            catalog,
            sources=SourceConfig(routes={"market.daily_metrics": "custom"}),
            adapters={"custom": incomplete},
        )


def test_daily_bars_obey_pit_projection_and_support_arrow_batches(tmp_path: Path) -> None:
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
    assert [batch.num_rows for batch in after.iter_batches(batch_size=1)] == [1, 1]
    assert not hasattr(after, "truncated")
    assert before.table.to_pylist()[0]["volume"] == 10_000.0
    assert before.table.to_pylist()[0]["amount"] == 200_000.0


def test_daily_bar_count_and_range_return_exact_rows(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    days = tuple(date(2024, 1, day) for day in range(2, 8))
    with TushareDataStore(tushare_root) as store:
        for day in days:
            store.write(
                "daily",
                _table(
                    "daily",
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": day,
                        "close": float(day.day),
                    },
                ),
            )
        store.write(
            "daily",
            _table(
                "daily",
                {
                    "ts_code": "000002.SZ",
                    "trade_date": days[0],
                    "close": 20.0,
                },
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"market.daily_bars": "tushare"}),
        )
        latest = reader.at(_as_of(7, 17)).market.bars(
            symbols=("000001.SZ",),
            frequency="1d",
            count=2,
            fields=("close",),
        )
        ranged = reader.at(_as_of(7, 17)).market.bars(
            symbols=("000001.SZ",),
            frequency="1d",
            start=date(2024, 1, 3),
            end=date(2024, 1, 5),
            fields=("close",),
        )
        sparse = reader.at(_as_of(7, 17)).market.bars(
            symbols=("000001.SZ", "000002.SZ"),
            frequency="1d",
            count=1,
            fields=("close",),
        )

    assert [row["close"] for row in latest.table.to_pylist()] == [6.0, 7.0]
    assert [row["close"] for row in ranged.table.to_pylist()] == [3.0, 4.0]
    assert [row["symbol"] for row in sparse.table.to_pylist()] == [
        "000002.SZ",
        "000001.SZ",
    ]


def test_previous_session_returns_latest_open_day(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    days = tuple(date(2024, 1, day) for day in range(1, 11))
    with TushareDataStore(tushare_root) as store:
        store.write(
            "trade_cal",
            _table(
                "trade_cal",
                *(
                    {
                        "exchange": "SSE",
                        "cal_date": day,
                        "is_open": int(day == date(2024, 1, 9)),
                    }
                    for day in days
                ),
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"calendar.sessions": "tushare"}),
        )
        previous = reader.at(_as_of(11, 9)).calendar.previous_session(exchange="SSE")

    assert previous == date(2024, 1, 9)


def test_market_status_requires_explicit_symbols_and_enforces_internal_result_limit(
    tmp_path: Path,
) -> None:
    tushare_root = tmp_path / "tushare"
    with TushareDataStore(tushare_root) as store:
        store.write(
            "stk_limit",
            _table(
                "stk_limit",
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 2),
                    "up_limit": 11.0,
                },
                {
                    "ts_code": "000002.SZ",
                    "trade_date": date(2024, 1, 2),
                    "up_limit": 12.0,
                },
                {
                    "ts_code": "000003.SZ",
                    "trade_date": date(2024, 1, 2),
                    "up_limit": 13.0,
                },
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        config = SourceConfig(routes={"market.price_limits": "tushare"})
        result = (
            DataReader(catalog, sources=config)
            .at(_as_of(2, 10))
            .market.status(
                symbols=("000001.SZ", "000002.SZ", "000003.SZ"),
                fields=("up_limit",),
            )
        )
        with pytest.raises(ValueError, match="暂不支持 ALL_SYMBOLS"):
            DataReader(catalog, sources=config).at(_as_of(2, 10)).market.status(
                symbols=ALL_SYMBOLS,
                fields=("up_limit",),
            )
        guarded = DataReader(catalog, sources=config, max_result_rows=2)
        with pytest.raises(DataResultTooLargeError, match="超过内部上限 2 行"):
            guarded.at(_as_of(2, 10)).market.status(
                symbols=("000001.SZ", "000002.SZ", "000003.SZ"),
                fields=("up_limit",),
            )

    assert result.table.schema.names == ["symbol", "up_limit"]
    assert result.table.to_pylist() == [
        {"symbol": "000001.SZ", "up_limit": 11.0},
        {"symbol": "000002.SZ", "up_limit": 12.0},
        {"symbol": "000003.SZ", "up_limit": 13.0},
    ]


def test_intraday_suspension_becomes_false_after_interval_and_rejects_bad_timing(
    tmp_path: Path,
) -> None:
    tushare_root = tmp_path / "tushare"
    with TushareDataStore(tushare_root) as store:
        store.write(
            "suspend_d",
            _table(
                "suspend_d",
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 2),
                    "suspend_timing": "09:30-10:30",
                    "suspend_type": "S",
                },
                {
                    "ts_code": "000002.SZ",
                    "trade_date": date(2024, 1, 2),
                    "suspend_timing": "invalid",
                    "suspend_type": "S",
                },
            ),
        )

    config = SourceConfig(routes={"market.suspensions": "tushare"})
    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(catalog, sources=config)
        during = reader.at(_as_of(2, 10)).market.status(
            symbols=("000001.SZ",), fields=("suspended",)
        )
        after = reader.at(_as_of(2, 11)).market.status(
            symbols=("000001.SZ",), fields=("suspended",)
        )
        with pytest.raises(DataSourceUnavailableError, match="停牌时段格式无效"):
            reader.at(_as_of(2, 10)).market.status(symbols=("000002.SZ",), fields=("suspended",))

    assert during.table.to_pylist()[0]["suspended"] is True
    assert after.table.to_pylist()[0]["suspended"] is False


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
                        open=9.7,
                        high=9.9,
                        low=9.6,
                        close=9.8,
                        volume=1,
                        amount=980.0,
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
        next_day = reader.at(_as_of(3, 10)).market.current(symbols=("000001.SZ",), fields=("last",))
        daily = reader.at(_as_of(2, 17)).market.bars(
            symbols=("000001.SZ",),
            frequency="1d",
            count=1,
            fields=("volume", "amount"),
        )

    assert before.table.num_rows == 0
    assert after.table.to_pylist()[0]["interval_end"] == _as_of(2, 9, 31)
    assert after.table.to_pylist()[0]["volume"] == 100.0
    assert tick_current.table.to_pylist() == [
        {"symbol": "000001.SZ", "last": 10.2, "volume": 10_000.0}
    ]
    assert current.table.to_pylist() == [{"symbol": "000001.SZ", "last": 10.2, "volume": 10_000.0}]
    assert next_day.table.num_rows == 0
    assert daily.table.to_pylist()[0]["volume"] == 10_000.0
    assert daily.table.to_pylist()[0]["amount"] == 1_000.0


def test_tushare_adapter_calculates_forward_adjustment_internally(tmp_path: Path) -> None:
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
                    "amount": 1.0,
                },
            ),
        )
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

    config = SourceConfig(routes={"market.daily_bars": "tushare"})
    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        result = (
            DataReader(catalog, sources=config)
            .at(_as_of(3, 10))
            .market.bars(
                symbols=("000001.SZ",),
                frequency="1d",
                count=1,
                adjustment="forward",
            )
        )

    row = result.table.to_pylist()[0]
    assert row["open"] == 5.0
    assert row["high"] == 6.0
    assert row["low"] == 4.5
    assert row["close"] == 5.5
    assert row["pre_close"] == 4.75
    assert row["volume"] == 10_000.0
    assert row["amount"] == 1_000.0
    assert result.sources == ("tushare",)


def test_tushare_forward_adjustment_rejects_missing_factor_after_projection(
    tmp_path: Path,
) -> None:
    tushare_root = tmp_path / "tushare"
    with TushareDataStore(tushare_root) as store:
        store.write(
            "daily",
            _table(
                "daily",
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 2),
                    "close": 11.0,
                    "vol": 100.0,
                },
            ),
        )

    config = SourceConfig(routes={"market.daily_bars": "tushare"})
    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(catalog, sources=config)
        with pytest.raises(
            DataCapabilityNotSupportedError,
            match="不能为全部行情提供 PIT 前复权因子",
        ):
            reader.at(_as_of(2, 17)).market.bars(
                symbols=("000001.SZ",),
                frequency="1d",
                count=1,
                adjustment="forward",
                fields=("volume",),
            )


def test_qmt_adapter_calculates_forward_intraday_from_raw_bars_and_dr(
    tmp_path: Path,
) -> None:
    qmt_root = tmp_path / "qmt"
    raw = HistoryBar(
        index=20240102093000,
        open=10.0,
        high=12.0,
        low=9.0,
        close=11.0,
        preClose=9.5,
        volume=100,
        amount=1_000.0,
    )
    adjusted = HistoryBar(
        index=20240102093000,
        open=5.0,
        high=6.0,
        low=4.5,
        close=5.5,
        preClose=4.75,
        volume=100,
        amount=1_000.0,
    )
    with QmtDataStore(qmt_root) as store:
        store.write_intraday({"000001.SZ": [raw]}, "1m", "none")
        store.write_intraday({"000001.SZ": [adjusted]}, "1m", "front_ratio")
        store.write_dividend_factors(
            {
                "000001.SZ": [
                    DividendFactor(
                        date="20240103",
                        time=1_704_211_200_000.0,
                        dr=2.0,
                    )
                ]
            }
        )
        store.mark_sync_completed(
            "dividend_factors",
            "000001.SZ",
            date(2024, 1, 2),
            date(2024, 1, 4),
        )

    config = SourceConfig(routes={"market.intraday_bars": "qmt"})
    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=qmt_root) as catalog:
        result = (
            DataReader(catalog, sources=config)
            .at(_as_of(4, 10))
            .market.bars(
                symbols=("000001.SZ",),
                frequency="1m",
                start=_as_of(2, 9, 30),
                end=_as_of(2, 9, 31),
                adjustment="forward",
            )
        )

    assert result.table.to_pylist()[0] == {
        "symbol": "000001.SZ",
        "interval_start": _as_of(2, 9, 30),
        "interval_end": _as_of(2, 9, 31),
        "open": 5.0,
        "high": 6.0,
        "low": 4.5,
        "close": 5.5,
        "pre_close": 4.75,
        "volume": 10_000.0,
        "amount": 1_000.0,
    }


def test_qmt_daily_forward_uses_raw_and_factors_not_legacy_adjusted_partitions(
    tmp_path: Path,
) -> None:
    qmt_root = tmp_path / "qmt"
    with QmtDataStore(qmt_root) as store:
        store.write_daily(
            {"000001.SZ": [HistoryBar(index=20240102, close=10.0)]},
            "none",
        )
        store.write_daily(
            {
                "000001.SZ": [
                    HistoryBar(index=20240102, close=8.0),
                    HistoryBar(index=20240103, close=9.0),
                ]
            },
            "front",
        )
        store.write_daily(
            {"000001.SZ": [HistoryBar(index=20240103, close=7.0)]},
            "front_ratio",
        )
        store.write_dividend_factors(
            {
                "000001.SZ": [
                    DividendFactor(
                        date="20240103",
                        time=1_704_211_200_000.0,
                        dr=2.0,
                    )
                ]
            }
        )
        store.mark_sync_completed(
            "dividend_factors",
            "000001.SZ",
            date(2024, 1, 2),
            date(2024, 1, 3),
        )

    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=qmt_root) as catalog:
        result = (
            DataReader(
                catalog,
                sources=SourceConfig(routes={"market.daily_bars": "qmt"}),
            )
            .at(_as_of(3, 17))
            .market.bars(
                symbols=("000001.SZ",),
                frequency="1d",
                count=2,
                adjustment="forward",
                fields=("close",),
            )
        )

    assert result.table.to_pylist() == [
        {
            "symbol": "000001.SZ",
            "interval_start": _as_of(2, 9, 30),
            "interval_end": _as_of(2, 15),
            "close": 5.0,
        }
    ]


def test_qmt_forward_rejects_incomplete_factor_sync_range(tmp_path: Path) -> None:
    qmt_root = tmp_path / "qmt"
    with QmtDataStore(qmt_root) as store:
        store.write_daily(
            {"000001.SZ": [HistoryBar(index=20240102, close=10.0)]},
            "none",
        )

    with (
        DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=qmt_root) as catalog,
        pytest.raises(
            DataCapabilityNotSupportedError,
            match="复权因子同步区间不完整",
        ),
    ):
        (
            DataReader(
                catalog,
                sources=SourceConfig(routes={"market.daily_bars": "qmt"}),
            )
            .at(_as_of(3, 17))
            .market.bars(
                symbols=("000001.SZ",),
                frequency="1d",
                count=1,
                adjustment="forward",
            )
        )


def test_qmt_intraday_count_returns_latest_bars(tmp_path: Path) -> None:
    qmt_root = tmp_path / "qmt"
    with QmtDataStore(qmt_root) as store:
        store.write_intraday(
            {
                "000001.SZ": [
                    HistoryBar(index=20240102093000, close=1.0),
                    HistoryBar(index=20240102093100, close=2.0),
                    HistoryBar(index=20240103093000, close=3.0),
                    HistoryBar(index=20240103093100, close=4.0),
                    HistoryBar(index=20240103093200, close=5.0),
                ]
            },
            "1m",
            "none",
        )

    with DataCatalog(tushare_root=tmp_path / "tushare", qmt_root=qmt_root) as catalog:
        result = (
            DataReader(
                catalog,
                sources=SourceConfig(routes={"market.intraday_bars": "qmt"}),
            )
            .at(_as_of(3, 10))
            .market.bars(
                symbols=("000001.SZ",),
                frequency="1m",
                count=2,
                fields=("close",),
            )
        )

    assert [row["close"] for row in result.table.to_pylist()] == [4.0, 5.0]


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


def test_statement_uses_latest_visible_adjusted_consolidated_revision(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    common = {
        "ts_code": "000001.SZ",
        "end_date": date(2023, 12, 31),
        "comp_type": "1",
        "update_flag": "1",
    }
    with TushareDataStore(tushare_root) as store:
        store.write(
            "cashflow",
            _table(
                "cashflow",
                {
                    **common,
                    "ann_date": date(2024, 1, 2),
                    "f_ann_date": date(2024, 1, 2),
                    "report_type": "1",
                    "free_cashflow": 100.0,
                },
                {
                    **common,
                    "ann_date": date(2024, 1, 3),
                    "f_ann_date": date(2024, 1, 3),
                    "report_type": "4",
                    "free_cashflow": 120.0,
                },
                {
                    **common,
                    "ann_date": date(2024, 1, 3),
                    "f_ann_date": date(2024, 1, 3),
                    "report_type": "5",
                    "free_cashflow": 999.0,
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
                },
            ),
        )

    config = SourceConfig(routes={"fundamentals.cashflow": "tushare"})
    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(catalog, sources=config)
        before = reader.at(_as_of(3, 10)).fundamentals.statements(
            kind="cash_flow",
            symbols=("000001.SZ",),
            periods=1,
            fields=("free_cash_flow",),
        )
        after = reader.at(_as_of(4, 10)).fundamentals.statements(
            kind="cash_flow",
            symbols=("000001.SZ",),
            periods=1,
            fields=("free_cash_flow",),
        )

    assert before.table.to_pylist()[0]["free_cash_flow"] == 100.0
    assert after.table.to_pylist()[0]["free_cash_flow"] == 120.0


def test_statement_ranks_periods_before_filtering_company_type(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    with TushareDataStore(tushare_root) as store:
        store.write(
            "cashflow",
            _table(
                "cashflow",
                {
                    "ts_code": "000001.SZ",
                    "ann_date": date(2024, 1, 2),
                    "f_ann_date": date(2024, 1, 2),
                    "end_date": date(2023, 12, 31),
                    "report_type": "1",
                    "comp_type": "2",
                    "update_flag": "1",
                    "free_cashflow": 2.0,
                },
                {
                    "ts_code": "000001.SZ",
                    "ann_date": date(2024, 1, 2),
                    "f_ann_date": date(2024, 1, 2),
                    "end_date": date(2022, 12, 31),
                    "report_type": "1",
                    "comp_type": "1",
                    "update_flag": "1",
                    "free_cashflow": 1.0,
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
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        result = (
            DataReader(
                catalog,
                sources=SourceConfig(routes={"fundamentals.cashflow": "tushare"}),
            )
            .at(_as_of(3, 10))
            .fundamentals.statements(
                kind="cash_flow",
                symbols=("000001.SZ",),
                periods=1,
                company_type="industrial",
                fields=("free_cash_flow",),
            )
        )

    assert result.table.num_rows == 0


def test_balance_sheet_exposes_monetary_funds_not_cash_equivalents(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    with TushareDataStore(tushare_root) as store:
        store.write(
            "balancesheet",
            _table(
                "balancesheet",
                {
                    "ts_code": "000001.SZ",
                    "ann_date": date(2024, 1, 2),
                    "f_ann_date": date(2024, 1, 2),
                    "end_date": date(2023, 12, 31),
                    "report_type": "1",
                    "comp_type": "1",
                    "update_flag": "1",
                    "money_cap": 123.0,
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
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        result = (
            DataReader(
                catalog,
                sources=SourceConfig(routes={"fundamentals.balance_sheet": "tushare"}),
            )
            .at(_as_of(3, 10))
            .fundamentals.statements(
                kind="balance_sheet",
                symbols=("000001.SZ",),
                periods=1,
                fields=("monetary_funds",),
            )
        )

    assert "cash_and_cash_equivalents" not in result.table.schema.names
    assert result.table.to_pylist()[0]["monetary_funds"] == 123.0


def test_visibility_query_fails_when_trade_calendar_coverage_ends(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    with TushareDataStore(tushare_root) as store:
        store.write(
            "forecast",
            _table(
                "forecast",
                {
                    "ts_code": "000001.SZ",
                    "ann_date": date(2024, 9, 30),
                    "end_date": date(2024, 12, 31),
                    "type": "预增",
                },
            ),
        )
        store.write(
            "trade_cal",
            _table(
                "trade_cal",
                {
                    "exchange": "SSE",
                    "cal_date": date(2024, 9, 30),
                    "is_open": 1,
                },
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"fundamentals.forecast": "tushare"}),
        )
        with pytest.raises(DataSourceUnavailableError, match="交易日历未覆盖"):
            reader.at(datetime(2024, 10, 8, 10, tzinfo=SHANGHAI)).fundamentals.disclosures(
                kind="forecast",
                symbols=("000001.SZ",),
            )


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
        store.write(
            "trade_cal",
            _table(
                "trade_cal",
                {
                    "exchange": "SSE",
                    "cal_date": date(2024, 1, 3),
                    "is_open": 1,
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


def test_statement_periods_returns_latest_report_periods(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    periods = (
        (date(2022, 12, 31), date(2023, 1, 2), 2.0),
        (date(2023, 12, 31), date(2024, 1, 2), 3.0),
        (date(2024, 12, 31), date(2025, 1, 2), 4.0),
    )
    with TushareDataStore(tushare_root) as store:
        for period_end, announcement_date, value in periods:
            store.write(
                "cashflow",
                _table(
                    "cashflow",
                    {
                        "ts_code": "000001.SZ",
                        "ann_date": announcement_date,
                        "f_ann_date": announcement_date,
                        "end_date": period_end,
                        "report_type": "1",
                        "comp_type": "1",
                        "update_flag": "1",
                        "free_cashflow": value,
                    },
                ),
            )
        store.write(
            "trade_cal",
            _table(
                "trade_cal",
                *(
                    {
                        "exchange": "SSE",
                        "cal_date": day,
                        "is_open": 1,
                    }
                    for day in (
                        date(2023, 1, 3),
                        date(2024, 1, 3),
                        date(2025, 1, 3),
                    )
                ),
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        result = (
            DataReader(
                catalog,
                sources=SourceConfig(routes={"fundamentals.cashflow": "tushare"}),
            )
            .at(datetime(2025, 1, 3, 10, tzinfo=SHANGHAI))
            .fundamentals.statements(
                kind="cash_flow",
                symbols=("000001.SZ",),
                periods=2,
                fields=("free_cash_flow",),
            )
        )

    assert [row["free_cash_flow"] for row in result.table.to_pylist()] == [4.0, 3.0]


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
        store.write(
            "trade_cal",
            _table(
                "trade_cal",
                {
                    "exchange": "SSE",
                    "cal_date": date(2024, 1, 3),
                    "is_open": 1,
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


def test_stock_reference_builds_point_in_time_cny_universe(tmp_path: Path) -> None:
    tushare_root = tmp_path / "tushare"
    with TushareDataStore(tushare_root) as store:
        store.write(
            "stock_basic",
            _table(
                "stock_basic",
                {
                    "ts_code": "000001.SZ",
                    "market": "主板",
                    "exchange": "SZSE",
                    "curr_type": "CNY",
                    "list_status": "L",
                    "list_date": date(2020, 1, 1),
                },
                {
                    "ts_code": "000002.SZ",
                    "market": "主板",
                    "exchange": "SZSE",
                    "curr_type": "CNY",
                    "list_status": "D",
                    "list_date": date(2020, 1, 2),
                    "delist_date": date(2024, 1, 3),
                },
                {
                    "ts_code": "000003.SZ",
                    "market": "创业板",
                    "exchange": "SZSE",
                    "curr_type": "CNY",
                    "list_status": "L",
                    "list_date": date(2024, 1, 3),
                },
                {
                    "ts_code": "900901.SH",
                    "market": "主板",
                    "exchange": "SSE",
                    "curr_type": "USD",
                    "list_status": "L",
                    "list_date": date(2020, 1, 1),
                },
            ),
        )

    with DataCatalog(tushare_root=tushare_root, qmt_root=tmp_path / "qmt") as catalog:
        reader = DataReader(
            catalog,
            sources=SourceConfig(routes={"reference.stocks": "tushare"}),
        )
        before = reader.at(_as_of(2, 10)).reference.stocks(fields=("listing_date",))
        listing_morning = reader.at(_as_of(3, 9, 24)).reference.stocks()
        after = reader.at(_as_of(3, 9, 25)).reference.stocks(market="创业板")
        all_currencies = reader.at(_as_of(2, 10)).reference.stocks(currency=None)

    assert before.table.to_pylist() == [
        {"symbol": "000001.SZ", "listing_date": date(2020, 1, 1)},
        {"symbol": "000002.SZ", "listing_date": date(2020, 1, 2)},
    ]
    assert [row["symbol"] for row in listing_morning.table.to_pylist()] == ["000001.SZ"]
    assert after.table.to_pylist() == [
        {
            "symbol": "000003.SZ",
            "exchange": "SZSE",
            "market": "创业板",
            "currency": "CNY",
            "listing_date": date(2024, 1, 3),
        }
    ]
    assert [row["symbol"] for row in all_currencies.table.to_pylist()] == [
        "000001.SZ",
        "000002.SZ",
        "900901.SH",
    ]


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
