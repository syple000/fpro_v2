"""面向研究、回测和实盘的统一 PIT 数据读取接口。"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time, timedelta
from typing import Literal, cast
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc

from data.adapters import QmtAdapter, TushareAdapter
from data.catalog import DataCatalog
from data.config import SourceConfig
from data.errors import (
    DataAdapterError,
    DataCapabilityNotSupportedError,
    DataSourceNotConfiguredError,
    DataSourceUnavailableError,
)
from models import CAPABILITY_SCHEMAS, STATUS_SCHEMA, DataCapability, QueryResult

SHANGHAI = ZoneInfo("Asia/Shanghai")
SUPPORTED_FREQUENCIES = frozenset({"1m", "5m", "15m", "30m", "60m", "1d"})
_SYMBOL = re.compile(r"^[A-Z0-9][A-Z0-9._-]{1,31}$")


class _AllSymbols:
    __slots__ = ()

    def __repr__(self) -> str:
        return "ALL_SYMBOLS"


ALL_SYMBOLS = _AllSymbols()
Symbols = Sequence[str] | _AllSymbols
SortDirection = Literal["ascending", "descending"]
CompanyType = Literal["industrial", "bank", "insurance", "securities"]
Adapter = TushareAdapter | QmtAdapter


def _tushare(adapter: Adapter) -> TushareAdapter:
    if not isinstance(adapter, TushareAdapter):
        raise DataCapabilityNotSupportedError("该逻辑数据集需要 Tushare 适配器")
    return adapter


def _qmt(adapter: Adapter) -> QmtAdapter:
    if not isinstance(adapter, QmtAdapter):
        raise DataCapabilityNotSupportedError("分钟行情需要 QMT 适配器")
    return adapter


class DataReader:
    """按固定来源配置创建指定 PIT 时间的数据视图。"""

    def __init__(
        self,
        catalog: DataCatalog,
        *,
        sources: SourceConfig,
        max_limit: int = 1_000_000,
    ) -> None:
        if isinstance(max_limit, bool) or not isinstance(max_limit, int) or max_limit < 1:
            raise ValueError("max_limit 必须是正整数")
        adapters: dict[str, Adapter] = {
            "tushare": TushareAdapter(catalog),
            "qmt": QmtAdapter(catalog),
        }

        for route, source_id in sources.routes.items():
            adapter = adapters.get(source_id)
            if adapter is None:
                raise DataSourceNotConfiguredError(f"路由 {route!r} 配置了未注册来源 {source_id!r}")
            if DataCapability(route) not in adapter.capabilities:
                raise DataCapabilityNotSupportedError(f"来源 {source_id!r} 不支持路由 {route!r}")

        self._sources = sources
        self._adapters = adapters
        self._max_limit = max_limit

    def at(self, as_of: datetime) -> DataView:
        """创建绑定带时区具体时间的数据视图。"""
        return DataView(
            as_of=_aware_datetime(as_of, "as_of"),
            adapters=self._adapters,
            source_config=self._sources,
            max_limit=self._max_limit,
        )


class DataView:
    """策略实际接收的指定 PIT 时间数据视图。"""

    __slots__ = (
        "_as_of",
        "market",
        "fundamentals",
        "corporate_actions",
        "classification",
        "calendar",
        "_adapters",
        "_source_config",
        "_max_limit",
    )

    def __init__(
        self,
        *,
        as_of: datetime,
        adapters: Mapping[str, Adapter],
        source_config: SourceConfig,
        max_limit: int,
    ) -> None:
        self._as_of = as_of
        self._adapters = adapters
        self._source_config = source_config
        self._max_limit = max_limit
        self.market = MarketReader(self)
        self.fundamentals = FundamentalsReader(self)
        self.corporate_actions = CorporateActionsReader(self)
        self.classification = ClassificationReader(self)
        self.calendar = CalendarReader(self)

    @property
    def as_of(self) -> datetime:
        return self._as_of

    def _read(
        self,
        route: str,
        *,
        query: Callable[[Adapter], pa.Table],
        columns: tuple[str, ...] | None = None,
    ) -> tuple[pa.Table, str]:
        source_id = self._source_config.routes.get(route)
        if source_id is None:
            raise DataSourceNotConfiguredError(f"逻辑数据集 {route!r} 未配置来源") from None
        adapter = self._adapters[source_id]
        try:
            table = query(adapter)
        except (
            DataCapabilityNotSupportedError,
            DataSourceUnavailableError,
            DataAdapterError,
        ):
            raise
        except Exception as exc:
            raise DataAdapterError(f"来源 {source_id!r} 读取 {route!r} 失败") from exc
        if not isinstance(table, pa.Table):
            raise DataAdapterError(f"来源 {source_id!r} 未返回 pyarrow.Table")
        expected_schema = CAPABILITY_SCHEMAS[DataCapability(route)]
        if columns is not None:
            expected_schema = pa.schema(expected_schema.field(name) for name in columns)
        _exact_schema(table, expected_schema, route)
        return table, source_id

    def _result(
        self,
        table: pa.Table,
        *,
        identity: tuple[str, ...],
        fields: Sequence[str] | None,
        sort: tuple[tuple[str, SortDirection], ...],
        limit: int | None,
        sources: Sequence[str],
        presorted: bool = True,
    ) -> QueryResult:
        _validate_identity(table, identity)
        payload = [name for name in table.schema.names if name not in identity]
        selected = _fields(fields, payload, identity)
        projected = table.select([*identity, *selected])
        missing_sort = [name for name, _ in sort if name not in projected.schema.names]
        if missing_sort:
            # Sort keys are platform identity fields and must never be projected away.
            raise DataAdapterError(f"结果缺少排序字段: {missing_sort}")
        if not presorted and projected.num_rows > 1:
            indices = pc.sort_indices(
                projected,
                sort_keys=list(sort),
                null_placement="at_end",
            )
            projected = projected.take(indices)
        truncated = limit is not None and projected.num_rows > limit
        if limit is not None:
            projected = projected.slice(0, limit)
        return QueryResult(
            table=projected,
            as_of=self.as_of,
            sources=tuple(dict.fromkeys(sources)),
            truncated=truncated,
        )


class MarketReader:
    __slots__ = ("_data",)

    def __init__(self, data: DataView) -> None:
        self._data = data

    def bars(
        self,
        *,
        symbols: Symbols,
        frequency: str,
        count: int | None = None,
        start: date | datetime | None = None,
        end: date | datetime | None = None,
        fields: Sequence[str] | None = None,
        adjustment: Literal["none", "forward"] = "none",
        order: Literal["asc", "desc"] = "asc",
        limit: int | None = None,
    ) -> QueryResult:
        if frequency not in SUPPORTED_FREQUENCIES:
            raise ValueError(f"不支持的 frequency: {frequency!r}")
        if adjustment not in {"none", "forward"}:
            raise ValueError("adjustment 只允许 'none' 或 'forward'")
        order = _order(order)
        limit = _limit(limit, self._data._max_limit)
        normalized_symbols = _symbols(symbols, limit)
        if count is not None:
            count = _positive_int(count, "count")
            if start is not None or end is not None:
                raise ValueError("count 与 start/end 互斥")
            normalized_start = None
            normalized_end: date | datetime = self._data.as_of
        else:
            if start is None:
                raise ValueError("范围模式必须提供 start")
            normalized_start = _business_time(start, "start")
            normalized_end = _business_time(end, "end") if end is not None else self._data.as_of
            _range(normalized_start, normalized_end, "start", "end")
            if _as_datetime(normalized_end) > self._data.as_of:
                raise ValueError("end 不得晚于 as_of")
        route = "market.daily_bars" if frequency == "1d" else "market.intraday_bars"
        identity = ("symbol", "interval_start", "interval_end")
        selected, columns = _projection(route, identity, fields)
        fetch_limit = limit + 1 if limit is not None else None
        if frequency == "1d":
            table, source = self._data._read(
                route,
                query=lambda adapter: adapter.daily_bars(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    start=normalized_start,
                    end=normalized_end,
                    count=count,
                    adjustment=adjustment,
                    order=order,
                    fetch_limit=fetch_limit,
                    columns=columns,
                ),
                columns=columns,
            )
        else:
            table, source = self._data._read(
                route,
                query=lambda adapter: _qmt(adapter).intraday_bars(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    frequency=frequency,
                    start=normalized_start,
                    end=normalized_end,
                    count=count,
                    adjustment=adjustment,
                    order=order,
                    fetch_limit=fetch_limit,
                    columns=columns,
                ),
                columns=columns,
            )
        direction: SortDirection = "ascending" if order == "asc" else "descending"
        sort: tuple[tuple[str, SortDirection], ...] = (
            ("interval_end", direction),
            ("symbol", "ascending"),
            ("interval_start", direction),
        )
        return self._data._result(
            table,
            identity=identity,
            fields=selected,
            sort=sort,
            limit=limit,
            sources=(source,),
        )

    def current(
        self,
        *,
        symbols: Symbols,
        fields: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> QueryResult:
        limit = _limit(limit, self._data._max_limit)
        normalized_symbols = _symbols(symbols, limit)
        identity = ("symbol",)
        selected, columns = _projection("market.realtime_quotes", identity, fields)
        table, source = self._data._read(
            "market.realtime_quotes",
            query=lambda adapter: adapter.current(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                fetch_limit=limit + 1 if limit is not None else None,
                columns=columns,
            ),
            columns=columns,
        )
        return self._data._result(
            table,
            identity=identity,
            fields=selected,
            sort=(("symbol", "ascending"),),
            limit=limit,
            sources=(source,),
        )

    def status(
        self,
        *,
        symbols: Symbols,
        fields: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> QueryResult:
        allowed = ("suspended", "up_limit", "down_limit", "st_type")
        selected = list(allowed) if fields is None else list(fields)
        _fields(selected, allowed, ("symbol",))
        limit = _limit(limit, self._data._max_limit)
        normalized_symbols = _symbols(symbols, limit)
        routes: list[tuple[str, tuple[str, ...]]] = []
        if "suspended" in selected:
            routes.append(("market.suspensions", ("suspended",)))
        limit_fields = tuple(name for name in ("up_limit", "down_limit") if name in selected)
        if limit_fields:
            routes.append(("market.price_limits", limit_fields))
        if "st_type" in selected:
            routes.append(("market.st_status", ("st_type",)))

        rows: dict[str, dict[str, object]] = {}
        if normalized_symbols is not None:
            rows = {symbol: {"symbol": symbol} for symbol in normalized_symbols}
        sources: list[str] = []
        for route, route_fields in routes:
            fetch_limit = limit + 1 if limit is not None else None
            columns = ("symbol", *route_fields)
            if route == "market.suspensions":
                part, source = self._data._read(
                    route,
                    query=lambda adapter, fetch_limit=fetch_limit, columns=columns: _tushare(
                        adapter
                    ).suspensions(
                        as_of=self._data.as_of,
                        symbols=normalized_symbols,
                        fetch_limit=fetch_limit,
                        columns=columns,
                    ),
                    columns=columns,
                )
            elif route == "market.price_limits":
                part, source = self._data._read(
                    route,
                    query=lambda adapter, fetch_limit=fetch_limit, columns=columns: _tushare(
                        adapter
                    ).price_limits(
                        as_of=self._data.as_of,
                        symbols=normalized_symbols,
                        fetch_limit=fetch_limit,
                        columns=columns,
                    ),
                    columns=columns,
                )
            else:
                part, source = self._data._read(
                    route,
                    query=lambda adapter, fetch_limit=fetch_limit, columns=columns: _tushare(
                        adapter
                    ).st_status(
                        as_of=self._data.as_of,
                        symbols=normalized_symbols,
                        fetch_limit=fetch_limit,
                        columns=columns,
                    ),
                    columns=columns,
                )
            sources.append(source)
            for item in part.to_pylist():
                symbol = item["symbol"]
                row = rows.setdefault(symbol, {"symbol": symbol})
                for name in route_fields:
                    if name not in item:
                        raise DataAdapterError(f"{route} 缺少字段 {name!r}")
                    row[name] = item[name]
        output_rows = []
        for row in rows.values():
            output_rows.append(
                {"symbol": row["symbol"], **{name: row.get(name) for name in selected}}
            )
        schema = pa.schema(
            [STATUS_SCHEMA.field("symbol"), *(STATUS_SCHEMA.field(x) for x in selected)]
        )
        table = pa.Table.from_pylist(output_rows, schema=schema)
        return self._data._result(
            table,
            identity=("symbol",),
            fields=selected,
            sort=(("symbol", "ascending"),),
            limit=limit,
            sources=sources,
            presorted=False,
        )

    def daily_metrics(
        self,
        *,
        symbols: Symbols,
        start: date,
        end: date | None = None,
        fields: Sequence[str] | None = None,
        order: Literal["asc", "desc"] = "asc",
        limit: int | None = None,
    ) -> QueryResult:
        return self._dated_table(
            route="market.daily_metrics",
            symbols=symbols,
            start=start,
            end=end,
            fields=fields,
            order=order,
            limit=limit,
        )

    def moneyflow(
        self,
        *,
        symbols: Symbols,
        start: date,
        end: date | None = None,
        fields: Sequence[str] | None = None,
        order: Literal["asc", "desc"] = "asc",
        limit: int | None = None,
    ) -> QueryResult:
        return self._dated_table(
            route="market.moneyflow",
            symbols=symbols,
            start=start,
            end=end,
            fields=fields,
            order=order,
            limit=limit,
        )

    def _dated_table(
        self,
        *,
        route: str,
        symbols: Symbols,
        start: date,
        end: date | None,
        fields: Sequence[str] | None,
        order: str,
        limit: int | None,
    ) -> QueryResult:
        start = _plain_date(start, "start")
        end = (
            _plain_date(end, "end")
            if end is not None
            else self._data.as_of.date() + timedelta(days=1)
        )
        _range(start, end, "start", "end")
        order = _order(order)
        limit = _limit(limit, self._data._max_limit)
        normalized_symbols = _symbols(symbols, limit)
        identity = ("symbol", "trade_date")
        selected, columns = _projection(route, identity, fields)
        fetch_limit = limit + 1 if limit is not None else None
        if route == "market.daily_metrics":
            table, source = self._data._read(
                route,
                query=lambda adapter: _tushare(adapter).daily_metrics(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    start=start,
                    end=end,
                    order=order,
                    fetch_limit=fetch_limit,
                    columns=columns,
                ),
                columns=columns,
            )
        else:
            table, source = self._data._read(
                route,
                query=lambda adapter: _tushare(adapter).moneyflow(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    start=start,
                    end=end,
                    order=order,
                    fetch_limit=fetch_limit,
                    columns=columns,
                ),
                columns=columns,
            )
        return self._data._result(
            table,
            identity=identity,
            fields=selected,
            sort=(
                ("trade_date", "ascending" if order == "asc" else "descending"),
                ("symbol", "ascending"),
            ),
            limit=limit,
            sources=(source,),
        )


class FundamentalsReader:
    __slots__ = ("_data",)

    _STATEMENTS = {
        "income": "fundamentals.income",
        "balance_sheet": "fundamentals.balance_sheet",
        "cash_flow": "fundamentals.cashflow",
    }
    _DISCLOSURES = {
        "forecast": "fundamentals.forecast",
        "express": "fundamentals.express",
        "audit": "fundamentals.audit",
    }

    def __init__(self, data: DataView) -> None:
        self._data = data

    def statements(
        self,
        *,
        kind: Literal["income", "balance_sheet", "cash_flow"],
        symbols: Symbols,
        report_start: date | None = None,
        report_end: date | None = None,
        periods: int | None = None,
        company_type: CompanyType | None = None,
        fields: Sequence[str] | None = None,
        order: Literal["asc", "desc"] = "desc",
        limit: int | None = None,
    ) -> QueryResult:
        try:
            route = self._STATEMENTS[kind]
        except KeyError:
            raise ValueError(f"不支持的财报 kind: {kind!r}") from None
        report_start, report_end, periods = _report_range(report_start, report_end, periods)
        order = _order(order)
        limit = _limit(limit, self._data._max_limit)
        normalized_symbols = _symbols(symbols, limit)
        identity = (
            "symbol",
            "period_end",
            "visible_at",
            "announcement_date",
            "actual_announcement_date",
            "company_type",
        )
        selected, columns = _projection(route, identity, fields)
        normalized_company_type = _company_type(company_type)
        fetch_limit = limit + 1 if limit is not None else None
        if kind == "income":
            table, source = self._data._read(
                route,
                query=lambda adapter: _tushare(adapter).income_statements(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    report_start=report_start,
                    report_end=report_end,
                    company_type=normalized_company_type,
                    periods=periods,
                    order=order,
                    fetch_limit=fetch_limit,
                    columns=columns,
                ),
                columns=columns,
            )
        elif kind == "balance_sheet":
            table, source = self._data._read(
                route,
                query=lambda adapter: _tushare(adapter).balance_sheets(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    report_start=report_start,
                    report_end=report_end,
                    company_type=normalized_company_type,
                    periods=periods,
                    order=order,
                    fetch_limit=fetch_limit,
                    columns=columns,
                ),
                columns=columns,
            )
        else:
            table, source = self._data._read(
                route,
                query=lambda adapter: _tushare(adapter).cash_flow_statements(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    report_start=report_start,
                    report_end=report_end,
                    company_type=normalized_company_type,
                    periods=periods,
                    order=order,
                    fetch_limit=fetch_limit,
                    columns=columns,
                ),
                columns=columns,
            )
        return self._data._result(
            table,
            identity=identity,
            fields=selected,
            sort=(
                ("period_end", "ascending" if order == "asc" else "descending"),
                ("symbol", "ascending"),
                ("company_type", "ascending"),
            ),
            limit=limit,
            sources=(source,),
        )

    def indicators(
        self,
        *,
        symbols: Symbols,
        report_start: date | None = None,
        report_end: date | None = None,
        periods: int | None = None,
        fields: Sequence[str] | None = None,
        order: Literal["asc", "desc"] = "desc",
        limit: int | None = None,
    ) -> QueryResult:
        report_start, report_end, periods = _report_range(report_start, report_end, periods)
        order = _order(order)
        limit = _limit(limit, self._data._max_limit)
        normalized_symbols = _symbols(symbols, limit)
        identity = ("symbol", "period_end", "visible_at", "announcement_date")
        selected, columns = _projection("fundamentals.indicators", identity, fields)
        table, source = self._data._read(
            "fundamentals.indicators",
            query=lambda adapter: _tushare(adapter).financial_indicators(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                report_start=report_start,
                report_end=report_end,
                periods=periods,
                order=order,
                fetch_limit=limit + 1 if limit is not None else None,
                columns=columns,
            ),
            columns=columns,
        )
        return self._data._result(
            table,
            identity=identity,
            fields=selected,
            sort=(
                ("period_end", "ascending" if order == "asc" else "descending"),
                ("symbol", "ascending"),
            ),
            limit=limit,
            sources=(source,),
        )

    def disclosures(
        self,
        *,
        kind: Literal["forecast", "express", "audit"],
        symbols: Symbols,
        visible_start: datetime | None = None,
        visible_end: datetime | None = None,
        fields: Sequence[str] | None = None,
        order: Literal["asc", "desc"] = "asc",
        limit: int | None = None,
    ) -> QueryResult:
        try:
            route = self._DISCLOSURES[kind]
        except KeyError:
            raise ValueError(f"不支持的披露 kind: {kind!r}") from None
        start, end = _visible_range(visible_start, visible_end, self._data.as_of)
        order = _order(order)
        limit = _limit(limit, self._data._max_limit)
        normalized_symbols = _symbols(symbols, limit)
        identity = ("symbol", "visible_at", "period_end", "announcement_date")
        selected, columns = _projection(route, identity, fields)
        fetch_limit = limit + 1 if limit is not None else None
        if kind == "forecast":
            table, source = self._data._read(
                route,
                query=lambda adapter: _tushare(adapter).forecasts(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    visible_start=start,
                    visible_end=end,
                    order=order,
                    fetch_limit=fetch_limit,
                    columns=columns,
                ),
                columns=columns,
            )
        elif kind == "express":
            table, source = self._data._read(
                route,
                query=lambda adapter: _tushare(adapter).express_reports(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    visible_start=start,
                    visible_end=end,
                    order=order,
                    fetch_limit=fetch_limit,
                    columns=columns,
                ),
                columns=columns,
            )
        else:
            table, source = self._data._read(
                route,
                query=lambda adapter: _tushare(adapter).audit_reports(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    visible_start=start,
                    visible_end=end,
                    order=order,
                    fetch_limit=fetch_limit,
                    columns=columns,
                ),
                columns=columns,
            )
        direction = "ascending" if order == "asc" else "descending"
        return self._data._result(
            table,
            identity=identity,
            fields=selected,
            sort=(
                ("visible_at", direction),
                ("symbol", "ascending"),
                ("period_end", "ascending"),
                ("announcement_date", "ascending"),
            ),
            limit=limit,
            sources=(source,),
        )


class CorporateActionsReader:
    __slots__ = ("_data",)

    def __init__(self, data: DataView) -> None:
        self._data = data

    def dividends(
        self,
        *,
        symbols: Symbols,
        visible_start: datetime | None = None,
        visible_end: datetime | None = None,
        fields: Sequence[str] | None = None,
        order: Literal["asc", "desc"] = "asc",
        limit: int | None = None,
    ) -> QueryResult:
        start, end = _visible_range(visible_start, visible_end, self._data.as_of)
        order = _order(order)
        limit = _limit(limit, self._data._max_limit)
        normalized_symbols = _symbols(symbols, limit)
        identity = (
            "symbol",
            "visible_at",
            "ex_date",
            "end_date",
            "ann_date",
            "div_proc",
            "implementation_ann_date",
        )
        selected, columns = _projection("corporate_actions.dividends", identity, fields)
        table, source = self._data._read(
            "corporate_actions.dividends",
            query=lambda adapter: _tushare(adapter).dividends(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                visible_start=start,
                visible_end=end,
                order=order,
                fetch_limit=limit + 1 if limit is not None else None,
                columns=columns,
            ),
            columns=columns,
        )
        direction = "ascending" if order == "asc" else "descending"
        return self._data._result(
            table,
            identity=identity,
            fields=selected,
            sort=(
                ("visible_at", direction),
                ("symbol", "ascending"),
                ("ex_date", "ascending"),
                ("end_date", "ascending"),
                ("ann_date", "ascending"),
                ("div_proc", "ascending"),
                ("implementation_ann_date", "ascending"),
            ),
            limit=limit,
            sources=(source,),
        )

    def adjustment_factors(
        self,
        *,
        symbols: Symbols,
        start: date | None = None,
        end: date | None = None,
        order: Literal["asc", "desc"] = "asc",
        limit: int | None = None,
    ) -> QueryResult:
        if start is not None:
            start = _plain_date(start, "start")
        if end is not None:
            end = _plain_date(end, "end")
        if start is not None and end is not None:
            _range(start, end, "start", "end")
        order = _order(order)
        limit = _limit(limit, self._data._max_limit)
        normalized_symbols = _symbols(symbols, limit)
        table, source = self._data._read(
            "corporate_actions.adjustment_factors",
            query=lambda adapter: _tushare(adapter).adjustment_factors(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                start=start,
                end=end,
                order=order,
                fetch_limit=limit + 1 if limit is not None else None,
            ),
        )
        return self._data._result(
            table,
            identity=("symbol", "trade_date"),
            fields=("factor",),
            sort=(
                ("trade_date", "ascending" if order == "asc" else "descending"),
                ("symbol", "ascending"),
            ),
            limit=limit,
            sources=(source,),
        )


class ClassificationReader:
    __slots__ = ("_data",)

    def __init__(self, data: DataView) -> None:
        self._data = data

    def industry(
        self,
        *,
        symbols: Symbols,
        level: Literal[1, 2, 3] = 1,
        limit: int | None = None,
    ) -> QueryResult:
        if isinstance(level, bool) or level not in {1, 2, 3}:
            raise ValueError("level 只允许 1、2、3")
        limit = _limit(limit, self._data._max_limit)
        normalized_symbols = _symbols(symbols, limit)
        table, source = self._data._read(
            "classification.industry",
            query=lambda adapter: _tushare(adapter).industry(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                level=level,
                fetch_limit=limit + 1 if limit is not None else None,
            ),
        )
        return self._data._result(
            table,
            identity=("symbol",),
            fields=("level", "industry_code", "industry_name"),
            sort=(("symbol", "ascending"),),
            limit=limit,
            sources=(source,),
        )


class CalendarReader:
    __slots__ = ("_data",)

    def __init__(self, data: DataView) -> None:
        self._data = data

    def sessions(
        self,
        *,
        start: date,
        end: date | None = None,
        exchange: str | None = None,
        fields: Sequence[str] | None = None,
        order: Literal["asc", "desc"] = "asc",
        limit: int | None = None,
    ) -> QueryResult:
        start = _plain_date(start, "start")
        end = (
            _plain_date(end, "end")
            if end is not None
            else self._data.as_of.date() + timedelta(days=1)
        )
        _range(start, end, "start", "end")
        order = _order(order)
        limit = _limit(limit, self._data._max_limit)
        identity = ("cal_date", "exchange")
        selected, columns = _projection("calendar.sessions", identity, fields)
        table, source = self._data._read(
            "calendar.sessions",
            query=lambda adapter: _tushare(adapter).sessions(
                as_of=self._data.as_of,
                start=start,
                end=end,
                exchange=_optional_code(exchange, "exchange"),
                order=order,
                fetch_limit=limit + 1 if limit is not None else None,
                columns=columns,
            ),
            columns=columns,
        )
        return self._data._result(
            table,
            identity=identity,
            fields=selected,
            sort=(
                ("cal_date", "ascending" if order == "asc" else "descending"),
                ("exchange", "ascending"),
            ),
            limit=limit,
            sources=(source,),
        )

    def previous_session(self, *, exchange: str) -> date | None:
        normalized_exchange = _optional_code(exchange, "exchange")
        assert normalized_exchange is not None
        table, _ = self._data._read(
            "calendar.sessions",
            query=lambda adapter: _tushare(adapter).previous_session(
                end=self._data.as_of.date(),
                exchange=normalized_exchange,
            ),
            columns=("cal_date", "exchange"),
        )
        if table.num_rows == 0:
            return None
        return cast(date, table.column("cal_date")[0].as_py())


def _validate_identity(table: pa.Table, identity: Sequence[str]) -> None:
    missing = [name for name in identity if name not in table.schema.names]
    if missing:
        raise DataAdapterError(f"适配器结果缺少平台身份字段: {missing}")
    required = {"symbol", "interval_start", "interval_end", "trade_date", "cal_date", "exchange"}
    for name in identity:
        if name in required and table.column(name).null_count:
            raise DataAdapterError(f"平台身份字段 {name!r} 不能为 null")


def _exact_schema(table: pa.Table, schema: pa.Schema, route: str) -> None:
    if not table.schema.equals(schema):
        raise DataAdapterError(f"{route} 平台 Schema 不匹配；期望 {schema}，实际 {table.schema}")
    null_fields = [
        field.name for field in schema if not field.nullable and table.column(field.name).null_count
    ]
    if null_fields:
        raise DataAdapterError(f"{route} 平台非空字段包含 null: {null_fields}")


def _fields(
    fields: Sequence[str] | None,
    allowed: Sequence[str],
    identity: Sequence[str],
) -> list[str]:
    if fields is None:
        return list(allowed)
    if isinstance(fields, (str, bytes)):
        raise TypeError("fields 必须是字段序列")
    selected = list(fields)
    if any(not isinstance(name, str) for name in selected):
        raise TypeError("fields 只能包含字符串")
    if len(selected) != len(set(selected)):
        raise ValueError("fields 不能包含重复字段")
    invalid = [name for name in selected if name not in allowed or name in identity]
    if invalid:
        raise ValueError(f"未知或不可投影字段: {invalid}")
    return selected


def _projection(
    route: str,
    identity: tuple[str, ...],
    fields: Sequence[str] | None,
) -> tuple[list[str], tuple[str, ...]]:
    schema = CAPABILITY_SCHEMAS[DataCapability(route)]
    payload = [name for name in schema.names if name not in identity]
    selected = _fields(fields, payload, identity)
    return selected, (*identity, *selected)


def _symbols(symbols: Symbols, limit: int | None) -> tuple[str, ...] | None:
    if symbols is ALL_SYMBOLS:
        if limit is None:
            raise ValueError("ALL_SYMBOLS 必须同时提供 limit")
        return None
    if isinstance(symbols, (str, bytes)) or not isinstance(symbols, Sequence):
        raise TypeError("symbols 必须是证券代码序列或 ALL_SYMBOLS")
    values = tuple(symbols)
    if any(not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol) for symbol in values):
        raise ValueError("symbols 包含格式错误的证券代码")
    if len(values) != len(set(values)):
        raise ValueError("symbols 不能包含重复证券代码")
    return values


def _limit(value: int | None, maximum: int) -> int | None:
    if value is None:
        return None
    value = _positive_int(value, "limit")
    if value > maximum:
        raise ValueError(f"limit 不能超过 {maximum}")
    return value


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} 必须是正整数")
    return value


def _order(value: str) -> Literal["asc", "desc"]:
    if value not in {"asc", "desc"}:
        raise ValueError("order 只允许 'asc' 或 'desc'")
    return cast(Literal["asc", "desc"], value)


def _aware_datetime(value: object, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} 必须是带时区 datetime")
    if value.utcoffset() is None:
        raise ValueError(f"{name} 必须包含时区")
    return value.astimezone(SHANGHAI)


def _plain_date(value: object, name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise TypeError(f"{name} 必须是 date")
    return value


def _business_time(value: object, name: str) -> date | datetime:
    if isinstance(value, datetime):
        return _aware_datetime(value, name)
    return _plain_date(value, name)


def _as_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, time.min, tzinfo=SHANGHAI)


def _range(start: date | datetime, end: date | datetime, start_name: str, end_name: str) -> None:
    if _as_datetime(start) >= _as_datetime(end):
        raise ValueError(f"{start_name} 必须早于 {end_name}")


def _report_range(
    report_start: date | None,
    report_end: date | None,
    periods: int | None,
) -> tuple[date | None, date | None, int | None]:
    if periods is not None:
        periods = _positive_int(periods, "periods")
        if report_start is not None or report_end is not None:
            raise ValueError("periods 与 report_start/report_end 互斥")
        return None, None, periods
    if report_start is None:
        raise ValueError("范围模式必须提供 report_start")
    report_start = _plain_date(report_start, "report_start")
    if report_end is not None:
        report_end = _plain_date(report_end, "report_end")
        _range(report_start, report_end, "report_start", "report_end")
    return report_start, report_end, None


def _visible_range(
    visible_start: datetime | None,
    visible_end: datetime | None,
    as_of: datetime,
) -> tuple[datetime | None, datetime]:
    start = _aware_datetime(visible_start, "visible_start") if visible_start is not None else None
    end = _aware_datetime(visible_end, "visible_end") if visible_end is not None else as_of
    if end > as_of:
        raise ValueError("visible_end 不得晚于 as_of")
    if start is not None and start > end:
        raise ValueError("visible_start 不得晚于 visible_end")
    return start, end


def _optional_code(value: str | None, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} 必须是非空字符串")
    return value


def _company_type(value: str | None) -> CompanyType | None:
    if value is None:
        return None
    if value not in {"industrial", "bank", "insurance", "securities"}:
        raise ValueError("company_type 只允许 'industrial'、'bank'、'insurance' 或 'securities'")
    return cast(CompanyType, value)
