"""面向研究、回测和实盘的统一 PIT 数据读取接口。"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta
from typing import Literal, cast
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc

from market_data.adapters import QmtAdapter, TushareAdapter
from market_data.catalog import DataCatalog
from market_data.config import SourceConfig
from market_data.errors import (
    DataAdapterError,
    DataResultTooLargeError,
    DataSourceNotConfiguredError,
)
from market_data.protocols import DataAdapter
from models import ROUTE_SCHEMAS, STATUS_SCHEMA, QueryResult

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


class DataReader:
    """按固定来源配置创建指定 PIT 时间的数据视图。"""

    def __init__(
        self,
        catalog: DataCatalog,
        *,
        sources: SourceConfig,
        adapters: Mapping[str, DataAdapter] | None = None,
        max_result_rows: int = 1_000_000,
    ) -> None:
        if (
            isinstance(max_result_rows, bool)
            or not isinstance(max_result_rows, int)
            or max_result_rows < 1
        ):
            raise ValueError("max_result_rows 必须是正整数")
        custom_adapters: dict[str, DataAdapter] = {}
        if adapters is not None:
            if not isinstance(adapters, Mapping):
                raise TypeError("adapters 必须是 source_id 到 DataAdapter 的映射")
            for source_id, adapter in adapters.items():
                if not isinstance(source_id, str) or not source_id.strip():
                    raise ValueError("adapters 的 source_id 必须是非空字符串")
                normalized_source_id = source_id.strip()
                if normalized_source_id in {"tushare", "qmt"}:
                    raise ValueError(f"数据来源 {normalized_source_id!r} 已注册")
                if not isinstance(adapter, DataAdapter):
                    raise TypeError("自定义 adapter 必须继承 DataAdapter")
                custom_adapters[normalized_source_id] = adapter

        for route, source_id in sources.routes.items():
            if source_id not in {"tushare", "qmt"} and source_id not in custom_adapters:
                raise DataSourceNotConfiguredError(f"路由 {route!r} 配置了未注册来源 {source_id!r}")

        self._sources = sources
        self._tushare_adapter = TushareAdapter(catalog)
        self._qmt_adapter = QmtAdapter(catalog)
        self._custom_adapters = custom_adapters
        self._max_result_rows = max_result_rows

    def at(self, as_of: datetime) -> DataView:
        """创建绑定带时区具体时间的数据视图。"""
        return DataView(
            as_of=_aware_datetime(as_of, "as_of"),
            tushare_adapter=self._tushare_adapter,
            qmt_adapter=self._qmt_adapter,
            custom_adapters=self._custom_adapters,
            source_config=self._sources,
            max_result_rows=self._max_result_rows,
        )


class DataView:
    """策略实际接收的指定 PIT 时间数据视图。"""

    __slots__ = (
        "_as_of",
        "market",
        "fundamentals",
        "corporate_actions",
        "classification",
        "reference",
        "calendar",
        "_tushare_adapter",
        "_qmt_adapter",
        "_custom_adapters",
        "_source_config",
        "_max_result_rows",
    )

    def __init__(
        self,
        *,
        as_of: datetime,
        tushare_adapter: TushareAdapter,
        qmt_adapter: QmtAdapter,
        custom_adapters: Mapping[str, DataAdapter],
        source_config: SourceConfig,
        max_result_rows: int,
    ) -> None:
        self._as_of = as_of
        self._tushare_adapter = tushare_adapter
        self._qmt_adapter = qmt_adapter
        self._custom_adapters = custom_adapters
        self._source_config = source_config
        self._max_result_rows = max_result_rows
        self.market = MarketReader(self)
        self.fundamentals = FundamentalsReader(self)
        self.corporate_actions = CorporateActionsReader(self)
        self.classification = ClassificationReader(self)
        self.reference = ReferenceReader(self)
        self.calendar = CalendarReader(self)

    @property
    def as_of(self) -> datetime:
        return self._as_of

    def _source(self, route: str) -> str:
        source_id = self._source_config.routes.get(route)
        if source_id is None:
            raise DataSourceNotConfiguredError(f"逻辑数据集 {route!r} 未配置来源") from None
        return source_id

    def _validate_table(
        self,
        route: str,
        source_id: str,
        table: pa.Table,
        columns: tuple[str, ...] | None = None,
    ) -> pa.Table:
        if not isinstance(table, pa.Table):
            raise DataAdapterError(f"来源 {source_id!r} 未返回 pyarrow.Table")
        expected_schema = ROUTE_SCHEMAS[route]
        if columns is not None:
            expected_schema = pa.schema(expected_schema.field(name) for name in columns)
        _exact_schema(table, expected_schema, route)
        return table

    def _result(
        self,
        table: pa.Table,
        *,
        identity: tuple[str, ...],
        fields: Sequence[str] | None,
        sort: tuple[tuple[str, SortDirection], ...],
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
        if projected.num_rows > self._max_result_rows:
            raise DataResultTooLargeError(
                f"查询结果超过内部上限 {self._max_result_rows} 行；"
                "请缩小 symbols、时间范围、count 或 periods"
            )
        return QueryResult(
            table=projected,
            as_of=self.as_of,
            sources=tuple(dict.fromkeys(sources)),
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
    ) -> QueryResult:
        if frequency not in SUPPORTED_FREQUENCIES:
            raise ValueError(f"不支持的 frequency: {frequency!r}")
        if adjustment not in {"none", "forward"}:
            raise ValueError("adjustment 只允许 'none' 或 'forward'")
        order = _order(order)
        normalized_symbols = _symbols(symbols)
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
        fetch_limit = self._data._max_result_rows + 1
        source = self._data._source(route)
        if frequency == "1d":
            if source == "tushare":
                table = self._data._tushare_adapter.daily_bars(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    start=normalized_start,
                    end=normalized_end,
                    count=count,
                    adjustment=adjustment,
                    order=order,
                    fetch_limit=fetch_limit,
                    columns=columns,
                )
            elif source == "qmt":
                table = self._data._qmt_adapter.daily_bars(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    start=normalized_start,
                    end=normalized_end,
                    count=count,
                    adjustment=adjustment,
                    order=order,
                    fetch_limit=fetch_limit,
                    columns=columns,
                )
            else:
                table = self._data._custom_adapters[source].daily_bars(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    start=normalized_start,
                    end=normalized_end,
                    count=count,
                    adjustment=adjustment,
                    order=order,
                    fetch_limit=fetch_limit,
                    columns=columns,
                )
        else:
            if source == "tushare":
                table = self._data._tushare_adapter.intraday_bars(
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
                )
            elif source == "qmt":
                table = self._data._qmt_adapter.intraday_bars(
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
                )
            else:
                table = self._data._custom_adapters[source].intraday_bars(
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
                )
        table = self._data._validate_table(route, source, table, columns)
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
            sources=(source,),
        )

    def current(
        self,
        *,
        symbols: Symbols,
        fields: Sequence[str] | None = None,
    ) -> QueryResult:
        normalized_symbols = _symbols(symbols)
        identity = ("symbol",)
        route = "market.realtime_quotes"
        selected, columns = _projection(route, identity, fields)
        source = self._data._source(route)
        if source == "tushare":
            table = self._data._tushare_adapter.current(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        elif source == "qmt":
            table = self._data._qmt_adapter.current(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        else:
            table = self._data._custom_adapters[source].current(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        table = self._data._validate_table(route, source, table, columns)
        return self._data._result(
            table,
            identity=identity,
            fields=selected,
            sort=(("symbol", "ascending"),),
            sources=(source,),
        )

    def status(
        self,
        *,
        symbols: Symbols,
        fields: Sequence[str] | None = None,
    ) -> QueryResult:
        if symbols is ALL_SYMBOLS:
            raise ValueError(
                "market.status() 暂不支持 ALL_SYMBOLS；缺少 PIT 股票池时请显式提供 symbols"
            )
        allowed = ("suspended", "up_limit", "down_limit", "st_type")
        selected = list(allowed) if fields is None else list(fields)
        _fields(selected, allowed, ("symbol",))
        normalized_symbols = _symbols(symbols)
        rows: dict[str, dict[str, object]] = {}
        if normalized_symbols is not None:
            rows = {symbol: {"symbol": symbol} for symbol in normalized_symbols}
        sources: list[str] = []
        fetch_limit = self._data._max_result_rows + 1

        if "suspended" in selected:
            route = "market.suspensions"
            columns = ("symbol", "suspended")
            source = self._data._source(route)
            if source == "tushare":
                part = self._data._tushare_adapter.suspensions(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    fetch_limit=fetch_limit,
                    columns=columns,
                )
            elif source == "qmt":
                part = self._data._qmt_adapter.suspensions(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    fetch_limit=fetch_limit,
                    columns=columns,
                )
            else:
                part = self._data._custom_adapters[source].suspensions(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    fetch_limit=fetch_limit,
                    columns=columns,
                )
            part = self._data._validate_table(route, source, part, columns)
            sources.append(source)
            for item in part.to_pylist():
                symbol = item["symbol"]
                row = rows.setdefault(symbol, {"symbol": symbol})
                row["suspended"] = item["suspended"]

        limit_fields = tuple(name for name in ("up_limit", "down_limit") if name in selected)
        if limit_fields:
            route = "market.price_limits"
            columns = ("symbol", *limit_fields)
            source = self._data._source(route)
            if source == "tushare":
                part = self._data._tushare_adapter.price_limits(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    fetch_limit=fetch_limit,
                    columns=columns,
                )
            elif source == "qmt":
                part = self._data._qmt_adapter.price_limits(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    fetch_limit=fetch_limit,
                    columns=columns,
                )
            else:
                part = self._data._custom_adapters[source].price_limits(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    fetch_limit=fetch_limit,
                    columns=columns,
                )
            part = self._data._validate_table(route, source, part, columns)
            sources.append(source)
            for item in part.to_pylist():
                symbol = item["symbol"]
                row = rows.setdefault(symbol, {"symbol": symbol})
                for name in limit_fields:
                    row[name] = item[name]

        if "st_type" in selected:
            route = "market.st_status"
            columns = ("symbol", "st_type")
            source = self._data._source(route)
            if source == "tushare":
                part = self._data._tushare_adapter.st_status(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    fetch_limit=fetch_limit,
                    columns=columns,
                )
            elif source == "qmt":
                part = self._data._qmt_adapter.st_status(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    fetch_limit=fetch_limit,
                    columns=columns,
                )
            else:
                part = self._data._custom_adapters[source].st_status(
                    as_of=self._data.as_of,
                    symbols=normalized_symbols,
                    fetch_limit=fetch_limit,
                    columns=columns,
                )
            part = self._data._validate_table(route, source, part, columns)
            sources.append(source)
            for item in part.to_pylist():
                symbol = item["symbol"]
                row = rows.setdefault(symbol, {"symbol": symbol})
                row["st_type"] = item["st_type"]
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
    ) -> QueryResult:
        start = _plain_date(start, "start")
        end = (
            _plain_date(end, "end")
            if end is not None
            else self._data.as_of.date() + timedelta(days=1)
        )
        _range(start, end, "start", "end")
        order = _order(order)
        normalized_symbols = _symbols(symbols)
        route = "market.daily_metrics"
        identity = ("symbol", "trade_date")
        selected, columns = _projection(route, identity, fields)
        source = self._data._source(route)
        if source == "tushare":
            table = self._data._tushare_adapter.daily_metrics(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                start=start,
                end=end,
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        elif source == "qmt":
            table = self._data._qmt_adapter.daily_metrics(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                start=start,
                end=end,
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        else:
            table = self._data._custom_adapters[source].daily_metrics(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                start=start,
                end=end,
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        table = self._data._validate_table(route, source, table, columns)
        return self._data._result(
            table,
            identity=identity,
            fields=selected,
            sort=(
                ("trade_date", "ascending" if order == "asc" else "descending"),
                ("symbol", "ascending"),
            ),
            sources=(source,),
        )

    def moneyflow(
        self,
        *,
        symbols: Symbols,
        start: date,
        end: date | None = None,
        fields: Sequence[str] | None = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> QueryResult:
        start = _plain_date(start, "start")
        end = (
            _plain_date(end, "end")
            if end is not None
            else self._data.as_of.date() + timedelta(days=1)
        )
        _range(start, end, "start", "end")
        order = _order(order)
        normalized_symbols = _symbols(symbols)
        route = "market.moneyflow"
        identity = ("symbol", "trade_date")
        selected, columns = _projection(route, identity, fields)
        source = self._data._source(route)
        if source == "tushare":
            table = self._data._tushare_adapter.moneyflow(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                start=start,
                end=end,
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        elif source == "qmt":
            table = self._data._qmt_adapter.moneyflow(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                start=start,
                end=end,
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        else:
            table = self._data._custom_adapters[source].moneyflow(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                start=start,
                end=end,
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        table = self._data._validate_table(route, source, table, columns)
        return self._data._result(
            table,
            identity=identity,
            fields=selected,
            sort=(
                ("trade_date", "ascending" if order == "asc" else "descending"),
                ("symbol", "ascending"),
            ),
            sources=(source,),
        )


class FundamentalsReader:
    __slots__ = ("_data",)

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
    ) -> QueryResult:
        if kind == "income":
            route = "fundamentals.income"
        elif kind == "balance_sheet":
            route = "fundamentals.balance_sheet"
        elif kind == "cash_flow":
            route = "fundamentals.cashflow"
        else:
            raise ValueError(f"不支持的财报 kind: {kind!r}")
        report_start, report_end, periods = _report_range(report_start, report_end, periods)
        order = _order(order)
        normalized_symbols = _symbols(symbols)
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
        fetch_limit = self._data._max_result_rows + 1
        source = self._data._source(route)
        if source == "tushare":
            table = self._data._tushare_adapter.statements(
                kind=kind,
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                report_start=report_start,
                report_end=report_end,
                company_type=normalized_company_type,
                periods=periods,
                order=order,
                fetch_limit=fetch_limit,
                columns=columns,
            )
        elif source == "qmt":
            table = self._data._qmt_adapter.statements(
                kind=kind,
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                report_start=report_start,
                report_end=report_end,
                company_type=normalized_company_type,
                periods=periods,
                order=order,
                fetch_limit=fetch_limit,
                columns=columns,
            )
        else:
            table = self._data._custom_adapters[source].statements(
                kind=kind,
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                report_start=report_start,
                report_end=report_end,
                company_type=normalized_company_type,
                periods=periods,
                order=order,
                fetch_limit=fetch_limit,
                columns=columns,
            )
        table = self._data._validate_table(route, source, table, columns)
        return self._data._result(
            table,
            identity=identity,
            fields=selected,
            sort=(
                ("period_end", "ascending" if order == "asc" else "descending"),
                ("symbol", "ascending"),
                ("company_type", "ascending"),
            ),
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
    ) -> QueryResult:
        report_start, report_end, periods = _report_range(report_start, report_end, periods)
        order = _order(order)
        normalized_symbols = _symbols(symbols)
        identity = ("symbol", "period_end", "visible_at", "announcement_date")
        route = "fundamentals.indicators"
        selected, columns = _projection(route, identity, fields)
        source = self._data._source(route)
        if source == "tushare":
            table = self._data._tushare_adapter.financial_indicators(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                report_start=report_start,
                report_end=report_end,
                periods=periods,
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        elif source == "qmt":
            table = self._data._qmt_adapter.financial_indicators(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                report_start=report_start,
                report_end=report_end,
                periods=periods,
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        else:
            table = self._data._custom_adapters[source].financial_indicators(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                report_start=report_start,
                report_end=report_end,
                periods=periods,
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        table = self._data._validate_table(route, source, table, columns)
        return self._data._result(
            table,
            identity=identity,
            fields=selected,
            sort=(
                ("period_end", "ascending" if order == "asc" else "descending"),
                ("symbol", "ascending"),
            ),
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
    ) -> QueryResult:
        if kind == "forecast":
            route = "fundamentals.forecast"
        elif kind == "express":
            route = "fundamentals.express"
        elif kind == "audit":
            route = "fundamentals.audit"
        else:
            raise ValueError(f"不支持的披露 kind: {kind!r}")
        start, end = _visible_range(visible_start, visible_end, self._data.as_of)
        order = _order(order)
        normalized_symbols = _symbols(symbols)
        identity = ("symbol", "visible_at", "period_end", "announcement_date")
        selected, columns = _projection(route, identity, fields)
        fetch_limit = self._data._max_result_rows + 1
        source = self._data._source(route)
        if source == "tushare":
            table = self._data._tushare_adapter.disclosures(
                kind=kind,
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                visible_start=start,
                visible_end=end,
                order=order,
                fetch_limit=fetch_limit,
                columns=columns,
            )
        elif source == "qmt":
            table = self._data._qmt_adapter.disclosures(
                kind=kind,
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                visible_start=start,
                visible_end=end,
                order=order,
                fetch_limit=fetch_limit,
                columns=columns,
            )
        else:
            table = self._data._custom_adapters[source].disclosures(
                kind=kind,
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                visible_start=start,
                visible_end=end,
                order=order,
                fetch_limit=fetch_limit,
                columns=columns,
            )
        table = self._data._validate_table(route, source, table, columns)
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
    ) -> QueryResult:
        start, end = _visible_range(visible_start, visible_end, self._data.as_of)
        order = _order(order)
        normalized_symbols = _symbols(symbols)
        identity = (
            "symbol",
            "visible_at",
            "ex_date",
            "end_date",
            "ann_date",
            "div_proc",
            "implementation_ann_date",
        )
        route = "corporate_actions.dividends"
        selected, columns = _projection(route, identity, fields)
        source = self._data._source(route)
        if source == "tushare":
            table = self._data._tushare_adapter.dividends(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                visible_start=start,
                visible_end=end,
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        elif source == "qmt":
            table = self._data._qmt_adapter.dividends(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                visible_start=start,
                visible_end=end,
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        else:
            table = self._data._custom_adapters[source].dividends(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                visible_start=start,
                visible_end=end,
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        table = self._data._validate_table(route, source, table, columns)
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
            sources=(source,),
        )

    def adjustment_factors(
        self,
        *,
        symbols: Symbols,
        start: date | None = None,
        end: date | None = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> QueryResult:
        if start is not None:
            start = _plain_date(start, "start")
        if end is not None:
            end = _plain_date(end, "end")
        if start is not None and end is not None:
            _range(start, end, "start", "end")
        order = _order(order)
        normalized_symbols = _symbols(symbols)
        route = "corporate_actions.adjustment_factors"
        source = self._data._source(route)
        if source == "tushare":
            table = self._data._tushare_adapter.adjustment_factors(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                start=start,
                end=end,
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
            )
        elif source == "qmt":
            table = self._data._qmt_adapter.adjustment_factors(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                start=start,
                end=end,
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
            )
        else:
            table = self._data._custom_adapters[source].adjustment_factors(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                start=start,
                end=end,
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
            )
        table = self._data._validate_table(route, source, table)
        return self._data._result(
            table,
            identity=("symbol", "trade_date"),
            fields=("factor",),
            sort=(
                ("trade_date", "ascending" if order == "asc" else "descending"),
                ("symbol", "ascending"),
            ),
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
    ) -> QueryResult:
        if isinstance(level, bool) or level not in {1, 2, 3}:
            raise ValueError("level 只允许 1、2、3")
        normalized_symbols = _symbols(symbols)
        route = "classification.industry"
        source = self._data._source(route)
        if source == "tushare":
            table = self._data._tushare_adapter.industry(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                level=level,
                fetch_limit=self._data._max_result_rows + 1,
            )
        elif source == "qmt":
            table = self._data._qmt_adapter.industry(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                level=level,
                fetch_limit=self._data._max_result_rows + 1,
            )
        else:
            table = self._data._custom_adapters[source].industry(
                as_of=self._data.as_of,
                symbols=normalized_symbols,
                level=level,
                fetch_limit=self._data._max_result_rows + 1,
            )
        table = self._data._validate_table(route, source, table)
        return self._data._result(
            table,
            identity=("symbol",),
            fields=("level", "industry_code", "industry_name"),
            sort=(("symbol", "ascending"),),
            sources=(source,),
        )


class ReferenceReader:
    __slots__ = ("_data",)

    def __init__(self, data: DataView) -> None:
        self._data = data

    def stocks(
        self,
        *,
        exchange: str | None = None,
        market: str | None = None,
        currency: str | None = "CNY",
        fields: Sequence[str] | None = None,
    ) -> QueryResult:
        route = "reference.stocks"
        identity = ("symbol",)
        selected, columns = _projection(route, identity, fields)
        source = self._data._source(route)
        if source == "tushare":
            table = self._data._tushare_adapter.stocks(
                as_of=self._data.as_of,
                exchange=_optional_code(exchange, "exchange"),
                market=_optional_code(market, "market"),
                currency=_optional_code(currency, "currency"),
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        elif source == "qmt":
            table = self._data._qmt_adapter.stocks(
                as_of=self._data.as_of,
                exchange=_optional_code(exchange, "exchange"),
                market=_optional_code(market, "market"),
                currency=_optional_code(currency, "currency"),
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        else:
            table = self._data._custom_adapters[source].stocks(
                as_of=self._data.as_of,
                exchange=_optional_code(exchange, "exchange"),
                market=_optional_code(market, "market"),
                currency=_optional_code(currency, "currency"),
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        table = self._data._validate_table(route, source, table, columns)
        return self._data._result(
            table,
            identity=identity,
            fields=selected,
            sort=(("symbol", "ascending"),),
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
    ) -> QueryResult:
        start = _plain_date(start, "start")
        end = (
            _plain_date(end, "end")
            if end is not None
            else self._data.as_of.date() + timedelta(days=1)
        )
        _range(start, end, "start", "end")
        order = _order(order)
        identity = ("cal_date", "exchange")
        route = "calendar.sessions"
        selected, columns = _projection(route, identity, fields)
        source = self._data._source(route)
        if source == "tushare":
            table = self._data._tushare_adapter.sessions(
                as_of=self._data.as_of,
                start=start,
                end=end,
                exchange=_optional_code(exchange, "exchange"),
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        elif source == "qmt":
            table = self._data._qmt_adapter.sessions(
                as_of=self._data.as_of,
                start=start,
                end=end,
                exchange=_optional_code(exchange, "exchange"),
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        else:
            table = self._data._custom_adapters[source].sessions(
                as_of=self._data.as_of,
                start=start,
                end=end,
                exchange=_optional_code(exchange, "exchange"),
                order=order,
                fetch_limit=self._data._max_result_rows + 1,
                columns=columns,
            )
        table = self._data._validate_table(route, source, table, columns)
        return self._data._result(
            table,
            identity=identity,
            fields=selected,
            sort=(
                ("cal_date", "ascending" if order == "asc" else "descending"),
                ("exchange", "ascending"),
            ),
            sources=(source,),
        )

    def previous_session(self, *, exchange: str) -> date | None:
        normalized_exchange = _optional_code(exchange, "exchange")
        assert normalized_exchange is not None
        route = "calendar.sessions"
        columns = ("cal_date", "exchange")
        source = self._data._source(route)
        if source == "tushare":
            table = self._data._tushare_adapter.previous_session(
                end=self._data.as_of.date(),
                exchange=normalized_exchange,
            )
        elif source == "qmt":
            table = self._data._qmt_adapter.previous_session(
                end=self._data.as_of.date(),
                exchange=normalized_exchange,
            )
        else:
            table = self._data._custom_adapters[source].previous_session(
                end=self._data.as_of.date(),
                exchange=normalized_exchange,
            )
        table = self._data._validate_table(route, source, table, columns)
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
    schema = ROUTE_SCHEMAS[route]
    payload = [name for name in schema.names if name not in identity]
    selected = _fields(fields, payload, identity)
    return selected, (*identity, *selected)


def _symbols(symbols: Symbols) -> tuple[str, ...] | None:
    if symbols is ALL_SYMBOLS:
        return None
    if isinstance(symbols, (str, bytes)) or not isinstance(symbols, Sequence):
        raise TypeError("symbols 必须是证券代码序列或 ALL_SYMBOLS")
    values = tuple(symbols)
    if any(not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol) for symbol in values):
        raise ValueError("symbols 包含格式错误的证券代码")
    if len(values) != len(set(values)):
        raise ValueError("symbols 不能包含重复证券代码")
    return values


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
