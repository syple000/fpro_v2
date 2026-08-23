"""把 Tushare 与 QMT 的有效 Parquet 文件映射为 DuckDB 视图。"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pyarrow as pa

from qmt_receiver import (
    BAR_SCHEMA,
    BAR_TABLE,
    DAILY_SCHEMA,
    DAILY_TABLE,
    DIVIDEND_FACTOR_SCHEMA,
    DIVIDEND_FACTOR_TABLE,
    FINANCIAL_SCHEMA,
    FINANCIAL_TABLE,
    TICK_SCHEMA,
    TICK_TABLE,
)
from tushare_data.schemas import TABLE_SCHEMAS

_TUSHARE_MARKET_TABLES = (
    "daily",
    "daily_basic",
    "adj_factor",
    "suspend_d",
    "stk_limit",
    "stock_st",
    "moneyflow",
)
_TUSHARE_STATEMENT_TABLES = ("income", "balancesheet", "cashflow")
_TUSHARE_ANNOUNCEMENT_TABLES = (
    "forecast",
    "express",
    "fina_audit",
    "fina_indicator",
)
_QMT_SCHEMAS = {
    TICK_TABLE: TICK_SCHEMA,
    BAR_TABLE: BAR_SCHEMA,
    DAILY_TABLE: DAILY_SCHEMA,
    FINANCIAL_TABLE: FINANCIAL_SCHEMA,
    DIVIDEND_FACTOR_TABLE: DIVIDEND_FACTOR_SCHEMA,
}


class DataCatalog:
    """在一个 DuckDB 连接中公开 Tushare/QMT 原始视图和 PIT 表宏。"""

    def __init__(
        self,
        *,
        tushare_root: str | Path,
        qmt_root: str | Path,
        connection: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        self._tushare_root = Path(tushare_root).expanduser().resolve()
        self._qmt_root = Path(qmt_root).expanduser().resolve()
        self._connection = connection or duckdb.connect(":memory:")
        self._owns_connection = connection is None
        self.refresh()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """返回已注册 `tushare` 和 `qmt` schema 的 DuckDB 连接。"""
        return self._connection

    def refresh(self) -> None:
        """根据最新 Manifest 重新注册原始视图和 as_of 表宏。"""
        self._connection.execute('CREATE SCHEMA IF NOT EXISTS "tushare"')
        self._connection.execute('CREATE SCHEMA IF NOT EXISTS "qmt"')
        for table, schema in TABLE_SCHEMAS.items():
            self._register_view("tushare", table, self._tushare_root, schema)
        for table, schema in _QMT_SCHEMAS.items():
            self._register_view("qmt", table, self._qmt_root, schema)
        self._register_as_of_macros()

    def close(self) -> None:
        """关闭由本对象创建的 DuckDB 连接。"""
        if self._owns_connection:
            self._connection.close()

    def __enter__(self) -> DataCatalog:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _register_view(
        self,
        source: str,
        table: str,
        root: Path,
        schema: pa.Schema,
    ) -> None:
        files = _active_files(root / table)
        qualified = f"{_quote_identifier(source)}.{_quote_identifier(table)}"
        if files:
            paths = ", ".join(_quote_string(str(path)) for path in files)
            query = (
                f"CREATE OR REPLACE VIEW {qualified} AS "
                f"SELECT * FROM read_parquet([{paths}], "
                "union_by_name = true, hive_partitioning = false)"
            )
        else:
            registration = f"__data_empty_{source}_{table}"
            self._connection.register(registration, schema.empty_table())
            query = (
                f"CREATE OR REPLACE VIEW {qualified} AS "
                f"SELECT * FROM {_quote_identifier(registration)}"
            )
        self._connection.execute(query)

    def _register_as_of_macros(self) -> None:
        for table in _TUSHARE_MARKET_TABLES:
            self._create_macro(
                "tushare",
                f"{table}_as_of",
                "as_of_date",
                f'SELECT * FROM "tushare".{_quote_identifier(table)} '
                "WHERE trade_date <= CAST(as_of_date AS DATE)",
            )

        for table in _TUSHARE_STATEMENT_TABLES:
            self._create_macro(
                "tushare",
                f"{table}_as_of",
                "as_of_date",
                f'SELECT * FROM "tushare".{_quote_identifier(table)} '
                "WHERE coalesce(f_ann_date, ann_date) <= CAST(as_of_date AS DATE) "
                "QUALIFY row_number() OVER ("
                "PARTITION BY ts_code, end_date, report_type, comp_type "
                "ORDER BY coalesce(f_ann_date, ann_date) DESC NULLS LAST, "
                "try_cast(update_flag AS INTEGER) DESC NULLS LAST"
                ") = 1",
            )

        for table in _TUSHARE_ANNOUNCEMENT_TABLES:
            update_order = (
                ", try_cast(update_flag AS INTEGER) DESC NULLS LAST"
                if "update_flag" in TABLE_SCHEMAS[table].names
                else ""
            )
            self._create_macro(
                "tushare",
                f"{table}_as_of",
                "as_of_date",
                f'SELECT * FROM "tushare".{_quote_identifier(table)} '
                "WHERE ann_date <= CAST(as_of_date AS DATE) "
                "QUALIFY row_number() OVER ("
                "PARTITION BY ts_code, end_date "
                f"ORDER BY ann_date DESC NULLS LAST{update_order}"
                ") = 1",
            )

        self._create_macro(
            "tushare",
            "dividend_as_of",
            "as_of_date",
            'SELECT * FROM "tushare"."dividend" '
            "WHERE coalesce(imp_ann_date, ann_date) <= CAST(as_of_date AS DATE)",
        )
        self._create_macro(
            "tushare",
            "sw_industry_as_of",
            "as_of_date",
            "SELECT * REPLACE (NULL::DATE AS out_date, NULL::VARCHAR AS is_new) "
            'FROM "tushare"."sw_industry" '
            "WHERE in_date <= CAST(as_of_date AS DATE) "
            "AND (out_date IS NULL OR out_date > CAST(as_of_date AS DATE))",
        )
        self._create_macro(
            "tushare",
            "trade_cal_as_of",
            "as_of_date",
            'SELECT * FROM "tushare"."trade_cal" WHERE cal_date <= CAST(as_of_date AS DATE)',
        )

        for table in (TICK_TABLE, BAR_TABLE):
            self._create_macro(
                "qmt",
                f"{table}_as_of",
                "as_of_us",
                f'SELECT * FROM "qmt".{_quote_identifier(table)} '
                "WHERE received_at <= CAST(as_of_us AS BIGINT)",
            )

    def _create_macro(self, schema: str, name: str, parameter: str, query: str) -> None:
        self._connection.execute(
            f"CREATE OR REPLACE MACRO {_quote_identifier(schema)}.{_quote_identifier(name)}"
            f"({parameter}) AS TABLE ({query})"
        )


def _active_files(table_root: Path) -> list[Path]:
    """仅返回每个分区 Manifest 当前引用的文件。"""
    files: list[Path] = []
    for manifest_path in sorted(table_root.rglob("_manifest.json")):
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"无法读取 Parquet Manifest: {manifest_path}") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"Parquet Manifest 格式无效: {manifest_path}")
        names = payload.get("files")
        if (
            not isinstance(names, list)
            or any(
                not isinstance(name, str)
                or Path(name).name != name
                or not name.endswith(".parquet")
                for name in names
            )
            or len(names) != len(set(names))
        ):
            raise ValueError(f"Parquet Manifest 格式无效: {manifest_path}")
        committed = payload.get("file_committed_at")
        if committed is not None and (
            not isinstance(committed, dict)
            or set(committed) != set(names)
            or any(
                not isinstance(value, int) or isinstance(value, bool) or value < 1
                for value in committed.values()
            )
        ):
            raise ValueError(f"Parquet Manifest 格式无效: {manifest_path}")
        if isinstance(committed, dict):
            commit_times = committed
            names.sort(key=lambda name: commit_times[name])
        for name in names:
            path = manifest_path.parent / name
            if not path.is_file():
                raise ValueError(f"Manifest 引用的 Parquet 文件不存在: {path}")
            files.append(path)
    return files


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
