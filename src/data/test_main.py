"""调用统一 Reader 的全部公共查询方法，并把结果写入测试目录。"""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.csv as pa_csv

from data import DataCatalog, DataReader, QueryResult, SourceConfig

SHANGHAI = ZoneInfo("Asia/Shanghai")
DEFAULT_OUTPUT_DIR = Path("data/test/data_reader")


@dataclass(frozen=True, slots=True)
class SmokeRunResult:
    """完整 Reader 冒烟运行的内存结果和落盘位置。"""

    queries: Mapping[str, QueryResult]
    previous_session: date | None
    files: Mapping[str, Path]
    errors: Mapping[str, str]
    manifest_path: Path


def run(
    tushare_dir: Path,
    qmt_dir: Path,
    as_of: datetime,
    symbol: str,
    periods: int,
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    lookback_days: int = 365,
    exchange: str = "SSE",
) -> SmokeRunResult:
    """查询全部公共方法，并把 Arrow 结果按稳定名称写成 CSV。"""
    periods = _positive_int(periods, "periods")
    lookback_days = _positive_int(lookback_days, "lookback_days")

    start_date = as_of.astimezone(SHANGHAI).date() - timedelta(days=lookback_days)
    visible_start = datetime.combine(start_date, time.min, tzinfo=SHANGHAI)
    symbols = (symbol,)
    sources = SourceConfig(
        routes={
            "market.daily_bars": "tushare",
            "market.intraday_bars": "qmt",
            "market.realtime_quotes": "qmt",
            "market.daily_metrics": "tushare",
            "market.moneyflow": "tushare",
            "market.suspensions": "tushare",
            "market.price_limits": "tushare",
            "market.st_status": "tushare",
            "fundamentals.income": "tushare",
            "fundamentals.balance_sheet": "tushare",
            "fundamentals.cashflow": "tushare",
            "fundamentals.indicators": "tushare",
            "fundamentals.forecast": "tushare",
            "fundamentals.express": "tushare",
            "fundamentals.audit": "tushare",
            "corporate_actions.dividends": "tushare",
            "corporate_actions.adjustment_factors": "tushare",
            "classification.industry": "tushare",
            "reference.stocks": "tushare",
            "calendar.sessions": "tushare",
        }
    )

    with DataCatalog(tushare_root=tushare_dir, qmt_root=qmt_dir) as catalog:
        data = DataReader(catalog, sources=sources).at(as_of)
        normalized_as_of = data.as_of
        operations: tuple[tuple[str, Callable[[], QueryResult]], ...] = (
            (
                "market.bars.daily",
                lambda: data.market.bars(
                    symbols=symbols,
                    frequency="1d",
                    count=periods,
                ),
            ),
            (
                "market.bars.intraday",
                lambda: data.market.bars(
                    symbols=symbols,
                    frequency="1m",
                    count=periods,
                ),
            ),
            ("market.current", lambda: data.market.current(symbols=symbols)),
            ("market.status", lambda: data.market.status(symbols=symbols)),
            (
                "market.daily_metrics",
                lambda: data.market.daily_metrics(
                    symbols=symbols,
                    start=start_date,
                ),
            ),
            (
                "market.moneyflow",
                lambda: data.market.moneyflow(
                    symbols=symbols,
                    start=start_date,
                ),
            ),
            (
                "fundamentals.statements.income",
                lambda: data.fundamentals.statements(
                    kind="income",
                    symbols=symbols,
                    periods=periods,
                ),
            ),
            (
                "fundamentals.statements.balance_sheet",
                lambda: data.fundamentals.statements(
                    kind="balance_sheet",
                    symbols=symbols,
                    periods=periods,
                ),
            ),
            (
                "fundamentals.statements.cash_flow",
                lambda: data.fundamentals.statements(
                    kind="cash_flow",
                    symbols=symbols,
                    periods=periods,
                ),
            ),
            (
                "fundamentals.indicators",
                lambda: data.fundamentals.indicators(
                    symbols=symbols,
                    periods=periods,
                ),
            ),
            (
                "fundamentals.disclosures.forecast",
                lambda: data.fundamentals.disclosures(
                    kind="forecast",
                    symbols=symbols,
                    visible_start=visible_start,
                ),
            ),
            (
                "fundamentals.disclosures.express",
                lambda: data.fundamentals.disclosures(
                    kind="express",
                    symbols=symbols,
                    visible_start=visible_start,
                ),
            ),
            (
                "fundamentals.disclosures.audit",
                lambda: data.fundamentals.disclosures(
                    kind="audit",
                    symbols=symbols,
                    visible_start=visible_start,
                ),
            ),
            (
                "corporate_actions.dividends",
                lambda: data.corporate_actions.dividends(
                    symbols=symbols,
                    visible_start=visible_start,
                ),
            ),
            (
                "corporate_actions.adjustment_factors",
                lambda: data.corporate_actions.adjustment_factors(
                    symbols=symbols,
                    start=start_date,
                ),
            ),
            (
                "classification.industry",
                lambda: data.classification.industry(symbols=symbols),
            ),
            ("reference.stocks", lambda: data.reference.stocks()),
            (
                "calendar.sessions",
                lambda: data.calendar.sessions(
                    start=start_date,
                    exchange=exchange,
                ),
            ),
        )
        queries: dict[str, QueryResult] = {}
        errors: dict[str, str] = {}
        for name, operation in operations:
            try:
                queries[name] = operation()
            except Exception as exc:
                errors[name] = f"{type(exc).__name__}: {exc}"

        previous_session: date | None = None
        try:
            previous_session = data.calendar.previous_session(exchange=exchange)
        except Exception as exc:
            errors["calendar.previous_session"] = f"{type(exc).__name__}: {exc}"

    files, manifest_path = _write_results(
        output_dir=output_dir,
        queries=queries,
        previous_session=previous_session,
        errors=errors,
        as_of=normalized_as_of,
        symbol=symbol,
        periods=periods,
        lookback_days=lookback_days,
        exchange=exchange,
    )
    return SmokeRunResult(
        queries=queries,
        previous_session=previous_session,
        files=files,
        errors=errors,
        manifest_path=manifest_path,
    )


def _write_results(
    *,
    output_dir: Path,
    queries: Mapping[str, QueryResult],
    previous_session: date | None,
    errors: Mapping[str, str],
    as_of: datetime,
    symbol: str,
    periods: int,
    lookback_days: int,
    exchange: str,
) -> tuple[dict[str, Path], Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    files: dict[str, Path] = {}
    datasets: dict[str, dict[str, object]] = {}
    for name in errors:
        stale_path = output_dir / f"{name.replace('.', '__')}.csv"
        stale_path.unlink(missing_ok=True)
        legacy_path = output_dir / f"{name.replace('.', '__')}.parquet"
        legacy_path.unlink(missing_ok=True)
    for name, result in queries.items():
        path = output_dir / f"{name.replace('.', '__')}.csv"
        legacy_path = output_dir / f"{name.replace('.', '__')}.parquet"
        legacy_path.unlink(missing_ok=True)
        pa_csv.write_csv(result.table, path)
        files[name] = path
        datasets[name] = {
            "file": path.name,
            "rows": result.table.num_rows,
            "fields": result.table.schema.names,
            "sources": list(result.sources),
        }

    previous_name = "calendar.previous_session"
    if previous_name not in errors:
        previous_path = output_dir / f"{previous_name.replace('.', '__')}.csv"
        legacy_path = output_dir / f"{previous_name.replace('.', '__')}.parquet"
        legacy_path.unlink(missing_ok=True)
        previous_table = pa.table(
            {
                "exchange": pa.array([exchange], type=pa.string()),
                "previous_session": pa.array([previous_session], type=pa.date32()),
            }
        )
        pa_csv.write_csv(previous_table, previous_path)
        files[previous_name] = previous_path
        datasets[previous_name] = {
            "file": previous_path.name,
            "rows": previous_table.num_rows,
            "fields": previous_table.schema.names,
            "sources": ["tushare"],
        }

    manifest_path = output_dir / "manifest.json"
    manifest = {
        "version": 1,
        "as_of": as_of.isoformat(),
        "parameters": {
            "symbol": symbol,
            "periods": periods,
            "lookback_days": lookback_days,
            "exchange": exchange,
        },
        "datasets": datasets,
        "errors": dict(errors),
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return files, manifest_path


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} 必须是正整数")
    return value


def main() -> None:
    now = datetime.now(SHANGHAI)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tushare-dir", type=Path, default=Path("data/tushare"))
    parser.add_argument("--qmt-dir", type=Path, default=Path("data/qmt"))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--as-of",
        type=datetime.fromisoformat,
        default=now,
        help="带时区的 PIT 时间，例如 2024-04-30T16:05:00+08:00",
    )
    parser.add_argument("--symbol", default="000001.SZ")
    parser.add_argument("--periods", type=int, default=10)
    parser.add_argument("--lookback-days", type=int, default=365)
    parser.add_argument("--exchange", default="SSE")
    arguments = parser.parse_args()
    if arguments.periods < 1:
        parser.error("--periods 必须大于等于 1")
    if arguments.lookback_days < 1:
        parser.error("--lookback-days 必须大于等于 1")

    result = run(
        arguments.tushare_dir,
        arguments.qmt_dir,
        arguments.as_of,
        arguments.symbol,
        arguments.periods,
        output_dir=arguments.output_dir,
        lookback_days=arguments.lookback_days,
        exchange=arguments.exchange,
    )
    for name, path in result.files.items():
        rows = result.queries[name].table.num_rows if name in result.queries else 1
        print(f"{name}: rows={rows} file={path}")
    for name, error in result.errors.items():
        print(f"{name}: ERROR {error}")
    print(f"manifest: {result.manifest_path}")
    if result.errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
