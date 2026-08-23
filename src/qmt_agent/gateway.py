"""xtdata 的薄边界：串行调用、JSON 化并校验原始返回结构。"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from numbers import Integral
from threading import Lock
from typing import Protocol, TypeAlias, TypeVar, cast

from pydantic import ValidationError

from qmt_agent.serialization import dataframe_records
from qmt_protocol import (
    DividendFactorsResponse,
    DividendType,
    FinancialDownloadResponse,
    FinancialQueryResponse,
    FinancialReportType,
    FinancialTable,
    HistoryDownloadResponse,
    HistoryQueryResponse,
    QuotePayload,
    TickQuote,
    XtDataPeriod,
    quote_model_for_period,
)

logger = logging.getLogger(__name__)

MarketQuotePush: TypeAlias = dict[str, TickQuote]
StockQuotePush: TypeAlias = dict[str, list[QuotePayload]]
MarketQuoteCallback: TypeAlias = Callable[[MarketQuotePush], None]
StockQuoteCallback: TypeAlias = Callable[[StockQuotePush], None]
QuoteCallback: TypeAlias = Callable[[MarketQuotePush | StockQuotePush], None]
QuotePushT = TypeVar("QuotePushT")


class QmtGatewayError(RuntimeError):
    """miniQMT 调用失败，或返回值不符合 XtData 已知结构。"""


class MarketDataGateway(Protocol):
    """service 使用的最小 QMT 接口。"""

    def subscribe_market_quote(self, market: str, callback: MarketQuoteCallback) -> int: ...

    def subscribe_stock_quote(
        self,
        stock: str,
        period: XtDataPeriod,
        callback: StockQuoteCallback,
    ) -> int: ...

    def unsubscribe(self, subscription_id: int) -> None: ...

    def get_full_tick(self, codes: Sequence[str]) -> dict[str, TickQuote]: ...

    def download_history(
        self,
        stocks: Sequence[str],
        period: XtDataPeriod,
        start_time: str,
        end_time: str,
        incrementally: bool,
    ) -> HistoryDownloadResponse: ...

    def get_history(
        self,
        stocks: Sequence[str],
        fields: Sequence[str],
        period: XtDataPeriod,
        start_time: str,
        end_time: str,
        count: int,
        dividend_type: DividendType,
        fill_data: bool,
    ) -> HistoryQueryResponse: ...

    def download_financial(
        self,
        stocks: Sequence[str],
        tables: Sequence[FinancialTable],
        start_time: str,
        end_time: str,
    ) -> FinancialDownloadResponse: ...

    def get_financial(
        self,
        stocks: Sequence[str],
        tables: Sequence[FinancialTable],
        start_time: str,
        end_time: str,
        report_type: FinancialReportType,
    ) -> FinancialQueryResponse: ...

    def get_dividend_factors(
        self,
        stocks: Sequence[str],
        start_time: str,
        end_time: str,
    ) -> DividendFactorsResponse: ...


class _XtDataModule(Protocol):
    """本机 xtquant 没有类型声明；动态结果统一从 ``object`` 校验。"""

    def subscribe_whole_quote(
        self, code_list: list[str], callback: Callable[[object], None]
    ) -> object: ...

    def subscribe_quote(
        self,
        stock_code: str,
        period: str,
        count: int,
        callback: Callable[[object], None],
    ) -> object: ...

    def unsubscribe_quote(self, seq: int) -> None: ...

    def get_full_tick(self, code_list: list[str]) -> object: ...

    def download_history_data2(
        self,
        stock_list: list[str],
        period: str,
        start_time: str,
        end_time: str,
        incrementally: bool,
    ) -> None: ...

    def get_local_data(
        self,
        field_list: list[str],
        stock_list: list[str],
        period: str,
        start_time: str,
        end_time: str,
        count: int,
        dividend_type: str,
        fill_data: bool,
    ) -> object: ...

    def download_financial_data2(
        self,
        stock_list: list[str],
        table_list: list[str],
        start_time: str,
        end_time: str,
        callback: Callable[[object], None] | None,
    ) -> None: ...

    def get_financial_data(
        self,
        stock_list: list[str],
        table_list: list[str],
        start_time: str,
        end_time: str,
        report_type: str,
    ) -> object: ...

    def get_divid_factors(
        self,
        stock_code: str,
        start_time: str,
        end_time: str,
    ) -> object: ...


class XtDataGateway:
    """只处理 xtdata 调用约束，不承载订阅状态或业务转换。"""

    def __init__(self) -> None:
        try:
            from xtquant import xtdata  # pyright: ignore[reportMissingImports]
        except Exception as exc:
            raise QmtGatewayError(
                "无法导入 xtquant.xtdata；请使用 scripts/qmt_agent/start_qmt_agent.cmd 启动。"
                f"原始错误：{exc}"
            ) from exc

        self._xtdata = cast(_XtDataModule, xtdata)
        # xtdata 底层客户端不是可重入接口，所有主动调用共用一把锁。
        self._call_lock = Lock()

    def subscribe_market_quote(self, market: str, callback: MarketQuoteCallback) -> int:
        def receive(raw: object) -> None:
            self._deliver_callback(
                callback,
                lambda: _validate_tick_mapping(raw, f"{market} 全推回调"),
                context=f"{market} 全推回调",
                raw=raw,
            )

        try:
            with self._call_lock:
                subscription_id = self._xtdata.subscribe_whole_quote(
                    code_list=[market], callback=receive
                )
        except Exception as exc:
            raise QmtGatewayError(f"订阅全市场行情失败：{exc}") from exc
        return _subscription_id(subscription_id, "订阅全市场行情失败")

    def subscribe_stock_quote(
        self,
        stock: str,
        period: XtDataPeriod,
        callback: StockQuoteCallback,
    ) -> int:
        def receive(raw: object) -> None:
            self._deliver_callback(
                callback,
                lambda: _validate_stock_mapping(raw, period, f"{stock} {period} 回调"),
                context=f"{stock} {period} 回调",
                raw=raw,
            )

        try:
            with self._call_lock:
                subscription_id = self._xtdata.subscribe_quote(
                    stock_code=stock,
                    period=period,
                    # 只接收订阅建立后的实时数据，避免隐式拉取历史行情。
                    count=0,
                    callback=receive,
                )
        except Exception as exc:
            raise QmtGatewayError(f"订阅 {stock} 的 {period} 行情失败：{exc}") from exc
        return _subscription_id(subscription_id, f"订阅 {stock} 的 {period} 行情失败")

    def unsubscribe(self, subscription_id: int) -> None:
        try:
            with self._call_lock:
                self._xtdata.unsubscribe_quote(seq=subscription_id)
        except Exception as exc:
            raise QmtGatewayError("取消订阅失败") from exc

    def get_full_tick(self, codes: Sequence[str]) -> dict[str, TickQuote]:
        try:
            with self._call_lock:
                raw = self._xtdata.get_full_tick(code_list=list(codes))
            return _validate_tick_mapping({} if raw is None else raw, "get_full_tick")
        except QmtGatewayError:
            raise
        except Exception as exc:
            raise QmtGatewayError(f"获取行情快照失败：{exc}") from exc

    def download_history(
        self,
        stocks: Sequence[str],
        period: XtDataPeriod,
        start_time: str,
        end_time: str,
        incrementally: bool,
    ) -> HistoryDownloadResponse:
        try:
            with self._call_lock:
                arguments = {
                    "stock_list": list(stocks),
                    "period": period,
                    "start_time": start_time,
                    "end_time": end_time,
                }
                try:
                    self._xtdata.download_history_data2(
                        **arguments,
                        incrementally=incrementally,
                    )
                except TypeError as exc:
                    if "unexpected keyword argument 'incrementally'" not in str(exc):
                        raise
                    # 保留对旧券商客户端四参数版本的兼容。
                    cast(Callable[..., None], self._xtdata.download_history_data2)(**arguments)
            return HistoryDownloadResponse(completed=True)
        except Exception as exc:
            raise QmtGatewayError(f"下载历史行情失败：{exc}") from exc

    def get_history(
        self,
        stocks: Sequence[str],
        fields: Sequence[str],
        period: XtDataPeriod,
        start_time: str,
        end_time: str,
        count: int,
        dividend_type: DividendType,
        fill_data: bool,
    ) -> HistoryQueryResponse:
        raw: object = None
        try:
            with self._call_lock:
                raw = self._xtdata.get_local_data(
                    field_list=list(fields),
                    stock_list=list(stocks),
                    period=period,
                    start_time=start_time,
                    end_time=end_time,
                    count=count,
                    dividend_type=dividend_type,
                    fill_data=fill_data,
                )
            frames = _string_mapping({} if raw is None else raw, "get_local_data")
            data = {code: dataframe_records(frame) for code, frame in frames.items()}
            return HistoryQueryResponse.model_validate({"period": period, "data": data})
        except ValidationError as exc:
            logger.debug("不符合协议的 XtData 历史行情：%r", raw)
            raise QmtGatewayError(f"历史行情返回结构不合法：{exc}") from exc
        except Exception as exc:
            raise QmtGatewayError(f"读取历史行情失败：{exc}") from exc

    def download_financial(
        self,
        stocks: Sequence[str],
        tables: Sequence[FinancialTable],
        start_time: str,
        end_time: str,
    ) -> FinancialDownloadResponse:
        try:
            with self._call_lock:
                self._xtdata.download_financial_data2(
                    stock_list=list(stocks),
                    table_list=list(tables),
                    start_time=start_time,
                    end_time=end_time,
                    callback=None,
                )
            return FinancialDownloadResponse(completed=True)
        except Exception as exc:
            raise QmtGatewayError(f"下载财务数据失败：{exc}") from exc

    def get_financial(
        self,
        stocks: Sequence[str],
        tables: Sequence[FinancialTable],
        start_time: str,
        end_time: str,
        report_type: FinancialReportType,
    ) -> FinancialQueryResponse:
        raw: object = None
        try:
            with self._call_lock:
                raw = self._xtdata.get_financial_data(
                    stock_list=list(stocks),
                    table_list=list(tables),
                    start_time=start_time,
                    end_time=end_time,
                    report_type=report_type,
                )
            stocks_data = _string_mapping({} if raw is None else raw, "get_financial_data")
            data: dict[str, dict[str, object]] = {}
            for code, tables_data in stocks_data.items():
                table_mapping = _string_mapping(tables_data, f"get_financial_data/{code}")
                data[code] = {
                    table: dataframe_records(frame) for table, frame in table_mapping.items()
                }
            return FinancialQueryResponse.model_validate({"data": data})
        except ValidationError as exc:
            logger.debug("不符合协议的 XtData 财务数据：%r", raw)
            raise QmtGatewayError(f"财务数据返回结构不合法：{exc}") from exc
        except Exception as exc:
            raise QmtGatewayError(f"读取财务数据失败：{exc}") from exc

    def get_dividend_factors(
        self,
        stocks: Sequence[str],
        start_time: str,
        end_time: str,
    ) -> DividendFactorsResponse:
        raw: dict[str, object] = {}
        try:
            with self._call_lock:
                for stock in stocks:
                    frame = self._xtdata.get_divid_factors(
                        stock_code=stock,
                        start_time=start_time,
                        end_time=end_time,
                    )
                    if frame is not None:
                        raw[stock] = frame
            data = {
                code: dataframe_records(frame, index_name="date") for code, frame in raw.items()
            }
            return DividendFactorsResponse.model_validate({"data": data})
        except ValidationError as exc:
            logger.debug("不符合协议的 XtData 除权数据：%r", raw)
            raise QmtGatewayError(f"除权数据返回结构不合法：{exc}") from exc
        except Exception as exc:
            raise QmtGatewayError(f"读取除权数据失败：{exc}") from exc

    @staticmethod
    def _deliver_callback(
        callback: Callable[[QuotePushT], None],
        validate: Callable[[], QuotePushT],
        *,
        context: str,
        raw: object,
    ) -> None:
        try:
            callback(validate())
        except Exception:
            # 回调运行在 XtData 线程；结构或业务异常不能终止其线程。
            logger.exception("处理 XtData %s失败", context)
            logger.debug("XtData %s原始数据：%r", context, raw)


def _subscription_id(value: object, message: str) -> int:
    if not isinstance(value, Integral) or isinstance(value, bool) or value <= 0:
        raise QmtGatewayError(f"{message}，客户端未返回有效订阅号")
    return int(value)


def _mapping(value: object, context: str) -> Mapping[object, object]:
    if not isinstance(value, Mapping):
        raise QmtGatewayError(f"{context} 顶层必须是代码到行情的映射，实际为 {type(value)}")
    return value


def _string_mapping(value: object, context: str) -> dict[str, object]:
    return {_code(key, context): item for key, item in _mapping(value, context).items()}


def _code(value: object, context: str) -> str:
    if not isinstance(value, str) or not value:
        raise QmtGatewayError(f"{context} 包含无效合约代码：{value!r}")
    return value


def _quote_dict(value: object, context: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise QmtGatewayError(f"{context} 行情必须是字段映射，实际为 {type(value)}")
    result: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise QmtGatewayError(f"{context} 行情字段名必须是字符串：{key!r}")
        result[key] = item
    return result


def _validate_tick_mapping(value: object, context: str) -> MarketQuotePush:
    result: MarketQuotePush = {}
    try:
        for raw_code, raw_quote in _mapping(value, context).items():
            code = _code(raw_code, context)
            result[code] = TickQuote.model_validate(_quote_dict(raw_quote, f"{context}/{code}"))
    except ValidationError as exc:
        raise QmtGatewayError(f"{context} 行情字段类型不合法：{exc}") from exc
    return result


def _validate_stock_mapping(value: object, period: XtDataPeriod, context: str) -> StockQuotePush:
    result: StockQuotePush = {}
    model = quote_model_for_period(period)
    try:
        for raw_code, raw_rows in _mapping(value, context).items():
            code = _code(raw_code, context)
            if not isinstance(raw_rows, (list, tuple)):
                raise QmtGatewayError(f"{context}/{code} 必须是行情列表，实际为 {type(raw_rows)}")
            result[code] = [
                model.model_validate(_quote_dict(row, f"{context}/{code}[{index}]"))
                for index, row in enumerate(raw_rows)
            ]
    except ValidationError as exc:
        raise QmtGatewayError(f"{context} 行情字段类型不合法：{exc}") from exc
    return result
