"""对 xtdata 的强类型边界，业务层不接触客户端原始对象。"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from numbers import Integral
from threading import Lock
from typing import Protocol, TypeAlias, cast

from pydantic import JsonValue, ValidationError

from qmt_agent.serialization import to_jsonable
from qmt_protocol import (
    BarQuote,
    DividendType,
    HistoryFrame,
    HistoryQueryResponse,
    QuotePayload,
    TickQuote,
    XtDataPeriod,
    quote_model_for_period,
    unix_timestamp_to_utc_us,
)

logger = logging.getLogger(__name__)

MarketQuotePush: TypeAlias = dict[str, TickQuote]
StockQuotePush: TypeAlias = dict[str, list[QuotePayload]]
MarketQuoteCallback: TypeAlias = Callable[[MarketQuotePush], None]
StockQuoteCallback: TypeAlias = Callable[[StockQuotePush], None]
# 仅保留给自定义网关实现做联合标注；生产协议方法使用上面两个精确回调类型。
QuoteCallback: TypeAlias = Callable[[MarketQuotePush | StockQuotePush], None]


class QmtGatewayError(RuntimeError):
    """miniQMT 调用失败，或返回值不符合已定义的数据结构。"""


class MarketDataGateway(Protocol):
    """业务层使用的强类型最小行情接口。"""

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
    ) -> None: ...

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
    ) -> dict[str, HistoryFrame]: ...


class _XtDataModule(Protocol):
    """本机 xtquant 没有类型声明；所有返回先按 object 接收再校验。"""

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


class XtDataGateway:
    """串行调用 xtdata，并在最底层把动态返回值转换成协议模型。"""

    def __init__(self) -> None:
        try:
            # xtquant 由 Windows 客户端提供，不属于项目可安装依赖。
            from xtquant import xtdata  # pyright: ignore[reportMissingImports]
        except Exception as exc:
            raise QmtGatewayError(
                "无法导入 xtquant.xtdata；请使用 scripts/qmt_agent/start_qmt_agent.cmd 启动。"
                f"原始错误：{exc}"
            ) from exc

        self._xtdata = cast(_XtDataModule, xtdata)
        self._call_lock = Lock()

    def subscribe_market_quote(self, market: str, callback: MarketQuoteCallback) -> int:
        def receive(raw: object) -> None:
            try:
                callback(_validate_tick_mapping(raw, f"{market} 全推回调"))
            except Exception:
                # XtData 在自己的线程中执行回调。结构错误会丢弃本次完整回调，
                # 所以必须同时留下异常和 DEBUG 级原始数据，不能静默跳过字段。
                logger.exception("丢弃结构不合法的 XtData 全市场行情回调：市场=%s", market)
                logger.debug("被丢弃的 XtData 全市场行情原始数据：%r", raw)

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
            try:
                callback(_validate_stock_mapping(raw, period, f"{stock} {period} 回调"))
            except Exception:
                # 与全推一致，不能让业务异常终止 XtData 回调线程。
                logger.exception(
                    "丢弃结构不合法的 XtData 单股行情回调：合约=%s，周期=%s",
                    stock,
                    period,
                )
                logger.debug("被丢弃的 XtData 单股行情原始数据：%r", raw)

        try:
            with self._call_lock:
                # 官方建议只订阅实时数据时使用 count=0，避免额外请求历史数据。
                subscription_id = self._xtdata.subscribe_quote(
                    stock_code=stock,
                    period=period,
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
        except Exception as exc:
            raise QmtGatewayError(f"获取行情快照失败：{exc}") from exc
        return _validate_tick_mapping({} if raw is None else raw, "get_full_tick")

    def download_history(
        self,
        stocks: Sequence[str],
        period: XtDataPeriod,
        start_time: str,
        end_time: str,
        incrementally: bool,
    ) -> None:
        """使用官方批量接口同步补充历史行情。"""
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
                    # 部分券商内置的旧版 xtquant 没有 incrementally 参数。
                    if "unexpected keyword argument 'incrementally'" not in str(exc):
                        raise
                    # 类型协议描述当前版本；这里显式兼容旧版本的四参数调用。
                    legacy_download = cast(Callable[..., None], self._xtdata.download_history_data2)
                    legacy_download(**arguments)
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
    ) -> dict[str, HistoryFrame]:
        raw: object = None
        try:
            with self._call_lock:
                # 历史数据已经由下载接口落盘，本地读取更快，也不会产生隐式订阅。
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
            json_data = to_jsonable({} if raw is None else raw)
            frames = HistoryQueryResponse.model_validate({"data": json_data}).data
            return {
                code: _normalise_history_time(frame, code)
                for code, frame in frames.items()
            }
        except ValidationError as exc:
            logger.debug("被拒绝的 XtData 历史行情原始数据：%r", raw)
            raise QmtGatewayError(f"历史行情返回结构不合法：{exc}") from exc
        except QmtGatewayError:
            raise
        except Exception as exc:
            raise QmtGatewayError(f"读取历史行情失败：{exc}") from exc


def _normalise_history_time(frame: HistoryFrame, code: str) -> HistoryFrame:
    """把历史 DataFrame 中明确名为 time 的 XtData 时间列统一成微秒。"""
    try:
        time_index = frame.columns.index("time")
    except ValueError:
        return frame

    rows: list[list[JsonValue]] = []
    for row_index, source_row in enumerate(frame.data):
        row = list(source_row)
        value = row[time_index]
        if value is not None:
            converted = unix_timestamp_to_utc_us(value)
            if converted is None:
                raise QmtGatewayError(
                    f"{code} 历史行情第 {row_index} 行 time 不是有效整数时间戳"
                )
            row[time_index] = converted
        rows.append(row)
    return frame.model_copy(update={"data": rows})


def _subscription_id(value: object, message: str) -> int:
    if not isinstance(value, Integral) or isinstance(value, bool) or value <= 0:
        raise QmtGatewayError(f"{message}，客户端未返回有效订阅号")
    return int(value)


def _mapping(value: object, context: str) -> Mapping[object, object]:
    if not isinstance(value, Mapping):
        raise QmtGatewayError(f"{context} 顶层必须是代码到行情的映射，实际为 {type(value)}")
    return value


def _code(value: object, context: str) -> str:
    if not isinstance(value, str):
        raise QmtGatewayError(f"{context} 的合约代码必须是字符串，实际为 {type(value)}")
    normalized = value.strip().upper()
    if not normalized:
        raise QmtGatewayError(f"{context} 包含空合约代码")
    return normalized


def _quote_dict(value: object, context: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise QmtGatewayError(f"{context} 行情必须是字段映射，实际为 {type(value)}")
    result: dict[str, object] = {}
    for raw_key, item in value.items():
        if not isinstance(raw_key, str):
            raise QmtGatewayError(f"{context} 行情字段名必须是字符串：{raw_key!r}")
        result[raw_key] = item
    return result


def _log_unknown_fields(
    fields: set[str], model: type[TickQuote] | type[BarQuote], context: str
) -> None:
    unknown = fields - set(model.model_fields)
    if unknown:
        # 未知字段仍在 QuoteModel.__pydantic_extra__ 中完整保留，此日志用于推动补充定义。
        logger.debug(
            "XtData %s 出现尚未定义的行情字段，字段不会丢弃并将原样传递：%s",
            context,
            sorted(unknown),
        )


def _validate_tick_mapping(value: object, context: str) -> MarketQuotePush:
    result: MarketQuotePush = {}
    observed_fields: set[str] = set()
    try:
        for raw_code, raw_quote in _mapping(value, context).items():
            code = _code(raw_code, context)
            quote = _quote_dict(raw_quote, f"{context}/{code}")
            observed_fields.update(quote)
            result[code] = TickQuote.model_validate(quote)
    except ValidationError as exc:
        raise QmtGatewayError(f"{context} 行情字段类型不合法：{exc}") from exc
    _log_unknown_fields(observed_fields, TickQuote, context)
    return result


def _validate_stock_mapping(value: object, period: XtDataPeriod, context: str) -> StockQuotePush:
    result: StockQuotePush = {}
    observed_fields: set[str] = set()
    model = quote_model_for_period(period)
    try:
        for raw_code, raw_rows in _mapping(value, context).items():
            code = _code(raw_code, context)
            if not isinstance(raw_rows, (list, tuple)):
                raise QmtGatewayError(f"{context}/{code} 必须是行情列表，实际为 {type(raw_rows)}")
            rows: list[QuotePayload] = []
            for index, raw_quote in enumerate(raw_rows):
                quote = _quote_dict(raw_quote, f"{context}/{code}[{index}]")
                observed_fields.update(quote)
                rows.append(model.model_validate(quote))
            result[code] = rows
    except ValidationError as exc:
        raise QmtGatewayError(f"{context} 行情字段类型不合法：{exc}") from exc
    _log_unknown_fields(observed_fields, model, context)
    return result
