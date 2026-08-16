"""对 xtdata 的薄封装，业务层不直接依赖东北证券客户端。"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from numbers import Integral
from threading import Lock
from typing import Any, Protocol

QuoteCallback = Callable[[dict[str, Any]], None]


class QmtGatewayError(RuntimeError):
    """miniQMT 调用失败。"""


class MarketDataGateway(Protocol):
    """业务层使用的最小行情接口。"""

    def subscribe_market_quote(
        self, market: str, callback: QuoteCallback
    ) -> int: ...

    def subscribe_stock_quote(
        self, stock: str, period: str, callback: QuoteCallback
    ) -> int: ...

    def unsubscribe(self, subscription_id: int) -> None: ...

    def get_full_tick(self, codes: Sequence[str]) -> dict[str, Any]: ...

    def download_history(
        self,
        stocks: Sequence[str],
        period: str,
        start_time: str,
        end_time: str,
        incrementally: bool,
    ) -> None: ...

    def get_history(
        self,
        stocks: Sequence[str],
        fields: Sequence[str],
        period: str,
        start_time: str,
        end_time: str,
        count: int,
        dividend_type: str,
        fill_data: bool,
    ) -> Any: ...


class XtDataGateway:
    """串行调用 xtdata，避免不同 HTTP 请求同时进入客户端接口。"""

    def __init__(self) -> None:
        try:
            # xtquant 由 Windows 客户端提供，不属于项目可安装依赖。
            from xtquant import xtdata  # pyright: ignore[reportMissingImports]
        except Exception as exc:
            raise QmtGatewayError(
                "无法导入 xtquant.xtdata；请使用 scripts/qmt_agent/start_qmt_agent.cmd 启动。"
                f"原始错误：{exc}"
            ) from exc

        self._xtdata = xtdata
        self._call_lock = Lock()

    def subscribe_market_quote(
        self, market: str, callback: QuoteCallback
    ) -> int:
        try:
            with self._call_lock:
                subscription_id = self._xtdata.subscribe_whole_quote(
                    code_list=[market], callback=callback
                )
        except Exception as exc:
            raise QmtGatewayError(f"订阅全市场行情失败：{exc}") from exc

        if not isinstance(subscription_id, Integral) or subscription_id <= 0:
            raise QmtGatewayError("订阅全市场行情失败，客户端未返回有效订阅号")
        return int(subscription_id)

    def subscribe_stock_quote(
        self, stock: str, period: str, callback: QuoteCallback
    ) -> int:
        try:
            with self._call_lock:
                # 官方建议只订阅实时数据时使用 count=0，避免额外请求历史数据。
                subscription_id = self._xtdata.subscribe_quote(
                    stock_code=stock,
                    period=period,
                    count=0,
                    callback=callback,
                )
        except Exception as exc:
            raise QmtGatewayError(f"订阅 {stock} 的 {period} 行情失败：{exc}") from exc

        if not isinstance(subscription_id, Integral) or subscription_id <= 0:
            raise QmtGatewayError(
                f"订阅 {stock} 的 {period} 行情失败，客户端未返回有效订阅号"
            )
        return int(subscription_id)

    def unsubscribe(self, subscription_id: int) -> None:
        try:
            with self._call_lock:
                self._xtdata.unsubscribe_quote(seq=subscription_id)
        except Exception as exc:
            raise QmtGatewayError(f"取消订阅失败：{exc}") from exc

    def get_full_tick(self, codes: Sequence[str]) -> dict[str, Any]:
        try:
            with self._call_lock:
                result = self._xtdata.get_full_tick(code_list=list(codes))
        except Exception as exc:
            raise QmtGatewayError(f"获取行情快照失败：{exc}") from exc
        return result or {}

    def download_history(
        self,
        stocks: Sequence[str],
        period: str,
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
                    self._xtdata.download_history_data2(**arguments)
        except Exception as exc:
            raise QmtGatewayError(f"下载历史行情失败：{exc}") from exc

    def get_history(
        self,
        stocks: Sequence[str],
        fields: Sequence[str],
        period: str,
        start_time: str,
        end_time: str,
        count: int,
        dividend_type: str,
        fill_data: bool,
    ) -> Any:
        try:
            with self._call_lock:
                # 历史数据已经由下载接口落盘，本地读取更快，也不会产生隐式订阅。
                return self._xtdata.get_local_data(
                    field_list=list(fields),
                    stock_list=list(stocks),
                    period=period,
                    start_time=start_time,
                    end_time=end_time,
                    count=count,
                    dividend_type=dividend_type,
                    fill_data=fill_data,
                )
        except Exception as exc:
            raise QmtGatewayError(f"读取历史行情失败：{exc}") from exc
