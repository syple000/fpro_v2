"""Tushare 全市场客户端的明确接口，以及请求频率和并发保护。"""

from __future__ import annotations

import logging
from collections.abc import Callable
from threading import BoundedSemaphore, Lock
from time import monotonic, sleep
from typing import ParamSpec, Protocol, TypeVar, runtime_checkable

import pandas as pd
from requests.exceptions import ChunkedEncodingError
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

DEFAULT_REQUESTS_PER_MINUTE = 120
DEFAULT_MAX_CONCURRENCY = 3
DEFAULT_MAX_RETRIES = 10
DEFAULT_RETRY_BACKOFF_SECONDS = 1.0

logger = logging.getLogger(__name__)

_P = ParamSpec("_P")
_R = TypeVar("_R")


@runtime_checkable
class TushareDataApi(Protocol):
    """Tushare SDK 唯一稳定的底层动态查询入口。"""

    def query(
        self,
        api_name: str,
        fields: str = "",
        **kwargs: object,
    ) -> pd.DataFrame: ...


class RequestExecutor(Protocol):
    def call(
        self,
        function: Callable[_P, _R],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _R: ...


class RequestLimiter:
    """限制全客户端的请求启动频率和同时在途请求数。"""

    def __init__(
        self,
        requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    ) -> None:
        if requests_per_minute < 1:
            raise ValueError("requests_per_minute 必须大于等于 1")
        if max_concurrency < 1:
            raise ValueError("max_concurrency 必须大于等于 1")
        self.requests_per_minute = requests_per_minute
        self.max_concurrency = max_concurrency
        self._interval_seconds = 60.0 / requests_per_minute
        self._next_request_at = 0.0
        self._rate_lock = Lock()
        self._slots = BoundedSemaphore(max_concurrency)

    def call(
        self,
        function: Callable[_P, _R],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _R:
        """等待频率和并发额度，然后在额度内完成一次真实请求。"""
        self._slots.acquire()
        try:
            with self._rate_lock:
                now = monotonic()
                delay = self._next_request_at - now
                if delay > 0:
                    sleep(delay)
                started_at = monotonic()
                self._next_request_at = started_at + self._interval_seconds
            return function(*args, **kwargs)
        finally:
            self._slots.release()


class TushareProClient:
    """可直接调用的全市场客户端；每个业务接口都显式列出完整参数。"""

    def __init__(
        self,
        client: TushareDataApi,
        limiter: RequestExecutor,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries 不能小于 0")
        if retry_backoff_seconds < 0:
            raise ValueError("retry_backoff_seconds 不能小于 0")
        self._client = client
        self.limiter = limiter
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds

    def _query(
        self,
        api_name: str,
        fields: str,
        limit: int,
        offset: int,
        **parameters: str,
    ) -> pd.DataFrame:
        for retry_index in range(self.max_retries + 1):
            try:
                return self.limiter.call(
                    self._client.query,
                    api_name,
                    fields=fields,
                    limit=limit,
                    offset=offset,
                    **parameters,
                )
            except (
                RequestsConnectionError,
                RequestsTimeout,
                ChunkedEncodingError,
                TimeoutError,
            ) as exc:
                if retry_index == self.max_retries:
                    raise
                delay = self.retry_backoff_seconds * (2**retry_index)
                logger.warning(
                    "%s 网络请求失败（%s: %s），%.1f 秒后进行第 %d 次重试",
                    api_name,
                    type(exc).__name__,
                    exc,
                    delay,
                    retry_index + 1,
                )
                sleep(delay)
        raise AssertionError("重试循环不应执行到此处")

    def daily(self, trade_date: str, fields: str, limit: int, offset: int) -> pd.DataFrame:
        return self._query("daily", fields, limit, offset, trade_date=trade_date)

    def daily_basic(self, trade_date: str, fields: str, limit: int, offset: int) -> pd.DataFrame:
        return self._query("daily_basic", fields, limit, offset, trade_date=trade_date)

    def adj_factor(self, trade_date: str, fields: str, limit: int, offset: int) -> pd.DataFrame:
        return self._query("adj_factor", fields, limit, offset, trade_date=trade_date)

    def suspend_d(self, trade_date: str, fields: str, limit: int, offset: int) -> pd.DataFrame:
        return self._query("suspend_d", fields, limit, offset, trade_date=trade_date)

    def stk_limit(self, trade_date: str, fields: str, limit: int, offset: int) -> pd.DataFrame:
        return self._query("stk_limit", fields, limit, offset, trade_date=trade_date)

    def stock_st(self, trade_date: str, fields: str, limit: int, offset: int) -> pd.DataFrame:
        return self._query("stock_st", fields, limit, offset, trade_date=trade_date)

    def moneyflow(self, trade_date: str, fields: str, limit: int, offset: int) -> pd.DataFrame:
        return self._query("moneyflow", fields, limit, offset, trade_date=trade_date)

    def forecast_vip(
        self,
        start_date: str,
        end_date: str,
        fields: str,
        limit: int,
        offset: int,
    ) -> pd.DataFrame:
        return self._query(
            "forecast_vip",
            fields,
            limit,
            offset,
            start_date=start_date,
            end_date=end_date,
        )

    def express_vip(
        self,
        start_date: str,
        end_date: str,
        fields: str,
        limit: int,
        offset: int,
    ) -> pd.DataFrame:
        return self._query(
            "express_vip",
            fields,
            limit,
            offset,
            start_date=start_date,
            end_date=end_date,
        )

    def fina_indicator_vip(
        self,
        start_date: str,
        end_date: str,
        fields: str,
        limit: int,
        offset: int,
    ) -> pd.DataFrame:
        return self._query(
            "fina_indicator_vip",
            fields,
            limit,
            offset,
            start_date=start_date,
            end_date=end_date,
        )

    def income_vip(
        self,
        start_date: str,
        end_date: str,
        fields: str,
        limit: int,
        offset: int,
    ) -> pd.DataFrame:
        return self._query(
            "income_vip",
            fields,
            limit,
            offset,
            start_date=start_date,
            end_date=end_date,
        )

    def balancesheet_vip(
        self,
        start_date: str,
        end_date: str,
        fields: str,
        limit: int,
        offset: int,
    ) -> pd.DataFrame:
        return self._query(
            "balancesheet_vip",
            fields,
            limit,
            offset,
            start_date=start_date,
            end_date=end_date,
        )

    def cashflow_vip(
        self,
        start_date: str,
        end_date: str,
        fields: str,
        limit: int,
        offset: int,
    ) -> pd.DataFrame:
        return self._query(
            "cashflow_vip",
            fields,
            limit,
            offset,
            start_date=start_date,
            end_date=end_date,
        )

    def dividend(
        self,
        ann_date: str | None,
        imp_ann_date: str | None,
        fields: str,
        limit: int,
        offset: int,
    ) -> pd.DataFrame:
        parameters: dict[str, str] = {}
        if ann_date is not None:
            parameters["ann_date"] = ann_date
        if imp_ann_date is not None:
            parameters["imp_ann_date"] = imp_ann_date
        return self._query("dividend", fields, limit, offset, **parameters)

    def index_member_all(self, is_new: str, fields: str, limit: int, offset: int) -> pd.DataFrame:
        return self._query("index_member_all", fields, limit, offset, is_new=is_new)

    def stock_basic(self, list_status: str, fields: str, limit: int, offset: int) -> pd.DataFrame:
        return self._query("stock_basic", fields, limit, offset, list_status=list_status)

    def fina_audit(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        fields: str,
        limit: int,
        offset: int,
    ) -> pd.DataFrame:
        return self._query(
            "fina_audit",
            fields,
            limit,
            offset,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )

    def trade_cal(
        self,
        exchange: str,
        start_date: str,
        end_date: str,
        fields: str,
        limit: int,
        offset: int,
    ) -> pd.DataFrame:
        return self._query(
            "trade_cal",
            fields,
            limit,
            offset,
            exchange=exchange,
            start_date=start_date,
            end_date=end_date,
        )


def require_tushare_data_api(value: object) -> TushareDataApi:
    """在唯一的第三方 SDK 边界确认动态对象具有底层 query 方法。"""
    if not isinstance(value, TushareDataApi):
        raise TypeError("tushare.pro_api 返回对象缺少 DataApi.query 方法")
    return value
