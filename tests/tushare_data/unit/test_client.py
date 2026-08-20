from __future__ import annotations

import inspect
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from time import sleep
from typing import ParamSpec, TypeVar

import pandas as pd
import pytest
from requests.exceptions import ChunkedEncodingError

from tushare_data.client import (
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_REQUESTS_PER_MINUTE,
    RequestLimiter,
    TushareProClient,
)

_P = ParamSpec("_P")
_R = TypeVar("_R")


class RecordingLimiter:
    def __init__(self) -> None:
        self.calls: list[tuple[object, tuple[object, ...], dict[str, object]]] = []

    def call(
        self,
        function: Callable[_P, _R],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _R:
        self.calls.append((function, args, kwargs))
        return function(*args, **kwargs)


class FakeDataApi:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str, dict[str, object]]] = []

    def query(
        self,
        api_name: str,
        fields: str = "",
        **kwargs: object,
    ) -> pd.DataFrame:
        self.requests.append((api_name, fields, kwargs))
        if api_name != "daily":
            return pd.DataFrame(columns=pd.Index(fields.split(",")))
        return pd.DataFrame(
            [{"ts_code": "000001.SZ", "trade_date": kwargs["trade_date"]}],
            columns=pd.Index(fields.split(",")),
        )


class OneTimeoutDataApi(FakeDataApi):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    def query(
        self,
        api_name: str,
        fields: str = "",
        **kwargs: object,
    ) -> pd.DataFrame:
        self.attempts += 1
        if self.attempts == 1:
            raise TimeoutError("temporary timeout")
        return super().query(api_name, fields, **kwargs)


class OneTruncatedResponseDataApi(FakeDataApi):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    def query(
        self,
        api_name: str,
        fields: str = "",
        **kwargs: object,
    ) -> pd.DataFrame:
        self.attempts += 1
        if self.attempts == 1:
            raise ChunkedEncodingError("response body was truncated")
        return super().query(api_name, fields, **kwargs)


def test_client_uses_full_market_parameters_and_one_limiter() -> None:
    limiter = RecordingLimiter()
    api = FakeDataApi()
    client = TushareProClient(api, limiter)

    result = client.daily("20240102", "ts_code,trade_date", 5_000, 0)

    assert result.iloc[0]["ts_code"] == "000001.SZ"
    assert len(limiter.calls) == 1
    assert api.requests == [
        (
            "daily",
            "ts_code,trade_date",
            {"limit": 5_000, "offset": 0, "trade_date": "20240102"},
        )
    ]


@pytest.mark.parametrize(
    ("business_method", "parameter_names"),
    [
        (TushareProClient.daily, ("trade_date", "fields", "limit", "offset")),
        (TushareProClient.daily_basic, ("trade_date", "fields", "limit", "offset")),
        (TushareProClient.adj_factor, ("trade_date", "fields", "limit", "offset")),
        (TushareProClient.suspend_d, ("trade_date", "fields", "limit", "offset")),
        (TushareProClient.stk_limit, ("trade_date", "fields", "limit", "offset")),
        (TushareProClient.stock_st, ("trade_date", "fields", "limit", "offset")),
        (TushareProClient.moneyflow, ("trade_date", "fields", "limit", "offset")),
        (
            TushareProClient.forecast_vip,
            ("start_date", "end_date", "fields", "limit", "offset"),
        ),
        (
            TushareProClient.express_vip,
            ("start_date", "end_date", "fields", "limit", "offset"),
        ),
        (
            TushareProClient.fina_indicator_vip,
            ("start_date", "end_date", "fields", "limit", "offset"),
        ),
        (
            TushareProClient.income_vip,
            ("start_date", "end_date", "fields", "limit", "offset"),
        ),
        (
            TushareProClient.balancesheet_vip,
            ("start_date", "end_date", "fields", "limit", "offset"),
        ),
        (
            TushareProClient.cashflow_vip,
            ("start_date", "end_date", "fields", "limit", "offset"),
        ),
        (
            TushareProClient.dividend,
            ("ann_date", "imp_ann_date", "fields", "limit", "offset"),
        ),
        (TushareProClient.index_member_all, ("is_new", "fields", "limit", "offset")),
        (TushareProClient.stock_basic, ("list_status", "fields", "limit", "offset")),
        (
            TushareProClient.fina_audit,
            ("ts_code", "start_date", "end_date", "fields", "limit", "offset"),
        ),
        (
            TushareProClient.trade_cal,
            ("exchange", "start_date", "end_date", "fields", "limit", "offset"),
        ),
    ],
)
def test_business_methods_have_only_explicit_parameters(
    business_method: Callable[..., pd.DataFrame],
    parameter_names: tuple[str, ...],
) -> None:
    parameters = tuple(inspect.signature(business_method).parameters.values())

    assert tuple(parameter.name for parameter in parameters) == ("self", *parameter_names)
    assert all(
        parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD for parameter in parameters
    )


def test_client_retries_transient_network_failure_through_limiter() -> None:
    limiter = RecordingLimiter()
    api = OneTimeoutDataApi()
    client = TushareProClient(
        api,
        limiter,
        max_retries=1,
        retry_backoff_seconds=0,
    )

    result = client.daily(
        trade_date="20240102",
        fields="ts_code,trade_date",
        limit=5_000,
        offset=0,
    )

    assert result.iloc[0]["trade_date"] == "20240102"
    assert api.attempts == len(limiter.calls) == 2


def test_client_retries_truncated_http_response_through_limiter() -> None:
    limiter = RecordingLimiter()
    api = OneTruncatedResponseDataApi()
    client = TushareProClient(
        api,
        limiter,
        max_retries=1,
        retry_backoff_seconds=0,
    )

    result = client.daily(
        trade_date="20240102",
        fields="ts_code,trade_date",
        limit=5_000,
        offset=0,
    )

    assert result.iloc[0]["trade_date"] == "20240102"
    assert api.attempts == len(limiter.calls) == 2


def test_client_routes_vip_financial_api_explicitly() -> None:
    limiter = RecordingLimiter()
    api = FakeDataApi()
    client = TushareProClient(api, limiter)

    client.income_vip(
        start_date="20240401",
        end_date="20240430",
        fields="ts_code,ann_date,f_ann_date",
        limit=5_000,
        offset=5_000,
    )

    assert api.requests == [
        (
            "income_vip",
            "ts_code,ann_date,f_ann_date",
            {
                "limit": 5_000,
                "offset": 5_000,
                "start_date": "20240401",
                "end_date": "20240430",
            },
        )
    ]


def test_request_limiter_uses_conservative_quicksync_defaults() -> None:
    limiter = RequestLimiter()

    assert limiter.requests_per_minute == DEFAULT_REQUESTS_PER_MINUTE == 120
    assert limiter.max_concurrency == DEFAULT_MAX_CONCURRENCY == 1


@pytest.mark.parametrize(
    ("requests_per_minute", "max_concurrency"),
    [(0, 1), (120, 0)],
)
def test_request_limiter_rejects_invalid_limits(
    requests_per_minute: int,
    max_concurrency: int,
) -> None:
    with pytest.raises(ValueError):
        RequestLimiter(requests_per_minute, max_concurrency)


def test_request_limiter_caps_simultaneous_in_flight_calls() -> None:
    limiter = RequestLimiter(requests_per_minute=60_000, max_concurrency=1)
    state_lock = Lock()
    active = 0
    maximum_active = 0

    def request() -> None:
        nonlocal active, maximum_active
        with state_lock:
            active += 1
            maximum_active = max(maximum_active, active)
        sleep(0.01)
        with state_lock:
            active -= 1

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(lambda _: limiter.call(request), range(4)))

    assert maximum_active == 1
