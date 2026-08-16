from __future__ import annotations

import sys
from threading import Lock
from types import ModuleType
from typing import Any

import pytest

from qmt_agent.gateway import QmtGatewayError, XtDataGateway


class FakeXtData:
    def __init__(self) -> None:
        self.arguments: dict[str, Any] = {}
        self.method: str | None = None

    def subscribe_whole_quote(self, **kwargs: Any) -> int:
        self.method = "subscribe_whole_quote"
        self.arguments = kwargs
        return 11

    def subscribe_quote(self, **kwargs: Any) -> int:
        self.method = "subscribe_quote"
        self.arguments = kwargs
        return 12

    def unsubscribe_quote(self, **kwargs: Any) -> None:
        self.method = "unsubscribe_quote"
        self.arguments = kwargs

    def get_full_tick(self, **kwargs: Any) -> dict[str, Any]:
        self.method = "get_full_tick"
        self.arguments = kwargs
        return {"000001.SZ": {"lastPrice": 10.0}}

    def download_history_data2(self, **kwargs: Any) -> None:
        self.method = "download_history_data2"
        self.arguments = kwargs

    def get_local_data(self, **kwargs: Any) -> dict[str, Any]:
        self.method = "get_local_data"
        self.arguments = kwargs
        return {"close": "data"}


def make_gateway(xtdata: FakeXtData) -> XtDataGateway:
    gateway = object.__new__(XtDataGateway)
    gateway._xtdata = xtdata
    gateway._call_lock = Lock()
    return gateway


def ignore_quotes(_: dict[str, Any]) -> None:
    pass


def test_gateway_uses_one_non_reentrant_lock(monkeypatch) -> None:
    xtquant = ModuleType("xtquant")
    xtquant.xtdata = FakeXtData()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "xtquant", xtquant)

    gateway = XtDataGateway()

    assert gateway._call_lock.acquire(blocking=False)
    assert not gateway._call_lock.acquire(blocking=False)
    gateway._call_lock.release()


def test_history_is_read_locally_without_implicit_subscription() -> None:
    xtdata = FakeXtData()
    gateway = make_gateway(xtdata)

    result = gateway.get_history(
        ["000001.SZ"],
        ["close"],
        "1d",
        "20250101",
        "20251231",
        -1,
        "none",
        True,
    )

    assert result == {"close": "data"}
    assert xtdata.arguments["stock_list"] == ["000001.SZ"]
    assert xtdata.arguments["field_list"] == ["close"]


def test_market_subscription_uses_whole_quote() -> None:
    xtdata = FakeXtData()
    gateway = make_gateway(xtdata)

    subscription_id = gateway.subscribe_market_quote("SH", ignore_quotes)

    assert subscription_id == 11
    assert xtdata.method == "subscribe_whole_quote"
    assert xtdata.arguments == {"code_list": ["SH"], "callback": ignore_quotes}


def test_stock_subscription_uses_period_and_realtime_only_count() -> None:
    xtdata = FakeXtData()
    gateway = make_gateway(xtdata)

    subscription_id = gateway.subscribe_stock_quote("000001.SZ", "1m", ignore_quotes)

    assert subscription_id == 12
    assert xtdata.method == "subscribe_quote"
    assert xtdata.arguments == {
        "stock_code": "000001.SZ",
        "period": "1m",
        "count": 0,
        "callback": ignore_quotes,
    }


def test_unsubscribe_and_snapshot_use_official_parameter_names() -> None:
    xtdata = FakeXtData()
    gateway = make_gateway(xtdata)

    result = gateway.get_full_tick(["000001.SZ"])
    assert result == {"000001.SZ": {"lastPrice": 10.0}}
    assert xtdata.method == "get_full_tick"
    assert xtdata.arguments == {"code_list": ["000001.SZ"]}

    gateway.unsubscribe(12)
    assert xtdata.method == "unsubscribe_quote"
    assert xtdata.arguments == {"seq": 12}


def test_gateway_errors_do_not_expose_subscription_ids() -> None:
    class InvalidSubscriptionXtData(FakeXtData):
        def subscribe_quote(self, **kwargs: Any) -> int:
            return -123

        def unsubscribe_quote(self, **kwargs: Any) -> None:
            raise RuntimeError("客户端调用失败")

    gateway = make_gateway(InvalidSubscriptionXtData())

    with pytest.raises(QmtGatewayError) as subscribe_error:
        gateway.subscribe_stock_quote("000001.SZ", "1m", ignore_quotes)
    with pytest.raises(QmtGatewayError) as unsubscribe_error:
        gateway.unsubscribe(456)

    assert "-123" not in str(subscribe_error.value)
    assert "456" not in str(unsubscribe_error.value)


def test_history_download_uses_official_batch_interface() -> None:
    xtdata = FakeXtData()
    gateway = make_gateway(xtdata)

    gateway.download_history(
        ["000001.SZ", "600000.SH"], "1d", "20250101", "20251231", False
    )

    assert xtdata.method == "download_history_data2"
    assert xtdata.arguments == {
        "stock_list": ["000001.SZ", "600000.SH"],
        "period": "1d",
        "start_time": "20250101",
        "end_time": "20251231",
        "incrementally": False,
    }
