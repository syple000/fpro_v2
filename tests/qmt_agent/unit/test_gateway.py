from __future__ import annotations

import sys
from threading import Lock
from types import ModuleType
from typing import Any, cast

import pytest

from qmt_agent.gateway import (
    MarketQuotePush,
    QmtGatewayError,
    StockQuotePush,
    XtDataGateway,
)
from qmt_protocol import HistoryFrame


class FakeXtData:
    def __init__(self) -> None:
        self.arguments: dict[str, Any] = {}
        self.method: str | None = None
        self.history: dict[str, Any] = {
            "000001.SZ": {
                "index": [20250101],
                "columns": ["close"],
                "data": [[10.0]],
            }
        }
        self.financial: dict[str, Any] = {
            "000001.SZ": {
                "Balance": {
                    "index": [20241231],
                    "columns": ["m_anntime", "m_timetag", "tot_assets"],
                    "data": [[20250331, 20241231, 100.0]],
                }
            }
        }
        self.dividend_factors: dict[str, Any] = {
            "index": [20240601],
            "columns": ["interest", "dr"],
            "data": [[0.1, 0.99]],
        }

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
        return self.history

    def download_financial_data2(self, **kwargs: Any) -> None:
        self.method = "download_financial_data2"
        self.arguments = kwargs

    def get_financial_data(self, **kwargs: Any) -> dict[str, Any]:
        self.method = "get_financial_data"
        self.arguments = kwargs
        return self.financial

    def get_divid_factors(self, **kwargs: Any) -> dict[str, Any]:
        self.method = "get_divid_factors"
        self.arguments = kwargs
        return self.dividend_factors


def make_gateway(xtdata: object) -> XtDataGateway:
    gateway = object.__new__(XtDataGateway)
    gateway._xtdata = cast(Any, xtdata)
    gateway._call_lock = Lock()
    return gateway


def ignore_market_quotes(_: MarketQuotePush) -> None:
    pass


def ignore_stock_quotes(_: StockQuotePush) -> None:
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

    assert result == {"000001.SZ": HistoryFrame(index=[20250101], columns=["close"], data=[[10.0]])}
    assert xtdata.arguments["stock_list"] == ["000001.SZ"]
    assert xtdata.arguments["field_list"] == ["close"]


def test_history_time_column_is_normalised_to_microseconds() -> None:
    xtdata = FakeXtData()
    xtdata.history = {
        "000001.SZ": {
            "index": [20250101],
            "columns": ["time", "close"],
            "data": [[1_735_689_600_000, 10.0]],
        }
    }
    gateway = make_gateway(xtdata)

    result = gateway.get_history(
        ["000001.SZ"],
        ["time", "close"],
        "1d",
        "20250101",
        "20251231",
        -1,
        "none",
        True,
    )

    assert result["000001.SZ"].data == [[1_735_689_600_000_000, 10.0]]


def test_market_subscription_uses_whole_quote() -> None:
    xtdata = FakeXtData()
    gateway = make_gateway(xtdata)

    subscription_id = gateway.subscribe_market_quote("SH", ignore_market_quotes)

    assert subscription_id == 11
    assert xtdata.method == "subscribe_whole_quote"
    assert xtdata.arguments["code_list"] == ["SH"]
    assert callable(xtdata.arguments["callback"])


def test_stock_subscription_uses_period_and_realtime_only_count() -> None:
    xtdata = FakeXtData()
    gateway = make_gateway(xtdata)

    subscription_id = gateway.subscribe_stock_quote("000001.SZ", "1m", ignore_stock_quotes)

    assert subscription_id == 12
    assert xtdata.method == "subscribe_quote"
    assert xtdata.arguments["stock_code"] == "000001.SZ"
    assert xtdata.arguments["period"] == "1m"
    assert xtdata.arguments["count"] == 0
    assert callable(xtdata.arguments["callback"])


def test_gateway_validates_callback_and_preserves_unknown_quote_fields(
    caplog: pytest.LogCaptureFixture,
) -> None:
    xtdata = FakeXtData()
    gateway = make_gateway(xtdata)
    received: list[MarketQuotePush] = []
    caplog.set_level("DEBUG", logger="qmt_agent.gateway")

    gateway.subscribe_market_quote("SH", received.append)
    callback = xtdata.arguments["callback"]
    assert callable(callback)
    callback(
        {
            "600000.SH": {
                "time": 1786944183000,
                "lastPrice": 9.06,
                "amount": 341471100,
                "vendorField": {"level": 1},
            }
        }
    )

    quote = received[0]["600000.SH"]
    assert quote.amount == 341471100.0
    assert quote.model_dump(exclude_none=True)["vendorField"] == {"level": 1}
    assert "字段不会丢弃" in caplog.text


def test_gateway_logs_raw_callback_when_invalid_data_is_dropped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    xtdata = FakeXtData()
    gateway = make_gateway(xtdata)
    received: list[MarketQuotePush] = []
    caplog.set_level("DEBUG", logger="qmt_agent.gateway")

    gateway.subscribe_market_quote("SH", received.append)
    callback = xtdata.arguments["callback"]
    assert callable(callback)
    callback({"600000.SH": {"volume": "not-an-int"}})

    assert received == []
    assert "被丢弃的 XtData 全市场行情原始数据" in caplog.text


def test_unsubscribe_and_snapshot_use_official_parameter_names() -> None:
    xtdata = FakeXtData()
    gateway = make_gateway(xtdata)

    result = gateway.get_full_tick(["000001.SZ"])
    assert result["000001.SZ"].lastPrice == 10.0
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
        gateway.subscribe_stock_quote("000001.SZ", "1m", ignore_stock_quotes)
    with pytest.raises(QmtGatewayError) as unsubscribe_error:
        gateway.unsubscribe(456)

    assert "-123" not in str(subscribe_error.value)
    assert "456" not in str(unsubscribe_error.value)


def test_history_download_uses_official_batch_interface() -> None:
    xtdata = FakeXtData()
    gateway = make_gateway(xtdata)

    gateway.download_history(["000001.SZ", "600000.SH"], "1d", "20250101", "20251231", False)

    assert xtdata.method == "download_history_data2"
    assert xtdata.arguments == {
        "stock_list": ["000001.SZ", "600000.SH"],
        "period": "1d",
        "start_time": "20250101",
        "end_time": "20251231",
        "incrementally": False,
    }


def test_history_download_supports_legacy_client_without_incremental_argument() -> None:
    class LegacyXtData:
        def __init__(self) -> None:
            self.arguments: dict[str, Any] = {}

        def download_history_data2(
            self,
            stock_list: list[str],
            period: str,
            start_time: str,
            end_time: str,
        ) -> None:
            self.method = "download_history_data2"
            self.arguments = {
                "stock_list": stock_list,
                "period": period,
                "start_time": start_time,
                "end_time": end_time,
            }

    xtdata = LegacyXtData()
    gateway = make_gateway(xtdata)

    gateway.download_history(["000001.SZ"], "1d", "", "", True)

    assert xtdata.arguments == {
        "stock_list": ["000001.SZ"],
        "period": "1d",
        "start_time": "",
        "end_time": "",
    }


def test_financial_download_and_query_use_official_interfaces() -> None:
    xtdata = FakeXtData()
    gateway = make_gateway(xtdata)

    gateway.download_financial(
        ["000001.SZ"], ["Balance"], "20240101", "20251231"
    )
    assert xtdata.method == "download_financial_data2"
    assert xtdata.arguments == {
        "stock_list": ["000001.SZ"],
        "table_list": ["Balance"],
        "start_time": "20240101",
        "end_time": "20251231",
        "callback": None,
    }

    result = gateway.get_financial(
        ["000001.SZ"], ["Balance"], "20240101", "20251231", "announce_time"
    )
    assert result["000001.SZ"]["Balance"].data == [[20250331, 20241231, 100.0]]
    assert xtdata.method == "get_financial_data"
    assert xtdata.arguments["report_type"] == "announce_time"


def test_dividend_factor_query_reads_each_stock() -> None:
    xtdata = FakeXtData()
    gateway = make_gateway(xtdata)

    result = gateway.get_dividend_factors(["000001.SZ"], "20240101", "20241231")

    assert result["000001.SZ"].data == [[0.1, 0.99]]
    assert xtdata.method == "get_divid_factors"
    assert xtdata.arguments == {
        "stock_code": "000001.SZ",
        "start_time": "20240101",
        "end_time": "20241231",
    }
