from __future__ import annotations

import pytest

from qmt_agent.gateway import QmtGatewayError
from qmt_agent.service import QmtMarketService, SubscriptionLimitError
from tests.qmt_agent.fakes import FakeGateway


def test_stock_subscription_can_be_changed_while_running() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)

    first = service.subscribe_stocks(["000001.SZ", "600000.SH"])
    second = service.subscribe_stocks(["000001.SZ", "300001.SZ"])
    removed = service.unsubscribe_stocks(["600000.SH", "999999.SZ"])

    assert first["added"] == ["000001.SZ", "600000.SH"]
    assert second["subscribed"] == ["000001.SZ", "300001.SZ", "600000.SH"]
    assert removed["subscribed"] == ["000001.SZ", "300001.SZ"]
    assert removed["not_found"] == ["999999.SZ"]
    assert gateway.unsubscribed == [1, 2]


def test_subscription_limit_does_not_change_existing_subscription() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway, max_stock_subscriptions=2)
    service.subscribe_stocks(["000001.SZ", "600000.SH"])

    with pytest.raises(SubscriptionLimitError):
        service.subscribe_stocks(["300001.SZ"])

    assert service.status()["stocks"] == ["000001.SZ", "600000.SH"]
    assert gateway.unsubscribed == []


def test_failed_hot_switch_restores_old_subscription() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    original = service.subscribe_stocks(["000001.SZ"])
    gateway.fail_next_subscribe = True

    with pytest.raises(QmtGatewayError, match="已恢复原订阅"):
        service.subscribe_stocks(["600000.SH"])

    assert service.status()["stocks"] == ["000001.SZ"]
    assert original["subscription_id"] in gateway.unsubscribed
    assert list(gateway.active.values())[0][0] == ["000001.SZ"]


def test_callback_keeps_only_latest_quote() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    result = service.subscribe_stocks(["000001.SZ"])

    gateway.push(
        result["subscription_id"],
        {"000001.SZ": [{"lastPrice": 10.0}, {"lastPrice": 10.2}]},
    )

    quotes = service.get_subscribed_quotes()
    assert quotes["data"] == {"000001.SZ": {"lastPrice": 10.2}}
    assert quotes["missing"] == []


def test_market_subscription_does_not_use_stock_quota() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway, max_stock_subscriptions=1)

    service.subscribe_markets(["SH", "SZ"])
    service.subscribe_stocks(["000001.SZ"])

    assert service.status()["markets"] == ["SH", "SZ"]
    assert service.status()["stock_count"] == 1


def test_market_quotes_are_available_from_subscription_cache() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    result = service.subscribe_markets(["SH"])
    gateway.push(
        result["subscription_id"],
        {
            "600000.SH": {"lastPrice": 10.0},
            "000001.SZ": {"lastPrice": 11.0},
        },
    )

    all_quotes = service.get_subscribed_quotes()
    selected_quotes = service.get_subscribed_quotes(["600000.SH", "000001.SZ"])

    assert list(all_quotes["data"]) == ["600000.SH"]
    assert list(selected_quotes["data"]) == ["600000.SH"]
    assert selected_quotes["not_subscribed"] == ["000001.SZ"]
