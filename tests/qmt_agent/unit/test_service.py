from __future__ import annotations

import pytest

from qmt_agent.gateway import QmtGatewayError, QuoteCallback
from qmt_agent.quote_sequence import QuoteSequenceOutOfRangeError
from qmt_agent.service import (
    QmtMarketService,
    SubscriptionLimitError,
    SubscriptionPeriodConflictError,
)
from tests.qmt_agent.fakes import FakeGateway


def test_stock_subscriptions_are_added_and_removed_individually() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)

    first = service.subscribe_stocks(["000001.SZ", "600000.SH"], "tick")
    second = service.subscribe_stocks(["000001.SZ", "300001.SZ"], "tick")
    removed = service.unsubscribe_stocks(["600000.SH", "999999.SZ"], "tick")

    assert first["added"] == ["000001.SZ", "600000.SH"]
    assert second["subscribed"] == ["000001.SZ", "300001.SZ", "600000.SH"]
    assert removed["subscribed"] == ["000001.SZ", "300001.SZ"]
    assert removed["not_found"] == ["999999.SZ"]
    assert gateway.unsubscribed == [2]
    assert gateway.active_stock_periods() == {
        "000001.SZ": "tick",
        "300001.SZ": "tick",
    }


def test_subscription_limit_does_not_change_existing_subscription() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway, max_stock_subscriptions=2)
    service.subscribe_stocks(["000001.SZ", "600000.SH"], "tick")

    with pytest.raises(SubscriptionLimitError):
        service.subscribe_stocks(["300001.SZ"], "tick")

    assert service.status()["stocks"] == ["000001.SZ", "600000.SH"]
    assert gateway.unsubscribed == []


def test_period_conflict_does_not_cancel_existing_subscription() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_stocks(["000001.SZ"], "1m")
    subscription_id = gateway.active_subscription_ids()["000001.SZ"]

    with pytest.raises(SubscriptionPeriodConflictError, match="显式取消"):
        service.subscribe_stocks(["000001.SZ"], "5m")

    assert service.status()["stocks"] == ["000001.SZ"]
    assert service.status()["stock_periods"] == {"000001.SZ": "1m"}
    assert subscription_id not in gateway.unsubscribed
    assert gateway.active_stock_periods() == {"000001.SZ": "1m"}


def test_callback_keeps_only_latest_quote() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_stocks(["000001.SZ"], "tick")

    gateway.push(
        gateway.active_subscription_ids()["000001.SZ"],
        {"000001.SZ": [{"lastPrice": 10.0}, {"lastPrice": 10.2}]},
    )

    quotes = service.get_stock_quotes()
    assert quotes["data"] == {"000001.SZ": {"lastPrice": 10.2}}
    assert quotes["missing"] == []


def test_single_subscription_callback_preserves_every_quote_in_sequence() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_stocks(["000001.SZ"], "1m")

    gateway.push(
        gateway.active_subscription_ids()["000001.SZ"],
        {
            "000001.SZ": [
                {"close": 10.0},
                {"close": 10.1},
                {"close": 10.2},
            ]
        },
    )

    result = service.get_subscribed_quote_sequence(1, 10)

    assert [item["seq"] for item in result["data"]] == [1, 2, 3]
    assert [item["quote"]["close"] for item in result["data"]] == [10.0, 10.1, 10.2]
    assert all(item["period"] == "1m" for item in result["data"])
    assert all(item["subscription"] == "000001.SZ" for item in result["data"])
    assert result["next_seq"] == 4
    assert service.get_stock_quotes()["data"]["000001.SZ"] == {"close": 10.2}


def test_quote_sequence_is_global_and_callback_ordered() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_markets(["SH"])
    service.subscribe_stocks(["000001.SZ"], "tick")
    subscription_ids = gateway.active_subscription_ids()

    gateway.push(
        subscription_ids["SH"],
        {
            "600000.SH": {"lastPrice": 10.0},
            "601000.SH": {"lastPrice": 11.0},
        },
    )
    gateway.push(
        subscription_ids["000001.SZ"],
        {"000001.SZ": {"lastPrice": 12.0}},
    )

    result = service.get_subscribed_quote_sequence(1, 3)

    assert [(item["seq"], item["code"]) for item in result["data"]] == [
        (1, "600000.SH"),
        (2, "601000.SH"),
        (3, "000001.SZ"),
    ]
    assert [item["source"] for item in result["data"]] == [
        "market",
        "market",
        "stock",
    ]


def test_quote_sequence_ring_reports_oldest_and_latest_boundaries() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway, quote_buffer_capacity=3)
    service.subscribe_stocks(["000001.SZ"], "tick")
    subscription_id = gateway.active_subscription_ids()["000001.SZ"]

    for value in range(1, 6):
        gateway.push(subscription_id, {"000001.SZ": {"value": value}})

    status = service.quote_sequence_status()
    result = service.get_subscribed_quote_sequence(3, 10)

    assert status == {
        "oldest_seq": 3,
        "latest_seq": 5,
        "next_seq": 6,
        "size": 3,
        "capacity": 3,
    }
    assert [item["quote"]["value"] for item in result["data"]] == [3, 4, 5]
    with pytest.raises(QuoteSequenceOutOfRangeError) as too_old:
        service.get_subscribed_quote_sequence(2, 10)
    with pytest.raises(QuoteSequenceOutOfRangeError) as too_new:
        service.get_subscribed_quote_sequence(6, 10)

    assert (too_old.value.oldest_seq, too_old.value.latest_seq) == (3, 5)
    assert "过旧" in str(too_old.value)
    assert (too_new.value.oldest_seq, too_new.value.latest_seq) == (3, 5)
    assert "过新" in str(too_new.value)


def test_quote_sequence_filter_advances_over_the_full_sequence_window() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_markets(["SH"])
    subscription_id = gateway.active_subscription_ids()["SH"]
    gateway.push(
        subscription_id,
        {
            "600000.SH": {"value": 1},
            "601000.SH": {"value": 2},
            "600000.SH ": {"value": 3},
        },
    )

    result = service.get_subscribed_quote_sequence(1, 2, ["601000.SH"])

    assert [(item["seq"], item["code"]) for item in result["data"]] == [
        (2, "601000.SH")
    ]
    assert result["next_seq"] == 3
    assert result["latest_seq"] == 3


def test_market_subscription_does_not_use_stock_quota() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway, max_stock_subscriptions=1)

    service.subscribe_markets(["SH", "SZ"])
    service.subscribe_stocks(["000001.SZ"], "tick")

    assert service.status()["markets"] == ["SH", "SZ"]
    assert service.status()["stock_count"] == 1


def test_stock_period_changes_only_after_precise_unsubscribe() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_stocks(["000001.SZ", "600000.SH"], "1m")
    original_subscription_id = gateway.active_subscription_ids()["000001.SZ"]
    gateway.push(
        original_subscription_id,
        {"000001.SZ": [{"close": 10.0}]},
    )

    mismatch = service.unsubscribe_stocks(["000001.SZ"], "5m")
    removed = service.unsubscribe_stocks(["000001.SZ"], "1m")
    changed = service.subscribe_stocks(["000001.SZ"], "5m")

    assert mismatch["removed"] == []
    assert mismatch["period_mismatches"] == {"000001.SZ": "1m"}
    assert removed["removed"] == ["000001.SZ"]
    assert changed["added"] == ["000001.SZ"]
    assert changed["periods"] == {"000001.SZ": "5m", "600000.SH": "1m"}
    assert gateway.unsubscribed == [original_subscription_id]
    assert gateway.active_stock_periods() == {"000001.SZ": "5m", "600000.SH": "1m"}
    assert service.get_stock_quotes(["000001.SZ"])["missing"] == ["000001.SZ"]


def test_market_quotes_are_available_from_subscription_cache() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_markets(["SH"])
    gateway.push(
        gateway.active_subscription_ids()["SH"],
        {
            "600000.SH": {"lastPrice": 10.0},
            "000001.SZ": {"lastPrice": 11.0},
        },
    )

    all_quotes = service.get_market_quotes()
    selected_quotes = service.get_market_quotes(["600000.SH", "000001.SZ"])

    assert list(all_quotes["data"]) == ["600000.SH"]
    assert list(selected_quotes["data"]) == ["600000.SH"]
    assert selected_quotes["not_subscribed"] == ["000001.SZ"]


def test_market_and_stock_quotes_are_returned_from_separate_caches() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_markets(["SH"])
    service.subscribe_stocks(["600000.SH"], "1m")
    subscription_ids = gateway.active_subscription_ids()

    gateway.push(
        subscription_ids["600000.SH"],
        {"600000.SH": [{"close": 10.1, "kind": "1m"}]},
    )
    gateway.push(
        subscription_ids["SH"],
        {"600000.SH": {"lastPrice": 10.2, "kind": "tick"}},
    )

    market_quotes = service.get_market_quotes(["600000.SH"])
    stock_quotes = service.get_stock_quotes(["600000.SH"])
    compatibility_quotes = service.get_subscribed_quotes(["600000.SH"])

    assert market_quotes["data"] == {
        "600000.SH": {"lastPrice": 10.2, "kind": "tick"}
    }
    assert market_quotes["periods"] == {"600000.SH": "tick"}
    assert stock_quotes["data"] == {
        "600000.SH": {"close": 10.1, "kind": "1m"}
    }
    assert stock_quotes["periods"] == {"600000.SH": "1m"}
    assert compatibility_quotes["data"] == stock_quotes["data"]


def test_adding_or_removing_market_never_replaces_other_market_subscription() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_markets(["SH"])
    sh_subscription_id = gateway.active_subscription_ids()["SH"]

    added = service.subscribe_markets(["SH", "SZ"])
    current_subscription_ids = gateway.active_subscription_ids()
    removed = service.unsubscribe_markets(["SZ"])

    assert added["added"] == ["SZ"]
    assert current_subscription_ids["SH"] == sh_subscription_id
    assert removed["subscribed"] == ["SH"]
    assert gateway.unsubscribed == [current_subscription_ids["SZ"]]
    assert sh_subscription_id in gateway.active


def test_failed_addition_never_cancels_existing_subscriptions() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_markets(["SH"])
    service.subscribe_stocks(["000001.SZ"], "1m")
    existing_ids = set(gateway.active_subscription_ids().values())
    gateway.fail_next_subscribe = True

    with pytest.raises(QmtGatewayError):
        service.subscribe_markets(["SZ"])

    gateway.fail_next_subscribe = True
    with pytest.raises(QmtGatewayError):
        service.subscribe_stocks(["600000.SH"], "1m")

    assert set(gateway.active) == existing_ids
    assert gateway.unsubscribed == []
    assert service.status()["markets"] == ["SH"]
    assert service.status()["stocks"] == ["000001.SZ"]


def test_callback_during_subscribe_is_delivered_after_subscription_succeeds() -> None:
    class ImmediateCallbackGateway(FakeGateway):
        def subscribe_stock_quote(
            self,
            stock: str,
            period: str,
            callback: QuoteCallback,
        ) -> int:
            subscription_id = super().subscribe_stock_quote(stock, period, callback)
            callback({stock: [{"close": 10.5}]})
            return subscription_id

    service = QmtMarketService(ImmediateCallbackGateway())

    service.subscribe_stocks(["000001.SZ"], "1m")

    assert service.get_stock_quotes()["data"] == {
        "000001.SZ": {"close": 10.5}
    }
    assert service.quote_sequence_status()["latest_seq"] == 1


def test_callback_from_failed_subscription_never_enters_quote_caches() -> None:
    class CallbackThenFailGateway(FakeGateway):
        def subscribe_market_quote(
            self,
            market: str,
            callback: QuoteCallback,
        ) -> int:
            callback({"600000.SH": {"lastPrice": 10.5}})
            raise QmtGatewayError("模拟返回订阅号前失败")

    service = QmtMarketService(CallbackThenFailGateway())

    with pytest.raises(QmtGatewayError):
        service.subscribe_markets(["SH"])

    assert service.get_market_quotes()["data"] == {}
    assert service.quote_sequence_status()["size"] == 0


def test_delayed_callback_from_old_subscription_is_ignored() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_stocks(["000001.SZ"], "1m")
    old_subscription_id = gateway.active_subscription_ids()["000001.SZ"]
    old_callback = gateway.active[old_subscription_id][2]

    service.unsubscribe_stocks(["000001.SZ"], "1m")
    service.subscribe_stocks(["000001.SZ"], "5m")
    old_callback({"000001.SZ": [{"close": 9.0}]})
    gateway.push(
        gateway.active_subscription_ids()["000001.SZ"],
        {"000001.SZ": [{"close": 11.0}]},
    )

    assert service.get_stock_quotes()["data"] == {
        "000001.SZ": {"close": 11.0}
    }
    sequence = service.get_subscribed_quote_sequence(1, 10)
    assert [item["quote"]["close"] for item in sequence["data"]] == [11.0]
    assert sequence["data"][0]["period"] == "5m"
