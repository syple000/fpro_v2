"""并发和高压力下的状态正确性验证。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from threading import Barrier, Event

import pytest

from qmt_agent.service import QmtMarketService, SubscriptionLimitError
from tests.qmt_agent.fakes import FakeGateway


def stock_code(number: int) -> str:
    return f"{number:06d}.SZ"


@pytest.mark.stress
def test_concurrent_subscriptions_keep_consistent_per_stock_subscriptions() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    batches = [
        [stock_code(number) for number in range(start, start + 10)]
        for start in range(0, 50, 10)
    ]
    barrier = Barrier(len(batches))

    def subscribe_batch(batch: list[str]) -> None:
        barrier.wait()
        service.subscribe_stocks(batch, "tick")

    with ThreadPoolExecutor(max_workers=len(batches)) as executor:
        futures = [executor.submit(subscribe_batch, batch) for batch in batches]
        for future in futures:
            future.result()

    expected = {stock_code(number) for number in range(50)}
    status = service.status()
    active_subscriptions = gateway.active_codes()

    assert status["stock_count"] == 50
    assert set(status["stocks"]) == expected
    assert len(active_subscriptions) == 50
    assert {codes[0] for codes in active_subscriptions} == expected
    assert gateway.active_stock_periods() == dict.fromkeys(expected, "tick")


@pytest.mark.stress
def test_concurrent_over_limit_requests_never_break_the_50_stock_invariant() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_stocks([stock_code(number) for number in range(50)], "tick")
    barrier = Barrier(32)

    def try_subscribe(number: int) -> bool:
        barrier.wait()
        try:
            service.subscribe_stocks([f"EXTRA{number}.SZ"], "tick")
        except SubscriptionLimitError:
            return False
        return True

    with ThreadPoolExecutor(max_workers=32) as executor:
        results = list(executor.map(try_subscribe, range(32)))

    status = service.status()
    active_subscriptions = gateway.active_codes()

    assert results == [False] * 32
    assert status["stock_count"] == 50
    assert len(active_subscriptions) == 50
    assert {codes[0] for codes in active_subscriptions} == set(status["stocks"])


@pytest.mark.stress
def test_quote_callbacks_and_reads_remain_consistent_under_pressure() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    stocks = [stock_code(number) for number in range(40)]
    service.subscribe_stocks(stocks, "tick")
    subscription_ids = gateway.active_subscription_ids()
    writer_groups = [stocks[index::8] for index in range(8)]
    barrier = Barrier(16)

    def write_quotes(writer_id: int, writer_stocks: list[str]) -> None:
        barrier.wait()
        for sequence in range(500):
            for stock in writer_stocks:
                gateway.push(
                    subscription_ids[stock],
                    {stock: {"writer": writer_id, "sequence": sequence}},
                )

    def read_quotes() -> None:
        barrier.wait()
        expected = set(stocks)
        for _ in range(1_000):
            result = service.get_stock_quotes()
            assert set(result["data"]) <= expected
            assert set(result["missing"]) <= expected
            assert not result["not_subscribed"]
            for quote in result["data"].values():
                assert 0 <= quote["sequence"] < 500

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [
            executor.submit(write_quotes, writer_id, writer_stocks)
            for writer_id, writer_stocks in enumerate(writer_groups)
        ]
        futures.extend(executor.submit(read_quotes) for _ in range(8))
        for future in futures:
            future.result()

    final_quotes = service.get_stock_quotes()
    sequence_status = service.quote_sequence_status()
    final_sequence_window = service.get_subscribed_quote_sequence(19_001, 1_000)
    assert set(final_quotes["data"]) == set(stocks)
    assert final_quotes["missing"] == []
    assert all(quote["sequence"] == 499 for quote in final_quotes["data"].values())
    assert sequence_status == {
        "oldest_seq": 10_001,
        "latest_seq": 20_000,
        "next_seq": 20_001,
        "size": 10_000,
        "capacity": 10_000,
    }
    assert [item["seq"] for item in final_sequence_window["data"]] == list(
        range(19_001, 20_001)
    )


@pytest.mark.stress
def test_market_callback_never_blocks_stock_callback_cache_write() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_markets(["SH"])
    service.subscribe_stocks(["000001.SZ"], "tick")
    subscription_ids = gateway.active_subscription_ids()
    market_callback_started = Event()

    class SignalingQuotes(dict[str, object]):
        def items(self):  # type: ignore[no-untyped-def]
            market_callback_started.set()
            return super().items()

    with ThreadPoolExecutor(max_workers=2) as executor:
        with service._market_quote_lock:
            market_future = executor.submit(
                gateway.push,
                subscription_ids["SH"],
                SignalingQuotes({"600000.SH": {"lastPrice": 10.0}}),
            )
            assert market_callback_started.wait(timeout=1)

            stock_future = executor.submit(
                gateway.push,
                subscription_ids["000001.SZ"],
                {"000001.SZ": {"lastPrice": 11.0}},
            )
            stock_future.result(timeout=1)
            assert not market_future.done()

        market_future.result(timeout=1)

    market_quotes = service.get_market_quotes()
    stock_quotes = service.get_stock_quotes()
    assert set(market_quotes["data"]) == {"600000.SH"}
    assert set(stock_quotes["data"]) == {"000001.SZ"}


@pytest.mark.stress
def test_market_operation_never_blocks_stock_subscription_state() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)

    with ThreadPoolExecutor(max_workers=1) as executor, service._market_operation_lock:
        future = executor.submit(service.subscribe_stocks, ["000001.SZ"], "tick")
        result = future.result(timeout=1)

    assert result["added"] == ["000001.SZ"]


@pytest.mark.stress
def test_concurrent_hot_switch_with_one_failure_preserves_state_consistency() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_stocks(["000001.SZ"], "tick")
    gateway.fail_next_subscribe = True
    additions = [stock_code(number) for number in range(1, 33)]
    barrier = Barrier(len(additions))

    def add_stock(stock: str) -> None:
        barrier.wait()
        with suppress(RuntimeError):
            service.subscribe_stocks([stock], "tick")

    with ThreadPoolExecutor(max_workers=len(additions)) as executor:
        futures = [executor.submit(add_stock, stock) for stock in additions]
        for future in futures:
            future.result()

    status = service.status()
    active_subscriptions = gateway.active_codes()

    assert 1 <= status["stock_count"] <= 33
    assert len(active_subscriptions) == status["stock_count"]
    assert {codes[0] for codes in active_subscriptions} == set(status["stocks"])
