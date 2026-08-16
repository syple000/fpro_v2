"""并发和高压力下的状态正确性验证。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from threading import Barrier

import pytest

from qmt_agent.service import QmtMarketService, SubscriptionLimitError
from tests.qmt_agent.fakes import FakeGateway


def stock_code(number: int) -> str:
    return f"{number:06d}.SZ"


@pytest.mark.stress
def test_concurrent_subscriptions_keep_single_consistent_upstream_subscription() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    batches = [
        [stock_code(number) for number in range(start, start + 10)]
        for start in range(0, 300, 10)
    ]
    barrier = Barrier(len(batches))

    def subscribe_batch(batch: list[str]) -> None:
        barrier.wait()
        service.subscribe_stocks(batch)

    with ThreadPoolExecutor(max_workers=len(batches)) as executor:
        futures = [executor.submit(subscribe_batch, batch) for batch in batches]
        for future in futures:
            future.result()

    expected = {stock_code(number) for number in range(300)}
    status = service.status()
    active_subscriptions = gateway.active_codes()

    assert status["stock_count"] == 300
    assert set(status["stocks"]) == expected
    assert len(active_subscriptions) == 1
    assert set(active_subscriptions[0]) == expected


@pytest.mark.stress
def test_concurrent_over_limit_requests_never_break_the_300_stock_invariant() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_stocks([stock_code(number) for number in range(300)])
    barrier = Barrier(32)

    def try_subscribe(number: int) -> bool:
        barrier.wait()
        try:
            service.subscribe_stocks([f"EXTRA{number}.SZ"])
        except SubscriptionLimitError:
            return False
        return True

    with ThreadPoolExecutor(max_workers=32) as executor:
        results = list(executor.map(try_subscribe, range(32)))

    status = service.status()
    active_subscriptions = gateway.active_codes()

    assert results == [False] * 32
    assert status["stock_count"] == 300
    assert len(active_subscriptions) == 1
    assert active_subscriptions[0] == status["stocks"]


@pytest.mark.stress
def test_quote_callbacks_and_reads_remain_consistent_under_pressure() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    stocks = [stock_code(number) for number in range(80)]
    subscription = service.subscribe_stocks(stocks)
    subscription_id = subscription["subscription_id"]
    writer_groups = [stocks[index::8] for index in range(8)]
    barrier = Barrier(16)

    def write_quotes(writer_id: int, writer_stocks: list[str]) -> None:
        barrier.wait()
        for sequence in range(500):
            gateway.push(
                subscription_id,
                {
                    stock: {"writer": writer_id, "sequence": sequence}
                    for stock in writer_stocks
                },
            )

    def read_quotes() -> None:
        barrier.wait()
        expected = set(stocks)
        for _ in range(1_000):
            result = service.get_subscribed_quotes()
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

    final_quotes = service.get_subscribed_quotes()
    assert set(final_quotes["data"]) == set(stocks)
    assert final_quotes["missing"] == []
    assert all(quote["sequence"] == 499 for quote in final_quotes["data"].values())


@pytest.mark.stress
def test_concurrent_hot_switch_with_one_failure_preserves_state_consistency() -> None:
    gateway = FakeGateway()
    service = QmtMarketService(gateway)
    service.subscribe_stocks(["000001.SZ"])
    gateway.fail_next_subscribe = True
    additions = [stock_code(number) for number in range(1, 33)]
    barrier = Barrier(len(additions))

    def add_stock(stock: str) -> None:
        barrier.wait()
        with suppress(RuntimeError):
            service.subscribe_stocks([stock])

    with ThreadPoolExecutor(max_workers=len(additions)) as executor:
        futures = [executor.submit(add_stock, stock) for stock in additions]
        for future in futures:
            future.result()

    status = service.status()
    active_subscriptions = gateway.active_codes()

    assert 1 <= status["stock_count"] <= 33
    assert len(active_subscriptions) == 1
    assert active_subscriptions[0] == status["stocks"]
