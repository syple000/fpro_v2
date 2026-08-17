from __future__ import annotations

from qmt_agent.subscription_callback import QuoteCallbackGate


def test_gate_preserves_callbacks_received_before_subscription_confirmation() -> None:
    received: list[int] = []
    gate = QuoteCallbackGate(lambda quotes, _: received.append(quotes["000001.SZ"]["value"]))

    gate({"000001.SZ": {"value": 1}})
    gate({"000001.SZ": {"value": 2}})
    assert received == []

    gate.activate()

    assert received == [1, 2]


def test_gate_discards_failed_or_closed_subscription_callbacks() -> None:
    received: list[int] = []
    gate = QuoteCallbackGate(lambda quotes, _: received.append(quotes["000001.SZ"]["value"]))
    gate({"000001.SZ": {"value": 1}})

    gate.close()
    gate.activate()
    gate({"000001.SZ": {"value": 2}})

    assert received == []


def test_gate_can_restore_callbacks_when_unsubscribe_fails() -> None:
    received: list[int] = []
    gate = QuoteCallbackGate(lambda quotes, _: received.append(quotes["000001.SZ"]["value"]))
    gate.activate()
    gate.suspend()
    gate({"000001.SZ": {"value": 1}})

    gate.activate()

    assert received == [1]
