from __future__ import annotations

from threading import RLock
from typing import Any

from qmt_agent.gateway import XtDataGateway


class FakeXtData:
    def __init__(self) -> None:
        self.arguments: dict[str, Any] = {}

    def get_local_data(self, **kwargs: Any) -> dict[str, Any]:
        self.arguments = kwargs
        return {"close": "data"}


def test_history_is_read_locally_without_implicit_subscription() -> None:
    xtdata = FakeXtData()
    gateway = object.__new__(XtDataGateway)
    gateway._xtdata = xtdata
    gateway._call_lock = RLock()

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
