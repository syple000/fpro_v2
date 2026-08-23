from __future__ import annotations

import os

from fpro_common import disable_environment_proxies
from fpro_common.network import PROXY_ENVIRONMENT_VARIABLES


def test_disable_environment_proxies_forces_direct_connections(monkeypatch) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:7890")
    monkeypatch.setenv("https_proxy", "http://127.0.0.1:7890")
    monkeypatch.setenv("ALL_PROXY", "socks5://127.0.0.1:7890")
    monkeypatch.setenv("FtP_PrOxY", "http://127.0.0.1:7890")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    monkeypatch.setenv("QMT_AGENT_PORT", "8765")

    disable_environment_proxies()

    assert not any(
        name != "NO_PROXY" and name.casefold() in PROXY_ENVIRONMENT_VARIABLES
        for name in os.environ
    )
    assert os.environ["NO_PROXY"] == "*"
    assert os.environ["QMT_AGENT_PORT"] == "8765"
