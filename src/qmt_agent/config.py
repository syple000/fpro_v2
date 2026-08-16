"""服务配置。"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    """只保留运行服务必需的配置，全部支持环境变量覆盖。"""

    host: str = "127.0.0.1"
    port: int = 8765
    log_level: str = "info"
    max_stock_subscriptions: int = 300

    @classmethod
    def from_env(cls) -> Settings:
        port = _read_int("QMT_AGENT_PORT", 8765)
        max_subscriptions = _read_int("QMT_AGENT_MAX_SUBSCRIPTIONS", 300)

        if not 1 <= port <= 65535:
            raise ValueError("QMT_AGENT_PORT 必须在 1 到 65535 之间")
        if not 1 <= max_subscriptions <= 300:
            raise ValueError("QMT_AGENT_MAX_SUBSCRIPTIONS 必须在 1 到 300 之间")

        return cls(
            host=os.getenv("QMT_AGENT_HOST", "127.0.0.1"),
            port=port,
            log_level=os.getenv("QMT_AGENT_LOG_LEVEL", "info").lower(),
            max_stock_subscriptions=max_subscriptions,
        )


def _read_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} 必须是整数") from exc
