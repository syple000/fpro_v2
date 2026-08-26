"""逻辑数据集到数据来源的不可变路由。"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from hashlib import sha256
from types import MappingProxyType

from models import DataCapability

KNOWN_ROUTES = frozenset(item.value for item in DataCapability)


@dataclass(frozen=True, slots=True)
class SourceConfig:
    """一个运行期固定使用的逻辑数据来源配置。"""

    routes: Mapping[str, str]
    source_config_id: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.routes, Mapping):
            raise TypeError("routes 必须是映射")
        normalized: dict[str, str] = {}
        for route, source_id in self.routes.items():
            if not isinstance(route, str) or route not in KNOWN_ROUTES:
                raise ValueError(f"未知逻辑数据集路由: {route!r}")
            if not isinstance(source_id, str) or not source_id.strip():
                raise ValueError(f"路由 {route!r} 的 source_id 不能为空")
            normalized[route] = source_id.strip()
        normalized = dict(sorted(normalized.items()))
        encoded = json.dumps(
            normalized,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        object.__setattr__(self, "routes", MappingProxyType(normalized))
        object.__setattr__(self, "source_config_id", sha256(encoded).hexdigest()[:24])
