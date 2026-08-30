"""数据来源适配器的最小公共协议。"""

from __future__ import annotations

from collections.abc import Collection
from typing import Protocol

from models import DataCapability


class DataAdapter(Protocol):
    """声明逻辑能力并按能力实现对应显式方法的数据来源。"""

    @property
    def capabilities(self) -> Collection[DataCapability]:
        """返回该来源支持的逻辑数据集能力。"""
        ...
