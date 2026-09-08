"""持久证券身份与交易代码历史；不根据代码形状推测更码关系。"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from market_data.errors import DataReaderError

CODE_HISTORY_SCHEMA = pa.schema(
    [
        pa.field("sid", pa.int64(), nullable=False),
        pa.field("code", pa.string(), nullable=False),
        pa.field("valid_from", pa.date32(), nullable=False),
        pa.field("valid_to", pa.date32()),
    ]
)


class SecurityMappingError(DataReaderError):
    """代码缺少映射、归属冲突或模拟日期没有有效交易代码。"""


@dataclass(frozen=True, slots=True)
class CodeInterval:
    sid: int
    code: str
    valid_from: date
    valid_to: date | None = None


class SecurityCodeHistory:
    """一次加载的不可变映射快照；sid 必须由数据维护者持久分配。"""

    def __init__(self, intervals: Iterable[CodeInterval]) -> None:
        rows = tuple(intervals)
        if not rows:
            raise SecurityMappingError("证券代码历史表为空")
        by_sid: dict[int, list[CodeInterval]] = defaultdict(list)
        self._by_code: dict[str, int] = {}
        for item in rows:
            if (
                isinstance(item.sid, bool)
                or not isinstance(item.sid, int)
                or not 1 <= item.sid < 2**63
            ):
                raise SecurityMappingError(f"sid 必须是持久分配的正整数: {item.sid!r}")
            if not isinstance(item.code, str) or not item.code or item.code != item.code.strip():
                raise SecurityMappingError(f"无效证券代码: {item.code!r}")
            if type(item.valid_from) is not date or (
                item.valid_to is not None
                and (type(item.valid_to) is not date or item.valid_to <= item.valid_from)
            ):
                raise SecurityMappingError(f"{item.code} 的代码有效期无效")
            previous = self._by_code.setdefault(item.code, item.sid)
            if previous != item.sid:
                raise SecurityMappingError(f"代码归属冲突: {item.code} 对应多个 sid")
            by_sid[item.sid].append(item)
        self._by_sid: dict[int, tuple[CodeInterval, ...]] = {}
        for sid, history in by_sid.items():
            history.sort(key=lambda item: item.valid_from)
            for previous, current in zip(history, history[1:], strict=False):
                if previous.valid_to is None or previous.valid_to > current.valid_from:
                    raise SecurityMappingError(f"sid {sid} 的代码有效期冲突")
                if previous.valid_to < current.valid_from:
                    raise SecurityMappingError(f"sid {sid} 的代码有效期存在空档")
            self._by_sid[sid] = tuple(history)
        snapshot = json.dumps(self.table().to_pylist(), default=str, sort_keys=True)
        self.snapshot_id = hashlib.sha256(snapshot.encode()).hexdigest()

    @classmethod
    def load(cls, path: str | Path) -> SecurityCodeHistory:
        """读取本地 Parquet 一次；后续查询不重新打开映射文件。"""
        try:
            table = pq.read_table(path)
            if table.schema.names != CODE_HISTORY_SCHEMA.names:
                raise SecurityMappingError("代码历史字段必须为 sid/code/valid_from/valid_to")
            if any(
                table.schema.field(field.name).type != field.type for field in CODE_HISTORY_SCHEMA
            ):
                raise SecurityMappingError("代码历史字段类型必须为 int64/string/date32/date32")
            return cls(CodeInterval(**row) for row in table.to_pylist())
        except (OSError, pa.ArrowException) as exc:
            raise SecurityMappingError(f"无法读取证券代码历史: {path}") from exc

    def table(self) -> pa.Table:
        return pa.Table.from_pylist(
            [
                {
                    "sid": sid,
                    "code": item.code,
                    "valid_from": item.valid_from,
                    "valid_to": item.valid_to,
                }
                for sid in sorted(self._by_sid)
                for item in self._by_sid[sid]
            ],
            schema=CODE_HISTORY_SCHEMA,
        )

    def sid(self, code: str) -> int:
        """解析来源代码身份，不用记录日期过滤供应商回标的历史。"""
        try:
            return self._by_code[code]
        except KeyError:
            raise SecurityMappingError(f"证券代码缺少映射: {code}") from None

    def code_at(self, sid: int, session: date) -> str:
        for item in self._by_sid.get(sid, ()):
            if item.valid_from <= session and (item.valid_to is None or session < item.valid_to):
                return item.code
        raise SecurityMappingError(f"sid {sid} 在 {session} 缺少有效交易代码")

    def aliases(self, sid: int) -> tuple[str, ...]:
        if sid not in self._by_sid:
            raise SecurityMappingError(f"缺少 sid {sid} 的代码历史")
        return tuple(dict.fromkeys(item.code for item in self._by_sid[sid]))

    def canonical(self, code: str) -> str:
        """仅供适配层关联使用的固定别名；不可当作历史交易代码展示。"""
        return self.aliases(self.sid(code))[0]

    def canonical_symbols(self, codes: Iterable[str]) -> tuple[str, ...]:
        return tuple(sorted({self.canonical(code) for code in codes}))

    def symbols_at(self, codes: Iterable[str], session: date) -> tuple[str, ...]:
        return tuple(sorted({self.code_at(self.sid(code), session) for code in codes}))
