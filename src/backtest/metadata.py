"""在回放开始前保存代码、策略参数和实际加载的数据版本。"""

from __future__ import annotations

import hashlib
import inspect
import json
import platform
import subprocess
from dataclasses import asdict, is_dataclass
from datetime import UTC, date, datetime, time
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from backtest.strategy import Strategy
from market_data import DataReader


def capture_run_metadata(reader: DataReader, strategy: Strategy) -> dict[str, Any]:
    """记录启动时的实际状态，避免策略执行后参数或数据目录已经改变。"""
    strategy_type = type(strategy)
    source = inspect.getsourcefile(strategy_type)
    source_path = Path(source).resolve() if source else None
    identities = getattr(reader, "identities", None)
    metadata = {
        "schema_version": 1,
        "captured_at": datetime.now(UTC),
        "code": _code_metadata(),
        "strategy": {
            "type": f"{strategy_type.__module__}.{strategy_type.__qualname__}",
            "parameters": strategy.parameters(),
            "schedule": strategy.schedule,
            "source_path": source_path,
            "source_sha256": (
                hashlib.sha256(source_path.read_bytes()).hexdigest()
                if source_path is not None and source_path.is_file() else None
            ),
        },
        "data": reader.snapshot_metadata(),
        "security_code_history": (
            {"snapshot_id": identities.snapshot_id, "rows": identities.table().to_pylist()}
            if identities is not None else None
        ),
    }
    # 此时转换也会复制可变参数，并在引擎启动前报告无法序列化的自定义配置。
    return json.loads(json.dumps(metadata, default=_json_default, allow_nan=False))


def _code_metadata() -> dict[str, Any]:
    root = Path(__file__).resolve().parents[2]
    dependencies = {}
    for name in ("fpro-v2", "duckdb", "pandas", "pyarrow"):
        try:
            dependencies[name] = version(name)
        except PackageNotFoundError:
            dependencies[name] = None
    try:
        commit = _git(root, "rev-parse", "HEAD").strip()
        status = _git(root, "status", "--porcelain")
        diff = _git(root, "diff", "--binary", "HEAD")
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        commit, status, diff = None, None, None
    return {
        "git_commit": commit,
        "git_dirty": bool(status) if status is not None else None,
        "git_diff_sha256": hashlib.sha256(diff.encode()).hexdigest() if diff else None,
        "python": platform.python_version(),
        "dependencies": dependencies,
    }


def _git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *arguments],
        capture_output=True, text=True, check=True, timeout=10,
    ).stdout


def _json_default(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, date | datetime | time):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(
        f"运行元数据不支持 {type(value).__name__}；请在 parameters() 中返回可序列化参数"
    )
