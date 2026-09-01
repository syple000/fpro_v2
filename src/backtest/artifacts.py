"""运行指纹、Parquet 明细和 HTML 报告落盘。"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from backtest.config import BacktestConfig, RunOptions
from backtest.engine import BacktestResult
from backtest.errors import ArtifactError
from backtest.report import render_report

USED_TUSHARE_DATASETS = (
    "daily",
    "dividend",
    "stk_limit",
    "stock_basic",
    "stock_st",
    "suspend_d",
    "trade_cal",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _read_manifest(
    root: Path,
    manifest_path: Path,
    *,
    hash_files: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactError(f"无法读取 Manifest: {manifest_path}") from exc
    names = payload.get("files") if isinstance(payload, dict) else None
    if not isinstance(names, list) or any(not isinstance(name, str) for name in names):
        raise ArtifactError(f"Manifest 格式无效: {manifest_path}")
    manifest = {
        "path": manifest_path.relative_to(root).as_posix(),
        "size": manifest_path.stat().st_size,
        "sha256": _sha256(manifest_path),
    }
    files: list[dict[str, Any]] = []
    for name in names:
        data_path = manifest_path.parent / name
        if not data_path.is_file():
            raise ArtifactError(f"Manifest 引用文件不存在: {data_path}")
        row = {
            "path": data_path.relative_to(root).as_posix(),
            "size": data_path.stat().st_size,
        }
        if hash_files:
            row["sha256"] = _sha256(data_path)
        files.append(row)
    return manifest, files


def build_data_snapshot(root: Path, *, hash_files: bool = False) -> dict[str, Any]:
    """默认绑定 Manifest；审计模式才读取并哈希全部数据文件。"""

    files: list[dict[str, Any]] = []
    manifests: list[dict[str, Any]] = []
    for dataset in USED_TUSHARE_DATASETS:
        for path in sorted((root / dataset).rglob("_manifest.json")):
            manifest, referenced = _read_manifest(root, path, hash_files=hash_files)
            manifests.append(manifest)
            files.extend(referenced)
    digest = hashlib.sha256()
    for row in manifests:
        digest.update(f"{row['path']}:{row['sha256']}\n".encode())
    for row in files:
        digest.update(f"{row['path']}:{row['size']}\n".encode())
    content_digest = None
    if hash_files:
        content = hashlib.sha256()
        for row in files:
            content.update(f"{row['path']}:{row['sha256']}\n".encode())
        content_digest = content.hexdigest()
    return {
        "snapshot_id": digest.hexdigest(),
        "content_snapshot_id": content_digest,
        "source": "tushare",
        "root": str(root),
        "datasets": list(USED_TUSHARE_DATASETS),
        "manifest_count": len(manifests),
        "file_count": len(files),
        "total_bytes": sum(row["size"] for row in files),
        "manifests": manifests,
        "files": files,
        "retention": "content_hash_only" if hash_files else "manifest_reference",
    }


def code_fingerprint(workspace: Path) -> dict[str, Any]:
    paths = [
        *sorted((workspace / "src/backtest").glob("*.py")),
        *sorted((workspace / "src/strategies").glob("*.py")),
        workspace / "uv.lock",
    ]
    rows = [
        {"path": path.relative_to(workspace).as_posix(), "sha256": _sha256(path)}
        for path in paths
        if path.is_file()
    ]
    digest = hashlib.sha256()
    for row in rows:
        digest.update(f"{row['path']}:{row['sha256']}\n".encode())
    return {"sha256": digest.hexdigest(), "files": rows}


def environment_metadata(workspace: Path, code: dict[str, Any]) -> dict[str, Any]:
    def git(*args: str) -> str | None:
        try:
            return subprocess.run(
                ("git", *args),
                cwd=workspace,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            return None

    packages = {}
    for name in ("duckdb", "pandas", "pyarrow"):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = None
    return {
        "python": sys.version,
        "platform": platform.platform(),
        "packages": packages,
        "git_commit": git("rev-parse", "HEAD"),
        "git_dirty": bool(git("status", "--porcelain")),
        "code_fingerprint": code,
    }


def deterministic_run_id(
    *,
    config: BacktestConfig,
    strategy: dict[str, Any],
    data_snapshot: dict[str, Any],
    code: dict[str, Any],
) -> str:
    payload = {
        "config": config.to_dict(),
        "strategy": strategy,
        "data_snapshot_id": data_snapshot["snapshot_id"],
        "code": code["sha256"],
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    return f"momentum-{digest[:16]}"


def _json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def _parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    table = pa.Table.from_pylist(rows) if rows else pa.table({"empty": pa.array([], pa.bool_())})
    pq.write_table(table, path, compression="zstd")


def write_artifacts(
    *,
    config: BacktestConfig,
    options: RunOptions,
    result: BacktestResult,
    metrics: dict[str, Any],
    strategy: dict[str, Any],
    data_snapshot: dict[str, Any],
    environment: dict[str, Any],
) -> Path:
    output_root = options.output_root
    output_root.mkdir(parents=True, exist_ok=True)
    final = output_root / result.run_id
    temporary = output_root / f".{result.run_id}.tmp-{os.getpid()}"
    if final.exists() or temporary.exists():
        raise ArtifactError(f"运行目录已存在，不覆盖: {final}")
    temporary.mkdir()
    _json(temporary / "config.json", config.to_dict())
    _json(temporary / "run_options.json", options.to_dict())
    _json(temporary / "environment.json", environment)
    _json(temporary / "data_snapshot.json", data_snapshot)
    _json(temporary / "strategy.json", strategy)
    _json(temporary / "metrics.json", metrics)
    _parquet(
        temporary / "orders.parquet",
        [
            {
                **asdict(row.order),
                "side": row.order.side.value,
                "filled_quantity": row.filled_quantity,
                "remaining_quantity": row.remaining_quantity,
                "status": row.status.value,
                "reason": row.reason.value,
            }
            for row in result.orders
        ],
    )
    _parquet(
        temporary / "fills.parquet",
        [{**asdict(fill), "side": fill.side.value} for fill in result.fills],
    )
    _parquet(
        temporary / "corporate_actions.parquet",
        [asdict(row) for row in result.corporate_actions],
    )
    _parquet(temporary / "equity.parquet", [asdict(row) for row in result.equity])
    (temporary / "report.html").write_text(
        render_report(result=result, metrics=metrics, strategy=strategy),
        encoding="utf-8",
    )
    temporary.rename(final)
    return final
