from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import cast

import psutil
import pytest

from qmt_agent import windows_launcher as launcher


def make_qmt_files(root: Path) -> tuple[Path, Path]:
    qmt_bin = root / "东北证券NET专业版" / "bin.x64"
    (qmt_bin / "Lib" / "site-packages" / "xtquant").mkdir(parents=True)
    shortcut = root / "东北证券NET专业版.lnk"
    shortcut.touch()
    return qmt_bin, shortcut


def test_qmt_paths_uses_passed_values(tmp_path: Path) -> None:
    qmt_bin, shortcut = make_qmt_files(tmp_path)

    result = launcher.qmt_paths(str(qmt_bin), str(shortcut))

    assert result == (qmt_bin.resolve(), shortcut.resolve())


def test_qmt_paths_rejects_missing_bin(tmp_path: Path) -> None:
    shortcut = tmp_path / "东北证券NET专业版.lnk"
    shortcut.touch()

    with pytest.raises(RuntimeError, match="bin.x64 目录不存在"):
        launcher.qmt_paths(str(tmp_path / "missing"), str(shortcut))


def test_process_matching_is_limited_to_agent_and_installation(tmp_path: Path) -> None:
    qmt_bin, _ = make_qmt_files(tmp_path)

    assert launcher._looks_like_agent(["python.exe", "-m", "qmt_agent.__main__"])
    assert not launcher._looks_like_agent(["python.exe", "strategy.py"])
    assert launcher._path_is_inside(str(qmt_bin / "client.exe"), qmt_bin.parent)
    assert not launcher._path_is_inside(str(tmp_path / "other" / "client.exe"), qmt_bin.parent)


class FakeProcess:
    def __init__(
        self,
        process_id: int,
        *,
        executable: str | None = None,
        command_line: list[str] | None = None,
    ) -> None:
        self.pid = process_id
        self.info = {"exe": executable, "cmdline": command_line or []}
        self.terminated = False
        self.killed = False

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True


def test_find_running_instances_excludes_current_process_tree(
    tmp_path: Path, monkeypatch
) -> None:
    qmt_bin, _ = make_qmt_files(tmp_path)
    client = cast(
        psutil.Process,
        FakeProcess(1, executable=str(qmt_bin / "XtMiniQmt.exe")),
    )
    agent = cast(
        psutil.Process,
        FakeProcess(2, command_line=["python.exe", "-m", "qmt_agent.__main__"]),
    )
    protected = cast(
        psutil.Process,
        FakeProcess(3, command_line=["qmt-agent-start"]),
    )
    unrelated = cast(
        psutil.Process,
        FakeProcess(4, command_line=["python.exe", "strategy.py"]),
    )
    monkeypatch.setattr(launcher, "_protected_process_ids", lambda: {3})
    monkeypatch.setattr(
        launcher.psutil,
        "process_iter",
        lambda _: [client, agent, protected, unrelated],
    )

    agents, clients = launcher.find_running_instances(qmt_bin)

    assert agents == [agent]
    assert clients == [client]


def test_stop_processes_forces_process_that_does_not_exit(monkeypatch) -> None:
    fake = FakeProcess(123)
    process = cast(psutil.Process, fake)
    waits = 0

    def wait_procs(
        processes: Sequence[psutil.Process], timeout: float,
    ) -> tuple[list[psutil.Process], list[psutil.Process]]:
        nonlocal waits
        waits += 1
        return ([], list(processes)) if waits == 1 else (list(processes), [])

    monkeypatch.setattr(launcher.psutil, "wait_procs", wait_procs)

    launcher.stop_processes([process], "测试进程")

    assert fake.terminated is True
    assert fake.killed is True
