from __future__ import annotations

import os
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


def test_run_removes_proxy_environment_before_path_validation(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(launcher.sys, "platform", "win32")
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:7890")
    monkeypatch.setenv("https_proxy", "http://127.0.0.1:7890")
    arguments = launcher.build_parser().parse_args(
        [
            "--qmt-bin",
            str(tmp_path / "missing"),
            "--qmt-shortcut",
            str(tmp_path / "missing.lnk"),
        ]
    )

    with pytest.raises(RuntimeError, match="bin.x64 目录不存在"):
        launcher.run(arguments)

    assert not any(
        name.casefold() in {"http_proxy", "https_proxy"} for name in os.environ
    )


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


def test_find_running_instances_excludes_current_process_tree(tmp_path: Path, monkeypatch) -> None:
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
    other_qmt_component = cast(
        psutil.Process,
        FakeProcess(5, executable=str(qmt_bin / "XtItClient.exe")),
    )
    miniqmt_from_other_installation = cast(
        psutil.Process,
        FakeProcess(6, executable=str(tmp_path / "other" / "XtMiniQmt.exe")),
    )
    monkeypatch.setattr(launcher, "_protected_process_ids", lambda: {3})
    monkeypatch.setattr(
        launcher.psutil,
        "process_iter",
        lambda _: [
            client,
            agent,
            protected,
            unrelated,
            other_qmt_component,
            miniqmt_from_other_installation,
        ],
    )

    agents, clients = launcher.find_running_instances(qmt_bin)

    assert agents == [agent]
    assert clients == [client]


def test_stop_processes_forces_process_that_does_not_exit(monkeypatch) -> None:
    fake = FakeProcess(123)
    process = cast(psutil.Process, fake)
    waits = 0

    def wait_procs(
        processes: Sequence[psutil.Process],
        timeout: float,
    ) -> tuple[list[psutil.Process], list[psutil.Process]]:
        nonlocal waits
        waits += 1
        return ([], list(processes)) if waits == 1 else (list(processes), [])

    monkeypatch.setattr(launcher.psutil, "wait_procs", wait_procs)

    launcher.stop_processes([process], "测试进程")

    assert fake.terminated is True
    assert fake.killed is True


def test_prevent_system_sleep_sets_and_restores_execution_state(monkeypatch) -> None:
    states: list[int] = []
    monkeypatch.setattr(launcher, "_set_thread_execution_state", states.append)

    with launcher.prevent_system_sleep():
        assert states == [launcher.ES_CONTINUOUS | launcher.ES_SYSTEM_REQUIRED]

    assert states == [
        launcher.ES_CONTINUOUS | launcher.ES_SYSTEM_REQUIRED,
        launcher.ES_CONTINUOUS,
    ]


def test_prevent_system_sleep_fails_before_starting_when_windows_rejects_request(
    monkeypatch,
) -> None:
    def reject(_: int) -> None:
        raise OSError("模拟系统调用失败")

    monkeypatch.setattr(launcher, "_set_thread_execution_state", reject)

    with (
        pytest.raises(RuntimeError, match="无法阻止 Windows 自动休眠"),
        launcher.prevent_system_sleep(),
    ):
        pytest.fail("系统调用失败后不应继续启动")


def test_prevent_system_sleep_does_not_mask_exit_when_restore_fails(monkeypatch, capsys) -> None:
    states: list[int] = []

    def fail_on_restore(flags: int) -> None:
        states.append(flags)
        if flags == launcher.ES_CONTINUOUS:
            raise OSError("模拟恢复失败")

    monkeypatch.setattr(launcher, "_set_thread_execution_state", fail_on_restore)

    with launcher.prevent_system_sleep():
        pass

    assert states[-1] == launcher.ES_CONTINUOUS
    assert "无法恢复 Windows 自动休眠" in capsys.readouterr().err
