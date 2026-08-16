from __future__ import annotations

from pathlib import Path

from qmt_agent import windows_launcher as launcher


def make_qmt_bin(root: Path) -> Path:
    qmt_bin = root / "东北证券NET专业版" / "bin.x64"
    (qmt_bin / "Lib" / "site-packages" / "xtquant").mkdir(parents=True)
    return qmt_bin


def test_find_qmt_bin_from_fixed_default(tmp_path: Path, monkeypatch) -> None:
    expected = make_qmt_bin(tmp_path)
    monkeypatch.delenv("QMT_BIN_PATH", raising=False)
    monkeypatch.setattr(launcher, "DEFAULT_QMT_BIN", expected)

    result = launcher.find_qmt_bin()

    assert result == expected.resolve()


def test_find_qmt_shortcut_prefers_broker_name(tmp_path: Path) -> None:
    qmt_shortcut = tmp_path / "东北证券NET专业版.lnk"
    other_shortcut = tmp_path / "普通NET工具.lnk"
    qmt_shortcut.touch()
    other_shortcut.touch()

    result = launcher.find_qmt_shortcut(desktop_roots=[tmp_path])

    assert result == qmt_shortcut.resolve()


def test_keep_system_awake_restores_state() -> None:
    states: list[int] = []

    def set_execution_state(state: int) -> int:
        states.append(state)
        return 1

    with launcher.keep_system_awake(set_execution_state):
        assert states == [launcher.ES_CONTINUOUS | launcher.ES_SYSTEM_REQUIRED]

    assert states == [
        launcher.ES_CONTINUOUS | launcher.ES_SYSTEM_REQUIRED,
        launcher.ES_CONTINUOUS,
    ]


def test_configure_qmt_environment(tmp_path: Path, monkeypatch) -> None:
    qmt_bin = make_qmt_bin(tmp_path)
    monkeypatch.setenv("PATH", "existing-path")

    launcher.configure_qmt_environment(qmt_bin)

    assert launcher.os.environ["QMT_BIN_PATH"] == str(qmt_bin)
    assert launcher.os.environ["QMT_XTQUANT_PATH"] == str(
        qmt_bin / "Lib" / "site-packages"
    )
    assert launcher.os.environ["PATH"].startswith(str(qmt_bin) + launcher.os.pathsep)


def test_agent_process_is_matched_by_command_not_python_name() -> None:
    assert launcher._looks_like_agent(
        ["python.exe", r"C:\project\start_qmt_agent.py"]
    )
    assert launcher._looks_like_agent(["uvicorn.exe", "qmt_agent.api:app"])
    assert not launcher._looks_like_agent(["python.exe", "unrelated_strategy.py"])


def test_client_process_must_be_inside_installation(tmp_path: Path) -> None:
    install_root = tmp_path / "qmt"
    executable = install_root / "bin.x64" / "client.exe"

    assert launcher._path_is_inside(str(executable), install_root)
    assert not launcher._path_is_inside(str(tmp_path / "other" / "client.exe"), install_root)


class FakeProcess:
    def __init__(self, process_id: int) -> None:
        self.pid = process_id
        self.terminated = False
        self.killed = False

    def children(self, recursive: bool = False) -> list[FakeProcess]:
        return []

    def name(self) -> str:
        return "fake.exe"

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True


def test_stop_processes_terminates_and_confirms_exit() -> None:
    process = FakeProcess(123)

    def wait_processes(processes, timeout):
        return list(processes), []

    launcher.stop_processes(
        [process],
        "测试进程",
        close_windows=lambda _: 0,
        wait_processes=wait_processes,
    )

    assert process.terminated is True
    assert process.killed is False


def test_stop_processes_prefers_closing_gui_window() -> None:
    process = FakeProcess(123)

    def wait_processes(processes, timeout):
        return list(processes), []

    launcher.stop_processes(
        [process],
        "测试进程",
        close_windows=lambda _: 1,
        wait_processes=wait_processes,
    )

    assert process.terminated is False
