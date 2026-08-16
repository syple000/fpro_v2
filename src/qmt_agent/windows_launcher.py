"""用明确路径启动 miniQMT 和 qmt-agent。"""

from __future__ import annotations

import argparse
import ctypes
import os
import sys
import time
from collections.abc import Generator, Sequence
from contextlib import contextmanager, suppress
from pathlib import Path

import psutil

AGENT_COMMAND_MARKERS = (
    "start_qmt_agent.cmd",
    "qmt-agent-start",
    "qmt_agent.windows_launcher",
    "qmt_agent.__main__",
    "qmt_agent.api:app",
)

ES_SYSTEM_REQUIRED = 0x00000001
ES_CONTINUOUS = 0x80000000


def _set_thread_execution_state(flags: int) -> None:
    """调用 Win32 电源 API；返回 NULL 表示设置失败。"""
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
    set_execution_state = kernel32.SetThreadExecutionState
    set_execution_state.argtypes = [ctypes.c_uint32]
    set_execution_state.restype = ctypes.c_uint32
    if not set_execution_state(flags):
        error_code = ctypes.get_last_error()  # type: ignore[attr-defined]
        raise OSError(error_code, "SetThreadExecutionState 调用失败")


@contextmanager
def prevent_system_sleep() -> Generator[None, None, None]:
    """在当前线程存活期间阻止 Windows 因空闲自动休眠。"""
    try:
        _set_thread_execution_state(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
    except OSError as exc:
        raise RuntimeError(f"无法阻止 Windows 自动休眠：{exc}") from exc

    try:
        yield
    finally:
        try:
            _set_thread_execution_state(ES_CONTINUOUS)
        except OSError as exc:
            # 进程即将退出时 Windows 也会释放线程执行状态；这里不能掩盖原始退出原因。
            print(f"警告：无法恢复 Windows 自动休眠：{exc}", file=sys.stderr, flush=True)


def qmt_paths(qmt_bin_value: str, shortcut_value: str) -> tuple[Path, Path]:
    """校验调用方直接传入的 QMT 路径。"""
    qmt_bin = Path(qmt_bin_value).expanduser()
    shortcut = Path(shortcut_value).expanduser()
    site_packages = qmt_bin / "Lib" / "site-packages"

    if not qmt_bin.is_dir():
        raise RuntimeError(f"QMT bin.x64 目录不存在：{qmt_bin}")
    if not (site_packages / "xtquant").is_dir():
        raise RuntimeError(f"xtquant 包不存在：{site_packages / 'xtquant'}")
    if not shortcut.is_file():
        raise RuntimeError(f"miniQMT 快捷方式不存在：{shortcut}")

    return qmt_bin.resolve(), shortcut.resolve()


def _looks_like_agent(command_line: Sequence[str]) -> bool:
    command = " ".join(command_line).casefold()
    return any(marker in command for marker in AGENT_COMMAND_MARKERS)


def _path_is_inside(path: str, root: Path) -> bool:
    candidate = os.path.normcase(os.path.abspath(path))
    expected_root = os.path.normcase(os.path.abspath(root))
    try:
        return os.path.commonpath((candidate, expected_root)) == expected_root
    except ValueError:
        return False


def _protected_process_ids() -> set[int]:
    """保护当前启动器及负责启动它的 uv、cmd 等父进程。"""
    protected = {os.getpid()}
    with suppress(psutil.Error):
        protected.update(process.pid for process in psutil.Process().parents())
    return protected


def find_running_instances(
    qmt_bin: Path,
) -> tuple[list[psutil.Process], list[psutil.Process]]:
    """查找旧 agent 和明确安装目录中的 miniQMT 进程。"""
    protected = _protected_process_ids()
    install_root = qmt_bin.parent
    agents: list[psutil.Process] = []
    clients: list[psutil.Process] = []

    for process in psutil.process_iter(["pid", "exe", "cmdline"]):
        try:
            if process.pid in protected:
                continue
            executable = process.info.get("exe")
            if executable and _path_is_inside(executable, install_root):
                clients.append(process)
                continue
            if _looks_like_agent(process.info.get("cmdline") or []):
                agents.append(process)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return agents, clients


def stop_processes(processes: Sequence[psutil.Process], label: str) -> None:
    """先终止进程，5 秒后仍未退出则强制关闭。"""
    targets = {process.pid: process for process in processes}.values()
    pending: list[psutil.Process] = []
    denied: list[int] = []

    for process in targets:
        try:
            process.terminate()
            pending.append(process)
        except psutil.NoSuchProcess:
            continue
        except psutil.AccessDenied:
            denied.append(process.pid)

    if denied:
        raise RuntimeError(f"没有权限关闭{label}，PID：{', '.join(map(str, denied))}")
    if not pending:
        return

    print(f"正在关闭{label}，PID：{', '.join(str(p.pid) for p in pending)}", flush=True)
    _, alive = psutil.wait_procs(pending, timeout=5)
    for process in alive:
        try:
            process.kill()
        except psutil.NoSuchProcess:
            continue
        except psutil.AccessDenied as exc:
            raise RuntimeError(f"没有权限强制关闭{label}，PID：{process.pid}") from exc

    if alive:
        _, alive = psutil.wait_procs(alive, timeout=5)
    if alive:
        raise RuntimeError(
            f"无法关闭{label}，PID：{', '.join(str(process.pid) for process in alive)}"
        )


def stop_running_instances(qmt_bin: Path) -> None:
    agents, clients = find_running_instances(qmt_bin)
    stop_processes(agents, "qmt-agent")
    stop_processes(clients, "miniQMT")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="启动 miniQMT 和 qmt-agent")
    parser.add_argument("--qmt-bin", required=True, help="miniQMT 的 bin.x64 目录")
    parser.add_argument("--qmt-shortcut", required=True, help="miniQMT 快捷方式")
    parser.add_argument(
        "--client-wait-seconds",
        type=int,
        default=60,
        help="启动客户端后等待的秒数，默认 60",
    )
    return parser


def run(args: argparse.Namespace) -> None:
    if sys.platform != "win32":
        raise RuntimeError("该启动器只能在 Windows 上运行")
    if args.client_wait_seconds < 0:
        raise RuntimeError("--client-wait-seconds 不能小于 0")

    qmt_bin, shortcut = qmt_paths(args.qmt_bin, args.qmt_shortcut)
    stop_running_instances(qmt_bin)

    with prevent_system_sleep():
        print("已阻止 Windows 在 qmt-agent 运行期间自动休眠。", flush=True)
        print(f"正在启动 miniQMT：{shortcut}", flush=True)
        os.startfile(shortcut)  # type: ignore[attr-defined]
        if args.client_wait_seconds:
            print(f"等待 miniQMT 初始化 {args.client_wait_seconds} 秒……", flush=True)
            time.sleep(args.client_wait_seconds)

        print("正在启动 qmt-agent……", flush=True)
        site_packages = str(qmt_bin / "Lib" / "site-packages")
        if site_packages not in sys.path:
            # 放在末尾，避免 QMT 自带的旧依赖覆盖 uv 环境中的依赖。
            sys.path.append(site_packages)

        try:
            with os.add_dll_directory(qmt_bin):  # type: ignore[attr-defined]
                from qmt_agent.__main__ import main as run_agent

                run_agent()
        except OSError as exc:
            raise RuntimeError(f"无法加载 miniQMT DLL 目录 {qmt_bin}：{exc}") from exc


def main(argv: Sequence[str] | None = None) -> int:
    try:
        run(build_parser().parse_args(argv))
    except KeyboardInterrupt:
        print("收到退出信号，qmt-agent 已停止。", flush=True)
        return 130
    except Exception as exc:
        print(f"启动失败：{exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
