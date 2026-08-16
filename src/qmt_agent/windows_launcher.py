"""qmt-agent 的 Windows 启动与进程管理。"""

from __future__ import annotations

import argparse
import ctypes
import os
import sys
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager, suppress
from pathlib import Path

import psutil

ES_SYSTEM_REQUIRED = 0x00000001
ES_CONTINUOUS = 0x80000000
WM_CLOSE = 0x0010


def _existing_path(path: str | Path | None) -> Path | None:
    if not path:
        return None
    candidate = Path(path).expanduser()
    return candidate.resolve() if candidate.exists() else None


def find_qmt_bin(
    explicit_path: str | Path | None = None,
    search_roots: Sequence[Path] | None = None,
) -> Path:
    """查找包含 xtquant 的 bin.x64 目录。"""
    explicit = _existing_path(explicit_path or os.getenv("QMT_BIN_PATH"))
    if explicit is not None:
        _validate_qmt_bin(explicit)
        return explicit
    if explicit_path or os.getenv("QMT_BIN_PATH"):
        raise RuntimeError("指定的 QMT bin.x64 目录不存在")

    roots = list(search_roots) if search_roots is not None else _program_files_roots()
    patterns = (
        "东北证券NET专业版/bin.x64",
        "*NET*/bin.x64",
        "*QMT*/bin.x64",
    )
    for root in roots:
        for pattern in patterns:
            try:
                candidates = root.glob(pattern)
                for candidate in candidates:
                    if _is_qmt_bin(candidate):
                        return candidate.resolve()
            except OSError:
                continue

    raise RuntimeError("没有找到 miniQMT 的 bin.x64；请使用 --qmt-bin 显式指定")


def _program_files_roots() -> list[Path]:
    roots: list[Path] = []
    for name in ("ProgramW6432", "ProgramFiles", "PROGRAMFILES"):
        path = _existing_path(os.getenv(name))
        if path is not None and path not in roots:
            roots.append(path)
    return roots


def _is_qmt_bin(path: Path) -> bool:
    return (path / "Lib" / "site-packages" / "xtquant").is_dir()


def _validate_qmt_bin(path: Path) -> None:
    if not _is_qmt_bin(path):
        raise RuntimeError(f"目录中没有找到 xtquant：{path / 'Lib' / 'site-packages'}")


def find_qmt_shortcut(
    explicit_path: str | Path | None = None,
    desktop_roots: Sequence[Path] | None = None,
) -> Path:
    """从用户桌面和公共桌面查找 miniQMT 快捷方式。"""
    explicit = _existing_path(explicit_path or os.getenv("QMT_SHORTCUT"))
    if explicit is not None:
        return explicit
    if explicit_path or os.getenv("QMT_SHORTCUT"):
        raise RuntimeError("指定的 miniQMT 快捷方式不存在")

    desktops = list(desktop_roots) if desktop_roots is not None else _desktop_roots()
    candidates: list[tuple[int, Path]] = []
    for desktop in desktops:
        try:
            shortcut_files = desktop.glob("*.lnk")
            for shortcut in shortcut_files:
                score = _shortcut_score(shortcut.stem)
                if score > 0:
                    candidates.append((score, shortcut))
        except OSError:
            continue

    if not candidates:
        raise RuntimeError("没有找到 miniQMT 桌面快捷方式；请使用 --qmt-shortcut 显式指定")
    return max(candidates, key=lambda item: (item[0], str(item[1])))[1].resolve()


def _desktop_roots() -> list[Path]:
    candidates = [
        Path.home() / "Desktop",
        Path.home() / "桌面",
        Path.home() / "OneDrive" / "Desktop",
        Path.home() / "OneDrive" / "桌面",
    ]
    public = os.getenv("PUBLIC")
    if public:
        candidates.append(Path(public) / "Desktop")

    # 注册表中的桌面位置能覆盖重定向目录和非默认 OneDrive 配置。
    try:
        import winreg

        key_path = r"Software\Microsoft\Windows\CurrentVersion\Explorer\User Shell Folders"
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path) as key:
            desktop, _ = winreg.QueryValueEx(key, "Desktop")
        candidates.insert(0, Path(os.path.expandvars(desktop)))
    except (ImportError, OSError):
        pass

    result: list[Path] = []
    for candidate in candidates:
        resolved = _existing_path(candidate)
        if resolved is not None and resolved not in result:
            result.append(resolved)
    return result


def _shortcut_score(name: str) -> int:
    upper_name = name.upper()
    score = 0
    for keyword, weight in (
        ("东北证券", 100),
        ("MINIQMT", 80),
        ("QMT", 60),
        ("NET专业版", 50),
        ("NET", 10),
    ):
        if keyword in upper_name:
            score += weight
    return score


def _looks_like_agent(command_line: Sequence[str]) -> bool:
    command = " ".join(command_line).casefold()
    return any(
        marker in command
        for marker in (
            "start_qmt_agent.py",
            "start_qmt_agent.cmd",
            "qmt-agent",
            "qmt_agent.__main__",
            "qmt_agent.api:app",
        )
    )


def _path_is_inside(path: str, root: Path) -> bool:
    normalized_path = os.path.abspath(path).replace("\\", "/").rstrip("/").casefold()
    normalized_root = (
        os.path.abspath(root).replace("\\", "/").rstrip("/").casefold()
    )
    return normalized_path == normalized_root or normalized_path.startswith(
        normalized_root + "/"
    )


def _protected_process_ids() -> set[int]:
    """保护当前 Python 启动器以及负责拉起它的 uv、cmd 等父进程。"""
    protected = {os.getpid()}
    with suppress(psutil.NoSuchProcess, psutil.AccessDenied):
        protected.update(parent.pid for parent in psutil.Process().parents())
    return protected


def find_running_agents() -> list[psutil.Process]:
    protected = _protected_process_ids()
    result: list[psutil.Process] = []
    for process in psutil.process_iter(["pid", "cmdline"]):
        try:
            command_line = process.info.get("cmdline") or []
            if process.pid not in protected and _looks_like_agent(command_line):
                result.append(process)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return result


def find_running_qmt_clients(qmt_bin: Path) -> list[psutil.Process]:
    """只匹配当前 miniQMT 安装目录中的进程，避免按进程名误杀。"""
    protected = _protected_process_ids()
    install_root = qmt_bin.parent
    result: list[psutil.Process] = []
    for process in psutil.process_iter(["pid", "exe"]):
        try:
            executable = process.info.get("exe")
            if (
                process.pid not in protected
                and executable
                and _path_is_inside(executable, install_root)
            ):
                result.append(process)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return result


def _collect_process_tree(processes: Sequence[psutil.Process]) -> list[psutil.Process]:
    by_pid: dict[int, psutil.Process] = {}
    for process in processes:
        try:
            for child in process.children(recursive=True):
                by_pid[child.pid] = child
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        by_pid[process.pid] = process
    return list(by_pid.values())


def _close_process_windows(process_ids: set[int]) -> int:
    """向 GUI 客户端发送关闭窗口消息，让它先尝试正常退出。"""
    if sys.platform != "win32" or not process_ids:
        return 0

    user32 = ctypes.WinDLL("user32", use_last_error=True)
    callback_type = ctypes.WINFUNCTYPE(
        ctypes.c_int, ctypes.c_void_p, ctypes.c_ssize_t
    )
    user32.EnumWindows.argtypes = [callback_type, ctypes.c_void_p]
    user32.EnumWindows.restype = ctypes.c_int
    user32.GetWindowThreadProcessId.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_ulong),
    ]
    user32.GetWindowThreadProcessId.restype = ctypes.c_ulong
    user32.PostMessageW.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint,
        ctypes.c_void_p,
        ctypes.c_void_p,
    ]
    user32.PostMessageW.restype = ctypes.c_int
    closed_count = 0

    def close_window(window: int, _: int) -> bool:
        nonlocal closed_count
        process_id = ctypes.c_ulong()
        user32.GetWindowThreadProcessId(window, ctypes.byref(process_id))
        if process_id.value in process_ids and user32.PostMessageW(
            window, WM_CLOSE, None, None
        ):
            closed_count += 1
        return True

    callback = callback_type(close_window)
    user32.EnumWindows(callback, 0)
    return closed_count


def _process_description(process: psutil.Process) -> str:
    try:
        return f"{process.name()}({process.pid})"
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return f"PID {process.pid}"


def stop_processes(
    processes: Sequence[psutil.Process],
    label: str,
    graceful_timeout: float = 8.0,
    force_timeout: float = 5.0,
    close_windows: Callable[[set[int]], int] = _close_process_windows,
    wait_processes: Callable[..., tuple[list[psutil.Process], list[psutil.Process]]] = (
        psutil.wait_procs
    ),
) -> None:
    """先关闭窗口，超时后终止进程，最后确认进程确实退出。"""
    targets = _collect_process_tree(processes)
    if not targets:
        print(f"未检测到运行中的{label}。", flush=True)
        return

    descriptions = "、".join(_process_description(process) for process in targets)
    print(f"检测到运行中的{label}：{descriptions}", flush=True)

    remaining = targets
    if close_windows({process.pid for process in targets}) > 0:
        print(f"正在请求{label}正常退出……", flush=True)
        _, remaining = wait_processes(targets, timeout=graceful_timeout)

    if remaining:
        print(f"正在终止仍未退出的{label}……", flush=True)
        errors: list[str] = []
        for process in remaining:
            try:
                process.terminate()
            except psutil.NoSuchProcess:
                continue
            except psutil.AccessDenied:
                errors.append(_process_description(process))
        if errors:
            raise RuntimeError(f"没有权限关闭{label}：{'、'.join(errors)}")
        _, remaining = wait_processes(remaining, timeout=force_timeout)

    if remaining:
        for process in remaining:
            try:
                process.kill()
            except psutil.NoSuchProcess:
                continue
            except psutil.AccessDenied as exc:
                raise RuntimeError(
                    f"没有权限强制关闭{label}：{_process_description(process)}"
                ) from exc
        _, remaining = wait_processes(remaining, timeout=force_timeout)

    if remaining:
        details = "、".join(_process_description(process) for process in remaining)
        raise RuntimeError(f"无法关闭{label}：{details}")
    print(f"{label}已关闭。", flush=True)


def stop_running_instances(qmt_bin: Path) -> None:
    """旧 agent 先停，随后关闭 miniQMT，确保端口和客户端状态干净。"""
    stop_processes(find_running_agents(), "qmt-agent")
    stop_processes(find_running_qmt_clients(qmt_bin), "miniQMT 客户端")


def _windows_execution_state_function() -> Callable[[int], int]:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    set_execution_state = kernel32.SetThreadExecutionState
    set_execution_state.argtypes = [ctypes.c_uint]
    set_execution_state.restype = ctypes.c_uint
    return set_execution_state


@contextmanager
def keep_system_awake(
    set_execution_state: Callable[[int], int] | None = None,
) -> Iterator[None]:
    """在上下文存续期间阻止 Windows 因空闲自动进入睡眠。"""
    if set_execution_state is None:
        if sys.platform != "win32":
            yield
            return
        set_execution_state = _windows_execution_state_function()

    if not set_execution_state(ES_CONTINUOUS | ES_SYSTEM_REQUIRED):
        raise RuntimeError("无法设置 Windows 防休眠状态")

    print("已启用 Windows 防休眠；agent 退出后将自动恢复。", flush=True)
    try:
        yield
    finally:
        if not set_execution_state(ES_CONTINUOUS):
            print("警告：恢复 Windows 电源状态失败。", file=sys.stderr, flush=True)


def configure_qmt_environment(qmt_bin: Path) -> None:
    """把 xtquant 和本地 DLL 路径传给 agent。"""
    site_packages = qmt_bin / "Lib" / "site-packages"
    os.environ["QMT_XTQUANT_PATH"] = str(site_packages)
    os.environ["QMT_BIN_PATH"] = str(qmt_bin)
    os.environ["PATH"] = str(qmt_bin) + os.pathsep + os.environ.get("PATH", "")


def start_qmt(shortcut: Path) -> None:
    print(f"正在启动 miniQMT：{shortcut}", flush=True)
    os.startfile(shortcut)  # type: ignore[attr-defined]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="启动 miniQMT 和 qmt-agent")
    parser.add_argument("--qmt-bin", help="miniQMT 的 bin.x64 目录")
    parser.add_argument("--qmt-shortcut", help="miniQMT 桌面快捷方式")
    parser.add_argument(
        "--client-wait-seconds",
        type=int,
        default=10,
        help="启动客户端后等待的秒数，默认 10",
    )
    return parser


def run(args: argparse.Namespace) -> None:
    if sys.platform != "win32":
        raise RuntimeError("该启动器只能在 Windows 上运行")
    if args.client_wait_seconds < 0:
        raise RuntimeError("--client-wait-seconds 不能小于 0")

    qmt_bin = find_qmt_bin(args.qmt_bin)
    configure_qmt_environment(qmt_bin)
    shortcut = find_qmt_shortcut(args.qmt_shortcut)

    with keep_system_awake():
        stop_running_instances(qmt_bin)
        start_qmt(shortcut)
        if args.client_wait_seconds:
            print(f"等待 miniQMT 初始化 {args.client_wait_seconds} 秒……", flush=True)
            time.sleep(args.client_wait_seconds)

        print("正在启动 qmt-agent……", flush=True)
        from qmt_agent.__main__ import main as run_agent

        run_agent()


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
