"""Start the local paper-only copy-trading control center in Brave.

This stays deliberately thin: it locates the project-local virtual
environment, starts the authoritative ``copy-control-center`` command when
needed, and opens the existing local UI.  It never selects a global Python or
changes any trading/operator state.
"""

from __future__ import annotations

import ctypes
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
from typing import Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


HOST = "127.0.0.1"
PORT = 8090
URL = f"http://{HOST}:{PORT}"
STARTUP_TIMEOUT_SECONDS = 30.0


def project_root(*, frozen: bool | None = None, executable: str | None = None, source_file: str | None = None) -> Path:
    """Return the executable directory once packaged, otherwise this source directory."""
    packaged = getattr(sys, "frozen", False) if frozen is None else frozen
    if packaged:
        return Path(executable or sys.executable).resolve().parent
    return Path(source_file or __file__).resolve().parent


def validate_project_root(root: Path) -> tuple[Path, Path]:
    """Require the executable to live beside the project-local runtime."""
    python = root / ".venv" / "Scripts" / "python.exe"
    entrypoint = root / "main.py"
    if not entrypoint.is_file() or not python.is_file():
        raise RuntimeError(
            "BeezConsole must be located in the Trader project root beside main.py and .venv\\Scripts\\python.exe."
        )
    return python, entrypoint


def port_is_open(host: str = HOST, port: int = PORT, *, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def server_is_responding(url: str = URL, *, timeout: float = 0.75) -> bool:
    """Check for an actual HTTP response, not merely a listener on the port."""
    try:
        request = Request(url, headers={"User-Agent": "BeezConsole"})
        with urlopen(request, timeout=timeout) as response:  # noqa: S310 - fixed localhost URL
            return 200 <= int(response.status) < 400
    except (HTTPError, URLError, OSError, ValueError):
        return False


def brave_candidates(
    environment: Mapping[str, str] | None = None, finder: Callable[[str], str | None] = shutil.which,
) -> tuple[Path, ...]:
    """Return Brave locations in the documented order, without choosing another browser."""
    env = os.environ if environment is None else environment
    program_files = env.get("PROGRAMFILES", r"C:\\Program Files")
    program_files_x86 = env.get("PROGRAMFILES(X86)", r"C:\\Program Files (x86)")
    local_app_data = env.get("LOCALAPPDATA", "")
    candidates = (
        env.get("BRAVE_PATH"),
        finder("brave.exe"),
        finder("brave"),
        os.path.join(program_files, "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
        os.path.join(program_files_x86, "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
        os.path.join(local_app_data, "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
    )
    return tuple(Path(candidate) for candidate in candidates if candidate)


def brave_path(
    *, environment: Mapping[str, str] | None = None, finder: Callable[[str], str | None] = shutil.which,
    exists: Callable[[Path], bool] | None = None,
) -> Path | None:
    exists = exists or Path.is_file
    for candidate in brave_candidates(environment, finder):
        if exists(candidate):
            return candidate
    return None


def _creation_flags() -> int:
    return getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "CREATE_NO_WINDOW", 0)


def start_server(root: Path) -> subprocess.Popen[bytes]:
    """Start the existing application command and redirect all backend output to logs."""
    python, entrypoint = validate_project_root(root)
    log_path = root / "logs" / "beez-console-server.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log_file:
        return subprocess.Popen(
            [str(python), str(entrypoint), "copy-control-center", "--with-watcher"],
            cwd=root,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            creationflags=_creation_flags(),
        )


def wait_for_server(process: subprocess.Popen[bytes] | None, *, timeout_seconds: float = STARTUP_TIMEOUT_SECONDS) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if server_is_responding():
            return
        if process is not None and process.poll() is not None:
            raise RuntimeError("The control-center server stopped during startup. See logs\\beez-console-server.log.")
        time.sleep(0.25)
    raise RuntimeError("The control-center server did not respond within 30 seconds. See logs\\beez-console-server.log.")


def open_brave(brave: Path, root: Path) -> None:
    subprocess.Popen([str(brave), "--new-window", URL], cwd=root, creationflags=_creation_flags())


def show_error(message: str) -> None:
    if os.name == "nt":
        ctypes.windll.user32.MessageBoxW(None, message, "BeezConsole startup failed", 0x10)
    else:  # pragma: no cover - the package is Windows-only
        print(message, file=sys.stderr)


def main() -> int:
    try:
        root = project_root()
        validate_project_root(root)
        process: subprocess.Popen[bytes] | None = None
        if not port_is_open():
            process = start_server(root)
        # A listener may still be warming up, including one started by another
        # launcher instance.  Never spawn a second backend for that case.
        wait_for_server(process)
        brave = brave_path()
        if brave is None:
            raise RuntimeError("Brave Browser was not found. Install Brave or set BRAVE_PATH to the brave.exe path.")
        open_brave(brave, root)
        return 0
    except (OSError, RuntimeError) as exc:
        show_error(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
