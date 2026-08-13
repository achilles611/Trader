"""Start the local copy-trading control center and open it in Brave."""

from __future__ import annotations

import ctypes
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time


HOST = "127.0.0.1"
PORT = 8090
URL = f"http://{HOST}:{PORT}"
STARTUP_TIMEOUT_SECONDS = 30


def project_root() -> Path:
    """Use the executable's directory after packaging, otherwise this file's."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def port_is_open() -> bool:
    try:
        with socket.create_connection((HOST, PORT), timeout=0.5):
            return True
    except OSError:
        return False


def brave_path() -> Path | None:
    candidates = [
        os.environ.get("BRAVE_PATH"),
        shutil.which("brave.exe"),
        shutil.which("brave"),
        os.path.join(os.environ.get("PROGRAMFILES", r"C:\\Program Files"), "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
        os.path.join(os.environ.get("PROGRAMFILES(X86)", r"C:\\Program Files (x86)"), "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
        os.path.join(os.environ.get("LOCALAPPDATA", ""), "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return Path(candidate)
    return None


def start_server(root: Path) -> subprocess.Popen[bytes]:
    python = root / ".venv" / "Scripts" / "python.exe"
    entrypoint = root / "main.py"
    if not python.is_file() or not entrypoint.is_file():
        raise RuntimeError("BeezConsole must be placed in the Trader project root beside .venv and main.py.")

    log_path = root / "logs" / "beez-console-server.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = log_path.open("a", encoding="utf-8")
    creation_flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "CREATE_NO_WINDOW", 0)
    return subprocess.Popen(
        [str(python), str(entrypoint), "copy-control-center", "--with-watcher"],
        cwd=root,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        creationflags=creation_flags,
    )


def wait_for_server(process: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if port_is_open():
            return
        if process.poll() is not None:
            raise RuntimeError("The control-center server stopped before port 8090 opened. See logs\\beez-console-server.log.")
        time.sleep(0.25)
    raise RuntimeError("The control-center server did not open port 8090 within 30 seconds. See logs\\beez-console-server.log.")


def show_error(message: str) -> None:
    if os.name == "nt":
        ctypes.windll.user32.MessageBoxW(None, message, "BeezConsole", 0x10)
    else:  # pragma: no cover - Windows launcher only
        print(message, file=sys.stderr)


def main() -> int:
    try:
        root = project_root()
        if not port_is_open():
            wait_for_server(start_server(root))
        brave = brave_path()
        if brave is None:
            raise RuntimeError("Brave Browser was not found. Install Brave or set BRAVE_PATH to brave.exe.")
        subprocess.Popen([str(brave), "--new-window", URL], cwd=root)
        return 0
    except RuntimeError as exc:
        show_error(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
