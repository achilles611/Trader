from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class RunLockError(RuntimeError):
    pass


class RunLock:
    def __init__(self, path: Path, metadata: dict[str, Any] | None = None) -> None:
        self.path = path
        self.metadata = metadata or {}
        self._handle = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+b")
        self._handle.seek(0)
        self._handle.write(b" ")
        self._handle.flush()
        try:
            if os.name == "nt":
                import msvcrt

                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._handle.close()
            self._handle = None
            return False

        self._handle.seek(0)
        self._handle.truncate()
        self._handle.write(json.dumps(self.metadata, sort_keys=True).encode("utf-8"))
        self._handle.flush()
        return True

    def release(self) -> None:
        if self._handle is None:
            return
        try:
            self._handle.seek(0)
            self._handle.truncate()
            self._handle.flush()
            if os.name == "nt":
                import msvcrt

                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None

    def read_metadata(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        content = self.path.read_text(encoding="utf-8").strip()
        if not content:
            return {}
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return {"raw": content}

    def __enter__(self) -> "RunLock":
        if not self.acquire():
            raise RunLockError(f"Unable to acquire lock at {self.path}")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
