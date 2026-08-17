"""Small Windows-friendly scheduler for the durable scientific worker."""

from __future__ import annotations

from threading import Event
from typing import Protocol


class _RunnableWorker(Protocol):
    def run_once(self, *, max_items: int | None = None) -> dict[str, object]: ...


class ScientificScheduler:
    """One long-lived process, backed by durable work rather than cron jobs."""

    def __init__(self, worker: _RunnableWorker, *, poll_interval_seconds: float = 1.0) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive.")
        self.worker = worker
        self.poll_interval_seconds = poll_interval_seconds
        self._stop = Event()

    def stop(self) -> None:
        self._stop.set()

    def run_once(self, *, max_items: int | None = None) -> dict[str, object]:
        return self.worker.run_once(max_items=max_items)

    def run_forever(self, *, max_items: int | None = None) -> None:
        while not self._stop.is_set():
            self.worker.run_once(max_items=max_items)
            self._stop.wait(self.poll_interval_seconds)
