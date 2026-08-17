"""Small deterministic latency instrumentation for the scientific hot path."""

from __future__ import annotations

from contextlib import contextmanager
from time import perf_counter_ns
from typing import Any, Iterator
from uuid import uuid4

from .models import iso, utc_now
from .science_repository import ScientificRepository


class ScientificLatencyMonitor:
    def __init__(self, repository: ScientificRepository | None = None) -> None:
        self.repository = repository
        self.samples: list[dict[str, Any]] = []

    @contextmanager
    def measure(self, stage: str, **metadata: Any) -> Iterator[None]:
        started = perf_counter_ns()
        try:
            yield
        finally:
            elapsed_ms = (perf_counter_ns() - started) / 1_000_000
            sample = {"stage": stage, "elapsed_ms": elapsed_ms, "metadata": metadata}
            self.samples.append(sample)
            if self.repository:
                self.repository.record_latency(uuid4().hex, observed_at=iso(utc_now()), stage=stage, elapsed_ms=elapsed_ms, metadata=metadata)

    def report(self) -> dict[str, Any]:
        stages: dict[str, list[float]] = {}
        for sample in self.samples:
            stages.setdefault(sample["stage"], []).append(sample["elapsed_ms"])
        return {stage: {"count": len(values), "mean_ms": sum(values) / len(values), "max_ms": max(values)} for stage, values in stages.items()}
