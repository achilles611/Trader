from __future__ import annotations

from typing import Iterable, Protocol

from .models import Target


class CandidateDiscoveryAdapter(Protocol):
    """Extension point for non-scraping discovery sources added after Alpha."""

    def discover(self) -> Iterable[Target]: ...
