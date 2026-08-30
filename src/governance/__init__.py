"""Offline, fail-closed governance validation for the Beelzebub repository.

This package is intentionally not imported by any application runtime module.
"""

from .errors import GovernanceError

__all__ = ["GovernanceError"]
