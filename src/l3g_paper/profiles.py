"""Closed, paper-only entry-confidence profiles for Lane III-G."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from .contracts import POLICY


class PaperEntryProfile(StrEnum):
    STANDARD = "STANDARD"
    BEEZTMODE_V1 = "BEEZTMODE_V1"


@dataclass(frozen=True)
class PaperEntryProfileSpec:
    profile: PaperEntryProfile
    version: str
    standard_threshold: Decimal
    effective_threshold: Decimal
    hard_confidence_floor: Decimal
    paper_only: bool = True
    live_capital: str = "DENIED"

    def __post_init__(self) -> None:
        if (
            not self.paper_only
            or self.live_capital != "DENIED"
            or self.standard_threshold != POLICY.entry_support_threshold
            or not Decimal("0") <= self.hard_confidence_floor <= self.effective_threshold <= Decimal("1")
        ):
            raise ValueError("Entry profiles are sealed to normalized Sim101 paper confidence.")

    def payload(self) -> dict[str, object]:
        return {
            "selected_profile": self.profile.value,
            "entry_profile": self.profile.value,
            "entry_profile_version": self.version,
            "standard_threshold": str(self.standard_threshold),
            "effective_threshold": str(self.effective_threshold),
            "hard_confidence_floor": str(self.hard_confidence_floor),
            "paper_only": self.paper_only,
            "live_capital": self.live_capital,
        }


STANDARD_PROFILE = PaperEntryProfileSpec(
    PaperEntryProfile.STANDARD,
    "STANDARD_V1",
    POLICY.entry_support_threshold,
    POLICY.entry_support_threshold,
    Decimal("0.50"),
)
BEEZTMODE_PROFILE = PaperEntryProfileSpec(
    PaperEntryProfile.BEEZTMODE_V1,
    "BEEZTMODE_V1",
    POLICY.entry_support_threshold,
    POLICY.entry_support_threshold * Decimal("0.85"),
    Decimal("0.50"),
)


def entry_profile_spec(profile: PaperEntryProfile | str) -> PaperEntryProfileSpec:
    try:
        selected = profile if type(profile) is PaperEntryProfile else PaperEntryProfile(str(profile))
    except ValueError as exc:
        raise ValueError("ENTRY_PROFILE_UNSUPPORTED") from exc
    return STANDARD_PROFILE if selected is PaperEntryProfile.STANDARD else BEEZTMODE_PROFILE
