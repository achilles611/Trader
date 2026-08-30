"""Phase f4 Counterfactual Execution Laboratory.

This package is a deliberately non-authoritative Lane II side-domain.  It
does not import Phase E, Trader V0, the F.2 bridge, or venue adapters.
"""

from .contracts import (
    COUNTERFACTUAL_ONLY,
    BackendType,
    CounterfactualAssertion,
    CounterfactualEvidence,
    CounterfactualMutation,
    CounterfactualRunIdentity,
    CounterfactualRunResult,
    CounterfactualScenario,
    CounterfactualStateDiff,
    LaboratoryAuthorityRefused,
    ScenarioValidationError,
)

__all__ = [
    "COUNTERFACTUAL_ONLY",
    "BackendType",
    "CounterfactualAssertion",
    "CounterfactualEvidence",
    "CounterfactualMutation",
    "CounterfactualRunIdentity",
    "CounterfactualRunResult",
    "CounterfactualScenario",
    "CounterfactualStateDiff",
    "LaboratoryAuthorityRefused",
    "ScenarioValidationError",
]
