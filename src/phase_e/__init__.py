"""Phase E scientific-experiment foundation.

The package reads immutable Phase D corpus provenance and persists its own
append-only scientific ledger.  It intentionally exposes no prediction,
signal, execution, or capital-allocation capability.
"""

from .ledger import CorpusProvenanceError, ExperimentConflictError, LedgerIntegrityError, PhaseELedger
from .runner import NullExperimentRunner
from .types import (
    ExperimentConclusion,
    ExperimentResult,
    ExperimentStatus,
    FeatureReference,
    HypothesisDefinition,
    OutcomeHorizon,
    PartitionIdentity,
    PromotionState,
    RejectionReason,
    StatisticSpec,
)

__all__ = [
    "CorpusProvenanceError",
    "ExperimentConflictError",
    "ExperimentConclusion",
    "ExperimentResult",
    "ExperimentStatus",
    "FeatureReference",
    "HypothesisDefinition",
    "LedgerIntegrityError",
    "NullExperimentRunner",
    "OutcomeHorizon",
    "PartitionIdentity",
    "PhaseELedger",
    "PromotionState",
    "RejectionReason",
    "StatisticSpec",
]
