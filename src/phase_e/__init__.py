"""Phase E scientific-experiment foundation.

The package reads immutable Phase D corpus provenance and persists its own
append-only scientific ledger.  It intentionally exposes no prediction,
signal, execution, or capital-allocation capability.
"""

from .ledger import CorpusProvenanceError, ExperimentConflictError, LedgerIntegrityError, PhaseELedger
from .materialization import (
    ALL_ELIGIBLE_V1,
    DETERMINISTIC_HASH_V1,
    TIME_STRATIFIED_HASH_V1,
    EligibilitySpec,
    MaterializationConflictError,
    MaterializationIntegrityError,
    MaterializationSpec,
    MaterializationStatus,
    OutcomeAccessError,
    PhaseEMaterializer,
    SourceUniverseProvenance,
    StratificationSpec,
)
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
    "ALL_ELIGIBLE_V1",
    "DETERMINISTIC_HASH_V1",
    "ExperimentConflictError",
    "ExperimentConclusion",
    "ExperimentResult",
    "ExperimentStatus",
    "EligibilitySpec",
    "FeatureReference",
    "HypothesisDefinition",
    "LedgerIntegrityError",
    "MaterializationConflictError",
    "MaterializationIntegrityError",
    "MaterializationSpec",
    "MaterializationStatus",
    "NullExperimentRunner",
    "OutcomeAccessError",
    "OutcomeHorizon",
    "PartitionIdentity",
    "PhaseELedger",
    "PhaseEMaterializer",
    "PromotionState",
    "RejectionReason",
    "StatisticSpec",
    "SourceUniverseProvenance",
    "StratificationSpec",
    "TIME_STRATIFIED_HASH_V1",
]
