"""Phase F.0 constitutional boundary for the future Lane II Trader.

This package intentionally has no dependency on Phase E scientific storage or
Phase D execution transport.  It defines only immutable, deny-by-default
authority and provenance contracts.
"""

from .boundary import (
    F0_AUTHORITY_MANIFEST_HASH,
    F0_BOUNDARY_SCHEMA,
    F0_MANIFEST,
    AuthorityCapability,
    AuthorityEvaluation,
    AuthorityOwner,
    AuthorityRecord,
    AuthorityState,
    AuthorityManifest,
    AuthorityRefused,
    ExecutionAuthorityRefused,
    InputProvenanceRefused,
    LaneIdentity,
    OperationalInput,
    OperationalInputSource,
    OperationalStrategyArtifact,
    StrategyAuthorityRegistry,
    StrategyRegistration,
    StrategyProvenanceRefused,
    TradeDirection,
    TradeIntentRequest,
    TradeIntentRefused,
    create_trade_intent,
    evaluate_lane_ii_authority,
    f0_strategy_registry,
    request_phase_d_execution,
)

SCIENTIFIC_LANE = LaneIdentity.SCIENTIFIC_LANE
TRADER_LANE = LaneIdentity.TRADER_LANE

__all__ = [
    "F0_AUTHORITY_MANIFEST_HASH",
    "F0_BOUNDARY_SCHEMA",
    "F0_MANIFEST",
    "SCIENTIFIC_LANE",
    "TRADER_LANE",
    "AuthorityCapability",
    "AuthorityEvaluation",
    "AuthorityManifest",
    "AuthorityOwner",
    "AuthorityRecord",
    "AuthorityRefused",
    "AuthorityState",
    "ExecutionAuthorityRefused",
    "InputProvenanceRefused",
    "LaneIdentity",
    "OperationalInput",
    "OperationalInputSource",
    "OperationalStrategyArtifact",
    "StrategyAuthorityRegistry",
    "StrategyProvenanceRefused",
    "StrategyRegistration",
    "TradeDirection",
    "TradeIntentRefused",
    "TradeIntentRequest",
    "create_trade_intent",
    "evaluate_lane_ii_authority",
    "f0_strategy_registry",
    "request_phase_d_execution",
]
