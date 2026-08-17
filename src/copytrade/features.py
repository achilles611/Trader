"""Versioned features: observations become explicit, reproducible inputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from .science_repository import ScientificRepository


@dataclass(frozen=True)
class FeatureDefinition:
    feature_id: str
    version: int
    exact_definition: str
    units: str
    required_inputs: tuple[str, ...]
    freshness_seconds: float | None
    missing_data_semantics: str
    created_at: str
    code_sha: str
    family: str = "market"

    def __post_init__(self) -> None:
        if not self.feature_id or self.version <= 0 or not self.exact_definition:
            raise ValueError("Features require a nonempty ID, positive version, and exact definition.")
        if self.freshness_seconds is not None and self.freshness_seconds <= 0:
            raise ValueError("Feature freshness must be positive when defined.")
        if not self.missing_data_semantics:
            raise ValueError("Feature missing-data semantics are required.")

    def payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FeatureValue:
    feature_id: str
    version: int
    observed_at: str
    value: float | None
    missing: bool
    source_observation_ids: tuple[str, ...] = ()


class FeatureRegistry:
    def __init__(self, repository: ScientificRepository) -> None:
        self.repository = repository

    def register(self, feature: FeatureDefinition) -> dict[str, Any]:
        return self.repository.register_feature(
            feature.feature_id, feature.version, feature.payload(), created_at=feature.created_at, code_sha=feature.code_sha,
        )

    def materialize(self, feature: FeatureDefinition, *, observed_at: str, inputs: Mapping[str, Any], value: float | None) -> FeatureValue:
        missing_inputs = [name for name in feature.required_inputs if inputs.get(name) is None]
        missing = bool(missing_inputs) or value is None
        # A missing value is explicit evidence, never silently changed to zero.
        return FeatureValue(feature.feature_id, feature.version, observed_at, None if missing else float(value), missing)


WALLET_FEATURES = (
    "wallet_action", "wallet_action_freshness", "wallet_convergence_count", "weighted_wallet_convergence",
    "wallet_disagreement", "wallet_size_relative", "wallet_action_acceleration", "wallet_repeat_entry_cadence",
    "wallet_regime_specialization", "wallet_symbol_specialization", "wallet_estimated_hold_horizon",
)
MARKET_FEATURES = (
    "short_term_return", "realized_volatility", "volatility_expansion", "local_price_acceleration",
    "volume_acceleration", "aggressive_flow_imbalance", "vwap_displacement", "local_momentum", "spread",
    "liquidity_depth_proxy", "liquidation_activity", "market_regime", "cross_symbol_context",
)
