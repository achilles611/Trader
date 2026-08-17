"""Durable Phase D.6 scientific research loop.

The worker only creates scientific evidence and simulation/shadow decisions.
It deliberately has no dependency on paper execution, venue signing, or cold
storage reads.  Every transition is represented by a leased SQLite work item.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from enum import StrEnum
import math
import shutil
from statistics import fmean, pstdev
from typing import Any, Mapping
from uuid import uuid4

from .confidence import ConfidenceEngine, ModelEvidence
from .config import CopyTradeConfig, ScientificWorkerConfig
from .decision import DecisionInput, DecisionRiskPolicy, ScientificDecisionEngine
from .drift import assess_forward_drift
from .experiments import ForwardShadowEngine, HistoricalExperimentEngine
from .features import FeatureDefinition, FeatureRegistry, MARKET_FEATURES, WALLET_FEATURES
from .hypotheses import HypothesisDefinition, HypothesisRegistry, HypothesisState
from .indicators import IndicatorProvenance, IndicatorRegistry, IndicatorState
from .pattern_discovery import BoundedPatternDiscovery, SearchFamily
from .performance import ScientificLatencyMonitor
from .science_repository import ScientificRepository, canonical_hash
from .scientific_models import ModelDefinition, ModelState, ScientificModelRegistry


class WorkerStage(StrEnum):
    OBSERVATION_INGEST = "OBSERVATION_INGEST"
    FEATURE_MATERIALIZATION = "FEATURE_MATERIALIZATION"
    OUTCOME_LABEL = "OUTCOME_LABEL"
    PATTERN_DISCOVERY = "PATTERN_DISCOVERY"
    HISTORICAL_EXPERIMENT = "HISTORICAL_EXPERIMENT"
    FORWARD_PREDICTION = "FORWARD_PREDICTION"
    FORWARD_RESOLUTION = "FORWARD_RESOLUTION"
    INDICATOR_PROMOTION = "INDICATOR_PROMOTION"
    MODEL_BUILD = "MODEL_BUILD"
    MODEL_CALIBRATION = "MODEL_CALIBRATION"
    SHADOW_DECISION = "SHADOW_DECISION"
    DRIFT_EVALUATION = "DRIFT_EVALUATION"
    ARCHIVAL = "ARCHIVAL"


_WORK_PRIORITIES = {
    WorkerStage.FEATURE_MATERIALIZATION: 100,
    WorkerStage.OUTCOME_LABEL: 90,
    WorkerStage.PATTERN_DISCOVERY: 80,
    WorkerStage.HISTORICAL_EXPERIMENT: 70,
    WorkerStage.FORWARD_PREDICTION: 60,
    WorkerStage.FORWARD_RESOLUTION: 50,
    WorkerStage.INDICATOR_PROMOTION: 40,
    WorkerStage.MODEL_BUILD: 30,
    WorkerStage.MODEL_CALIBRATION: 25,
    WorkerStage.SHADOW_DECISION: 20,
    WorkerStage.DRIFT_EVALUATION: 10,
    WorkerStage.ARCHIVAL: 1,
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _time(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


class ScientificWorker:
    """Incremental and restart-safe D.6 scientific worker."""

    def __init__(self, repository: ScientificRepository, config: CopyTradeConfig, *, worker_id: str | None = None) -> None:
        self.repository = repository
        self.config = config
        self.settings: ScientificWorkerConfig = config.scientific_worker
        self.worker_id = worker_id or f"scientific-worker-{uuid4().hex[:12]}"
        self.repository.initialize()
        self.features = FeatureRegistry(repository)
        self.hypotheses = HypothesisRegistry(repository)
        # Resampling is intentionally bounded; heavy work is queued and never
        # performed in an observation callback.
        self.experiments = HistoricalExperimentEngine(repository, seed=7, resamples=self.settings.historical_resamples)
        self.forward = ForwardShadowEngine(repository)
        self.indicators = IndicatorRegistry(repository)
        self.models = ScientificModelRegistry(repository)
        self.confidence = ConfidenceEngine()
        self.decisions = ScientificDecisionEngine(repository)
        self.discovery = BoundedPatternDiscovery()
        self.latency = ScientificLatencyMonitor(repository)
        self._code_sha = "d6-scientific-worker-v1"
        self._config_hash = canonical_hash(config.research_snapshot())
        # Definition identity must not contain the current worker tick.  A
        # restart can re-register an identical immutable version safely.
        self._definition_created_at = _iso(_utc_now())
        self._definitions_ready = False
        self._last_observation_schedule_fingerprint: str | None = None
        self._last_workflow_schedule_fingerprint: str | None = None
        self._last_forward_schedule_fingerprint: str | None = None

    # ----- durable observation bridge -------------------------------------------------
    def ingest_observation(self, *, kind: str, source: str, source_event_id: str, payload: Mapping[str, Any],
                           event_at: str | datetime, received_at: str | datetime | None = None, wallet: str | None = None,
                           symbol: str | None = None, network: str = "mainnet-public", quality_flags: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Persist a normalized public observation and schedule feature work.

        Source time and receipt time remain separate. Impossible future source
        time is rejected before it can contaminate a feature or outcome.
        """
        return self.ingest_observations(({
            "kind": kind, "source": source, "source_event_id": source_event_id, "payload": payload,
            "event_at": event_at, "received_at": received_at, "wallet": wallet, "symbol": symbol,
            "network": network, "quality_flags": quality_flags,
        },))[0]

    def ingest_observations(self, observations: tuple[Mapping[str, Any], ...] | list[Mapping[str, Any]], *, schedule_features: bool = True) -> list[dict[str, Any]]:
        """Append a bounded batch through the same D.6 observation bridge.

        Archive parsers may call this to avoid one SQLite transaction per raw
        event.  It keeps the immutable observation identity and queues exactly
        the same feature materialization work as ``ingest_observation``.
        """
        now = _utc_now()
        prepared: list[dict[str, Any]] = []
        for item in observations:
            kind, source, source_event_id = str(item.get("kind") or ""), str(item.get("source") or ""), str(item.get("source_event_id") or "")
            if not kind or not source or not source_event_id:
                raise ValueError("Observation kind, source, and source_event_id are required.")
            event_time = _time(item["event_at"])
            receive_time = _time(item.get("received_at") or now)
            if event_time > receive_time + timedelta(minutes=5):
                raise ValueError("Future observation timestamp exceeds the scientific tolerance.")
            body = dict(item.get("payload") or {})
            raw_fingerprint = canonical_hash(body)
            prepared.append({
                "observation_id": f"obs-{canonical_hash({'source': source, 'source_event_id': source_event_id, 'payload': raw_fingerprint})[:28]}",
                "kind": kind, "source": source, "source_event_id": source_event_id, "wallet": item.get("wallet"),
                "symbol": item.get("symbol") or str(body.get("symbol") or "") or None, "event_at": _iso(event_time),
                "received_at": _iso(receive_time), "normalized_at": _iso(event_time),
                "network": str(item.get("network") or "mainnet-public"), "raw_fingerprint": raw_fingerprint,
                "schema_version": 1, "code_sha": self._code_sha, "config_hash": self._config_hash,
                "quality_flags": dict(item.get("quality_flags") or {}), "payload": body, "persisted_at": _iso(now),
            })
        rows = self.repository.record_observations_batch(prepared)
        work_time = _iso(now)
        feature_work = [{
            "work_id": f"work-{canonical_hash({'stage': WorkerStage.FEATURE_MATERIALIZATION.value, 'subject': 'observation', 'id': row['observation_id'], 'version': 1, 'fingerprint': row['raw_fingerprint']})[:28]}",
            "work_type": WorkerStage.FEATURE_MATERIALIZATION.value, "subject_type": "observation", "subject_id": row["observation_id"],
            "subject_version": 1, "priority": _WORK_PRIORITIES[WorkerStage.FEATURE_MATERIALIZATION], "created_at": work_time,
            "available_at": work_time, "max_attempts": self.settings.max_attempts, "input_fingerprint": row["raw_fingerprint"],
        } for row in rows]
        if schedule_features:
            self.repository.enqueue_work_batch(feature_work)
        if rows:
            self.repository.set_stage_health(WorkerStage.OBSERVATION_INGEST.value, "ACTIVE", detail={"observation_count": len(rows), "kinds": sorted({row["kind"] for row in rows})}, updated_at=work_time)
        return rows

    # ----- worker lifecycle -----------------------------------------------------------
    def run_once(self, *, max_items: int | None = None) -> dict[str, object]:
        with self.repository.session():
            return self._run_once(max_items=max_items)

    def _run_once(self, *, max_items: int | None = None) -> dict[str, object]:
        now = _utc_now()
        recovered = self.repository.recover_expired_leases(now=_iso(now))
        self._ensure_definitions(now)
        if self.repository.worker_control()["paused"]:
            self.repository.set_stage_health("Scientific Worker", "PAUSED", detail=self.repository.worker_control(), updated_at=_iso(now))
            return {"state": "PAUSED", "recovered_leases": recovered, "processed": 0, "queue": self.repository.work_queue_status(now=_iso(now))}
        free = shutil.disk_usage(self.repository.path.parent).free
        if free < self.settings.minimum_hot_free_bytes:
            self.repository.set_worker_paused(True, reason="RESEARCH_PAUSED_STORAGE", updated_at=_iso(now))
            self.repository.set_stage_health("Scientific Worker", "RESEARCH_PAUSED_STORAGE", detail={"hot_free_bytes": free}, updated_at=_iso(now))
            return {"state": "RESEARCH_PAUSED_STORAGE", "recovered_leases": recovered, "processed": 0, "queue": self.repository.work_queue_status(now=_iso(now))}
        budget = max_items if max_items is not None else self.settings.max_batch_size
        self._schedule_incremental_work(now)
        processed, failures = 0, 0
        for _ in range(max(0, budget)):
            current = _utc_now()
            work = self.repository.claim_work(worker_id=self.worker_id, now=_iso(current), lease_expires_at=_iso(current + timedelta(seconds=self.settings.lease_seconds)))
            if work is None:
                break
            try:
                result = self._handle(work, current)
                self.repository.complete_work(str(work["work_id"]), worker_id=self.worker_id, completed_at=_iso(_utc_now()), result_reference=str(result or ""))
                processed += 1
            except ValueError as exc:
                self.repository.fail_work(str(work["work_id"]), worker_id=self.worker_id, available_at=_iso(_utc_now()), error_class="SCIENTIFIC_INVALID", message_redacted=str(exc), permanent=True)
                failures += 1
            except (OSError, TimeoutError) as exc:
                self.repository.fail_work(str(work["work_id"]), worker_id=self.worker_id, available_at=_iso(_utc_now() + timedelta(seconds=self.settings.poll_interval_seconds)), error_class="TEMPORARY_IO", message_redacted=str(exc))
                failures += 1
            except Exception as exc:  # crash-safe classification; details remain redacted
                self.repository.fail_work(str(work["work_id"]), worker_id=self.worker_id, available_at=_iso(_utc_now()), error_class="WORKER_CRASH_RECOVERY", message_redacted=type(exc).__name__)
                failures += 1
        # A single post-batch schedule coalesces dependent work after a
        # bounded burst of writes.  Scheduling after every feature would make
        # every intermediate snapshot look like a new experiment batch.
        self._schedule_incremental_work(_utc_now())
        self._write_journal(_utc_now())
        self.repository.set_stage_health("Scientific Worker", "ACTIVE", detail={"processed": processed, "failures": failures, "hot_free_bytes": free}, updated_at=_iso(_utc_now()))
        return {"state": "ACTIVE", "recovered_leases": recovered, "processed": processed, "failures": failures, "queue": self.repository.work_queue_status(now=_iso(_utc_now()))}

    def run_until_idle(self, *, max_cycles: int = 128) -> dict[str, object]:
        summary: dict[str, object] = {"cycles": 0, "processed": 0, "failures": 0}
        for _ in range(max_cycles):
            result = self.run_once()
            summary["cycles"] = int(summary["cycles"]) + 1
            summary["processed"] = int(summary["processed"]) + int(result.get("processed", 0))
            summary["failures"] = int(summary["failures"]) + int(result.get("failures", 0))
            states = result.get("queue", {}).get("states", {}) if isinstance(result.get("queue"), Mapping) else {}
            if not int(result.get("processed", 0)) and not any(states.get(name, 0) for name in ("PENDING", "RETRYABLE")):
                break
        return summary

    def pause(self, reason: str = "operator requested scientific pause") -> None:
        self.repository.set_worker_paused(True, reason=reason, updated_at=_iso(_utc_now()))

    def resume(self) -> None:
        self.repository.set_worker_paused(False, reason="", updated_at=_iso(_utc_now()))

    # ----- scheduling -----------------------------------------------------------------
    def _enqueue(self, stage: WorkerStage, subject_type: str, subject_id: str, subject_version: int, fingerprint: str,
                 *, supersede_available: bool = False) -> None:
        work_id = f"work-{canonical_hash({'stage': stage.value, 'subject': subject_type, 'id': subject_id, 'version': subject_version, 'fingerprint': fingerprint})[:28]}"
        now = _iso(_utc_now())
        if supersede_available:
            self.repository.supersede_available_work(
                work_type=stage.value, subject_type=subject_type, subject_id=subject_id,
                subject_version=subject_version, keep_fingerprint=fingerprint,
            )
        self.repository.enqueue_work(work_id, work_type=stage.value, subject_type=subject_type, subject_id=subject_id,
                                     subject_version=subject_version, priority=_WORK_PRIORITIES.get(stage, 1), created_at=now,
                                     available_at=now, max_attempts=self.settings.max_attempts, input_fingerprint=fingerprint)

    def _schedule_incremental_work(self, now: datetime) -> None:
        observations = self.repository.list_observations(limit=1_000)
        if self.config.commissioning.enabled:
            selection = self.repository.get_watermark("d7_historical_corpus_selection")
            selected_ids = set(selection.get("details", {}).get("selected_observation_ids", [])) if selection else set()
            # D.7 archive data is retained in full but its bounded corpus is
            # the only historical source allowed to create new D.6 projection
            # work. Live observations remain eligible for forward shadow.
            if selected_ids:
                historical = self.repository.observations_by_ids(tuple(sorted(selected_ids)))
                live = self.repository.recent_observations_excluding_source(
                    source="HISTORICAL_OFFICIAL_ARCHIVE", limit=1_000,
                )
                observations = sorted([*historical, *live], key=lambda row: (str(row["normalized_at"]), str(row["observation_id"])))
        observation_fingerprint = canonical_hash([(row["observation_id"], row["raw_fingerprint"]) for row in observations])
        labels = self.repository.list_outcome_labels()
        labelled = {(row["observation_id"], int(row["horizon_seconds"])) for row in labels}
        if observations and observation_fingerprint != self._last_observation_schedule_fingerprint:
            for observation in observations[-self.settings.max_batch_size:]:
                if observation["kind"] != "WALLET_FILL":
                    continue
                if all((str(observation["observation_id"]), int(horizon)) in labelled for horizon in self.settings.horizons_seconds):
                    continue
                market_fingerprint = canonical_hash([(row["observation_id"], row["raw_fingerprint"]) for row in observations if row["kind"] == "MARKET_PRICE" and row.get("symbol") == observation.get("symbol")])
                self._enqueue(WorkerStage.OUTCOME_LABEL, "observation", str(observation["observation_id"]), 1, market_fingerprint,
                              supersede_available=True)
            self._last_observation_schedule_fingerprint = observation_fingerprint
        workflow_fingerprint = self._workflow_fingerprint(observations=observations, labels=labels)
        if observations and workflow_fingerprint != self._last_workflow_schedule_fingerprint:
            for stage, subject_type, subject_id in (
                (WorkerStage.PATTERN_DISCOVERY, "family", "initial-interpretable"),
                (WorkerStage.HISTORICAL_EXPERIMENT, "family", "initial-interpretable"),
                (WorkerStage.FORWARD_RESOLUTION, "prediction-scan", "all"),
                (WorkerStage.INDICATOR_PROMOTION, "promotion-scan", "all"),
                (WorkerStage.MODEL_BUILD, "model-scan", "all"),
                (WorkerStage.MODEL_CALIBRATION, "model-calibration", "all"),
                (WorkerStage.DRIFT_EVALUATION, "drift-scan", "all"),
            ):
                self._enqueue(stage, subject_type, subject_id, 1, workflow_fingerprint, supersede_available=True)
            self._last_workflow_schedule_fingerprint = workflow_fingerprint
        forward_hypotheses = self.repository.list_hypotheses(state="FORWARD_SHADOW")
        forward_fingerprint = canonical_hash({
            "observations": observation_fingerprint,
            "features": [(row["feature_value_id"], row["data_fingerprint"]) for row in self.repository.list_feature_values()],
            "hypotheses": [(row["hypothesis_id"], row["version"], row["config_hash"]) for row in forward_hypotheses],
        })
        if forward_hypotheses and forward_fingerprint != self._last_forward_schedule_fingerprint:
            for observation in observations[-self.settings.max_batch_size:]:
                if observation["kind"] != "WALLET_FILL":
                    continue
                feature_fingerprint = canonical_hash({"observation": observation["observation_id"], "hypotheses": forward_hypotheses})
                self._enqueue(WorkerStage.FORWARD_PREDICTION, "observation", str(observation["observation_id"]), 1, feature_fingerprint,
                              supersede_available=True)
            self._last_forward_schedule_fingerprint = forward_fingerprint
        self._enqueue(WorkerStage.ARCHIVAL, "journal", now.date().isoformat(), 1, now.date().isoformat())

    def _research_fingerprint(self, observations: list[dict[str, Any]] | None = None) -> str:
        """Fingerprint exactly the evidence a research-stage handler may read.

        This prevents an old queued experiment from silently evaluating newer
        data and provides a deterministic coalescing key while ingestion is
        active.
        """
        observations = observations if observations is not None else self.repository.list_observations(limit=1_000)
        features = self.repository.list_feature_values()
        labels = self.repository.list_outcome_labels()
        return canonical_hash({
            "observations": [(row["observation_id"], row["raw_fingerprint"]) for row in observations],
            "features": [(row["feature_value_id"], row["data_fingerprint"]) for row in features],
            "labels": [(row["label_id"], row["payload_hash"]) for row in labels],
            "forward": [(row["prediction_id"], row["realized_at"]) for row in self.repository.list_forward_records()],
        })

    def _workflow_fingerprint(self, *, observations: list[dict[str, Any]] | None = None,
                              labels: list[dict[str, Any]] | None = None) -> str:
        """Fingerprint evidence plus durable object state for downstream work."""
        observations = observations if observations is not None else self.repository.list_observations(limit=1_000)
        labels = labels if labels is not None else self.repository.list_outcome_labels()
        evidence = canonical_hash({
            "observations": [(row["observation_id"], row["raw_fingerprint"]) for row in observations],
            "features": [(row["feature_value_id"], row["data_fingerprint"]) for row in self.repository.list_feature_values()],
            "labels": [(row["label_id"], row["payload_hash"]) for row in labels],
            "forward": [(row["prediction_id"], row["realized_at"]) for row in self.repository.list_forward_records()],
        })
        return canonical_hash({
            "evidence": evidence,
            "hypotheses": [(row["hypothesis_id"], row["version"], row["state"], row["config_hash"]) for row in self.repository.list_hypotheses()],
            "indicators": [(row["indicator_id"], row["version"], row["state"]) for row in self.repository.list_indicators()],
            "models": [(row["model_id"], row["version"], row["state"]) for row in self.repository.list_models()],
        })

    # ----- feature definitions and materialization ------------------------------------
    def _ensure_definitions(self, now: datetime) -> None:
        if self._definitions_ready:
            return
        existing_features = {item["feature_id"]: item for item in self.repository.list_features() if int(item["version"]) == 1}
        for feature_id in WALLET_FEATURES + MARKET_FEATURES:
            family = "wallet" if feature_id in WALLET_FEATURES else "market"
            required = ("wallet_fill",) if family == "wallet" else ("market_price",)
            created_at = str(existing_features.get(feature_id, {}).get("created_at", self._definition_created_at))
            definition = FeatureDefinition(feature_id, 1, f"D.6 deterministic {feature_id} from normalized public {family} evidence.", "unitless", required, 600.0, "MISSING means required source evidence was unavailable; never imputed as zero.", created_at, self._code_sha, family)
            self.features.register(definition)
        family = SearchFamily("initial-interpretable", 1, ("wallet_action", "wallet_disagreement", "short_term_return", "local_momentum"), (self.settings.horizons_seconds[0],), 1, self.settings.minimum_sample, self.settings.max_proposals_per_family_per_cycle, self.settings.minimum_effect_size)
        self.repository.register_search_family(family.family_id, family.version, family.payload(), created_at=_iso(now))
        self._definitions_ready = True

    def _handle(self, work: Mapping[str, Any], now: datetime) -> str:
        stage = WorkerStage(str(work["work_type"]))
        self.repository.set_stage_health(stage.value, "ACTIVE", detail={"work_id": work["work_id"]}, updated_at=_iso(now))
        research_stages = {
            WorkerStage.PATTERN_DISCOVERY, WorkerStage.HISTORICAL_EXPERIMENT,
            WorkerStage.FORWARD_RESOLUTION, WorkerStage.INDICATOR_PROMOTION,
            WorkerStage.MODEL_BUILD, WorkerStage.MODEL_CALIBRATION,
            WorkerStage.DRIFT_EVALUATION,
        }
        if stage in research_stages and str(work["input_fingerprint"]) != self._workflow_fingerprint():
            # A newer work item has been queued with the complete current
            # evidence fingerprint.  Never let this item read beyond the
            # evidence snapshot for which it was scheduled.
            return "superseded by newer scientific evidence"
        if stage is WorkerStage.FEATURE_MATERIALIZATION:
            return self._materialize_features(str(work["subject_id"]), now)
        if stage is WorkerStage.OUTCOME_LABEL:
            return self._label_outcomes(str(work["subject_id"]), now)
        if stage is WorkerStage.PATTERN_DISCOVERY:
            return self._discover(now)
        if stage is WorkerStage.HISTORICAL_EXPERIMENT:
            return self._evaluate_hypotheses(now)
        if stage is WorkerStage.FORWARD_PREDICTION:
            return self._emit_predictions(str(work["subject_id"]), now)
        if stage is WorkerStage.FORWARD_RESOLUTION:
            return self._resolve_predictions(now)
        if stage is WorkerStage.INDICATOR_PROMOTION:
            return self._promote_indicators(now)
        if stage is WorkerStage.MODEL_BUILD:
            return self._build_models(now)
        if stage is WorkerStage.MODEL_CALIBRATION:
            return self._calibrate_models(now)
        if stage is WorkerStage.SHADOW_DECISION:
            return self._shadow_decisions(now)
        if stage is WorkerStage.DRIFT_EVALUATION:
            return self._evaluate_drift(now)
        if stage is WorkerStage.ARCHIVAL:
            return "journal updated; cold flush remains an explicit background CLI action"
        raise ValueError(f"Unsupported scientific work stage {stage}.")

    def _materialize_features(self, observation_id: str, now: datetime) -> str:
        observation = self.repository.observation_by_id(observation_id)
        if observation is None:
            raise ValueError("Feature materialization requires a persisted observation.")
        if self.config.commissioning.enabled and observation["source"] == "HISTORICAL_OFFICIAL_ARCHIVE":
            all_observations = self.repository.observations_before(
                symbol=observation.get("symbol"), end=str(observation["normalized_at"]), lookback_seconds=600,
                source="HISTORICAL_OFFICIAL_ARCHIVE",
            )
        else:
            all_observations = self.repository.list_observations(limit=5_000)
        with self.latency.measure("observation_to_feature", observation_id=observation_id):
            values = self._feature_values(observation, all_observations)
            rows: list[dict[str, Any]] = []
            for feature_id in WALLET_FEATURES + MARKET_FEATURES:
                value, sources = values.get(feature_id, (None, (observation_id,)))
                fingerprint = canonical_hash({"feature": feature_id, "observation": observation["raw_fingerprint"], "sources": [(item["observation_id"], item["raw_fingerprint"]) for item in all_observations if item["observation_id"] in sources]})
                feature_value_id = f"fv-{canonical_hash({'feature': feature_id, 'observation': observation_id, 'fingerprint': fingerprint})[:28]}"
                rows.append({"feature_value_id": feature_value_id, "feature_id": feature_id, "feature_version": 1,
                             "observation_id": observation_id, "value": value, "missing": value is None,
                             "source_observation_ids": tuple(sources), "data_fingerprint": fingerprint,
                             "materialized_at": _iso(now)})
            self.repository.record_feature_values(rows)
        self.repository.set_watermark("last_feature_materialization", f"{observation['normalized_at']}|{observation_id}", updated_at=_iso(now), details={"observation_id": observation_id})
        return f"features:{observation_id}"

    def _feature_values(self, observation: Mapping[str, Any], all_observations: list[dict[str, Any]]) -> dict[str, tuple[float | None, tuple[str, ...]]]:
        payload, kind, symbol = observation["payload"], str(observation["kind"]), observation.get("symbol")
        event_time = _time(str(observation["normalized_at"]))
        wallet_rows = [row for row in all_observations if row["kind"] == "WALLET_FILL" and row.get("symbol") == symbol and _time(str(row["normalized_at"])) <= event_time]
        market_rows = [row for row in all_observations if row["kind"] == "MARKET_PRICE" and row.get("symbol") == symbol and _time(str(row["normalized_at"])) <= event_time]
        side = str(payload.get("side") or payload.get("action") or "").lower()
        action = 1.0 if side in {"buy", "long", "open_long"} else -1.0 if side in {"sell", "short", "open_short"} else None
        prices = [(row, _finite(row["payload"].get("price"))) for row in market_rows]
        prices = [(row, value) for row, value in prices if value and value > 0]
        short_return = None
        if len(prices) >= 2:
            short_return = prices[-1][1] / prices[-2][1] - 1.0
        same_window = [row for row in wallet_rows if (event_time - _time(str(row["normalized_at"]))).total_seconds() <= 60]
        signs = []
        for row in same_window:
            item = str(row["payload"].get("side") or row["payload"].get("action") or "").lower()
            if item in {"buy", "long", "open_long"}: signs.append(1.0)
            elif item in {"sell", "short", "open_short"}: signs.append(-1.0)
        convergence = float(sum(1 for sign in signs if action is not None and sign == action)) if action is not None else None
        disagreement = (sum(1 for sign in signs if action is not None and sign != action) / len(signs)) if signs and action is not None else None
        latency = max(0.0, (_time(str(observation["received_at"])) - event_time).total_seconds())
        prior_sizes = [_finite(row["payload"].get("notional")) for row in wallet_rows[:-1]]
        prior_sizes = [value for value in prior_sizes if value is not None and value > 0]
        notional = _finite(payload.get("notional"))
        size_relative = notional / fmean(prior_sizes) if notional and prior_sizes else None
        volatility = pstdev([value for _, value in prices[-10:]]) / prices[-1][1] if len(prices) >= 3 else None
        result: dict[str, tuple[float | None, tuple[str, ...]]] = {name: (None, (str(observation["observation_id"]),)) for name in WALLET_FEATURES + MARKET_FEATURES}
        if kind == "WALLET_FILL":
            result.update({"wallet_action": (action, (str(observation["observation_id"]),)), "wallet_action_freshness": (math.exp(-latency / 60), (str(observation["observation_id"]),)), "wallet_convergence_count": (convergence, tuple(str(row["observation_id"]) for row in same_window)), "weighted_wallet_convergence": (convergence, tuple(str(row["observation_id"]) for row in same_window)), "wallet_disagreement": (disagreement, tuple(str(row["observation_id"]) for row in same_window)), "wallet_size_relative": (size_relative, tuple(str(row["observation_id"]) for row in wallet_rows[-10:])), "wallet_action_acceleration": (float(len(same_window)) / 60.0 if same_window else None, tuple(str(row["observation_id"]) for row in same_window)), "wallet_repeat_entry_cadence": ((event_time - _time(str(wallet_rows[-2]["normalized_at"]))).total_seconds() if len(wallet_rows) >= 2 else None, tuple(str(row["observation_id"]) for row in wallet_rows[-2:])), "wallet_regime_specialization": (1.0 if volatility is not None else None, tuple(str(row["observation_id"]) for row in market_rows[-10:])), "wallet_symbol_specialization": (1.0 / max(1, len({row.get("symbol") for row in wallet_rows})), tuple(str(row["observation_id"]) for row in wallet_rows)), "wallet_estimated_hold_horizon": (None, (str(observation["observation_id"]),))})
        if kind == "MARKET_PRICE":
            result.update({"short_term_return": (short_return, tuple(str(row["observation_id"]) for row, _ in prices[-2:])), "realized_volatility": (volatility, tuple(str(row["observation_id"]) for row, _ in prices[-10:])), "volatility_expansion": (volatility, tuple(str(row["observation_id"]) for row, _ in prices[-10:])), "local_price_acceleration": (short_return, tuple(str(row["observation_id"]) for row, _ in prices[-3:])), "local_momentum": (short_return, tuple(str(row["observation_id"]) for row, _ in prices[-3:])), "market_regime": (1.0 if volatility is not None and volatility > 0.002 else 0.0 if volatility is not None else None, tuple(str(row["observation_id"]) for row, _ in prices[-10:])), "spread": (_finite(payload.get("spread")), (str(observation["observation_id"]),)), "volume_acceleration": (_finite(payload.get("volume")), (str(observation["observation_id"]),)), "liquidity_depth_proxy": (_finite(payload.get("liquidity_depth")), (str(observation["observation_id"]),)), "liquidation_activity": (_finite(payload.get("liquidations")), (str(observation["observation_id"]),))})
        return result

    # ----- labels, discovery, and experiments -----------------------------------------
    def _label_outcomes(self, observation_id: str, now: datetime) -> str:
        anchor = self.repository.observation_by_id(observation_id)
        if anchor is None or anchor["kind"] != "WALLET_FILL":
            return "not an outcome anchor"
        anchor_time, symbol = _time(str(anchor["normalized_at"])), anchor.get("symbol")
        historical = self.config.commissioning.enabled and anchor["source"] == "HISTORICAL_OFFICIAL_ARCHIVE"
        market = ([] if historical else [item for item in self.repository.list_observations(limit=5_000)
                                         if item["kind"] == "MARKET_PRICE" and item.get("symbol") == symbol])
        start_price = _finite(anchor["payload"].get("price"))
        if start_price is None:
            prior = ([item for item in market if _time(str(item["normalized_at"])) <= anchor_time]
                     if not historical else self.repository.observations_before(
                         symbol=symbol, end=str(anchor["normalized_at"]), lookback_seconds=600,
                         source="HISTORICAL_OFFICIAL_ARCHIVE", kinds=("MARKET_PRICE",)))
            start_price = _finite(prior[-1]["payload"].get("price")) if prior else None
        if not start_price or start_price <= 0:
            return "awaiting anchor price"
        resolved = 0
        for horizon in self.settings.horizons_seconds:
            if historical:
                end = self.repository.first_market_price_at_or_after(
                    symbol=symbol, at=_iso(anchor_time + timedelta(seconds=horizon)), source="HISTORICAL_OFFICIAL_ARCHIVE",
                )
                if end is None or _finite(end["payload"].get("price")) is None:
                    continue
                path = [end]
            else:
                end_candidates = [item for item in market if _time(str(item["normalized_at"])) >= anchor_time + timedelta(seconds=horizon) and _finite(item["payload"].get("price"))]
                if not end_candidates:
                    continue
                end = end_candidates[0]
                path = [item for item in market if anchor_time <= _time(str(item["normalized_at"])) <= _time(str(end["normalized_at"])) and _finite(item["payload"].get("price"))]
            end_price = float(_finite(end["payload"].get("price")) or start_price)
            gross_long = end_price / start_price - 1.0
            direction = 1.0 if str(anchor["payload"].get("side") or anchor["payload"].get("action") or "").lower() in {"buy", "long", "open_long"} else -1.0
            path_returns = [(float(_finite(item["payload"].get("price")) or start_price) / start_price - 1.0) * direction for item in path]
            cost = float(anchor["payload"].get("estimated_cost", 0.001))
            net = gross_long * direction - cost
            payload = {"forward_gross_return": gross_long, "long_return": gross_long, "short_return": -gross_long,
                       "maximum_favorable_excursion": max(path_returns, default=0.0), "maximum_adverse_excursion": min(path_returns, default=0.0),
                       "time_to_mfe_seconds": horizon, "time_to_mae_seconds": horizon, "end_of_horizon_return": gross_long,
                       "transaction_cost_estimate": cost, "arrival_time_cost": 0.0, "net_outcome": net,
                       "market_observation_id": end["observation_id"], "direction": "long" if direction > 0 else "short"}
            label_id = f"label-{canonical_hash({'observation': observation_id, 'horizon': horizon, 'end': end['observation_id']})[:28]}"
            self.repository.record_outcome_label(label_id, observation_id=observation_id, horizon_seconds=int(horizon), resolved_at=str(end["normalized_at"]), payload=payload)
            resolved += 1
        self.repository.set_watermark("last_forward_resolution_scan", f"{anchor['normalized_at']}|{observation_id}", updated_at=_iso(now), details={"labels": resolved})
        return f"labels:{resolved}"

    def _research_records(self) -> list[dict[str, Any]]:
        selection = self.repository.get_watermark("d7_historical_corpus_selection") if self.config.commissioning.enabled else None
        selected_ids = tuple(sorted(selection.get("details", {}).get("selected_observation_ids", []))) if selection else ()
        source_rows = (self.repository.observations_by_ids(selected_ids) if selected_ids
                       else self.repository.list_observations(limit=5_000))
        observations = {item["observation_id"]: item for item in source_rows}
        feature_values = self.repository.list_feature_values()
        features: dict[str, dict[str, float]] = {}
        for value in feature_values:
            if not value["missing"] and isinstance(value["value"], (int, float)):
                features.setdefault(str(value["observation_id"]), {})[str(value["feature_id"])] = float(value["value"])
        records = []
        for label in self.repository.list_outcome_labels():
            observation = observations.get(label["observation_id"])
            if observation and label["payload"].get("net_outcome") is not None:
                records.append({"observation_id": label["observation_id"], "timestamp": observation["normalized_at"], "symbol": observation.get("symbol"), "horizon_seconds": label["horizon_seconds"], "features": features.get(label["observation_id"], {}), "net_outcome": float(label["payload"]["net_outcome"]), "outcome": label["payload"]})
        return sorted(records, key=lambda item: (item["timestamp"], item["observation_id"]))[-2_000:]

    def _commissioning_snapshot(self) -> dict[str, Any] | None:
        """Return a D.7 corpus only when its declared coverage policy passed.

        Pre-D.7 fixture configurations deliberately keep commissioning off, so
        their frozen D.6 behavior is unchanged.  Production D.7 research is
        fail-closed: a hypothesis cannot be promoted from unproven, partial,
        missing, or corrupt historical evidence.
        """
        if not self.config.commissioning.enabled:
            return {"corpus_fingerprint": "d6-compatibility-local-evidence", "payload": {"coverage": {"state": "COMPATIBILITY"}}}
        snapshot = self.repository.latest_corpus_snapshot()
        coverage = snapshot.get("payload", {}).get("coverage", {}) if snapshot else {}
        if (not snapshot or coverage.get("state") != "PROVEN_COMPLETE"
                or float(coverage.get("coverage_fraction") or 0.0) < self.config.commissioning.min_coverage_fraction):
            return None
        return snapshot

    def _discover(self, now: datetime) -> str:
        snapshot = self._commissioning_snapshot()
        if snapshot is None:
            self.repository.set_stage_health(WorkerStage.PATTERN_DISCOVERY.value, "COVERAGE_BLOCKED",
                                             detail={"reason": "D.7 corpus coverage is not PROVEN_COMPLETE"}, updated_at=_iso(now))
            return "coverage blocked"
        records = self._research_records()
        family = SearchFamily("initial-interpretable", 1, ("wallet_action", "wallet_disagreement", "short_term_return", "local_momentum"), (self.settings.horizons_seconds[0],), 1, self.settings.minimum_sample, self.settings.max_proposals_per_family_per_cycle, self.settings.minimum_effect_size)
        candidates = self.discovery.discover(family, records)
        registered = 0
        day_start = _iso(now.replace(hour=0, minute=0, second=0, microsecond=0))
        today_registered = sum(
            1 for item in self.repository.list_hypotheses()
            if str(item.get("registered_at", "")) >= day_start
            and item["definition"].get("multiple_testing_family") == family.family_id
        )
        existing_hypotheses = {(item["hypothesis_id"], int(item["version"])) for item in self.repository.list_hypotheses()}
        for candidate in candidates:
            self.repository.record_discovery(candidate.discovery_id, family_id=family.family_id, family_version=family.version, state="PROPOSED", payload=candidate.payload(), created_at=_iso(now))
            if today_registered + registered >= self.settings.max_registered_per_family_per_day:
                continue
            hypothesis_id = f"hypothesis-{canonical_hash(candidate.payload())[:24]}"
            if (hypothesis_id, 1) in existing_hypotheses:
                continue
            definition = self._hypothesis_from_candidate(candidate, records, now, corpus_fingerprint=str(snapshot["corpus_fingerprint"]))
            if self.hypotheses.similar_rejections(definition, minimum_similarity=0.95):
                continue
            self.hypotheses.register(definition, state=HypothesisState.REGISTERED)
            registered += 1
        self.repository.set_watermark("last_observation_processed", records[-1]["timestamp"] if records else "", updated_at=_iso(now), details={"records": len(records), "registered": registered})
        return f"discoveries:{len(candidates)} registered:{registered}"

    def _hypothesis_from_candidate(self, candidate: Any, records: list[dict[str, Any]], now: datetime, *, corpus_fingerprint: str = "d6-compatibility-local-evidence") -> HypothesisDefinition:
        start = records[0]["timestamp"] if records else _iso(now)
        end = records[-1]["timestamp"] if records else _iso(now)
        split_index = max(1, int(len(records) * 0.75))
        discovery_end = records[split_index - 1]["timestamp"] if records else end
        validation_start = _iso(_time(str(discovery_end)) + timedelta(seconds=float(candidate.horizon_seconds)))
        hypothesis_id = f"hypothesis-{canonical_hash(candidate.payload())[:24]}"
        return HypothesisDefinition(hypothesis_id, 1, f"{candidate.feature_id} {candidate.condition} {candidate.threshold:.6g}", f"{candidate.feature_id} {candidate.condition} threshold predicts positive cost-adjusted {candidate.horizon_seconds}s outcome", "Condition has no positive net effect after costs.", "Condition has positive net effect after costs.", ((candidate.feature_id, candidate.feature_version),), {"condition": candidate.condition, "threshold": candidate.threshold, "discovery_id": candidate.discovery_id}, tuple(sorted({str(record.get("symbol") or "") for record in records if record.get("symbol")})), ("unknown", "calm", "volatile"), candidate.horizon_seconds, f"{candidate.feature_id} {candidate.condition} {candidate.threshold}", "cost-adjusted directional outcome", {"transaction_cost": 0.001}, 0, {"fee": 0.001}, {"slippage": 0.0}, self.settings.minimum_sample, {"start": start, "end": discovery_end}, {"start": validation_start, "end": end}, float(candidate.horizon_seconds), {"minimum_effect_size": self.settings.minimum_effect_size, "maximum_q_value": self.settings.maximum_q_value, "coverage_policy": "PROVEN_COMPLETE" if self.config.commissioning.enabled else "D6_COMPATIBILITY"}, {"net_expectancy": 0.0}, candidate.family_id, _iso(now), self._code_sha, {"discovery": candidate.data_fingerprint, "corpus": corpus_fingerprint}, tags=("automated", "interpretable", candidate.feature_id))

    def _evaluate_hypotheses(self, now: datetime) -> str:
        snapshot = self._commissioning_snapshot()
        if snapshot is None:
            self.repository.set_stage_health(WorkerStage.HISTORICAL_EXPERIMENT.value, "COVERAGE_BLOCKED",
                                             detail={"reason": "D.7 corpus coverage is not PROVEN_COMPLETE"}, updated_at=_iso(now))
            return "coverage blocked"
        records = self._research_records()
        candidates = self.repository.list_hypotheses(state="REGISTERED")[:self.settings.max_historical_tests_per_cycle]
        pending: list[tuple[dict[str, Any], list[dict[str, Any]], str]] = []
        for hypothesis in candidates:
            definition = hypothesis["definition"]
            if self.config.commissioning.enabled and definition.get("data_fingerprints", {}).get("corpus") != snapshot["corpus_fingerprint"]:
                continue
            thresholds = definition.get("thresholds", {})
            feature = definition.get("feature_versions", [{}])[0].get("feature_id")
            condition, threshold = thresholds.get("condition"), float(thresholds.get("threshold", 0.0))
            frozen_start = str(definition["discovery_range"]["start"])
            frozen_end = str(definition["validation_range"]["end"])
            filtered = [record for record in records if frozen_start <= str(record["timestamp"]) <= frozen_end and feature in record["features"] and ((condition == "above" and record["features"][feature] > threshold) or (condition == "below" and record["features"][feature] <= threshold)) and record["horizon_seconds"] == int(definition["prediction_horizon_seconds"])]
            if len(filtered) < self.settings.minimum_sample:
                continue
            experiment_id = f"experiment-{canonical_hash({'hypothesis': hypothesis['config_hash'], 'records': [record['observation_id'] for record in filtered]})[:24]}"
            config = {"temporal_ordered": True, "purge_embargo_seconds": definition["purge_embargo_seconds"], "family": definition["multiple_testing_family"], "validation_fraction": 0.33, "cost_adjusted": True, "corpus_snapshot": snapshot["corpus_fingerprint"]}
            fingerprint = canonical_hash({"corpus_snapshot": snapshot["corpus_fingerprint"], "scientific_records": HistoricalExperimentEngine.fingerprint(filtered, config)})
            self.repository.create_experiment(experiment_id, hypothesis_id=hypothesis["hypothesis_id"], hypothesis_version=hypothesis["version"], kind="HISTORICAL", state="RUNNING", dataset_fingerprint=fingerprint, configuration=config, created_at=_iso(now))
            object_definition = self._hypothesis_from_payload(definition)
            self.hypotheses.transition(object_definition, from_state=HypothesisState.REGISTERED, to_state=HypothesisState.HISTORICAL_TESTING, reason="automated bounded historical evaluation", event_id=f"event-{experiment_id}", created_at=_iso(now), evidence={"records": len(filtered)})
            # Chronological train/validation split with a prediction-horizon
            # purge.  No overlapping outcome window may appear on both sides.
            split_index = max(1, int(len(filtered) * 0.75))
            training_records = filtered[:split_index]
            if len(training_records) < self.settings.minimum_sample:
                continue
            train_end = _time(str(training_records[-1]["timestamp"]))
            validation = [record for record in filtered[split_index:]
                          if _time(str(record["timestamp"])) >= train_end + timedelta(seconds=float(definition["prediction_horizon_seconds"]))
                          and str(record["timestamp"]) >= str(definition["validation_range"]["start"])]
            historical_rows = [{"gross_return": record["net_outcome"] + record["outcome"]["transaction_cost_estimate"], "fee": record["outcome"]["transaction_cost_estimate"], "timestamp": record["timestamp"]} for record in training_records]
            pending.append((hypothesis, historical_rows, experiment_id + "|" + str(fmean(record["net_outcome"] for record in validation) if validation else -1.0)))
        family_inputs = [(item[2].split("|")[0], item[1], self.settings.minimum_sample) for item in pending]
        results = self.experiments.evaluate_family(family_inputs) if family_inputs else {}
        survivors, rejected = 0, 0
        for hypothesis, _, compound in pending:
            experiment_id, validation_expectancy = compound.split("|", 1)
            result = results[experiment_id]
            result["validation_net_expectancy"] = float(validation_expectancy)
            result["promotion_batch_complete"] = True
            self.experiments.persist_result(experiment_id, result, recorded_at=_iso(now))
            definition = self._hypothesis_from_payload(hypothesis["definition"])
            passed = result["sample_count"] >= self.settings.minimum_sample and result["net_expectancy"] > 0 and abs(result["effect_size"]) >= self.settings.minimum_effect_size and result["q_value"] <= self.settings.maximum_q_value and result["validation_net_expectancy"] > 0
            if passed:
                self.hypotheses.transition(definition, from_state=HypothesisState.HISTORICAL_TESTING, to_state=HypothesisState.FORWARD_SHADOW, reason="historical family completed with cost-adjusted validation pass", event_id=f"event-forward-{experiment_id}", created_at=_iso(now), evidence=result)
                survivors += 1
            else:
                self.hypotheses.reject(definition, experiment_id=experiment_id, reason=f"historical criteria failed: q={result['q_value']:.6g}, net={result['net_expectancy']:.6g}, validation={result['validation_net_expectancy']:.6g}", result=result, recorded_at=_iso(now), event_id=f"event-reject-{experiment_id}")
                rejected += 1
        return f"historical survivors:{survivors} rejected:{rejected}"

    def _hypothesis_from_payload(self, payload: Mapping[str, Any]) -> HypothesisDefinition:
        body = dict(payload)
        body["feature_versions"] = tuple((item["feature_id"], int(item["version"])) for item in body["feature_versions"])
        for name in ("symbol_scope", "regime_scope", "tags"):
            body[name] = tuple(body.get(name, ()))
        return HypothesisDefinition(**body)

    # ----- forward evidence, promotion, models, decisions, drift ----------------------
    def _emit_predictions(self, observation_id: str, now: datetime) -> str:
        observation = self.repository.observation_by_id(observation_id)
        if observation is None or observation["kind"] != "WALLET_FILL":
            return "no forward anchor"
        # Historical labels make an observation ineligible for forward shadow:
        # emitting after that outcome would be a retrospective prediction.
        labels = {(item["observation_id"], item["horizon_seconds"]) for item in self.repository.list_outcome_labels()}
        values = {item["feature_id"]: item["value"] for item in self.repository.list_feature_values(observation_ids=(observation_id,)) if not item["missing"]}
        emitted = 0
        for hypothesis in self.repository.list_hypotheses(state="FORWARD_SHADOW")[:self.settings.max_forward_shadow_candidates]:
            definition, threshold = hypothesis["definition"], hypothesis["definition"].get("thresholds", {})
            feature = definition["feature_versions"][0]["feature_id"]
            value, condition, cutoff = values.get(feature), threshold.get("condition"), float(threshold.get("threshold", 0.0))
            matches = value is not None and ((condition == "above" and value > cutoff) or (condition == "below" and value <= cutoff))
            if not matches:
                continue
            if (observation_id, int(definition["prediction_horizon_seconds"])) in labels:
                continue
            experiment = next((item for item in self.repository.list_experiments(kind="HISTORICAL") if item["hypothesis_id"] == hypothesis["hypothesis_id"] and item["hypothesis_version"] == hypothesis["version"]), None)
            if not experiment:
                continue
            prediction_id = f"prediction-{canonical_hash({'experiment': experiment['experiment_id'], 'observation': observation_id})[:24]}"
            direction = "long" if float(value) >= 0 else "short"
            prediction_features = {**values, "source_observation_id": observation_id}
            self.forward.predict(prediction_id, experiment_id=experiment["experiment_id"], predicted_at=observation["normalized_at"], market=observation.get("symbol") or "UNKNOWN", horizon_seconds=float(definition["prediction_horizon_seconds"]), features=prediction_features, predicted_direction=direction, predicted_net_edge=max(self.settings.minimum_edge, 0.001), trade_confidence=0.60, model_confidence=0.50, expected_costs=0.001)
            emitted += 1
        return f"forward_predictions:{emitted}"

    def _resolve_predictions(self, now: datetime) -> str:
        labels = {(item["observation_id"], item["horizon_seconds"]): item for item in self.repository.list_outcome_labels()}
        resolved = 0
        for prediction in self.repository.list_forward_records():
            if prediction["outcome"] is not None:
                continue
            payload = prediction["payload"]
            source_observation = payload.get("features", {}).get("source_observation_id")
            # A prediction may resolve only against the label for its own
            # persisted pre-outcome anchor; cross-observation matching leaks.
            candidates = [label for (obs_id, horizon), label in labels.items() if obs_id == source_observation and horizon == int(prediction["horizon_seconds"])]
            if not candidates:
                continue
            label = min(candidates, key=lambda item: abs((_time(item["resolved_at"]) - _time(prediction["predicted_at"])).total_seconds()))
            if _time(label["resolved_at"]) < _time(prediction["predicted_at"]) + timedelta(seconds=float(prediction["horizon_seconds"])):
                continue
            self.forward.resolve(prediction["prediction_id"], realized_at=label["resolved_at"], realized_net_outcome=float(label["payload"]["net_outcome"]), outcome_metadata=label["payload"])
            resolved += 1
        return f"forward_resolved:{resolved}"

    def _promote_indicators(self, now: datetime) -> str:
        records = self.repository.list_forward_records()
        promoted = 0
        for hypothesis in self.repository.list_hypotheses(state="FORWARD_SHADOW"):
            experiment = next((item for item in self.repository.list_experiments(kind="HISTORICAL") if item["hypothesis_id"] == hypothesis["hypothesis_id"]), None)
            if not experiment:
                continue
            forward = [item for item in records if item["experiment_id"] == experiment["experiment_id"] and item["outcome"] is not None]
            outcomes = [float(item["outcome"]["realized_net_outcome"]) for item in forward]
            if len(outcomes) < self.settings.minimum_forward_observations or fmean(outcomes) <= self.settings.minimum_forward_net_expectancy:
                continue
            result = experiment.get("result", {})
            provenance = IndicatorProvenance(hypothesis["hypothesis_id"], hypothesis["version"], experiment["experiment_id"], (experiment["experiment_id"],), tuple((item["feature_id"], int(item["version"])) for item in hypothesis["definition"]["feature_versions"]), {"type": "transparent_weighted_score"}, {"historical": experiment["dataset_fingerprint"]}, self._code_sha, ("unknown",), (), (), (), float(hypothesis["definition"]["prediction_horizon_seconds"]), tuple(self._alpha_curve(hypothesis["definition"])), {"passed": True}, {"forward_count": len(outcomes), "forward_net_expectancy": fmean(outcomes), "q_value": result.get("q_value")})
            indicator_id = f"indicator-{hypothesis['hypothesis_id']}"
            self.indicators.register(indicator_id, 1, provenance, state=IndicatorState.VALIDATED, created_at=_iso(now))
            self.hypotheses.transition(self._hypothesis_from_payload(hypothesis["definition"]), from_state=HypothesisState.FORWARD_SHADOW, to_state=HypothesisState.PROMOTED, reason="forward evidence reached configured validation gate", event_id=f"event-promote-{indicator_id}", created_at=_iso(now), evidence={"forward_count": len(outcomes), "net_expectancy": fmean(outcomes)})
            promoted += 1
        return f"indicators_promoted:{promoted}"

    def _alpha_curve(self, definition: Mapping[str, Any]) -> list[dict[str, float]]:
        """Use only supported persisted horizons; never invent sub-second decay."""
        thresholds = definition.get("thresholds", {})
        feature = definition.get("feature_versions", [{}])[0].get("feature_id")
        condition, threshold = thresholds.get("condition"), float(thresholds.get("threshold", 0.0))
        points: list[dict[str, float]] = []
        for horizon in self.settings.horizons_seconds:
            outcomes = [float(record["net_outcome"]) for record in self._research_records()
                        if record["horizon_seconds"] == horizon and feature in record["features"]
                        and ((condition == "above" and record["features"][feature] > threshold)
                             or (condition == "below" and record["features"][feature] <= threshold))]
            if len(outcomes) >= self.settings.minimum_sample:
                points.append({"age_seconds": float(horizon), "expected_net_edge": fmean(outcomes)})
        if not points:
            return []
        baseline = max(points[0]["expected_net_edge"], 1e-12)
        return [{**point, "alpha_survival": max(point["expected_net_edge"], 0.0) / baseline} for point in points]

    def _build_models(self, now: datetime) -> str:
        indicators = [item for item in self.repository.list_indicators() if item["state"] in {"VALIDATED", "ACTIVE"}]
        if not indicators:
            return "no validated indicators"
        inputs = tuple((item["indicator_id"], int(item["version"])) for item in indicators[:self.settings.max_active_candidate_models])
        fingerprint = canonical_hash(inputs)
        model_id = f"model-{fingerprint[:20]}"
        existing = next((item for item in self.repository.list_models()
                         if item["model_id"] == model_id and int(item["version"]) == 1), None)
        if existing is not None:
            return f"model:{model_id}:existing"
        feature_sets = {
            item["indicator_id"]: {entry["feature_id"] for entry in item["provenance"].get("feature_versions", [])}
            for item in indicators if item["indicator_id"] in {identifier for identifier, _ in inputs}
        }
        redundancy_matrix = {
            f"{left}|{right}": (len(feature_sets.get(left, set()) & feature_sets.get(right, set())) /
                                max(1, len(feature_sets.get(left, set()) | feature_sets.get(right, set()))))
            for left, _ in inputs for right, _ in inputs if left < right
        }
        redundancy_penalty = max(redundancy_matrix.values(), default=0.0)
        definition = ModelDefinition(model_id, 1, inputs, {"start": "immutable-historical"}, {"end": "immutable-validation"}, {"weights": {name: 1 / len(inputs) for name, _ in inputs}, "regularization": "L1-like complexity penalty", "redundancy_penalty": redundancy_penalty, "redundancy_matrix": redundancy_matrix}, {"state": "not_calibrated_until_forward_samples"}, self._code_sha, self._config_hash, {"selection": "simplest validated indicator set", "complexity": len(inputs), "redundancy_penalty": redundancy_penalty}, _iso(now))
        self.models.register(definition, state=ModelState.SHADOW)
        champion = next((item for item in self.repository.model_roles() if item["role"] == "CHAMPION"), None)
        role = "CHAMPION" if champion is None else "CHALLENGER"
        self.repository.assign_model_role(role, model_id, 1, evidence={"reason": "first validated transparent model" if role == "CHAMPION" else "new transparent candidate; compare forward evidence before replacement", "indicator_count": len(inputs), "redundancy_penalty": redundancy_penalty}, assigned_at=_iso(now))
        self._enqueue(WorkerStage.SHADOW_DECISION, "model", model_id, 1, fingerprint)
        return f"model:{model_id}"

    def _calibrate_models(self, now: datetime) -> str:
        """Persist empirical forward calibration; do not label it probability-calibrated early."""
        forward = self.repository.list_forward_records()
        persisted = 0
        for model in self.repository.list_models():
            inputs = {item["indicator_id"] for item in model["definition"].get("input_indicator_versions", [])}
            experiment_ids = {
                str(indicator["provenance"].get("historical_experiment_id"))
                for indicator in self.repository.list_indicators()
                if indicator["indicator_id"] in inputs
            }
            records = [item for item in forward if item["experiment_id"] in experiment_ids and item["outcome"] is not None]
            fingerprint = canonical_hash({"model": (model["model_id"], model["version"]), "predictions": [
                (item["prediction_id"], item["realized_at"], item["outcome"]) for item in records
            ]})
            buckets: dict[str, dict[str, float | int]] = {}
            for record in records:
                confidence = float(record["payload"].get("trade_confidence", 0.5))
                lower = min(0.95, max(0.50, math.floor(confidence * 20) / 20))
                key = f"{lower:.2f}-{min(1.0, lower + 0.05):.2f}"
                bucket = buckets.setdefault(key, {"count": 0, "positive_net_count": 0, "net_outcome_sum": 0.0})
                outcome = float(record["outcome"]["realized_net_outcome"])
                bucket["count"] = int(bucket["count"]) + 1
                bucket["positive_net_count"] = int(bucket["positive_net_count"]) + int(outcome > 0)
                bucket["net_outcome_sum"] = float(bucket["net_outcome_sum"]) + outcome
            brier = (fmean((float(item["payload"].get("trade_confidence", 0.5)) - (1.0 if float(item["outcome"]["realized_net_outcome"]) > 0 else 0.0)) ** 2 for item in records) if records else None)
            payload = {
                "status": "EMPIRICAL" if len(records) >= self.settings.minimum_forward_observations else "INSUFFICIENT_FORWARD_EVIDENCE",
                "sample_count": len(records), "brier_score": brier,
                "buckets": buckets,
                "note": "trade confidence is not claimed calibrated until the configured forward sample gate is met",
            }
            calibration_id = f"calibration-{canonical_hash({'model': model['model_id'], 'version': model['version'], 'source': fingerprint})[:24]}"
            self.repository.record_model_calibration(calibration_id, model_id=str(model["model_id"]), version=int(model["version"]), source_fingerprint=fingerprint, payload=payload, created_at=_iso(now))
            if len(records) >= self.settings.minimum_forward_observations and model["state"] == ModelState.SHADOW.value:
                self.repository.set_model_state(str(model["model_id"]), int(model["version"]), ModelState.ACTIVE_SIMULATION.value)
            persisted += 1
        return f"model_calibrations:{persisted}"

    def _shadow_decisions(self, now: datetime) -> str:
        champion = next((item for item in self.repository.model_roles() if item["role"] == "CHAMPION"), None)
        if not champion:
            return "no champion model"
        model = next((item for item in self.repository.list_models() if item["model_id"] == champion["model_id"] and item["version"] == champion["version"]), None)
        indicators = [item for item in self.repository.list_indicators() if item["state"] in {"VALIDATED", "ACTIVE"}]
        if not model or not indicators:
            return "model or indicators unavailable"
        calibration = next((item for item in self.repository.list_model_calibrations(model_id=str(model["model_id"]))
                            if int(item["version"]) == int(model["version"])), None)
        calibration_payload = calibration["payload"] if calibration else {}
        forward_count = int(calibration_payload.get("sample_count", 0))
        brier = calibration_payload.get("brier_score")
        calibration_quality = max(0.0, min(1.0, 1.0 - float(brier))) if brier is not None else 0.5
        sample_strength = min(1.0, forward_count / max(1, self.settings.minimum_forward_observations))
        evidence = ModelEvidence(sample_strength, 0.70, 0.70, sample_strength if forward_count else None, 0.60, 1.0, calibration_quality, 0.80, 0.70, forward_minimum_observations=self.settings.minimum_forward_observations, forward_observations=forward_count)
        snapshot = self.confidence.snapshot(evidence, 0.60, evidence_updates={"validated_indicator": 0.1, "forward_calibration": 0.05 if forward_count >= self.settings.minimum_forward_observations else 0.0})
        policy = DecisionRiskPolicy(account_risk_budget=100.0, available_equity=1_000.0, max_leverage=2.0, max_notional=600.0, entry_min_effective_confidence=self.config.scientific_execution.entry_min_effective_confidence, exit_effective_confidence=self.config.scientific_execution.exit_effective_confidence, max_position_age_seconds=self.config.scientific_execution.max_position_age_seconds)
        with self.latency.measure("confidence_to_decision", model_id=model["model_id"]):
            record = self.decisions.decide(DecisionInput(_iso(now), "SCIENCE-SHADOW", "long", model["model_id"], int(model["version"]), str(model["state"]), tuple((item["indicator_id"], int(item["version"])) for item in indicators), {item["indicator_id"]: 1.0 for item in indicators}, snapshot, 0.004, 0.001, 0.0005, 0.0005, 0.0005, 0.0005, 0.02, None, 0.0, 1.0, "unknown"), policy)
        return f"shadow_decision:{record.decision_id}:{record.decision.value}"

    def _evaluate_drift(self, now: datetime) -> str:
        events = 0
        records = self.repository.list_forward_records()
        for indicator in self.repository.list_indicators():
            experiment_id = indicator["provenance"].get("historical_experiment_id")
            forward = [item for item in records if item["experiment_id"] == experiment_id and item["outcome"] is not None]
            assessment = assess_forward_drift([{
                "net_outcome": item["outcome"]["realized_net_outcome"],
                "trade_confidence": item["payload"].get("trade_confidence", 0.5),
                "expected_cost": item["payload"].get("expected_costs"),
                "actual_cost": item["outcome"].get("outcome_metadata", {}).get("transaction_cost_estimate"),
            } for item in forward], minimum_observations=self.settings.drift_minimum_observations, net_expectancy_floor=self.settings.drift_net_expectancy_floor)
            if assessment.state == "DEGRADED" and indicator["state"] != "DEGRADED":
                self.indicators.set_state(indicator["indicator_id"], int(indicator["version"]), IndicatorState.DEGRADED)
                self.repository.record_drift(f"drift-{canonical_hash({'indicator': indicator['indicator_id'], 'count': assessment.sample_count})[:24]}", object_type="INDICATOR", object_id=indicator["indicator_id"], version=int(indicator["version"]), state="DEGRADED", reason=assessment.reason, evidence=asdict(assessment), created_at=_iso(now))
                events += 1
        return f"drift_events:{events}"

    def _write_journal(self, now: datetime) -> None:
        health = self.repository.health()["counts"]
        journal = {"observations_processed": len(self.repository.list_observations(limit=5_000)), "feature_values_produced": len(self.repository.list_feature_values()), "candidate_relationships": len(self.repository.list_discoveries()), "hypotheses_registered": health["science_hypotheses"], "historical_rejections": len(self.repository.list_graveyard()), "forward_predictions": health["science_forward_predictions"], "forward_outcomes_resolved": sum(item["outcome"] is not None for item in self.repository.list_forward_records()), "indicators": health["science_indicators"], "models": health["science_models"], "queue": self.repository.work_queue_status(now=_iso(now)), "archive": "spooled_only; explicit cold flush required"}
        self.repository.write_journal(now.date().isoformat(), journal, updated_at=_iso(now))
