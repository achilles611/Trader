"""Read-only Control Center projection for the scientific alpha ecosystem."""

from __future__ import annotations

from pathlib import Path
from shutil import disk_usage
from typing import Any

from .config import CopyTradeConfig
from .science_repository import ScientificRepository
from .science_storage import ColdArchiveSpool, StorageRoots


class ScientificReadModel:
    def __init__(self, config: CopyTradeConfig, database_path: str | Path) -> None:
        database_path = Path(database_path)
        cold_root = config.storage.cold_root
        self.roots = StorageRoots(home=database_path.parent.parent, hot_root=database_path.parent, cold_root=cold_root)
        self.spool = ColdArchiveSpool(self.roots, max_bytes=config.storage.archive_spool_max_bytes, max_age_seconds=config.storage.archive_spool_max_age_seconds)
        self.repository = ScientificRepository(database_path, archive_spool=self.spool)
        self.repository.initialize()

    def health(self) -> dict[str, Any]:
        return {**self.repository.health(), "storage": {**self.roots.cold_status(), "spool": self.spool.backlog()}, "execution_mode": "SIMULATION_SHADOW_ONLY"}

    def ecosystem(self) -> dict[str, Any]:
        health = self.health()
        counts = health["counts"]
        def state(count: int, no_evidence: str = "NO_EVIDENCE") -> str:
            return "HEALTHY" if count else no_evidence
        nodes = [
            {"id": "core", "label": "Core Orchestrator", "state": "HEALTHY"},
            {"id": "soil", "label": "Data Soil / Provenance", "state": state(counts["science_features"])},
            {"id": "sensors", "label": "Wallet Sensor Network", "state": state(len(self.repository.list_wallet_sensors()))},
            {"id": "hypotheses", "label": "Hypothesis Lab", "state": state(counts["science_hypotheses"])},
            {"id": "indicators", "label": "Indicator Forge", "state": state(counts["science_indicators"])},
            {"id": "experiments", "label": "Experiment Engine", "state": state(counts["science_experiments"])},
            {"id": "confidence", "label": "Confidence Engine", "state": state(counts["science_decisions"])},
            {"id": "risk", "label": "Execution / Risk Gates", "state": "SIMULATION_ONLY"},
            {"id": "watchers", "label": "Watchers", "state": "UNAVAILABLE"},
            {"id": "control", "label": "Control Center", "state": "HEALTHY"},
        ]
        return {"nodes": nodes, "cycle": ["OBSERVE", "DISCOVER", "REGISTER", "HISTORICAL TEST", "FORWARD SHADOW", "PROMOTE", "DECIDE / EXECUTE", "LEARN"], "health": health}

    def wallet_sensors(self) -> dict[str, Any]:
        return {"items": self.repository.list_wallet_sensors(), "empty_state": "No evidence"}

    def hypotheses(self, state: str | None = None) -> dict[str, Any]:
        return {"items": self.repository.list_hypotheses(state=state), "immutable": True, "empty_state": "No registered hypotheses"}

    def experiments(self, kind: str | None = None) -> dict[str, Any]:
        return {"items": self.repository.list_experiments(kind=kind), "empty_state": "No experiments"}

    def indicators(self) -> dict[str, Any]:
        return {"items": self.repository.list_indicators(), "empty_state": "No validated indicators"}

    def models(self) -> dict[str, Any]:
        return {"items": self.repository.list_models(), "empty_state": "No model versions"}

    def confidence(self) -> dict[str, Any]:
        return {"items": self.repository.list_decisions(), "empty_state": "No active signals or positions"}

    def decisions(self) -> dict[str, Any]:
        return {"items": self.repository.list_decisions(), "execution_mode": "SIMULATION_SHADOW_ONLY"}

    def graveyard(self, search: str = "") -> dict[str, Any]:
        return {"items": self.repository.list_graveyard(search=search), "empty_state": "No rejected hypotheses"}

    def storage(self) -> dict[str, Any]:
        return {**self.roots.cold_status(), "spool": self.spool.backlog(), "hot_database": str(self.repository.path)}

    def automated(self) -> dict[str, Any]:
        """Truthful D.6 projection; it never fabricates worker activity."""
        forward = self.repository.list_forward_records()
        journal = self.repository.latest_journal()
        return {
            "execution_mode": "SIMULATION_SHADOW_ONLY",
            "worker_control": self.repository.worker_control(),
            "queue": self.repository.work_queue_status(now="control-center-read"),
            "watermarks": self.repository.list_watermarks(),
            "stage_health": self.repository.stage_health(),
            "discoveries": self.repository.list_discoveries()[:50],
            "model_roles": self.repository.model_roles(),
            "model_calibrations": self.repository.list_model_calibrations()[:25],
            "drift": self.repository.list_drift()[:50],
            "journal": journal,
            "latency": self.repository.latency_report(),
            "counts": {
                "observations": len(self.repository.list_observations(limit=5_000)),
                "feature_values": len(self.repository.list_feature_values()),
                "forward_predictions": len(forward),
                "resolved_forward_predictions": sum(item["outcome"] is not None for item in forward),
                "discoveries": len(self.repository.list_discoveries()),
                "hypotheses": len(self.repository.list_hypotheses()),
                "indicators": len(self.repository.list_indicators()),
                "models": len(self.repository.list_models()),
            },
            "resource_state": {"hot_free_bytes": disk_usage(self.repository.path.parent).free, **self.storage()},
            "scheduler": "external durable CLI process; current database state is shown above",
        }

    def pause_automated_worker(self, reason: str) -> dict[str, Any]:
        from .models import iso, utc_now
        self.repository.set_worker_paused(True, reason=reason or "operator requested scientific pause", updated_at=iso(utc_now()))
        return self.automated()

    def resume_automated_worker(self) -> dict[str, Any]:
        from .models import iso, utc_now
        self.repository.set_worker_paused(False, reason="", updated_at=iso(utc_now()))
        return self.automated()
