"""TEST ONLY: deterministic protected-periodic bridge and telemetry proof.

This tool never starts the host thread, opens a paper ledger, contacts the
native receiver, or grants activation authority.  It uses authenticated test
observations, a real durable PongRuntimeBridge under an explicitly isolated N:
root, and a virtual monotonic/QPC clock.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.l3f_provider.ninjatrader_observation import expected_observer_source_fingerprint
from src.l3g_paper.contracts import PONG_ENTRY_PROFILE_VERSION
from src.l3g_paper.pong import PongStateMachine
from src.l3g_paper.pong_contract import DEC26_TOPOLOGY
from src.l3g_paper.pong_periodic_telemetry import aligned_periodic_window
from src.l3g_paper.pong_production import PongProductionHost
from src.l3g_paper.pong_qualifying import PONG_NO_FHB_QUALIFYING_ARCHITECTURE
from src.l3g_paper.pong_quantity import PongQuantityPolicy
from src.l3g_paper.pong_runtime import PongLoadedRuntimeIdentity, PongRuntimeBridge
from src.l3g_paper.pong_scheduler import PongSchedulingConfig, PongSchedulingMode
from tests import test_l3g_pong_production as production_fixtures
from tests.test_l3g_pong_inputs import verified


REPORT_SCHEMA = "beelzebub-asia-periodic-healthy-flat-offline-v1"


class VirtualClock:
    def __init__(self) -> None:
        self.ns = 0

    def qpc_ns(self) -> int:
        return self.ns

    def monotonic(self) -> float:
        return self.ns / 1_000_000_000

    def advance_ms(self, milliseconds: int) -> None:
        self.ns += milliseconds * 1_000_000


def _collector_snapshot() -> dict[str, object]:
    return {
        "state": "TEST_ONLY_OFFLINE",
        "durability": {
            "enqueued_records": 0,
            "written_records": 0,
            "durable_records": 0,
            "queue_depth": 0,
            "queue_high_water_records": 0,
            "last_queue_age_milliseconds": 0.0,
            "maximum_queue_age_milliseconds": 0.0,
            "queue_overflow_records": 0,
            "producer_queue_overflow_records": 0,
            "producer_transport_failed_records": 0,
        },
    }


def _create_bridge(
    host: PongProductionHost,
    *,
    runtime: dict[str, object],
    bridge_path: Path,
) -> None:
    topology = DEC26_TOPOLOGY
    identity = PongLoadedRuntimeIdentity(
        execution_session_id=str(runtime["execution_session_id"]),
        addon_protocol_version=str(runtime["addon_protocol_version"]),
        addon_source_fingerprint=str(runtime["addon_source_fingerprint"]),
        addon_build_fingerprint=str(runtime["addon_build_fingerprint"]),
        verified_at=str(runtime["observed_at"]),
        attestation_verified=True,
        instrument=topology.native_instrument,
        canonical_contract=topology.canonical_contract,
        topology_version=topology.version,
        topology_hash=topology.binding_hash,
    )
    policy = PongQuantityPolicy(
        instrument=topology.native_instrument,
        canonical_contract=topology.canonical_contract,
        reconciliation_source_fingerprint=identity.addon_source_fingerprint,
        reconciliation_build_fingerprint=identity.addon_build_fingerprint,
        topology_version=topology.version,
        topology_hash=topology.binding_hash,
    )
    host._bridge = PongRuntimeBridge.create(
        PongStateMachine(policy), bridge_path, runtime_identity=identity,
    )


def _depth(sequence: int, side: str, observed: str):
    template = production_fixtures.PongProductionHostTests._qualifying_depth(
        sequence,
        side=side,
        mutation_price=100 if side == "Bid" else 100.25,
    )
    return verified(
        "DEPTH", dict(template.payload),
        local_sequence=sequence, observed=observed,
    )


def _telemetry(host: PongProductionHost) -> dict[str, object]:
    return host.status()["evaluation_schedule"]["periodic_ratio_telemetry"]


def run_offline_scenario(state_root: Path) -> dict[str, object]:
    resolved = state_root.resolve()
    if resolved.drive.upper() != "N:":
        raise ValueError("ASIA_PERIODIC_TEST_ROOT_MUST_BE_ISOLATED_N_DRIVE")
    resolved.mkdir(parents=True, exist_ok=True)
    clock = VirtualClock()
    native_dispatches: list[dict[str, object]] = []
    runtime = production_fixtures.qualifying_runtime_snapshot()
    collector = _collector_snapshot()
    host = PongProductionHost(
        resolved / "host",
        selected_profile=PONG_ENTRY_PROFILE_VERSION,
        expected_market_source_fingerprint=expected_observer_source_fingerprint(),
        runtime_snapshot=lambda: runtime,
        collector_status=lambda: collector,
        controller_dispatch=lambda _kind, _reason: False,
        native_dispatch=lambda directive, evidence: native_dispatches.append({
            "directive_id": directive.directive_id,
            "evidence": dict(evidence),
        }) or {"accepted": False, "directive_id": directive.directive_id},
        activation_authorized=False,
        scheduling=PongSchedulingConfig(
            PongSchedulingMode.PERIODIC_FLAT_PROTECTED, 20,
        ),
        qualifying_architecture=PONG_NO_FHB_QUALIFYING_ARCHITECTURE,
        topology=DEC26_TOPOLOGY,
        clock_ns=clock.qpc_ns,
        monotonic_clock=clock.monotonic,
    )
    host._runtime = runtime
    host._collection = collector

    clock.advance_ms(20)
    host._service_periodic_deadline()
    bridge_unavailable = host._event_protection_reason_locked()
    _create_bridge(
        host,
        runtime=runtime,
        bridge_path=resolved / "bridge" / "TEST-ONLY.json",
    )

    observed = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    host._consume_observation(_depth(1, "Bid", observed))
    host._consume_observation(_depth(2, "Ask", observed))
    host._consume_observation(verified("TRADE", {
        "contract_id": DEC26_TOPOLOGY.native_instrument,
        "price": 100.25,
        "size": 1,
        "aggressor_side": "BUY",
        "aggressor_source": "NATIVE_BID_ASK",
        "source_event_sequence": 1,
        "source_generation": 1,
        "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
    }, local_sequence=3, observed=observed))
    healthy_start = _telemetry(host)
    host._consume_observation(_depth(3, "Bid", observed))
    host._consume_observation(_depth(4, "Ask", observed))
    clock.advance_ms(20)
    host._service_periodic_deadline()
    healthy_end = _telemetry(host)

    host._runtime = {**runtime, "reconciliation_current": False}
    clock.advance_ms(20)
    host._service_periodic_deadline()
    host._consume_observation(_depth(5, "Bid", observed))
    host._consume_observation(_depth(6, "Ask", observed))
    protected_end = _telemetry(host)

    host._runtime = runtime
    host._consume_observation(_depth(7, "Bid", observed))
    host._consume_observation(_depth(8, "Ask", observed))
    recovery_start = _telemetry(host)
    clock.advance_ms(20)
    host._service_periodic_deadline()
    recovery_end = _telemetry(host)

    return {
        "schema": REPORT_SCHEMA,
        "test_only": True,
        "authority": "DISARMED_NO_ORDERS_NO_NATIVE_RECEIVER",
        "state_root": str(resolved),
        "paper_ledger": "NOT_INSTANTIATED",
        "virtual_time": True,
        "initial_protection_reason": bridge_unavailable,
        "healthy_flat_window": aligned_periodic_window(
            healthy_start, healthy_end,
        ),
        "protected_window": aligned_periodic_window(
            healthy_end, protected_end,
        ),
        "recovery_window": aligned_periodic_window(
            recovery_start, recovery_end,
        ),
        "final_active_path": host.status()["evaluation_schedule"]["active_path"],
        "native_dispatches": native_dispatches,
        "bridge_path": str(resolved / "bridge" / "TEST-ONLY.json"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = run_offline_scenario(args.state_root)
    output = args.output or args.state_root / "asia-periodic-healthy-flat.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "report": str(output.resolve()),
        "healthy_ratio": report["healthy_flat_window"][
            "depth_events_per_ordinary_completed_full_analysis"
        ],
        "final_active_path": report["final_active_path"],
        "native_dispatches": len(report["native_dispatches"]),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
