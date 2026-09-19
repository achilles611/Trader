from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from src.l3g_paper.discernment import DiscernmentEngine
from src.l3g_paper.pong_contract import (
    DEC26_TOPOLOGY,
    SEP26_TOPOLOGY,
    resolve_pong_topology,
    validate_topology_bundle,
)
from src.l3g_paper.pong_inputs import (
    PONG_HOURLY_WARM_START_POLICY_HASH,
    hourly_warm_start_policy_hash,
)
from src.l3g_paper.pong_microstructure import MicrostructureEvidenceMappingPolicy
from src.l3g_paper.pong_production import PongProductionHost
from src.l3g_paper.pong_quantity import (
    PONG_QUANTITY_POLICY,
    PongQuantityPolicy,
    PongReconciliationProof,
)
from src.l3g_paper.pong_runtime import PongLoadedRuntimeIdentity
from src.l3g_paper.risk_continuity import (
    read_pong_risk_continuity_artifact,
    validate_pong_risk_continuity_snapshot,
    validate_risk_continuity_snapshot,
    write_pong_risk_continuity_artifact,
)
from src.l3g_paper.sessions import OFF_SESSION_PROFILE


class PongContractTopologyTests(unittest.TestCase):
    def test_exact_identities_are_distinct_and_default_is_dec26(self) -> None:
        self.assertEqual(resolve_pong_topology(), DEC26_TOPOLOGY)
        self.assertEqual(resolve_pong_topology("PONG_CONTRACT_TOPOLOGY_SEP26_V1"), SEP26_TOPOLOGY)
        self.assertEqual(resolve_pong_topology("DEC26"), DEC26_TOPOLOGY)
        self.assertNotEqual(SEP26_TOPOLOGY.binding_hash, DEC26_TOPOLOGY.binding_hash)
        self.assertNotEqual(SEP26_TOPOLOGY.state_namespace, DEC26_TOPOLOGY.state_namespace)
        self.assertNotEqual(
            PONG_HOURLY_WARM_START_POLICY_HASH,
            hourly_warm_start_policy_hash(DEC26_TOPOLOGY),
        )

    def test_unknown_selector_and_mixed_bundle_refuse(self) -> None:
        with self.assertRaisesRegex(ValueError, "SELECTOR_UNKNOWN"):
            resolve_pong_topology("MNQZ6")
        bundle = {
            "topology_version": DEC26_TOPOLOGY.version,
            "topology_hash": SEP26_TOPOLOGY.binding_hash,
            "native_instrument": DEC26_TOPOLOGY.native_instrument,
            "canonical_contract": DEC26_TOPOLOGY.canonical_contract,
            "state_namespace": DEC26_TOPOLOGY.state_namespace,
            "account_name": DEC26_TOPOLOGY.account_name,
            "account_class": DEC26_TOPOLOGY.account_class,
            "live_authority": DEC26_TOPOLOGY.live_authority,
            "live_capital": DEC26_TOPOLOGY.live_capital,
            "maximum_quantity": DEC26_TOPOLOGY.maximum_quantity,
        }
        with self.assertRaisesRegex(ValueError, "BUNDLE_MISMATCH"):
            validate_topology_bundle(DEC26_TOPOLOGY, bundle)

    def test_dec26_policy_runtime_and_discernment_bind_exact_identity(self) -> None:
        mapping = MicrostructureEvidenceMappingPolicy(
            topology_version=DEC26_TOPOLOGY.version,
            topology_hash=DEC26_TOPOLOGY.binding_hash,
        )
        engine = DiscernmentEngine(
            mapping.discernment_policy(),
            instrument=DEC26_TOPOLOGY.native_instrument,
            contract=DEC26_TOPOLOGY.canonical_contract,
            topology=DEC26_TOPOLOGY,
        )
        update = engine.on_time("2026-09-19T16:00:00Z")
        self.assertEqual(update.snapshot.instrument, "MNQ DEC26")
        self.assertEqual(update.snapshot.contract, "MNQZ6")
        self.assertEqual(update.snapshot.topology_version, DEC26_TOPOLOGY.version)
        self.assertEqual(update.snapshot.topology_hash, DEC26_TOPOLOGY.binding_hash)
        quantity = PongQuantityPolicy(
            instrument="MNQ DEC26",
            canonical_contract="MNQZ6",
            topology_version=DEC26_TOPOLOGY.version,
            topology_hash=DEC26_TOPOLOGY.binding_hash,
        )
        self.assertNotEqual(quantity.configuration_hash, PONG_QUANTITY_POLICY.configuration_hash)
        proof = PongReconciliationProof.create(
            source_hash="d" * 64,
            observed_at="2026-09-19T16:00:00Z",
            available_at="2026-09-19T16:00:00Z",
            position_quantity=0,
            working_owned_orders=0,
            unresolved_execution=False,
            instrument="MNQ DEC26",
            canonical_contract="MNQZ6",
            topology_version=DEC26_TOPOLOGY.version,
            topology_hash=DEC26_TOPOLOGY.binding_hash,
        )
        self.assertIsNone(
            proof.validation_reason(at="2026-09-19T16:00:01Z", policy=quantity),
        )
        identity = PongLoadedRuntimeIdentity(
            execution_session_id="l3g-es-dec26-topology-test",
            addon_protocol_version="test-v1",
            addon_source_fingerprint="a" * 64,
            addon_build_fingerprint="b" * 64,
            verified_at="2026-09-19T16:00:00Z",
            attestation_verified=True,
            instrument="MNQ DEC26",
            canonical_contract="MNQZ6",
            topology_version=DEC26_TOPOLOGY.version,
            topology_hash=DEC26_TOPOLOGY.binding_hash,
        )
        self.assertNotEqual(identity.identity_hash, "")

    def test_dec26_host_is_disarmed_and_has_isolated_state(self) -> None:
        dispatched: list[object] = []
        with tempfile.TemporaryDirectory() as directory:
            host = PongProductionHost(
                Path(directory),
                selected_profile="BEELZEBUB_PONG_DISCERNMENT_V1",
                expected_market_source_fingerprint="c" * 64,
                runtime_snapshot=lambda: {
                    "observed_at": "2026-09-19T16:00:00Z",
                    "runtime_active": False,
                    "instrument": "MNQ SEP26",
                    "canonical_contract": "MNQU6",
                    "addon_provenance_valid": False,
                    "position_quantity": 0,
                    "working_owned_orders": 0,
                    "working_entry_orders": 0,
                    "unresolved_execution": True,
                },
                collector_status=lambda: {},
                controller_dispatch=lambda *_: False,
                native_dispatch=lambda directive, evidence: dispatched.append((directive, evidence)) or {},
                activation_authorized=False,
                topology=DEC26_TOPOLOGY,
            )
            self.assertEqual(
                host.controller_path.parts[-4:],
                ("pong", "MNQZ6", "v1", "controller-state.json"),
            )
            self.assertEqual(host.status()["instrument"], "MNQ DEC26")
            self.assertEqual(host.status()["canonical_contract"], "MNQZ6")
            self.assertEqual(host.status()["live_authority"], "DISARMED")
            host.start()
            time.sleep(0.05)
            host.stop()
            self.assertEqual(dispatched, [])
            self.assertFalse(host.status()["entry_authority_ready"])

    def test_dec26_risk_continuity_preserves_shared_totals_and_lockout(self) -> None:
        snapshot = {
            "schema": "lane-iii-paper-risk-continuity-snapshot-v2",
            "generated_at": "2026-09-19T16:00:00Z",
            "account_name": "Sim101",
            "account_class": "LOCAL_SIMULATION",
            "instrument": "MNQ DEC26",
            "source_profile": "BEELZEBUB_PONG_DISCERNMENT_V1",
            "source_ledger": {
                "path": "N:\\Beelzebub\\offline\\dec26-ledger.sqlite",
                "ledger_identity": "offline-dec26-ledger",
                "ledger_epoch": "epoch-dec26-v1",
                "risk_boundary_sequence": 1,
                "risk_boundary_hash": "a" * 64,
                "coverage_complete": True,
            },
            "authority_lockout": {
                "locked_out": True,
                "lockout_reason": "OFFLINE_CONTINUITY_TEST_LOCKOUT",
                "lockout_trade_date": "2026-09-18",
            },
            "trade_dates": [{
                "trade_date": "2026-09-18",
                "realized_pnl": "5.00",
                "unrealized_pnl": "0",
                "entry_count": 1,
            }],
            "profile_trade_dates": [{
                "profile": "BEELZEBUB_PONG_DISCERNMENT_V1",
                "trade_date": "2026-09-18",
                "entry_count": 1,
                "consecutive_losses": 0,
            }],
            "sessions": [{
                "profile": "BEELZEBUB_PONG_DISCERNMENT_V1",
                "session_kind": "OFF_SESSION",
                "session_family": "OFF_SESSION",
                "session_id": "MNQZ6:OFF_SESSION:2026-09-18",
                "trade_date": "2026-09-18",
                "session_profile_hash": OFF_SESSION_PROFILE.profile_hash,
                "session_generation": 1,
                "entry_count": 1,
                "realized_pnl": "5.00",
            }],
            "entry_execution_ids": ["entry-dec26-1"],
            "exit_execution_ids": ["exit-dec26-1"],
        }
        with self.assertRaisesRegex(RuntimeError, "ACCOUNT_IDENTITY_MISMATCH"):
            validate_risk_continuity_snapshot(snapshot, require_flat=True)
        validated = validate_pong_risk_continuity_snapshot(
            snapshot, topology=DEC26_TOPOLOGY, require_flat=True,
        )
        self.assertEqual(validated["instrument"], "MNQ DEC26")
        self.assertEqual(validated["trade_dates"][0]["entry_count"], 1)
        self.assertTrue(validated["authority_lockout"]["locked_out"])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pong-risk-continuity.json"
            artifact = write_pong_risk_continuity_artifact(
                path,
                operation_id="pong-dec26-continuity-test",
                source_profile="BEELZEBUB_PONG_DISCERNMENT_V1",
                target_profile="BEELZEBUB_PONG_DISCERNMENT_V1",
                snapshot=snapshot,
                topology=DEC26_TOPOLOGY,
            )
            restored = read_pong_risk_continuity_artifact(
                path,
                topology=DEC26_TOPOLOGY,
                operation_id="pong-dec26-continuity-test",
                target_profile="BEELZEBUB_PONG_DISCERNMENT_V1",
            )
            self.assertEqual(restored["artifact_sha256"], artifact["artifact_sha256"])
            self.assertEqual(restored["snapshot"]["trade_dates"], validated["trade_dates"])
            self.assertEqual(restored["snapshot"]["authority_lockout"], validated["authority_lockout"])


if __name__ == "__main__":
    unittest.main()
