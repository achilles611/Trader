from pathlib import Path
import os
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.copytrade.config import CopyTradeConfig
from src.copytrade.control_center import create_control_center_app
from src.l3g_paper.contracts import (
    ACCOUNT_BINDING,
    CAPABILITY,
    DEC26_TOPOLOGY,
    POLICY,
    RISK_PROFILE,
    ExecutionAccountBinding,
    ExecutionCapabilityManifest,
    PaperAuthorityBundle,
    paper_binding_for_topology,
    paper_policy_for_topology,
    paper_risk_for_topology,
)
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import PaperExecutionTransport
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.l3g_paper.risk import PaperRiskAuthority
from src.l3g_paper.sessions import PaperSessionKind, PaperSessionResolver


class VersionedOperationalContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = paper_policy_for_topology(POLICY, DEC26_TOPOLOGY)
        self.risk = paper_risk_for_topology(RISK_PROFILE, DEC26_TOPOLOGY)
        self.binding = paper_binding_for_topology(DEC26_TOPOLOGY)
        self.capability = ExecutionCapabilityManifest(instrument=DEC26_TOPOLOGY.native_instrument)

    def test_integrated_operational_path_accepts_coherent_dec26_and_persists_identity(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "dec26.sqlite3"
            ledger = PaperLedger(
                path,
                epoch_id="L3G-PAPER-EPOCH-DEC26-TEST",
                policy=self.policy,
                risk=self.risk,
            )
            runtime = LaneIIIPaperRuntime(ledger)
            transport = PaperExecutionTransport(
                ledger, port=48179, policy=self.policy, risk=self.risk,
            )
            try:
                status = runtime.status()
                self.assertEqual(status["market_instrument"], "MNQ DEC26")
                self.assertEqual(status["canonical_contract"], "MNQZ6")
                self.assertEqual(status["contract_topology_version"], DEC26_TOPOLOGY.version)
                self.assertEqual(transport.status().as_dict()["instrument"], "MNQ DEC26")
                self.assertEqual(transport.status().as_dict()["canonical_contract"], "MNQZ6")
                self.assertTrue(transport._validate_reconciliation({
                    "account_name": "Sim101", "account_class": "LOCAL_SIMULATION",
                    "instrument": "MNQ DEC26", "position_quantity": 0,
                    "working_order_count": 0, "working_entry_count": 0,
                    "foreign_activity": False, "position_snapshot_complete": True,
                    "order_snapshot_complete": True,
                }))
                self.assertFalse(transport._validate_reconciliation({
                    "account_name": "Sim101", "account_class": "LOCAL_SIMULATION",
                    "instrument": "MNQ SEP26", "position_quantity": 0,
                    "working_order_count": 0, "working_entry_count": 0,
                    "foreign_activity": False, "position_snapshot_complete": True,
                    "order_snapshot_complete": True,
                }))

                context = PaperSessionResolver(session_contract="MNQZ6").resolve(
                    "2026-09-21T14:00:00Z", generation=4,
                ).context
                self.assertEqual(context.session_id, "MNQZ6:NEW_YORK_RTH:2026-09-21")
                ledger.set_session_context(context)
                identity = "l3g-dec26-session-opened-test"
                ledger.append(
                    "SESSION_OPENED", {**context.payload(), "reason": "DEC26_TEST"},
                    identity=identity,
                )
                record = ledger.record_by_identity(identity)
                self.assertIsNotNone(record)
                envelope = record["record"]  # type: ignore[index]
                self.assertEqual(envelope["instrument"], "MNQ DEC26")
                self.assertEqual(envelope["canonical_contract"], "MNQZ6")
                self.assertEqual(envelope["contract_topology_version"], DEC26_TOPOLOGY.version)
            finally:
                runtime.stop()
                transport.stop()
                ledger.close()

            reopened = PaperLedger(
                path,
                epoch_id="L3G-PAPER-EPOCH-DEC26-TEST",
                policy=self.policy,
                risk=self.risk,
            )
            reopened.close()

    def test_mixed_contract_authority_and_ledger_reopen_fail_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "PAPER_CONTRACT_TOPOLOGY_BUNDLE_MISMATCH"):
            PaperAuthorityBundle(
                self.policy,
                RISK_PROFILE,
                self.binding,
                self.capability,
            )

        with TemporaryDirectory() as directory:
            path = Path(directory) / "dec26.sqlite3"
            ledger = PaperLedger(path, policy=self.policy, risk=self.risk)
            ledger.close()
            with self.assertRaisesRegex(RuntimeError, "PAPER_PROFILE_LEDGER_EPOCH_MISMATCH"):
                PaperLedger(path, policy=POLICY, risk=RISK_PROFILE)

    def test_operating_control_center_factory_constructs_disarmed_dec26_path(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "BEELZEBUB_PROFILE_SWITCH_ROOT": str(root / "switch"),
                "BEELZEBUB_L3G_PAPER_LEDGER": str(root / "paper.sqlite3"),
                "BEELZEBUB_LEDGER_AUDIT_ROOT": str(root / "audit"),
                "BEELZEBUB_L3G_PAPER_CONTRACT_TOPOLOGY": "DEC26",
            }
            with patch.dict(os.environ, environment, clear=False):
                app = create_control_center_app(CopyTradeConfig())
            self.assertGreater(len(app.routes), 0)
            self.assertEqual(environment["BEELZEBUB_L3G_PAPER_CONTRACT_TOPOLOGY"], "DEC26")

    def test_sep26_compatibility_binding_remains_unchanged(self) -> None:
        self.assertEqual(ACCOUNT_BINDING.instrument, "MNQ SEP26")
        self.assertEqual(ACCOUNT_BINDING.canonical_contract, "MNQU6")
        self.assertEqual(CAPABILITY.instrument, "MNQ SEP26")
        PaperRiskAuthority(
            profile=RISK_PROFILE, binding=ACCOUNT_BINDING, policy=POLICY,
        )


if __name__ == "__main__":
    unittest.main()
