from __future__ import annotations

import unittest
from pathlib import Path

from src.governance.cli import collect
from src.governance.errors import GovernanceError
from src.governance.models import load_adoptions


ROOT = Path(__file__).resolve().parents[1]


class PhaseF5GovernanceAdversarialTests(unittest.TestCase):
    def test_governance_has_no_execution_or_wallet_authority(self) -> None:
        governance = next(item for item in collect(ROOT)["components"] if item["component_id"] == "governance")
        denied = {"submit_paper_orders", "submit_testnet_orders", "submit_mainnet_orders", "sign_wallet_actions", "allocate_live_capital"}
        self.assertFalse(denied.intersection(governance["current_authority"]))

    def test_every_active_direct_record_has_license_and_provenance(self) -> None:
        for item in load_adoptions(ROOT):
            if item["lifecycle_status"] in {"APPROVED", "INTEGRATED", "COMMISSIONED"}:
                self.assertNotEqual(item["license_expression"], "UNKNOWN")
                self.assertNotEqual(item["immutable_provenance"], "UNKNOWN")
