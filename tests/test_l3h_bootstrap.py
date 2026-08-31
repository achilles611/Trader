from __future__ import annotations

import unittest

from src.l3h_live.bootstrap import AccountEvidence, NativeCapabilityBinding, classify_account, create_attestation
from src.l3h_live.contracts import AccountClass


class L3HBootstrapTests(unittest.TestCase):
    def evidence(self, *, name: str = "Sim101", program: str | None = None) -> AccountEvidence:
        return AccountEvidence("sim101-id", name, "NinjaTrader", "NinjaTrader", program, None, "2026-08-30T00:00:00Z")

    def test_simulation_classification_is_not_live_and_ambiguous_evidence_refuses(self) -> None:
        classified, reasons = classify_account(self.evidence())
        self.assertEqual((classified, reasons), (AccountClass.LOCAL_SIMULATION, ("CLASSIFIED_FROM_CONCORDANT_METADATA",)))
        ambiguous, reasons = classify_account(self.evidence(name="Sim101 Evaluation"))
        self.assertEqual((ambiguous, reasons), (AccountClass.UNKNOWN, ("ACCOUNT_CLASSIFICATION_AMBIGUOUS",)))

    def test_attestations_and_native_binding_are_exact_and_signed(self) -> None:
        key = b"k" * 32
        evidence = self.evidence()
        attestation = create_attestation(evidence, AccountClass.LOCAL_SIMULATION, operator_confirmation=True, attestation_id="l3h-attest-001").signed(key)
        attestation.verify(key)
        binding = NativeCapabilityBinding("lane-iii-phase-h-native-binding-v1", "Sim101", evidence.binding_hash, "a" * 64, "l3h-cap-unit", "l3h-epoch-unit").signed(key)
        binding.verify(key)
        with self.assertRaisesRegex(ValueError, "SIGNATURE"):
            binding.verify(b"x" * 32)


if __name__ == "__main__":
    unittest.main()
