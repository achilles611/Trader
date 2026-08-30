from __future__ import annotations

import copy
import gzip
import hashlib
import json
import tempfile
import unittest

from src.lane_ii.lab.anvil_backend import PINNED_ANVIL_VERSION, AnvilBackend, anvil_version, installed_anvil
from src.lane_ii.lab.anvil_state import RawDumpCapture
from src.lane_ii.lab.contracts import (
    BackendType,
    CounterfactualAssertion,
    CounterfactualMutation,
    CounterfactualScenario,
    RunStatus,
    SCENARIO_SCHEMA_VERSION,
)
from src.lane_ii.lab.coordinator import CounterfactualCoordinator
from src.lane_ii.lab.evidence import persist_result, validate_persisted_result


ADDRESS = "0x1111111111111111111111111111111111111111"
SLOT = "0x" + ("00" * 32)
VALUE = "0x" + ("01" * 32)
ANVIL_AVAILABLE = bool(anvil_version(installed_anvil()) and PINNED_ANVIL_VERSION in (anvil_version(installed_anvil()) or ""))


def _scenario(mutation: CounterfactualMutation, assertion: CounterfactualAssertion | None = None, *, name: str) -> CounterfactualScenario:
    return CounterfactualScenario(
        schema_version=SCENARIO_SCHEMA_VERSION,
        scenario_id=name,
        scenario_version="v1",
        deterministic_seed=7331,
        backend=BackendType.ANVIL,
        target_domain="LOCAL_ANVIL",
        target_chain_id=31337,
        fixed_fork_block=None,
        initial_state_fingerprint=AnvilBackend.expected_initial_fingerprint(31337),
        mutations=(mutation,),
        assertions=(assertion,) if assertion is not None else (),
        timeout_seconds=30,
    )


def _run(scenario: CounterfactualScenario, backend_type=AnvilBackend, **backend_kwargs):
    captured: list[AnvilBackend] = []

    def factory(item: CounterfactualScenario) -> AnvilBackend:
        backend = backend_type(chain_id=item.target_chain_id, binary=installed_anvil(), **backend_kwargs)
        captured.append(backend)
        return backend

    result = CounterfactualCoordinator(anvil_factory=factory).run(scenario)
    return result, captured[0]


class _ResidualBalance(AnvilBackend):
    def revert(self, snapshot: object) -> bool:
        restored = super().revert(snapshot)
        if restored:
            self.apply(CounterfactualMutation("set_native_balance", {"address": ADDRESS, "balance": 99}))
        return restored


class _ResidualCode(AnvilBackend):
    def revert(self, snapshot: object) -> bool:
        restored = super().revert(snapshot)
        if restored:
            self.apply(CounterfactualMutation("set_contract_code", {"address": ADDRESS, "code": "0x6001"}))
        return restored


class _ResidualStorage(AnvilBackend):
    def revert(self, snapshot: object) -> bool:
        restored = super().revert(snapshot)
        if restored:
            self.apply(CounterfactualMutation("set_storage_slot", {"address": ADDRESS, "slot": SLOT, "value": "0x" + ("02" * 32)}))
        return restored


class _TxpoolResidue(AnvilBackend):
    def revert(self, snapshot: object) -> bool:
        restored = super().revert(snapshot)
        if restored:
            self._call("evm_setAutomine", [False])
            accounts = self._call("eth_accounts", [])
            self._call("eth_sendTransaction", [{"from": accounts[0], "to": accounts[1], "value": "0x0", "gas": "0x5208"}])
        return restored


class _UnknownDumpDifference(AnvilBackend):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._capture_count = 0

    def capture_raw_dump(self) -> RawDumpCapture:
        capture = super().capture_raw_dump()
        self._capture_count += 1
        if self._capture_count < 2:
            return capture
        decoded = copy.deepcopy(capture.decoded)
        decoded["unknown_provider_field"] = "deliberate-test-drift"
        encoded = gzip.compress(json.dumps(decoded, separators=(",", ":")).encode("utf-8"), mtime=0).hex()
        raw = "0x" + encoded
        return RawDumpCapture(raw, decoded, hashlib.sha256(raw.encode("utf-8")).hexdigest())


class _RefuseRevert(AnvilBackend):
    def revert(self, snapshot: object) -> bool:
        return False


@unittest.skipUnless(ANVIL_AVAILABLE, "Pinned official Anvil v1.8.1 is required.")
class PhaseF4RealAnvilRegressionTests(unittest.TestCase):
    def _assert_positive(self, result) -> None:
        self.assertEqual(result.evidence.run_status, RunStatus.SUCCEEDED)
        self.assertTrue(result.evidence.restoration_verified)
        evidence = result.evidence.restoration_evidence
        self.assertTrue(evidence["semantic_state_equal"])
        self.assertTrue(evidence["mutation_witness_restoration"])
        self.assertTrue(evidence["txpool_restored_and_empty"])
        self.assertEqual(evidence["unknown_difference_count"], 0)
        self.assertTrue(evidence["process_shutdown_verified"])
        self.assertTrue(evidence["port_release_verified"])

    def test_positive_balance_restoration(self) -> None:
        result, _ = _run(_scenario(
            CounterfactualMutation("set_native_balance", {"address": ADDRESS, "balance": 12345}),
            CounterfactualAssertion("native_balance_equals", {"address": ADDRESS, "balance": 12345}),
            name="f4-real-balance",
        ))
        self._assert_positive(result)

    def test_positive_code_restoration(self) -> None:
        result, _ = _run(_scenario(
            CounterfactualMutation("set_contract_code", {"address": ADDRESS, "code": "0x6000"}),
            CounterfactualAssertion("code_equals", {"address": ADDRESS, "code": "0x6000"}),
            name="f4-real-code",
        ))
        self._assert_positive(result)

    def test_positive_storage_restoration(self) -> None:
        result, _ = _run(_scenario(
            CounterfactualMutation("set_storage_slot", {"address": ADDRESS, "slot": SLOT, "value": VALUE}),
            CounterfactualAssertion("storage_equals", {"address": ADDRESS, "slot": SLOT, "value": VALUE}),
            name="f4-real-storage",
        ))
        self._assert_positive(result)

    def test_positive_time_and_block_restoration(self) -> None:
        result, _ = _run(_scenario(
            CounterfactualMutation("advance_timestamp", {"timestamp": 1_700_000_100}),
            CounterfactualAssertion("block_number_at_least", {"block_number": 1}),
            name="f4-real-time-block",
        ))
        self._assert_positive(result)

    def test_persisted_real_anvil_evidence_validates(self) -> None:
        scenario = _scenario(
            CounterfactualMutation("set_native_balance", {"address": ADDRESS, "balance": 12345}),
            CounterfactualAssertion("native_balance_equals", {"address": ADDRESS, "balance": 12345}),
            name="f4-real-evidence-integrity",
        )
        result, _ = _run(scenario)
        with tempfile.TemporaryDirectory() as temporary:
            path = persist_result(result, scenario=scenario, artifact_root=temporary)
            validation = validate_persisted_result(path)
            self.assertTrue(validation["valid"])
            self.assertEqual(validation["provider_artifact_count"], 6)

    def test_real_provider_raw_mismatch_semantic_restore_passes(self) -> None:
        result, _ = _run(_scenario(
            CounterfactualMutation("set_native_balance", {"address": ADDRESS, "balance": 12345}),
            CounterfactualAssertion("native_balance_equals", {"address": ADDRESS, "balance": 12345}),
            name="f4-real-provider-reanchor",
        ), genesis_timestamp=None)
        self._assert_positive(result)
        self.assertFalse(result.evidence.restoration_evidence["raw_provider_dump_equal"])
        self.assertEqual(result.evidence.restoration_evidence["restoration_reason_code"], "RESTORED_SEMANTICALLY_RAW_DUMP_DIFFERED")

    def _assert_quarantine(self, result, reason: str) -> None:
        self.assertEqual(result.evidence.run_status, RunStatus.QUARANTINED)
        self.assertFalse(result.evidence.restoration_verified)
        self.assertEqual(result.evidence.restoration_evidence["restoration_reason_code"], reason)
        self.assertTrue(result.evidence.restoration_evidence["process_shutdown_verified"])
        self.assertTrue(result.evidence.restoration_evidence["port_release_verified"])

    def test_residual_balance_drift_quarantines(self) -> None:
        result, _ = _run(_scenario(CounterfactualMutation("set_native_balance", {"address": ADDRESS, "balance": 12345}), name="f4-negative-balance"), _ResidualBalance)
        self._assert_quarantine(result, "MUTATION_WITNESS_DRIFT_PROCESS_QUARANTINED")

    def test_residual_code_drift_quarantines(self) -> None:
        result, _ = _run(_scenario(CounterfactualMutation("set_contract_code", {"address": ADDRESS, "code": "0x6000"}), name="f4-negative-code"), _ResidualCode)
        self._assert_quarantine(result, "MUTATION_WITNESS_DRIFT_PROCESS_QUARANTINED")

    def test_residual_storage_drift_quarantines(self) -> None:
        result, _ = _run(_scenario(CounterfactualMutation("set_storage_slot", {"address": ADDRESS, "slot": SLOT, "value": VALUE}), name="f4-negative-storage"), _ResidualStorage)
        self._assert_quarantine(result, "MUTATION_WITNESS_DRIFT_PROCESS_QUARANTINED")

    def test_txpool_residue_quarantines(self) -> None:
        result, _ = _run(_scenario(CounterfactualMutation("set_native_balance", {"address": ADDRESS, "balance": 12345}), name="f4-negative-txpool"), _TxpoolResidue)
        self._assert_quarantine(result, "TXPOOL_DRIFT_PROCESS_QUARANTINED")

    def test_unknown_structural_difference_quarantines(self) -> None:
        result, _ = _run(_scenario(CounterfactualMutation("set_native_balance", {"address": ADDRESS, "balance": 12345}), name="f4-negative-unknown"), _UnknownDumpDifference)
        self._assert_quarantine(result, "UNKNOWN_DUMP_DIFFERENCE_PROCESS_QUARANTINED")

    def test_failed_revert_quarantines(self) -> None:
        result, _ = _run(_scenario(CounterfactualMutation("set_native_balance", {"address": ADDRESS, "balance": 12345}), name="f4-negative-revert"), _RefuseRevert)
        self._assert_quarantine(result, "REVERT_RPC_REJECTED_PROCESS_QUARANTINED")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
