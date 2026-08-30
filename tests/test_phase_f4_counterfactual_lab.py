from __future__ import annotations

import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.lane_ii.boundary import evaluate_lane_ii_authority
from src.lane_ii.lab.anvil_backend import PINNED_ANVIL_VERSION, AnvilBackend, anvil_version, installed_anvil
from src.lane_ii.lab.contracts import (
    BackendType,
    COUNTERFACTUAL_ONLY,
    CounterfactualAssertion,
    CounterfactualMutation,
    CounterfactualScenario,
    RunStatus,
    SCENARIO_SCHEMA_VERSION,
    ScenarioValidationError,
)
from src.lane_ii.lab.coordinator import CounterfactualCoordinator
from src.lane_ii.lab.evidence import persist_result
from src.lane_ii.lab.model_backend import VenueModelBackend
from src.lane_ii.lab.rpc import assert_loopback_endpoint
from src.lane_ii.lab.scenario import branch_scenario, scenario_from_dict


def model_scenario(
    *mutations: CounterfactualMutation,
    assertions: tuple[CounterfactualAssertion, ...] = (),
    scenario_id: str = "f4-model-case",
) -> CounterfactualScenario:
    return CounterfactualScenario(
        schema_version=SCENARIO_SCHEMA_VERSION,
        scenario_id=scenario_id,
        scenario_version="v1",
        deterministic_seed=7331,
        backend=BackendType.MODEL,
        target_domain="SYNTHETIC_HYPERCORE",
        target_chain_id=None,
        fixed_fork_block=None,
        initial_state_fingerprint=VenueModelBackend.expected_initial_fingerprint(),
        mutations=tuple(mutations),
        assertions=assertions,
        timeout_seconds=15,
    )


class _FailAfterMutation(VenueModelBackend):
    def apply(self, mutation: CounterfactualMutation) -> None:
        super().apply(mutation)
        raise RuntimeError("deliberate mutation failure")


class _RefuseRevert(VenueModelBackend):
    def revert(self, snapshot: object) -> bool:
        return False


class PhaseF4CounterfactualLabTests(unittest.TestCase):
    def test_malformed_and_unknown_scenarios_fail_before_mutation(self) -> None:
        with self.assertRaises(ScenarioValidationError):
            scenario_from_dict({"schema_version": SCENARIO_SCHEMA_VERSION})
        with self.assertRaises(ScenarioValidationError):
            model_scenario(CounterfactualMutation("not_a_real_mutation", {}))
        with self.assertRaises(ScenarioValidationError):
            CounterfactualMutation("set_balance", {"method": "anvil_setBalance"})
        with self.assertRaises(ScenarioValidationError):
            CounterfactualMutation("set_balance", {"private_key": "not-accepted"})
        with self.assertRaises(ScenarioValidationError):
            CounterfactualMutation("set_balance", {"actor": "0x" + ("11" * 32)})

    def test_loopback_and_authority_routing_refuse_substitution(self) -> None:
        self.assertEqual(assert_loopback_endpoint("http://127.0.0.1:8545"), "http://127.0.0.1:8545")
        for endpoint in ("http://localhost:8545", "https://api.hyperliquid-testnet.xyz", "http://10.0.0.2:8545"):
            with self.subTest(endpoint=endpoint), self.assertRaises(ScenarioValidationError):
                assert_loopback_endpoint(endpoint)

    def test_hashes_are_deterministic_and_mutation_order_is_semantic(self) -> None:
        first = model_scenario(
            CounterfactualMutation("set_balance", {"asset": "USD", "amount": 10, "actor": "actor"}),
            CounterfactualMutation("set_mark_price", {"symbol": "BTC", "price": 100}),
        )
        same = model_scenario(
            CounterfactualMutation("set_balance", {"actor": "actor", "amount": 10, "asset": "USD"}),
            CounterfactualMutation("set_mark_price", {"price": 100, "symbol": "BTC"}),
        )
        reversed_order = model_scenario(
            CounterfactualMutation("set_mark_price", {"symbol": "BTC", "price": 100}),
            CounterfactualMutation("set_balance", {"actor": "actor", "asset": "USD", "amount": 10}),
        )
        self.assertEqual(first.scenario_hash, same.scenario_hash)
        self.assertNotEqual(first.scenario_hash, reversed_order.scenario_hash)
        child = branch_scenario(first, scenario_id="f4-child", mutation_delta=(CounterfactualMutation("advance_time", {"seconds": 1}),))
        self.assertEqual(child.parent_scenario_hash, first.scenario_hash)
        self.assertEqual(child.mutation_delta[0].verb, "advance_time")

    def test_mutation_experiment_and_assertion_failures_restore_state(self) -> None:
        captured: list[VenueModelBackend] = []

        def failing_factory() -> VenueModelBackend:
            backend = _FailAfterMutation()
            captured.append(backend)
            return backend

        mutation_failure = CounterfactualCoordinator(model_factory=failing_factory).run(
            model_scenario(CounterfactualMutation("set_balance", {"actor": "actor", "asset": "USD", "amount": 1}))
        )
        self.assertEqual(mutation_failure.evidence.run_status, RunStatus.FAILED)
        self.assertTrue(mutation_failure.evidence.restoration_verified)
        self.assertEqual(captured[0].fingerprint(), VenueModelBackend.expected_initial_fingerprint())

        experiment_failure = CounterfactualCoordinator().run(
            model_scenario(CounterfactualMutation("advance_time", {"seconds": 1})), force_experiment_failure=True
        )
        self.assertEqual(experiment_failure.evidence.run_status, RunStatus.FAILED)
        self.assertTrue(experiment_failure.evidence.restoration_verified)

        assertion_failure = CounterfactualCoordinator().run(
            model_scenario(
                CounterfactualMutation("set_balance", {"actor": "actor", "asset": "USD", "amount": 1}),
                assertions=(CounterfactualAssertion("balance_equals", {"actor": "actor", "asset": "USD", "amount": 2}),),
            )
        )
        self.assertEqual(assertion_failure.evidence.run_status, RunStatus.FAILED)
        self.assertTrue(assertion_failure.evidence.restoration_verified)

    def test_revert_failure_quarantines_and_discards_the_universe(self) -> None:
        captured: list[_RefuseRevert] = []

        def factory() -> VenueModelBackend:
            backend = _RefuseRevert()
            captured.append(backend)
            return backend

        result = CounterfactualCoordinator(model_factory=factory).run(
            model_scenario(CounterfactualMutation("advance_time", {"seconds": 1}))
        )
        self.assertEqual(result.evidence.run_status, RunStatus.QUARANTINED)
        self.assertEqual(result.evidence.cleanup_status, "REVERT_FAILED_PROCESS_QUARANTINED")
        with self.assertRaises(Exception):
            captured[0].start()

    def test_hostile_states_converge_and_fail_closed(self) -> None:
        backend = VenueModelBackend()
        backend.start()
        backend.apply(CounterfactualMutation("inject_external_position", {"symbol": "BTC", "quantity": 1}))
        self.assertFalse(backend.entry_is_safe())
        self.assertEqual(backend.state()["safety_state"], "RECONCILIATION_REQUIRED")

        backend = VenueModelBackend()
        backend.start()
        backend.apply(CounterfactualMutation("inject_submission_timeout", {}))
        self.assertEqual(backend.state()["submission_state"], "UNKNOWN")
        self.assertFalse(backend.entry_is_safe())

        backend = VenueModelBackend()
        backend.start()
        first = {"fill_id": "fill-1", "symbol": "BTC", "quantity": 0.4, "price": 100, "timestamp": 20}
        backend.apply(CounterfactualMutation("inject_partial_fill", first))
        backend.apply(CounterfactualMutation("inject_duplicate_fill", first))
        backend.apply(CounterfactualMutation("inject_out_of_order_fill", {"fill_id": "fill-2", "symbol": "BTC", "quantity": 0.6, "price": 100, "timestamp": 10}))
        self.assertEqual(len(backend.state()["fills"]), 2)
        backend.apply(CounterfactualMutation("inject_open_order", {"order_id": "foreign-order", "symbol": "BTC", "quantity": 1}))
        self.assertTrue(backend.state()["open_orders"][0]["foreign"])
        backend.apply(CounterfactualMutation("change_precision", {"symbol": "BTC", "quantity_decimals": 3}))
        self.assertFalse(backend.entry_is_safe())

    def test_concurrent_runs_receive_separate_mutable_universes(self) -> None:
        instances: list[VenueModelBackend] = []

        def factory() -> VenueModelBackend:
            backend = VenueModelBackend()
            instances.append(backend)
            return backend

        scenario = model_scenario(CounterfactualMutation("set_balance", {"actor": "actor", "asset": "USD", "amount": 100}))
        coordinator = CounterfactualCoordinator(model_factory=factory)
        with ThreadPoolExecutor(max_workers=2) as workers:
            results = list(workers.map(lambda _: coordinator.run(scenario), range(2)))
        self.assertEqual({item.evidence.run_status for item in results}, {RunStatus.SUCCEEDED})
        self.assertEqual(len(instances), 2)
        self.assertIsNot(instances[0], instances[1])
        self.assertTrue(all(item.fingerprint() == VenueModelBackend.expected_initial_fingerprint() for item in instances))

    def test_a_backend_factory_cannot_reuse_a_mutable_universe(self) -> None:
        one_backend = VenueModelBackend()
        coordinator = CounterfactualCoordinator(model_factory=lambda: one_backend)
        scenario = model_scenario(CounterfactualMutation("advance_time", {"seconds": 1}))
        self.assertEqual(coordinator.run(scenario).evidence.run_status, RunStatus.SUCCEEDED)
        with self.assertRaises(Exception):
            coordinator.run(scenario)

    def test_evidence_cannot_become_f0_or_f2_authority_and_artifacts_are_secret_free(self) -> None:
        scenario = model_scenario(CounterfactualMutation("set_balance", {"actor": "actor", "asset": "USD", "amount": 3}))
        result = CounterfactualCoordinator().run(scenario)
        self.assertEqual(result.evidence.provenance, COUNTERFACTUAL_ONLY)
        self.assertFalse(evaluate_lane_ii_authority(result.evidence, [result.evidence]).allowed)
        from src.lane_ii.phase_d_bridge import LaneIIAdmissionRefused, LaneIIPhaseDBridge
        bridge = LaneIIPhaseDBridge(None, execution_account_id="counterfactual-test", phase_d_notional_limit=1.0)
        with self.assertRaises(LaneIIAdmissionRefused):
            bridge._verify_entry_request(result.evidence)
        with tempfile.TemporaryDirectory() as temp:
            target = persist_result(result, scenario=scenario, artifact_root=temp)
            text = target.read_text(encoding="utf-8").lower()
            self.assertNotIn("private_key", text)
            self.assertNotIn("mnemonic", text)
            self.assertIn("counterfactual_only", text)

    @unittest.skipUnless(
        bool(anvil_version(installed_anvil()) and PINNED_ANVIL_VERSION in (anvil_version(installed_anvil()) or "")),
        "Pinned Foundry Anvil v1.8.1 is unavailable; real-Anvil commissioning is blocked.",
    )
    def test_real_anvil_snapshot_restore_and_termination_smoke(self) -> None:
        backend = AnvilBackend(chain_id=31337)
        backend.start()
        address = "0x1111111111111111111111111111111111111111"
        slot = "0x" + ("00" * 32)
        original_balance = backend._native_balance(address)
        original_code = backend._code(address)
        original_storage = backend._storage(address, slot)
        snapshot = backend.snapshot()
        endpoint = backend.endpoint
        try:
            backend.apply(CounterfactualMutation("set_native_balance", {"address": address, "balance": 12345}))
            backend.apply(CounterfactualMutation("set_contract_code", {"address": address, "code": "0x6000"}))
            backend.apply(CounterfactualMutation("set_storage_slot", {"address": address, "slot": slot, "value": "0x" + ("01" * 32)}))
            raise RuntimeError("deliberate experiment failure")
        except RuntimeError:
            self.assertTrue(backend.revert(snapshot))
        self.assertEqual(backend._native_balance(address), original_balance)
        self.assertEqual(backend._code(address), original_code)
        self.assertEqual(backend._storage(address, slot), original_storage)
        backend.close()
        self.assertFalse(backend.running)
        self.assertIn("127.0.0.1", endpoint)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
