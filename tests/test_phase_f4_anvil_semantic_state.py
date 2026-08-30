from __future__ import annotations

import copy
import gzip
import hashlib
import json
import unittest
from dataclasses import replace

from src.lane_ii.lab.anvil_state import (
    ANVIL_EXECUTION_STATE_SCHEMA,
    DifferenceClassification,
    MutationWitnessRecord,
    MutationWitnessSpec,
    RawDumpCapture,
    UnsupportedAnvilDumpSchema,
    capture_raw_dump,
    classify_structural_differences,
    restoration_verdict,
    semantic_state_from_components,
    validate_dump_schema,
    witness_specs,
)
from src.lane_ii.lab.contracts import CounterfactualMutation, ScenarioValidationError
from src.lane_ii.lab.evidence import _reject_secret


ZERO_ADDRESS = "0x" + ("00" * 20)
ADDRESS_A = "0x" + ("11" * 20)
ADDRESS_B = "0x" + ("22" * 20)
ZERO_32 = "0x" + ("00" * 32)
ONE_32 = "0x" + ("00" * 31) + "01"


def _head(*, timestamp: str = "0x64", state_root: str = ZERO_32) -> dict[str, object]:
    return {
        "parentHash": ZERO_32,
        "sha3Uncles": ZERO_32,
        "miner": ZERO_ADDRESS,
        "stateRoot": state_root,
        "transactionsRoot": ZERO_32,
        "receiptsRoot": ZERO_32,
        "logsBloom": "0x" + ("00" * 256),
        "difficulty": "0x0",
        "number": "0x0",
        "gasLimit": "0x1c9c380",
        "gasUsed": "0x0",
        "timestamp": timestamp,
        "extraData": "0x",
        "mixHash": ZERO_32,
        "nonce": "0x" + ("00" * 8),
        "baseFeePerGas": "0x3b9aca00",
        "withdrawalsRoot": ZERO_32,
        "blobGasUsed": "0x0",
        "excessBlobGas": "0x0",
        "parentBeaconBlockRoot": ZERO_32,
        "requestsHash": ZERO_32,
        "hash": ZERO_32,
        "totalDifficulty": "0x0",
        "size": "0x1",
        "transactions": [],
        "uncles": [],
        "withdrawals": [],
    }


def _dump() -> dict[str, object]:
    head = _head()
    header = {key: value for key, value in head.items() if key not in {"hash", "totalDifficulty", "size", "transactions", "uncles", "withdrawals"}}
    return {
        "block": {
            "number": "0x0",
            "beneficiary": ZERO_ADDRESS,
            "timestamp": "0x64",
            "gas_limit": 30_000_000,
            "basefee": 1_000_000_000,
            "difficulty": "0x0",
            "prevrandao": ZERO_32,
            "blob_excess_gas_and_price": {"excess_blob_gas": 0, "blob_gasprice": 1},
            "slot_num": 0,
        },
        "accounts": {
            ADDRESS_A: {"nonce": 0, "balance": "0x1", "code": "0x", "storage": {}},
            ADDRESS_B: {"nonce": 1, "balance": "0x2", "code": "0x6000", "storage": {ZERO_32: ONE_32}},
        },
        "best_block_number": 0,
        "blocks": [{"header": header, "transactions": [], "ommers": [], "withdrawals": None}],
        "transactions": [],
        "historical_states": None,
    }


def _metadata() -> dict[str, object]:
    return {
        "clientVersion": "anvil/v1.8.1",
        "clientSemver": "1.8.1+982849d314.1787939283.dist",
        "clientCommitSha": "982849d3140c01fd3b72905759581a132df7aa98",
        "chainId": 31337,
        "latestBlockHash": ZERO_32,
        "latestBlockNumber": 0,
        "instanceId": ONE_32,
        "forkedNetwork": None,
        "snapshots": {},
    }


def _node_info() -> dict[str, object]:
    return {
        "currentBlockNumber": "0x0",
        "currentBlockTimestamp": 100,
        "currentBlockHash": ZERO_32,
        "hardFork": "Bpo1",
        "transactionOrder": "fees",
        "environment": {"baseFee": "0x3b9aca00", "chainId": 31337, "gasLimit": "0x1c9c380", "gasPrice": "0x77359400"},
        "forkConfig": {"forkUrl": None, "forkBlockNumber": None, "forkRetryBackoff": None},
        "network": "ethereum",
    }


def _semantic(decoded: dict[str, object] | None = None, *, head: dict[str, object] | None = None, metadata: dict[str, object] | None = None):
    observation = semantic_state_from_components(
        decoded or _dump(),
        metadata=metadata or _metadata(),
        node_info=_node_info(),
        canonical_head=head or _head(),
        automine=True,
        interval_mining=None,
        expected_chain_id=31337,
    )
    return replace(observation, txpool={"pending": 0, "queued": 0})


def _raw(decoded: dict[str, object]) -> RawDumpCapture:
    encoded = gzip.compress(json.dumps(decoded, separators=(",", ":")).encode("utf-8"), mtime=0).hex()
    raw = "0x" + encoded
    return RawDumpCapture(raw, decoded, hashlib.sha256(raw.encode("utf-8")).hexdigest())


class PhaseF4AnvilSemanticStateTests(unittest.TestCase):
    def test_canonical_mapping_order_does_not_change_hash(self) -> None:
        first = _dump()
        second = dict(reversed(list(first.items())))
        self.assertEqual(_semantic(first).semantic_state_sha256, _semantic(second).semantic_state_sha256)

    def test_account_order_does_not_change_hash(self) -> None:
        second = _dump()
        second["accounts"] = dict(reversed(list(second["accounts"].items())))
        self.assertEqual(_semantic().semantic_state_sha256, _semantic(second).semantic_state_sha256)

    def test_storage_order_does_not_change_hash(self) -> None:
        first = _dump()
        first["accounts"][ADDRESS_B]["storage"] = {ZERO_32: ONE_32, ONE_32: ZERO_32}
        second = copy.deepcopy(first)
        second["accounts"][ADDRESS_B]["storage"] = dict(reversed(list(second["accounts"][ADDRESS_B]["storage"].items())))
        self.assertEqual(_semantic(first).semantic_state_sha256, _semantic(second).semantic_state_sha256)

    def test_hex_normalization_is_deterministic(self) -> None:
        second = _dump()
        record = second["accounts"].pop(ADDRESS_B)
        record["code"] = "0xAA00"
        second["accounts"][ADDRESS_B.upper().replace("0X", "0x")] = record
        first = copy.deepcopy(second)
        first["accounts"][ADDRESS_B]["code"] = "0xaa00"
        self.assertEqual(_semantic(first).semantic_state_sha256, _semantic(second).semantic_state_sha256)

    def _assert_account_change_changes_hash(self, mutator) -> None:
        changed = _dump()
        mutator(changed)
        self.assertNotEqual(_semantic().semantic_state_sha256, _semantic(changed).semantic_state_sha256)

    def test_balance_change_alters_hash(self) -> None:
        self._assert_account_change_changes_hash(lambda state: state["accounts"][ADDRESS_A].__setitem__("balance", "0x2"))

    def test_nonce_change_alters_hash(self) -> None:
        self._assert_account_change_changes_hash(lambda state: state["accounts"][ADDRESS_A].__setitem__("nonce", 1))

    def test_code_change_alters_hash(self) -> None:
        self._assert_account_change_changes_hash(lambda state: state["accounts"][ADDRESS_A].__setitem__("code", "0x6000"))

    def test_storage_change_alters_hash(self) -> None:
        self._assert_account_change_changes_hash(lambda state: state["accounts"][ADDRESS_B]["storage"].__setitem__(ZERO_32, ZERO_32))

    def test_account_addition_alters_hash(self) -> None:
        self._assert_account_change_changes_hash(lambda state: state["accounts"].__setitem__("0x" + ("33" * 20), {"nonce": 0, "balance": "0x0", "code": "0x", "storage": {}}))

    def test_account_removal_alters_hash(self) -> None:
        self._assert_account_change_changes_hash(lambda state: state["accounts"].pop(ADDRESS_A))

    def test_canonical_head_change_fails_restoration(self) -> None:
        before = _semantic()
        after = _semantic(head=_head(state_root=ONE_32))
        ok, reason = restoration_verdict(revert_succeeded=True, before_semantic=before, after_semantic=after, witnesses=(), differences=())
        self.assertFalse(ok)
        self.assertEqual(reason, "SEMANTIC_STATE_DRIFT_PROCESS_QUARANTINED")

    def test_timestamp_head_change_fails_restoration(self) -> None:
        after = _semantic(head=_head(timestamp="0x65"))
        self.assertNotEqual(_semantic().semantic_state_sha256, after.semantic_state_sha256)

    def test_txpool_residue_fails_restoration(self) -> None:
        before = replace(_semantic(), txpool={"pending": 0, "queued": 0})
        after = replace(_semantic(), txpool={"pending": 1, "queued": 0})
        self.assertEqual(restoration_verdict(revert_succeeded=True, before_semantic=before, after_semantic=after, witnesses=(), differences=())[1], "TXPOOL_DRIFT_PROCESS_QUARANTINED")

    def test_exact_pinned_provider_difference_is_classified(self) -> None:
        before = _dump()
        before["block"]["timestamp"] = "0x1"
        after = _dump()
        differences = classify_structural_differences(_raw(before), _raw(after), before_semantic=_semantic(before), after_semantic=_semantic(after))
        self.assertEqual(len(differences), 1)
        self.assertEqual(differences[0].classification, DifferenceClassification.PROVIDER_SERIALIZATION)

    def test_unknown_difference_fails_closed(self) -> None:
        after = _dump()
        after["new_provider_field"] = 1
        differences = classify_structural_differences(_raw(_dump()), _raw(after), before_semantic=_semantic(), after_semantic=None)
        self.assertEqual(restoration_verdict(revert_succeeded=True, before_semantic=_semantic(), after_semantic=_semantic(), witnesses=(), differences=differences)[1], "UNKNOWN_DUMP_DIFFERENCE_PROCESS_QUARANTINED")

    def test_unsupported_dump_schema_fails_closed(self) -> None:
        malformed = _dump()
        malformed["new_provider_field"] = 1
        with self.assertRaises(UnsupportedAnvilDumpSchema):
            validate_dump_schema(malformed)

    def test_malformed_dump_fails_closed(self) -> None:
        with self.assertRaises(UnsupportedAnvilDumpSchema):
            capture_raw_dump(lambda _method, _params: "0x00")

    def test_missing_required_account_field_fails_closed(self) -> None:
        malformed = _dump()
        malformed["accounts"][ADDRESS_A].pop("balance")
        with self.assertRaises(UnsupportedAnvilDumpSchema):
            validate_dump_schema(malformed)

    def test_missing_restoration_witness_fails_closed(self) -> None:
        witness = MutationWitnessRecord(MutationWitnessSpec("set_native_balance", {"address": ADDRESS_A}, "eth_getBalance"), 1, 2, {"observation": "UNAVAILABLE"})
        self.assertEqual(restoration_verdict(revert_succeeded=True, before_semantic=_semantic(), after_semantic=_semantic(), witnesses=(witness,), differences=())[1], "MUTATION_WITNESS_DRIFT_PROCESS_QUARANTINED")

    def test_raw_mismatch_semantic_equality_and_witnesses_can_pass(self) -> None:
        before = _dump()
        before["block"]["timestamp"] = "0x1"
        differences = classify_structural_differences(_raw(before), _raw(_dump()), before_semantic=_semantic(before), after_semantic=_semantic())
        witness = MutationWitnessRecord(MutationWitnessSpec("set_native_balance", {"address": ADDRESS_A}, "eth_getBalance"), 1, 2, 1)
        self.assertEqual(restoration_verdict(revert_succeeded=True, before_semantic=_semantic(before), after_semantic=_semantic(), witnesses=(witness,), differences=differences), (True, "RESTORED_SEMANTICALLY_RAW_DUMP_DIFFERED"))

    def test_raw_equality_with_witness_mismatch_fails(self) -> None:
        witness = MutationWitnessRecord(MutationWitnessSpec("set_native_balance", {"address": ADDRESS_A}, "eth_getBalance"), 1, 2, 3)
        self.assertFalse(restoration_verdict(revert_succeeded=True, before_semantic=_semantic(), after_semantic=_semantic(), witnesses=(witness,), differences=())[0])

    def test_semantic_equality_with_unknown_raw_difference_fails(self) -> None:
        after = _dump()
        after["unknown"] = True
        differences = classify_structural_differences(_raw(_dump()), _raw(after), before_semantic=_semantic(), after_semantic=_semantic())
        self.assertFalse(restoration_verdict(revert_succeeded=True, before_semantic=_semantic(), after_semantic=_semantic(), witnesses=(), differences=differences)[0])

    def test_classification_rule_is_exact_and_version_bound(self) -> None:
        before = _dump()
        before["block"]["timestamp"] = "0x1"
        wrong = replace(_semantic(before), provider_identity={**_semantic(before).provider_identity, "client_commit": "0" * 40})
        difference = classify_structural_differences(_raw(before), _raw(_dump()), before_semantic=wrong, after_semantic=_semantic())[0]
        self.assertEqual(difference.classification, DifferenceClassification.CANONICAL_CHAIN_STATE)

    def test_another_anvil_version_cannot_reuse_schema(self) -> None:
        metadata = _metadata()
        metadata["clientVersion"] = "anvil/v1.8.2"
        with self.assertRaises(Exception):
            _semantic(metadata=metadata)

    def test_unknown_mutation_without_witness_is_rejected(self) -> None:
        with self.assertRaises(ScenarioValidationError):
            witness_specs((CounterfactualMutation("impersonate_account", {"address": ADDRESS_A}),))

    def test_failed_revert_cannot_be_overridden_by_semantic_equality(self) -> None:
        self.assertEqual(restoration_verdict(revert_succeeded=False, before_semantic=_semantic(), after_semantic=_semantic(), witnesses=(), differences=())[1], "REVERT_RPC_REJECTED_PROCESS_QUARANTINED")

    def test_secret_filter_remains_fail_closed(self) -> None:
        with self.assertRaises(ScenarioValidationError):
            _reject_secret({"note": "seed phrase must never persist"})

    def test_schema_identifier_is_explicit(self) -> None:
        self.assertEqual(_semantic().projection["fingerprint_schema"], ANVIL_EXECUTION_STATE_SCHEMA)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
