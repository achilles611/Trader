"""Strict Anvil v1.8.1 restoration observations for f4.1.1.

The raw provider dump is forensic evidence.  The semantic fingerprint is a
separate, version-bound commitment to account state, canonical chain state,
and observable execution configuration.  Nothing in this module grants a
scenario access to arbitrary JSON-RPC methods.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .contracts import CounterfactualMutation, ScenarioValidationError, canonical_hash, canonical_json, validate_address


ANVIL_EXECUTION_STATE_SCHEMA = "ANVIL_EXECUTION_STATE_V1"
PINNED_ANVIL_SEMVER = "1.8.1"
PINNED_ANVIL_COMMIT = "982849d3140c01fd3b72905759581a132df7aa98"
PINNED_ANVIL_RELEASE_TAG = "v1.8.1"
PINNED_ANVIL_RELEASE_PROVENANCE = "github-foundry-rs-foundry-release-v1.8.1"
PINNED_WINDOWS_ARCHIVE_SHA256 = "02d98fc2c573793960ee06b7f642487d483fe30572f7e248804c207334a418d8"
PINNED_WINDOWS_ANVIL_SHA256 = "c6e29da1b010fe00bac6c0dc5c29484bd641deb5a84050aea10d13e9dc4fe26f"
MAX_RAW_DUMP_BYTES = 64 * 1024 * 1024
MAX_DECODED_DUMP_BYTES = 64 * 1024 * 1024


class UnsupportedAnvilDumpSchema(RuntimeError):
    """The provider dump is malformed or not the exact commissioned schema."""


class RestorationObservationError(RuntimeError):
    """A required independent restoration observation was unavailable."""


class DifferenceClassification(StrEnum):
    EXECUTION_STATE = "EXECUTION_STATE"
    CANONICAL_CHAIN_STATE = "CANONICAL_CHAIN_STATE"
    TRANSACTION_POOL_STATE = "TRANSACTION_POOL_STATE"
    DECLARED_MUTATION_WITNESS = "DECLARED_MUTATION_WITNESS"
    PROVIDER_SERIALIZATION = "PROVIDER_SERIALIZATION"
    PROVIDER_RETAINED_HISTORY = "PROVIDER_RETAINED_HISTORY"
    SNAPSHOT_LIFECYCLE_METADATA = "SNAPSHOT_LIFECYCLE_METADATA"
    VOLATILE_PROCESS_EVIDENCE = "VOLATILE_PROCESS_EVIDENCE"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class RawDumpCapture:
    raw: str
    decoded: Mapping[str, object]
    sha256: str


@dataclass(frozen=True)
class SemanticObservation:
    projection: Mapping[str, object]
    semantic_state_sha256: str
    canonical_head: Mapping[str, object]
    txpool: Mapping[str, int]
    provider_identity: Mapping[str, object]
    provider_environment: Mapping[str, object]


@dataclass(frozen=True)
class MutationWitnessSpec:
    mutation_verb: str
    target: Mapping[str, object]
    probe: str
    equality_rule: str = "EXACT_JSON_EQUALITY"
    missingness_rule: str = "MISSING_IS_DISTINCT"

    def payload(self) -> dict[str, object]:
        return {
            "mutation_verb": self.mutation_verb,
            "target": dict(self.target),
            "probe": self.probe,
            "equality_rule": self.equality_rule,
            "missingness_rule": self.missingness_rule,
        }


@dataclass(frozen=True)
class MutationWitnessRecord:
    spec: MutationWitnessSpec
    before: object
    mutated: object
    restored: object

    @property
    def mutation_observed(self) -> bool:
        return self.before != self.mutated

    @property
    def restored_exactly(self) -> bool:
        return self.after_is_present and self.restored == self.before

    @property
    def after_is_present(self) -> bool:
        return self.restored is not _MISSING

    def payload(self) -> dict[str, object]:
        return {
            "spec": self.spec.payload(),
            "before": _json_value(self.before),
            "mutated": _json_value(self.mutated),
            "restored": _json_value(self.restored),
            "mutation_observed": self.mutation_observed,
            "restored_exactly": self.restored_exactly,
        }


@dataclass(frozen=True)
class StructuralDifference:
    json_pointer: str
    before_type: str
    after_type: str
    before_value: object
    after_value: object
    classification: DifferenceClassification
    classification_rule_id: str
    participates_in_semantic_fingerprint: bool
    justification: str

    def payload(self) -> dict[str, object]:
        return {
            "json_pointer": self.json_pointer,
            "before_type": self.before_type,
            "after_type": self.after_type,
            "before_value": self.before_value,
            "after_value": self.after_value,
            "classification": self.classification.value,
            "classification_rule_id": self.classification_rule_id,
            "participates_in_semantic_fingerprint": self.participates_in_semantic_fingerprint,
            "justification": self.justification,
        }


class _Missing:
    pass


_MISSING = _Missing()
_RPC = Callable[[str, list[object]], object]
_TOP_LEVEL_KEYS = {"block", "accounts", "best_block_number", "blocks", "transactions", "historical_states"}
_BLOCK_ENV_KEYS = {
    "number", "beneficiary", "timestamp", "gas_limit", "basefee", "difficulty",
    "prevrandao", "blob_excess_gas_and_price", "slot_num",
}
_BLOB_ENV_KEYS = {"excess_blob_gas", "blob_gasprice"}
_ACCOUNT_KEYS = {"nonce", "balance", "code", "storage"}
_SERIALIZED_BLOCK_KEYS = {"header", "transactions", "ommers", "withdrawals"}
_HEAD_KEYS = {
    "parentHash", "sha3Uncles", "miner", "stateRoot", "transactionsRoot", "receiptsRoot",
    "logsBloom", "difficulty", "number", "gasLimit", "gasUsed", "timestamp", "extraData",
    "mixHash", "nonce", "baseFeePerGas", "withdrawalsRoot", "blobGasUsed", "excessBlobGas",
    "parentBeaconBlockRoot", "requestsHash", "hash", "totalDifficulty", "size", "transactions",
    "uncles", "withdrawals",
}
_SERIALIZED_HEADER_KEYS = _HEAD_KEYS - {"hash", "totalDifficulty", "size", "transactions", "uncles", "withdrawals"}
_NODE_INFO_KEYS = {
    "currentBlockNumber", "currentBlockTimestamp", "currentBlockHash", "hardFork",
    "transactionOrder", "environment", "forkConfig", "network",
}
_NODE_ENV_KEYS = {"baseFee", "chainId", "gasLimit", "gasPrice"}
_FORK_CONFIG_KEYS = {"forkUrl", "forkBlockNumber", "forkRetryBackoff"}
_METADATA_KEYS = {
    "clientVersion", "clientSemver", "clientCommitSha", "chainId", "latestBlockHash",
    "latestBlockNumber", "instanceId", "forkedNetwork", "snapshots",
}
_COMMISSIONED_MUTATION_PROBES = {
    "set_native_balance": "eth_getBalance",
    "set_nonce": "eth_getTransactionCount",
    "set_contract_code": "eth_getCode",
    "set_storage_slot": "eth_getStorageAt",
    "advance_timestamp": "canonical_head",
    "mine_block": "canonical_head",
    "mine_blocks": "canonical_head",
}


def sha256_exact_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def capture_raw_dump(rpc: _RPC) -> RawDumpCapture:
    raw = rpc("anvil_dumpState", [])
    if not isinstance(raw, str):
        raise UnsupportedAnvilDumpSchema("anvil_dumpState did not return text.")
    encoded = raw.encode("utf-8")
    if not encoded or len(encoded) > MAX_RAW_DUMP_BYTES or not raw.startswith("0x") or len(raw) % 2:
        raise UnsupportedAnvilDumpSchema("Anvil raw dump encoding is malformed or unbounded.")
    try:
        compressed = bytes.fromhex(raw[2:])
        decoded_bytes = gzip.decompress(compressed)
        if len(decoded_bytes) > MAX_DECODED_DUMP_BYTES:
            raise UnsupportedAnvilDumpSchema("Decoded Anvil dump exceeds the evidence bound.")
        decoded = json.loads(decoded_bytes.decode("utf-8"))
    except (ValueError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UnsupportedAnvilDumpSchema("Anvil raw dump could not be decoded as gzip JSON.") from exc
    if not isinstance(decoded, dict):
        raise UnsupportedAnvilDumpSchema("Decoded Anvil dump must be an object.")
    return RawDumpCapture(raw, decoded, hashlib.sha256(encoded).hexdigest())


def _exact_keys(value: object, expected: set[str], field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise UnsupportedAnvilDumpSchema(f"{field} does not match the pinned Anvil v1.8.1 schema.")
    return value


def _quantity(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise UnsupportedAnvilDumpSchema(f"{field} is not an unsigned quantity.")
    if isinstance(value, int):
        if value < 0:
            raise UnsupportedAnvilDumpSchema(f"{field} is not an unsigned quantity.")
        return value
    if isinstance(value, str) and value.startswith("0x") and value[2:] and all(char in "0123456789abcdefABCDEF" for char in value[2:]):
        return int(value, 16)
    raise UnsupportedAnvilDumpSchema(f"{field} is not a deterministic unsigned quantity.")


def _hex_bytes(value: object, field: str, *, exact_bytes: int | None = None) -> str:
    if not isinstance(value, str) or not value.startswith("0x") or len(value) % 2:
        raise UnsupportedAnvilDumpSchema(f"{field} is not even-length hexadecimal bytes.")
    digits = value[2:]
    if any(char not in "0123456789abcdefABCDEF" for char in digits):
        raise UnsupportedAnvilDumpSchema(f"{field} is not hexadecimal bytes.")
    if exact_bytes is not None and len(digits) != exact_bytes * 2:
        raise UnsupportedAnvilDumpSchema(f"{field} has the wrong byte width.")
    return "0x" + digits.lower()


def _address(value: object, field: str) -> str:
    try:
        return validate_address(value, field)
    except ScenarioValidationError as exc:
        raise UnsupportedAnvilDumpSchema(f"{field} is not an address.") from exc


def validate_dump_schema(decoded: Mapping[str, object]) -> tuple[dict[str, object], ...]:
    top = _exact_keys(decoded, _TOP_LEVEL_KEYS, "dump")
    block = _exact_keys(top["block"], _BLOCK_ENV_KEYS, "dump.block")
    _address(block["beneficiary"], "dump.block.beneficiary")
    for field in ("number", "timestamp", "gas_limit", "basefee", "difficulty", "slot_num"):
        _quantity(block[field], f"dump.block.{field}")
    _hex_bytes(block["prevrandao"], "dump.block.prevrandao", exact_bytes=32)
    blob = _exact_keys(block["blob_excess_gas_and_price"], _BLOB_ENV_KEYS, "dump.block.blob_excess_gas_and_price")
    _quantity(blob["excess_blob_gas"], "dump.block.blob_excess_gas_and_price.excess_blob_gas")
    _quantity(blob["blob_gasprice"], "dump.block.blob_excess_gas_and_price.blob_gasprice")
    _quantity(top["best_block_number"], "dump.best_block_number")
    if top["historical_states"] is not None:
        raise UnsupportedAnvilDumpSchema("Historical states must be absent from commissioned dumps.")
    if top["transactions"] != []:
        raise UnsupportedAnvilDumpSchema("Commissioned dumps may not retain transactions.")

    accounts = top["accounts"]
    if not isinstance(accounts, Mapping):
        raise UnsupportedAnvilDumpSchema("dump.accounts must be an object.")
    projected_accounts: list[dict[str, object]] = []
    for raw_address in sorted(accounts):
        address = _address(raw_address, "dump account key")
        record = _exact_keys(accounts[raw_address], _ACCOUNT_KEYS, f"dump.accounts.{address}")
        storage = record["storage"]
        if not isinstance(storage, Mapping):
            raise UnsupportedAnvilDumpSchema(f"dump.accounts.{address}.storage must be an object.")
        projected_storage = []
        for raw_slot in sorted(storage):
            projected_storage.append({
                "slot": _hex_bytes(raw_slot, "storage slot", exact_bytes=32),
                "value": _hex_bytes(storage[raw_slot], "storage value", exact_bytes=32),
            })
        projected_accounts.append({
            "address": address,
            "nonce": _quantity(record["nonce"], f"dump.accounts.{address}.nonce"),
            "balance": _quantity(record["balance"], f"dump.accounts.{address}.balance"),
            "code": _hex_bytes(record["code"], f"dump.accounts.{address}.code"),
            "storage": projected_storage,
        })

    blocks = top["blocks"]
    if not isinstance(blocks, list) or not blocks:
        raise UnsupportedAnvilDumpSchema("dump.blocks must contain the canonical local block history.")
    for index, value in enumerate(blocks):
        serialized = _exact_keys(value, _SERIALIZED_BLOCK_KEYS, f"dump.blocks[{index}]")
        header = _exact_keys(serialized["header"], _SERIALIZED_HEADER_KEYS, f"dump.blocks[{index}].header")
        _normalize_head_fields(header, f"dump.blocks[{index}].header", serialized=True)
        if serialized["transactions"] != [] or serialized["ommers"] != []:
            raise UnsupportedAnvilDumpSchema("Commissioned block history may not contain transactions or ommers.")
        if serialized["withdrawals"] not in (None, []):
            raise UnsupportedAnvilDumpSchema("Commissioned block withdrawals are unsupported.")
    return tuple(projected_accounts)


def _normalize_head_fields(head: Mapping[str, object], field: str, *, serialized: bool = False) -> dict[str, object]:
    quantity_fields = {
        "difficulty", "number", "gasLimit", "gasUsed", "timestamp", "baseFeePerGas",
        "blobGasUsed", "excessBlobGas", "totalDifficulty", "size",
    }
    address_fields = {"miner"}
    byte_widths = {
        "parentHash": 32, "sha3Uncles": 32, "stateRoot": 32, "transactionsRoot": 32,
        "receiptsRoot": 32, "logsBloom": 256, "mixHash": 32, "nonce": 8,
        "withdrawalsRoot": 32, "parentBeaconBlockRoot": 32, "requestsHash": 32, "hash": 32,
    }
    result: dict[str, object] = {}
    for key in sorted(head):
        value = head[key]
        if key in quantity_fields:
            result[key] = _quantity(value, f"{field}.{key}")
        elif key in address_fields:
            result[key] = _address(value, f"{field}.{key}")
        elif key in byte_widths:
            result[key] = _hex_bytes(value, f"{field}.{key}", exact_bytes=byte_widths[key])
        elif key == "extraData":
            result[key] = _hex_bytes(value, f"{field}.{key}")
        elif not serialized and key in {"transactions", "uncles", "withdrawals"}:
            if value != []:
                raise RestorationObservationError(f"{field}.{key} must be empty in the commissioned laboratory.")
            result[key] = []
        else:
            raise UnsupportedAnvilDumpSchema(f"{field}.{key} is not part of the pinned schema.")
    return result


def canonical_head_from_rpc(rpc: _RPC) -> dict[str, object]:
    value = rpc("eth_getBlockByNumber", ["latest", False])
    head = _exact_keys(value, _HEAD_KEYS, "eth_getBlockByNumber(latest)")
    return _normalize_head_fields(head, "canonical_head")


def _txpool_from_rpc(rpc: _RPC) -> dict[str, int]:
    value = _exact_keys(rpc("txpool_status", []), {"pending", "queued"}, "txpool_status")
    return {"pending": _quantity(value["pending"], "txpool.pending"), "queued": _quantity(value["queued"], "txpool.queued")}


def semantic_state_from_components(
    decoded: Mapping[str, object],
    *,
    metadata: object,
    node_info: object,
    canonical_head: Mapping[str, object],
    automine: object,
    interval_mining: object,
    expected_chain_id: int,
) -> SemanticObservation:
    accounts = validate_dump_schema(decoded)
    canonical_head = _normalize_head_fields(
        _exact_keys(canonical_head, _HEAD_KEYS, "canonical_head"),
        "canonical_head",
    )
    meta = _exact_keys(metadata, _METADATA_KEYS, "anvil_metadata")
    node = _exact_keys(node_info, _NODE_INFO_KEYS, "anvil_nodeInfo")
    environment = _exact_keys(node["environment"], _NODE_ENV_KEYS, "anvil_nodeInfo.environment")
    fork_config = _exact_keys(node["forkConfig"], _FORK_CONFIG_KEYS, "anvil_nodeInfo.forkConfig")
    if meta["clientVersion"] != f"anvil/v{PINNED_ANVIL_SEMVER}" or meta["clientCommitSha"] != PINNED_ANVIL_COMMIT:
        raise RestorationObservationError("Anvil client identity is not the commissioned v1.8.1 release.")
    if not isinstance(meta["clientSemver"], str) or not meta["clientSemver"].startswith(PINNED_ANVIL_SEMVER + "+"):
        raise RestorationObservationError("Anvil client semver is unsupported.")
    if meta["forkedNetwork"] is not None or any(fork_config[key] is not None for key in _FORK_CONFIG_KEYS):
        raise RestorationObservationError("A fork or remote RPC is active in the commissioned laboratory.")
    if meta["chainId"] != expected_chain_id or environment["chainId"] != expected_chain_id:
        raise RestorationObservationError("Anvil chain identity drifted.")
    if node["network"] != "ethereum" or node["transactionOrder"] not in {"fees", "fifo"}:
        raise RestorationObservationError("Anvil execution profile is unsupported.")
    if type(automine) is not bool or (interval_mining is not None and (isinstance(interval_mining, bool) or not isinstance(interval_mining, int) or interval_mining < 0)):
        raise RestorationObservationError("Anvil mining configuration could not be observed.")
    if not isinstance(meta["snapshots"], Mapping):
        raise RestorationObservationError("Anvil snapshot lifecycle metadata is malformed.")
    if meta["latestBlockHash"] != canonical_head["hash"] or meta["latestBlockNumber"] != canonical_head["number"]:
        raise RestorationObservationError("Anvil metadata disagrees with the canonical head.")
    if _quantity(node["currentBlockNumber"], "node.currentBlockNumber") != canonical_head["number"] or node["currentBlockHash"] != canonical_head["hash"]:
        raise RestorationObservationError("Anvil node info disagrees with the canonical head.")

    provider_identity = {
        "backend": "EVM_ANVIL",
        "client_version": meta["clientVersion"],
        "client_semver": meta["clientSemver"],
        "client_commit": meta["clientCommitSha"],
        "release_tag": PINNED_ANVIL_RELEASE_TAG,
        "release_provenance": PINNED_ANVIL_RELEASE_PROVENANCE,
        "chain_id": expected_chain_id,
        "network": node["network"],
        "hard_fork": node["hardFork"],
        "forked": False,
    }
    provider_environment = {
        "base_fee": _quantity(environment["baseFee"], "node.environment.baseFee"),
        "gas_limit": _quantity(environment["gasLimit"], "node.environment.gasLimit"),
        "gas_price": _quantity(environment["gasPrice"], "node.environment.gasPrice"),
        "transaction_order": node["transactionOrder"],
        "automine": automine,
        "interval_mining_seconds": interval_mining,
        "coinbase": canonical_head["miner"],
        "canonical_time": canonical_head["timestamp"],
    }
    projection = {
        "fingerprint_schema": ANVIL_EXECUTION_STATE_SCHEMA,
        "provider_identity": provider_identity,
        "accounts": list(accounts),
        "canonical_head": dict(canonical_head),
        "execution_environment": provider_environment,
    }
    return SemanticObservation(
        projection=projection,
        semantic_state_sha256=canonical_hash(projection),
        canonical_head=dict(canonical_head),
        txpool={},
        provider_identity=provider_identity,
        provider_environment={
            **provider_environment,
            "raw_node_current_block_timestamp": _quantity(node["currentBlockTimestamp"], "node.currentBlockTimestamp"),
            "snapshot_count": len(meta["snapshots"]),
        },
    )


def capture_semantic_observation(rpc: _RPC, raw: RawDumpCapture, *, expected_chain_id: int) -> SemanticObservation:
    head = canonical_head_from_rpc(rpc)
    observation = semantic_state_from_components(
        raw.decoded,
        metadata=rpc("anvil_metadata", []),
        node_info=rpc("anvil_nodeInfo", []),
        canonical_head=head,
        automine=rpc("anvil_getAutomine", []),
        interval_mining=rpc("anvil_getIntervalMining", []),
        expected_chain_id=expected_chain_id,
    )
    return SemanticObservation(
        projection=observation.projection,
        semantic_state_sha256=observation.semantic_state_sha256,
        canonical_head=observation.canonical_head,
        txpool=_txpool_from_rpc(rpc),
        provider_identity=observation.provider_identity,
        provider_environment=observation.provider_environment,
    )


def witness_specs(mutations: Sequence[CounterfactualMutation]) -> tuple[MutationWitnessSpec, ...]:
    values: list[MutationWitnessSpec] = []
    for mutation in mutations:
        probe = _COMMISSIONED_MUTATION_PROBES.get(mutation.verb)
        if probe is None:
            raise ScenarioValidationError(f"Anvil mutation has no commissioned restoration witness: {mutation.verb}.")
        params = mutation.parameters
        if mutation.verb in {"set_native_balance", "set_nonce", "set_contract_code"}:
            target = {"address": validate_address(params.get("address"))}
        elif mutation.verb == "set_storage_slot":
            target = {"address": validate_address(params.get("address")), "slot": params.get("slot")}
        else:
            target = {"canonical_chain_head": "latest"}
        values.append(MutationWitnessSpec(mutation.verb, target, probe))
    return tuple(values)


def read_witness(rpc: _RPC, spec: MutationWitnessSpec) -> object:
    target = spec.target
    if spec.probe == "eth_getBalance":
        return _quantity(rpc("eth_getBalance", [target["address"], "latest"]), "witness.balance")
    if spec.probe == "eth_getTransactionCount":
        return _quantity(rpc("eth_getTransactionCount", [target["address"], "latest"]), "witness.nonce")
    if spec.probe == "eth_getCode":
        return _hex_bytes(rpc("eth_getCode", [target["address"], "latest"]), "witness.code")
    if spec.probe == "eth_getStorageAt":
        return _hex_bytes(rpc("eth_getStorageAt", [target["address"], target["slot"], "latest"]), "witness.storage", exact_bytes=32)
    if spec.probe == "canonical_head":
        return canonical_head_from_rpc(rpc)
    raise RestorationObservationError("Mutation witness probe is unsupported.")


def _pointer(parent: str, key: object) -> str:
    escaped = str(key).replace("~", "~0").replace("/", "~1")
    return f"{parent}/{escaped}"


def _bounded(value: object) -> object:
    if value is _MISSING:
        return {"missing": True}
    serialized = canonical_json(value)
    if len(serialized.encode("utf-8")) <= 512:
        return value
    return {"sha256": hashlib.sha256(serialized.encode("utf-8")).hexdigest(), "canonical_json_bytes": len(serialized.encode("utf-8"))}


def _walk_differences(before: object, after: object, path: str = "") -> list[tuple[str, object, object]]:
    values: list[tuple[str, object, object]] = []
    if type(before) is not type(after):
        return [(path or "/", before, after)]
    if isinstance(before, Mapping):
        for key in sorted(set(before) | set(after)):
            child = _pointer(path, key)
            if key not in before:
                values.append((child, _MISSING, after[key]))
            elif key not in after:
                values.append((child, before[key], _MISSING))
            else:
                values.extend(_walk_differences(before[key], after[key], child))
    elif isinstance(before, list):
        for index in range(max(len(before), len(after))):
            child = _pointer(path, index)
            left = before[index] if index < len(before) else _MISSING
            right = after[index] if index < len(after) else _MISSING
            if left is _MISSING or right is _MISSING:
                values.append((child, left, right))
            else:
                values.extend(_walk_differences(left, right, child))
    elif before != after:
        values.append((path or "/", before, after))
    return values


def classify_structural_differences(
    before: RawDumpCapture,
    after: RawDumpCapture,
    *,
    before_semantic: SemanticObservation | None,
    after_semantic: SemanticObservation | None,
) -> tuple[StructuralDifference, ...]:
    values: list[StructuralDifference] = []
    for path, left, right in _walk_differences(before.decoded, after.decoded):
        classification = DifferenceClassification.UNKNOWN
        rule = "ANVIL_V1_8_1_UNKNOWN_V1"
        participates = False
        justification = "No exact pinned-schema classification rule matched this structural difference."
        if (
            path == "/block/timestamp"
            and before_semantic is not None
            and after_semantic is not None
            and before_semantic.semantic_state_sha256 == after_semantic.semantic_state_sha256
            and before_semantic.canonical_head == after_semantic.canonical_head
            and before.decoded.get("best_block_number") == after.decoded.get("best_block_number") == 0
            and isinstance(before.decoded.get("block"), Mapping)
            and isinstance(after.decoded.get("block"), Mapping)
            and before.decoded["block"].get("number") == after.decoded["block"].get("number") == "0x0"
            and left == "0x1"
            and right == hex(int(before_semantic.canonical_head["timestamp"]))
            and before_semantic.provider_identity.get("client_commit") == PINNED_ANVIL_COMMIT
            and after_semantic.provider_identity.get("client_commit") == PINNED_ANVIL_COMMIT
        ):
            classification = DifferenceClassification.PROVIDER_SERIALIZATION
            rule = "ANVIL_V1_8_1_GENESIS_BLOCK_ENV_REANCHOR_V1"
            justification = (
                "Pinned Anvil v1.8.1 serializes the initial BlockEnv timestamp sentinel as 1; "
                "evm_revert re-anchors BlockEnv to the unchanged canonical genesis header timestamp."
            )
        elif path.startswith("/accounts/"):
            classification = DifferenceClassification.EXECUTION_STATE
            rule = "ANVIL_V1_8_1_ACCOUNT_STATE_V1"
            participates = True
            justification = "Account membership, nonce, balance, code, and storage are semantic execution state."
        elif path == "/block" or path.startswith("/block/") or path == "/blocks" or path.startswith("/blocks/") or path == "/best_block_number":
            classification = DifferenceClassification.CANONICAL_CHAIN_STATE
            rule = "ANVIL_V1_8_1_CHAIN_STATE_V1"
            participates = True
            justification = "Serialized block environment or canonical block history changed outside the one exact provider rule."
        elif path == "/transactions" or path.startswith("/transactions/"):
            classification = DifferenceClassification.TRANSACTION_POOL_STATE
            rule = "ANVIL_V1_8_1_TRANSACTION_STATE_V1"
            participates = True
            justification = "Serialized transaction state changed."
        values.append(StructuralDifference(
            json_pointer=path,
            before_type="MISSING" if left is _MISSING else type(left).__name__,
            after_type="MISSING" if right is _MISSING else type(right).__name__,
            before_value=_bounded(left),
            after_value=_bounded(right),
            classification=classification,
            classification_rule_id=rule,
            participates_in_semantic_fingerprint=participates,
            justification=justification,
        ))
    return tuple(values)


def restoration_verdict(
    *,
    revert_succeeded: bool,
    before_semantic: SemanticObservation | None,
    after_semantic: SemanticObservation | None,
    witnesses: Sequence[MutationWitnessRecord],
    differences: Sequence[StructuralDifference],
    observation_error: str | None = None,
) -> tuple[bool, str]:
    if not revert_succeeded:
        return False, "REVERT_RPC_REJECTED_PROCESS_QUARANTINED"
    unknown = any(item.classification is DifferenceClassification.UNKNOWN for item in differences)
    if unknown:
        return False, "UNKNOWN_DUMP_DIFFERENCE_PROCESS_QUARANTINED"
    if observation_error is not None or before_semantic is None or after_semantic is None:
        if observation_error == "UNSUPPORTED_ANVIL_DUMP_SCHEMA":
            return False, "UNSUPPORTED_ANVIL_DUMP_SCHEMA_PROCESS_QUARANTINED"
        return False, "RESTORATION_OBSERVATION_FAILED_PROCESS_QUARANTINED"
    if any(not item.restored_exactly for item in witnesses):
        return False, "MUTATION_WITNESS_DRIFT_PROCESS_QUARANTINED"
    if before_semantic.txpool != after_semantic.txpool or any(after_semantic.txpool.get(key) != 0 for key in ("pending", "queued")):
        return False, "TXPOOL_DRIFT_PROCESS_QUARANTINED"
    if before_semantic.canonical_head != after_semantic.canonical_head:
        return False, "SEMANTIC_STATE_DRIFT_PROCESS_QUARANTINED"
    if before_semantic.semantic_state_sha256 != after_semantic.semantic_state_sha256:
        return False, "SEMANTIC_STATE_DRIFT_PROCESS_QUARANTINED"
    if any(item.classification in {
        DifferenceClassification.EXECUTION_STATE,
        DifferenceClassification.CANONICAL_CHAIN_STATE,
        DifferenceClassification.TRANSACTION_POOL_STATE,
        DifferenceClassification.DECLARED_MUTATION_WITNESS,
    } for item in differences):
        return False, "SEMANTIC_STATE_DRIFT_PROCESS_QUARANTINED"
    raw_equal = len(differences) == 0
    return True, "RESTORED_EXACTLY" if raw_equal else "RESTORED_SEMANTICALLY_RAW_DUMP_DIFFERED"


def differences_payload(differences: Sequence[StructuralDifference]) -> dict[str, object]:
    payload = {
        "schema": "anvil-raw-dump-structural-diff-v1",
        "classification_schema": "ANVIL_V1_8_1_EXACT_CLASSIFICATION_V1",
        "differences": [item.payload() for item in differences],
        "classified_difference_count": sum(item.classification is not DifferenceClassification.UNKNOWN for item in differences),
        "unknown_difference_count": sum(item.classification is DifferenceClassification.UNKNOWN for item in differences),
    }
    payload["structural_diff_sha256"] = canonical_hash(payload)
    return payload


def _json_value(value: object) -> object:
    return {"missing": True} if value is _MISSING else value
