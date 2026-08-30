"""Fresh-process Foundry Anvil backend for EVM counterfactual runs.

Every instance binds a new Anvil process to 127.0.0.1 on an ephemeral port.
The class exposes only named privileged capabilities; generic RPC is kept in
the private transport module.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .contracts import (
    CounterfactualAssertion,
    CounterfactualMutation,
    ScenarioValidationError,
    canonical_hash,
    validate_address,
    validate_hex,
    validate_uint,
)
from .rpc import RpcTransportError, _LoopbackRpc, assert_loopback_endpoint


PINNED_ANVIL_VERSION = "1.8.1"


class AnvilUnavailable(RuntimeError):
    """A real Anvil process is not available for an EVM laboratory run."""


class AnvilExperimentError(RuntimeError):
    """An allowlisted EVM laboratory capability failed."""


def installed_anvil() -> str | None:
    return shutil.which("anvil")


def anvil_version(binary: str | None = None) -> str | None:
    path = binary or installed_anvil()
    if path is None:
        return None
    try:
        completed = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return None
    output = (completed.stdout or completed.stderr).strip().splitlines()
    return output[0] if completed.returncode == 0 and output else None


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _valid_fork_source(value: object) -> str:
    if not isinstance(value, str):
        raise ScenarioValidationError("Fork source must be configured HTTPS text.")
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ScenarioValidationError("Fork source must be a credential-free HTTPS endpoint.")
    if parsed.hostname in {"127.0.0.1", "localhost", "::1"}:
        raise ScenarioValidationError("Fork source may not be a mutation endpoint.")
    return value


class AnvilBackend:
    """One disposable mutable chain; instances are never reused after a run."""

    def __init__(
        self,
        *,
        chain_id: int,
        fixed_fork_block: int | None = None,
        fork_source: str | None = None,
        binary: str | None = None,
        startup_timeout_seconds: float = 10.0,
    ) -> None:
        self.chain_id = validate_uint(chain_id, "chain_id", maximum=(2**63) - 1)
        if self.chain_id == 0:
            raise ScenarioValidationError("chain_id must be positive.")
        if fixed_fork_block is not None:
            self.fixed_fork_block = validate_uint(fixed_fork_block, "fixed_fork_block", maximum=(2**63) - 1)
            if self.fixed_fork_block == 0:
                raise ScenarioValidationError("fixed_fork_block must be positive.")
        else:
            self.fixed_fork_block = None
        if (fork_source is None) != (self.fixed_fork_block is None):
            raise ScenarioValidationError("Fork source and a pinned fork block must be supplied together.")
        self.fork_source = _valid_fork_source(fork_source) if fork_source is not None else None
        self.binary = binary or installed_anvil()
        self.startup_timeout_seconds = startup_timeout_seconds
        self._process: subprocess.Popen[bytes] | None = None
        self._rpc: _LoopbackRpc | None = None
        self._endpoint: str | None = None
        self._scenario_snapshots: dict[str, str] = {}
        self._dump_cache: dict[str, str] = {}
        self._impersonated: set[str] = set()
        self._discarded = False

    @classmethod
    def expected_initial_fingerprint(cls, chain_id: int, *, fixed_fork_block: int | None = None) -> str:
        """Predict the deterministic empty-chain fingerprint used by local scenarios."""
        return canonical_hash({"chain_id": chain_id, "block_number": 0, "fork_block": fixed_fork_block})

    @property
    def endpoint(self) -> str:
        if self._endpoint is None:
            raise AnvilExperimentError("Anvil process has not started.")
        return self._endpoint

    @property
    def running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    @property
    def toolchain_version(self) -> str:
        return anvil_version(self.binary) or "ANVIL_UNAVAILABLE"

    def start(self) -> None:
        if self._discarded:
            raise AnvilExperimentError("Discarded Anvil process cannot be reused.")
        if self.running:
            return
        if self.binary is None:
            raise AnvilUnavailable("Foundry Anvil is not installed.")
        version = anvil_version(self.binary)
        if version is None or PINNED_ANVIL_VERSION not in version:
            raise AnvilUnavailable("Foundry Anvil is not the pinned f4 v1.8.1 toolchain.")
        port = _free_loopback_port()
        arguments = [self.binary, "--host", "127.0.0.1", "--port", str(port), "--chain-id", str(self.chain_id), "--accounts", "2", "--silent"]
        if self.fork_source is not None:
            arguments.extend(["--fork-url", self.fork_source, "--fork-block-number", str(self.fixed_fork_block)])
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        try:
            self._process = subprocess.Popen(
                arguments, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                creationflags=creationflags,
            )
        except OSError as exc:
            raise AnvilUnavailable("Foundry Anvil could not be started.") from exc
        self._endpoint = assert_loopback_endpoint(f"http://127.0.0.1:{port}")
        self._rpc = _LoopbackRpc(self._endpoint)
        deadline = time.monotonic() + self.startup_timeout_seconds
        while time.monotonic() < deadline:
            if not self.running:
                self.kill()
                raise AnvilUnavailable("Foundry Anvil stopped during startup.")
            try:
                if self._chain_id() == self.chain_id:
                    return
            except RpcTransportError:
                time.sleep(0.05)
        self.kill()
        raise AnvilUnavailable("Foundry Anvil did not become ready before the hard timeout.")

    def _call(self, method: str, params: list[object]) -> object:
        if not self.running or self._rpc is None:
            raise AnvilExperimentError("No live isolated Anvil process exists.")
        try:
            return self._rpc.call(method, params)
        except RpcTransportError as exc:
            raise AnvilExperimentError("Anvil privileged operation failed.") from exc

    def _chain_id(self) -> int:
        value = self._call("eth_chainId", [])
        if not isinstance(value, str):
            raise AnvilExperimentError("Anvil chain identity response was invalid.")
        return int(value, 16)

    def _block_number(self) -> int:
        value = self._call("eth_blockNumber", [])
        if not isinstance(value, str):
            raise AnvilExperimentError("Anvil block response was invalid.")
        return int(value, 16)

    def snapshot(self) -> str:
        value = self._call("evm_snapshot", [])
        if not isinstance(value, str) or not value:
            raise AnvilExperimentError("Anvil did not return a snapshot identity.")
        return value

    def revert(self, snapshot: object) -> bool:
        if not isinstance(snapshot, str) or not snapshot:
            return False
        return self._call("evm_revert", [snapshot]) is True

    def fingerprint(self) -> str:
        """Hash full local state without persisting its potentially large dump."""
        dumped = self._call("anvil_dumpState", [])
        if not isinstance(dumped, str):
            raise AnvilExperimentError("Anvil state dump response was invalid.")
        return canonical_hash({"chain_id": self._chain_id(), "block_number": self._block_number(), "fork_block": self.fixed_fork_block, "state_hash": canonical_hash(dumped)})

    def local_fingerprint(self) -> str:
        """Small empty-chain fingerprint for reproducible no-fork scenarios."""
        return canonical_hash({"chain_id": self._chain_id(), "block_number": self._block_number(), "fork_block": self.fixed_fork_block})

    def apply(self, mutation: CounterfactualMutation) -> None:
        if type(mutation) is not CounterfactualMutation:
            raise AnvilExperimentError("Anvil requires an exact counterfactual mutation.")
        capability = getattr(self, f"_apply_{mutation.verb}", None)
        if capability is None:
            raise AnvilExperimentError("Unknown Anvil capability.")
        capability(mutation.parameters)

    @staticmethod
    def _params(parameters: Mapping[str, object], names: set[str], *, optional: set[str] = set()) -> None:
        unexpected = set(parameters) - names - optional
        missing = names - set(parameters)
        if unexpected or missing:
            raise ScenarioValidationError("Anvil capability parameters are incomplete or unrecognized.")

    def _apply_snapshot(self, parameters: Mapping[str, object]) -> None:
        self._params(parameters, {"snapshot_id"})
        snapshot_id = parameters["snapshot_id"]
        if not isinstance(snapshot_id, str) or not snapshot_id or len(snapshot_id) > 96:
            raise ScenarioValidationError("snapshot_id must be bounded text.")
        self._scenario_snapshots[snapshot_id] = self.snapshot()

    def _apply_revert(self, parameters: Mapping[str, object]) -> None:
        self._params(parameters, {"snapshot_id"})
        snapshot_id = parameters["snapshot_id"]
        if not isinstance(snapshot_id, str) or snapshot_id not in self._scenario_snapshots:
            raise ScenarioValidationError("Scenario snapshot identity is unknown.")
        if not self.revert(self._scenario_snapshots[snapshot_id]):
            raise AnvilExperimentError("Anvil scenario revert failed.")

    def _apply_set_native_balance(self, parameters: Mapping[str, object]) -> None:
        self._params(parameters, {"address", "balance"})
        address = validate_address(parameters["address"])
        balance = validate_uint(parameters["balance"], "balance")
        self._call("anvil_setBalance", [address, hex(balance)])

    def _apply_set_contract_code(self, parameters: Mapping[str, object]) -> None:
        self._params(parameters, {"address", "code"})
        self._call("anvil_setCode", [validate_address(parameters["address"]), validate_hex(parameters["code"], "code")])

    def _apply_set_storage_slot(self, parameters: Mapping[str, object]) -> None:
        self._params(parameters, {"address", "slot", "value"})
        self._call("anvil_setStorageAt", [
            validate_address(parameters["address"]), validate_hex(parameters["slot"], "slot", exact_bytes=32),
            validate_hex(parameters["value"], "value", exact_bytes=32),
        ])

    def _apply_impersonate_account(self, parameters: Mapping[str, object]) -> None:
        self._params(parameters, {"address"})
        address = validate_address(parameters["address"])
        self._call("anvil_impersonateAccount", [address])
        self._impersonated.add(address)

    def _apply_stop_impersonation(self, parameters: Mapping[str, object]) -> None:
        self._params(parameters, {"address"})
        address = validate_address(parameters["address"])
        self._call("anvil_stopImpersonatingAccount", [address])
        self._impersonated.discard(address)

    def _apply_advance_timestamp(self, parameters: Mapping[str, object]) -> None:
        self._params(parameters, {"timestamp"})
        timestamp = validate_uint(parameters["timestamp"], "timestamp", maximum=(2**63) - 1)
        self._call("evm_setNextBlockTimestamp", [timestamp])
        self._call("evm_mine", [])

    def _apply_mine_block(self, parameters: Mapping[str, object]) -> None:
        self._params(parameters, set())
        self._call("evm_mine", [])

    def _apply_mine_blocks(self, parameters: Mapping[str, object]) -> None:
        self._params(parameters, {"count"})
        count = validate_uint(parameters["count"], "count", maximum=1024)
        if count == 0:
            raise ScenarioValidationError("count must be positive.")
        self._call("anvil_mine", [hex(count)])

    def _apply_dump_state(self, parameters: Mapping[str, object]) -> None:
        self._params(parameters, {"state_id"})
        state_id = parameters["state_id"]
        if not isinstance(state_id, str) or not state_id or len(state_id) > 96:
            raise ScenarioValidationError("state_id must be bounded text.")
        dump = self._call("anvil_dumpState", [])
        if not isinstance(dump, str):
            raise AnvilExperimentError("Anvil state dump was invalid.")
        self._dump_cache[state_id] = dump

    def _apply_load_state(self, parameters: Mapping[str, object]) -> None:
        self._params(parameters, {"state_id"})
        state_id = parameters["state_id"]
        if not isinstance(state_id, str) or state_id not in self._dump_cache:
            raise ScenarioValidationError("Anvil state_id is unknown.")
        self._call("anvil_loadState", [self._dump_cache[state_id]])

    def _native_balance(self, address: object) -> int:
        value = self._call("eth_getBalance", [validate_address(address), "latest"])
        if not isinstance(value, str):
            raise AnvilExperimentError("Anvil balance response was invalid.")
        return int(value, 16)

    def _code(self, address: object) -> str:
        value = self._call("eth_getCode", [validate_address(address), "latest"])
        return validate_hex(value, "returned code")

    def _storage(self, address: object, slot: object) -> str:
        value = self._call("eth_getStorageAt", [validate_address(address), validate_hex(slot, "slot", exact_bytes=32), "latest"])
        return validate_hex(value, "returned storage", exact_bytes=32)

    def assert_state(self, assertion: CounterfactualAssertion) -> None:
        values = assertion.parameters
        if assertion.verb == "chain_id_equals" and self._chain_id() == values.get("chain_id"):
            return
        if assertion.verb == "block_number_at_least" and isinstance(values.get("block_number"), int) and self._block_number() >= values["block_number"]:
            return
        if assertion.verb == "native_balance_equals" and self._native_balance(values.get("address")) == validate_uint(values.get("balance"), "balance"):
            return
        if assertion.verb == "code_equals" and self._code(values.get("address")) == validate_hex(values.get("code"), "code"):
            return
        if assertion.verb == "storage_equals" and self._storage(values.get("address"), values.get("slot")) == validate_hex(values.get("value"), "value", exact_bytes=32):
            return
        raise AnvilExperimentError("Anvil assertion failed or is unsupported.")

    def close(self) -> None:
        self._scenario_snapshots.clear()
        self._dump_cache.clear()
        self._impersonated.clear()
        if self._process is None:
            return
        if self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=3)
        self._process = None
        self._rpc = None
        self._endpoint = None
        self._discarded = True

    def kill(self) -> None:
        if self._process is not None and self._process.poll() is None:
            self._process.kill()
            try:
                self._process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                pass
        self._process = None
        self._rpc = None
        self._endpoint = None
        self._scenario_snapshots.clear()
        self._dump_cache.clear()
        self._impersonated.clear()
        self._discarded = True
