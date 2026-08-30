"""Private loopback JSON-RPC transport used only by ``anvil_backend``."""

from __future__ import annotations

import json
from itertools import count
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from .contracts import ScenarioValidationError


class RpcTransportError(RuntimeError):
    """A local Anvil RPC request failed without retaining provider payloads."""


def assert_loopback_endpoint(endpoint: object) -> str:
    if not isinstance(endpoint, str):
        raise ScenarioValidationError("Anvil endpoint must be text.")
    parsed = urlparse(endpoint)
    if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.username or parsed.password:
        raise ScenarioValidationError("Counterfactual mutation endpoint must be exact loopback HTTP.")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment or parsed.port is None:
        raise ScenarioValidationError("Counterfactual endpoint must be a bare loopback host and port.")
    return endpoint


class _LoopbackRpc:
    """Not a scenario API: raw RPC is private to the Anvil process adapter."""

    def __init__(self, endpoint: str, *, timeout_seconds: float = 5.0) -> None:
        self._endpoint = assert_loopback_endpoint(endpoint)
        self._timeout_seconds = timeout_seconds
        self._identifiers = count(1)

    def call(self, method: str, params: list[object]) -> object:
        request_body = json.dumps({
            "jsonrpc": "2.0", "id": next(self._identifiers), "method": method, "params": params,
        }, separators=(",", ":")).encode("utf-8")
        request = Request(self._endpoint, data=request_body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:  # nosec B310: endpoint is exact loopback
                body = json.loads(response.read().decode("utf-8"))
        except Exception as exc:  # diagnostic messages can contain provider data; never persist them
            raise RpcTransportError("local Anvil RPC unavailable") from exc
        if not isinstance(body, dict) or "error" in body or "result" not in body:
            raise RpcTransportError("local Anvil RPC returned a rejected response")
        return body["result"]
