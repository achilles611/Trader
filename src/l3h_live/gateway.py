"""Dispatch boundary.  The default concrete gateway can only refuse safely."""

from __future__ import annotations

from typing import Mapping, Protocol, runtime_checkable


class GatewayDispatchError(RuntimeError):
    pass


@runtime_checkable
class LiveGateway(Protocol):
    def dispatch(self, command: Mapping[str, object]) -> Mapping[str, object]: ...


class NoDispatchLiveGateway:
    """Safe production default until the isolated signed AddOn handshake exists."""

    def dispatch(self, command: Mapping[str, object]) -> Mapping[str, object]:
        del command
        raise GatewayDispatchError("LIVE_GATEWAY_NOT_CONFIGURED")
