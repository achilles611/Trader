# Lane III Phase F (`l3f`) — Tradovate observation boundary

`l3f` adds a concrete, **read-only** Lucid Trading / Tradovate integration boundary. It is downstream of frozen `l3b → l3c → l3d` and may feed frozen `l3e` only as simulation input.

Status on 2026-08-20: implementation and deterministic adversarial tests complete; real-provider commissioning is **not performed**. No credentials, explicit provider account ID, API-app metadata, concrete active MNQ contract, or account entitlement were supplied. This is deliberately not a freeze.

The implementation is [tradovate_observation.py](../../../src/l3f_provider/tradovate_observation.py). It sits outside the frozen Lane III package and contains a named read-only REST transport, a read-only WebSocket subscription client, strict L3-B normalization, account/position/order observation, reconciliation, health tracking, secret redaction, and future-only compliance diagnostics. It contains no real order authority.

See [closure audit](closure-audit.md) for the current hard-gate result.
