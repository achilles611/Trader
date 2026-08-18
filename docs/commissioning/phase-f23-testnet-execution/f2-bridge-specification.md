# Phase F.2 Lane II to Phase D Bridge Specification

Status: commissioned for deterministic simulation.

## Boundary

`LaneIIPhaseDBridge` is the sole module that imports both the frozen Lane II
contracts and Phase D. Trader V0 does not import Phase D or an execution
adapter. The bridge persists an immutable `ExecutionIntent`; only the Phase D
engine may validate, size within its configured ceiling, submit, cancel, or
reconcile that intent.

The commissioned simulator scope is:

```text
execution_domain = LANE_II_SIMULATOR
execution_account_id = explicit, non-default identity
```

It cannot collide with `SIMULATOR:default`, paper compatibility, or the later
Hyperliquid testnet scope.

## Exact entry admission

The bridge accepts only the exact `TradeIntentRequest` type and verifies:

- the frozen F.0 manifest hash;
- the frozen F.1 manifest hash and F.0 anchor;
- the sole F.1 strategy registration;
- Trader V0 strategy ID, version, immutable identity, and artifact hash;
- risk- and exit-policy references;
- reconstructed trade-intent payload and deterministic intent ID;
- sorted, unique input provenance hashes;
- the reconstructed F.1 authority-decision hash;
- positive bounded notional, strict symbol syntax, explicit direction, and
  current expiry.

The immutable Phase D provenance records `source = LANE_II`, the F.0/F.1
hashes, strategy artifact identity/hash, F.1 trade-intent ID and integrity
hash, authority-decision hash, input provenance, policy references, sizing
evidence hash, and the explicit fact that Lane II execution authority is
false.

## Sizing

F.1 notional is a ceiling. The bridge applies the smaller of the F.1 request
and Phase D's configured notional limit. Quantity is derived from fresh
execution-side price and instrument metadata with decimal round-down. Missing,
future, stale, malformed, below-minimum, or ceiling-violating evidence is
refused. Source-wallet leverage is absent from the bridge contract.

## Idempotency and recovery

The Phase D signal ID and intent ID are deterministic descendants of execution
domain, account identity, and F.1 trade-intent ID. SQLite claims the immutable
signal under `BEGIN IMMEDIATE`. Submission and client-order IDs are durably
prepared before the adapter call. A duplicate delivery or restart returns the
same intent; an ambiguous submission enters `SUBMISSION_UNKNOWN` and the engine
reconciles the existing client-order identity without submitting again.

## Exit compatibility

Frozen F.1 intentionally emits `TradeIntentRequest` only for LONG/SHORT entry
decisions and refuses EXIT conversion. F.2 does not alter that contract.
`admit_verified_flatten` instead replays the exact F.1 `TraderV0Decision` and
requires separate fresh authoritative Phase D position truth. Phase D truth,
not Lane II, selects direction and maximum quantity. The resulting intent is
`FLATTEN` and its `SubmissionRequest.reduce_only` is true. Zero, stale,
unverified, mismatched, or precision-invalid position evidence is refused.

## Authority invariant

Trader V0 may request. Phase D may execute. Trader V0 never executes.

