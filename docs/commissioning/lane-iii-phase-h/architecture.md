# L3H architecture

```text
local signed capability -> authority gates -> write-ahead event store
                                           -> isolated gateway -> NinjaTrader Live AddOn
broker snapshots --------> reconciliation -> risk/protection -> projections/UI
```

`src/l3h_live` owns L3H contracts, gate derivation, canonical event evidence,
reconciliation, canary risk, signed gateway frames, and the fail-closed
runtime. It does not import `src/l3g_paper`; L3G remains an independently
sealed Sim101 capability.

The event store writes `COMMAND_SEALED` before the gateway may dispatch. A
network failure after seal creates `UNKNOWN` and quarantines the runtime. It
never retries an unresolved entry. A `FLAT` result requires complete, fresh
broker position and order snapshots with no foreign or unclassified activity.

The default gateway is `NoDispatchLiveGateway`; it always refuses. The only
concrete gateway is `AuthenticatedLoopbackGateway`: an explicit loopback-only
listener on port `48137`, with its own 256-bit local key, HMAC header,
canonical payload hash, protocol version, timestamp freshness, nonce replay
guard, request ID, and AddOn provenance handshake. A lost acknowledgement is
`UNKNOWN`, not a retry.

`BeelzebubLiveExecutionAddOn.cs` is a separate NinjaTrader artifact. It uses
the `BZ-L3H-` order namespace, `l3h.execution.local.key`, and starts disarmed
on every platform start/reconnect. Its native guard rejects absent or stale
authority, account/contract/hash mismatch, quantity other than one, non-flat
or foreign activity, missing protection mechanism, heartbeat loss, and kill
latch. The source is not installed-artifact evidence: current status is
blocked until visible NT8 compile, source/install/runtime provenance, and the
real installed Sim101 matrix are recorded.
