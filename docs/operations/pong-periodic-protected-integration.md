# Pong periodic protected integration

## Current state

**IMPLEMENTED, not `INTEGRATED_VALIDATED`, deployed, or paper armed.**

This integration starts from the sealed periodic experiment commit
`b6f361bde01ff0529225a0087426824481b24a5d` in the isolated worktree
`H:\PongPeriodic\20260918\integration`.  The integrated commit is recorded in
the final receipt below.  The dirty operating checkout remains untouched.  No
controller, bridge, listener, NinjaTrader process, or paper session was started
for this change.

`OfflinePeriodicPong` remains offline-only and still rejects a production
bridge/controller attachment.  Production now uses a separate bounded QPC
deadline helper through `PongProductionHost`.

## Owned production path

| Boundary | Owner | Behavior |
| --- | --- | --- |
| Native source receiver | `PongProductionHost.on_observation` | Bounded enqueue; every accepted record is still authenticated and applied by the host owner. |
| Authoritative input/state | `PongProductionHost._consume_observation` | Validates sequence/identity, retains book/trade/history state, then chooses the existing event path or a timer opportunity. |
| Flat periodic cadence | `PongPeriodicDeadline` | QPC absolute 10/20/25 ms grid; one current opportunity only; obsolete opportunities are counted, never queued. |
| Exposure/protection | `PongProductionHost._event_protection_reason_locked` | Pending entry, working order, partial fill, position, exit, unresolved execution, stale reconciliation, bridge absence, or native directive keeps the established event-driven path. |
| Strategy/risk/bridge | `PongProductionHost._apply_discernment_update` and `PongRuntimeBridge` | An entry intent marks protection mode before the entry admission/native-dispatch boundary.  The existing ordered state machine remains the only directive owner. |
| Durability | Existing collector and paper runtime | This change does not alter journal enqueue, flush/fsync acknowledgement, controller persistence, or ledger writes.  Their current counters remain separate from analytical completion. |

The production status payload at `GET /api/lane-iii/pong` now includes
`evaluation_schedule`: selected mode/interval, active path, protection reason,
QPC information, deadline reconciliation, source-receiver/admission counts, and
separate event-driven versus periodic evaluation counts.

## Explicit configuration

The maintained launcher copies these two process-scoped values unchanged; the
control center parses both or refuses the incomplete configuration.  They grant
no authority and do not override the existing Sim101, one-contract, risk,
freshness, controller, or native-identity gates.

```powershell
$env:BEELZEBUB_PONG_SCHEDULING_MODE = 'PERIODIC_FLAT_PROTECTED'
$env:BEELZEBUB_PONG_PERIODIC_INTERVAL_MS = '20'
```

The production default remains explicit event-driven operation:

```powershell
$env:BEELZEBUB_PONG_SCHEDULING_MODE = 'EVENT_DRIVEN'
$env:BEELZEBUB_PONG_PERIODIC_INTERVAL_MS = '0'
```

These assignments are configuration only, not a standalone launch command.
They are read by the existing control-center construction path, which rejects a
periodic selection outside `BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2` and rejects any
compute-worker configuration for the periodic path.

Do not alter either variable in an active runtime.  Event-driven rollback is
safe only after the existing controlled stop/reconciliation path has proved
flat position, zero owned and entry orders, no unresolved execution, and a
terminal controller/bridge state.  No production launch or stop command is
recorded here because neither was exercised in this integration pass.

## Timestamp repair

`src/l3g_paper/discernment.py` no longer compares normalized timestamps as
variable-width strings.  It canonicalizes explicit UTC-offset values, retains
up to nine fractional digits (including 100 ns), and compares fixed-width UTC
keys.  Equal numeric timestamp representations are equal; exact-second,
fractional, adjacent-second, offset, and expiry comparisons use chronological
order.  Source envelope bytes are not rewritten.

## Evidence and remaining gates

- Focused code verification includes scheduler absolute-grid/supersession,
  production flat-versus-protected routing, launcher pass-through, timestamp
  precision/offset/expiry, bridge, controller, input, and existing negative
  offline protective tests.
- Fresh corrected-reference fidelity evidence is
  `H:\PongPeriodic\20260918\integration-fidelity-v3-single20\result.json`.
  It contains 150,016 authenticated records, 8,812 exact shared cutoffs, and
  zero parity errors for the 20 ms phase-zero offline calculation comparison.
  Its SHA-256 is
  `3E303DD751C7A47D825DE96EFDEC65BB753649049B71E8D9D5BA0BEBDFD9FF80`.
- The two prior directories `integration-fidelity-v1` and
  `integration-fidelity-v2-single20` are preserved incomplete: the foreground
  tool boundary ended them before a receipt.  They are not evidence.

The import-time loaded-artifact identity is
`c9890b42e300cdbcbf9e7678d351ebffa6c2b7403a359994e0a6e4f16837d30b`.
The focused command from the handoff plus the new scheduler, timestamp, and
launcher checks completed **161 tests** successfully.  A final recoverable
patch and its SHA-256 accompany the sealed integration artifact.

Paper operation remains blocked until a compatible fresh current-contract
source, full protected-path under-load measurements (flat, pending/entry,
held-position, and transitions), durable-drain verification, and every current
freshness/reconciliation/controller/risk gate are established.  The historical
MNQ SEP26 corpus cannot be represented as current DEC26 authority.
