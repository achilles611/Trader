# Lane III commissioning runbook

This is the operator path for the explicit one-contract `Sim101` / `MNQ SEP26`
mechanics commissioning lifecycle. It does not change the Lane III scientific
methodology, prove an edge, authorize Lucid, or permit live capital.

## Before the entry window

1. Start NinjaTrader and connect the existing market-data connection.
2. Open the live `MNQ SEP26` chart.
3. Attach `BeelzebubReadOnlyMarketObserver` to that chart.
4. Start BeezConsole through the official launcher and open **Lane III Paper**.
5. Confirm the runtime binding shown in **Ledger health** is the intended hot
   ledger/epoch and audit root. Do not rotate, prune, VACUUM, checkpoint, or
   adopt an epoch during commissioning.
6. Confirm AddOn source/build/protocol is `MATCH` and the observer becomes
   `ACTIVE`. `STALE` and `NOT_ACTIVE` are blockers. For `NOT_ACTIVE`, reopen the
   MNQ chart, attach the observer, and wait for authentic level-one and depth
   callbacks.
7. Confirm `Sim101` is `FLAT`, quantity is zero, owned orders are zero, the
   execution bridge is authenticated/reconciled, ownership is `NONE`, the
   runtime is `READY_DISARMED`, and live capital is `DENIED`.

## Warmup and rehearsal

The commissioning warmup is independent of strategy alpha. It starts cold on
every process/session/continuity epoch and becomes `WARMED` only after authentic
`STRUCTURAL_CONTEXT`, `ORDER_FLOW`, and `RESTING_LIQUIDITY` evidence has appeared
in the exact current session identity and generation. Natural evidence expiry
may return **Strategy evidence** to `INCOMPLETE`; it does not clear the
commissioning latch. Fresh quote, classified trade, and depth callbacks remain
mandatory at actual admission.

The NinjaTrader observer publishes a level-one quote only for a strict positive
spread (`bestBid < bestAsk`). A transient locked book is ignored at that
producer boundary and cannot refresh quote freshness. A malformed, locked, or
crossed quote that nevertheless reaches the Python boundary still clears
provisional evidence and the commissioning latch; that fail-closed reset was
not weakened.

Warmup `WARMED` and reset rows are output-only, version-1 readiness
attestations. They carry `authority_effect=NONE` and exact commissioning
readiness semantics, but are never replayed to recreate the process-local
latch. New exact attestations are authority observations under tail policy v3,
not passive market data and not authority mutations. Old unmarked or malformed
attestations remain unknown until a verifier PASS covers them.

Wait until BeezConsole shows the exact current session entry window as open and
all three warmup-family rows as `SEEN`. Then select **Run Read-Only Commissioning
Rehearsal**. Rehearsal invokes the production validation graph but cannot reserve
ownership, ARM, create an executable grant/intent, submit/cancel an order,
flatten, or touch Lucid.

Do not proceed unless the final rehearsal result is **READY FOR COMMISSIONING**
and every individual gate agrees. Resolve exact blocker codes rather than
repeating start attempts. Storage warnings, a stale/failed verifier, an
untrusted authority tail, observer inactivity, stale market inputs, or any
ambiguous broker fact are hard blockers.

The **Ledger health** panel exposes the verified anchor, captured tail tip and
row count, trust state, last mutation/observation/unknown sequence-domain-kind
tuples, and the latest blocking classification without exposing record
payloads. For a nonempty accepted tail, confirm that every mutation and unknown
watermark is at or before the verified anchor and that current independent
broker/runtime reconciliation is clean.

## Canonical commissioning start

Use only **Atomic Commissioning Start**. The UI generates and retains one
idempotency request ID across client timeout/retry. The server revalidates the
coherent broker/runtime snapshot and verified-anchor/passive-tail gate under the
runtime admission lock, reserves commissioning ownership, and arms a single-use
authorization. Under `NY_HIGH_CONFLUENCE_COMMISSIONING_V1`, the authorization
waits without submitting an order until the current `NEW_YORK_RTH` generation
produces a fresh policy decision with support at least `0.675`, dominance at
least `0.10`, all three required families, and no blocking contradiction. That
exact decision's sources and confluence summary are sealed into the separate
commissioning decision/intent/risk grant/command, and the authorization is
consumed only at transport admission. A duplicate request returns the existing
lifecycle and cannot create another entry command. The legacy split ARM/entry
routes are diagnostic-only and are intentionally absent from the operator UI.

After the owned entry fill, require the one-contract protective stop to become
accepted/working. A missing, rejected, cancelled, duplicate, wrong-account,
wrong-instrument, or wrong-quantity protective stop initiates the existing
emergency flatten path and locks out re-entry. Never override that behavior.

Use **Run Commissioning Exit** for the controlled owned exit. The lifecycle is
not terminal until NinjaTrader confirms `FLAT`, quantity zero, zero owned
orders, and a fresh clean reconciliation returns the runtime to
`READY_DISARMED` with ownership `NONE`.

The NY high-confluence commissioning profile permits at most one entry and a
maximum position age of 3,600 seconds. Strategy retention/exit fluctuations do
not close the commissioned position. The accepted protective stop, stale-data
emergency exit, operator commissioning exit, one-hour maximum age, and 15:58
America/New_York hard-flat deadline remain authoritative.

## Post-run closure

Run **Auto** ledger verification after the clean reconciliation. A closure is
`PASS` only when an actual Incremental PASS covers the immutable closure record,
the checkpoint matches, the account is flat/disarmed with no orders, entry and
exit fills pair exactly, MNQ point economics match realized P&L, no incident is
present, and Lucid mutation count is zero. Flat broker state alone is never a
commissioning PASS. BeezConsole reports the exact post-run blocker when any
condition is absent; the lifecycle remains `COMMISSIONING_INCOMPLETE`.

London V1 authentic commissioning is read-only: during `[08:00, 11:30)
Europe/London`, wait for the exact `LONDON / EUROPE` generation to warm, run
the read-only rehearsal, then run a fresh incremental verification. Do not use
ARM, Atomic Commissioning Start, or Start Paper Trading to establish London
session commissioning.

PowerShell and direct API calls are diagnostic fallback only. They are not part
of the normal operator workflow and must never be used to bypass a displayed
gate.

## Warmup reset boundaries

Authentic rewarming is required after session kind/ID/trade-date/profile/
generation change, `OFF_SESSION`, local or execution bridge disconnect/reconnect,
market-data disconnect/reconnect, observation-session change, sequence gap,
rejected or malformed observation, timestamp reversal, depth reset/recovery
boundary, contract mismatch, or process restart. Family evidence never carries
from Asia to London, London to New York RTH, New York RTH to NY After, NY After
to Asia, or across generations.
