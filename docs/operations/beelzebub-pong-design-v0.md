# Beelzebub Pong design v0 — execution disabled

Status: **DESIGN REVIEW ONLY — NOT REGISTERED — NO EXECUTION TRANSPORT**

`BEELZEBUB_PONG_DESIGN_V0` is a proposed profile distinct from Perpetual. The
only implementation in this revision is a pure deterministic state machine in
`src/l3g_paper/pong.py` plus action-sequence tests. It has no account binding,
order sender, simulated fill side effects, persistence writer, runtime route,
profile-selector entry, startup registration, or commissioning path.

These controls are invariants, not TODOs:

- `PONG_EXECUTION_ENABLED = False`
- `PONG_PROFILE_REGISTRATION = NOT_REGISTERED`
- live capital remains denied
- no Pong process or runtime may be started from this design
- execution work requires a separate review after the Perpetual comparison and
  explicit approval of the beat and daily-bias contracts

## Pinned source signal

Pong observes logical direction switches from the existing Perpetual signal; it
does not share or imitate Perpetual's position manager. The design is pinned to:

- source profile: `BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2`
- source policy hash:
  `daf3cc6daacdb32d5629fd6cbd97ef2246172426a00392f59f1246c567658fe7`
- instrument: `MNQ SEP26`

A source profile or policy mismatch latches `SAFETY_BLOCKED`. A future event
contract must carry a unique switch ID, monotonic sequence, prior switch ID,
previous and new directions, causal occurrence time, source identity, and a
continuity result. Repeated same-direction observations, evaluation ticks,
fills, reconnects, recovery, and startup seed observations are not switches.

## Working beat interpretation — review required

For this design, one beat is the complete directional leg between two
consecutive, distinct Perpetual switches. If Pong exits at switch B, the B→C
leg is the required waiting beat; at switch C it may enter when the new
direction aligns with the then-current daily bias and every admission gate
passes. It does **not** automatically skip C and wait until a later switch.

Stable LONG-bias example:

| Event | Perpetual direction | Pong action |
|---|---|---|
| A | SHORT → LONG | Candidate LONG entry; revalidate before admission |
| B | LONG → SHORT | Exit LONG; never reverse directly |
| B→C | remains SHORT | Stay flat for the complete leg |
| C | SHORT → LONG | Candidate LONG entry if still aligned and admissible |
| D | LONG → SHORT | Exit LONG and repeat |

SHORT bias is the exact mirror. A non-aligned switch after the waiting leg is
observed and consumed but creates no entry. Pong can therefore remain flat for
any number of additional beats; there is no forced-entry timeout.

## Daily-bias dependency — intentionally unconfigured

Repository review found no canonical, causally available daily-bias contract
that can safely authorize Pong. The design therefore accepts an injected
`DailyBiasSnapshot` for tests and denies every real entry by default with
`DAILY_BIAS_UNCONFIGURED`. No price-versus-open, VWAP, moving-average, daily
close, or hindsight proxy has been selected.

Before any order-capable work, the owner must approve a provider contract with:

- instrument and allowed direction states: LONG, SHORT, NEUTRAL, UNKNOWN
- input lineage and source identity
- trading-day anchor and timezone
- calculation rule and update/freeze schedule
- `as_of`, causally available `available_at`, and `expires_at` timestamps
- invalidation and correction rules
- durable provenance, restart/replay behavior, and continuity semantics

Unconfigured, unknown, neutral, incomplete, not-yet-available, stale,
instrument-mismatched, or direction-mismatched bias denies new entries. A bias
change by itself never exits or reverses a position and never disables a
protective or safety exit.

## State and transition contract

The pure state machine tracks shadow direction, unique switch identity and
sequence, last consumed switch, actual position, pending action, confirmed-flat
switch sequence, waiting-leg completion, bias provenance, admission state, fill
state, and retained safety reason.

| State | Genuine switch behavior |
|---|---|
| `FLAT_WAIT_ALIGNMENT` | If bias and gates align, create a non-executable entry candidate; otherwise remain flat |
| `ENTRY_PENDING_LONG/SHORT` | A newer switch cancels the candidate; it never queues a replacement |
| `LONG/SHORT` | The next opposite switch creates an exit request, never a reverse entry |
| `EXIT_PENDING` | Continue tracking the shadow direction but queue no entry |
| `FLAT_WAIT_BEAT` | The next valid switch completes the B→C waiting leg, then may be evaluated |
| `SAFETY_BLOCKED` | Consume valid source observations but retain lockout and create no entry |

Startup can seed the observed direction but cannot fabricate a switch or beat.
Reporting boundaries do not reset any position, waiting, provenance, or safety
state. A sequence/identity conflict, missing continuity, source mismatch, or
shadow-direction mismatch latches safety state; missing switches are never
invented.

## Admission, fills, reconciliation, and races

An aligned switch produces only a deterministic abstract candidate. A future
adapter would have to call the distinct admission transition immediately before
submission; it rechecks source freshness, current bias/provenance, and all
entry gates. Failure cancels the candidate and leaves Pong flat. This design
does not perform submission.

An entry becomes a position only after its exact admitted action receives the
expected native one-contract fill. A late or stale entry fill is treated as
real exposure and produces a safety-exit request. An exit is complete only
after the exact native fill and signed reconciliation prove zero position and
zero owned working orders. A switch while exit is unresolved updates the
shadow sequence but grants no queued entry. Duplicate and out-of-order events
produce no action or beat.

A safety exit retains its lockout after native fill and signed-flat/order
reconciliation. It cannot re-enter from a cached direction. Recovery semantics
and authorization to clear any future Pong lockout are outside this design.

## Test evidence and exclusions

`tests/test_l3g_pong.py` covers:

- mirrored LONG and SHORT A/B/C examples
- one or more extra beats caused by bias mismatch
- unconfigured, unknown, and stale bias
- seed-only startup and same-direction repeats
- entry-pending cancellation and late-fill risk reduction
- entry-admission freshness, bias, and risk rechecks
- switches during unresolved exits
- native exit and safety-exit fill plus signed-flat/order reconciliation
- duplicate, invalid, out-of-order, and missing-continuity events
- bias changes and reporting-boundary persistence
- retained safety lockout

There is no Pong P&L counterfactual. Daily-bias history, Pong-specific
admissibility, prices, stops, and costs are not available, so selecting a
profitable subset of Perpetual fills would be non-causal and misleading.

## Decisions required before further work

1. Confirm or change the B→C working definition of one waiting beat.
2. Approve the complete causal daily-bias contract and provider.
3. Separately review persistence, signed event provenance, risk/protection,
   restart, reconciliation, ownership, UI visibility, and commissioning design.
4. Explicitly authorize any order-capable implementation and a later Sim101
   commissioning phase. This document provides no such authority.
