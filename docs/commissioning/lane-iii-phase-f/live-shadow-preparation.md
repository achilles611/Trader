# Lane III closed-market live-shadow preparation

Status: **PARTIALLY READY** — the transport, safety, audit, and shadow wiring
are ready for an honest observation run, but a frozen scientific-input blocker
prevents valid directional L3-D output. This document and the automated checks
contain no live-market claim. The offline replay below is `SYNTHETIC / REPLAY
— NOT LIVE COMMISSIONING`.

## Exact runtime path

The existing downstream architecture is retained; no alternate scientific
pipeline was added:

```text
NinjaTrader AddOn / indicator
  -> LoopbackNinjaTraderBridge admission + session ledger
  -> NinjaTraderCommissioningHarness notification
  -> NinjaTraderListenerWorker (one FastAPI-lifespan owner)
  -> LaneIIIShadowRuntime (one locked consumer)
  -> NinjaTraderMarketDataAdapter
  -> frozen MarketDataPipeline (L3-B)
  -> frozen HypothesisEngine (L3-C)
  -> frozen TraderV0 (L3-D)
  -> bounded in-memory shadow audit record only
```

`LaneIIIShadowRuntime` is deliberately in `src/l3f_provider`, outside the
frozen `src/lane_iii` package. It calls the existing public L3-B, L3-C, and
L3-D interfaces. There is no Lane II, broker, order, simulated-execution, or
provider-execution import reachable from that module.

The normal Control Center lifespan creates one `LaneIIIShadowRuntime`, attaches
its four one-way sinks before starting the one existing
`NinjaTraderListenerWorker`, and exposes status at:

```text
GET /api/lane-iii/shadow
GET /api/lane-iii/shadow/audit
```

The worker remains the sole `127.0.0.1:48135` listener owner. There is no
extra PowerShell process, listener, console, or startup command.

## Mode, authority, and audit

Every status and audit record carries `mode: LANE_III_SHADOW`. Its fixed
authority report is:

| Boundary | Authority |
| --- | --- |
| Observation | `OBSERVE_ONLY` |
| Interpretation | `SHADOW_ONLY` |
| Decision | `SHADOW_ONLY` |
| Execution | `DENIED` |
| Live capital | `DENIED` |

The runtime retains bounded structured counters for raw accepted/rejected
observations, quote/trade/depth events, normalized admissions, downstream
rejections, stale/malformed/duplicate events, interpretation calls/failures,
decision evaluations/emissions/suppressions, transport resets, and execution
denials. `execution_attempts` is an invariant constant of `0`.

An emitted hypothetical decision audit record includes its observation and
session identity, instrument, canonical event ID, market-state hash, L3-C
snapshot hash, interpreter configuration hash, decision identity/artifact,
hypothetical action, reason code, shadow marker, and denied execution state.
Suppressed decisions record a reason instead. Audit retention is bounded to
512 in-memory records; no new scientific provenance or persistence store was
introduced.

`ShadowExecutionGuard.deny(...)` is the only execution-boundary object exposed
by the runtime. It raises before an execution interface is constructible,
records an `EXECUTION_HARD_DENIAL`, and leaves `execution_attempts == 0`.

## Freshness, reconnect, and contract behavior

The bridge uses the already frozen L3-C time values exactly: quote age 10
seconds, trade age 30 seconds, and book age 15 seconds. The runtime uses those
values only as admission safety bounds; it does not alter a scientific
threshold. A missing authoritative provider/exchange timestamp, old event,
backward event time, or stream gap greater than the relevant frozen value is
suppressed. A disconnected bridge or new market session discards the complete
in-memory L3-B/L3-C/L3-D shadow state before new observations can be consumed.
Partial depth snapshots are also suppressed rather than treated as healthy.

The NinjaTrader adapter now carries a supplied exchange timestamp into the
frozen L3-B three-clock `EventTimestamps` header. This fixes a concrete
provenance defect: the bridge previously validated `exchange_timestamp` but
discarded it before canonical L3-B ordering. Provider timestamp remains the
explicit fallback. No timing was fabricated.

The only admitted contract configuration is the observed native `MNQ SEP26`
binding with the provider contract ID `MNQ SEPT26`, canonicalized to `MNQU6`,
`2026-09`, `CME`, and tick size `0.25`. A wrong/unknown contract, malformed
instrument record, or new expiry is visible as `CONTRACT_NOT_FOUND`; there is
no automatic rollover or symbol substitution.

## Closed-market checks

`tests/test_lane_iii_live_shadow.py` is explicitly synthetic and covers:

| Condition | Expected result |
| --- | --- |
| Empty, quote-only, trade-only, depth-only | No shadow evaluation/decision from incomplete state |
| Duplicate, malformed, partial depth, wrong/unknown contract | Counted and suppressed; no market-state admission |
| Old, backward, missing-timestamp, and large-gap events | Fail closed with a reason code |
| Disconnect/reconnect and session transition | Prior shadow state discarded; no extra consumer |
| Downstream exception | State discarded, reason audited, no authority change |
| Forced execution handoff | Hard denial; `execution_attempts = 0` |
| Repeated identical fixture input | Identical decision/state hashes (`REPLAY — NOT LIVE COMMISSIONING`) |
| Normal FastAPI lifespan | One listener routes an admitted frame to the one shadow consumer |

Existing F3 receiver tests additionally cover listener ownership, later client
connect, disconnect/reconnect, UI/websocket churn, restart, and port
collision. The L3-B through L3-D suites verify the frozen deterministic
normalization, interpretation, and decision interfaces.

No locally retained raw authentic MNQ observation frames were found in the
workspace. Therefore an authentic offline replay was not fabricated; the
deterministic replay test uses only its clearly labeled synthetic fixture.

## Directional-shadow blocker

The current read-only NinjaScript bridge sends every `TRADE` record with
`aggressor_side: "UNKNOWN"`. Frozen L3-C therefore cannot derive its
`ORDER_FLOW` evidence: its flow window expressly refuses any unknown side.
Separately, NinjaScript's aggregated `DEPTH` frames include a trigger
`operation` and `side`, but the frozen NinjaTrader adapter emits them only as
`BookSnapshotEvent`s. Frozen L3-C derives `RESTING_LIQUIDITY` evidence only
from applied `BookDeltaEvent` changes, not snapshots.

Frozen Trader V0 requires `STRUCTURAL_CONTEXT`, `ORDER_FLOW`, and
`RESTING_LIQUIDITY` for an entry. Consequently, with the current bridge it can
correctly audit `NO_TRADE` and suppression results but cannot legitimately
emit an evidence-complete hypothetical `LONG` or `SHORT` from authentic flow.
This pass does not fill that gap with inferred aggressor sides, snapshot-delta
guesses, or a relaxed Trader V0 policy.

The remaining technical/scientific requirement before a directional live
shadow commissioning can be called ready is an explicitly reviewed provider
provenance contract for trade aggressor classification and for safe depth-delta
normalization (or a separately reviewed change to the frozen scientific
requirements). That decision is out of scope for this plumbing pass.

## Market-open commissioning procedure

1. Start NinjaTrader normally with the existing read-only AddOn/indicator.
2. Start Beelzebub normally with `python main.py copy-control-center`.
3. Confirm exactly one `127.0.0.1:48135` listener through the Control Center
   health response.
4. Confirm `/api/lane-iii/shadow` reports `LANE_III_SHADOW`, native `MNQ SEP26`,
   and authentic counters advancing.
5. Inspect `/api/lane-iii/shadow/audit` for admissions, suppressions, and
   hypothetical decisions. Treat every record as shadow-only.
6. Confirm `execution_attempts` remains `0`; a nonzero value is a stop and
   investigation condition.
7. Capture the resulting authentic evidence, stop cleanly, and review it.

The remaining requirements are (1) the reviewed directional-input contract
above and (2) authentic live MNQ SEP26 flow at market open, followed by
collection and review of the resulting shadow counter/audit evidence. No
execution, order change, flatten action, or capital interaction is authorized
during that run.
