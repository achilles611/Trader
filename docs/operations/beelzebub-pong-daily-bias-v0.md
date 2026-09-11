# Beelzebub Pong daily-bias v0 — causal contract, execution disabled

Status: **SOFTWARE/DESIGN REVIEW ONLY — PRODUCTION INPUTS UNCONFIGURED — NO EXECUTION AUTHORITY**

This document specifies the standalone pure component in
`src/l3g_paper/daily_bias.py` and its read-only eligibility integration with the
execution-disabled Pong state machine. It does not register a profile, select a
runtime, read or write a trading ledger, bind an account, open a port, submit an
order, or authorize commissioning. `PONG_DAILY_BIAS_PRODUCTION_CONFIGURED` is
`False`; Pong remains `NOT_REGISTERED` with execution disabled and live capital
denied.

## Repository capability inventory

The inventory was performed before defining the v0 contract:

| Source | Reusable capability found | Production daily-bias status |
|---|---|---|
| Hourly trend | No canonical hourly or higher-timeframe trend calculation was found. | `UNCONFIGURED`; only the typed component interface exists. No moving average, price/open, VWAP, or close-based proxy was invented. |
| Hourly CRT | `docs/commissioning/lane-iii-phase-a/l3-a-specification.md` names future `CRT-derived range structure`, states CRT has no special authority, and explicitly says no CRT signal is implemented. | `UNCONFIGURED`; Joseph's exact range, sweep, re-entry, acceptance/rejection, direction, veto, and invalidation rules are still required. Generic candle logic is not substituted. |
| Order flow | `src/l3g_paper/policy.py` authentically classifies a trade only from provider-native aggressor side or a causally referenced matching bid/ask quote. Its existing decision evidence is an 8-trade, up-to-30-second rolling sell-flow calculation, not an hourly bias. | Classification and source hashes are reusable through an adapter. A standalone completed-hour aggregation policy is implemented below, but remains design-only and is not connected to the runtime. |
| Options | `src/lane_iii/market_data.py` defines `DerivativesContextEvent` with expiry, strike, right, open interest, volume, and explicit vintage. `src/lane_iii/hypothesis_engine.py` records only directional-neutral vintage evidence. Tests state no provider exists. | Conditional interface only. There is no configured expiry classifier, materiality rule, gamma/delta calculation, current positioning source, or directional policy. |

The existing Perpetual order-flow evidence is deliberately not reused as an
hourly direction: it is short-lived, asymmetric policy evidence for Perpetual's
own hypotheses. Pong's daily bias also never reads Perpetual's position or
desired direction.

## Contract and provenance

`HourlyBiasFrame` contains exactly one completed UTC hour for `MNQ SEP26`, one
result for each core component, one options overlay, the decision time,
continuity state, and the preceding result ID. `DailyBiasResult` records:

- `LONG`, `SHORT`, `NEUTRAL`, or `UNAVAILABLE` direction;
- confidence tier, exact four-way vote counts, reason codes, configured state,
  update reason, prior result ID, and deterministic result/frame hashes;
- boundary start/end, causal availability, earliest component/overlay expiry;
- all three component results and the complete options-overlay result.

Every component and overlay has a `BiasInputProvenance` envelope containing
source, calculation ID/version/hash, input window, latest event time,
observation time, available-at time, validity, and aligned input IDs/hashes.
Construction normalizes explicit offsets to UTC and rejects timezone-naive,
unordered, future, duplicate, incomplete, or hash-invalid provenance. The
deterministic payload is sufficient to replay the vote from inputs that were
available at the decision; it does not assert that the underlying market
source is configured.

The default is a full, deterministic, provenance-carrying denial. It reports all
unsupported components and the options day classifier as `UNCONFIGURED` rather
than fabricating observations.

## Causal update policy

Core evidence can change normally only at an exact completed UTC-hour boundary.
A new boundary must be contiguous with the preceding one and identify the prior
result. Missing boundaries produce `UNAVAILABLE`; they are never backfilled with
an inferred direction. Duplicate and out-of-order frames do not change state.

Between completed boundaries the result stays stable. The only same-boundary
updates are:

- `DATA_INVALIDATION`, identifying the prior result, when source evidence was
  explicitly invalidated or corrected; or
- `OPTIONS_OVERLAY_CHANGE`, identifying the prior result and retaining
  byte-identical core evidence.

Event and window times cannot extend past the completed core boundary, and no
input may be used before its available-at time. An options event may arrive
after that core boundary only through its explicit causal availability and the
defined overlay update. Quote-by-quote input is not an update trigger.

## Core vote

All three required core components must be configured, valid, fresh, and
continuous before a directional result is possible.

| Core result | Bias | Confidence |
|---|---|---|
| 3 LONG | LONG | `CORE_3_OF_3` |
| 2 LONG | LONG | `CORE_2_OF_3` |
| 3 SHORT | SHORT | `CORE_3_OF_3` |
| 2 SHORT | SHORT | `CORE_2_OF_3` |
| No 2-of-3 agreement | NEUTRAL | `NONE` |
| Component veto | NEUTRAL | `NONE` |
| Unconfigured, invalid, stale, unavailable, or discontinuous required core | UNAVAILABLE | `NONE` |

The typed component contract can carry a veto, but no production trend/CRT
strong-contrary definition has been invented. Confidence does not alter Pong's
beat mechanics in v0.

## Design-only hourly order-flow evaluator

`HourlyOrderFlowEvaluator` accepts authenticated aggressor-classified trades,
each with source, immutable ID/hash, instrument, side, positive size, event,
observed, and available-at times. It requires:

- the exact half-open UTC interval `[hour start, hour end)`;
- complete interval coverage and valid continuity;
- publication from hour end through no later than five minutes after it;
- at least 8 unique authenticated classified trades;
- all trades available by publication.

It computes `buy_volume`, `sell_volume`, `total_volume`, and
`signed_imbalance = (buy_volume - sell_volume) / total_volume`. Imbalance at or
above `+0.10` is LONG, at or below `-0.10` is SHORT, and strictly between is
NEUTRAL. Evidence expires 65 minutes after the boundary. Insufficient trades or
invalid continuity returns UNAVAILABLE; unauthenticated, duplicate, future, or
out-of-window input is rejected.

Eight trades and 10 percent are deterministic, unoptimized design constants,
not calibrated predictive claims or production approval. The eight-trade floor
is a conservative use of the existing Lane III classified-flow window size;
the hourly interval and threshold are new Pong-v0 review choices. No runtime
adapter is included, so this evaluator cannot consume the active Perpetual
process or ledger.

## Conditional options overlay

The interface supports day class `ORDINARY`, `MATERIAL_EXPIRY`, or `UNKNOWN` and
outcome `NOT_APPLICABLE`, `CONFIRM`, `CAUTION`, `VETO`, or `UNAVAILABLE`, with an
optional LONG/SHORT direction and complete provenance.

| Overlay outcome | V0 effect |
|---|---|
| Ordinary / `NOT_APPLICABLE` | Core result is unchanged. |
| `CONFIRM` matching a directional core | Raises the recorded confidence tier only. |
| `CONFIRM` conflicting with core | NEUTRAL; no new entry. |
| `CAUTION` with 2/3 core | Denies the marginal direction as NEUTRAL. |
| `CAUTION` with 3/3 core | Keeps direction but lowers the recorded tier. |
| `VETO` | NEUTRAL; prevents a new entry. |
| Required overlay unavailable or stale | UNAVAILABLE; fails conservatively. |

Options never creates a direction. Until an authoritative calendar/materiality
classifier and a vintage-aware Greeks/positioning provider are approved, the
default options result is required and `UNAVAILABLE`; an adapter may inject
`NOT_APPLICABLE` only after it has actually classified the day as ordinary.

## Pong integration and retained authority boundary

Pong now consumes `DailyBiasResult` rather than its former shallow placeholder.
At a genuine, distinct Perpetual switch it checks that the result is configured,
causally available, unexpired, directional, and aligned. It rechecks those
conditions at the separate abstract admission transition. NEUTRAL, UNAVAILABLE,
stale, not-yet-available, unconfigured, or opposed bias denies a new entry while
preserving observation and beat state.

A bias update alone records the new result and produces no action, entry, exit,
reversal, or beat. Bias does not override safety exits, protection,
reconciliation, continuity lockouts, duplicate/reordered switch handling, or
the one-complete-leg wait. Status now reports result identity/hash, confidence,
reasons, boundary, availability, and expiry.

No Pong registration, selector, transport, account binding, persistence writer,
runtime route, commissioning path, order API, or deployment was added.

## Review decisions still required

1. Define and approve the exact causal hourly-trend calculation, source,
   timezone/session treatment, freshness, invalidation, and veto semantics.
2. Define Joseph's exact CRT range construction, observation timing, direction,
   completion/invalidation rules, and any strong-contrary veto.
3. Review or replace the design-only order-flow interval, eight-trade minimum,
   10 percent threshold, freshness, and the future authenticated adapter.
4. Select and approve an expiry calendar, material-day classifier,
   vintage-aware options/Greeks source, calculations, and overlay policy.
5. Separately review durable persistence/replay, signed provenance, source
   continuity, risk/protection, ownership, reconciliation, UI, deployment, and
   commissioning before considering any execution-capable Pong work.
