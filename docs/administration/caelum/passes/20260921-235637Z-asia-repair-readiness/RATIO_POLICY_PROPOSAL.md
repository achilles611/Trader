# Proposed periodic measurement contract

Proposal ID: `PONG_PERIODIC_MEASUREMENT_CONTRACT_V1_PROPOSED`

Approval state: **UNAPPROVED — POLICY DECISION REQUIRED**.

This proposal is prepared before qualification outcomes. It does not alter the
current source, tests or qualification gate, and Caelum review is not Joseph's
approval. It is designed to close the missing contract without inventing a
market-rate-dependent pass number.

## Proposed definition

Take immutable start and end snapshots from one predeclared eligible natural
window. Let:

- `D` be the delta of authenticated, topology-valid, continuity-valid DEPTH
  admissions at the authoritative Pong-host boundary;
- `E` be the delta of ordinary periodic analyses that pass all input/readiness
  checks, execute the full analytical path, and return a terminal result;
- `O` be the delta of eligible current periodic opportunities;
- `R = D / E` when `E > 0`.

When `E == 0`, `R` is JSON null and `ratio_undefined_reason` is mandatory. Raw
counts `D:E` are canonical; any decimal is presentation only.

Protected evaluations, protection-superseded deadlines, obsolete deadlines,
shutdown deadlines, intentional no-work, input-unavailable outcomes, thrown or
cancelled analyses and incomplete asynchronous outcomes are excluded from `E`
and reported in distinct terminal classes. Each opportunity has exactly one
terminal class so labels cannot double-count.

## Proposed eligibility

The whole window is ineligible for acceptance, without trimming, if any of the
following occurs: identity/epoch mismatch, counter reset, source gap/duplicate,
producer overflow or transport failure, bridge loss/unhealthy reconciliation,
position/order/pending directive, canonical-depth failure, freshness breach,
terminal host/collector fault, durability discontinuity, or a transition into
protection. The receipt still reports all deltas and the exact reasons.

## Proposed numeric treatment and derivation

`R` has **no standalone minimum or maximum**. Natural DEPTH rate changes its
value, and both a lower DEPTH rate and fewer completed evaluations can move it
in misleading directions. At a healthy 20 ms grid, the expected ordinary rate
is derived from scheduler ownership as `1 / 0.020 = 50 evaluations/second`, so
the expected observed ratio is `natural DEPTH/second / 50`, not a universal
constant.

The proposed acceptance arithmetic is instead exact:

1. `E == O` for the eligible window;
2. eligible obsolete/missed opportunities equal zero;
3. all terminal-class counts reconcile exactly to scheduled opportunities;
4. the displayed decimal recomputes from `D/E` within relative error `1e-9`,
   while the integer pair remains authoritative; and
5. all independent freshness, losslessness, equilibrium, durability and safety
   gates pass simultaneously.

Late current opportunities remain visible. Zero obsolete opportunities bounds
their start lateness below one interval under the current scheduler; the next
version should also persist maximum and percentile start lateness. There is no
tolerance for missing eligible work or mixed epochs in this proposal.

## Failure examples

- `D=120000, E=15000, O=15000` over 300 seconds gives `R=8`; it is eligible
  only if every independent gate passes.
- `D=60000, E=14500, O=15000` gives a smaller-looking `R`, but fails because
  500 eligible opportunities did not produce completed full analysis.
- `D=120000, E=0` produces null with a reason such as
  `NO_ORDINARY_FULL_COMPLETIONS`, never infinity or zero.
- A five-second bridge loss makes the declared window ineligible even if the
  remaining segment has favorable counts; it is not trimmed away.
- `D=0, E=15000` is arithmetically defined but does not prove natural DEPTH
  flow and fails the independent input/eligibility contract.

## Decision requested at closeout

Joseph should either approve this version, revise its exact-count cadence rule,
or explicitly designate `R` diagnostic-only while selecting another versioned
cadence acceptance contract. No implementation or qualification acceptance may
claim this proposal is active until that decision is recorded.
