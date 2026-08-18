# Phase E.2 — Scientific Materialization & Sampling

Status: hardened V2 implementation; freeze evidence is recorded in
`PHASE_E2_ADVERSARIAL_AUDIT.md`.

## Boundary, time, and source universe

Phase D remains the evidence owner. E.2 reads immutable D.7 observations,
feature definitions, coverage, and corpus snapshots and writes only
`phase_e_materialization*` records. It does not write a `science_*` table or
change Phase D production semantics.

The official historical archive has three distinct clocks. `event_at` is when
the source event occurred, `received_at` is when Beelzebub acquired it, and
`persisted_at` is when the row was stored. The frozen D archive contract sets
`normalized_at` to the canonical UTC representation of `event_at`; V2 verifies
their instant equality on every row. Therefore historical partitioning,
ordering, feature windows, membership timestamps, and outcome horizons use
source event time under `HISTORICAL_EVENT_AT_V1`. Receipt time is the correct
information-availability clock for prospective/live science and is
deliberately unsupported by this retrospective materializer.

D.7's `corpus-0a4d73730b49ec6e4a3b88c441cd` fingerprint is a bounded D.6
commissioning projection of 256 wallet-fill anchors, not the scientific
retention limit. E.2 validates that snapshot and its full E.1 provenance, then
forms `PHASE_D_RETAINED_INTERVAL_V2`: a streaming full-row fingerprint over
every official-archive observation in the start-inclusive/end-exclusive
interval. It binds and validates observation identity, event/receipt/
persistence chronology, symbol and source fields, payload and quality JSON,
payload bytes and hashes, schema, code, and configuration provenance.

All time comparisons use parsed instants rendered to fixed-width UTC keys.
Canonical D timestamps may validly mix whole and fractional seconds, for which
raw SQLite text ordering is not chronological.

The audited retained interval contains 796,514 observations. The D.7
commissioning report's 796,076 was its last ingestion-pass count: 792,024 rows
at or after second one plus 4,052 boundary rows. Of those boundary rows, 4,050
were actually in range and two were pre-interval. The append-only D table also
retains 462 unique rows from earlier interrupted/replayed acquisition attempts:
440 in-range second-zero rows and 22 pre-interval rows. The current D table thus
contains 796,538 source rows. Exact V2 retention is
`796,076 - 2 + 440 = 796,514`; all 24 genuinely pre-interval rows are excluded.
Of the retained rows, 398,445 market-price rows are ineligible,
82,786 wallet fills fall outside the declared train/validation/test windows,
and 1,518 anchors cross a split's outcome boundary, leaving 313,765 eligible
wallet-fill anchors.

During E.2 commissioning, E.1's D-provenance resolver was narrowed to accept
only the documented official-archive pre-interval anomaly within one second of
the boundary, while ignoring only a later coverage recomputation timestamp.
Observation counts, coverage state/fraction, interval, source, feature
definitions, and other semantic coverage details remain bound and fail closed
on drift.

## Identity and outcome-blind selection

`MaterializationSpec` binds the full source provenance, E.1 partition,
horizon/lookback, feature versions, eligibility, sampling algorithm/version,
seed, sample count or full mode, sampling strata, outcome-resolution policy,
missing-data policy, tier/purpose audit namespace, and materializer code/config.
Registration time is audit evidence, not identity. Exact identity is the only
cache-reuse key. Tier and purpose remain conservatively identity-bearing so an
artifact cannot be reused under a different declared scientific intent.

Selection reads only D observation identity and anchor-time fields. It rejects
outcome relation names, never reads `science_outcome_labels`, and does not use
persisted D features, derived regimes, labels, costs, or missingness. An anchor
is excluded if its declared feature lookback crosses its partition start or if
its horizon reaches the exclusive split end.

- `ALL_ELIGIBLE_V1` includes every eligible anchor exactly once in event-time/
  observation-ID order.
- `DETERMINISTIC_HASH_V1` ranks a SHA-256 key over algorithm, seed, observation
  ID, and causal stratum, with observation ID as a total tie-break.
- `TIME_STRATIFIED_HASH_V2` allocates equally across occupied UTC event-time
  buckets. Seeded hash order assigns the remainder buckets, and a deterministic
  global hash rank backfills undersized strata. Empty strata do not exist.

V1 time stratification is retained only for reading legacy V1 artifacts and
cannot be registered by V2. V2 freezes an immutable sampling-design artifact
before feature/outcome work. It records eligible and selected counts per
partition and stratum, exact rational inclusion probabilities and weights,
target/primary/backfill counts, exclusion counts, allocation/backfill policy,
and tie handling. E.4 can therefore distinguish an intentionally balanced
sample from the underlying population and apply its own valid inference.

Membership stores immutable ordinal, D observation ID, event timestamp,
partition, stratum, and selection key. Its fingerprint and sampling-design
fingerprint freeze before features or outcomes. SQLite triggers reject later
membership or design insertion.

## Features, outcomes, verification, and recovery

After freeze, V2 validates every D source row and feature definition/hash,
requires exact feature-to-source lineage inside the same bound source and
partition, enforces the declared causal lookback, and recomputes D feature data
fingerprints. Numeric values must be finite and missingness must be explicit.
`wallet_action` can be replayed deterministically. Historical
`wallet_action_freshness` is explicitly missing with
`HISTORICAL_ACQUISITION_LATENCY_IS_NOT_A_CAUSAL_FEATURE`: archive acquisition
latency is operational metadata, not a historical causal value.

Outcomes attach only after membership freeze. A point horizon uses the anchor
fill price and the first same-symbol, same-source `MARKET_PRICE` trade at or
after the exact event-time endpoint, breaking simultaneous ties by observation
ID. `FIRST_TRADE_AT_OR_AFTER_WITHIN_TOLERANCE_V1` bounds resolution lag to the
versioned `maximum_lag_seconds` (default five) and never crosses the partition
end. Each resolved artifact records its exact target endpoint, actual elapsed
time, resolution lag, and source observation. Missing/malformed start price,
symbol, direction, cost, or bounded market evidence is a distinct explicit
reason. No cost is imputed. Every selected member receives exactly one outcome
artifact and can never be removed or replaced.

The lifecycle is `REGISTERED → SELECTING → MEMBERSHIP_FROZEN →
MATERIALIZING_FEATURES → ATTACHING_OUTCOMES → VERIFYING → COMPLETE`. Events and
artifacts are append-only. Stage triggers restrict artifact creation. Every
read reconciles the projection against exact ordered event type/reason/payload,
stage timestamps, counts, and hashes. Completion and `verify` run in an
immediate transaction, rebind the current D source, independently replay exact
membership, and recompute every feature and outcome artifact. A check/use race,
valid-looking forged projection, corrupted artifact, or changed D row therefore
fails closed.

Full-population selection streams bounded insert batches but holds one
immediate transaction for a single atomic D view. It independently recomputes
the exact eligible count/fingerprint before freeze. The free-space guard
reserves the configured minimum plus twice the estimated artifact footprint
for SQLite journal/WAL headroom. Raw D payloads are never copied into E.2.

## Operator surface and non-goals

```powershell
python main.py materialization plan --database E:\Beelzebub\runtime\hot\science.sqlite3 --corpus corpus-... --partition-json .\partition.json
python main.py materialization build --database E:\Beelzebub\runtime\hot\science.sqlite3 --corpus corpus-... --partition-json .\partition.json --sample-size 10000 --seed 17 --outcome-maximum-lag-seconds 5
python main.py materialization verify --database E:\Beelzebub\runtime\hot\science.sqlite3 --materialization e2-...
```

E.2 has no hypothesis generation, candidate search, statistical test,
result-aware sample escalation, model fitting, prediction, signal, paper-trade
trigger, trade, capital allocation, or execution authority.
