# Phase E.2 — Scientific Materialization & Sampling

Status: implemented as an E-owned deterministic dataset layer.

## Boundary and source universe

Phase D remains the evidence owner. E.2 reads immutable D.7 observations,
feature definitions, coverage, and corpus snapshots and writes only
`phase_e_materialization*` records. No D production schema or semantic was
changed.

D.7's `corpus-0a4d73730b49ec6e4a3b88c441cd` fingerprint remains a bounded
D.6 commissioning projection of 256 wallet-fill anchors. It is not the
scientific retention limit. E.2 validates that D snapshot and coverage, then
forms `PHASE_D_RETAINED_INTERVAL_V1`: a streaming fingerprint over every
immutable `science_observations` row from the declared D source and
start-inclusive/end-exclusive snapshot interval. Its payload records the
source, interval, full observation count, full observation fingerprint, and
the hash of hardened E.1 D provenance.

This makes larger retained populations accessible without changing Phase D or
recasting the D.6 selection watermarks as a full-corpus definition.

During E.2 commissioning, E.1's D-provenance resolver was corrected narrowly:
it now retains snapshot-time coverage evidence across a harmless later coverage
recalculation timestamp, and accepts the documented single pre-interval D.7
anomaly that the corpus interval excludes. Semantic coverage drift remains a
hard failure. This is an E.1 integrity correction with regression coverage,
not a Phase D change.

## Identity and selection

`MaterializationSpec` binds full source provenance, unchanged E.1 partition,
horizon/lookback, feature versions, eligibility, sampling algorithm/version,
seed, sample count or full mode, tier, causal strata, missing-data policy, and
materializer code/config. Registration time is audit evidence, not identity.
Exact identity is the only cache-reuse key.

Selection uses a restricted D observation query containing anchor-time fields
only. It rejects outcome relation names and never reads `science_outcome_labels`
before membership freezes. An anchor whose declared horizon reaches the
exclusive split end is rejected even if its label later happens to exist.

- `ALL_ELIGIBLE_V1` includes every eligible anchor in chronological/ID order.
- `DETERMINISTIC_HASH_V1` ranks stable hashes of algorithm, seed, observation
  ID, and stratum.
- `TIME_STRATIFIED_HASH_V1` uses only UTC anchor-time buckets.

Membership stores immutable ordinal, D observation ID, timestamp, partition,
stratum, and selection key. It is fingerprinted before features or outcomes.

## Features, outcomes, and recovery

After freeze, E.2 validates D feature definitions, source observation
references, finite values, and declared causal lookback. It replays frozen D.6
`wallet_action` and `wallet_action_freshness` where necessary; unsupported
available feature versions are explicit missing values, never zeroes.

Outcomes attach only after freeze, using the declared horizon and first
same-source market price at/after its endpoint and strictly before split end.
Every selected member gets an outcome artifact: missing evidence remains a
durable reason and can neither remove nor replace the member.

The lifecycle is `REGISTERED → SELECTING → MEMBERSHIP_FROZEN →
MATERIALIZING_FEATURES → ATTACHING_OUTCOMES → VERIFYING → COMPLETE`.
Events and artifacts are append-only. Reads reconcile the mutable projection
with lifecycle evidence, membership, counts, and hashes. A crash leaves an
incomplete idempotently restartable stage; it cannot masquerade as `COMPLETE`.
Full-population membership uses bounded keyset batches and a hot-free-space
guard; raw evidence is never copied into E.2.

## Operator surface and non-goals

```powershell
python main.py materialization plan --database E:\Beelzebub\runtime\hot\science.sqlite3 --corpus corpus-... --partition-json .\partition.json
python main.py materialization build --database E:\Beelzebub\runtime\hot\science.sqlite3 --corpus corpus-... --partition-json .\partition.json --sample-size 10000 --seed 17
python main.py materialization verify --database E:\Beelzebub\runtime\hot\science.sqlite3 --materialization e2-...
```

E.2 has no hypothesis generation, candidate search, statistical test,
result-aware sample escalation, model fitting, prediction, signal, paper-trade
trigger, trade, capital allocation, or execution authority.
