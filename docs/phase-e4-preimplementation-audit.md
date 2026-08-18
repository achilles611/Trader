# Phase E.4 pre-implementation scientific audit

Date: 2026-08-17
Baseline: `60a46f827b950ba95ec4590fce521454bbe6415f`

## Existing contract boundaries

- E.1 owns immutable hypothesis definitions, D.7 corpus lineage, append-only
  lifecycle evidence, and denial of trading authority. Its `ExperimentResult`
  is deliberately too small for an E.4 family evaluation because it cannot
  represent censoring, correction denominators, cluster dependence, or
  practical relevance independently from statistical evidence.
- E.2 freezes outcome-blind membership before features and outcomes, records
  exact train/validation/test membership, stores sampling probabilities and
  weights, enforces horizon-contained end-exclusive partitions, and verifies
  its artifacts against the bound Phase D source. Its completed artifact is
  the eligible E.4 evidence snapshot.
- E.3 reads predictor artifacts only, derives thresholds from the train
  partition, freezes a semantically deduplicated proposal universe, maps every
  proposal to E.1, and persists the exact multiple-testing family identifier.
  E.4 must consume that universe without adding, removing, or reprioritizing
  hypotheses.
- Phase D remains the source/provenance owner. E.4 needs read-only access to
  the authoritative anchor wallet, symbol, and source-event identity only to
  construct conservative dependence clusters. Wallet identities must never be
  emitted in an E.4 result.

## Legacy outcome-aware code review

`src/copytrade/pattern_discovery.py`, `src/copytrade/experiments.py`, and the
D.6 worker evaluation path are scientifically incompatible with E.4 and will
not be imported or reused:

1. `BoundedPatternDiscovery` chooses a threshold from outcome-bearing rows,
   suppresses candidates below an observed effect, ranks by absolute observed
   effect, and truncates the family before registration.
2. The D.6 worker silently skips hypotheses with insufficient filtered support
   and constructs the correction family only from the remaining `pending`
   subset. That is not the frozen E.3 denominator.
3. Its validation expectancy is inspected as an additional survivor gate, and
   there is no one-shot reserved test/holdout access contract.
4. `HistoricalExperimentEngine` applies a block sign test to selected outcome
   values rather than the E.3 predicate-versus-complement contrast. It does not
   use E.2 sampling weights and does not cluster repeated observations by
   wallet or shared/overlapping causal evidence.
5. Missing, immature, corrupt, and preregistered exclusions are not represented
   as distinct family-retained scientific states.

The only reusable ideas are generic mathematical definitions, not code paths:
canonical ordering, deterministic seeded resampling, and monotone adjusted
values. E.4 will implement and version these independently under `src/phase_e`.

## Initial E.4 methodology decision

- Experimental observation: one frozen E.2 validation-partition wallet-fill
  anchor with its exact E.2 feature and horizon outcome artifacts.
- Independence unit: a conservative connected component. Anchors are joined
  when they share a wallet, a feature/outcome source event, or overlapping
  same-symbol outcome windows. Rows inside a component are never resampled
  independently.
- Estimand: E.2-sampling-weighted difference in mean net outcome between the
  exact frozen predicate and its within-partition complement.
- Uncertainty/test: deterministic whole-component nonparametric bootstrap,
  with a percentile interval and a centered, finite-resample, two-sided
  p-value. Insufficient independent components fails closed.
- Multiplicity: Holm family-wise error correction over every ordinal in the
  exact E.3 universe. Holm is valid under arbitrary cross-hypothesis
  dependence and is appropriate for the small initial family. Unevaluable,
  pending, invalid, and insufficient-support members remain in the denominator
  with a conservative correction input of one.
- Holdout: the initial protocol evaluates validation only. The E.2 test split
  is reserved and has no E.4 query operation. A different experiment requires
  a new immutable protocol/evidence version; observed results cannot edit one.
- Missingness: unresolved evidence is pending, never negative. Mature missing
  evidence, corrupt evidence, and incomplete predictors fail closed rather
  than being silently dropped.
- Decisions: statistical evidence and practical relevance are separate.
  Neither grants prediction, signal, execution, or capital-allocation authority.

## Known limitation encoded fail-closed

The repository has no authoritative graph proving that distinct wallets are
economically independent. The connected-component policy handles observed
same-wallet, shared-event, and temporal-overlap dependence, but not hidden
coordination between otherwise unlinked wallets. E.4 will identify this
limitation in its protocol/result manifests and will not claim causal or
production performance from fixture evidence.
