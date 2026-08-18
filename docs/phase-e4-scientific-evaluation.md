# Phase E.4 scientific evaluation

Phase E.4 judges the exact hypothesis universe frozen by E.3. It produces
immutable scientific evidence only. It has no prediction, signal, execution,
capital-allocation, or trading authority.

## Scientific contract

One E.4 protocol binds exactly one complete E.3 generation run. Registration
reconstructs the family, proposal-to-E.1 mappings, and E.2 artifact lineage
from authoritative SQLite state. The sealed E.2 verifier internally validates
outcome artifacts but returns only fingerprints; no outcome value is exposed to
the protocol builder. It then freezes a contiguous protocol-member
list in the same order as the E.3 universe. One generation run may have only
one protocol, preventing a second method from being selected after results are
visible.

The initial evaluator uses only the E.2 `validation` partition. Its SQL has a
literal validation predicate and accepts no partition argument. There is no E.4
operation that reads the E.2 `test` partition. Test/holdout query count is fixed
at zero in protocols, lifecycle evidence, results, replay evidence, and
manifests.

## Statistical judgment

**Experimental observation.** One frozen E.2 validation wallet-fill anchor,
its exact frozen predicate feature, and its exact E.2 horizon outcome.

**Independence unit.** Rows are not assumed IID. A union-find pass constructs
connected components. Two anchors join when they share a wallet, a causal
feature/outcome observation, a source event, or overlapping same-symbol outcome
windows. Only whole components are resampled. If there are too few total or
per-arm components, the hypothesis is `INSUFFICIENT_SUPPORT`.

**Known dependence limitation.** The repository cannot prove that otherwise
unlinked wallets are economically independent. The limitation is persisted in
every protocol. No causal or production-performance claim follows from E.4.

**Null.** The E.2-sampling-weighted mean net outcome of the frozen predicate is
equal to that of its within-validation complement.

**Alternative.** The two weighted means differ. The test is two-sided because
the initial E.3 contract predeclares a two-sided distribution-difference test;
E.4 rejects incompatible test plans rather than silently substituting one.

**Effect.** The predicate weighted mean minus the complement weighted mean.
Exact E.2 inverse-inclusion weights are used. Statistical evidence and absolute
practical effect are separate decisions. E.4 preregisters a practical threshold
(default 0.001 net-outcome difference) that may be stricter, but never weaker,
than E.3's effect floor.

**Uncertainty and p-value.** The evaluator resamples whole dependence
components with replacement using the E.3 resample count and a deterministic
SHA-256-derived per-proposal seed. It stores a percentile interval, a centered
two-sided plus-one bootstrap p-value, requested/valid resample counts, and a
fingerprint over bootstrap effects. Too few valid whole-component resamples
fail closed.

**Minimum support.** Each arm must meet the E.3 minimum observation count. The
protocol additionally preregisters minimum total and per-arm dependence
components. Non-finite outcomes/weights and values beyond preregistered numeric
bounds cannot enter a statistic.

**Censoring.** Resolved positive, resolved negative, not-yet-mature, mature
missing, invalid/corrupt, and preregistered exclusion counts are distinct.
Unresolved outcomes are pending and never negative. Mature missing or invalid
required evidence makes the snapshot inconclusive/invalid rather than being
dropped. Every family member remains in correction.

**Multiple testing.** Holm-Bonferroni controls family-wise error over every
ordinal in the exact E.3 universe. Holm is valid under arbitrary
cross-hypothesis dependence and is intentionally conservative for the small
initial family. Unevaluable members use correction input p=1 and remain in the
stored denominator. Ties resolve by raw p-value, E.3 ordinal, then proposal ID.

**Supported.** `STATISTICALLY_SUPPORTED` means the Holm-adjusted p-value is at
most the family alpha fixed in E.3. `SCIENTIFICALLY_SUPPORTED` additionally
requires the separately fixed practical-effect threshold. The only downstream
state is `SCIENTIFIC_REVIEW_ONLY`.

**Not supported.** `NULL_NOT_REJECTED` means the registered test did not reject
the null; it does not prove the null true. Pending, invalid, missing, and
insufficient-support states are `INCONCLUSIVE`, not statistical rejection.

## Persistence and replay

The implementation owns these tables:

- `phase_e_evaluation_protocols`
- `phase_e_evaluation_protocol_members`
- `phase_e_evaluation_protocol_events`
- `phase_e_evaluation_runs`
- `phase_e_hypothesis_evaluations`
- `phase_e_evaluation_manifests`
- `phase_e_evaluation_events`

SQLite triggers prevent updates/deletes of protocols, members, results,
manifests, and events. Protocol members can be inserted only while the protocol
is registering. Results and the manifest can be inserted only while a run is
evaluating. Evaluation computes and finalizes the entire family in one
`BEGIN IMMEDIATE` transaction: process death rolls the attempt back completely,
and concurrent workers converge on the one deterministic run identity.

Replay re-verifies E.2 and E.3, recomputes the validation snapshot, every
bootstrap statistic, Holm ordering/value, decision, result hash, and manifest
linkage. Operational timestamps are excluded from deterministic scientific
identity.

## Operator CLI

Use `python main.py hypothesis-evaluation --database <path> <command>`.

- `eligible`: inspect complete E.3 universes.
- `preregister --generation-run <id>`: freeze a protocol before E.4 outcome
  values are exposed.
- `protocols` / `protocol --protocol <id>`: inspect protocols.
- `evaluate --protocol <id>`: atomically evaluate validation evidence.
- `results --protocol <id>`: inspect results and pending reason codes.
- `run --evaluation-run <id>`: inspect a run and manifest.
- `verify --evaluation-run <id>` / `reproduce --evaluation-run <id>`: verify
  deterministic replay.

Changing a method, alpha, seed, component rule, support rule, or numeric bound
requires new scientific lineage. The same E.3 generation run cannot be
re-registered after any result is known.
