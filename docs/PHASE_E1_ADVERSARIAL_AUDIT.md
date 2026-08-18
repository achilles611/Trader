# Phase E.1 adversarial audit

Status: **freeze candidate hardened; E.2/materialization not started**

Audited E.1 baseline: `bc9ac9d8d6ab283d8e5a7f359cfcd03e4f2dec9a`.

The Phase D SHA supplied in the handoff ended in `964b03`, but that object is
not present. The repository freeze record and E.1 parent both resolve to the
actual frozen D baseline
`93206d3dc9ca780e1d6a58994a4adb7cb9d6a11a`. No Phase D production file was
changed by this audit.

## Disposition

### High findings, fixed

1. Mutable lifecycle columns were trusted on reads. A direct valid SQL update
   could present `REGISTERED` as `COMPLETED`, turn a rejected experiment into a
   survivor, alter attempts/timestamps, or change promotion state without
   corresponding evidence. Reads now structurally derive and reconcile the
   only legal projection from append-only lifecycle/result/promotion evidence.
2. `record_result` accepted an arbitrary well-shaped result from a direct
   caller. That could create a historical survivor without running the only
   supported E.1 statistic. The ledger now independently reconstructs and
   requires the exact deterministic-null result and rolls back any mismatch.

### Medium findings, fixed

1. D provenance bound only part of the coverage record and omitted referenced
   feature definitions. E now freezes and compares the full corpus payload,
   columns, coverage row, and feature definitions/hashes.
2. Registration and result commit had D check/use windows. Both recheck D while
   an immediate SQLite transaction excludes concurrent writers.
3. Creation time was included in definition hashing but excluded from
   experiment identity, causing irrelevant-time conflicts. It is now immutable
   registration evidence but not a scientific criterion.
4. Canonical hashing depended on ordinary JSON float/type behavior. The frozen
   version now has type tags, exact finite float encoding, Unicode
   normalization, strict numeric types, and canonical persisted JSON.
5. Lifecycle comparisons used canonical UTC text whose variable fractional-
   second form is not lexicographically time ordered. All ordering and output
   sorting now compare parsed UTC instants.
6. Lifecycle event IDs did not bind status endpoints or reason. Those fields
   are now hashed into the event identity, so trigger-defeating edits are
   detected without relying only on transition-shape checks.
7. The partition contract did not encode feature temporal windows, precise
   interval/outcome-boundary policy, or a sampling algorithm version. Those
   are now immutable identity inputs and exact boundary gaps are validated.
8. Runner code/config labels could be overridden through the constructor.
   E.1 runner identity is now implementation-owned and stale versions fail
   closed.

### Low findings and explicit limits

- Identical simultaneous denied-promotion events once collided on their
  deterministic event ID. They are now idempotent; authority remains false.
- SQLite transactions recover incomplete writes on connection/process death;
  they do not preserve uncommitted diagnostic detail about where a process
  died. A committed `RUNNING` attempt is resolved by an explicit recovery
  event on restart.
- Reproduction after software evolution requires retention of the declared
  runner implementation. The ledger records schema/canonicalization,
  code/config, specification, corpus, feature, and result identities and fails
  closed when the implementation is unavailable; it does not embed an
  executable artifact.
- This is structural integrity, not an external authenticity system. An
  attacker limited to projection edits, evidence corruption, or dropped
  triggers is detected unless they coherently rewrite every dependent row and
  recompute every hash. A fully privileged owner able to rewrite the entire
  database and program can forge an unkeyed local ledger. Preventing that
  requires an external signed/WORM anchor and is outside E.1's local SQLite
  trust boundary.

## Adversarial regression coverage

The E.1 suite now attacks projection forgery, missing/extra/duplicate/reordered
events, result/status/attempt/recovery mismatches, fabricated survivors,
noncanonical and duplicate-key JSON, numeric coercion and nonfinite values,
registration-time identity, Unicode/float canonicalization, exact partition
boundaries and timezone normalization, full D coverage/feature drift, D race
windows, fresh-process reproduction, two-process registration/start/result/
failure/recovery/promotion races, and real process death with open projection
or result transactions. A static repository guard rejects imports from E into
production trading paths and trading-authority calls from E.

## Freeze rule

E.1 may be frozen only while all targeted and complete repository tests pass,
the D diff remains empty, and the final Git working tree is clean. Scalable
materialization must implement the frozen interval/lookback/lookforward and
outcome-containment contract; it must not reinterpret it.

## E.2 commissioning integrity correction

Real D.7 commissioning exposed two narrow resolver assumptions that rejected
the frozen evidence for operational rather than scientific changes. D.7
retains one documented source event immediately before the declared interval,
counts it as a timestamp anomaly, and excludes it from the end-exclusive
corpus. E.1 now accepts only that exact one-boundary form; in-interval,
post-interval, multiple, or otherwise unexplained anomalies still fail closed.

Separately, `science_data_coverage` is a mutable coverage projection and a
later recomputation updates only `computed_at`. E.1 now preserves the immutable
coverage payload embedded in the corpus snapshot while requiring every other
coverage field to match current D state exactly. Counter, details, source,
interval, feature, snapshot, or provenance drift remains a hard failure.

Both corrections have E.1 regression coverage. They change no Phase D code or
evidence and preserve the E.1 scientific contract; they make its provenance
resolver correctly represent the documented frozen D.7 evidence.
