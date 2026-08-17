# Phase E.1 — Scientific Experiment Foundation

E.1 is implemented in `src/phase_e/` as a narrow, fail-closed vertical slice.
Use `PhaseELedger` with the configured hot scientific SQLite path, register a
`HypothesisDefinition` against a D.7 corpus fingerprint, then run the
`NullExperimentRunner` for the lifecycle control. The public API returns the
persisted experiment ID, specification hash, exact corpus provenance hash,
status, result, and append-only evidence.

The initial runnable test remains intentionally a null control. It proves
predeclaration, rejection retention, explicit restart recovery, process-safe
execution exclusion, and exact result reproduction without claiming alpha.
The ledger independently validates the null result; bypassing the runner
cannot create a historical survivor.

## Hardened ledger model

`phase_e_experiments` is a mutable projection, not authoritative evidence.
Every read, reproduction, and lifecycle mutation reconciles it against the
immutable hypothesis, lifecycle events, result, and promotion history. The
validator rejects missing, extra, duplicate, reordered, or impossible
transitions; attempt/timestamp disagreement; result/status disagreement; and
promotion state without its exact history. Valid SQLite enum values therefore
cannot forge a credible state on their own.

Identity uses `phase-e1-type-tagged-sha256-v1`: mappings are ordered and typed,
Unicode is NFC-normalized, finite floats use exact hexadecimal IEEE-754 form,
negative zero is normalized, and persisted JSON must use one canonical storage
encoding. Scientific identity excludes only creation/registration time. That
time remains immutable ledger evidence and the first registration wins.

## D boundary and partitions

E reads D and writes only `phase_e_*` tables. Its frozen provenance includes
the complete D corpus payload, every snapshot column, the complete coverage
row, and every referenced feature definition/hash. Registration and result
commit recheck current D evidence while `BEGIN IMMEDIATE` excludes a D writer,
closing check/use races. Changed coverage counters/details, observation count,
feature definition, snapshot content, malformed numeric values, or reused
fingerprints fail closed.

Partitions use start-inclusive/end-exclusive intervals. Each split gap covers
the outcome horizon, maximum declared feature lookback, purge, and embargo.
Feature references forbid lookforward, outcome anchors must be contained in
their split, and the sampling algorithm/version plus seed are identity inputs.
Later materialization must enforce these already-frozen timestamp rules and
retain source evidence; E.1 does not materialize anything.

## Reproducibility and authority

The result is reproducible in-process, after a fresh process, and after a
restart when the declared runner version remains available. A changed or
unavailable runner/config version fails closed. The repository must retain the
versioned implementation (normally through Git) for reproduction after
software evolution; executable code is not embedded in SQLite.

E.1 exposes no prediction, signal, decision, risk, execution, paper-trade, or
capital-allocation integration. `trading_authority` and `qualified_signal` are
always false, including on denied promotion requests.

The complete adversarial disposition and trust boundary are recorded in
`PHASE_E1_ADVERSARIAL_AUDIT.md`.
