# Phase E.1 — Scientific Experiment Foundation

E.1 is implemented in `src/phase_e/` as a narrow, fail-closed vertical slice.
Use `PhaseELedger` with the configured hot scientific SQLite path, register a
`HypothesisDefinition` against a D.7 corpus fingerprint, then run the
`NullExperimentRunner` for the lifecycle control. The public API returns the
persisted experiment ID, specification hash, exact corpus provenance hash,
status, result, and append-only events.

The initial runnable test is intentionally a null control. It demonstrates
predeclaration, deterministic identity, rejection retention, explicit restart
recovery, concurrent-start exclusion, and bit-for-bit result reproduction
without pretending to have discovered alpha.

The E.1 tests adversarially cover changed success criteria, direct SQLite
mutation/deletion, missing/corrupt provenance, unavailable required features,
invalid horizon/partition/leakage conditions, NaN/infinity, stale
code/configuration identity, partial failure, restart recovery, duplicate
execution, unknown stored status, and attempted promotion. All cases fail
closed; no test or production E.1 path creates a signal or trading authority.
