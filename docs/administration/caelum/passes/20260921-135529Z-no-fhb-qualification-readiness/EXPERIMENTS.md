# Experiment registry — no-FHB qualification readiness

All optional experiments are **not authorized for timing yet**. `BASELINE_R`
and a clean authoritative-initialization corpus do not exist. Mandatory R1–R4
repairs are correctness/lifecycle work and are not required to show a speedup.

## Common finite budget and adoption rule

- One initial implementation/configuration plus at most one corrective revision
  justified by a named defect or measured finding.
- One untimed warmup and one short screen per configuration.
- Full comparison only after correctness and short screen: exactly three paired
  baseline/candidate runs with alternating A/B order, same fixed corpus/path,
  all results retained.
- One replacement run only for an independently demonstrated harness fault.
- Default small-optimization gate: at least 5% end-to-end primary-metric gain,
  consistent across all three pairs and greater than baseline variability, with
  no critical regression; or at least 10% relevant tail-latency improvement
  with adequate samples, non-regressing throughput/CPU, and unchanged limits.
- New dependency, ownership boundary, or substantial representation migration:
  at least 15% end-to-end gain, or reproducibly remove a continuity/recovery
  blocker that a simpler repair cannot solve.
- Dispositions are limited to `ACCEPTED_FOR_INTEGRATION`,
  `REJECTED_CORRECTNESS`, `REJECTED_REGRESSION`,
  `PARKED_NO_MATERIAL_BENEFIT`, `PARKED_INCONCLUSIVE`, or
  `NOT_APPLICABLE_WITH_EVIDENCE`.
- Faster helper span, lower memory, fewer processed records, unready path,
  weakened verification/durability, or different scheduler mode cannot pass.

## E1 — validated event representation and reusable canonical material

- **Status:** `PREREGISTERED_CONDITIONAL`; first performance family after
  `BASELINE_R`.
- **Measured bottleneck required:** current production-path counts/timing must
  confirm repeat encode/decode, canonicalization, parsing/sorting/copying, or
  digest reconstruction after the trust boundary.
- **Hypothesis:** one immutable/content-bound verified observation retaining
  original signed bytes and safely reusable canonical fields/digests removes
  redundant work without losing independent replay/ledger verification.
- **Owner/files:** Sol assigns a Python admission/representation owner after
  R1–R4; no file ownership granted at intake.
- **Baseline/candidate:** `BASELINE_R` versus one E1 candidate.
- **Corpus:** clean initialized authenticated corpus; negative golden vectors
  are correctness-only, never timing evidence.
- **Primary metric:** repaired full-host end-to-end wall/service rate; secondary
  call-count removal, CPU, p95/p99 age, queues, durability.
- **Comparator:** signed bytes/digests, strict rejection, same-prefix book,
  features, blockers and decisions.
- **Dependency impact:** prefer existing dependencies/in-process refactor.
- **Acceptance:** common 5%/10% rule; arbitrary caller `verified=True`, mutable
  aliasing, lost independent verification, or weak gain is rejected/parked.
- **Attempts used:** 0 implementation, 0 corrective, 0 warmup/screen/pairs.

## E2 — bounded batch drain and byte-stream framing

- **Status:** `PREREGISTERED_CONDITIONAL_ON_MEASUREMENT`.
- **Measured bottleneck required:** current exact-path read/write/encode call
  counts or framing/buffer overhead remain material after repairs/E1.
- **Hypothesis:** drain already-waiting complete frames in bounded writes and
  parse all complete frames per receive without waiting for a batch or holding
  low-rate traffic.
- **Owner/files:** native publication/transport owner, assigned by Sol after
  current call-path measurement.
- **Primary metric:** identical-input full-path CPU/wall and p95/p99/source age;
  frame call counts and bounded backlog secondary.
- **Comparator:** exact frame identity/accounting under one-byte fragmentation,
  multi-frame reads, split UTF-8, malformed/oversized/truncated input,
  partial writes, disconnect and uncertain delivery.
- **Dependency impact:** package optional; no platform migration.
- **Acceptance:** common gate, unchanged latency/security/delivery semantics.
  Buffer growth, batch delivery without proof, duplicates, or low-rate delay
  rejects the candidate.
- **Attempts used:** 0.

## E3 — sequence-owned pooled buffers or bounded ring storage

- **Status:** `DEFERRED_CONDITIONAL`; cannot run before E1/E2 measurement.
- **Measured bottleneck required:** residual allocation or lock pressure at the
  native producer/sender boundary.
- **Hypothesis:** a compatible bounded short-lock pool or sequence-owned queue
  reduces allocation/lock cost while preserving multi-callback publication
  order and lifetime through auth/send/journal/readers.
- **Owner/files:** native publication/transport owner; no hand-written lock-free
  algorithm or Disruptor dependency without framework/license proof.
- **Primary metric:** full-path end-to-end wall/CPU and tail latency.
- **Comparator:** concurrent stream ordering, wraparound, saturation, shutdown,
  reuse-after-return, torn/mutated snapshot, duplicate/drop tests.
- **Acceptance:** stronger 15% gate if it adds dependency/ownership complexity.
  Broad rewrite, false single-producer premise, busy-spin contention, or weak
  whole-path gain parks/rejects it.
- **Attempts used:** 0.

## E4 — collector/host lifecycle separation

- **Status:** `APPLICABLE_RELIABILITY_PROTOTYPE_AFTER_R1_R3`; supports R2.
- **Observed blocker:** last proof started with stale pre-start producer backlog
  and large failure deltas; receiver readiness and producer generation were not
  a clean authoritative boundary.
- **Hypothesis:** the smallest supported lifecycle extension around the existing
  independently owned collector removes receiver-downtime/stale-start coupling
  without introducing an always-on service or another analytical owner.
- **Owner/files:** Sol owns lifecycle/shared configuration. A specialist may
  prepare isolated source/tests only with disjoint paths; Sol alone operates.
- **Primary metric:** reliability: listener-ready handshake, attributable new
  source boundary, lossless bounded capture across planned host stop/reattach.
- **Comparator:** source absent, host stop with collector retained, authoritative
  reattach, same-identity increases/regressions, new identity, disk-full/quota,
  cleanup, graceful drain; isolated sinks only.
- **Dependency impact:** no daemon, startup service, or order/control channel.
- **Acceptance:** reproducibly remove stale-start coupling with non-regressing
  admission/latency/durability. Prefer a smaller existing-collector lifecycle
  change over process splitting.
- **Attempts used:** 0.

## E5 — strict faster decoder alternatives

- **Status:** `DEFERRED_CONDITIONAL_AFTER_E1`.
- **Measured bottleneck required:** strict decode/adapter work remains material
  in E1-repaired production path.
- **Hypothesis:** one preregistered msgspec or orjson role reduces whole-path
  cost while retaining the original canonical signer/verifier and all input
  restrictions.
- **Owner/files:** Python representation owner after dependency/runtime/license
  compatibility review.
- **Primary metric:** end-to-end production-path wall/CPU, not raw codec speed.
- **Comparator:** duplicate/unknown keys, Unicode/UTF-8, null/absent, numeric
  bounds/precision/ticks, bool-as-int, non-finite, timestamps, nesting,
  truncation/oversize, exact auth/canonical vectors.
- **Dependency impact:** pinned supported Windows artifact with provenance and
  rollback; alternatives are named before results, not an open sweep.
- **Acceptance:** stronger 15% gate for a new dependency. A second full parse,
  canonical drift, unsupported wheel/runtime, or weak whole-path benefit parks.
- **Attempts used:** 0.

## E6 — DBN-inspired compact internal record layout

- **Status:** `DEFERRED_CONDITIONAL_AFTER_E1`; avoid double-counting E1.
- **Measured bottleneck required:** representation/materialization cost remains
  material after E1.
- **Hypothesis:** compact typed internal fields, exact integer ticks where valid,
  bounded book arrays, and immutable provenance references reduce work while
  retaining original signed wire and full-depth evidence.
- **Owner/files:** Python representation owner; binary wire is a second variant
  only if encoding remains dominant and is preregistered before results.
- **Primary metric:** end-to-end CPU/wall and tail/queue behavior; byte count is
  secondary only.
- **Comparator:** native/Python interpretation, bounds/endian/schema mismatch,
  tamper/truncation/source changes, frame preservation, state/feature parity.
- **Dependency impact:** internal format preferred; a broad protocol migration
  is parked for a separately approved architecture pass.
- **Acceptance:** common gate for internal-only; 15% and full installer/
  certificate coverage for substantial representation or wire migration.
- **Attempts used:** 0.

## E7 — staged durability preparation with truthful barriers

- **Status:** `DEFERRED_CONDITIONAL_ON_MEASURED_STALL`.
- **Measured bottleneck required:** current full path shows disk, encoding, lock,
  or writer wait on the critical path after prior repairs.
- **Hypothesis:** prepare journal material once and batch already-pending writes
  through the existing writer without weakening any required acknowledgement.
- **Owner/files:** runtime/scheduler/durability owner; isolated test files only.
- **Primary metric:** full-path wall/CPU, flush latency, open-ingress queue
  recovery, event-owner blocking.
- **Comparator:** writer stall/failure, partial write, disk full, clean and
  interrupted shutdown, incomplete-tail recovery, chain/checkpoint parity,
  delayed durability acknowledgement.
- **Dependency impact:** no second writer, weaker SQLite setting, fsync removal,
  or risk-ledger batching shortcut.
- **Acceptance:** common gate; if writer is not the bottleneck or buffering merely
  relocates latency, disposition is not applicable or parked.
- **Attempts used:** 0.

## Composite rule

Individually accepted candidates must be retested together against `BASELINE_R`.
Their gains are not additive by assumption. Any candidate that becomes
redundant, interacts adversely, changes the active scheduler path, or no longer
earns its complexity is removed before the final source freeze. Unsuccessful
prototypes and optional dependencies remain outside the qualifying import/hash
graph.
