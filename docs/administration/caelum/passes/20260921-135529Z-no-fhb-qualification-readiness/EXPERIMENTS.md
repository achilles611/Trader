# Experiment registry — no-FHB qualification readiness

`BASELINE_R` is sealed at `c76cccc2a3242f9bda00f4677648f589f70be317`
with observer source identity `585a57d4...fa2344`. No clean authoritative-
initialization corpus exists, so no performance timing is authorized. E1–E3
and E5–E7 are closed below from current-code/telemetry evidence. E4 was the only
applicable reliability prototype; it is now parked inconclusive and excluded
from the qualifying source.

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

- **Status:** `NOT_APPLICABLE_WITH_EVIDENCE`.
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
- **Disposition evidence:** baseline already parses once into one frozen typed
  observation, freezes nested JSON on ownership transfer, caches signed and
  unsigned canonical bytes plus wire hash, and fans that same admitted object
  to collector and host. No current telemetry isolates residual repeat
  representation work as a material bottleneck; a duplicate prototype would
  not test a distinct hypothesis.
- **Attempts used:** 0 implementation, 0 corrective, 0 warmup/screen/pairs.

## E2 — bounded batch drain and byte-stream framing

- **Status:** `NOT_APPLICABLE_WITH_EVIDENCE`.
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
- **Disposition evidence:** baseline native transport already drains a bounded
  maximum of 256 frames/524,288 characters into one ordered write outside the
  callback lock. The receiver consumes every complete newline frame in each
  bounded read and retains partial/oversize/controlled-shutdown semantics. The
  failed natural epoch attributes loss to absent receiver/transport availability,
  not missing batching or one-frame parsing.
- **Attempts used:** 0.

## E3 — sequence-owned pooled buffers or bounded ring storage

- **Status:** `NOT_APPLICABLE_WITH_EVIDENCE`.
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
- **Disposition evidence:** E1 and E2 mechanisms are already present, and no
  residual allocation/lock bottleneck is measured. Native callbacks are
  multi-producer into an existing bounded FIFO, so a single-producer ring premise
  is false; no new dependency or hand-written lock-free structure is justified.
- **Attempts used:** 0.

## E4 — collector/host lifecycle separation

- **Status:** `PARKED_INCONCLUSIVE`; fixed budget exhausted; not integrated.
- **Observed blocker:** last proof started with stale pre-start producer backlog
  and large failure deltas; receiver readiness and producer generation were not
  a clean authoritative boundary.
- **Hypothesis:** the smallest supported lifecycle extension around the existing
  independently owned collector removes receiver-downtime/stale-start coupling
  without introducing an always-on service or another analytical owner.
- **Owner/files:** `/root/python_r3` owns only new
  `src/l3g_paper/pong_lifecycle.py` and
  `tests/test_l3g_pong_lifecycle.py`. Sol owns any later integration/shared
  configuration and alone operates.
- **Primary metric:** reliability: listener-ready handshake, attributable new
  source boundary, lossless bounded capture across planned host stop/reattach.
- **Comparator:** source absent, host stop with collector retained, authoritative
  reattach, same-identity increases/regressions, new identity, disk-full/quota,
  cleanup, graceful drain; isolated sinks only.
- **Dependency impact:** no daemon, startup service, or order/control channel.
- **Acceptance:** reproducibly remove stale-start coupling with non-regressing
  admission/latency/durability. Prefer a smaller existing-collector lifecycle
  change over process splitting.
- **Initial review:** not accepted. The first prototype had an unbounded
  in-flight delivery wait and no authoritative collector/source-generation/
  sequence/durability checkpoint passed to the replacement host, so it could
  hang lifecycle control or attach mid-mutation without an attributable prefix.
- **Corrective result:** not accepted. The two untracked prototype files were
  `src/l3g_paper/pong_lifecycle.py` (695 lines, SHA-256
  `1196d21ff5d18f6b603d2bde130c38df16a03d898cee4df0bbc81de7af0fe474`)
  and `tests/test_l3g_pong_lifecycle.py` (542 lines, SHA-256
  `f3a435a539bf39f996a621ac9c6693c9ba09f9b3b45bf7e21e08bea3fbf39f53`).
  Compilation and no-index diff checks passed; focused tests were 11/13, with
  two stale expectations. The material failures are architectural: lifecycle
  ingress is held across real host startup; a continuously settled
  enqueued=written=durable boundary may not exist; and the collector has no
  durable-prefix per-stream source-sequence checkpoint for authoritative
  reattach. A second corrective is forbidden.
- **Disposition path:** preserve these hashes and findings, delete the two
  untracked prototype files, and keep them absent from the qualifying import/
  hash graph. Reopening requires a separately authorized architecture pass.
- **Attempts used:** one initial implementation and one corrective revision;
  no runtime, timing, warmup or paired runs.

## E5 — strict faster decoder alternatives

- **Status:** `NOT_APPLICABLE_WITH_EVIDENCE`.
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
- **Disposition evidence:** baseline E1 behavior is already present, no strict
  decoder dominance is measured, and neither named binary codec is an existing
  dependency. The stronger dependency gate therefore has no admissible trigger.
- **Attempts used:** 0.

## E6 — DBN-inspired compact internal record layout

- **Status:** `NOT_APPLICABLE_WITH_EVIDENCE`.
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
- **Disposition evidence:** the baseline already has a frozen typed observation,
  immutable nested payload and cached canonical material. No remaining
  representation/materialization cost is measured; another internal format
  would double-count E1 and a wire migration is outside this pass.
- **Attempts used:** 0.

## E7 — staged durability preparation with truthful barriers

- **Status:** `NOT_APPLICABLE_WITH_EVIDENCE`.
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
- **Disposition evidence:** the collector already has a bounded writer queue,
  buffered append, 1,024-record/bounded-interval flush policy, explicit fsync
  durable watermark and final drain. The failed epoch drained
  52,517/52,517/52,517 with collector overflow zero; its admitted failures were
  producer overflow and transport loss, not an attributed disk/lock stall.
- **Attempts used:** 0.

## Composite rule

Individually accepted candidates must be retested together against `BASELINE_R`.
Their gains are not additive by assumption. Any candidate that becomes
redundant, interacts adversely, changes the active scheduler path, or no longer
earns its complexity is removed before the final source freeze. Unsuccessful
prototypes and optional dependencies remain outside the qualifying import/hash
graph.
