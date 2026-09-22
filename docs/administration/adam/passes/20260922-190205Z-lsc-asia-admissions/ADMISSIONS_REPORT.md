# Adam admissions report — LSC continuation and Asia qualification

Pass ID: `20260922-190205Z-lsc-asia-admissions`
Opened: `2026-09-22T19:02:05Z`
Closed: `2026-09-22T19:04:55Z`
Parent integrator: `Sol (/root)`
Caelum record: `docs/administration/caelum/passes/20260922-184107Z-lsc-integration-review/LSC_INTEGRATION_REVIEW.md`
Adam report: `docs/administration/adam/passes/20260922-190205Z-lsc-asia-admissions/ADMISSIONS_REPORT.md`

## Decision

`HOLD`

One-sentence decision: `HOLD_LSC_ASIA_QUALIFICATION_NOT_PROVEN` — admit the exact
`99f43c5d44cfc7c583c77676e2a3a6a6b86048af` candidate only as a default-off
source integration with a supported narrow recovery primitive; do not install,
load, benchmark as qualification proof, start preflight, recover the operational
ledger latch, or begin the Asia qualifying window until the crash matrix, full
parity, 2,111/s repeat set, operational N: selection and fresh authenticated
safety state all pass on one unchanged candidate.

## Authority and scope

- Joseph's assignment: review the frozen Atlas LSC continuation candidate and
  the supplied focused, Full, crash, parity and operational-destination evidence
  for admission to the next transition.
- Adam's bounded scope: admissions review and this one report only.
- Caelum's superior review scope: the cited LSC default-off integration review;
  Caelum is superior to Adam and may revise or supersede this report.
- Parent integrator and runtime owner: Sol is the sole integrator and sole
  runtime, ledger, NinjaTrader, deployment and benchmark operator.
- Generic specialists and owners: implementation and test owners remain under
  Sol; Adam assigned no additional work and changed no application or test file.
- Explicit exclusions: implementation, test edits, installation, loading,
  runtime operation, ledger access or mutation, NinjaTrader operation,
  benchmark execution, latch recovery, arming, paper start, orders and
  qualification operation.

## Entry state

| Item | State | Evidence |
|---|---|---|
| Repository/branch/HEAD | **OBSERVED** `codex/lsc-v1-current-20260922` at `99f43c5d44cfc7c583c77676e2a3a6a6b86048af`; tree `3cf03f74482e185e4e56101b7c38965870973b96`; parent `f44e747e3fb7d161b154c251e9af6774b2dc3829` | Read-only `git rev-parse`, `git show`; worktree `N:\Beelzebub\worktrees\atlas-lsc-current-20260922` |
| Local modifications | **OBSERVED** no source/test modification; Caelum's dated report was the sole pre-existing untracked path before Adam opened this pass directory | `git status --short` at review entry |
| Account/position/orders/arming | **UNKNOWN** current authenticated state; no current endpoint or receipt proves Sim101, flat, zero working/owned/entry orders, reconciliation, or disarmed state | No listeners at `2026-09-22T19:02:03.2010703Z`; historical ledger file metadata is not current account evidence |
| Runtime/listeners/processes | **OBSERVED** no listener on `8090`, `48135`, `48136`, or `48137`; **UNKNOWN** installed/loaded LSC identity and current runtime safety state | Read-only `Get-NetTCPConnection -State Listen` at `2026-09-22T19:02:03.2010703Z` |
| Candidate identity | **OBSERVED** exact frozen source candidate and unchanged authority/source blobs | Commit/tree above; `ledger.py` blob `1eefcf28d2f94450871228af007f273e7f8aae4d`, `runtime.py` blob `9b1761bd8167620c218b891cc7e864b190061f70`, `ninjatrader_observation.py` blob `03ef14e33129f4c1e5ca0eb7f8c2ac2a7b8332d9` |
| Operational storage | **OBSERVED** N: has `225612668928` available bytes; operational DB is `1769721856` bytes and its WAL exists at zero bytes; **UNKNOWN** current trusted tip and reconciliation | Read-only filesystem observation at `2026-09-22T19:02:03.2010703Z`; `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\hot\lane_iii_paper.sqlite3` |

## Transition decisions

| Transition | Required evidence | Result | Owner / release condition |
|---|---|---|---|
| Frozen candidate to default-off shared source integration | Exact identity, post-authenticated placement, nonblocking authority callback, separate persistence, default-off state, authority-source invariance | **ADMITTED**, narrowly at `99f43c5...` | Sol keeps the feature default off and the candidate unchanged; Caelum may supersede |
| Source candidate to exact-head regression acceptance | Full suite on the frozen candidate with zero failures/errors/skips | **ADMITTED** | Exact-head Full receipt has 2,315/2,315 clean nodes |
| Recovery primitive to supported narrow source acceptance | Fail-closed unclean marker handling, absent-PID proof, interrupted false-proof receipt, verified durable prefix, clean subsequent continuation | **ADMITTED_WITH_CONDITIONS** | Primitive is accepted only as source/test evidence; Sol must complete the external crash-boundary matrix before operational use |
| Retained corpus to event-level parity | Exact count/order/sequences/payload types and hashes/timestamps/provenance/depth checkpoints | **ADMITTED**, event-level only | Preserve the 1,885-event receipt and do not relabel it full parity |
| Event-level parity to full parity | Evidence families, hypotheses, decisions, reason codes, final replay state and final replay hash on both sides | **HOLD** | Sol or assigned implementation owner must produce an independently verified zero-mismatch full-gate receipt |
| Narrow crash probe to full recovery qualification | External termination at every declared write/flush/rename/commit/completion-marker boundary with truthful prefix/tail/quarantine proof | **HOLD** | Sol must run and independently verify the complete boundary matrix on this exact candidate |
| Default-off source to operational N: selection | Explicit production factory, resolved N: segment/journal roots, actual-destination capacity measurement, disjointness from PaperLedger, bound DEC26/profile/source/build/session/generation identity | **HOLD** | Sol must select and prove the supported configuration; available N: capacity alone does not select or load it |
| Candidate to 2,111 events/s capacity gate | At least three sustained and three burst like-for-like passes, stable/non-growing queues, zero overflow/loss/gaps, bounded residence/tail latency, deterministic drainage and separately reported storage/recovery costs | **HOLD / UNKNOWN** | Sol is sole benchmark operator; no qualifying repeat set was supplied |
| Source to installed/loaded/runtime exercised | Exact deployed and loaded commit/build/profile/route/DEC26/storage identities and fresh runtime evidence | **HOLD** | Sol only after all prior offline gates pass; no installation or load is evidenced |
| Runtime to supported latch recovery | Current authenticated flat, zero orders, reconciled state plus verified actual ledger destination and trusted tip | **HOLD** | Sol only; ports are closed and current state is unknown, so recovery is not admissible |
| Candidate to Asia preflight/qualification | All preceding gates plus fresh continuous native data, disarmed Sim101, live-capital denial, fixed-window timing and exact closeout proof | **HOLD** `HOLD_LSC_ASIA_QUALIFICATION_NOT_PROVEN` | Sol may proceed only when every release condition is complete on one unchanged candidate before the controlling cutoff; otherwise abort without moving the window |

## Evidence register

| Evidence | Classification | Result |
|---|---|---|
| `N:\Beelzebub\lsc-v1-tests-20260922\current-integration-corrected-focused\results.xml` | **OBSERVED**; independently SHA-256 hashed and JUnit parsed | SHA `6DC14CD61648EB715F63CBAE547F622699DCD7DD6E3D50DBF9FA714DCB6F2976`; 175 total nodes, zero failures/errors/skips, 16.029 s; consistent with 121 cases plus 54 subtests |
| `N:\Beelzebub\lsc-v1-tests-20260922\parent-current-full-integrated\results.xml` | **OBSERVED**; independently SHA-256 hashed and JUnit parsed | SHA `B4B761EF30B1E109DD63E72C2DA66522DA74473F48A149A3F562779FF670EA56`; 2,315 total nodes, zero failures/errors/skips, 279.982 s; consistent with 1,707 cases plus 608 subtests |
| `N:\Beelzebub\lsc-v1-tests-20260922\external-crash-probe-20260922T1900Z\report.json` | **OBSERVED**; independently SHA-256 hashed and parsed | SHA `62A57D08969F386020BB46134E332328355E6C02534539EE6CBFC57A14C7855B`; `PASS`, one kill point only, interrupted proof false, one verified durable segment/commitment prefix and possible unsealed-tail loss, clean continuation proof true |
| `N:\Beelzebub\lsc-v1-tests-20260922\retained-observation-parity-20260922\parity-full-gate-report-v2.json` | **OBSERVED**; independently SHA-256 hashed and parsed | SHA `9D5A0834980D30588F74F1838C67D7FB1D530FCC9FCA2E0C5CF9E09A5476EAD1`; 1,885/1,885 event-level match but `FAIL` full gate because both sides lack proof fields |
| `docs/administration/caelum/passes/20260922-184107Z-lsc-integration-review/LSC_INTEGRATION_REVIEW.md` | **OBSERVED**, superior review | Caelum passes corrected default-off integration and narrow recovery primitive; holds crash matrix, operational selection, full parity, performance, install/load/runtime and qualification |
| Current N: and ledger filesystem metadata | **OBSERVED**, read-only | `225612668928` N: available bytes; DB `1769721856` bytes; zero-byte WAL; proves destination capacity facts only, not ledger truth or safety state |
| Current flat/zero-orders/reconciled/disarmed state | **UNKNOWN** | Runtime ports are closed and no current authenticated receipt was supplied |
| Full crash-boundary matrix | **UNKNOWN / MISSING** | One probe cannot establish all required publication and completion boundaries |
| 2,111/s repeated capacity result | **UNKNOWN / MISSING** | No current three-sustained plus three-burst receipt set |

## Holds and unresolved items

### `HOLD_LSC_FULL_PARITY_NOT_PROVEN`

- Action held: claiming full native-corpus parity or using parity to admit runtime.
- Evidence: full-gate report explicitly returns `FAIL` with missing evidence
  families, hypotheses, decisions, reason codes, final replay state and final
  replay hash on both sides.
- Consequence: the matching 1,885 event stream is only event-level proof.
- Release condition: zero-mismatch independent full-gate verification containing
  every required proof family.
- Owner: Sol or a parent-assigned implementation owner.
- Independent work that may continue: default-off source inspection and offline
  proof construction without changing thresholds or relabeling evidence.

### `HOLD_LSC_EXTERNAL_CRASH_BOUNDARY_MATRIX_NOT_PROVEN`

- Action held: operational recovery acceptance and qualification.
- Evidence: the single external probe proves one committed-prefix/unsealed-tail
  scenario, not every write, flush, rename, commitment or completion boundary.
- Consequence: false-clean, prefix, quarantine and boundary-specific behavior is
  not comprehensively established.
- Release condition: external-process termination and independent verification at
  every declared boundary on the exact candidate.
- Owner: Sol as sole runtime/recovery operator.
- Independent work that may continue: offline harness construction and review.

### `HOLD_LSC_OPERATIONAL_DESTINATION_AND_IDENTITY_NOT_SELECTED`

- Action held: installation, loading and runtime start.
- Evidence: available N: space and the historical operational-ledger path are
  observed, but no explicit production LSC factory/root or current bound identity
  is selected.
- Consequence: source tests cannot establish the actual runtime destination,
  separation, capacity measurement or loaded identity.
- Release condition: select resolved N: segment and shadow-journal roots disjoint
  from PaperLedger, measure those exact roots, and bind exact DEC26/profile/source/
  build/session/generation identities.
- Owner: Sol.
- Independent work that may continue: default-off configuration review.

### `HOLD_LSC_2111_CAPACITY_NOT_PROVEN`

- Action held: capacity admission and qualification preflight.
- Evidence: no current qualifying three-sustained plus three-burst repeat set.
- Consequence: callback correctness and test success cannot substitute for
  like-for-like sustained capacity.
- Release condition: all preserved throughput, queue, loss, gap, residence,
  drainage, storage and recovery criteria pass without queue widening or
  threshold reduction.
- Owner: Sol as sole benchmark operator.
- Independent work that may continue: offline benchmark preparation.

### `HOLD_LSC_ASIA_QUALIFICATION_NOT_PROVEN`

- Action held: Asia preflight, qualification, latch recovery, paper start,
  arming and orders.
- Evidence: full parity, full crash matrix, performance, operational selection,
  installation/loading and current authenticated flat/no-orders/reconciled state
  are incomplete or unknown.
- Consequence: there is no admissible qualifying hour on this evidence.
- Release condition: all gates pass on one unchanged candidate before the fixed
  cutoff, followed by the exact authorized fresh preflight and closeout chain.
- Owner: Sol; Caelum retains superior administrative review.
- Independent work that may continue: bounded default-off offline work only.

## Disagreements and policy gaps

- No material disagreement with Caelum. This report adopts Caelum's superior
  narrow source acceptance and remaining HOLDs. If Caelum later supersedes any
  finding, Caelum's dated record controls administration for this pass.
- The 1,885-event comparison cannot be interpreted as full parity: the governing
  artifact itself returns `FAIL` for missing proof fields.
- N: free space cannot be interpreted as an operational storage admission until
  the exact LSC destination is selected, measured and proved disjoint from the
  unchanged PaperLedger.
- Closed ports cannot be interpreted as flat, reconciled, disarmed or zero-order
  proof. Those states remain `UNKNOWN` until freshly authenticated.
- A focused or Full test pass does not establish runtime identity, capacity,
  recovery at all boundaries, or qualification authority.

## Closeout

- Implemented: **OBSERVED** LSC default-off shared integration and narrow unclean
  recovery primitive at frozen commit `99f43c5...`; Adam made no implementation
  change.
- Installed: **NO / NOT EVIDENCED**.
- Loaded: **NO / NOT EVIDENCED**.
- Exercised: **OBSERVED** focused synthetic tests, exact-head Full, one external
  crash probe and retained event-level parity only; no operational runtime
  exercise.
- Accepted: narrow default-off source integration, exact-head regression state,
  narrow source recovery primitive with conditions, and event-level parity only.
- Remaining live-only checks: exact installed/loaded identity, selected N: LSC
  destination, current Sim101/flat/zero-orders/reconciled/disarmed state, fresh
  continuous native data, trusted ledger tip, controlled latch recovery and
  exact qualifying-window closeout. None is authorized by this report.
- Smallest next action: without starting runtime, complete and independently
  verify the missing full-parity proof and full external crash-boundary matrix on
  the unchanged candidate; only then run the preserved 2,111/s repeat set before
  considering operational configuration or preflight.
