# Caelum review — LSC default-off integration

Review time: `2026-09-22T18:41:07Z`

Role: `beelzebub_admin` concurrent specialist

Initial reviewed commit: `f44e747e3fb7d161b154c251e9af6774b2dc3829`

Final corrected commit: `99f43c5d44cfc7c583c77676e2a3a6a6b86048af`

Final corrected tree: `3cf03f74482e185e4e56101b7c38965870973b96`

Final corrected parent: `f44e747e3fb7d161b154c251e9af6774b2dc3829`

## Disposition

- **PASS_LSC_DEFAULT_OFF_INTEGRATION_ACCEPTANCE**, narrowly: the corrected source
  integration is default-off, attaches after authenticated listener admission,
  covers all ordinary routing branches, performs canonical work on a separate
  worker, leaves the authority ledger/runtime blobs unchanged, and closes after
  the authority ledger on the tested clean shutdown path.
- **PASS_LSC_SUPPORTED_UNCLEAN_RECOVERY_PRIMITIVE**, narrowly: the corrected
  source refuses live or unknown prior-process state, preserves the failed
  active marker, recovers and verifies the sealed prefix, keeps the interrupted
  run `proof_valid=false`, and permits a new clean run only afterward. One
  external subprocess kill/recovery/continuation probe passes.
- **HOLD_LSC_EXTERNAL_CRASH_BOUNDARY_MATRIX_NOT_PROVEN**: the single probe kills
  after two records are processed, with the first sealed/committed and the
  second retained only as a possible unsealed tail. It does not cover every
  required write, flush, rename, segment, commitment, or completion-marker
  boundary.
- **HOLD_LSC_OPERATIONAL_DESTINATION_AND_IDENTITY_NOT_SELECTED**: the integration
  supplies only a factory seam. No production factory, resolved N: destination,
  source/build/session/generation context, or disjoint operational-ledger path
  is selected or runtime-verified.
- Full representative parity, 2,111/s performance, installation, loading,
  runtime exercise, preflight, and qualification all remain
  **HOLD / NOT PROVEN**. Exact-final-HEAD Full is now **PASS**.

This report releases no runtime, ledger, NinjaTrader, deployment, benchmark,
paper-start, latch-recovery, arming, order, or qualification action.

## Objective, authority, and assignments

Joseph's controlling objective is an LSC v1 shadow-first implementation, with
Sol as sole integrator and sole runtime, deployment, benchmark, NinjaTrader,
and ledger operator. Caelum owns this one dated administrative review only.
The applicable repository `AGENTS.md`, Caelum `CHARTER.md`, Caelum
`CONCURRENCY.md`, the Atlas continuation handoff, the pre-edit requirements
matrix, the frozen integration diff, and the supplied focused receipt were
read. No implementation or test file was edited by Caelum.

Worktree: `N:\Beelzebub\worktrees\atlas-lsc-current-20260922`

Branch: `codex/lsc-v1-current-20260922`

HEAD at review start: exact assigned commit `f44e747...`

Local differences at review start: none

Diff scope: `src/copytrade/control_center.py`, new
`src/lsc/shadow_runtime.py`, new `tests/lsc/test_shadow_runtime.py`, and
`tests/test_l3g_paper_control_center.py` only. `git diff --check` was clean.
Sol then corrected and froze the reviewed defects plus bounded parity/crash
tooling at `99f43c5...`; Caelum re-reviewed the correction diff and immutable
receipts. The worktree contained only this untracked report after that freeze.

## Evidence reviewed

The final corrected focused receipt is:

`N:\Beelzebub\lsc-v1-tests-20260922\current-integration-corrected-focused\results.xml`

- Independently recomputed SHA-256:
  `6DC14CD61648EB715F63CBAE547F622699DCD7DD6E3D50DBF9FA714DCB6F2976`.
- JUnit reports 175 total pytest nodes, zero failures, zero errors, zero
  skipped, in 16.029 seconds. This is consistent with the parent's
  classification of 121 cases plus 54 subtests.
- Caelum hashed and parsed this receipt but did not rerun the suite. The receipt
  is focused source evidence, not a performance, installation, runtime, or
  qualification receipt.

Additional bounded receipts:

- Exact-head whole-repository Full:
  `N:\Beelzebub\lsc-v1-tests-20260922\parent-current-full-integrated\results.xml`,
  SHA-256
  `B4B761EF30B1E109DD63E72C2DA66522DA74473F48A149A3F562779FF670EA56`.
  The parent command was
  `python -m pytest -q --junitxml=N:\Beelzebub\lsc-v1-tests-20260922\parent-current-full-integrated\results.xml`.
  On exact commit/tree `99f43c5...` / `3cf03f7...`, pytest reports 1,707
  cases plus 608 subtests, 2,315 total nodes, zero failures, zero errors and
  zero skipped in 280.09 seconds (JUnit suite time 279.982 seconds). Caelum
  independently recomputed the receipt hash and parsed those zero-failure
  counts; the exact-head Full HOLD is released.
- External subprocess probe:
  `N:\Beelzebub\lsc-v1-tests-20260922\external-crash-probe-20260922T1900Z\report.json`,
  SHA-256
  `62A57D08969F386020BB46134E332328355E6C02534539EE6CBFC57A14C7855B`,
  result `PASS`. The child was forcibly killed after two authenticated records
  were processed: the first had sealed/committed and the second could remain an
  unsealed lost tail. Recovery preserved an interrupted-run false proof,
  confirmed prior PID absence, retained the one verified segment/commitment
  prefix, and a fresh continuation closed with `proof_valid=true`. This is one
  external probe, not the required crash-boundary matrix.
- Retained authenticated corpus full-gate report:
  `N:\Beelzebub\lsc-v1-tests-20260922\retained-observation-parity-20260922\parity-full-gate-report-v2.json`,
  SHA-256
  `9D5A0834980D30588F74F1838C67D7FB1D530FCC9FCA2E0C5CF9E09A5476EAD1`,
  result `FAIL` as required by missing proof. All 1,885 events match for count,
  order, sequences, payload types/hashes, timestamps, provenance, and depth
  checkpoints. Both sides lack evidence families, hypotheses, decisions,
  reason codes, final replay state, and final replay hash, so full parity is not
  established.

Unchanged authority/source blobs at the reviewed commit:

| File | Git blob |
| --- | --- |
| `src/l3g_paper/ledger.py` | `1eefcf28d2f94450871228af007f273e7f8aae4d` |
| `src/l3g_paper/runtime.py` | `9b1761bd8167620c218b891cc7e864b190061f70` |
| `src/l3f_provider/ninjatrader_observation.py` | `03ef14e33129f4c1e5ca0eb7f8c2ac2a7b8332d9` |

## Requirement and safety review

### Authenticated admitted-path placement — PASS, source

`LoopbackNinjaTraderBridge.accept_observation` decodes, verifies the L3F3 HMAC
attestation and expected source fingerprint, and calls
`NinjaTraderSessionLedger.admit` before returning a record. The listener worker
forwards only that returned, non-duplicate, monotonically admitted record to
`collect_and_fanout_observation`.

The LSC offer is the first operation inside that composite callback, before
collector/Pong/fanout routing. Therefore the collection-only and paper-only
branches cannot bypass LSC, while rejected and duplicate frames do not enter
it. The worker independently rejects anything lacking current verified L3F3
identity. This is the correct semantic tee point for every admitted passive
QUOTE, TRADE, and DEPTH MEVE.

### Listener handoff — PASS, corrected source and focused test

The normal `LSCShadowRuntime.submit` path performs a passive-kind/type check,
reads the monotonic clock, calls bounded `Queue.put_nowait`, and updates bounded
counters while holding its state lock. Encoding, context validation,
compression, segment publication, commitment writes, chain verification, and
failure-receipt I/O run on the worker. The saturation test demonstrates a
non-waiting false return and invalid proof when the queue is full.

The initial commit synchronously invoked `LOGGER.exception` after an unexpected
adapter exception. Caelum held that path because a logger handler may block or
write to disk. Commit `99f43c5...` removes that call: the callback now only
latches in-memory failure state, and the worker owns the durable failure
receipt. The corrected focused test injects a raising adapter, observes bounded
return, closes the worker, and verifies `LSC_SHADOW_CALLBACK_FAILED` plus a
durable false-proof receipt. Source flow then continues into the pre-existing
collector/Pong/fanout dispatch because `_offer_lsc_shadow` returns normally.

The integration acceptance defect is released at `99f43c5...`. Performance
latency and capacity remain separately unproved.

### Contract and source identity — PASS codec/runtime checks; operational selection HOLD

The selected test runtime binds native `MNQ DEC26` separately from canonical
`MNQZ6`, tick size `0.25`, profile, journal, L3F3 schema, provider/environment,
producer source/build fingerprints and timestamp, session, generation, and
resolved storage identity. Native-contract substitution is refused. Once a
context is bound, codec/store validation prevents a different identity from
joining the chain.

The shared app does not itself construct a production `LSCShadowConfig`; the
feature exists only when an explicit factory is supplied. That preserves
default-off behavior, but it also means DEC26, N: storage, and current runtime
identity are test-selected rather than installed or loaded facts.

**Release condition:** Sol freezes one explicit default-off production
configuration whose resolved segment and shadow-journal roots are on the
authorized N: destination and provably disjoint from the operational
`PaperLedger`; startup/status evidence must show exact DEC26, profile,
source/build/session/generation, store, and journal identities. The selected
destination—not C: or a test fixture—must be the path measured for capacity.

### Run markers and recovery — PASS supported primitive; boundary matrix HOLD

Startup creates an exclusive, fsynced `runtime-state/active.json`. Clean close
verifies the segment and commitment chains, writes a content-addressed
completion marker, and archives the active marker. A pre-existing active marker
causes `LSC_SHADOW_UNCLEAN_PREVIOUS_RUN` before acceptance, which correctly
prevents an interrupted run from masquerading as clean.

Commit `99f43c5...` adds an explicit `recover_unclean` administrative
transition. It validates marker/config identity, proves the prior PID absent,
fails closed for live or unknown process state, recovers/quarantines store
temporaries, verifies the sealed chain, commits a sealed-but-uncommitted
prefix, writes an immutable `proof_valid=false` interrupted-run receipt, and
archives the active marker before a new runtime may start. The live-process
test proves refusal without marker mutation. The external subprocess probe
forcibly kills the writer, recovers the false-proof interrupted run, and proves
a clean subsequent continuation.

This releases the missing supported recovery primitive. It does not release
the mandatory crash matrix: the one probe kills after two records are
processed, leaving one committed prefix and one possible unsealed tail. It
does not discriminate crash
before/during write, after data flush, before/after atomic rename,
before/after commitment, segment-without-commitment,
commitment-without-segment, or completion-marker boundaries.

**Remaining release condition:** run external-process termination at every
declared publication/commitment/completion boundary on the exact candidate.
Require no false clean marker or seal, no missing/duplicate/reordered durable
prefix event, truthful unsealed-tail loss, preserved quarantine artifacts, and
independent verification of every result.

### Authority and operational-ledger independence — PASS, narrow source/test

The LSC package does not import or write `PaperLedger`, and the authority
ledger/runtime source blobs are unchanged. In the tested normal lifecycle,
LSC starts before listener sink attachment, each admitted passive observation
is offered exactly once, authority runtime/transport stop and `PaperLedger`
close finish before `lsc_shadow.close`, and an LSC close exception is caught
after authority-ledger closure. Thus LSC drainage cannot delay the authority
ledger's own close on that path.

When the independent authority watchdog cannot establish safe shutdown, the
existing control center deliberately leaves the authority transport/writer
alive and also does not close LSC; the test confirms that state. It is a HOLD
state, not a clean LSC receipt. Failures before or during authority-ledger close
may likewise leave the daemon/active marker for later supported recovery. This
preserves authority precedence but reinforces the external recovery HOLD.

No current runtime or ledger was opened, measured, migrated, or modified in
this review. Operational-ledger path, capacity, trusted tip, WAL, account,
position, working orders, and arming state are **UNKNOWN** here.

## PASS/HOLD matrix

| Item | Disposition | Evidence / release condition |
| --- | --- | --- |
| Default off | **PASS, source** | `lsc_shadow_factory=None`; ordinary launch calls supply no factory. |
| Post-authentication/post-dedup placement | **PASS, source** | Listener bridge verifies and admits before callback; LSC precedes route branches. |
| Every admitted passive route offered once | **PASS, focused synthetic integration** | One composite hook before all Pong/legacy branches; lifecycle test observes one submit. Representative corpus still required. |
| Normal callback has no codec/filesystem/fsync | **PASS, source/focused** | `submit` is bounded in-memory enqueue/counters; worker owns canonical/durable work. |
| Exceptional callback cannot block authority | **PASS, corrected source/focused** | `99f43c5...` removes synchronous logging; callback only latches memory state and worker writes failure receipt. |
| Mixed-kind exact ordering and deterministic clean drain | **PASS, focused synthetic** | Three-kind replay and close receipt pass; full required parity remains HOLD. |
| DEC26/canonical/source/build/session/generation binding | **PASS in runtime code; HOLD operational selection** | Test config and context checks pass; no production factory, N: root, installed or loaded identity. |
| Separate shadow persistence | **PASS, source/focused** | Independent segment/journal roots; no SQLite under test LSC root; authority blobs unchanged. Prove resolved operational roots disjoint before use. |
| Clean shutdown authority precedence | **PASS, focused normal path** | Runtime/transport/ledger close precede LSC close. |
| Supported unclean-run recovery | **PASS, narrow source/focused/external probe** | Identity/PID guard, false-proof receipt, marker archive, prefix verification and clean continuation pass. |
| Full external crash-boundary matrix | **HOLD** | One committed-prefix/unsealed-tail subprocess kill passes; every required write/flush/rename/commit/completion boundary is not covered. |
| Exact-head Full | **PASS** | 2,315/2,315 nodes clean on exact commit/tree `99f43c5...` / `3cf03f7...`; immutable receipt hash verified above. |
| Representative event-level parity | **PASS, retained 1,885-event corpus** | Count/order/sequences/payload types and hashes/timestamps/provenance/depth checkpoints match exactly. |
| Full required parity | **HOLD** | The report correctly fails because evidence families, hypotheses, decisions, reason codes, final replay state and hash are absent on both sides. |
| 2,111/s performance | **HOLD / UNKNOWN** | No like-for-like current benchmark. Preserve at least three sustained and three burst repeats, stable queues, zero loss/gaps/overflow, latency/storage/drain/recovery metrics. |
| Integrated | **YES, accepted default-off source at `99f43c5...`** | Callback isolation and supported unclean recovery corrections pass focused review; operational proof gates remain separate. |
| Installed / loaded / runtime exercised | **NO / NO / NO** | Not performed or evidenced. |
| Asia preflight / qualification | **NOT STARTED; HOLD** | Remaining crash, full-parity, performance, install/load, and fresh operational gates must pass first. |

## Superior review of Adam admissions

Superior review time: `2026-09-22T19:06:19Z`

Adam report:
`docs/administration/adam/passes/20260922-190205Z-lsc-asia-admissions/ADMISSIONS_REPORT.md`

Independently verified Adam report SHA-256:
`00D3E080E1EC63FBE29617FAC964BF81A7BE96A29C4DF3B570AD452007616A1C`

Disposition: **AFFIRM_ADAM_ADMISSIONS_REPORT_NO_SUPERSESSION**.

Caelum affirms Adam's admissions decision and does not supersede it. Adam
correctly admits only the frozen default-off source integration, exact-head
regression state, narrow supported recovery primitive, and 1,885-event
event-level parity. Adam also correctly refuses to promote the full-gate parity
artifact, single external crash probe, available N: space, closed ports, or
historical operational-ledger metadata into runtime or qualification proof.

The report's commit/tree, source blob identities, focused and Full receipt
hashes/counts, crash receipt, parity result, installed/loaded state, and
release conditions agree with Caelum's independently reviewed evidence. Its
port, capacity, database-size, and WAL statements are expressly timestamped
point-in-time observations. They do not establish current account, position,
orders, reconciliation, arming, trusted-tip, installed, loaded, or runtime
state; Adam correctly leaves those states unknown.

The following remain controlling and are neither weakened nor released by
Adam's review:

- `HOLD_LSC_FULL_PARITY_NOT_PROVEN`
- `HOLD_LSC_EXTERNAL_CRASH_BOUNDARY_MATRIX_NOT_PROVEN`
- `HOLD_LSC_OPERATIONAL_DESTINATION_AND_IDENTITY_NOT_SELECTED`
- `HOLD_LSC_2111_CAPACITY_NOT_PROVEN`
- `HOLD_LSC_ASIA_QUALIFICATION_NOT_PROVEN`

Sol remains the sole integrator and sole runtime, ledger, NinjaTrader,
deployment, benchmark, operational recovery, and qualification operator.
Neither Adam's report nor this superior affirmation authorizes installation,
loading, runtime start, supported operational-ledger latch recovery, preflight,
paper start, arming, or orders.

## Qualification release conditions

The narrow shared integration acceptance HOLD is released at `99f43c5...`.
Qualification remains independently held until that exact candidate also has:

1. independently verified full native-corpus parity with zero mismatch or
   loss, including evidence families, hypotheses, decisions/reasons and final
   replay state/hash; the current 1,885-event result is event-level only;
2. external-process crash and recovery receipts across every declared
   boundary; the current one-point subprocess probe is insufficient;
3. at least three sustained and three burst like-for-like passes at the
   preserved 2,111 events/s floor, with non-growing queues, zero overflow,
   zero transport loss/gaps, bounded residence/tail latency, deterministic
   drainage, and separately reported storage/recovery costs;
4. an explicit, resolved N: LSC destination with adequate runway and proof it
   is separate from the unchanged operational ledger;
5. exact installed and loaded source/build/profile/route/DEC26 identity; and
6. a fresh Sol-operated preflight proving Sim101 identity, fresh continuous
   native data, reconciled flat position, zero working/owned/entry orders,
   trusted ledger tip, disarmed start, and live-capital authority denied.

Until all are complete on one unchanged candidate, the controlling result is:

`HOLD_LSC_ASIA_QUALIFICATION_NOT_PROVEN`

No elapsed clock, stale backlog, replay, legacy fallback, moved window,
threshold reduction, queue widening, synthetic proof, or fabricated
certificate releases that HOLD.
