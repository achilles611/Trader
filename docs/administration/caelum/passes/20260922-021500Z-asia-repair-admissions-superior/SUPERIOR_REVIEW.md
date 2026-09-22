# Caelum superior review — Asia repair admissions

- Pass: `20260922-021500Z-asia-repair-admissions-superior`
- Closed: `2026-09-22T02:24:06Z`
- Parent integrator and sole runtime/ledger operator: Sol (`/root`)
- Candidate reviewed: `62583b6c9bf081563dae5835d469f87b56615128`, tree `e009c28c542426e5a19ff2bb4a9ada4fdc98511b`
- Adam report reviewed: commit `dd8eff7cc6df557456c187884bc3e99c53debf77`, SHA-256 `9208A74A491E36D3469B78FC359604D2AD2C0EFC527E78705FD9DBC3BDAB186D`
- Review mode: source, Git, receipt, result and current read-only host inspection. Caelum did not benchmark, install, reload, start or stop a runtime, mutate a ledger, invoke recovery, consume R2, arm, or place orders.

## Superior disposition

**`CONFIRM IN PART; MODIFY AND SUPERSEDE IN PART — OVERALL HOLD`**

Caelum confirms Adam's decisions on source retention, N:-rooted test/temp use, no ledger migration, latch recovery, ratio policy, natural-feed qualification, and all operational/trading boundaries. Caelum modifies Adam's service admission: the evidence releases the bounded host/collector **queue-residence subdefect offline**, but it does not satisfy the earlier Caelum release condition requiring both bounded collector age and no persistent open-ingress lag. The broader `HOLD_RECEIVER_COLLECTOR_SERVICE_DEFICIT` therefore remains active.

This is not a demand for the retired `2,111/s` target or a new `1,000/s` capacity floor. The observed defect is simpler: under the declared 1,000/s offered schedule, the receiver completed at `711.411/s`, finished `4.058 s` behind schedule, and forced substantial sender socket blocking. A later policy decision may define representative capacity acceptance, but it cannot erase the present matched-screen lag.

## Authority, scope and repository state

- **OBSERVED:** Joseph directed Adam and Caelum to review concurrently, made Caelum superior to Adam, and retained Sol as parent integrator and sole runtime/ledger operator.
- **OBSERVED:** Adam stayed within his assigned single-report directory. Caelum did not edit Adam's report.
- **OBSERVED:** the current branch is `codex/lane-iii-perpetual-corrective-20260906`. Adam's documentation commit follows candidate `62583b6`; no source/test repair change was added by Adam.
- **OBSERVED:** unrelated pre-existing Caelum audit and operations evidence modifications remain outside this pass. Sol's runbook revision is a separate uncommitted, Sol-owned change and was reviewed read-only.
- **UNKNOWN current:** authenticated Sim101 position, orders, reconciliation, loaded native identity, selected runtime profile, ledger verification and arming state. Offline synthetic receipts and historical flat evidence do not establish them.

## Independent evidence findings

| Finding | Classification | Evidence and decision |
|---|---|---|
| Exact bounded service integrity | **OBSERVED / PASS offline** | Candidate result SHA-256 `91392B67CD260E88BDC71166D91444A52EBEDF5F2993F1DE6911CA797A351ACE`: receiver, paper and durable collector each reached `10,000`; expected/receiver/paper/collector digest matched; rejection, duplicate, host/collector overflow, producer-overflow delta and transport-failure delta were all zero; final queues drained; ledger shutdown was clean; execution transport was absent. |
| Queue-residence symptom | **OBSERVED / RELEASED offline only** | Retained baseline `84e09db` recorded collector max age `1,141 ms`, host max age `141 ms` and collector high-water `841`. Candidate recorded `469 ms`, `94 ms` and `319`, below the unchanged `1,000 ms` freshness boundary. |
| Open-ingress service equilibrium | **OBSERVED / HOLD** | The same candidate completed in `14.057 s` against a `9.999 s` schedule, for `4.058 s` receiver end lag and `711.411/s`. Sender elapsed `10.563 s`, accumulated `3.994 s` in socket writes and had a `1.334 s` maximum write. Candidate completion and lag are slightly worse than the retained `84e09db` baseline (`720.169/s`, `3.887 s`). This is persistent upstream backpressure, not closed service equilibrium. |
| Focused regression | **OBSERVED receipt / PASS** | `N:\Beelzebub\asia-repair-readiness\yield16-full-tests-20260922T0328Z\unittest.log`, SHA-256 `517B1AA8D7E4D85573A2330502787E818DCF6C9A77A9FB00D447D29DE23FFACC`, reports 104 tests in 4.974 s, `OK`. Caelum attributes this to the retained receipt and did not rerun the suite. |
| Capacity destination | **OBSERVED / NO MIGRATION** | `PaperLedger._current_database_free_bytes()` uses `shutil.disk_usage(self.path.parent).free`; source floors remain 8 GiB warning and 4 GiB minimum. The operational ledger file exists on N: at `1,498,558,464` bytes. At `2026-09-22T02:21:47Z`, N: had `226,170,540,032` free bytes and C: had `4,731,461,632`. The C: refusal was the actual test-temporary ledger destination. |
| Ledger identity/history preservation | **INFERRED from traced configuration and retained ledger evidence** | Because the selected operational ledger is already on N:, moving, copying, resetting or recreating it is unnecessary and would introduce identity/epoch/anchor/history risk. No supported migration is warranted. |
| Current runtime and recovery proof | **OBSERVED unavailable / UNKNOWN state** | At `2026-09-22T02:21:47Z`, ports `8090`, `48135` and `48136` had no listener. Sol separately observed the status endpoints refuse connection. Therefore current authenticated flat/no-orders/reconciled state and a current Full proof are unavailable; no recovery call is admissible. |
| Periodic ratio policy | **OBSERVED / HOLD** | `src/l3g_paper/pong_periodic_telemetry.py` computes `delta_depth_events / delta_ordinary_completed_full_analyses` but emits `threshold=null` and `threshold_reason=POLICY_NOT_DEFINED`. Threshold value, direction, tolerance and minimum denominator remain unapproved. |

## Review of Adam's admissions report

### Confirmed

1. Retain the integrated source changes and their exact offline integrity proof.
2. Continue to use N: for eligible test, temp and evidence files while preserving capacity floors.
3. Reject C:-to-N: operational ledger migration as inapplicable because the selected ledger is already on N:.
4. Keep supported capacity-latch recovery held until current authenticated exact flat/no-orders/reconciled state, healthy current writer capacity and a Full verification through the stable tip exist; after recovery, require a second Full.
5. Keep periodic-ratio acceptance held until Joseph approves a versioned numeric policy before observation.
6. Keep R2, natural-feed qualification, Asia operational admission, Gates B-D, paper start, arming and orders outside this pass.

### Modified or superseded

1. Adam's closure of the prior service HOLD is narrowed to **queue-residence subdefect released offline only**. Caelum supersedes any reading that the broader receiver/collector service deficit is closed.
2. The final predeclaration's release rule—drained queues plus host/collector maximum age below 1,000 ms—was narrower than the earlier Caelum release condition, which also required no persistent open-ingress lag. Predeclaration before the final attempt does not authorize silently removing that earlier condition.
3. The runbook may be rebaselined to `62583b6` and the new receipt, but it must describe this distinction. Wording that releases the entire service defect is not accepted. Its manual, single-operator, disarmed, no-order safety boundaries remain valid.
4. Caelum's review-pending prerequisite is now satisfied. The substantive R2/natural-window blockers remain: unresolved open-ingress service, current live preflight, and this pass's explicit no-R2/no-natural-feed scope.

## Concrete HOLDs

### `HOLD_RECEIVER_COLLECTOR_SERVICE_DEFICIT`

- **Action held:** claiming complete receiver-service repair, consuming R2, or beginning a natural qualification epoch on the strength of this synthetic result.
- **Evidence:** candidate `62583b6` has bounded local queue residence but `4.058 s` receiver schedule lag, `711.411/s` completion and sender socket backpressure under the declared 1,000/s offered schedule.
- **Consequence:** the queue-age improvement is retained, but the load is still displaced upstream rather than proven sustainably serviced.
- **Release condition:** produce a new frozen, predeclared matched proof that preserves exact order/durability and unchanged loss/freshness floors, keeps queue age bounded, and shows no persistent receiver/sender lag under its declared applicable load; alternatively document a reproducible immutable limit and obtain an explicit policy decision without reverse-fitting the observed result.
- **Owner:** Sol coordinates the source owner; Sol remains sole runtime operator. Independent source analysis and N:-rooted offline tests may continue.

### `HOLD_LATCH_RECOVERY_CURRENT_STATE`

- **Action held:** calling the supported ledger-capacity recovery endpoint or changing ledger/latch artifacts.
- **Evidence:** operational ledger destination is healthy on N:, but no current runtime listener, authenticated bridge reconciliation, exact flat/no-orders snapshot or Full verification is available.
- **Consequence:** the durable capacity lockout and ledger chain remain unchanged.
- **Release condition:** exact current ledger/epoch binding, healthy current writer snapshot, provenance-valid authenticated bridge, complete flat/no-orders reconciliation, no newer fault, Full verification through the stable arm-snapshot tip, supported recovery, then a second Full.
- **Owner:** Sol only. Read-only prerequisite preparation may continue.

### `HOLD_PERIODIC_RATIO_POLICY`

- **Action held:** declaring a ratio pass/fail or periodic-ratio acceptance.
- **Evidence:** numeric threshold, direction, tolerance and minimum denominator are undefined; offline `R=2.0` is diagnostic only.
- **Consequence:** a future eligible fixed window may be recorded, but not judged retrospectively.
- **Release condition:** Joseph approves an attributable versioned policy before the observation window, then the window passes all identity, continuity, reconciliation and disqualifier rules.
- **Owner:** Joseph for policy; Sol for any later authorized capture; Caelum for readiness review.

### `HOLD_ASIA_OPERATIONAL_ADMISSION`

- **Action held:** natural-feed qualification in this pass, Gates B-D, paper start, arming and orders.
- **Evidence:** the service HOLD above remains; current operational state is unavailable; no fresh natural DEC26 epoch ran; latch and ratio prerequisites remain unresolved.
- **Consequence:** source remains implemented and offline-exercised, not installed/loaded/runtime-qualified.
- **Release condition:** resolve the service HOLD, rebaseline and independently review the runbook, obtain separate authorization for the manual diagnostic, pass current identity/account/flat/reconciliation/Full gates, and satisfy each later gate without weakening thresholds.
- **Owner:** Sol and Joseph within their existing authority. Documentation and evidence preparation may continue.

## Tuesday Asia diagnostic scope

The Tuesday runbook remains a preparation artifact, not execution authority in this pass. The current identity update to `62583b6` is appropriate, and the fixed window/no-rerun/manual/disarmed/no-order boundaries should remain. Before use it must accurately preserve the residual open-ingress HOLD, retain ratio output as diagnostic while policy is undefined, and receive a fresh official venue-schedule check and current preflight. R2 is still `0/1` consumed and must not be spent while this HOLD remains.

## Closeout

- **Implemented:** source lineage through `62583b6`.
- **Installed:** not established in this pass.
- **Loaded:** current state unknown; relevant listeners were absent at the recorded check.
- **Exercised:** exact offline synthetic integrity and queue-residence behavior; 104-test receipt retained.
- **Accepted:** N:-rooted test/temp routing, no-migration decision, source retention, and queue-residence improvement only.
- **Not accepted:** sustainable open-ingress service, representative capacity, natural-feed qualification, ratio acceptance, current flat/reconciled runtime state, latch recovery, R2, Asia operational admission, arming or orders.
- **Smallest next action:** repair or explicitly policy-resolve the demonstrated receiver/sender lag, then produce one new predeclared matched proof before any R2 or natural-feed work.
