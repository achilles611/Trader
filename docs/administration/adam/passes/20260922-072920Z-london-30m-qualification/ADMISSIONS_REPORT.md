# Adam admissions report — London natural-feed qualifying-hour closeout

Pass ID: `20260922-072920Z-london-30m-qualification`
Opened: `2026-09-22T07:29:20Z`
Closed: `2026-09-22T07:50:24Z`
Parent integrator and sole operator: `Sol (/root)`
Caelum record: `docs/administration/caelum/passes/20260922-045541Z-aggressive-expedited-recovery/OVERSIGHT.md` (historical entry state); current superior review pending
Adam report: `docs/administration/adam/passes/20260922-072920Z-london-30m-qualification/ADMISSIONS_REPORT.md`

## Decision

`HOLD`

One-sentence decision: the final exact qualifying hour `[2026-09-22T08:00:00Z,09:00:00Z)` was correctly **not started** and earned `0` qualified seconds because pre-window producer loss, transport failure, DEPTH sequence gaps, freshness breaches, receiver fail-closure, missing warm-up, retained capacity lockout and runtime Git mismatch violated the admission contract; no rerun, ratio/capacity claim, recovery, paper start, arming or order is admitted.

## Frozen interval and no-repair rule

- **Final sole governing interval:** `[2026-09-22T08:00:00Z, 2026-09-22T09:00:00Z)` / `[02:00,03:00) MDT` / `[09:00,10:00) Europe/London`.
- Repository session contract: `LONDON / EUROPE`, `Europe/London`, inside the half-open weekday session `[08:00, 11:30)`.
- The earlier `07:40Z–08:10Z` internal draft was rejected before freeze. The prior 30-minute `[08:00Z,08:30Z)` preparation artifact was superseded before start by Joseph's final one-hour objective. Neither earlier interval started or carries qualified time.
- The exact hour cannot be started late, shortened, extended, slid, trimmed, restarted, backfilled, relabelled, substituted with replay/diagnostic data, or rerun in this pass.
- `03-prestart-hold.json` was sealed at `07:45:00Z`, before the boundary, and records `started=false`, `qualified_seconds=0` and `HOLD_LONDON_QUALIFYING_HOUR_PRESTART_NATIVE_LOSS_AND_SEQUENCE_GAPS`. The no-repair rule therefore closes the pass without a collection attempt.

## Authority and scope

- Joseph's final assignment: Adam closes the same report against one exact London qualifying hour; Caelum is superior; Sol remains sole runtime, ledger, install, reload, recovery and window operator.
- Adam's bounded scope: establish the admission contract before the window, review routed evidence read-only, maintain exactly this report, and classify transitions without operating any system.
- Caelum's superior scope: review this contract and resulting evidence, and confirm, modify or supersede Adam's disposition.
- Adam's writable scope: only this report and its containing new directory.
- Explicit exclusions: no source, test, runtime, ledger, UI or NinjaTrader action; no install/reload/R2; no recovery call; no listener/process control; no account state change; no Gates B–D; no paper start; no arming; no native directive; no order.

## Entry state

| Item | State | Evidence |
|---|---|---|
| Repository/branch/HEAD | **OBSERVED** | Two-phase implementation `2331c744ccaa09d6644496142daa4682a0476f72`, tree `8b14f35e8c6bb4c3395dadf756a226803b37e6be`, preceded Adam/Caelum records. Prestart evidence named repository HEAD `15db37034bec3c4af504777f1e49c8dd5d4e9e50`, tree `ce120e257b930eae773b4c615a89d44c52ad1a78`, but the running binding reported Git `5998a8dd462f09e274183054c31ef6b34ee4535d`; `RUNTIME_GIT_IDENTITY_PRE_FREEZE_MISMATCH` was correctly disqualifying. |
| Local modifications | **OBSERVED** | Pre-existing unrelated Caelum audit and operations evidence paths were dirty/untracked. Adam will neither stage nor edit them. |
| Runtime/evidence tuple | **OBSERVED, not admissible** | Correct profile/architecture/topology/ledger tuple appeared, and the native source was `17995c...`, build `215e96...`, session `891602...`, generation `1`; however repository/runtime Git identity disagreed, transport disconnected, native receiver failed closed and continuity was invalid. |
| Account/position/orders/reconciliation/arming | **OBSERVED safe at prestart** | `READY_DISARMED`; Sim101/local simulation; runtime and broker flat quantity `0`; zero owned/entry orders; complete snapshots; reconciliation current; activation unauthorized; live capital denied. No paper start, arming or order occurred. |
| R2/install/load status | **OBSERVED, exhausted** | R2 was consumed `1/1`; no second attempt is permitted. `01-R2-source-preparation.json` proves source parity/compile preparation but is itself `verified=false`; the later authenticated runtime snapshot proves distinct producer source/build/session/generation. This success does not override the loss/continuity failures. |
| Ledger verification/latch/capacity | **OBSERVED closeout integrity; recovery not admitted** | The capacity latch remained `LEDGER_CAPACITY_INADEQUATE:DEGRADED`; no supported recovery completed. Supported shutdown closed at `07:45:45Z`, and Full passed at `07:47:26Z` through stable tip `334199`, `quick_check=ok`, no storage warnings, WAL `0`, N: free `225,697,296,384` bytes. This proves closeout integrity, not qualification or latch recovery. |
| Official venue availability | **UNKNOWN** | Sol must recheck the official CME schedule immediately before the window. Application-session eligibility does not prove venue availability. |

## Governing admission contract

### 1. Exact identity and provenance

Every pre-window, boundary, collector, persisted metric, shutdown and Full receipt must agree on:

- repository checkout commit/tree and executable/measurement source hashes;
- runtime Git identity, launch command, working directory and imported-module hashes;
- account `Sim101`, environment `LOCAL_SIMULATION`, live capital `DENIED`, maximum quantity `1`;
- native/canonical instrument `MNQ DEC26 / MNQZ6`;
- selected profile `BEELZEBUB_PONG_DISCERNMENT_V1`;
- qualifying architecture `FULL_NATIVE_DEPTH_PERIODIC_20MS_NO_FHB_V1`;
- topology version/hash, state namespace and experiment identity;
- scheduling `PERIODIC_FLAT_PROTECTED` at 20 ms with FHB absent/inactive and no helper;
- exact ledger path, audit root, ledger identity, epoch, verified tip and risk-continuity anchor;
- installed and loaded native source/build fingerprints, execution session, authenticated source session/build/fingerprint and one distinct stable source generation; and
- one evidence root on N: with artifact hashes.

Blank, mixed, stale, mismatched, changing or inferred identity makes the interval ineligible. A NinjaTrader process, a listener, callbacks or an installed source file alone is not loaded provenance proof.

### 2. Pre-window Full, recovery and safety sandwiches

Before `08:00:00Z`, Sol must obtain:

1. a current binding/safety snapshot;
2. a supported Full verification passing through the stable current ledger tip with `quick_check=ok`, chain/risk guard valid, no unverified tail and no WAL/checkpoint defect;
3. a second binding/safety snapshot with no identity drift; and
4. current collector/host/native/storage baselines immediately before the fixed boundary.

Each safety snapshot must be authenticated and show exact Sim101/local-simulation binding, broker and runtime position flat, zero owned/working/entry/foreign orders, complete position/order snapshots, current reconciliation, no unresolved execution or pending native directive, runtime disarmed/not operationally started, activation disabled, live capital denied, valid AddOn provenance, and no safety lockout except the specifically handled ledger-capacity latch.

If the capacity latch remains set, recovery is permitted only after the first Full and a clean current sandwich prove the same ledger identity/epoch, healthy actual destination, exact flat/no-orders and authenticated current reconciliation. Only the supported recovery route may be used. It must return `recovered=true`, `operational_paper_started=false`, `armed=false` and `new_full_verification_required=true`. Sol must then complete a second passing Full through the new stable tip and another clean no-drift safety sandwich before `08:00:00Z`. Any refusal, ambiguity, manual latch/database edit, missing second Full or unfinished recovery at the boundary yields `WINDOW_NOT_STARTED / HOLD`.

### 3. Capacity destination and runway precondition

- Test/temp/evidence artifacts must use eligible N: roots.
- The operational ledger's actual parent volume must be sampled through the selected path, not process CWD or an unrelated temp root.
- Existing 8 GiB warning, 4 GiB minimum and 24-hour projected-runway requirements remain unchanged.
- The writer must be `HEALTHY`, warning-free, with causal post-repair growth telemetry and projected runway at or above 24 hours. Static free space or an earlier stopped-state Full is insufficient.
- No ledger migration, replacement, copy, new identity or history reset is admitted. If the selected operational ledger is already on N:, migration remains inapplicable.

### 4. Authentic London start rule

At the `08:00:00Z` boundary, all preconditions above must already pass. The session resolver must return the exact current `LONDON / EUROPE` generation and trade date under `Europe/London`; official venue availability must be confirmed; required listeners and the distinct authenticated native producer must already be stable; and the collector/host must already be running disarmed with zero queues or a fully reconciled bounded pre-boundary warm-up state.

Only source events whose authentic source/receipt times fall in `[08:00:00Z,09:00:00Z)` could belong to the final qualification interval. Pre-boundary warm-up may establish readiness but cannot be counted as interval evidence. If the first attributable event/counter snapshot is late, the interval fails; no partial-hour claim is allowed.

### 5. Continuous natural-feed collection requirements

For the entire half-open interval, without restart, reconnect or generation reset:

- retain every authenticated DEPTH, QUOTE and TRADE update and every durability acknowledgement;
- require continuous per-stream source sequences, with zero gaps, duplicates or out-of-order records;
- require zero new producer overflow, producer transport failure, receiver/collector rejection, receiver duplicate, collector overflow, host overflow, failed required-consumer delivery or unexplained missing event;
- require source receipt age, event processing age, host queue age and collector queue age each remain strictly below the existing `1,000 ms` boundary throughout, not merely at endpoints;
- require bounded queues with no persistent positive growth, no queue-capacity approach and no stale backlog carried across the end boundary;
- require depth to be coherent exact `10 x 10` whenever available, or to use the explicit canonical unavailable state with attributable reset/recovery semantics; never synthesize levels or treat `10 x 1` as `10 x 10`;
- require current-session warm-up and every profile-required evidence family to become available from authentic London observations; no inherited Asia/New York generation, replay, fixture or synthetic history may satisfy warm-up;
- require identity continuity and current flat/reconciled/disarmed/no-order state throughout; any exposure, intent, order, directive, unresolved execution, reconciliation loss, provenance loss, protection-state transition or source-generation change disqualifies the whole interval; and
- persist periodic/host/collector metrics at the start, bounded checkpoints and end, including identities, cumulative counters, freshness maxima, queue depths/high-water/ages, loss/failure counters, canonical-depth state, warm-up state, scheduler reconciliation and protection/disqualifier counters.

An explicit unavailable-depth interval may prove honest collection and continuity semantics, but it cannot satisfy canonical-depth/warm-up requirements needed for the full London qualification. It must be reported as unavailable, not passed by interpretation.

### 6. Disqualifier rule

Any missing prerequisite or any in-window violation above immediately classifies the fixed interval `NATURAL_FEED_WINDOW_INELIGIBLE`. Collection may continue only for safe closeout/evidence preservation; the failing span cannot be trimmed, repaired by later recovery, restarted, relabelled or combined with another span. A process crash, listener restart, reconnection, new source session/generation, counter reset, time reversal, metric gap or missing end receipt is a full-window failure.

### 7. End, durable drain and closeout rule

Had the interval started, at `09:00:00Z` Sol would have captured the exact end boundary before any reset or restart. Because the start gate failed before `08:00Z`, the applicable closeout was immediate supported shutdown, durable preservation and final Full. The following requirements remain the standard for any future separately authorized pass:

- receiver admitted count, paper/host accepted count, collector enqueued/written/durable count and per-stream counts must reconcile exactly for the governed scope;
- expected/receiver/paper/collector ordered digests must match where the instrumentation defines them;
- all required queues and compute work must drain to zero without loss, coalescing or suppressing safety incidents;
- the collector journal and status must be fsync-durable through the final interval record;
- host, collector, listener, paper runtime and ledger must stop cleanly through supported paths; no force kill;
- final safety proof must remain Sim101 flat, zero orders/directives/executions/fills, reconciled, disarmed and live-capital denied;
- final Full verification must pass through the stable post-close tip with no unverified tail; and
- the evidence root must contain hashes for the predeclaration, all boundary/checkpoint receipts, collector journal/status, host telemetry, recovery/Full receipts, safety sandwiches and closeout summary.

Eventual finite drain does not repair an in-window freshness, continuity, queue-growth or capacity failure.

### 8. Qualification categories kept separate

1. **Natural-feed collection qualification:** could pass only if the single continuous one-hour interval satisfied Sections 1–7. This pass recorded zero qualified seconds, so it did not qualify collection.
2. **Periodic-ratio qualification:** not admitted in this pass. Telemetry formula remains `R = aligned delta DEPTH / aligned delta ordinary completed full analyses`. Protection/event-driven evaluations are not denominator evidence. Numeric threshold, comparison direction, tolerance and minimum denominator are `POLICY_NOT_DEFINED`. If the denominator is zero, the ratio must be null with an explicit reason. The observed value, if any, is diagnostic only.
3. **Engineering-capacity qualification:** not admitted from a natural hour, exact drain or observed average rate. No new threshold may be inferred. Any capacity decision must use the separately applicable predeclared contract, representative load, unchanged accepted floor(s), stable queues, zero loss and required repeat structure.
4. **Operational/trading admission:** never follows automatically from any category above. No Gates B-D, operational paper start, arming, native directive or order is authorized.

## Transition decisions

| Transition | Required evidence | Current result | Owner / release condition |
|---|---|---|---|
| Prepare exact disarmed runtime and evidence packet | Sections 1–3 complete before boundary; no state-changing authority beyond Joseph-approved R2 and supported recovery | **HOLD / FAILED PRESTART** | Correct native producer and safe account state were observed, but current loss, gaps, staleness, warm-up, lockout and Git identity gates failed. |
| Start fixed London interval | Pre-window receipt sealed before `08:00Z`; exact eligible session and every start rule passes | **HOLD — WINDOW NOT STARTED** | `03-prestart-hold.json` records `started=false`, `qualified_seconds=0`. No later start or rerun is permitted. |
| Natural-feed collection qualification | One genuine continuous `[08:00Z,09:00Z)` span plus exact durable closeout and final Full | **HOLD — NOT QUALIFIED** | The qualifying hour does not exist. Pre-window events cannot be relabelled into it. |
| Periodic-ratio acceptance | Approved threshold/direction/tolerance/minimum denominator plus eligible ordinary-analysis window | **HOLD — POLICY DECISION REQUIRED** | Joseph owns policy. Do not derive policy from this interval. |
| Engineering-capacity acceptance | Separate applicable predeclared contract and evidence | **HOLD — OUT OF SCOPE** | Pre-window drain and a later clean Full are not capacity qualification. |
| Gates B-D, paper start, arming or orders | Separate explicit authority and all operational gates | **REJECTED for this pass** | No such action is authorized regardless of collection outcome. |

## Evidence register

| Evidence | Classification | Result |
|---|---|---|
| Repository identity commands at `2026-09-22T07:29Z` | **OBSERVED** | HEAD/tree/branch recorded above; unrelated dirty paths preserved. |
| `src/l3g_paper/sessions.py` and `docs/operations/lane-iii-commissioning-runbook.md` | **OBSERVED** | London is `LONDON / EUROPE`, IANA `Europe/London`, weekday half-open `[08:00,11:30)`. |
| `docs/administration/caelum/passes/20260922-045541Z-aggressive-expedited-recovery/OVERSIGHT.md` | **OBSERVED historical** | At `05:15Z`, current native identity, runtime exercise and writer-capacity health were held; R2 was `0/1`; stable-tip Full passed historically; no recovery/window occurred. Every drift-prone fact requires fresh proof. |
| Implementation commit `2331c744ccaa09d6644496142daa4682a0476f72`, tree `8b14f35e8c6bb4c3395dadf756a226803b37e6be` | **OBSERVED repository identity; test receipt pending** | Two-phase commissioning repair is integrated. Sol reports 62 focused tests passed, but Adam has not received the immutable test receipt path/hash; runtime installation/loading/exercise remains pending. |
| `N:\Beelzebub\london-qualification-20260922T080000Z\00-predeclaration.json`, SHA-256 `2FA884C6797B4A47201FB81EE7F788DE188B4645E73B3E0CEDD04E9EA957D7E1` | **OBSERVED historical preparation** | Names the superseded 30-minute draft and N:-rooted identity/storage gates. It is not evidence that the final hour started. |
| `N:\Beelzebub\london-qualification-20260922T080000Z\01-R2-source-preparation.json`, SHA-256 `E34912EB88DB3BC68AE4305180643B81417CF0A00FF6811B966BA74F5D47D33F` | **OBSERVED** | R2 source install/parity/compile preparation succeeded, but receipt says `verified=false`, loaded identity unclaimed and distinct producer pending. R2 became `1/1`. |
| `N:\Beelzebub\london-qualification-20260922T080000Z\03-prestart-hold.json`, SHA-256 `D2513C03FABC12AF9AD86A2DE5044C6FFAA5B0260D71A249F55CCC56F3585F26` | **OBSERVED controlling decision** | Final hour `[08:00Z,09:00Z)` not started; `0` qualified seconds; correct producer identity observed, but +17,218 overflow, +5,617 transport failures, 534 DEPTH gaps, 1,796 freshness breaches, receiver fail-closure and Git mismatch forced HOLD. |
| Current `...\audit\pong-authentic-hour\collection-status.json`, SHA-256 `AC0E3A6356248099380E663E03144CEB6DD7C7E9FB3EBE800CC180980C93882B` | **OBSERVED closeout** | Collector stopped/disconnected; `1,885` enqueued/written/durable pre-window records; queue depth `0`, collector max age `63 ms`, rejected `1`; producer deltas remained disqualifying; required hour state `PENDING`, `0%`, incomplete and ineligible. |
| `...\audit\ledger-verification\20260922T074726710984Z-lv-london-hour-prestart-hold-20260922T0746Z.json`, SHA-256 `CAD752F7B015A78D34299A0BFAF29AB6341CAE5A2FD1EDC8DACA6868474ECC05` | **OBSERVED** | Post-shutdown Full `PASS` through all `334199` rows, stable tip hash `68489dee...c5ea5`, chain/checkpoint/risk guard valid, `quick_check=ok`, WAL `0`, no storage warnings, N: free `225,697,296,384`. |
| Continuous qualifying-hour evidence | **OBSERVED absent** | `started=false` and `qualified_seconds=0`; no replay, partial span or pre-window journal may substitute. |

## Holds and unresolved items

### `HOLD_WINDOW_START_PREREQUISITES`

- Action held: beginning the final `[08:00Z,09:00Z)` qualification hour.
- Evidence: the prestart packet records nonzero producer overflow/transport-failure deltas, 534 DEPTH gaps, 1,796 freshness breaches, one rejection, failed-closed receiver, no warm-up, retained capacity lockout and Git identity mismatch.
- Consequence: the hour was not started and receives exactly zero qualified seconds; R2 is exhausted `1/1`.
- Release condition: none within this pass. A future attempt requires new explicit authority, a genuinely new predeclaration and correction of every causal defect; it cannot reuse or repair this hour.
- Owner: Joseph controls any future pass; Sol remains sole operator; Caelum is superior reviewer.
- Independent work: read-only causal diagnosis and evidence preservation may continue without retrying runtime actions.

### `HOLD_NATURAL_FEED_QUALIFICATION`

- Action held: labelling the pass `NATURAL_FEED_COLLECTION_QUALIFIED`.
- Evidence: the governed hour was explicitly not started; pre-window records are disqualified and the qualifying interval contains zero admitted evidence.
- Consequence: tests, prior windows, replay, queue drain or short diagnostics remain non-qualifying.
- Release condition: not releasable in this pass under the no-rerun rule.
- Owner: Sol captures; Adam reviews admissions; Caelum may supersede.
- Independent work: none may modify or repair the fixed interval after observation.

### `HOLD_PERIODIC_RATIO_POLICY`

- Action held: periodic ratio pass/fail or acceptance.
- Evidence: threshold, direction, tolerance and minimum denominator are undefined.
- Consequence: even a structurally eligible computed ratio is diagnostic only.
- Release condition: Joseph approves a versioned policy before a separately governed acceptance window.
- Owner: Joseph.
- Independent work: preserve raw counters and telemetry without interpretation.

### `HOLD_ENGINEERING_CAPACITY_QUALIFICATION`

- Action held: using natural-window rate, exact drain or queue behavior as full capacity acceptance.
- Evidence: this contract defines collection qualification, not a representative capacity workload or repeat protocol.
- Consequence: no capacity floor is changed or inferred.
- Release condition: separately approved predeclaration under the existing applicable capacity contract.
- Owner: Joseph/Sol; Caelum reviews.
- Independent work: retain diagnostic rates and maxima with explicit non-authority labels.

## Disagreements and policy gaps

- No numeric periodic-ratio acceptance policy exists; Adam will not invent one.
- Exact `10 x 10` and explicit unavailable depth are honest source states, but explicit unavailable depth cannot be promoted into the canonical-depth/warm-up proof required for full qualification.
- A clean continuous natural interval can qualify collection only; it cannot itself prove the separate engineering capacity floor or grant operational authority.
- The `1,885` pre-window records were exactly enqueued/written/durable and collector queues drained, but producer loss/gaps/staleness make them disqualifying diagnostics, not qualifying-hour evidence.
- Correct loaded native source identity is necessary but not sufficient: source `17995c...` / build `215e96...` / session `891602...` / generation `1` coexisted with large new loss counters and failed continuity.
- The final Full `PASS` proves preserved ledger integrity after safe shutdown; it does not retroactively start the hour, clear the retained capacity latch or qualify feed/capacity.
- The earlier Caelum instruction prohibited an improvised London substitution for the failed Asia pass. This is a new, separately assigned and predeclared London pass; it does not rewrite or backfill Asia evidence.
- Caelum is superior and may narrow, add conditions to, HOLD or supersede this contract.

## Closeout

- Implemented: two-phase commissioning repair is present at `2331c744ccaa09d6644496142daa4682a0476f72`, tree `8b14f35e8c6bb4c3395dadf756a226803b37e6be`.
- Installed: source parity and compile preparation **OBSERVED**; R2 consumed `1/1`.
- Loaded: exact repaired producer source/build/session/generation **OBSERVED**, but runtime admission failed on identity/continuity/loss/freshness.
- Exercised: pre-window only; `1,885` collector records durably retained before fail-closed stop. The qualifying hour was **NOT STARTED**.
- Accepted: fail-closed refusal, supported clean shutdown, evidence preservation and final stable-tip Full only.
- Not accepted: natural-feed collection, canonical-depth/warm-up, periodic ratio, engineering capacity, latch recovery, paper start, Gates B-D, arming or orders.
- Remaining live-only checks: none in this closed no-rerun pass. A future pass must begin from a new authority and predeclaration after causal producer loss/transport/sequence/freshness and identity issues are resolved.
- Smallest next action: preserve this packet and diagnose the first causal source of the +17,218 overflow / +5,617 transport-failure deltas without consuming another R2 or attempting to reconstruct the missed hour.
