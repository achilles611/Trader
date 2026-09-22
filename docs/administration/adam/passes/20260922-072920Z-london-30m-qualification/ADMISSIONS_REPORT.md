# Adam admissions report — London 30-minute natural-feed qualification

Pass ID: `20260922-072920Z-london-30m-qualification`
Opened: `2026-09-22T07:29:20Z`
Closed: `OPEN`
Parent integrator and sole operator: `Sol (/root)`
Caelum record: `docs/administration/caelum/passes/20260922-045541Z-aggressive-expedited-recovery/OVERSIGHT.md` (historical entry state); current superior review pending
Adam report: `docs/administration/adam/passes/20260922-072920Z-london-30m-qualification/ADMISSIONS_REPORT.md`

## Decision

`HOLD`

One-sentence decision: preparation for one fixed natural-feed interval is admitted only under the contract below; actual London collection qualification, periodic-ratio acceptance, engineering-capacity qualification, recovery, operational paper, arming and orders remain held until attributable closeout evidence passes every applicable condition and Caelum completes superior review.

## Frozen interval and no-repair rule

- **Sole governing interval:** `[2026-09-22T08:00:00Z, 2026-09-22T08:30:00Z)`.
- Europe/London civil time: `[2026-09-22 09:00:00, 09:30:00)` under the current IANA time-zone rules.
- Repository session contract: `LONDON / EUROPE`, `Europe/London`, inside the half-open weekday session `[08:00, 11:30)`.
- The earlier `07:40Z–08:10Z` suggestion was an internal Adam draft rejected before the operator predeclaration because commissioning prerequisites could not complete. It is `REJECTED_NOT_PREDECLARED`, carries no attempt status, and is not a moved or repaired observation.
- The `08:00Z–08:30Z` interval is the first and only operator-facing predeclaration for this pass. It cannot be started late, shortened, extended, slid, trimmed, restarted, backfilled, relabelled, substituted with replay/diagnostic data, or rerun in this pass after any result is known.
- Sol must seal a pre-window receipt before `08:00:00Z` containing this exact interval, all identities below, baseline counters, current safety/storage proof, and hashes of the measurement/runtime artifacts. Missing or late predeclaration means `WINDOW_NOT_STARTED / HOLD`, not a shortened window.

## Authority and scope

- Joseph's assignment: Adam owns admissions review for a London 30-minute natural-feed qualification pass; Caelum is superior; Sol remains sole runtime, ledger, install, reload, recovery and window operator.
- Adam's bounded scope: establish the admission contract before the window, review routed evidence read-only, maintain exactly this report, and classify transitions without operating any system.
- Caelum's superior scope: review this contract and resulting evidence, and confirm, modify or supersede Adam's disposition.
- Adam's writable scope: only this report and its containing new directory.
- Explicit exclusions: no source, test, runtime, ledger, UI or NinjaTrader action; no install/reload/R2; no recovery call; no listener/process control; no account state change; no Gates B–D; no paper start; no arming; no native directive; no order.

## Entry state

| Item | State | Evidence |
|---|---|---|
| Repository/branch/HEAD | **OBSERVED** | Adam opened at `ea388a86558729bd7c0c08e7a15347a7e5e254ea`; before contract freeze Sol integrated the two-phase commissioning implementation at `2331c744ccaa09d6644496142daa4682a0476f72`, tree `8b14f35e8c6bb4c3395dadf756a226803b37e6be`, on `codex/lane-iii-perpetual-corrective-20260906`. This implementation identity, plus later report-only descendants, must be reconciled exactly in the pre-window tuple. |
| Local modifications | **OBSERVED** | Pre-existing unrelated Caelum audit and operations evidence paths were dirty/untracked. Adam will neither stage nor edit them. |
| Current runtime/evidence tuple | **UNKNOWN** | Sol must freeze exact checkout HEAD/tree, executable/source hashes, command/CWD/import hashes, selected profile/topology, native producer/build/source/session/generation, ledger path/identity/epoch/tip and evidence root before the window. |
| Current account/position/orders/reconciliation/arming | **UNKNOWN** | Prior clean Sim101 evidence is historical. A fresh authenticated sandwich is required before and after any recovery and at window boundaries. |
| R2/install/load status | **UNKNOWN current** | Caelum's `05:15:45Z` record observed R2 `0/1`, obsolete installed source `051808...`, and no authenticated listener. The new pass must prove whether the single R2 was authorized/consumed and whether the exact current source is installed, loaded and producing a distinct authenticated generation. |
| Ledger verification/latch/capacity | **UNKNOWN current** | Historical Full through sequence `334116` and N:-destination observations are not current authority. Stable-tip Full, current writer health/runway, recovery result if needed, and post-recovery Full are required. |
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

Only source events whose authentic source/receipt times fall in `[08:00:00Z,08:30:00Z)` belong to the qualification interval. Pre-boundary warm-up may establish readiness but cannot be counted as interval evidence. If the first attributable event/counter snapshot is late, the interval fails; no partial 30-minute claim is allowed.

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

At `08:30:00Z`, Sol must capture the exact end boundary before any reset or restart, stop new qualification accounting, and preserve the complete interval. Then:

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

1. **Natural-feed collection qualification:** may pass only if the single continuous 30-minute interval satisfies Sections 1–7. This proves the named collection under its exact identity and conditions; it does not authorize trading.
2. **Periodic-ratio qualification:** not admitted in this pass. Telemetry formula remains `R = aligned delta DEPTH / aligned delta ordinary completed full analyses`. Protection/event-driven evaluations are not denominator evidence. Numeric threshold, comparison direction, tolerance and minimum denominator are `POLICY_NOT_DEFINED`. If the denominator is zero, the ratio must be null with an explicit reason. The observed value, if any, is diagnostic only.
3. **Engineering-capacity qualification:** not admitted from a 30-minute natural interval, exact drain or observed average rate. No new threshold may be inferred. Any capacity decision must use the separately applicable predeclared contract, representative load, unchanged accepted floor(s), stable queues, zero loss and required repeat structure.
4. **Operational/trading admission:** never follows automatically from any category above. No Gates B-D, operational paper start, arming, native directive or order is authorized.

## Transition decisions

| Transition | Required evidence | Current result | Owner / release condition |
|---|---|---|---|
| Prepare exact disarmed runtime and evidence packet | Sections 1–3 complete before boundary; no state-changing authority beyond Joseph-approved R2 and supported recovery | **ADMITTED_WITH_CONDITIONS** | Sol only; Caelum reviews. Any unresolved prerequisite before `08:00Z` means do not start the window. |
| Start fixed London interval | Pre-window receipt sealed before `08:00Z`; exact eligible session and every start rule passes | **HOLD** | Sol releases only on contemporaneous evidence. Adam cannot infer release from preparation. |
| Natural-feed collection qualification | One genuine continuous `[08:00Z,08:30Z)` span plus exact durable closeout and final Full | **HOLD** | Await immutable evidence; Caelum may supersede Adam's later finding. |
| Periodic-ratio acceptance | Approved threshold/direction/tolerance/minimum denominator plus eligible ordinary-analysis window | **HOLD — POLICY DECISION REQUIRED** | Joseph owns policy. Do not derive policy from this interval. |
| Engineering-capacity acceptance | Separate applicable predeclared contract and evidence | **HOLD — OUT OF SCOPE** | A natural 30-minute collection is insufficient. |
| Gates B-D, paper start, arming or orders | Separate explicit authority and all operational gates | **REJECTED for this pass** | No such action is authorized regardless of collection outcome. |

## Evidence register

| Evidence | Classification | Result |
|---|---|---|
| Repository identity commands at `2026-09-22T07:29Z` | **OBSERVED** | HEAD/tree/branch recorded above; unrelated dirty paths preserved. |
| `src/l3g_paper/sessions.py` and `docs/operations/lane-iii-commissioning-runbook.md` | **OBSERVED** | London is `LONDON / EUROPE`, IANA `Europe/London`, weekday half-open `[08:00,11:30)`. |
| `docs/administration/caelum/passes/20260922-045541Z-aggressive-expedited-recovery/OVERSIGHT.md` | **OBSERVED historical** | At `05:15Z`, current native identity, runtime exercise and writer-capacity health were held; R2 was `0/1`; stable-tip Full passed historically; no recovery/window occurred. Every drift-prone fact requires fresh proof. |
| Implementation commit `2331c744ccaa09d6644496142daa4682a0476f72`, tree `8b14f35e8c6bb4c3395dadf756a226803b37e6be` | **OBSERVED repository identity; test receipt pending** | Two-phase commissioning repair is integrated. Sol reports 62 focused tests passed, but Adam has not received the immutable test receipt path/hash; runtime installation/loading/exercise remains pending. |
| Exact operator predeclaration, identity tuple, Full/recovery/safety evidence | **UNKNOWN / pending** | Must exist and pass before `08:00Z`. |
| Continuous 30-minute collector/host/native evidence | **UNKNOWN / not yet observed** | Cannot be substituted with diagnostics, replay, partial interval or restarted capture. |
| Closeout drain, final safety sandwich and post-close Full | **UNKNOWN / pending** | Required before any natural-feed qualification finding. |

## Holds and unresolved items

### `HOLD_WINDOW_START_PREREQUISITES`

- Action held: beginning qualification accounting at `08:00Z` unless every pre-window requirement is already satisfied.
- Evidence: current runtime/native/profile/ledger/safety/storage/recovery state is presently unknown to Adam.
- Consequence: a late or incomplete start receives no partial credit.
- Release condition: Sol seals the exact pre-window packet before the boundary; Caelum may independently HOLD.
- Owner: Sol; Joseph alone authorizes any R2; Caelum is superior reviewer.
- Independent work: read-only evidence preparation and hashing may continue.

### `HOLD_NATURAL_FEED_QUALIFICATION`

- Action held: labelling the pass `NATURAL_FEED_COLLECTION_QUALIFIED`.
- Evidence: the governed natural interval and closeout do not yet exist.
- Consequence: tests, prior windows, replay, queue drain or short diagnostics remain non-qualifying.
- Release condition: Sections 1–7 pass with attributable immutable evidence and no disqualifier.
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
- The earlier Caelum instruction prohibited an improvised London substitution for the failed Asia pass. This is a new, separately assigned and predeclared London pass; it does not rewrite or backfill Asia evidence.
- Caelum is superior and may narrow, add conditions to, HOLD or supersede this contract.

## Closeout

- Implemented: two-phase commissioning repair is present at `2331c744ccaa09d6644496142daa4682a0476f72`, tree `8b14f35e8c6bb4c3395dadf756a226803b37e6be`; immutable test receipt and runtime identity admission remain pending.
- Installed: **UNKNOWN current**.
- Loaded: **UNKNOWN current**.
- Exercised: **NOT YET OBSERVED for this pass**.
- Accepted: preparation only, conditionally; no qualification outcome yet.
- Remaining live-only checks: every item in Sections 1–7, official venue schedule, and final superior review.
- Smallest next action: Sol either seals the complete pre-window identity/Full/recovery/safety/storage packet before `08:00Z` or records `WINDOW_NOT_STARTED / HOLD`; no partial or later substitute is admissible.
