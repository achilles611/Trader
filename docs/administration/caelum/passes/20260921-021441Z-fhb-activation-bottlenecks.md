# Caelum administrative pass — FHB activation and bottlenecks

Pass opened: 2026-09-21 02:14:41Z
Pass closed: 2026-09-21 03:49:55Z
Role: `beelzebub_admin` (native registered role)
Integrator/runtime operator: parent Sol
Starting branch: `codex/lane-iii-perpetual-corrective-20260906`
Starting revision: `ebeac168b54456f064e8cc937430375ef802d7bd`
Controlling request: `C:\Users\atlas\Desktop\bis\Beelzebub_FHB_Activation_Bottleneck_Fixes_Multi_Caelum_2026-09-21.md`

## Objective and authorization boundary

Review requirements, source semantics, existing receipts, specialist changes, FHB activation/readiness, positional continuity, warm-start/history, periodic interaction, and the scope of the 2,111 records/s requirement. Return evidence-tagged defects, scoped holds, release conditions, acceptance assessment, and smallest next actions.

Caelum is read-only except for this pass record. Caelum did not edit application, test, configuration, or runtime files; control processes; install/reload artifacts; write ledgers; run a competing benchmark; or stage/commit work.

## Assignments

| Owner | Responsibility | Write/runtime boundary |
|---|---|---|
| Parent Sol | Integration, shared configuration, compile/install/load, runtime proof, ledger/process operations, final closeout | Sole integrator and runtime operator |
| Depth-continuity owner | Private native positional working cursor and independent seam regressions | Native source/tests assigned by parent |
| FHB integration owner | Activation/maintenance/consumer/status wiring and focused tests | FHB/runtime/test paths assigned by parent |
| Caelum | Requirements/evidence/source review and this administrative record | This file only |

## Starting state and preserved work

- **OBSERVED:** Git HEAD at pass start was `ebeac168b54456f064e8cc937430375ef802d7bd`. The implementation commits reviewed in this pass are `0cb0eb9` (`fix native depth cursor continuity`), `2a6cebb` (`wire FHB activation and truthful status`), `caf0198` (`bind exact FHB scheduling identity`), and `d5544ae` (`preserve exact FHB state across depth seams`); current reviewed HEAD is `d5544ae75bd200293657eac87a186872abe66d87`.
- **OBSERVED:** Pre-existing user/earlier-pass changes were present and left untouched:
  - modified `docs/administration/caelum/audits/20260920-1700MDT-rehearsal/CAELUM_REHEARSAL_REVIEW.md`;
  - untracked `docs/administration/caelum/audits/20260920-104755MDT/`;
  - untracked `docs/operations/evidence/beelzebub-disarmed-rehearsal-20260920.md`;
  - untracked `docs/operations/evidence/depth-recovery-20260920T1819MDT-probe1/`, `...1822MDT-sustained/`, and `...1823MDT-probe2/`.
- **OBSERVED:** At the first read-only process inventory, only operator-owned NinjaTrader PID 17208 was relevant; there was no listener on 8090, 48135, 48136, or 48137.
- **OBSERVED:** The authoritative preceding closeout is `docs/operations/evidence/beelzebub-full-host-continuity-recovery-20260920.md`. It preserves the failed positional-continuity proof, stopped/drained collector, verified ledger checkpoint, disarmed Sim101 state, and stale-history refusal.

## Evidence labels

- **OBSERVED** — directly read from current source, Git state, or a preserved receipt in this pass.
- **INFERRED** — conclusion from observed facts; release evidence is still required.
- **UNKNOWN** — not established by current evidence.

## Requirements trace

### FHB contract

- **OBSERVED:** Current profile ID is `TOP10_CORE_WITH_5S_SHADOW_V1` (`src/l3g_paper/fhb.py`). Defaults are 10 bid plus 10 ask levels, five-second shadow history, 32 movement-reserve ticks, 0.25 tick size, serial owner, and no transport reduction.
- **OBSERVED:** The 32-tick value is presently diagnostic. The emitted configuration says `movement_reserve_policy=DIAGNOSTIC_ALL_SOURCE_COVERED_PRICES_RETAINED`; distance beyond 32 increments a counter but does not filter or widen the core.
- **OBSERVED:** The source feed is bounded to 10 published levels per side, so the FHB top-10 core normally equals all published levels. FHB cannot claim upstream callback, transport, parsing, authentication, admission, or durable-record reduction. Any benefit must be measured in downstream feature/evaluation work.
- **OBSERVED:** FHB retains every admitted DEPTH record for state maintenance and selects which records seed the expensive microstructure depth windows. QUOTE, TRADE, risk, reconciliation, protective execution, and collector durability remain outside this selection.

### Requested → admitted → instantiated → maintained → consumed → reported

1. **Requested — OBSERVED:** `beez_console.server_environment()` preserves process-scoped `BEELZEBUB_PONG_FHB_PROFILE` through the clean launcher boundary.
2. **Admitted — OBSERVED:** `create_control_center_app()` accepts only the exact FHB profile ID, requires the Pong paper profile, and chooses `FHBConfig.periodic_protected()` when periodic scheduling is enabled; otherwise it chooses `FHBConfig()`.
3. **Instantiated — OBSERVED:** `PongProductionHost(..., fhb_config=...)` constructs `FocusedHorizonBand`; it rejects compute helpers and double entry throttling with periodic scheduling.
4. **Maintained — OBSERVED:** after producer/source/generation/sequence checks, every DEPTH record calls `_admit_depth_fhb()` and then `FocusedHorizonBand.process_depth()`.
5. **Consumed — OBSERVED:** only an `EVALUATE` decision extends the host `_depth` feature window; `_evaluate_microstructure()` reads those FHB-seeded bid/ask windows. `REWARM_REQUIRED` and `SKIP_OUT_OF_CORE` do not seed them.
6. **Reported — OBSERVED:** committed host status reports effective FHB configuration/fingerprint, signed core, shadow/history, decision counters, rewarm/invalidity, DEPTH freshness/heartbeat, explicit maintenance ticks/last source sequence, emitted output batches/last sequence, real consumer count/last sequence, and fallback `NONE`.

### Pre-repair identity and activation-evidence gap

- **OBSERVED:** FHB selection is process environment carried through the launcher, not a field of `LaunchBinding` and not shown as part of its sealed identity. `FHBConfig` publishes a policy/configuration payload and qualification-policy hash, and `PongProductionHost` includes the FHB module in its loaded-artifact hash, but those are not equivalent to a signed request/admission receipt.
- **HOLD — FHB acceptance identity:** Do not classify FHB as accepted from an environment toggle plus non-null status alone.
  - Evidence: launcher and control-center call path above; absence of explicit requested/admitted identity and consumer watermark in current status.
  - Consequence: a runtime screenshot or maintenance counter cannot prove that the intended policy was the admitted and consumed policy.
  - Release condition: receipt/status must bind requested profile, admitted/effective payload and hash, loaded artifact identity, maintenance source watermark, real consumer count/last sequence, fallback state, and any invalidity reason in one attributable runtime epoch.
  - Owner: FHB integration owner / parent integrator.

- **IMPLEMENTED / source-verified update:** `LaunchBinding` now contains the FHB profile and effective policy fingerprint; the clean child environment refuses drift after binding; backend runtime binding publishes the same pair; ordinary existing-listener validation compares both exactly. The supported requested-target adoption path preserves the pair only from a durable `target_runtime_binding` already exactly equal to the live binding, requires the Pong target profile, and allow-lists the fingerprint to the two policies currently admitted by Control Center (`FHBConfig()` and `FHBConfig.periodic_protected()`). Its focused launcher suite reports 36/36 passing.
- **SUPERSEDED:** The `caf0198` brief later proved the loaded identity pair but not a real consumer. The `d5544ae` brief later reached maintained/consumed with fallback `NONE`; its independent performance/continuity failures are recorded below.

### Periodic launch identity remains outside the sealed binding

- **OBSERVED:** `LaunchBinding.expected_runtime_binding()` and the backend `/api/runtime-binding` projection now seal the FHB profile/fingerprint, but neither seals `BEELZEBUB_PONG_SCHEDULING_MODE` nor `BEELZEBUB_PONG_PERIODIC_INTERVAL_MS`. The periodic-protected FHB fingerprint is shared across allowed 10/20/25 ms scheduler intervals; it cannot distinguish the intended 20 ms process from another periodic interval.
- **HOLD — exact intended runtime configuration:** Existing-listener reuse/adoption cannot by itself prove `PERIODIC_FLAT_PROTECTED / 20 ms`.
  - Consequence: FHB identity can validate while scheduling cadence differs from the intended qualification configuration.
  - Release condition: either bind scheduler mode and interval into the launcher/runtime identity and adoption regression, or require a same-epoch attributable Pong status receipt proving the exact mode/interval before runtime acceptance. This does not block compilation, installation or disarmed collection.
  - Owner: parent integrator.
- **IMPLEMENTED / source-verified update:** The parent added typed `pong_scheduling_mode` and `pong_periodic_interval_ms` fields to `LaunchBinding` and `/api/runtime-binding`, environment-drift refusal, approved-pair validation for requested-target adoption, and an invariant requiring the FHB fingerprint appropriate to event-driven versus periodic selection. Caelum first caught one stale test that changed cadence after resolving the binding; the test now resolves and launches from the same environment. Caelum reran the focused launcher/FHB/production/periodic set: 78/78 passed. Source-level HOLD is cleared. The later `caf0198` receipt proves the loaded pair exactly, but the active path remained `EVENT_DRIVEN_PROTECTION`; actual periodic scheduling is still unqualified.

## Concrete source defects

### Pre-repair defect: FHB core removal was lost when NinjaTrader reported zero mutation price

- **OBSERVED:** `FocusedHorizonBand.process_depth()` derives `price_ticks` only from `mutation_price`. When it is zero, parsing fails and `price_ticks` becomes `None`; after state maintenance the decision falls to `SKIP_OUT_OF_CORE` even if a position-0 removal changed the core.
- **OBSERVED:** The prior natural proof contained 27,644 valid zero-price positional removals. Existing FHB removal coverage uses an artificial positive removal price and does not test the native zero-price shape.
- **INFERRED:** Core membership is advanced from the post-mutation signed snapshot, but the removed pre-core price is not appended to exact-price history, no seed is forwarded, and event-driven FHB evaluation need not run. This breaks the claim that every relevant core transition is consumed.
- **HOLD — FHB effective consumer semantics:** An ACTIVE AND CONSUMED verdict is held.
  - Release condition: when operation is Remove and the native price is absent/zero, infer the removed price only from coherent prior core plus authenticated side and zero-based mutation position; otherwise fail closed. Add position-0, inner-core, outside-core, unknown-position, reset, and invalidity tests using native-shaped payloads. Prove removal/promotion seed parity and consumer watermark advancement.

### Pre-repair defect: volume-reference completeness did not enforce the requested 2,016 bars

- **OBSERVED:** Native production requests 2,016 five-minute bars but publishes `COMPLETE` for any nonzero result up to 2,016. The Python decoder accepts a self-consistent total of any positive size up to 2,016. Tests intentionally accept three and 129 bars.
- **OBSERVED:** The controlling runbook requires all chunks and 2,016 bars for the fresh current-source DEC26/provider receipt.
- **HOLD — history/paper readiness:** Current history admission is weaker than this pass's required evidence contract.
  - Consequence: a smaller self-consistent history response can be reported ready for the current UTC slot without satisfying the required full history request.
  - Release condition: producer and consumer must require the complete current-source response defined by the accepted contract (2,016 bars here), all chunks in order, exact DEC26/provider policy, authentication/HMAC/hash, pending=0, error=null, durable atomic replacement, and the old incompatible receipt preserved.
  - Owner: parent integrator unless explicitly reassigned.

- **IMPLEMENTED / source-verified update:** Current working-tree producer now publishes no history unless exactly 2,016 completed rows survive validation. The Python loader now requires exactly 16 ordered chunks (15×128 plus 96), exact per-chunk shapes, `total_bars=2016`, and 2,016 decoded windows. Tests were rewritten to reject a 2,015-bar response and admit only the full contract.
- **LOADED, EVIDENCE STILL MISSING:** This repair is included in the later `d5544ae` loaded native identity. No new exact 2,016-bar current-source provider response is preserved, so history and warm-start readiness remain unproven.

### Pre-repair defect: freshness persistence and heartbeat semantics were incomplete

- **OBSERVED:** `persistent_breach_seconds` is serialized in `FHBConfig` but is not consumed anywhere.
- **OBSERVED:** `_fhb_guard_blocker` is overwritten on every consumed observation and clears on the next record under both one-second limits. Historical `breach_count` remains diagnostic but is not itself an authority blocker.
- **OBSERVED:** `_last_callback_monotonic` is updated by `on_observation()` for every admitted observation type before DEPTH/FHB processing, so non-depth callbacks can keep the FHB heartbeat apparently current.
- **HOLD — freshness acceptance:** Do not claim the configured persistence/heartbeat contract is exercised.
  - Release condition: define and test whether persistence means latch, continuous-duration threshold, or proof-window zero-delta; use an FHB-relevant source heartbeat/watermark; expose it; and demonstrate breach/recovery behavior without dropping state.

### First FHB integration review and corrections

- **OBSERVED:** The FHB owner added strict 10/5-second/32-tick policy validation and a deterministic policy fingerprint; explicit requested/admitted/instantiated/maintained/consumed/reported status; maintenance, output, and consumer watermarks; no-fallback reporting; invalidity inheritance; and native-shaped zero-price removal handling.
- **OBSERVED:** Caelum rejected the first zero-price implementation because any zero-price Remove deeper than the retained top-10 core was treated as unresolved and cleared all FHB windows. Preserved native examples include positions 23 and 25, and the preceding proof counted 27,644 zero-price removals. The owner corrected this: a deep Remove is `SKIP_OUT_OF_CORE` only when both signed post-event core prefixes exactly equal their coherent prior prefixes; any changed or unknown core still fails closed.
- **OBSERVED:** Caelum rejected the first activation-status implementation because `fhb_activation.ready` could remain true after DEPTH heartbeat expiry. Authority computed `FHB_SOURCE_HEARTBEAT_TIMEOUT`, but FHB readiness/unavailability did not inherit it. The owner centralized the DEPTH heartbeat blocker and wired it into authority, FHB readiness, unavailable reason, and freshness status.
- **OBSERVED:** The owner added a bounded 8,192-entry exact-string MNQ price-to-tick cache for the immutable 0.25-tick contract. Its helper-only QPC check reports 200,000 calls over 20 prices: 0.3516025 seconds uncached versus 0.0234068 seconds cached, equal checksum, 199,980 hits/20 misses (15.021x). This is focused helper evidence, not full-pipeline or capacity evidence.
- **OBSERVED:** Owner-reported verification after the two corrections is 57 focused FHB tests, 42 adjacent Pong tests, and Python compilation passing. Exact commands and parent integration rerun remain to be recorded below.

## Positional continuity review

- **OBSERVED:** Pre-pass source withheld a crossed candidate without committing it to the retained positional state. The next native positional callback was therefore applied to the pre-cross cursor. The preserved closeout correctly classified this as `REJECTED_POSITIONAL_TRANSITION_CURSOR_DESYNCHRONIZATION`.
- **OBSERVED:** Five cited seams are present in the preserved journal at journal/source pairs 87603/186798, 92264/191291, 93189/192014, 93678/192469, and 95691/194409. Each has a preceding position-0 bid removal, an explicit crossed bid add, and subsequent positional mutations with no source-sequence gap at the seam.
- **INFERRED:** Advancing a private native positional cursor before publication validation is the correct bounded design if every interpretable callback is applied once and an unavailable crossed/partial state remains unpublished. Invalid/uninterpretable operations must latch continuity broken and trigger only the supported resubscription/rebuild contract.
- **OBSERVED:** Commit `0cb0eb9` implements a private working cursor that commits every interpretable callback exactly once before partial/cross publishability checks. Crossed and partial states remain explicit unavailable records with empty books. An uninterpretable mutation latches `depthContinuityBroken`; the existing supported observer-replacement path is required before any later state can be published.
- **OBSERVED:** The final executable regression extracts the five exact mutation methods from the checked-in C# AddOn, compiles them with a minimal native-event stub using Framework `csc`, and compares every production working-state and publication result to a separate Python oracle. It covers 319 authentic captured transitions through the first coherent state:
  - journal 87602–87664 / source 186797–186859 (63);
  - 92263–92325 / 191290–191352 (63);
  - 93188–93254 / 192013–192079 (67);
  - 93677–93739 / 192468–192530 (63);
  - 95690–95752 / 194408–194470 (63).
- **OBSERVED:** Owner verification reports 55 focused tests passing. Caelum independently ran `python -m unittest tests.test_native_depth_cursor_replay`: six tests passed in 0.649 seconds.
- **OBSERVED — fixture provenance closed:** Caelum compared the compact fixture directly with the preserved authenticated collector journal `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\pong-authentic-hour\collectors\pong-collector-20260921T013634787154Z-d0f233622165\authenticated-observations.jsonl`. The collector status seals 229,584 durable records and journal tip SHA-256 `089a2f0c1cd7b72e63ecaacf7bec5943b46c2a82c971bc32c8ce22a21b7daba2`. All five pre-seam 10+10 seed books and all 319 compacted mutation identities (sequence, operation, side, position, price and volume) matched source-generation 1 records exactly, with zero discrepancies. Cross/recovery endpoint record hashes were:
  - 87603/186798 `dad2dfe20929fa59d116819bca1ac20e000c483d73ede79a1778eaabe82b0141` → 87664/186859 `391581724e00f91374520ec302c1971504c0b985c7fd0709498e86e6c74cd480`;
  - 92264/191291 `be907e4268527c759366877605310dfd6338a4e048125fb04cc60d5082038559` → 92325/191352 `18423fd0f4103f4adba4f9199dd7b1c0b5afe461c2ae257109015188ce293278`;
  - 93189/192014 `e144a993776fed2cbf2c90b74ea599946fb58379ed73879ecdfbc99e07c26425` → 93254/192079 `77b0728082c5da81f1d12afac14f9098d15976a2733a7ee33a012f5edf4e842e`;
  - 93678/192469 `eac1279212772be1c935a71e4b169a6cfa655db0ebbc6d13ee415993fd548982` → 93739/192530 `1e2d6a560a48f44f7ef248b6b87a6c6fb90684a710c0568ef00a7577a345a2a0`;
  - 95691/194409 `9229a29f2c952f6baa741ebe9366f318630893e51a13cb7285496d5823179d79` → 95752/194470 `4e326d1df84315b2ec4bfc6d5261156aa878416ba9f39937c058119a49fa60bc`.
- **LIMITATION:** The retained publication exposes only the 10-level pre-seam prefix, not the producer's hidden deeper dictionary. The executable replay proves the known prefix and every retained mutation through first coherent publication; full loaded-native 10×10 behavior remains a runtime proof dependency.
- **HOLD — positional runtime acceptance:** Source/offline semantics and fixture provenance are accepted. Installed/loaded acceptance remains held until a matching compiled native build produces a fresh clean epoch, with natural seam comparison if a seam occurs; if none occurs, natural seam coverage must be reported missing rather than fabricated.

## Periodic/FHB interaction

- **OBSERVED:** With scheduling configured, FHB uses `FHBConfig.periodic_protected()` and fixed FHB entry cadence is zero, preventing two entry throttles.
- **OBSERVED:** If runtime/bridge truth is not cleanly flat, `_event_protection_reason_locked()` selects `EVENT_DRIVEN_PROTECTION`. In that path every canonical state change evaluates, including FHB out-of-core skips. The existing test explicitly expects two evaluations from two DEPTH records while this fallback is active.
- **INFERRED:** FHB may still maintain a restricted feature history in protected fallback, but it provides no evaluation-cadence saving there. A performance claim must report active path and measure FHB maintenance plus evaluation separately.
- **HOLD — periodic scheduling acceptance:** Configured `PERIODIC_FLAT_PROTECTED / 20 ms` is not exercised periodic scheduling. Release requires actual active-periodic counters, deadline/missed/obsolete metrics, FHB maintenance/consumer watermarks, and preservation of immediate protection; fallback intervals must be reported separately.

## Warm start, qualification, and source invalidation

- **OBSERVED:** Warm start requires exactly the two immediately preceding completed UTC hourly bars relative to the request's UTC-hour boundary, with no gaps, exact DEC26 topology/policy, verified source/build/session identity, and provider `DoNotMerge` semantics.
- **OBSERVED:** The AddOn provider request can retrieve those prior completed bars after a supported start; they need not have been collected live by the current host. Historical bars do not substitute for the subsequent complete qualifying live hour.
- **OBSERVED:** Any native source change creates a new observer source fingerprint and build identity. The existing volume-history receipt is already refused for the prior source mismatch and cannot be relabeled. A further cursor change again invalidates source-bound history/warm-start context.
- **HOLD — paper readiness:** Requires a matching loaded native build, current-source complete warm start/history, valid DEC26 daily bias, current Sim101 reconciliation/risk continuity, a whole clean qualifying interval and accepted certificate, and all applicable latency/loss/durability/recovery gates. FHB activation alone does not release it.

## 2,111 records/s requirements audit

- **OBSERVED:** The earliest current derivation located is a historical observed arrival range of 1,345–1,689 records/s and a 25% headroom proposal: 1,689 × 1.25 = 2,111.25, rounded to 2,111.
- **OBSERVED:** The formal isolated acceptance contract at `H:\Trader-pong-serial-measurement\docs\operations\pong-serial-acceptance-v1.json` defines 2,111 as an offered end-to-end schedule for the authenticated receiver, production analytical owner, and durable collector, with serial compute, at least three sustained and three burst repeats, zero gaps/loss/overflow/rejects/duplicates, bounded non-growing queues, complete durable drain/worker shutdown, and signed-wire/journal/decision parity.
- **OBSERVED:** The representative corpus is 150,016 records (143,117 DEPTH, 2,813 QUOTE, 4,070 TRADE) and includes a 2,191-record native receipt second. The old benchmark is an offline capacity contract, not a fresh-market qualification or minimum market-event count.
- **OBSERVED:** Current production source does not enforce 2,111. Current root references are benchmark/tool defaults and operations documents. Current FHB qualification policies explicitly say `minimum_observed_event_rate=NONE` and require actual-offered accounting instead.
- **DECISION:** Keep the claims separate:
  - actual-offered integrity can describe a bounded disarmed natural-feed exercise only at the load actually observed;
  - it cannot be cited as proof that input/whole-system capacity meets 2,111/s;
  - FHB can reduce downstream work per event, but does not reduce authentic input rate or durable records;
  - no threshold revision is accepted in this pass absent a versioned proposal with a current demand envelope, representative bursts, queue-age/protective-latency/durability evidence, and explicit authority.
- **HOLD — input capacity qualification:** No current same-build integrated 2,111/s sustained/burst acceptance evidence exists. Owner is parent/performance scope. Release is the unchanged formal acceptance contract or an explicitly accepted versioned replacement; natural traffic below the floor is not a failure but leaves capacity unproven.

## Historical evidence disposition

- **OBSERVED:** Prior FHB implementation receipt reports 49,364 depth records, 1,992 eligible FHB evaluations, 2,240 microstructure evaluations, no FHB capacity loss, and no FHB queue overflow, but severe upstream age/backlog and 25,552 transport failures kept the run disarmed.
- **OBSERVED:** The five-minute Option 3 screen retained 71,203 records at 237.343/s average and 1,513/s Ninja-receipt one-second peak, with zero new loss during the accepted window, but 3,899 records exceeded the one-second source-age ceiling (maximum about 1,992 ms). It failed freshness.
- **OBSERVED:** These receipts prove earlier implementation/exercise states only. They do not establish the current branch, loaded build, current FHB policy, positional coherence, 2,111 capacity, or paper readiness.

## Current retained-capture bottleneck evidence

- **OBSERVED:** Parent-generated receipts are under `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\fhb-activation-bottleneck-20260921\`.
- **OBSERVED:** On the clean 5,000-record prefix, periodic baseline versus FHB was 30.965510 versus 42.152186 seconds wall (+36.1% FHB) and 26.015625 versus 37.703125 CPU seconds (+44.9% FHB). FHB reduced peak traced memory from 8,052,191 to 3,779,534 bytes, but produced zero eligible evaluations, 4,691 rewarm-required events out of 4,692 DEPTH records, and ended not ready.
- **OBSERVED:** On the 50,000-record prefix, baseline versus FHB was 190.862634 versus 255.442644 seconds wall (+33.8% FHB) and 170.5 versus 214.84375 CPU seconds (+26.0% FHB). FHB reduced peak traced memory from 37,594,254 to 11,143,414 bytes, but had 44,049 rewarm-required events out of 47,304 admitted DEPTH records and only 609 eligible evaluations. That prefix also inherited large source gaps and `PONG_NATIVE_INPUT_SOURCE_SEQUENCE_GAP`; it is not correctness or full-pipeline capacity evidence.
- **OBSERVED:** The 5,000-record profile attributes 14.636 seconds cumulative to `FocusedHorizonBand.process_depth()` for 4,692 calls (about 3.12 ms per DEPTH event), including 6.993 seconds in two-side snapshot parsing. Baseline `_admit_depth()` was 1.901 seconds cumulative. The exact-tick conversion accounted for 1.912 seconds and motivated the bounded cache above.
- **OBSERVED — ranked profile costs, with overlap:** The combined two-pass cProfile places shared wire decode at 21.595 seconds cumulative/10,000 records and attestation verification at 16.323 seconds; JSON encoding at 19.162 seconds and canonical hashing at 7.522 seconds are also shared. The FHB-specific owner adds `_admit_depth_fhb` 15.360 seconds / 4,692 calls, dominated by `process_depth` 14.636 seconds, `_snapshot` 6.993 seconds, pre-cache `_ticks` 1.912 seconds, and `_rewarm_prices` 1.098 seconds; ordinary `_admit_depth` is 1.901 seconds. These cumulative spans overlap and must not be summed as wall time. The profile is Python instrumentation on retained input, not native callback/sender, queue-residence, storage, full-host CPU, allocation/GC, percentile latency, or live scheduler evidence.
- **DECISION:** The receipt's `PASS` at 50,000 means its narrow comparison predicates matched and eligible FHB events existed; it is not a performance-pass classification. Current retained-capture evidence shows lower memory but a material CPU/wall regression and no demonstrated strategy-evaluation saving. The source already publishes at most 10+10 and periodic scheduling already owns scoring, so no transport/event-count reduction is available to offset FHB maintenance in this configuration.
- **OBSERVED — committed rerun:** `integrated-2a6cebb-50000-r1` is the same retained 50,000-record prefix at committed HEAD `2a6cebb`. Baseline/FHB wall time is 151.552798/184.835036 seconds (+33.282238 seconds) and CPU is 147.375/179.875 seconds (+32.5 seconds); both dispatch zero and preserve equal stream/gap/verified-record counts. All 47,304 admitted DEPTH records fail closed as `FHB_SIGNED_CORE_INCOMPLETE`, with zero eligible evaluations, zero intentional skips and zero consumer work. Direct journal inspection confirms this old pre-repair capture supplies only one bid level versus ten asks in its early signed snapshots. The receipt is correctly `FAIL` and explicitly excludes collector-disk, ledger-durability, live-scheduler and runtime-bridge behavior.
- **DECISION — evidence dependency, not source regression:** This rerun proves the committed FHB rejects an incomplete signed core and that the retained input cannot exercise the intended FHB path. Its timing is invalid for claiming either FHB path improvement or regression because no valid 10+10 band is instantiated or consumed. The prior complete-path cost concern remains unresolved; only a fresh post-compile authenticated 10+10 stream can release it.
- **HOLD — performance repair:** Do not claim a measured FHB performance improvement or 2,111 capacity from these runs.
  - Release condition: obtain a fresh post-compile authenticated 10+10 stream, then report full-path wall/CPU, FHB readiness/consumption, stage costs, and correctness at the offered load. If FHB still adds net cost, name it as the measured bottleneck rather than treating memory reduction or fewer evaluations while unready as success.

## Acceptance matrix (interim)

| Dimension | Interim verdict | Reason |
|---|---|---|
| Positional coherence | **OFFLINE SOURCE ACCEPTED; runtime HOLD** | Executed production C# matches independent oracle and authenticated historical journal; fresh natural stream contains thousands of unreviewed crossed/partial seams |
| FHB effective wiring | **PARTIAL — maintained, not ready or consumed** | Exact loaded identity and 135,395 maintenance ticks proven; 0 completed rewarms/eligible outputs/consumers |
| Performance repair | **HOLD / valid-path evidence dependency** | Fresh loaded FHB never reaches a valid consumable path; timing cannot show a benefit or regression for that path |
| Input capacity | **UNPROVEN** | Fresh journal averages 390.501/s and peaks at 1,378/s; no 2,111/s acceptance series |
| Natural-feed continuity | **FAIL for the brief** | No stream sequence gaps/out-of-order, but producer transport-failure delta is +8,448 and freshness backlog is material |
| Periodic scheduling | **UNQUALIFIED** | Exact 20 ms configuration proven; active path remains event-driven protection and periodic-flat evaluations remain zero |
| Paper readiness | **HOLD / NOT QUALIFIED** | Coherence, current history/warm start, certificate, bias, capacity and other retained gates remain |

## Fresh post-compile disarmed brief — runtime review

Receipt root: `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\fhb-activation-bottleneck-20260921\fresh-brief-caf0198-20260921T025626Z\`
Collector: `...\pong-authentic-hour\collectors\pong-collector-20260921T025626296260Z-de75f8e4cdee\`

- **OBSERVED — loaded identity:** Runtime binding is commit `caf01982059f63fb7fe7a3dc75f84f99df7577f7`, MNQ DEC26/MNQZ6 topology hash `a339cb56...e5e7`, FHB profile `TOP10_CORE_WITH_5S_SHADOW_V1`, FHB fingerprint `4e006c1e...96a`, and `PERIODIC_FLAT_PROTECTED / 20 ms`. Pong reports loaded-artifact hash `be27eb3a...f51`, matching the committed expected hash. Native producer source is `89952a9a...e716`; loaded build is `d3ca4f91...389d` at `2026-09-21T02:54:47.5811426Z`.
- **OBSERVED — safe authority:** The before-stop snapshot is `READY_DISARMED`, Sim101/LOCAL_SIMULATION, quantity 0 in runtime and broker snapshots, zero working entry/owned orders, no unresolved execution/native order, reconciliation current, paper execution/session arming disarmed, entries paused, commands sent 0, and live capital denied. The inherited `LEDGER_CAPACITY_INADEQUATE:DEGRADED` lockout remains; it was not cleared.
- **OBSERVED — authentic load/durability:** The final collector contains 147,547 signed durable records over 377.840738 seconds (390.501 records/s journal average; 1,378 records in the highest Ninja-receipt second), including 137,623 DEPTH, 4,389 QUOTE, 5,534 TRADE and one hourly bar. It drained enqueued=written=durable=147,547, final queue=0, high-water=6,708/262,144, collector maximum queue age=5,516 ms, rejected=duplicate=collector-overflow=producer-overflow=0, and tip `29f98203...7cf0`.
- **OBSERVED — native depth shape:** 120,698 DEPTH records are complete 10×10, 5,913 are explicit crossed/unavailable, and approximately 11,000 are nonempty but partial 1–9-by-10/10-by-1–9 intermediate positional states. The last 20,000 collector records contain 18,868 DEPTH: 16,464 complete 10×10, 948 crossed, and 1,456 partial. All 64,830 Remove callbacks carry zero mutation price and are paired with 64,830 Adds across the positional rebuild stream.
- **OBSERVED — FHB is partial, not consumed:** At the before-stop Pong snapshot FHB is requested, admitted, instantiated, maintained and reported for the intended policy, with 135,395 maintenance ticks and a current signed 10×10 core. It is not ready: reset count 25,355, rewarm-required 135,395, completed rewarms 0, eligible evaluations 0, output batches/observations 0/0, consumer count 0, fallback `NONE`. The latest invalidity is `FHB_MUTATION_IDENTITY_UNRESOLVED`; current authority also inherits `PONG_WARM_START_MISSING`.
- **OBSERVED — freshness/performance:** Pong's serial host queue peaks at 219 ms, but source-receipt age peaks at 66,603.285 ms with 49,899 freshness breaches and a 49.453-second breach episode before recovering at the snapshot. Periodic cadence is configured but not exercised: active path is `EVENT_DRIVEN_PROTECTION` due `RUNTIME_BRIDGE_UNAVAILABLE`, periodic-flat evaluations 0, event-driven analytical evaluations 112,507. Therefore this receipt does not qualify periodic 20 ms, FHB valid-path performance, or 2,111/s capacity.
- **OBSERVED — transport failure:** The producer transport-failure counter rises from 3,333 on the first journal records to 11,781 on the last, a new delta of 8,448 during this collector. Final interval status explicitly contains `OBSERVATION_TRANSPORT_DISCONTINUITY`; a later stable tail is a separate recovery observation and cannot relabel the whole brief.
- **OBSERVED — remaining readiness dependencies:** Current-source history is refused against the old source fingerprint, warm start is absent, certificate is absent, and daily bias remains the unavailable SEP26 placeholder. The before-stop authority ledger has a verified anchor only through sequence 162,080 and an unverified 1,559-row tail; a fresh post-stop checkpoint is required before final closeout.

### HOLD — FHB transient positional maintenance

- Evidence: thousands of crossed/unavailable and partial positional transitions repeatedly clear the entire FHB shadow, producing zero completed rewarms or real consumers despite sustained complete 10×10 snapshots between seams.
- Consequence: effective wiring is **PARTIAL**, not `ACTIVE AND CONSUMED`; no valid-path timing or strategy-equivalence claim exists.
- Release condition: while crossed or partial seams remain entry-ineligible and cannot emit feature work, advance a private exact positional cursor only for fully interpretable mutations; preserve only per-price history whose identity and coverage remain provable; require exact prefix reconciliation with the next coherent signed 10×10 snapshot; rewarm newly unknown/promoted prices; and fully invalidate on ambiguity, gap, generation/reset, or mismatch. The repair needs an independent replay over the fresh crossed and partial seams plus a new disarmed runtime proof with nonzero completed rewarm, eligible output and real-consumer watermarks.
- Owner: FHB integration / parent integrator. This HOLD does not authorize filtering native mutations or relaxing five-second history.

#### Review of first follow-up repair — rejected against authentic partial reconstruction

- **OBSERVED:** The first follow-up working-tree repair adds `_coherence_pending` and projects crossed/partial mutations into the existing dense `_core` price tuple. Its focused synthetic FHB plus collector tests pass 31/31, but those tests model a dense partial prefix and do not model the fresh native partial-rebuild shape.
- **OBSERVED — concrete counterexample:** In the fresh authenticated journal, journal/source 2013/12761 is a coherent Ask Remove at position 0 with ten published asks. Journal/source 2014/12762 is Add Ask position 2, price 30085.75, but its valid signed prefix is only `[30085.5, 30085.75]`; 2015/12763 is Remove Ask position 0 and publishes one ask; 2016/12764 is Add Ask position 3, price 30086, and still publishes one ask. The loaded native implementation retains a `SortedDictionary<int,...>` positional working book and `DepthPrefix` stops at the first gap, invalid row, or non-monotonic price. Thus unpublished positions remain in the native cursor even when the signed prefix is short.
- **OBSERVED — defect:** The proposed FHB projector inserts into a dense ten-price tuple and requires the signed partial payload to be at least as long as that projected tuple. It therefore rejects authentic record 2014 immediately. Reversing the prefix comparison would not repair the model: replacing `_core` with the two-row signed prefix collapses the native positional gap, so the next position-3 mutation cannot be applied with native semantics.
- **HOLD — first follow-up repair is not releasable:** It would reproduce destructive invalidation on the fresh partial sequences and cannot establish exact reconciliation.
  - Consequence: 31 passing synthetic tests do not release FHB activation, valid-path performance, or runtime acceptance.
  - Release condition: maintain a true sparse positional cursor keyed by native position, retain displaced known rows beyond the published top ten, apply the native shift/update/remove semantics, represent unknown positions explicitly, and derive the signed prefix without densifying gaps or removing hidden rows. A signed partial/full payload may fill an unknown contiguous prefix only after every already-known position agrees; contradiction, unknown required identity, sequence/generation/reset break, or reconciliation mismatch must clear state fail-closed. Add a regression sourced from the fresh 2013–2016 sequence and replay the complete coherent→crossed/partial→coherent episodes independently before a loaded proof.

#### Second follow-up repair — source accepted for integration; runtime acceptance held

- **OBSERVED:** The parent replaced the rejected dense projection with a signed sparse-position contract. The native AddOn now includes full `{position, price, size}` rows only on crossed, partial, or position-zero-unavailable records and exactly the first subsequent coherent record. Ordinary coherent records continue to carry the source mutation plus producer-resolved mutation price without the expanded rows.
- **OBSERVED:** FHB maintains the sparse cursor across both disclosed and undisclosed mutations using the native positional shift semantics. It compares the complete disclosed dictionaries exactly, derives the published prefix with the same gap/non-monotonic stopping rule, and compares both price and quantity with the signed book arrays. A partial or crossed frame without the signed position contract is rejected; entry remains ineligible while coherence is pending; reset, sequence/generation break, transition contradiction, or prefix mismatch clears the cursor and history fail-closed.
- **OBSERVED:** Caelum found and the parent corrected a second-seam defect during review: the first conditional-disclosure draft left the consumer cursor stale on undisclosed ordinary records. The final implementation projects every undisclosed ordinary mutation and validates its derived 10×10 rows before accepting the next disclosed seam.
- **OBSERVED:** Regressions now cover the fresh journal 2013–2016 duplicate/gap shape, disclosed crossed seam → coherent recovery → undisclosed ordinary mutations → second disclosed seam, executable production C# disclosure/latch behavior, exact full position rows, and the producer-resolved zero-price Remove identity. The older dense/no-position input is explicitly refused rather than treated as recoverable.
- **SOURCE VERDICT:** No source-level correctness HOLD blocked integration of this second repair. It is committed at `d5544ae75bd200293657eac87a186872abe66d87`. The old loaded build does not emit this contract; old source-bound history and the previous fresh brief cannot accept it.
- **RUNTIME UPDATE:** The later `d5544ae` natural brief loaded the matching native source, reached FHB ready/maintained/consumed with fallback `NONE`, and therefore resolves the old destructive seam-invalidation activation defect at the source/runtime-semantic level. The epoch does not release performance, continuity, qualification, or paper readiness because transport failures increased and host queue age breached persistently.

### HOLD — collector counter-delta semantics

- Evidence: `PongAuthenticHourCollector` initializes producer failure totals at zero and treats the first inherited nonzero absolute counter as a new failure. That can falsely invalidate a newly started collector even when the producer counter is stable. Separately, this fresh brief has a genuine post-first-record +8,448 delta and therefore fails regardless of that defect.
- Consequence: an inherited absolute total can be mistaken for a new measurement-window loss; current brief remains failed on actual delta.
- Release condition: bind a baseline to producer session/source/build/generation, report baseline/current/delta, accept a stable inherited nonzero total, reject any same-producer increase, fail closed on unexplained same-producer regression, and establish a new baseline only for an authenticated new producer identity. Add focused stable/increase/regression/new-session tests. A later zero-delta window must be named separately and may not erase this one.
- Owner: collector / parent integrator.

- **IMPLEMENTED / source-verified update:** The collector now records producer absolute baseline/current/delta separately for queue overflow and transport failure. A stable inherited nonzero total is accepted; any same-identity increase invalidates the affected interval; a regression fails closed as `PRODUCER_COUNTER_REGRESSION`; and an authenticated replacement session establishes a new zero-delta baseline while retaining `PRODUCER_IDENTITY_CHANGED` as its independent continuity blocker. Focused tests cover both counters and the replacement-session case.
- **RUNTIME HOLD REMAINS:** The prior brief still has a genuine +8,448 producer transport-failure delta and an independent `OBSERVATION_TRANSPORT_DISCONTINUITY`; source correction does not relabel that epoch. The later `d5544ae` brief correctly reports baseline 0/current 209/delta +209 and therefore also fails. A new zero-delta window is required.

## Verification and owner artifacts

- **FHB owner command:** `python -m unittest tests.test_l3g_fhb tests.test_beez_console_launcher_binding tests.test_l3g_pong_production tests.test_l3g_pong_periodic tests.test_pong_periodic_clock tests.test_l3g_pong_authorization tests.test_l3g_pong_volume_history tests.test_pong_contract_topology` — 99 passed in 7.947 seconds.
- **FHB owner syntax/checks:** `python -m py_compile src/l3g_paper/fhb.py src/l3g_paper/pong_production.py src/copytrade/control_center.py` passed; scoped `git diff --check` passed with line-ending warnings only.
- **Depth owner command:** `python -m unittest tests.test_native_depth_cursor_replay tests.test_l3g_ninjascript_source tests.test_l3f_ninjatrader_observation` — 55 passed.
- **Caelum integrated committed-tree command:** `python -m unittest tests.test_native_depth_cursor_replay tests.test_l3g_ninjascript_source tests.test_l3f_ninjatrader_observation tests.test_l3g_fhb tests.test_beez_console_launcher_binding tests.test_l3g_pong_production tests.test_l3g_pong_periodic tests.test_pong_periodic_clock tests.test_l3g_pong_authorization tests.test_l3g_pong_volume_history tests.test_pong_contract_topology` — 158 passed in 9.489 seconds at HEAD `caf0198`. One immediately preceding run completed all test bodies but hit a transient Windows `WinError 5` while deleting the temporary compiled C# harness; an isolated replay rerun passed 6/6 and the full identical rerun then passed 158/158. No production artifact was involved in that temporary-file cleanup event.
- **Caelum follow-up integration command:** `python -m unittest tests.test_native_depth_cursor_replay tests.test_l3g_ninjascript_source tests.test_l3f_ninjatrader_observation tests.test_l3g_fhb tests.test_l3g_pong_collection tests.test_beez_console_launcher_binding tests.test_l3g_pong_production tests.test_l3g_pong_periodic tests.test_pong_periodic_clock tests.test_l3g_pong_authorization tests.test_l3g_pong_volume_history tests.test_pong_contract_topology` — 169 passed in 9.744 seconds against the final sparse-cursor/counter tree subsequently committed as `d5544ae`. `py_compile` for the three changed Python modules passed and scoped `git diff --check` reported no whitespace errors (line-ending warnings only).
- **OBSERVED follow-up identities:** Dynamically recomputed and embedded expected native fingerprint both equal `179461968b31942f8becb3661a2f69be6025253448489821c93a9bc4513aeede`; the changed Python/FHB collector tree computes Pong loaded-artifact hash `a7f0f587da089c903b19c3b253d20c15d0bc1c95f6ca0f280350296e0e54a717`. The periodic FHB policy fingerprint remains `4e006c1eced1345e4945bee19007fdf69486132dea6b09e25784d195ae6c096a` because the admitted strategy contract did not change.
- **Caelum independent focused checks:** `python -m unittest tests.test_native_depth_cursor_replay` — six passed; `python -m unittest tests.test_l3g_fhb` — 23 passed; targeted native source/history assertions — two passed; full `git diff --check` — no whitespace errors (line-ending warnings only).
- **OBSERVED historical identity at `caf0198`:** dynamically recomputed source hash, AddOn embedded `ProducerSourceFingerprint`, Python `EXPECTED_OBSERVER_SOURCE_FINGERPRINT`, and source-test expectation all equal `89952a9a476d4b9c8336dfca76d868b63c3222ff7abcceb7686553dc8bd0e716`.
- **OBSERVED current Python/FHB identity at `caf0198`:** expected Pong loaded-artifact hash is `be27eb3a9756fcbbcfd36c562036866b158a31faf285fdd2c600e9019c220f51`; periodic-protected FHB policy fingerprint is `4e006c1eced1345e4945bee19007fdf69486132dea6b09e25784d195ae6c096a`.
- **OBSERVED installed/compiled native state:** Read-only comparison after the supported install/compile showed canonical installed AddOn parity and installed Indicator pre-generated-prefix parity with the repository. `C:\Users\atlas\Documents\NinjaTrader 8\bin\Custom\NinjaTrader.Custom.dll` had UTC mtime `2026-09-21T02:54:49.4844190Z`, SHA-256 `468a1b84fceed3519a2175816fb69208facd63d02681d006dd01549259715a83`, and contained the expected native source fingerprint plus `BeelzebubAutomaticMarketObserver`, `ShiftKnownPositions`, and `DepthPrefix` markers. This proves installed source and compiled assembly identity, not what the current NinjaTrader process has loaded.
- **HISTORICAL `caf0198` LOADED STATE:** `0cb0eb9` contains the native cursor/history contract repair, `2a6cebb` contains the FHB activation/status/launcher repair, and `caf0198` seals scheduler identity plus parameterizes the supported commissioning check for DEC26. The `caf0198` brief loaded matching identities and exercised maintenance, but not readiness/output/real consumption. It is superseded operationally by the distinct `d5544ae` epoch below and remains preserved as failed evidence.
- **FOLLOW-UP COMMITTED / LOADED / EXERCISED:** `d5544ae` contains the sparse signed-position seam contract and collector baseline/delta correction. Its expected native source is `179461968b31942f8becb3661a2f69be6025253448489821c93a9bc4513aeede`; the natural brief's producer source matches it, and the producer build is `7439c7be78d627d15c28a82f82db71e5dfaa86e8312dec6a2b0cb293f5a6519c`. Parent runtime observation reports FHB reached ready/maintained/consumed with fallback `NONE`. The expected Pong loaded artifact is `a7f0f587da089c903b19c3b253d20c15d0bc1c95f6ca0f280350296e0e54a717`; a standalone same-epoch Pong status file was not preserved in the reviewed receipt set, so exact host hash/status details remain attributable to the runtime operator rather than independently re-readable from disk.
- **FOLLOW-UP INSTALL/COMPILE OBSERVATION:** Read-only inspection after parent integration found content parity between repository and installed AddOn (`git diff --no-index` returned no content difference; raw hashes differ only with the installed line-ending form). The installed file embeds `179461...eede`. `NinjaTrader.Custom.dll` has UTC mtime `2026-09-21T03:29:23.6079657Z`, SHA-256 `bfd55947cdf9bb563d8dc7a32d33a6785729255b34f895b519eef6edeb7f33f1`, and contains the new source fingerprint. The subsequent authenticated producer session carrying the same source identity proves that the new native code was loaded and exercised.

## Fresh `d5544ae` natural brief — final runtime review

Collector: `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\pong-authentic-hour\collectors\pong-collector-20260921T033216734509Z-468d687551cf\`
Ledger closeout: `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\ledger-verification\20260921T033846574039Z-fhb-activation-failed-brief-closeout-20260921T0337Z.json`

- **OBSERVED — native identity and durable ingress:** The authenticated session is source `179461968b31942f8becb3661a2f69be6025253448489821c93a9bc4513aeede`, build `7439c7be78d627d15c28a82f82db71e5dfaa86e8312dec6a2b0cb293f5a6519c`, build timestamp `2026-09-21T03:32:07.8815707Z`, session `8e173db212af48ed9779c0ffdf66b3e5`. The collector stopped and drained 107,537 enqueued/written/durable records (306,098,341 bytes), queue 0, high-water 754/262,144, maximum collector queue age 2,016 ms, and zero reject, duplicate, collector-overflow, or producer-queue-overflow delta. From collector start to final fsync (306.284949 seconds), the aggregate captured rate is 351.101 records/s and 999,390.737 bytes/s; this is actual offered/captured-window evidence only, not capacity qualification.
- **OBSERVED — continuity failure:** Producer transport failures rise from baseline 0 to current 209; the repaired collector reports delta +209 rather than confusing an inherited total. This epoch is failed even though the queue drained and record admission was otherwise clean.
- **OPERATOR-OBSERVED — FHB activation:** The parent runtime operator observed FHB ready, maintained and consumed with fallback `NONE` before it became persistently blocked by `FHB_HOST_QUEUE_AGE_EXCEEDED`; the cited sample at `2026-09-21T03:35:55Z` has source age 11,158.297 ms and `ready=false`. No standalone same-epoch Pong snapshot survived the supported shutdown/client timeout, so exact activation counters are **UNKNOWN** to independent receipt review. This is enough to show the old always-invalidating seam defect is no longer the first activation blocker, but not enough for final acceptance.
- **OBSERVED — ledger closeout:** Incremental ledger verification passes through sequence 164,814 with tip `a753e1c2c8cd3e7808e548480fa5f97b88923d96973802e40d81b7bcadc77375`, valid risk-continuity guard sequence 134,169, zero errors, and no unverified tail in the checkpointed range.
- **OPERATOR-OBSERVED — safe authority:** Final state remained Sim101 flat, execution disarmed/live capital denied, and orders/commands zero. The supported shutdown request timed out at the client but completed asynchronously; both service ports became free and the pass-owned host PID exited. A read-only final inventory found no listener on 8090/48135/48136/48137; NinjaTrader PID 17208 remained operator-owned. Parent-owned Python PIDs 13808/16944 completed the offline comparison and were absent from the final inventory; Caelum owned no remaining process.
- **VERDICT:** Sparse seam continuity is **implemented, committed, installed, compiled, loaded and exercised**. The activation defect is resolved. This brief is **FAILED / NOT QUALIFIED** on new transport loss and persistent host/source queue age; it cannot support FHB performance, 20-minute rehearsal, qualifying-hour, 2,111/s capacity, or paper readiness claims.

### Same-capture `d5544ae` comparison — activation proven, performance regressed

Primary unprofiled receipt: `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\fhb-activation-bottleneck-20260921\wall-d5544ae-fresh-10000-r1.json`
Diagnostic cProfile receipt: `...\profile-d5544ae-fresh-10000-r1.json` and `...\profile-d5544ae-fresh-10000-r1.pstats`

- **OBSERVED:** The disarmed, non-dispatching harness uses the same first 10,000 authenticated records and source identity for baseline and FHB. It reports 8,984 DEPTH, 412 QUOTE and 604 TRADE records, zero missing/out-of-order records, identical verified counts and native dispatches zero. It is retained replay with a synthetic deterministic QPC grid, no collector disk/ledger/runtime bridge, and is explicitly `NOT_QUALIFIED_HISTORICAL_RETAINED_REPLAY`.
- **OBSERVED — FHB is genuinely exercised:** 8,984 maintenance ticks include 118 crossed-unavailable and 937 partial signed records, 567 coherent reconciliations, 319 completed rewarms, 1,308 eligible evaluations and 2,782 out-of-core skips. This closes the old “configured but never consumable” defect for the bounded replay.
- **OBSERVED — exact fail-closed attribution:** The six coherence-reconciliation failures are startup ambiguities at journal/source 252/407, 254/409, 256/411, 262/417, 264/419 and 268/423. Every one is a zero-price BID Remove whose disclosed sparse cursor lacks the removed-position identity because collection joined source generation 1 in progress rather than at authoritative initialization. Journal/source 270/425 is the final fail-closed `FHB_MUTATION_IDENTITY_UNRESOLVED` reset. Later disclosed state recovers, and the live host subsequently reaches ready/consumed. These are correct refusals, not evidence that the private cursor fabricated a price or retained stale readiness. Their cumulative counters remain historical and must not be relabeled as zero.
- **OBSERVED — material unprofiled regression:** Baseline is 35.734431 seconds wall / 35.40625 CPU (279.842 records/s diagnostic); FHB is 48.968338 seconds wall / 48.484375 CPU (204.214 records/s), a +13.233906-second wall and +13.078125-second CPU regression, while peak traced memory falls from 15,410,156 to 9,693,623 bytes. Reduced memory does not offset the CPU/wall failure. FHB reports 14.1368282 seconds of shadow-maintenance span under `tracemalloc`; it is attribution evidence, not a native/live latency measurement.
- **OBSERVED — ranked FHB costs:** Combined-profile cumulative/internal time for `process_depth` is 38.948/14.088 seconds; `_validate_position_transition` 8.918/1.065; `_snapshot` 8.177/4.816; `_position_snapshot` 5.698/3.620. The exact-price cache is no longer a material cost (`_mnq_price_ticks`: 132 calls, 0.002 seconds total). Times overlap and are instrumentation-bearing; they identify the current optimization target but are not additive wall time.
- **OBSERVED — periodic negative evidence:** Both passes schedule 1,331 opportunities and apply/complete 612, skip 719 obsolete deadlines, and mark all 612 applied evaluations late with zero on-time. This proves offline periodic ownership/counters, but fails the timing screen and is not live scheduler qualification.
- **HOLD — performance and qualification:** Do not accept the FHB performance repair, active-periodic timing, 20-minute rehearsal, qualifying hour, or paper transition from this receipt.
  - Evidence: +13.233906 seconds wall/+13.078125 seconds CPU in the unprofiled comparison, 612/612 deadlines late, live transport delta +209, and persistent live queue-age breach.
  - Consequence: FHB is active and capable of valid consumption, but its current full maintenance cost is the dominant added work in this replay; the live path is neither loss-free nor fresh enough for qualification.
  - Release condition: apply one measured bounded reduction in repeated snapshot/position validation work without weakening signed comparison, ordered state, correct startup ambiguity refusal, shadow history or durability. Repeat the same-capture comparison with semantic parity and no material wall/CPU regression, then require a new authoritative-initialization, loss-free natural epoch with bounded source/host queue age.
  - Owner: parent integrator/performance owner; Caelum remains reviewer only.

## Final acceptance matrix

| Dimension | Final verdict | Evidence / consequence |
|---|---|---|
| Positional coherence | **PASS source/offline; clean live epoch still required** | Independent executable C# oracle passes 319 historical transitions; six new startup ambiguities refuse fail-closed and later recover rather than inventing identity |
| FHB identity and wiring | **ACTIVE AND CONSUMED for the bounded path; overall acceptance held** | Exact identity is loaded; replay has 1,308 eligible/319 completed rewarms and live operator observed ready/maintained/consumed with fallback `NONE`; standalone final live Pong counters are absent |
| Performance repair | **FAIL / REGRESSION** | Same unprofiled 10k records: FHB adds 13.233906s wall and 13.078125s CPU; lower memory is insufficient |
| Natural-feed continuity | **FAIL** | Producer transport-failure delta +209 in the new distinct epoch |
| Freshness/host queue | **FAIL** | Persistent `FHB_HOST_QUEUE_AGE_EXCEEDED`; sample source age 11,158.297 ms |
| Collector durability | **PASS for captured records** | 107,537 durable, queue drained, zero collector reject/duplicate/overflow; ledger verification PASS through 164,814 |
| Periodic 20 ms scheduling | **FAIL offline / unqualified live** | Active offline path completes 612/1,331 opportunities, all 612 late; no accepted live periodic proof |
| Current history/warm start | **MISSING** | No complete current-source 2,016-bar receipt or two-hour warm-start certificate |
| Input capacity | **UNPROVEN** | No unchanged 2,111/s sustained/burst acceptance series |
| Paper readiness | **HOLD / NOT QUALIFIED** | Transport/freshness failures plus missing history, qualifying certificate and applicable readiness gates |

## Unresolved items and smallest next actions

1. Parent/performance: use the measured `process_depth`/snapshot/position-validation costs to make one bounded semantic-preserving optimization, then repeat the identical 10,000-record unprofiled comparison. Preserve the now-proven startup ambiguity refusal; do not count the tick-cache microcheck or lower memory as a performance pass.
2. Parent: preserve the failed `d5544ae` live epoch and diagnose the measured host-queue-age/transport coupling from its existing capture; do not reopen the seam design without contradictory evidence.
3. Parent: run a new distinct short disarmed natural epoch from authoritative initialization, requiring producer transport delta 0, no source gaps/reject/duplicate/overflow, bounded host and collector age, FHB ready+maintained+consumed with output/consumer watermarks and fallback `NONE`, flat/disarmed zero orders, complete drain, and post-stop ledger PASS.
4. Parent: rebuild the exact current-source 2,016-bar history and two immediately preceding completed UTC-hour warm-start evidence. Keep the later qualifying hour separate and subsequent.
5. Parent: only after the short epoch and performance screen pass, proceed to the authorized 20-minute rehearsal; do not substitute the helper cache microcheck, failed offline capture, or an unready/aged live path.
6. Parent/performance: keep actual-offered natural load distinct from the 2,111 capacity contract; no current evidence releases the capacity HOLD.

No HOLD above blocks independent offline implementation or focused tests. They hold only the affected acceptance, load, qualification, or paper transition.

## Rollback and evidence preservation

- Do not reset, rewrite, relabel or delete the preserved run root, collector journals, failed positional epoch, old history receipt, ledger database, risk-continuity anchor or verification receipts.
- If a loaded repair fails, Sol should keep execution disarmed, stop/drain/checkpoint through the supported owner workflow, record the exact source/build/runtime identities and failure counters, and make any source rollback as a new reviewable commit rather than rewriting Git history.
- Reinstall/reload only an exact supported native source/build pair. The commissioning script's DEC26 path must be invoked with `-ExpectedInstrument 'MNQ DEC26'`; its default remains SEP26 for older workflows. A clean compile is not proof that the installed source or currently loaded assembly changed.
- A rollback or observer replacement creates a distinct evidence epoch and invalidates source/build/session-bound history, warm-start and runtime-consumption claims until rebuilt. Risk/account continuity and prior receipts remain inherited; no FHB toggle, new ledger or source fingerprint clears them.

## Post-review disposition — 2026-09-21 04:00:59Z

Scope: bounded read-only Caelum review of the parent-owned Python allocation cleanup in `src/l3g_paper/fhb.py` and its already-produced receipts. Caelum made no application edit, runtime/process change, ledger write, or benchmark run in this addendum.

### Implementation and verification state

- **OBSERVED:** Commit `ce7f64d669284aadada4344af68fb53f159c7070` (`reduce FHB sparse snapshot allocations`) replaces sorted-copy validation of the signed core with a one-pass order/duplicate check, clones only the mutated positional side, reuses the normalized operation string and prior-core sets, and builds prior touch quantities only for the removal branch that consumes them. It does not change the admitted FHB profile, signed book comparison, sparse positional semantics, fail-closed reasons, shadow duration, reserve, scheduler ownership, transport policy or execution authority.
- **OWNER-REPORTED VERIFICATION:** Focused tests pass 38/38 and the full affected integration set passes 169/169. Both retained 10,000-record comparison receipts preserve exact source/stream/gap/verified-record counts, decision counts, 1,308 eligible evaluations, 2,782 skips, 4,894 rewarm-required events, 319 completed rewarms and zero native dispatches. This supports source-semantic acceptance of the allocation cleanup; it is not installed/loaded/live verification.
- **STATE:** The optimization is implemented and committed at current HEAD `ce7f64d669284aadada4344af68fb53f159c7070`, but it is not loaded or exercised by a fresh host in the evidence reviewed here. The Pong loaded-artifact identity changes and prior host-performance/wiring receipts remain historical; the native producer fingerprint and its preserved journal are not relabeled.

### Same-capture outcome

Pre-change reference: `wall-d5544ae-fresh-10000-r1.json`. Post-change receipts: `wall-fhb-snapshot-opt-fresh-10000-r1.json` and `wall-fhb-snapshot-opt-fresh-10000-r2.json`.

| Metric | Pre-change | Post r1 | Post r2 | Disposition |
|---|---:|---:|---:|---|
| FHB maintenance span | 14.1368282 s | 13.4942676 s (-4.545%) | 13.7908171 s (-2.448%) | Directionally improved in both runs |
| Total FHB wall | 48.9683375 s | 48.7284607 s (-0.490%) | 49.2480054 s (+0.571%) | Straddles zero; no repeatable total improvement |
| Total FHB CPU | 48.484375 s | 48.28125 s (-0.419%) | 48.625 s (+0.290%) | Straddles zero; no repeatable total improvement |
| FHB-minus-baseline wall | 13.2339064 s | 11.5127122 s | 13.4805259 s | Material full-path overhead remains |
| FHB-minus-baseline CPU | 13.078125 s | 11.90625 s | 13.15625 s | Material full-path overhead remains |

- **DECISION:** The cleanup is source-correct and consistently reduces its directly instrumented maintenance span, but total wall and CPU move in opposite directions across the two repetitions and remain materially slower than the paired non-FHB path. Therefore it is a useful allocation cleanup, not an accepted performance repair or evidence that the bottleneck is fixed.

### HOLD disposition and exact release condition

- **UNCHANGED — performance/qualification HOLD:** r1's small total improvement is not repeatable; r2 regresses slightly. Neither result releases the prior full-path performance, periodic, 20-minute rehearsal, qualifying-hour, capacity or paper-readiness holds. Natural-feed transport `+209`, persistent live queue age, 612/612 late offline periodic evaluations, missing current history/warm start and unproven 2,111/s capacity remain independently unchanged.
- **NO NEW SOURCE-CORRECTNESS HOLD:** Exact outcome parity and the test results support the committed bounded allocation cleanup. It must not be described as loaded or runtime-verified until the parent recomputes the Pong artifact identity and launches that exact committed host source.
- **RELEASE CONDITION:** First establish a repeatable total-path improvement with at least three matched, alternating pre/post same-capture pairs on a recorded quiescent host, exact semantic parity, and wall plus CPU improvement outside the observed run-to-run sign-changing noise. The candidate must eliminate the material paired FHB overhead or be covered by an explicitly versioned and accepted overhead/benefit budget; a maintenance-only reduction is insufficient. Then load the exact committed Python artifact and obtain a new authoritative-initialization disarmed natural epoch with producer transport delta 0, no gaps/rejects/duplicates/overflows, bounded source and host queue age, FHB ready/maintained/consumed with output and consumer watermarks, complete drain and post-stop ledger PASS. Only after those conditions and the separately required current-source history/warm-start gates may the 20-minute or qualifying proof proceed.
