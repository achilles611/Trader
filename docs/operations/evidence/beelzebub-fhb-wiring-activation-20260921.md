# Beelzebub FHB wiring and activation evidence — 2026-09-21

## Verdict

The exact `TOP10_CORE_WITH_5S_SHADOW_V1` implementation through `d5544ae` is committed, installed, compiled, loaded, maintained, and consumed on authentic MNQ DEC26 data while execution remained disarmed. The former stale positional-cursor defect and the former “configured but not consumed” FHB defect are resolved at the source and runtime-semantic boundaries. A later Python-only allocation repair at `ce7f64d669284aadada4344af68fb53f159c7070` is committed and offline-verified but was deliberately not loaded into a new host after the failed brief.

The fresh natural-feed epoch is **not accepted for continuity, performance, qualification, or paper readiness**. It accumulated 209 producer transport failures during startup and later entered a persistent `FHB_HOST_QUEUE_AGE_EXCEEDED` state. The brief was stopped rather than extended to 20 minutes. No order, activation, risk reset, threshold relaxation, synthetic depth, or live-capital action occurred.

## Authority and ownership

- Sol/root was the sole integrator, installer, runtime operator, ledger verifier, and final evidence owner.
- Caelum was a read-only administrator reviewer. Its dated report is `docs/administration/caelum/passes/20260921-021441Z-fhb-activation-bottlenecks.md`.
- NinjaTrader remained operator-owned. BeezConsole was pass-owned only during the bounded proof.
- Account and authority observed during the proof: `Sim101`, `LOCAL_SIMULATION`, MNQ DEC26/MNQZ6, flat quantity zero, zero working owned/entry orders, reconciliation current, paper execution and session arming `DISARMED`, live capital `DENIED`.

## Committed implementation

| Commit | Purpose |
|---|---|
| `0cb0eb9` | Advance the private native positional cursor through every interpretable callback while withholding crossed/partial publication; retain fail-closed continuity and source-bound history identity. |
| `2a6cebb` | Wire FHB admission, maintenance, exact-price shadow state, real strategy consumption, freshness gates, and truthful activation reporting. |
| `caf0198` | Bind exact FHB profile, policy fingerprint, scheduler mode, interval, and DEC26 selector into launcher adoption. |
| `d5544ae75bd200293657eac87a186872abe66d87` | Preserve a signed sparse positional cursor across crossed/partial seams, validate disclosed and undisclosed mutations, resolve zero-price Removes where identity is provable, and report producer counter baseline/current/delta. |
| `ce7f64d669284aadada4344af68fb53f159c7070` | Reduce repeated FHB signed-snapshot and sparse-cursor allocations without changing ordering, identity, history, ambiguity or fail-closed semantics. |

No prior commit or failed epoch was reset, rewritten, or relabelled.

## Exact call path and owners

1. `BEELZEBUB_PONG_FHB_PROFILE=TOP10_CORE_WITH_5S_SHADOW_V1`, `BEELZEBUB_PONG_SCHEDULING_MODE=PERIODIC_FLAT_PROTECTED`, and `BEELZEBUB_PONG_PERIODIC_INTERVAL_MS=20` are admitted in `src/copytrade/control_center.py` and sealed in `beez_console.py` launch identity.
2. `PongProductionHost` receives the exact `FHBConfig.periodic_protected()` policy and instantiates one serial `FocusedHorizonBand` owner.
3. Every authenticated DEPTH record advances source/generation continuity and enters `PongProductionHost._admit_depth_fhb()`.
4. `FocusedHorizonBand.process_depth()` validates both signed top-10 sides, the sparse positional mutation cursor, explicit unavailable seams, five-second exact-price history, and the 32-tick diagnostic movement reserve. It never filters the transport or durable evidence stream.
5. Only `EVALUATE` decisions populate the bounded host depth windows. `REWARM_REQUIRED` clears or blocks feature work; `SKIP_OUT_OF_CORE` does no strategy feature work.
6. Both serial liquidity adapters must successfully consume the FHB-populated windows before `fhb_activation.consumer_count` advances. There is no full-depth fallback.
7. Status reports requested → admitted → instantiated → maintained → output → consumed → reported, including policy identity, source sequence, band, history, cursor/coherence state, freshness, consumer watermark, helper state, and fallback.

Relevant symbols are in:

- `src/l3g_paper/fhb.py`: `FHBConfig`, `FocusedHorizonBand.process_depth()` and sparse-cursor validation.
- `src/l3g_paper/pong_production.py`: `_admit_depth_fhb()`, real consumer accounting, freshness and `fhb_activation` status.
- `src/copytrade/control_center.py`: environment admission and production-host construction.
- `ninjatrader/NinjaScript/AddOns/BeelzebubReadOnlyAddOn.cs`: private native cursor, `mutation_resolved_price`, conditional `position_bids`/`position_asks`, strict spread validation, and explicit unavailable publication.

## Effective contract

| Field | Effective value |
|---|---|
| Profile | `TOP10_CORE_WITH_5S_SHADOW_V1` |
| Core | signed 10 bid + 10 ask, canonical best-to-worst |
| Shadow history | five seconds, exact price identity |
| Movement reserve | 32 ticks, diagnostic only; no authentic input is discarded |
| Execution owner | one serial ordered owner |
| Helper | disabled |
| Transport reduction | none |
| Full-depth fallback | forbidden / observed `NONE` |
| Scheduler configuration | `PERIODIC_FLAT_PROTECTED`, 20 ms |
| Active path in the natural brief | `EVENT_DRIVEN_PROTECTION` because the runtime bridge was unavailable |
| FHB policy fingerprint | `4e006c1eced1345e4945bee19007fdf69486132dea6b09e25784d195ae6c096a` |
| Pong loaded-artifact hash | `a7f0f587da089c903b19c3b253d20c15d0bc1c95f6ca0f280350296e0e54a717` |
| Current source expected Pong hash | `bd7826edd2d955bbb8ea59c2cc9160ead87532f36f976abba0b028865f575f7b` (offline verified; not live-loaded) |

The scheduler configuration was proven, but periodic-flat execution was not. The event-driven fallback makes no event-trigger reduction claim.

## Positional continuity and invalidity

The native AddOn now owns an exact sparse working cursor separate from publishability. Every interpretable mutation is committed before the candidate is tested for empty, partial, or crossed state. Unavailable records remain sequenced DEPTH records and disclose the exact sparse position rows when the consumer needs an anchor or reconciliation. The first subsequent coherent record also discloses the cursor; ordinary coherent mutations continue to advance the consumer cursor without expanded position arrays. Unknown mutation identity, source gap/generation change, reset, disclosed-state mismatch, or prefix mismatch invalidates FHB fail-closed.

Independent executable C# replay applies the production mutation methods to five retained authentic seams and compares them to a separate Python positional oracle. The five 63–67 event episodes cover 319 authentic transitions around journal/source pairs `87603/186798`, `92264/191291`, `93189/192014`, `93678/192469`, and `95691/194409`. The checked-in regression also covers the fresh sparse-gap sequence at journal/source `2013/12761` through `2016/12764`, a disclosed seam followed by coherent recovery, undisclosed ordinary updates, and a second disclosed seam.

In the new natural capture, six early zero-price BID Removes at source sequences 407, 409, 411, 417, 419, and 423 could not identify a removed row because the newly loaded sparse cursor had not yet reconstructed that position. FHB correctly cleared and refused feature work. Source sequence 425 was the final startup identity refusal; subsequent authentic mutations rebuilt the cursor, and live status later showed `coherence_pending=false`, exact 10+10 signed core, nonzero completed rewarms, `fhb_activation.ready=true`, nonzero outputs and nonzero real consumers. These startup refusals are preserved as fail-closed evidence, not reclassified as successful seam recovery.

## Installed and loaded identities

| Artifact | Identity |
|---|---|
| Repository AddOn | raw SHA-256 `f2c860ccaf1cae05c233e14d022f5973f0762ea358aad46411d58c0117b395fb` |
| Installed AddOn | raw CRLF SHA-256 `11a0f7b02da74b310df6ab61ac827808b940b4d6dda2b88da8ead013f1ceec92`; normalized content equals repository, normalized SHA-256 `498bac1b48a4d0a73dbeb030b9b0844e70c550868a01f3aa0874d9b55bde0348` |
| Compiled DLL | `C:\Users\atlas\Documents\NinjaTrader 8\bin\Custom\NinjaTrader.Custom.dll`, UTC mtime `2026-09-21T03:32:09.5191936Z`, SHA-256 `9f8beffb5b34e7c37c9f2488320d5076565bbc77c724720b07f5197fec8fdb76` |
| Native source fingerprint | `179461968b31942f8becb3661a2f69be6025253448489821c93a9bc4513aeede` |
| Authenticated producer | source fingerprint above; build `7439c7be78d627d15c28a82f82db71e5dfaa86e8312dec6a2b0cb293f5a6519c`; session `8e173db212af48ed9779c0ffdf66b3e5` |

The installed source contains the exact expected fingerprint, the compiled DLL contains that fingerprint, and the authenticated natural producer presented the same source identity. This closes installed/compiled/loaded identity for this epoch.

## Verification

Root integrated command:

```text
python -m unittest tests.test_native_depth_cursor_replay tests.test_l3g_ninjascript_source tests.test_l3f_ninjatrader_observation tests.test_l3g_fhb tests.test_l3g_pong_collection tests.test_beez_console_launcher_binding tests.test_l3g_pong_production tests.test_l3g_pong_periodic tests.test_pong_periodic_clock tests.test_l3g_pong_authorization tests.test_l3g_pong_volume_history tests.test_pong_contract_topology
Ran 169 tests in 9.653s — OK
```

Caelum independently repeated the affected set: 169 tests passed. After the bounded allocation repair, Sol repeated the same 169-test command: 169 passed in 9.801 seconds; Python compilation and scoped `git diff --check` passed. `tools/validate_ninjatrader_observer.ps1` completed with success=true and zero errors; its 1,279 compiler warnings are the known broad NinjaTrader reference-set warning surface, not errors.

## Fresh natural-feed brief

Collector:

`N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\pong-authentic-hour\collectors\pong-collector-20260921T033216734509Z-468d687551cf\`

| Evidence | Result |
|---|---|
| Durable records | 107,537 = 99,081 DEPTH + 3,728 QUOTE + 4,728 TRADE |
| Journal | 306,098,341 bytes; SHA-256 `b8c95b3ce4b84c1e16d14d7e218f7771e9aaef158de3cdd6304f36049e5dcc92`; tip `0b17ca5207928f8b0cd6703d52e14a6448c85f8eebda9246739076e0e0ebab48` |
| DEPTH shape | 94,779 available; 4,302 explicit unavailable; 18,862 records carried sparse position disclosure |
| Source continuity inside journal | zero DEPTH/QUOTE/TRADE sequence gaps and zero duplicate/out-of-order records |
| Collector admission | rejected 0; duplicate 0; collector overflow 0; producer overflow delta 0 |
| Collector queue | high-water 754/262,144; maximum age 2,016 ms; final depth 0 |
| Producer transport | baseline 0, current 209, delta +209 — failure |
| FHB live activation | requested/admitted/instantiated/maintained/output/consumed/reported; fallback `NONE`; helper disabled |
| FHB sample before fault | 51,185 DEPTH maintained, 12,036 output batches, 22,636 consumer count, 3,386 coherence reconciliations, ready=true |
| Fault sample | at `2026-09-21T03:35:55.6941245Z`, host queue age max 2,016 ms, source age 11,158.297 ms, persistent `FHB_HOST_QUEUE_AGE_EXCEEDED`, ready=false |
| Account/authority | flat 0, orders 0, entry orders 0, reconciled, disarmed, live capital denied |

Because the brief faulted, it was not extended to the requested 20-minute rehearsal. The collector drained completely. The first supported stop request timed out at the HTTP client while the overloaded host was already shutting down; the shutdown completed asynchronously without force. Ports 8090 and 48135 became free and the pass-owned host process exited.

Post-stop incremental ledger verification passed through sequence 164,814 with tip `a753e1c2c8cd3e7808e548480fa5f97b88923d96973802e40d81b7bcadc77375`, valid checkpoint and risk-continuity guard:

`N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\ledger-verification\20260921T033846574039Z-fhb-activation-failed-brief-closeout-20260921T0337Z.json`

SHA-256: `40f8e6c16656f5c00f74cead18947f4a4f62d574826acf1949633020ff8b3458`.

## State classification

| State | Verdict |
|---|---|
| Implemented | PASS |
| Committed | PASS at `ce7f64d669284aadada4344af68fb53f159c7070` |
| Installed | PASS |
| Compiled | PASS |
| Loaded | PASS through `d5544ae`; current Python allocation repair not live-loaded |
| FHB maintained | PASS |
| Real consumers exercised | PASS |
| Positional source/offline coherence | PASS |
| Whole natural epoch continuity | FAIL: producer transport delta +209 |
| Freshness/performance | FAIL: persistent queue-age breach |
| Periodic scheduling | UNQUALIFIED: configured 20 ms, active event-driven protection |
| 20-minute rehearsal | NOT RUN: brief gate failed |
| Paper readiness | HOLD / NOT QUALIFIED |

## Release conditions

The smallest next accepted natural epoch must be distinct and must show all of the following without changing thresholds or authority:

1. a supported startup/load sequence with producer transport-failure delta zero for the measured epoch;
2. zero source gaps, duplicate/out-of-order, reject, collector overflow, and producer overflow;
3. host and collector queue age within the existing one-second FHB limits, no persistent freshness breach, bounded non-growing queues and complete drain;
4. FHB ready, maintained, output and consumed with fallback `NONE`, including a natural seam/reconciliation if one occurs;
5. exact Sim101/MNQ DEC26 flat reconciliation, zero orders/commands, execution disarmed and live capital denied;
6. post-stop ledger PASS.

Paper qualification additionally requires a current-source warm start, 2,016-bar authenticated volume history, valid DEC26 daily bias, a subsequent whole eligible live hour, accepted certificate, retained risk continuity, and every applicable loss/latency/durability/recovery gate. The two warm-start hours may be retrieved after a supported resume, but they must be the exact immediately preceding completed UTC hours and do not substitute for or overlap the subsequent qualifying live hour.

The failed 22:00–23:00 Mountain target was not entered. With the host stopped and the performance HOLD unresolved, the earliest theoretical clock window is no earlier than 23:00–00:00 Mountain (05:00–06:00Z) if every pre-boundary history, identity, risk, latency and continuity condition is first satisfied; this is not a promised start time and rolls to the next complete UTC hour otherwise.

## Rollback and stop

- Supported stop: `scripts/stop_beezconsole.ps1`. It verifies the exact port owner, requests guarded graceful shutdown, drains collectors/ledger/transports, and never force-terminates on refusal.
- Preserve the complete run root, collector journal, old failed epochs, history refusals, risk-continuity state and verification receipts.
- If source rollback is required, create a new reviewable commit and reinstall the exact matched source/build. Do not rewrite Git history or relabel source-bound receipts. A rollback creates a new evidence epoch and requires new warm-start/history evidence.
