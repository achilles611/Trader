# Beelzebub full-host continuity recovery — 2026-09-20

## Verdict

**HOLD — full DEPTH continuity is not resolved.** Native MNQ DEC26 depth transport remains treated as proven, and the post-drain host path sustained ingress, admission, and durable writing without new transport loss. The proof nevertheless failed the required content/coherence gate: 43 authentic positional mutations produced `CROSSED_NATIVE_POSITIONAL_SNAPSHOT`, and later available books can be calculated from a stale positional cursor.

Exact defect classification from the bounded Caelum review:

`REJECTED_POSITIONAL_TRANSITION_CURSOR_DESYNCHRONIZATION`

No qualification, paper activation, order submission, synthetic depth, threshold change, parser weakening, capacity increase, or automated restart occurred.

## Scope and authority

- Sol was the sole integrator and runtime operator.
- Caelum performed bounded, read-only requirements and evidence review.
- Execution remained disarmed, live authority disabled, and Sim101 flat.
- Native DEC26 transport/feed availability was accepted as established and was not reopened for diagnosis.
- The proof used only natural native traffic.

## Source and build identities

| Item | Identity |
|---|---|
| Branch HEAD at closeout | `646b69ac1a03e14809f0e397a98e8edba878ff9b` |
| Last-valid-book repair | `297bf986fdc6dede1fd838ee4f36ae068df24000` |
| Stale-history fail-closed repair | `646b69ac1a03e14809f0e397a98e8edba878ff9b` |
| NinjaTrader AddOn source fingerprint | `3c2248a8b676f35ca0b2442bb0463b9786096b1882f04f0e8f6f2eb60e6454e3` |
| Loaded native build fingerprint | `97786aa614f94d961a81e0dc636a35ec721bfa0ed342cd52bc75b0f1c75cd8e3` |
| Native producer session | `b10c9f23fc804f5fa68d81cb51dbf4a4` |
| Native compilation time | `2026-09-21T01:31:57.7788906Z` |
| Proof host commit | `646b69ac1a03e14809f0e397a98e8edba878ff9b` |

The operator performed one F5 compile. Compilation completed with no errors. Installed source matched the canonical source, and the loaded assembly reported the source and build fingerprints above.

## Focused repairs and verification

### Observation-sink policy-identity regression

The previously known observation-sink policy-identity `ValueError` did not recur. The new ledger increment contained no `INCIDENT_OBSERVATION_SINK_FAILURE` and no policy-identity `ValueError` row.

The focused regression and surrounding observer/adapter suite passed:

- 49 observer/adapter tests passed.
- 166 Pong tests passed at commit `297bf98`.
- 17 FHB/periodic tests passed.
- Roslyn in-memory compilation passed with zero errors; existing warnings were unchanged.

### Last-valid-book repair

Commit `297bf98` prevents a crossed candidate from replacing the last valid published book, preserves partial/reset/empty-side candidates needed for rebuild, and records the authentic mutation details on an unavailable observation. It did not weaken the strict `bid < ask` validation gate.

### Stale history fail-closed repair

The first supported maintained-binding startup after the source-fingerprint change failed closed while restoring an old signed volume-history receipt. The exact refusal was:

`PONG_VOLUME_HISTORY_RESTORE_REFUSED:AUTH_FAILED:ninjatrader_source_fingerprint`

Commit `646b69a` makes source-authentication refusals a typed, retained input refusal: it leaves the restored windows empty and does not overwrite or accept the stale receipt. Verification passed:

- 9 volume-history tests passed.
- 167 Pong tests passed.
- Python compilation checks passed.

The original receipt and its pre-repair evidence copy remain byte-identical:

| File | Bytes | SHA-256 |
|---|---:|---|
| `N:\Beelzebub\runtime\pong-production\pong\MNQZ6\v1\volume-reference.json` | 208,516 | `111b9a4d407cd2e9c804ab8a59bf85707cf82e4b6513b4ed3289f6b1e2656fb9` |
| `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\full-host-continuity-recovery-20260921T0132Z\stale-volume-reference-old-source.json` | 208,516 | `111b9a4d407cd2e9c804ab8a59bf85707cf82e4b6513b4ed3289f6b1e2656fb9` |

The failed startup is retained in `logs/beez-console-server.log`, lines 3,930,437–3,930,465. At closeout that log was 264,038,264 bytes with SHA-256 `9e84b0662117ac32605f10481d93422e69c16be7d4ce5b509529debcefee4e33`.

## Causality established before the final proof

The evidence separates several successive boundaries rather than treating all loss as a feed problem:

1. The original native collection contained 149,809 records: 137,150 DEPTH, 5,294 QUOTE, and 7,365 TRADE. One `bridge_json` item was an expected shutdown partial.
2. It also contained 623 `MALFORMED_DEPTH_MUTATION` records, eight local sequence gaps, and 6,236 unavailable depth snapshots. Zero-price positional removes appeared before the later gap/overflow counters, proving an independent mutation-contract issue.
3. The first full-host capacity boundary was `LaneIIIPaperRuntime.ingest`, about 1,445.8 events/s against the formal 2,111 events/s target.
4. The native producer used a shared 32,768-item bounded queue. A failed sender batch could drop the entire batch; sender batches were 256 observations with a 524,288-character cap.
5. Commit `3f150a0` repaired the host-side continuity path, but its first proof still accepted only about 590 events/s, with 1.076 ms paper fanout, queue high-water 21,144, maximum age 36.625 seconds, and producer loss.
6. Commit `cbbad4c` removed the duplicate Pong market projection. A drained throughput proof then passed the transport/capacity boundary, but still produced 1,729 unavailable depth snapshots. That left content correctness unresolved and motivated the focused last-valid-book regression.

## Final full-host proof

### Maintained runtime binding

- Ledger: `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\hot\lane_iii_paper.sqlite3`
- Audit root: `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit`
- Epoch: `L3G-PAPER-EPOCH-BEELZEBUB_PONG_DISCERNMENT_V1-20260920T211544Z-ca2ceba7da11`
- Profile: `BEELZEBUB_PONG_DISCERNMENT_V1`
- Instrument: `MNQ DEC26` / `MNQZ6` on both host and NinjaTrader topology selectors
- Account/mode: `Sim101` / `LOCAL_SIMULATION`
- Independent collection: enabled
- Bounded activation: disabled
- Live trading authority: disabled
- FHB: disabled; maintenance excluded
- Configured periodic policy: `PERIODIC_FLAT_PROTECTED`, 20 ms

The active protection mode remained `EVENT_DRIVEN_PROTECTION` because the runtime bridge was unavailable. Periodic dispatch, completion, and evaluation counts were zero. This proof therefore does not qualify periodic scheduling.

### Measurement window and counts

| Field | Result |
|---|---:|
| Start | `2026-09-21T01:38:21.678022Z` |
| End | `2026-09-21T01:40:36.408546Z` |
| Duration | 134.730524 s |
| Durable journal range | 86,440–148,803 |
| Total durable records | 62,364 |
| DEPTH | 58,572 (434.734/s) |
| QUOTE | 1,672 (12.410/s) |
| TRADE | 2,120 (15.735/s) |
| Aggregate observed load | 462.879/s |
| Duplicate observation IDs | 0 |
| Journal chain breaks in window | 0 |
| Parser/admission rejects | 0 |
| Collector/Pong queue overflow delta | 0 |
| Producer overflow delta | 0 (`77,103 → 77,103`) |
| Producer transport-failure delta | 0 (`33,981 → 33,981`) |
| Queue depth start/mid/end | 0 / 0 / 0 |
| Collector receipt-vs-native latency | mean 19.791 ms; p95 68 ms; max 181.002 ms |

The journal was:

`N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\pong-authentic-hour\collectors\pong-collector-20260921T013634787154Z-d0f233622165\authenticated-observations.jsonl`

Internal source-sequence checks for the window:

| Stream | Source sequence | Gaps | Duplicate/out-of-order |
|---|---|---:|---:|
| DEPTH | 185,688–244,259 | 0 | 0 |
| QUOTE | 4,921–6,592 | 0 | 0 |
| TRADE | 6,021–8,140 | 0 | 0 |

Natural traffic therefore established sustainable transport/admission/durability only at the 462.879/s load actually observed. It is not a 2,111/s capacity qualification.

### Depth content result

| Depth check | Result |
|---|---:|
| Available records | 58,529 |
| Both bid and ask populated | 58,529 |
| Full 10×10 retained | 49,236 |
| Unavailable records | **43** |
| Unavailable reason | `CROSSED_NATIVE_POSITIONAL_SNAPSHOT` for all 43 |
| Malformed markers | 0 |
| Valid zero-price positional removes | 27,644 |

All 43 unavailable runs had length one. This shows validation resumed on the next callback; it does **not** prove that the positional book was coherently rebuilt.

Five representative seams were consecutive and free of source gaps or new overflow:

| Journal seq | Source seq | Authentic mutation | Prior best bid | Prior best ask |
|---:|---:|---|---:|---:|
| 87,603 | 186,798 | Bid Add position 0, 30,111.50, volume 1 | 30,110.75 | 30,111.25 |
| 92,264 | 191,291 | Bid Add position 0, 30,110.50, volume 1 | 30,109.50 | 30,110.25 |
| 93,189 | 192,014 | Bid Add position 0, 30,111.75, volume 2 | 30,110.25 | 30,111.00 |
| 93,678 | 192,469 | Bid Add position 0, 30,112.25, volume 5 | 30,111.25 | 30,112.00 |
| 95,691 | 194,409 | Bid Add position 0, 30,112.00, volume 1 | 30,111.00 | 30,111.75 |

Each cited mutation followed an accepted bid remove and was followed by further authentic positional mutations. The producer correctly emitted the crossed state as unavailable, but it kept the pre-cross retained cursor. Because the native positional book had already advanced, the next callback position was applied to a stale cursor. A later book could therefore become non-crossed and even 10×10 while remaining positionally misindexed.

## Caelum administrative finding and release condition

Caelum independently reviewed the source and receipts and returned **HOLD / NOT RESOLVED**.

The bounded release condition is:

1. On every rejected crossed mutation, either advance a private native positional working cursor while withholding publication, or latch the stream unavailable and publish no available snapshot until an authoritative reset/rebuild establishes a new cursor.
2. Make it impossible to apply the next positional callback to the pre-cross cursor.
3. Deterministically replay all five captured four-event seams, on both sides and relevant positions. The crossed event must remain explicit, every authentic event must remain consecutive, and the first later available snapshot must equal an independent application of every intervening mutation.
4. Run one fresh, brief, disarmed natural-feed proof that actually encounters crossed seams and shows zero replay/coherence mismatches, zero source gaps, zero overflow/rejects, and bounded/drained queues.

Suppressing the 43 unavailable records, weakening the crossed-book gate, fabricating reset state, sampling, or dropping authentic mutations is not an acceptable repair.

## Preserved startup faults and non-qualification state

The successful proof session inherited preserved startup loss from the source backlog before the measurement window:

- Producer overflow: 77,103.
- Producer transport failures: 33,981.
- `PONG_NATIVE_INPUT_SOURCE_SEQUENCE_GAP` remained latched.
- Missing-event totals: DEPTH 72,714; QUOTE 1,690; TRADE 2,089.
- Queue high-water: 17,805; maximum age: 11,782 ms.

Those counters and the latch were not cleared, so the overall session cannot qualify even though their deltas were zero during the post-drain measurement window.

Further retained blockers:

- `LEDGER_CAPACITY_INADEQUATE:DEGRADED` remained latched.
- Volume-history restoration remained refused; loaded windows were zero.
- Warm start was false.
- DEC26 daily bias was unavailable.
- No fresh accepted certificate existed.
- Periodic policy was configured but not active or evaluated.

The old authenticated volume-history receipt was not relabeled or substituted. A replacement requires the current source fingerprint, exact DEC26/provider policy, all required chunks and 2,016 bars, valid receipt HMAC/hash, no pending/error status, and a successful complete admission.

The required two immediately preceding completed UTC hourly bars may be retrieved after a supported resume if the current source and receipt contract admit them. A missed live qualifying interval cannot be recreated by later collection. The previously projected 20:05 MDT paper-start time is missed and invalid. No exact new paper-start time can be stated until the cursor defect is repaired, a fresh matching native build is loaded, current authenticated history/warmup completes, and a new complete eligible qualifying hour produces a fresh accepted certificate.

## Safety and account result

At the end of the proof:

- Runtime state: `READY_DISARMED` only; not activation authority.
- Sim101 position: flat, quantity 0.
- Working/owned entry orders: 0 / 0.
- Reconciliation: valid.
- Paper/session activation: disabled.
- Live authority: denied.
- Native orders, executions, and fills: 0.
- Order/activation commands: 0. A `COMMAND_RECEIPT_RECONCILIATION` ledger row was a signed reconciliation snapshot, not an order command.

One `INCIDENT_EXECUTION_TRANSPORT_FAULT` was retained at the earlier failed startup for port 48136. It predates the successful proof and is not a market-observation transport failure delta.

## Clean stop, drain, and ledger checkpoint

The supported stop command was used:

`scripts\stop_beezconsole.ps1 -Port 8090`

Final collector status:

| Field | Result |
|---|---|
| Collector | `pong-collector-20260921T013634787154Z-d0f233622165` |
| State | `STOPPED` |
| Error | null |
| Enqueued / written / durable | 229,584 / 229,584 / 229,584 |
| Queue depth | 0 |
| Collector overflow / rejected / duplicate | 0 / 0 / 0 |
| Final journal bytes | 544,704,791 |
| Journal tip | `089a2f0c1cd7b72e63ecaacf7bec5943b46c2a82c971bc32c8ce22a21b7daba2` |

Collector status receipt:

`N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\pong-authentic-hour\collectors\pong-collector-20260921T013634787154Z-d0f233622165\status.json`

- Bytes: 3,887
- SHA-256: `fd5fbafd1492fe2a63243f62aa717e235a922dfe4cdb98353e76727e2789eafc`

The ledger shut down cleanly at sequence 162,080 with tip:

`f043aff957fc820e47af80c4532168dce4cd2fa65f3084726990145db5636d1c`

Incremental verifier `full-host-continuity-closeout-20260921T0144Z` passed the chain, checkpoint, and risk-continuity guard through that sequence and tip.

Latest verification receipt:

`N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\ledger-verification-latest.json`

- Bytes: 2,540
- SHA-256: `b81a667aa63d2757fa70ed451f27483a57648adbb6571c38cd63633705f508de`

## Remaining processes and ownership

- NinjaTrader PID 17,208 remained running and operator-owned so the proven native feed session was not disrupted.
- No pass-owned BeezConsole or Python process remained.
- No listener remained on ports 8090, 48135, 48136, or 48137.
- No automated restart, heartbeat, or unattended monitor was installed.

## Final classification

- Native DEC26 transport availability: **PROVEN; not reopened**.
- Post-drain host ingress/admission/durability at observed natural load: **PASS**.
- Observation-sink policy-identity regression: **PASS**.
- Full DEPTH positional continuity: **FAIL / HOLD**.
- Qualification/paper readiness: **NOT QUALIFIED; no earliest valid paper-start time yet**.

