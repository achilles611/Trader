# Caelum Gate A review — London offline source/receiver correction

- Pass opened: `2026-09-22T08:02:06Z`
- Role: Caelum, superior administrative reviewer
- Source baseline: commit `9d56868e691854b097d25b9b245f5412e5185e25`, tree `736fb8dca7eaf630734ca1647829ad73cce0517c`
- Scope: offline review and evidence only
- Runtime owner: Sol; no runtime, ledger, install, reload, NinjaTrader compilation, recovery, or collection action is authorized by this review
- Final Gate A classification: `PASS_OFFLINE_GATE_A_INTEGRATION_ONLY`
- Operational classification: `HOLD_NO_NEW_R2_OR_RUNTIME_PROOF`
- Reviewed implementation: commit `a091a28186fc4a32685d93046b6883dcdfd1b6db`, tree `44e6620eedbced6145cce3682fce126a387f7b0a`
- Integrated evidence head before this report: commit `01ba94d7a7ce5884424d48a0054343e73b3150c8`, tree `28a7960a8817d64f042485cf64df1ee85996ccbe`

## 1. Controlling evidence

The following immutable evidence was read from the failed London packet and its stopped collector:

- `N:\Beelzebub\london-qualification-20260922T080000Z\01-R2-source-preparation.json`, SHA-256 `E34912EB88DB3BC68AE4305180643B81417CF0A00FF6811B966BA74F5D47D33F`.
- `N:\Beelzebub\london-qualification-20260922T080000Z\03-prestart-hold.json`, SHA-256 `D2513C03FABC12AF9AD86A2DE5044C6FFAA5B0260D71A249F55CCC56F3585F26`.
- stopped collector `status.json`, SHA-256 `AC0E3A6356248099380E663E03144CEB6DD7C7E9FB3EBE800CC180980C93882B`.
- stopped collector `authenticated-observations.jsonl`, 5,297,894 bytes, SHA-256 `473B7F0C863D6572E5D54386F1060F74C72171F41874E55DE83E051F81613EF5`.
- final supported Full verification `20260922T074726710984Z-lv-london-hour-prestart-hold-20260922T0746Z.json`, SHA-256 `CAD752F7B015A78D34299A0BFAF29AB6341CAE5A2FD1EDC8DACA6868474ECC05`.
- offline regression receipt `N:\Beelzebub\london-offline-repair-20260922\regression-results.xml`, SHA-256 `1E6602568D285109233DCCEEB90C0075FDC5B97FED787F81915AD465F8726CAE`.
- offline startup attribution `N:\Beelzebub\london-offline-repair-20260922\startup-attribution.json`, SHA-256 `87821F3C0F4EC94D2978DC0092540D4C5F41421A4974DFF8CEA9874610FFA613`.
- integrated operator evidence `docs/operations/evidence/london-offline-repair-20260922.md`, SHA-256 `CF662A4F4BBEAC36A1282A966EE08EEF8DCD44FA10B537E65D256178640AF050`.

The London hour did not start. Qualified duration is zero. R2 is `1/1`; a second reload attempt was explicitly prohibited. The supported stop and final Full PASS are accepted narrowly as safe closeout, not as source/receiver qualification.

## 2. Observed chronology and causal boundary

### OBSERVED

- The installed preparation produced expected observation source fingerprint `17995c2795e16bf52bedd9644a865261e3a20473f1e2bb7b75b4947bc558a525`, loaded build fingerprint `215e96a96913f37d3da3312c91f141d2e4d46f7b43aab37671a503cd9bc1a7d0`, producer session `8916026d85734e5097e50af83ca9fcb8`, and generation `1`.
- The collector started at `2026-09-22T07:41:56.651633Z` and durably wrote exactly 1,885 authenticated frames with collector queue overflow `0`, queue high-water `36`, maximum collector queue age `63 ms`, and final queue depth `0`.
- Journal record 1 carried native receipt time `2026-09-22T07:36:55.1018541Z` but collector receipt time `2026-09-22T07:41:56.806637Z`, approximately 301.705 seconds later. Its global sequence was `110549`, its DEPTH source sequence was `103378`, its producer queue-overflow counter was `54,250`, and its transport-failure counter was `23,309`.
- Journal record 2 carried source sequence `103379`. Journal record 3 carried source sequence `103914`, a missing interval of 534 DEPTH events immediately after the second admitted frame.
- The final durable journal frame carried native receipt time `2026-09-22T07:37:45.6891625Z`, collector receipt time `2026-09-22T07:42:00.671804Z`, global sequence `133175`, DEPTH source sequence `124554`, queue-overflow counter `71,468`, and transport-failure counter `28,926`.
- Full-chain analysis found missing sequence values of DEPTH `19,381` over 10 jumps, QUOTE `95` over 8 jumps, and TRADE `84` over 6 jumps. The earlier host value `534` was the first latched DEPTH gap, not the complete retained-journal gap count.
- Across the admitted stale corpus, producer overflow increased by `17,218` and producer transport failures increased by `5,617`. Offline attribution measured retained-frame age `254,982.642` to `301,710.783 ms`; the host recorded 1,796 freshness breaches. The native receiver failed closed.
- The prestart receipt recorded repository HEAD `15db37034bec3c4af504777f1e49c8dd5d4e9e50` while runtime binding reported `5998a8dd462f09e274183054c31ef6b34ee4535d`; exact runtime Git identity therefore correctly failed its freeze check. Those commits were created at `07:41:55Z` and `07:40:35Z`, respectively, but commit timestamps are not runtime launch timestamps. Their diff changed only the dated Caelum report, not executable code. This establishes an administrative same-worktree race, not an executable identity exception.

### INFERRED

The complete 1,885-record journal is startup backlog generated roughly 4 minutes 15 seconds to 5 minutes 2 seconds before collector receipt. It spans about 50.587 seconds of native production time but was drained in about 3.865 seconds after the collector began. The measured producer loss therefore occurred while production preceded receiver readiness. This is a concrete startup-order defect and invalidates the entire epoch.

The stopped collector's fast drain, exact durability, zero collector overflow, and 63 ms maximum queue age are narrow proof that the downstream collector could consume the retained backlog. They are not proof that the native producer/socket path can sustain contemporaneous natural-feed ingress without loss.

### UNKNOWN

- No post-catch-up natural-feed interval exists in this packet.
- Sustainable native enqueue-to-socket-to-receiver capacity remains unmeasured.
- The exact count or disposition of frames rejected or never written before the retained journal boundary is not recoverable from this packet.
- Loaded behavior after any proposed repair is unverified. Offline tests cannot establish loaded NinjaTrader identity, natural-feed continuity, or qualification.

Accordingly, it would be unsound both to relabel the loss as harmless startup history and to claim that the evidence proves a persistent steady-state capacity deficit.

## 3. Exact minimum correction admitted for implementation

Gate A accepts the following bounded offline correction implemented by `a091a28186fc4a32685d93046b6883dcdfd1b6db`:

1. Remove or fail closed the `-PrepareSourceOnly` path in `tools/commission_ninjatrader_observer.ps1` before compiler validation, installed-source mutation, compile waiting, or evidence that could be mistaken for loaded proof. Source preparation may not deliberately start the native producer before the independent receiver/collector exists.
2. Require receiver readiness for every `-InstallSource` request, whether or not `-RequireDistinctProducer` is also present. Re-evaluate readiness immediately before every installed-source mutation; a stale earlier check is insufficient.
3. Bind readiness to loopback listener `127.0.0.1:48135`, `LISTENING`, no listener error, collection `COLLECTING`, live collector thread, no collector fault, and collector configuration for the exact candidate source fingerprint and `MNQ DEC26` / `MNQZ6`. This is configured receiver readiness, not authenticated producer proof; producer authentication is necessarily post-load evidence.
4. Freeze and compare exact repository HEAD, tree, candidate source hashes, expected source fingerprint, collector identity, and expected instrument through the install boundary. A launcher must refuse a supplied expected HEAD/tree mismatch before any mutation, and the installer must recheck the frozen tuple immediately before writes.
5. Preserve the existing no-feed-reduction contract: no counter reset within a producer generation, no record discard, no queue enlargement offered as a capacity fix, no coalescing, no synthetic continuity, and no retry hidden inside the repaired path.
6. Preserve fail-closed behavior after activation: a disconnect, queue overflow, transport failure, fingerprint mismatch, contract mismatch, or source-sequence gap invalidates that generation. Reconnect may not resume a qualifying epoch across the gap.
7. For a two-file NinjaScript change, prevent a mixed old/new build from being accepted. Since the AddOn embeds the combined source fingerprint, the safe write order is staged/validated indicator first and fingerprint-bearing AddOn last, with the receiver already configured for the new fingerprint. AddOn-first can transiently assert the new combined fingerprint while executing an old indicator body and is not admitted.

This correction addresses the evidenced receiver-late recurrence. It does not claim to repair or prove native sustainable throughput.

## 4. Candidate review and evidence

The implementation commit is an exact descendant of the stated baseline and changes ten scoped documentation, launcher, receiver, analyzer, and test files. It changes no NinjaScript file. Its principal behaviors are:

- `-PrepareSourceOnly` refuses before native-file access, validation, backup/write, compile wait, or evidence publication.
- Every requested install requires an exact loopback receiver, error-free live collector, expected source fingerprint, exact requested instrument, matching runtime Git SHA, clean source paths, and frozen HEAD/tree/source hashes. The checks repeat before every changed-file write and after loaded-producer checkpoints.
- Unchanged installed files are not rewritten. If both native files differ, live installation is refused. Cold staging writes the indicator first and fingerprint-bearing AddOn last, preventing the AddOn from transiently asserting the new combined fingerprint against an old indicator body.
- `start_beezconsole.ps1` accepts paired expected HEAD/tree, checks them before and after startup and after runtime-binding retrieval, rejects dirty runtime source, freezes entrypoint hashes, and attempts no automatic restart on mismatch.
- Authenticated ordering refusals now retain bounded channel, session, current/prior sequence, prior-verification state, and wire hash. Admission behavior and reason prefixes are unchanged; unverified input receives no authenticated-context claim.
- The read-only analyzer validates the full retained hash chain/status identity and makes native-time attribution explicit. It reports `signature_reverified=false`, `sustainable_native_capacity=UNKNOWN`, `rejected_wire_identity=UNKNOWN_NOT_RETAINED`, and `qualified_seconds=0`.

The JUnit receipt records 188 test cases: 154 tests plus 34 parameterized subtests, with zero failures, errors, or skips in 58.08 seconds. Coverage includes receiver-first guards, frozen-launch guards, NinjaScript static contracts, rejection provenance, startup attribution, native observation, Lane III Phase F2, receiver safety, and launcher binding. `git diff --check` passed.

The installed AddOn remains SHA-256 `FFBB655D60A392E20DBEEA9DD8F8DB1ECBD4E68A1CD56B9C4F5A258F11E619E5`; the installed DLL remains `B70556DDCCE897C018255B7CFD49E2D5EC03B96CDF6A6EC6A0A06328A2CB8756`. These unchanged hashes confirm the offline pass did not install or compile the candidate into NinjaTrader. They do not prove that either installed artifact is currently loaded.

No sustained same-corpus or natural-feed measurement was required to accept this preventive offline integration. Such a run would be operational evidence and remains prohibited in this pass. Sustainable capacity, loaded identity, rejected-wire cause, and post-catch-up continuity therefore remain `UNKNOWN` by design.

## 5. Invariants not open to weakening

- Every market update and every durability acknowledgement remains required.
- Native exact 10-by-10 depth or honest `UNAVAILABLE` remains required; partial depth is not accepted as canonical.
- `<1,000 ms` source/host freshness and governing cadence remain unchanged.
- Periodic analytical ownership, event-driven protection, the five-second shadow, 32-tick reserve, single ordered owner, disabled helper, and no-FHB qualifying architecture remain unchanged.
- Capacity floors, N: destination measurement, disarmed/no-orders state, and ledger identity/history remain unchanged and outside this offline code gate.
- The failed London epoch remains failed; no baseline reset, backfill, relabel, or selective truncation can rehabilitate it.

## 6. Gate decision, holds, and remaining operational decision

`PASS_OFFLINE_GATE_A_INTEGRATION_ONLY`

- Evidence: implementation `a091a28186fc4a32685d93046b6883dcdfd1b6db` and evidence head `01ba94d7a7ce5884424d48a0054343e73b3150c8` satisfy the bounded offline criteria above; immutable tests pass and the attribution output preserves every limitation.
- Consequence: the offline correction is accepted in the repository. No additional offline Gate A work is held.
- Scope limit: this PASS does not establish installed, loaded, runtime-verified, natural-feed, capacity, ledger, recovery, latch, qualification, paper-start, arming, or order readiness.
- Owner: Sol remains integrator; Caelum's offline review is complete with this report.

`HOLD_NO_NEW_R2_OR_RUNTIME_PROOF`

- Evidence: the sole authorized R2 was consumed (`1/1`), the qualifying hour never began, and the current task expressly prohibits install, reload, compilation into NinjaTrader, recovery, and collection.
- Consequence: even a Gate A PASS authorizes only offline integration. It does not authorize another native reload, service start, latch recovery, collection, paper activation, arming, or orders.
- Release condition: Joseph must make a separate explicit operational decision that supplies new native-generation/reload authority and a new predeclared natural-feed proof plan. Before execution, Sol must freeze the final HEAD/tree through supported shutdown, resolve the documented cold receiver-first lifecycle, and predeclare exact identity, zero-loss/continuity/freshness, current flat/no-orders/reconciled/disarmed, actual-destination capacity, Full/recovery/second-Full, and no-rerun boundaries. No such authority is inferred here.
- Owner: Joseph for any future operational authorization; Sol remains sole operator if authorized.

The guarded installer is intentionally not a complete cold-start orchestrator. With two native files changed, NinjaTrader must be stopped for staging, yet its source-watcher/loaded checks need a separately coordinated authorized startup within the bounded wait. The runtime endpoint still omits tree and loaded-module hashes, and no wrapper can prevent a commit after it returns; a frozen operational worktree remains mandatory. The present loaded producer and old backlog cannot be resumed or rebaselined into a qualifying epoch.

## 7. Final disposition

Caelum confirms the integrated candidate without superseding Sol's evidence: `PASS_OFFLINE_GATE_A_INTEGRATION_ONLY`. It corrects the evidenced commissioning ordering and provenance weaknesses without weakening safety or inventing capacity. `HOLD_NO_NEW_R2_OR_RUNTIME_PROOF` remains fully in force. No runtime, install, reload, NinjaTrader compilation, ledger, recovery, collection, latch, arming, or order action was taken or authorized by this review.
