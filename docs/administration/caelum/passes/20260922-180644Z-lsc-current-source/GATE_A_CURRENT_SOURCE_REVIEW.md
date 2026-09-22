# Caelum Gate A current-source review — LSC v1

Review time: 2026-09-22T18:13:14Z
Role: `beelzebub_admin` concurrent specialist
Exact reviewed HEAD: `1c490167fe926dffc456da55b6ac0ce223428a8e`
LSC base core commit: `3c839a43b04b65b8943aec934343897c0f5e0b46`
Final depth-price correction: `1c490167fe926dffc456da55b6ac0ce223428a8e`
Disposition: **PASS_GATE_A_CURRENT_SOURCE_FOR_DEFAULT_OFF_INTEGRATION_IMPLEMENTATION**
Operational disposition: **HOLD_SHARED_INTEGRATION_ACCEPTANCE_AND_ALL_RUNTIME_OR_QUALIFICATION_USE**

The current-source standalone LSC codec, bounded segment store, independent
verifier, and separate shadow commitment journal satisfy Gate A sufficiently
for Sol to begin the parent-owned, default-off shared integration
implementation. This is a release to implement that integration, not evidence
that integration exists or has passed. No installation, loading, deployment,
runtime use, benchmark, ledger operation, paper start, arming, order, or
qualification is released by this review.

## Authority and scope

Sol remains the sole integrator and sole runtime, deployment, benchmark,
NinjaTrader, and ledger operator. Caelum reviewed source and immutable offline
evidence only and owns only this report. No implementation file was edited by
Caelum. No runtime, ledger, NinjaTrader, deployment, benchmark, account,
position, order, or qualification action occurred in this review.

The applicable project `AGENTS.md`, Caelum `CHARTER.md`, and Caelum
`CONCURRENCY.md` were read before review. The earlier Neptune requirements
matrix is present at `docs/architecture/lsc-v1-pre-edit-requirements.md`; its
old-base implementation HOLD was satisfied by the current-source port, but its
operational proof requirements remain controlling.

## Exact checkpoint and evidence

- Worktree: `N:\Beelzebub\worktrees\atlas-lsc-current-20260922`
- Branch: `codex/lsc-v1-current-20260922`
- Reviewed HEAD: `1c490167fe926dffc456da55b6ac0ce223428a8e`
- Reviewed tree: `21ba699e729bfc78b59b87155e6116d2ecbef7d2`
- Sole parent: `8d716c702f2f6c1e3077f65ae773dd4de900fbdc`
- The tracked source was clean at exact-commit inspection; this report is the
  only untracked review output.
- `3c839a43...` is an ancestor of the reviewed HEAD. Commit `8d716c7...`
  corrects current receiver telemetry allowlisting. Commit `1c49016...`
  applies the final LSC depth-price path correction for
  `mutation_resolved_price`, `position_bids`, and `position_asks`.
- Final focused JUnit receipt:
  `N:\Beelzebub\lsc-v1-tests-20260922\parent-current-price-path-final\results.xml`
  - SHA-256:
    `B85295E5979286E368D0AF298582A06DF679B260CED5C96CEA7BB9FCA3F992A9`
  - 40 named cases plus 37 subtests, 77 total, zero failures, zero errors,
    zero skipped.
  - Receipt timestamp: `2026-09-22T12:10:49.416122-06:00`.
- Clean predecessor Full receipt:
  `N:\Beelzebub\lsc-v1-tests-20260922\parent-current-full-final\results.xml`
  - SHA-256:
    `6765473FDB30E9B769B39B305B15BD5AD4814A09F849C0E94D62D5AB91FF83D0`
  - 1,692 named cases plus 608 subtests, 2,300 total, zero failures,
    zero errors, zero skipped on immediate predecessor `8d716c7...`.
  - Receipt timestamp: `2026-09-22T12:04:17.617257-06:00`.
- Current independent golden verification:
  `N:\Beelzebub\lsc-v1-tests-20260922\current-price-path-final-20260922\independent-golden-verification.json`
  - SHA-256:
    `84561709574196C8AB6E79D755FEBB08694BD64A3163B10533FA27811B34FC1A`
  - Exact committed golden-vector bytes SHA-256:
    `D2AFDD47EE5F63821C5E8B827143EB5F510F78BE8C135B79E975781C851E3A62`
    (Git blob `01c2cdfb3df6f9388f54e076701792aca37eeee9`).
  - Result `PASS`, three mixed-kind records, context digest
    `b3d4ac9631d1ceafd94e734d4403867b3ece916659f85a93d39200b8f2538630`.
  - Caelum regenerated the verifier result in memory from the exact committed
    golden vectors; the bytes matched this artifact exactly.
  - The QUOTE record uses current policy
    `EVENT_DRIVEN_SAME_CALLBACK_QUOTE_V1`, native contract `MNQ DEC26`, and
    canonical contract `MNQZ6`.
- The earlier `verifier\golden-verification.json` receipt with a combined
  `MNQ DEC26-MNQZ6` identity and old QUOTE policy is **excluded as stale**.

## Current-source invariant review

| Requirement | Finding | Gate A state |
| --- | --- | --- |
| Current L3F3 schema | The codec imports the current `L3F3_SCHEMA` and reconstructs the current `NinjaTraderObservation` / producer-attestation types. Legacy L3F2 is explicit and fail-closed. | **PASS, source/offline** |
| Attestation, signature, and signed-wire identity | L3F3 encoding requires `signature_verified is True`, retains the full producer attestation and signature, reconstructs exact signed-wire bytes, and verifies wire/payload hashes and canonical re-encoding. The codec does not possess the producer HMAC key and therefore does not independently reauthenticate HMAC; integration must feed it only observations already authenticated by the current admission boundary. | **PASS WITH INTEGRATION CONDITION** |
| Exact semantic types and timestamps | Golden coverage retains strings versus floats, negative-zero float bits, nested values, seven-digit timestamps, payload hashes, observation IDs, and unknown bounded extensions. | **PASS, synthetic goldens** |
| Mixed-kind global order | QUOTE, TRADE, and DEPTH remain in one strictly increasing local sequence; stream and publication policy stay per record. Store verification rejects non-increasing or cross-segment sequence drift. | **PASS, source/offline** |
| Native and canonical contract identity | Context and decoded records bind native `MNQ DEC26` separately from canonical `MNQZ6`; substitutions refuse. | **PASS, source/offline** |
| Current publication identity | The corrected QUOTE golden uses the active paired policy `EVENT_DRIVEN_SAME_CALLBACK_QUOTE_V1`; TRADE and DEPTH retain their current event-driven policies. | **PASS, corrected** |
| Depth and nested wrapper fidelity | Independent verification covers exact depth mutation fields, positional ladders, book checkpoint, and typed nested values. | **PASS, synthetic goldens** |
| Compressor identity | The header now binds the actual commissioned runtime and explicit parameters: `zlib-runtime-1.3.1-deflate-wbits15-mem8-level6-default`. Different identities refuse instead of claiming a false `1.2.13` writer. | **PASS, corrected** |
| Bounded immutable storage | Record/context/container limits, bounded decompression, trailing/truncated refusal, content and container commitments, Merkle ordering, atomic same-volume publication, and temp quarantine are implemented. Windows uses a truthful write-through replacement contract and does not claim unsupported directory-fsync semantics. | **PASS, source/offline** |
| Storage provenance | Resolved store, segment, and independent journal roots are identity-bound. Aliases, root substitution, cross-identity chains, missing segments, gaps, forks, and uncommitted references refuse. | **PASS, corrected** |
| Separate shadow commitments | `ShadowCommitmentLog` is independent of the authority ledger and resolves exact segment/event/local-sequence provenance. | **PASS, source/offline** |
| Existing `PaperLedger` unchanged | `src/l3g_paper/ledger.py` has identical base/current blob `1eefcf28d2f94450871228af007f273e7f8aae4d`; `src/l3g_paper/runtime.py` likewise remains `9b1761bd8167620c218b891cc7e864b190061f70`. | **PASS** |
| Current observation source unchanged by LSC | `src/l3f_provider/ninjatrader_observation.py` remains blob `03ef14e33129f4c1e5ca0eb7f8c2ac2a7b8332d9`. | **PASS** |
| Default-off / no runtime / no order boundary | No LSC reference or wiring exists in `src/l3g_paper`, `src/l3f_provider`, scripts, or config. LSC modules own no socket, broker action, authority ledger, or order surface. | **PASS; implemented standalone, not integrated** |
| Independent verifier | The verifier does not import the production codec/store and recomputes the bounded byte contract, hashes, types, identities, ordering, depth checkpoint, and proof fields. | **PASS, goldens** |

## Implemented, integrated, installed, loaded, verified

| State | Finding |
| --- | --- |
| Standalone source implemented | **YES** at `3c839a43...`, with final depth-price correction at `1c49016...` |
| Current-source port committed | **YES** |
| Shared runtime/fanout integration implemented | **NO** |
| Integration accepted | **NO — HOLD** |
| Installed | **NO / not evidenced** |
| Loaded | **NO / not evidenced** |
| Runtime exercised | **NO / not authorized** |
| Existing operational ledger touched | **NO in reviewed source; no ledger operation authorized** |
| Real representative-corpus parity | **NOT RUN** |
| External-process crash recovery | **NOT RUN** |
| 2,111/s performance qualification | **NOT RUN / UNKNOWN** |
| Asia or other session qualification | **NOT STARTED / NOT AUTHORIZED** |

## Throughput-claim classification

No performance benchmark was run in this review. Unit tests of the benchmark
evaluator use synthetic passing receipts and are not measured capacity.

| Figure / claim | Classification | Current disposition |
| --- | --- | --- |
| Historical arrival `1,345-1,689/s` | **OBSERVED, historical** | Demand evidence only; not current LSC performance. |
| `1,689 x 1.25 = 2,111.25`, rounded to `2,111/s` | **INFERRED derivation** | Headroom arithmetic, not achieved throughput. |
| `2,111/s` acceptance floor | **OBSERVED requirement; achieved state UNKNOWN** | Preserved as the acceptance target. No current-candidate performance receipt exists. |
| Repository targets `1,259/s` steady and `>1,500/s` drain | **OBSERVED test constants; SUPERSEDED as LSC acceptance** | They cannot replace 2,111/s. |
| Prior offline service near `409.547/s` | **OBSERVED, historical; SUPERSEDED for this candidate** | Receiver-path evidence, not LSC capacity. |
| Admission boundary about `1,445.8/s` | **OBSERVED, historical secondary receipt** | Not a like-for-like current LSC measurement. |
| Whole-path `1,536.53/s` and analytical owner `1,783.80/s` | **UNKNOWN for this review** | No current inspected raw receipt; not promoted from memory. |
| LSC ingress/admission/encode/durable/decode/replay rates, tail latency, bytes/MEVE, and compression ratio | **UNKNOWN** | Not benchmarked. |

## Gate release

**Default-off shared shadow integration implementation may proceed under Sol's
ownership.** The integration must:

1. Attach only after current L3F3 authentication/admission succeeds.
2. Preserve the one global local sequence across mixed QUOTE/TRADE/DEPTH.
3. Perform only a bounded O(1), nonblocking handoff at the fanout boundary; no
   codec, compression, filesystem, queue wait, or fsync work may execute under
   the fanout or authority-runtime locks.
4. Keep the feature default off and identity-bind exact source/build/session/
   generation/contracts/configuration and the actual resolved N: destination.
5. Keep segment and shadow-commitment persistence completely separate from
   `PaperLedger` and all operational epochs.
6. Make queue full, worker failure, lag, overflow, storage refusal, or chain
   failure durably visible and invalidate LSC proof without delaying,
   suppressing, reordering, or authorizing the authority path.
7. Preserve all current capacity floors. N: may hold eligible test/temp/shadow
   artifacts, but this review does not authorize operational-ledger migration.

## Remaining HOLDs

### HOLD_LSC_SHARED_INTEGRATION_ACCEPTANCE_NOT_PROVEN

- **Evidence:** no LSC runtime/fanout wiring exists at the reviewed HEAD, so no
  integrated safety, ordering, queue, shutdown, or failure-isolation test has
  run.
- **Consequence:** implementation may proceed, but it may not be called
  integrated, installed, loaded, runtime verified, or qualification ready.
- **Release:** Sol freezes the exact default-off integration diff and passes
  focused plus affected safety tests proving authenticated placement,
  nonblocking bounded handoff, exact mixed-kind order, failure isolation,
  separate storage/commitments, deterministic drain, and unchanged authority
  behavior.
- **Owner:** Sol as sole integrator; Caelum reviews the frozen evidence.

### HOLD_LSC_EXTERNAL_CRASH_RECOVERY_NOT_PROVEN

- **Evidence:** segment and commitment recovery tests inject exceptions through
  same-process fault hooks. No subprocess termination, process-abort, or
  power-loss-equivalent harness receipt was produced.
- **Consequence:** deterministic fault-boundary behavior is proved narrowly;
  crash durability and restart recovery are not qualified.
- **Release:** run supported external-process crash injection at every declared
  seal and commitment boundary on the exact integrated candidate, preserve all
  artifacts, and independently verify quarantine, durable prefixes, and no
  false seals.
- **Owner:** Sol for authorization/execution; storage specialist for fixtures;
  Caelum for evidence review.

### HOLD_LSC_REPRESENTATIVE_PARITY_NOT_PROVEN

- **Evidence:** three immutable synthetic goldens pass, but no current captured
  representative native corpus was compared legacy-versus-LSC.
- **Consequence:** real-corpus lossless equivalence remains unproved.
- **Release:** independently compare the exact same admitted current corpus and
  pacing, including mixed kinds, native depth, unknown bounded fields, exact
  timestamps/types/hashes/provenance, decisions/reasons, and final replay
  proof; zero mismatches and zero loss are required.
- **Owner:** Sol as sole evidence operator; independent verifier supplies the
  comparison.

### HOLD_LSC_2111_PERFORMANCE_NOT_PROVEN

- **Evidence:** there is no current raw benchmark receipt. Evaluator unit tests
  exercise synthetic receipt shapes only.
- **Consequence:** no capacity, latency, storage, drain, or qualification claim
  may be made.
- **Release:** on the exact integrated candidate, run at least three sustained
  and three burst like-for-like repeats at the preserved 2,111/s floor, with
  zero loss/gaps/overflow, stable queue, exact identity, stage rates and tail
  latency, storage metrics, deterministic drain/recovery, and independent Full
  verification. Use eligible N: test/evidence paths and keep capacity floors.
- **Owner:** Sol as sole benchmark/operator; Caelum reviews receipts.

### HOLD_EXACT_FINAL_HEAD_FULL_RECEIPT_NOT_PRODUCED

- **Evidence:** the clean Full receipt covers immediate predecessor
  `8d716c7...` (2,300/2,300), and the exact final LSC delta at `1c49016...` has a
  clean 77/77 focused receipt. A whole-repository Full receipt was not rerun
  after the final six-file depth-price correction.
- **Consequence:** the bounded current-source Gate A release is supported by a
  clean predecessor Full plus a clean exact-delta focused run, but the final
  HEAD cannot yet claim its own clean Full.
- **Release:** produce a clean Full receipt on exact HEAD `1c49016...`, with
  source/tree identity and artifact hashes recorded.
- **Owner:** Sol.

### HOLD_LSC_RUNTIME_AND_ASIA_QUALIFICATION_NOT_PROVEN

- **Evidence:** LSC is not integrated, installed, loaded, runtime exercised,
  representative-parity proved, externally crash-recovery proved, safety
  proved, performance qualified, or Full-clean receipted. This review made no
  current account, position, order, ledger, latch, or runtime observation.
- **Consequence:** no preflight PASS, qualifying-hour start, latch recovery,
  paper start, arming, or order authority can be inferred from this Gate A
  review or from elapsed time. Runtime/account/order state remains **UNKNOWN**.
- **Release:** every preceding HOLD passes on one exact current candidate, then
  Sol performs the separately authorized current-state preflight and receives
  the required qualification disposition. No evidence fabrication, relabeling,
  replay substitution, moved window, or stale backlog is acceptable.
- **Owner:** Sol as sole runtime/ledger/operator; Caelum reviews final evidence.

## Administrative conclusion

The 129-commit old-base drift is no longer the blocking issue: the standalone
implementation is now ported to and frozen on current source with current L3F3,
signed-wire, contract, publication, depth, storage, and shadow-journal
identities. The prior zlib, storage-root, QUOTE-policy, depth, nested-wrapper,
and receiver-allowlist defects are corrected in the reviewed lineage and the
final focused evidence is clean.

Accordingly, the narrow Gate A release is **YES** for Sol to implement the
default-off shared shadow integration. Every operational and qualification
gate remains **HOLD** until the evidence and release conditions above are met.
