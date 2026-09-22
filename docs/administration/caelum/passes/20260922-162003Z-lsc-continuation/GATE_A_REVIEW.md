# Caelum Gate A review — LSC continuation

Review time: 2026-09-22T16:20:03Z
Role: `beelzebub_admin` concurrent specialist
Disposition: **PASS_OFFLINE_STANDALONE_IMPLEMENTATION_WITH_MANDATORY_CORRECTIONS**

This disposition permits only isolated implementation and tests for the new LSC
codec, segment store, independent verifier, and a separate shadow commitment
model. It does not approve edits to the existing runtime or authority ledger,
integration into the qualifying branch, installation, loading, deployment,
benchmarking on Atlas, NinjaTrader operation, runtime operation, ledger access,
paper operation, arming, orders, or qualification.

## Objective and authorization

Review the transferred Neptune pre-edit matrix before implementation. Preserve
the exact checkpoint, classify remembered throughput claims, and turn the LSC
plan into a lossless, bounded, shadow-only implementation contract. Sol remains
the sole integrator and sole runtime, deployment, benchmark, NinjaTrader and
ledger operator. Caelum owns only this record.

## Checkpoint verification

The former matrix-availability HOLD is released.

- Isolated worktree: `N:\Beelzebub\worktrees\atlas-lsc-20260922`
- Branch: `codex/lsc-v1-20260922`
- HEAD: `5455ad00cef07a042a2d607303cb404c70acc9d6`
- Tree: `e48af5cb308109b718a7cb214f407a354a3837e9`
- Sole parent: `64adc337b97f7d4b757176a18dc11235cef67897`
- The commit adds only
  `docs/architecture/lsc-v1-pre-edit-requirements.md`.
- Canonical Git blob: `6e535f2b0443c4727d6cfe26eee522c2a4d20528`,
  11,375 bytes.
- Windows working copy: SHA-256
  `FFDC2D24EEBD9D326B81486DBC71472EE8F2918CB30C48C3804C852DD13EF385`,
  11,498 bytes. `git hash-object` maps the CRLF materialization back to the
  exact canonical blob.
- Controlling handoff: SHA-256
  `8F22B9E3DB9F90F400CCAE05DBF766A48624D0E0222145026970326367C59F83`.
- The worktree was clean at review intake.

The matrix's Neptune host, Python, free-space and unknown-runtime statements are
historical checkpoint observations. They are not Atlas current-state proof.

## Source-lineage warning

The exact base is reproducible, but it is not the current operational source.
Base `64adc337...` is an ancestor of current Atlas evidence HEAD
`5d5a603dce1d806da153613fce16678af618130c` by 129 commits. Across the four
reviewed observation/commission/runtime/ledger files alone, current Atlas has
3,384 insertions and 272 deletions beyond the base. That drift includes current
DEC26 topology, receiver repairs, authentication and commissioning controls.

The old base still hard-codes SEP26 in `NinjaTraderContract`; its observation
object also predates current producer/build attestation fields. Therefore:

1. New standalone modules may be developed against stable, explicit value
   objects and immutable golden vectors at this checkpoint.
2. No old-base runtime/fanout/ledger integration may be promoted.
3. Before integration, Sol must port or rebase the standalone implementation
   onto the then-current Atlas source, resolve every conflict deliberately,
   bind DEC26/current producer identity, and repeat all parity, recovery,
   safety and performance proof on that exact candidate.

## Mandatory corrections to the matrix

### 1. Define lossless equivalence precisely

"Exact" means that decoding reconstructs, for every admitted passive MEVE, the
same observation kind, observation ID, channel/session/generation, local and
source sequence, all timestamps at original precision, source/build/protocol/
publication identity, contract identity, payload value **and JSON type**, and
the original payload and wire hashes. Recomputed canonical hashes must match.
Count and order parity alone are insufficient.

The codec may not truncate timestamps to microseconds. Current native evidence
can carry seven fractional decimal places. Use an exact integer time unit with
recorded precision or preserve the original normalized timestamp string so a
round trip is byte-semantically identical under the canonical hash contract.

### 2. Tick compaction needs a lossless escape path

MNQ prices may use integer ticks only when the record's explicit contract and
tick-size identity prove exact divisibility. Conversion must use exact decimal
arithmetic, never binary-float rounding. The format must retain enough type and
scale metadata to reconstruct `30035`, `30035.0`, and `30035.25` distinctly
when their canonical payload hashes differ.

An accepted passive event with an unknown field, unexpected numeric type, or
non-tick price may not be silently dropped, rounded, or rewritten. It must use
a bounded lossless typed escape encoding or cause an explicit LSC proof failure
while the authoritative runtime remains unaffected. Such a failure cannot be
counted as a successful segment.

### 3. The shadow tee must never obstruct authority

At the exact base, `ObservationFanout.on_observation` invokes the shadow sink
first while holding its ordering lock, then invokes paper. Synchronous LSC
encoding, compression, queue waiting, fsync or recovery there would delay the
paper/protection path.

The eventual parent-owned integration must perform only a bounded O(1),
nonblocking handoff at that boundary; no filesystem or compression work may run
under the fanout or runtime authority locks. A full queue, worker failure or
writer lag must increment durable/inspectable failure and overflow counters and
invalidate LSC qualification. It must not backpressure, suppress, reorder or
delay authority callbacks. Final queue drain does not erase an earlier loss or
lag.

### 4. Keep commitments out of the existing operational ledger

The handoff requires the existing operational ledger to remain untouched.
Initial segment commitments therefore belong in a separate append-only shadow
manifest/commit journal with its own identity, hash chain, fsync and verifier.
No LSC row, migration, table, index, checkpoint or recovery marker may be added
to an existing operational epoch during offline or shadow proof.

Proposed future authority-ledger commitment records and decision coordinates
may be designed and tested in isolated temporary ledgers, but integration is a
separate parent decision after proof. Historical epochs remain immutable. No
`VACUUM`, rewrite, backfill or synthetic coordinate is permitted.

### 5. Make the segment format self-describing and bounded

The format specification must fix magic, version, endianness, integer widths,
signedness, maximum record count, maximum canonical and compressed bytes,
maximum dictionary entries/string bytes, maximum nesting, exact timestamp
unit/precision, codec/compressor identities and hash domains. Unknown versions,
unknown kinds, malformed varints, integer overflow, trailing bytes and resource
limit breaches must refuse deterministically.

Dictionaries are per-segment committed content. Their order must be canonical,
and wrong, missing or substituted dictionaries must fail before records are
trusted. The decoder must enforce output limits before or while allocating and
decompressing; a post-decompression size check alone is insufficient.

### 6. Define platform-honest sealing and recovery

Temporary and final files must be in the same directory and volume. A sealed
segment requires complete canonical bytes, content commitment, data flush and
fsync, atomic same-volume replacement, and a supported parent-directory
durability step where the platform provides one. Windows/NTFS limitations must
be stated rather than converted into an unproved power-loss guarantee.

Recovery tests must use subprocess/crash injection at every declared boundary.
`.tmp`, truncated, uncommitted and commitment-only artifacts are never sealed
evidence. Quarantine must preserve bytes and reason; it may not delete or
silently repair them. Duplicate/reordered/missing segments and cross-session,
contract, build, generation or chain substitutions must fail closed.

### 7. Separate content identity from container identity

Commit at least the canonical uncompressed segment content, ordered event
coordinates/Merkle root, previous-segment commitment, schema/configuration,
producer/source/build/session/generation/contract identity, compressor identity
and compressed-container digest. Compression changes may not change semantic
content identity. Segment IDs and event indexes must be stable and unambiguous.

### 8. Require a genuinely independent verifier

The independent decoder/verifier must not import or call the production encoder
or production decoder. Golden vectors must be immutable byte fixtures with
recorded hashes. The verifier must compare reconstructed canonical payloads,
hashes, book checkpoints, evidence/hypothesis families, decisions/reasons and
final replay hash—not merely event counts.

### 9. Preserve authority and safety independence

Only passive `DEPTH`, `QUOTE` and `TRADE` MEVEs enter LSC. Account, health,
session, order, execution, position, command, risk, decision, reconciliation,
flatten, kill-latch and protection records stay individually readable and on
their existing durable paths. LSC failure must not delay protective stops,
emergency exits, flatten, callbacks, reconciliation or authority barriers.

The safety test must saturate, stall, corrupt and crash LSC while measuring
these authority paths. A source-only assertion is not proof.

### 10. Benchmark only the integrated current candidate

Correctness tests at this checkpoint may proceed. Performance acceptance may
not. The final comparison must use the exact same capture, pacing, contract,
producer/build, host state, configuration and physical storage class/path for
legacy and LSC, with one benchmark at a time. It must report all metrics listed
in the handoff, including tail latency, queue slope, loss, drain, recovery and
verification time. A finite drain or storage reduction alone is not capacity.

Use an isolated high-capacity N: temporary/evidence root for eligible tests;
retain production 8 GiB warning and 4 GiB hard capacity floors. Measuring N:
does not authorize changing an operational ledger location or identity.

## Throughput-claim classification

These classifications distinguish requirements from measured performance and
retain the scope of each historical receipt.

| Figure / claim | Classification | Disposition |
| --- | --- | --- |
| Historical arrival range `1,345-1,689/s` | **OBSERVED, historical** | Located prior review records the range; it is sampled arrival evidence, not current sustained demand or LSC capacity. |
| `1,689 x 1.25 = 2,111.25`, rounded to `2,111/s` | **INFERRED derivation** | The arithmetic/headroom proposal is inferred from the historical peak. It is not measured throughput. |
| `2,111/s` acceptance floor | **OBSERVED requirement; current performance UNKNOWN** | The current handoff expressly preserves it. Historical contract `H:\Trader-pong-serial-measurement\docs\operations\pong-serial-acceptance-v1.json`, SHA-256 `B502A94820C3C3F10FC8B4D8E8C618027394261E6BC828E5789CF56D4A6266FD`, defines three sustained and three burst repeats with every run passing. No LSC candidate has been measured. |
| Old-base test targets `1,259/s` steady and `>1,500/s` drain | **OBSERVED test constants; SUPERSEDED as LSC acceptance** | They remain unit/integration test coordinates only and cannot replace 2,111/s. |
| Admission boundary about `1,445.8/s` | **OBSERVED, historical secondary receipt** | Current operations report attributes the first full-host boundary to `LaneIIIPaperRuntime.ingest`. It is not a same-candidate LSC baseline. |
| Whole-path `1,536.53/s` and analytical owner `1,783.80/s` | **UNKNOWN for this review** | Remembered figures were not tied to an inspected raw receipt in this pass. Do not promote them from memory. |
| Full-host `~1,112 frames/s over 134.73 s` | **INFERRED / unsupported** | The inspected 134.730524 s report instead records 62,364 records, or 462.879/s. The remembered 1,112/s mix is not accepted. |
| Natural full-host `462.879/s` | **OBSERVED, historical** | Clean transport/admission/durability only at the offered load; explicitly not 2,111/s capacity. |
| Producer overflow `+44,135` and transport failures `+32,686` | **OBSERVED, historical failed cut** | Preserved in the no-FHB qualifying-cut record. These are failure evidence, not demand/capacity. |
| Host queue peak at least `7,430` | **OBSERVED, historical failed cut** | The recorded queue did not drain during the measurement; not an LSC result. |
| Collector queue `0 -> 869 -> 0` | **OBSERVED, historical failed cut** | Eventual drain does not cure the simultaneous producer loss or qualify capacity. |
| Drain `6.22-6.65 s` | **UNKNOWN for this review** | No exact inspected raw receipt was located; do not use it. |
| Operational ledger `>1 GiB` / approximately `28.9 GiB` | **OBSERVED historical, current state UNKNOWN** | The source document described an unavailable historical ledger. It is not a current Atlas measurement and LSC must not touch it. |
| Prior `5.15x` target or a new `1,000/s` floor | **SUPERSEDED / unauthorized** | Neither is revived by this handoff; neither may replace the explicit 2,111/s requirement. |
| LSC ingress, admission, durable, decode and replay rates | **UNKNOWN** | No implementation or benchmark exists. |
| LSC bytes/MEVE and compression ratio | **UNKNOWN** | No implementation or benchmark exists; reduced bytes would not prove losslessness or qualification. |

## Gate disposition

| Gate | State |
| --- | --- |
| Neptune checkpoint sealed | **PASS** — exact one-file descendant and canonical blob verified |
| Matrix transferred and hash-verified | **PASS** |
| Requirements review | **PASS WITH MANDATORY CORRECTIONS** — this record |
| Standalone codec/store/verifier implementation | **AUTHORIZED OFFLINE ONLY** |
| Existing fanout/runtime/ledger integration | **HOLD** |
| Lossless parity | **NOT RUN** |
| Crash recovery | **NOT RUN** |
| Safety isolation | **NOT RUN** |
| 2,111/s performance requirement | **NOT RUN / UNKNOWN** |
| Integrated / installed / loaded | **NO / NO / NO** |
| Preflight / qualification | **NOT RUN / NOT STARTED** |
| Account / position / orders / arming | **UNKNOWN in this pass** — no runtime observation authorized |
| Live-capital authority | **DENIED by controlling handoff** |

## Holds and release conditions

### HOLD_LSC_SHARED_INTEGRATION_NOT_PROVEN

- **Evidence:** 129-commit source drift; old-base SEP26 and pre-attestation
  structures; synchronous shadow callback under the fanout lock; commitment
  destination not yet separated from the operational ledger.
- **Consequence:** no edits to existing fanout/runtime/ledger may be promoted
  from this branch and no operational integration may occur.
- **Release:** implement and test standalone contracts with the corrections
  above; then port onto current source, produce a reviewed diff proving
  nonblocking safety and separate shadow storage, and pass focused/full tests.
- **Owner:** Sol for shared integration; specialists for their disjoint new
  modules.

### HOLD_LSC_ASIA_QUALIFICATION_NOT_PROVEN

- **Evidence:** LSC is not implemented, integrated, installed, loaded, parity
  proved, crash-recovery proved, safety proved or performance qualified.
- **Consequence:** no preflight PASS, qualification start, paper start, arming
  or orders can be inferred from this review or from the clock.
- **Release:** every mandatory proof gate in the controlling handoff passes on
  the exact current candidate, followed by clean current-state preflight and
  the parent's evidence-based decision at the declared cutoff. If incomplete,
  retain this HOLD without moving the window or falling back silently.
- **Owner:** Sol as sole integrator/operator; Caelum reviews final evidence.

## Next action

Specialists may implement only the corrected standalone codec, segment store,
independent verifier and isolated shadow commitment model, each with disjoint
ownership and focused tests. Sol should freeze their exact commits and evidence
before any shared integration. Caelum must review implementation evidence and
the current-source port before Gate A can be extended beyond offline standalone
work.

No tests or benchmarks were run by Caelum. No runtime, ledger, NinjaTrader,
deployment, branch or qualification action occurred in this review.
