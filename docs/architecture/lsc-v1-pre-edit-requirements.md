# LSC v1 pre-edit requirements matrix

Date: 2026-09-22 (America/Denver)
Pass state: PRE-EDIT / CAELUM REVIEW REQUIRED
Safety boundary: offline and shadow-only; no NinjaTrader operation, paper or
live runtime, broker action, deployment, or operational-ledger mutation is
authorized.

## Checkout and host baseline

- Repository: `https://github.com/achilles611/trader.git`.
- Integration worktree: `C:\Users\Administrator\Documents\ChatGPT\Beelzebub\atlas-lsc`.
- Branch: `codex/lsc-v1-20260922`, based on
  `origin/codex/lane-iii-perpetual-corrective-20260906`.
- HEAD: `64adc337b97f7d4b757176a18dc11235cef67897` (`fix(lane-iii): wait through
  transient Scalper freshness`). The worktree was clean before this matrix.
- No applicable `AGENTS.md`, Caelum charter, project charter, concurrency
  protocol, or `docs/administration` tree exists in the checkout or the supplied
  handoff directory. The supplied LSC handoff is therefore the only pass-specific
  governance source; this absence is an explicit review gap, not permission to
  invent policy.
- Host: Windows 11 `10.0.26200`, healthy 256 GB NVMe/NTFS system volume with
  47,055,814,656 bytes free at inspection.
- The default `python` resolves to GIMP Python 3.12.11 and cannot import
  `_sqlite3`. Verification must use the Codex bundled Python 3.12.14
  (`sqlite 3.53.1`) or a repository-local ignored virtual environment.
- No `.env` or `BEELZEBUB_*` process variables were present. Active operational
  ledger, epoch, profile, audit root, and installed runtime identity are UNKNOWN.
- Baseline tests with bundled Python: all 11
  `tests.test_l3g_ledger_throughput_recovery` tests passed; the F2/F3 modules
  could not import because the bundled environment lacks `requests` and
  `PyYAML`. This is an environment dependency failure, not a source failure.

## Current architecture facts

- `NinjaTraderObservation.from_wire` validates the exact L3F2 JSON shape;
  `NinjaTraderSessionLedger.admit` is the duplicate/session/monotonic-order
  admission boundary (`src/l3f_provider/ninjatrader_observation.py:187-264,
  282-330`).
- The commissioning harness calls downstream only after `accept_observation`
  returns an admitted record (`src/l3f_provider/ninjatrader_commission.py:258-280`).
- `ObservationFanout` serializes shadow and paper callbacks under one lock and
  isolates sink failures (`src/l3g_paper/runtime.py:247-367`). This is the only
  safe shadow tee point; LSC must never create a second socket owner.
- Paper ingestion currently creates a compact envelope without the raw payload
  (`src/l3g_paper/runtime.py:3805-3877`). Production `PaperLedger` defaults
  `persist_high_frequency_records=False` and suppresses raw passive observation,
  evidence, and no-effect decision records (`src/l3g_paper/ledger.py:1119-1169,
  2922-2974`). The authority-ledger policy documents that behavior
  (`docs/operations/lane-iii-authority-ledger-storage.md`).
- Test-only high-frequency mode persists each item twice as canonical JSON (the
  global audit table and a domain table) in one SQLite transaction and hash chain
  (`src/l3g_paper/ledger.py:2750-2786`). This is the legacy comparison path, not
  an authorized production configuration.
- Synchronous authority append flushes the deferred prefix before any authority
  row; operational domains select `synchronous=FULL`. Controlled close seals
  admission, drains, joins writer/checkpoint workers, verifies the exact durable
  tip, and performs the final TRUNCATE checkpoint (`src/l3g_paper/ledger.py:
  2305-2324, 3974-4125`). LSC cannot weaken or overtake these boundaries.
- Decisions currently retain observation IDs, local sequences, and payload
  hashes, but no segment coordinate (`src/l3g_paper/ledger.py:150-158`). Runtime
  seed provenance retains complete admitted observations in memory
  (`src/l3g_paper/runtime.py:661-769`).

## Historical claim classification

These labels describe evidence available in this checkout, not the truth of an
unavailable Atlas runtime. The supplied handoff alone is not treated as its own
benchmark receipt.

| Claim | Label | Current receipt / conflict |
| --- | --- | --- |
| Qualification contract 2,111 records/s | UNKNOWN | No receipt in the checkout. The current benchmark instead targets 1,259/s steady admission and requires drain above 1,500/s (`tests/test_l3g_ledger_throughput_recovery.py:1367-1416`). The handoff's higher target remains the acceptance target until Joseph/Caelum resolves scope. |
| Admission-only ceiling ~1,445.81/s | UNKNOWN | No matching artifact or source value found. |
| Best parent/full-path ~1,536.53/s | UNKNOWN | No matching artifact or source value found. |
| Analytics ceiling ~1,783.80/s | UNKNOWN | No matching artifact or source value found. |
| Full-host inferred mix ~1,112 frames/s over 134.73 s | UNKNOWN | No source capture or calculation found. |
| Producer overflow +44,135 | UNKNOWN | No source capture found. |
| Transport failures +32,686 | UNKNOWN | No source capture found. |
| Host queue peak 7,430 | UNKNOWN | No source capture found. |
| Collector queue 0 -> 869 -> 0 | UNKNOWN | No source capture found. Historical F-phase documentation independently mentions 869 L2 trades, which is not this queue claim. |
| Drain 6.22-6.65 s | UNKNOWN | No matching benchmark report found. |
| Operational ledger exceeded 1 GiB | OBSERVED (historical, unavailable) | `docs/operations/local-ledger-verification.md:166-172` reports an approximately 28.9 GB operational ledger but explicitly says it was unavailable to that workspace. No current ledger was opened or measured. |

No claim above may be used as a measured LSC baseline without its raw receipt.

## Requirements-to-proof matrix

| Invariant / gate | Current path | Proposed owner | Required proof | Acceptance receipt |
| --- | --- | --- | --- | --- |
| Every admitted passive MEVE is preserved exactly and in order | `accept_observation` then `ObservationFanout.on_observation` | A: codec; parent: tee | Golden/property round trips plus independent event-by-event comparison using observation ID, session, sequence, timestamps, payload and hash | Corpus parity JSON with counts and first mismatch absent |
| Admission semantics are unchanged | `NinjaTraderSessionLedger.admit` | Parent | Prove LSC receives only non-duplicate records returned by `accept_observation`; rejection/duplicate counters remain separate | Listener/fanout integration tests |
| Only passive `QUOTE`, `TRADE`, `DEPTH` are compacted | `_PASSIVE_MARKET_OBSERVATION_TYPES`; authority classifications | A + Caelum | Strict kind whitelist and rejection of account, health, order, execution, position, session, decision, risk and command records | Codec/store rejection matrix |
| Canonical encoding is deterministic and versioned | No current binary format | A | Document magic/version/schema/endianness, bounded integers, UTC microseconds, exact decimal/tick rule, dictionary order, hash domains, limits and unknown-version refusal | Architecture spec plus immutable golden vectors |
| Exact payload survives without float ambiguity | `NinjaTraderObservation.payload`; `canonical_hash` | A | Canonical typed JSON representation; decimal price fields validated as exact 0.25 ticks while preserving original payload value/type | Boundary and malformed-payload tests |
| Segment resource use is bounded | No current segment store | A/B | Maximum record count, canonical bytes, compressed bytes, age and decompressed output; no unbounded allocation | Limit/overflow/decompression tests |
| Segment durability precedes authority commitment | Current ledger has no segment commitment | B/C; parent integrates only after Gate D | Temp create, write, flush, fsync, atomic replace, directory sync where supported; commitment API refuses absent/unverified segment | Crash-boundary matrix and commitment-without-segment test |
| Partial segments never masquerade as sealed | No current segment store | B | `.tmp` is never readable as sealed; startup quarantines incomplete/corrupt temp files without data claim | Recovery receipts for every write boundary |
| Cryptographic chain and per-event resolution | Current ledger hash chain only | A/B/C | SHA-256 domain-separated content digest, ordered Merkle root, previous segment commitment, index/checkpoints and exact `(segment_id, event_index, local_sequence)` lookup | Chain/corruption/substitution tests |
| Decisions resolve to exact evidence | IDs/sequences/hashes only today | C/D; parent gated integration | Verification API resolves existing provenance tuples to reconstructed events and later supports explicit coordinates without rewriting old epochs | Evidence lookup and legacy-readability tests |
| Authority ordering/durability is unchanged | `PaperLedger.append`, barriers, FULL/NORMAL selection | Parent | No authority record uses passive queue/store; LSC saturation cannot delay protection, flatten, reconciliation or callbacks | Barrier and saturation/failure tests |
| Shadow failure is visible and fail-closed for proof | Fanout currently isolates sink errors | B/parent | Bounded queue and health telemetry; failure invalidates shadow proof, never becomes a success or hidden drop | Injected failure/backpressure tests |
| Shutdown drains/seals deterministically | `PaperLedger.close` exact-prefix receipt | B/parent | LSC seals admitted shadow prefix before returning its own clean receipt; runtime authority close remains independent | Shutdown/restart receipt tests |
| Legacy epochs remain untouched/readable | Current append-only SQLite epochs | C/D | No migration or mutation of historical ledger; legacy replay tests remain green | Legacy suite plus no-write audit |
| Feature is default off and identity-bound | No LSC config today | Parent (after offline proof) | Explicit enum/config; storage root and format identity included in runtime binding; unknown/missing values refuse activation | Config/identity tests |
| Throughput/storage claims are like-for-like | Existing temp-ledger benchmark only | D + parent | Identical generated/captured corpus and pacing; separate ingress/admission/encode/durable/decode/replay rates, latency percentiles, CPU/memory, DB/WAL/segment bytes and drain/recovery | Raw benchmark JSON and summarized report |
| 2,111/s is not silently revised | Current repo says 1,259/1,500 | Caelum + parent | Preserve 2,111/s as pending external acceptance target and report all stage scopes separately | Caelum decision and qualification report |

## Planned ownership and edit boundary

- Specialist A: new `src/lsc/codec.py` and focused codec/golden tests only.
- Specialist B: new `src/lsc/segment_store.py` and focused seal/recovery tests only.
- Specialist C: new ledger commitment/provenance support and tests in a separate
  branch/worktree after the offline store contract is fixed.
- Specialist D: independent verifier/benchmark tooling and fixtures only.
- Caelum: `docs/administration/caelum/passes/2026-09-22-lsc-v1.md`, review and
  acceptance assessment only.
- Parent: this matrix, architecture, shared configuration, fanout/runtime/ledger
  integration, dependency decisions, integration tests, benchmarks and commits.

Implementation may begin only after Caelum reviews this matrix. Authority-ledger
and runtime activation remain held until offline correctness, recovery and
like-for-like performance gates have receipts.
