# Phase E.2 Adversarial Audit & Freeze Record

Status: **E.2 FREEZE APPROVED**

Frozen E.2 code baseline:
`fbc642ae83405336f8710f9ea8d19009438d1ab5`

This record is committed immediately after that baseline so it can name the
immutable code commit exactly. The preceding audit baseline was
`244a14228994e396e7b813d52749ee2674ef1475`; frozen Phase D remains
`93206d3dc9ca780e1d6a58994a4adb7cb9d6a11a`; the original hardened E.1
baseline remains `46a8d093aab4d002f55125121eca653bb4946732` plus the narrow,
audited provenance correction described below.

## Disposition

The audit treated the prior implementation and passing tests as claims to
falsify. It found and fixed four high-severity and seven medium-severity
scientific-integrity issue classes. No medium or high issue remains open.

E.2 is safe to freeze beneath E.3 because outcome-blind deterministic replay
now proves the exact source universe, membership, sampling design, features,
and labels. E.2 still has no hypothesis discovery, statistical testing, model,
prediction, signal, trade, execution, or capital authority.

## High findings — 4 found, 4 fixed, 0 unresolved

### H1 — text ordering was not instant ordering

D correctly stores canonical UTC text but validly mixes whole and fractional
seconds. SQLite text comparison therefore placed `00:00:00.973Z` before
`00:00:00Z` and `00:10:20Z` after `00:10:20.158Z`. V1 silently omitted 4,490
valid second-zero observations and could choose a pre-horizon trade print.

V2 parses every timestamp as an instant and uses a deterministic fixed-width
microsecond UTC key for SQL comparison/order. Boundary and fractional-horizon
regressions prove start inclusion, end exclusion, and at/after resolution.

### H2 — labels did not mean their declared horizon

V1 used the first later same-symbol market observation anywhere before the
split end. In the old 10k artifact, 2,791 of 9,889 resolved labels were more
than five seconds late, the maximum lag was about 693.967 seconds, and four
resolutions were before the exact endpoint because of H1. V1 could also use an
arbitrary prior market price as the start and silently impute cost.

V2 introduces immutable `OutcomeResolutionSpec` semantics. The start is the
anchor fill price; resolution is the first same-source, same-symbol trade at or
after the exact endpoint, ordered by event time then observation ID, within an
explicit versioned lag tolerance and before the partition end. The default
tolerance is five seconds. Exact endpoint, elapsed time, and resolution lag are
stored. Missing/malformed price, direction, symbol, cost, or market evidence is
explicit; no value is imputed and no member is replaced.

### H3 — the source fingerprint did not bind the scientific source

`PHASE_D_RETAINED_INTERVAL_V1` bound only a narrow subset of each row. Changes
to event/receipt/persistence time, symbol, source identity, quality, code, or
configuration could escape even though they can change selection or outcomes.
Canonical Unicode hashing also normalized different SQL string spellings to
one identity unless input normalization was enforced.

`PHASE_D_RETAINED_INTERVAL_V2` validates and fingerprints every semantically
relevant observation column. It verifies canonical payload and quality JSON,
payload bytes/hash/fingerprint, NFC text, canonical UTC text, event/receipt/
persistence chronology, official historical origin, and `event_at ==
normalized_at` by instant. Alternate-offset and decomposed-Unicode attacks now
fail closed rather than collide with a trusted identity.

### H4 — source checks and artifact use were not one proof

V1 could bind one D universe and consume another between bounded selection,
feature, or outcome batches. A changed-then-restored source could also escape a
final source fingerprint if artifacts were not independently replayed. Outcome
queries were not explicitly contained inside the bound source universe.

V2 revalidates D and independently recomputes expected membership in the same
`BEGIN IMMEDIATE` transaction immediately before freeze. Completion and
`verify` rebind the full source and replay exact membership, sampling design,
every feature, and every outcome under another immediate transaction. Feature
and outcome source rows must remain in the same official source and bound
universe. Tests inject a D mutation between selection and freeze and prove that
it cannot cross the barrier; recovery succeeds only after exact evidence is
restored.

## Medium findings — 7 found, 7 fixed, 0 unresolved

1. `TIME_STRATIFIED_HASH_V1` gave every allocation remainder to lexically early
   buckets, so `target < occupied strata` sampled only early time. V2 assigns
   remainder buckets by seeded total hash and deterministically backfills
   undersized strata. V1 remains readable for legacy artifacts but cannot be
   newly registered.
2. Sampling provenance lacked eligible/selected counts, allocation/backfill
   rules, inclusion probabilities, and weights. V2 freezes these as exact
   rational per-partition/per-stratum metadata. Verification reconstructs the
   entire design; a test forges and consistently rehashes a false weight and
   proves exact replay still rejects it.
3. Feature evidence could cross a partition start or the source interval. V2
   excludes anchors whose declared lookback crosses their partition start and
   validates every persisted feature source against the same partition,
   source, universe, and lookback. D feature data fingerprints are recomputed.
4. A persisted supported `wallet_action` value was provenance-bound but not
   compared with the simple causal transform. V2 always replays it and fails if
   a D value disagrees. Unsupported persisted D features retain exact frozen D
   definition and source-lineage validation.
5. Historical `wallet_action_freshness` measured archive acquisition latency,
   not a causal source-event feature. It is now explicit missing evidence with
   `HISTORICAL_ACQUISITION_LATENCY_IS_NOT_A_CAUSAL_FEATURE`.
6. Valid enum values and self-consistent hashes were not enough to prove true
   lifecycle semantics. V2 validates the exact event type, reason, payload,
   chronology, stage artifacts, counts, and final timestamp. Stage triggers
   prevent membership, design, feature, or outcome insertion at the wrong
   phase. Process-death and consistently rehashed false-state tests fail closed.
7. E.1's commissioning exception was broader than the documented anomaly. It
   now permits only one official HyperCore-source anomaly no more than one
   second before the interval. Later `computed_at` remains the only ignored
   coverage field; count, state, fraction, interval, source, details, and
   feature-definition drift still fail.

## Low findings

Four low-severity observations remain non-blocking. One was fixed: the
free-space guard now reserves the configured minimum plus twice the estimated
artifact size for journal/WAL headroom and low-space refusal is restart-tested.

Three are intentionally deferred:

- `tier` and `purpose` remain in identity. This reduces cache reuse but
  conservatively prevents reuse under a different declared audit intent.
- `FAILED` and `RECOVERABLE` are reserved enum/schema values; V2 accepts only
  the exact happy-path ledger and resumes an interrupted build at its durable
  stage. A richer operator failure workflow is not needed for freeze.
- Full D rebinding/replay is deliberately the explicit `verify` operation and
  is computationally expensive. Ordinary reads prove internal immutable
  ledger/artifact consistency; future scientific consumers must call `verify`
  at their trust boundary. Phase D immutability triggers remain the intervening
  protection.

Hot E.2 artifacts are appropriate at the commissioned scale. Future cold
archive policy is storage lifecycle work, not a current correctness blocker.

## Causal-time verdict

For the official historical archive:

- `event_at` is the source event time.
- `received_at` is when Beelzebub acquired/observed the archive record.
- `persisted_at` is when the normalized row was stored.
- D's `normalized_at` is the canonical UTC form of `event_at`.

`normalized_at` is therefore the correct stored historical anchor only because
V2 validates its exact instant equality with `event_at` on every source row.
The scientific policy is explicitly named `HISTORICAL_EVENT_AT_V1`; event time
defines partitions, membership order, feature windows, strata, and horizons.

Prospective/live science has a different causal question: information cannot
be used before `received_at`. V2 rejects non-official-archive sources rather
than pretending historical event-time semantics apply to live receipt-time
science.

## D.7 / E.2 population reconciliation

The former `796,076 → 792,024 → 311,708` story mixed an ingestion-pass report,
raw text boundary filtering, and V1 eligibility:

- D.7's last ingestion pass reported 796,076: 792,024 rows from second one
  onward plus 4,052 boundary records. Of those boundary records, 4,050 were
  actually in range and two were truly pre-interval.
- The append-only table also retained 462 unique observations from prior
  interrupted/replayed acquisition attempts: 440 valid second-zero records and
  22 truly pre-interval records. The current official-source table contains
  796,538 rows.
- V1 raw text comparison retained 792,024 and omitted all 4,490 valid
  fractional second-zero records.
- V2 instant comparison excludes all 24 true pre-interval rows and retains
  exactly `796,076 - 2 + 440 = 796,514`.
- V2 excludes 398,445 unsupported market-price anchors, 82,786 wallet fills
  outside the declared partitions, and 1,518 anchors whose horizon crosses a
  split end. Exactly 313,765 anchors remain: train 159,705, validation 80,414,
  test 73,646.

The discrepancy is completely explained. It was not harmless reporting alone:
V1 had silently lost valid boundary evidence, now corrected and versioned.

## Missing-outcome audit

The legacy 10k artifact's 111 missing outcomes were all
`OUTCOME_MARKET_EVIDENCE_UNAVAILABLE`: train 43, validation 33, test 35. They
had no later same-symbol trade before their split ended. That missingness was
real, but the count understated label uncertainty because V1 accepted
arbitrarily late prints.

The corrected 10k artifact keeps the same predeclared size but has 2,914 honest
five-second-tolerance misses, all
`OUTCOME_MARKET_EVIDENCE_NOT_WITHIN_TOLERANCE`: train 1,201, validation 874,
test 839. It resolves 7,086 labels; resolution lag is minimum 0, mean about
1.226386, maximum 4.999 seconds. Missingness concentrates in sparsely printed
symbols (for example `xyz:CXMT`, `xyz:SHAZ`, `xyz:PURRDAT`, `LINK`,
`xyz:GOLD`, and `xyz:KIOXIA`) and is therefore systematic source-cadence
evidence that E.4 must handle. E.2 labels it truthfully and never replaces a
member.

## Sampling and full-population conclusions

`DETERMINISTIC_HASH_V1` is sound: SHA-256 identity includes algorithm, seed,
observation ID, and causal stratum; observation ID is a total tie-break; final
order is event time plus observation ID. Tests cover insertion/query order,
batching, restart, thread/process concurrency, seed/target/resolution identity,
and Python-process independence.

`TIME_STRATIFIED_HASH_V1` is not sufficiently specified for new downstream
inference and is retired from new V2 registration. `TIME_STRATIFIED_HASH_V2`
is sound for its explicit intent—equal allocation across occupied UTC buckets,
seeded remainder assignment, deterministic global backfill—and preserves the
population counts, conditional inclusion probabilities, and weights E.4 needs.
E.2 does not claim that balanced time sampling represents the raw population.

`ALL_ELIGIBLE_V1` was independently replayed over the real retained universe:
exact count 313,765 and membership fingerprint
`2d318e0ff8cedcd8d66fad185737bf97203ac8912354e8d2ad98fedec81169d1`.
It contains each eligible observation once in contiguous causal order.

## Real recommissioning and scaling evidence

The hardened real 10k materialization is:

- materialization ID: `e2-5f761a9f987d17003c1a20c2f7b72c12`
- specification hash:
  `7dddf3df33d9fa2248aa17fc81dab16d2695763dbc02dca18678ced9e8c7363d`
- source fingerprint:
  `d511fefe0eb45b9d5f65862654bfc65cba153110bd2578a9af16cc39f7751664`
- membership fingerprint:
  `f56a8ddeb123954397034f3ec02f0613a8b356fe1828f2db9e1e13f3de344bd6`
- sampling-design fingerprint:
  `3864cb36bb0f9677cbf35844677522cd0d855152d82ece7afb75bc6afe5c531e`
- feature fingerprint:
  `3c451895bb15fb678afbd869f7dee4879489fb7c823996d81f0ab00f1878ae1c`
- outcome fingerprint:
  `0260a91c9796e4bb7cf5509f890b0b446ec266f79a48a69ec81f4ed2283de925`
- complete fingerprint:
  `35965007442c1952c11ac6323020aa56667d9ea0dba94354605cec0c990f21d5`

Selection is train 5,001, validation 2,571, test 2,428. The global exact
inclusion probability is 2,000/62,753 and weight 62,753/2,000. All 10,000
`wallet_action` features resolve; outcomes are as diagnosed above.

Initial V2 build time was about 792.226 seconds and database growth was
14,909,440 bytes, from 1,886,261,248 to 1,901,170,688. An idempotent rebuild
took about 434.536 seconds with identical identity and all five artifact
fingerprints. The final hardened verifier took 331.424 seconds, reproduced all
fingerprints, and left database size unchanged.

The infrastructure-only 50k selection used predeclared seed 17 and did not
materialize features/outcomes or inspect performance. Two independent passes
selected 50,000 from 313,765 and both produced
`8a323e620941333bc306aba67ecc847c4ada106c33e9a035cc0c76d8daed537b`.
They took about 59.998 and 60.470 seconds. Including a 158.701-second full
source verification and 65.214-second all-eligible proof, the read-only scaling
run took 345.953 seconds and changed database size by zero bytes. External
process samples observed a working set below about 92 MB; selection keeps only
the bounded target heap, while all-eligible fingerprinting is streaming.

## Lifecycle, artifact, recovery, and concurrency integrity

- Projection forgery: exact event sequence/reason/payload and stage artifacts
  are reconciled on every read; a valid-looking status alone is rejected.
- Membership: stage trigger plus exact count, contiguous ordinal, fingerprint,
  independent selection replay, and post-freeze immutability.
- Sampling: immutable design hash plus exact deterministic population/design
  reconstruction; self-consistent false weights are rejected.
- Features: exact member coverage, typed/missingness validation, source-lineage
  fingerprints, supported-feature replay, and final replay from D.
- Outcomes: one per member, explicit missingness, bounded exact source lineage,
  no replacement, and final replay from D.
- Complete artifact: hashes specification, membership, sampling design,
  features, and outcomes and must agree with the exact COMPLETE event/time.
- Restart: real process death after freeze, after a feature batch, after an
  outcome batch, and between projection update and event commit is recoverable
  without changed membership or artifacts.
- Concurrency: thread and independent-process builders, including two
  full-population builders, converge on one artifact for one identity.
- Resource refusal: low disk stops before membership and the same registered
  identity remains restartable.

## Validation record

- E.1: 23/23 passed.
- E.2: 25/25 passed.
- Focused D.6/D.7/closure: 23/23 passed.
- Full backend: 268/268 passed in 422.703 seconds.
- Frontend: 15/15 passed.
- Frontend production build: passed.
- `pip check`: no broken requirements.
- `npm audit` (full and production-only): zero vulnerabilities.
- Python `pip-audit` was not installed; no dependency mutation was made merely
  to add an audit tool.
- PowerShell parse check: not applicable; no PowerShell file changed.
- Repository static trading-authority/bridge guard: passed.
- Phase D production diff against
  `93206d3dc9ca780e1d6a58994a4adb7cb9d6a11a`: empty.

The first focused D run used Windows' C: temporary directory with only about
1.016 GB free, below D.6's intentional 1 GiB safety threshold, and one D.7
worker assertion stopped with zero processed items. The identical 23-test run
with temporary files on D: (ample free space) passed. This was a successful
resource guard, not a D regression; the full 268-test run also passed on D:.

## Phase boundaries and remaining trust boundary

Phase D production code is unchanged. E.1 remains scientifically intact; its
only modification is the narrowed official-source/one-second anomaly check,
with semantic drift tests. No E.3 capability was added. Static and runtime
guards confirm E.2 has no route into legacy D.6 hypothesis machinery or any
decision, risk, order, paper/live execution, leverage, or capital path.

E.2 trusts the frozen Phase D observation writer and feature-definition
contracts, SQLite/cryptographic primitives, and the operator to run `verify`
before a downstream scientific trust transition. Prospective receipt-time
science remains intentionally out of scope.

## Direct final answers

1. Can selection receive future/outcome information directly or indirectly?
   **No.** It uses only validated source identity/event time and declared
   partition/eligibility/sampling inputs; features and outcomes are post-freeze.
2. Is the anchor timestamp causally correct? **Yes.** Historical event time is
   correct, and V2 proves stored `normalized_at` equals `event_at`; live science
   must use a separate receipt-time contract.
3. Is the D.7/E.2 count difference fully explained? **Yes.** Exact accounting
   is given above.
4. Can changed D evidence after registration silently change the result? **No.**
   Pre-freeze and final atomic rebinding/replay reject it.
5. Can a member disappear or be replaced after outcome inspection? **No.**
6. Are feature lookbacks safe at every partition boundary? **Yes.**
7. Does a horizon label mean its declaration? **Yes.** It is the exact endpoint
   plus a stored, versioned, bounded first-trade resolution lag.
8. Can valid-looking SQL projection changes forge COMPLETE? **No.** Exact event
   and deterministic artifact replay reject them.
9. Can process death change membership on recovery? **No.**
10. Can concurrent builders produce different valid artifacts for one identity?
    **No.**
11. Is `TIME_STRATIFIED_HASH_V1` sufficient? **No for new science; it is
    read-only legacy. `TIME_STRATIFIED_HASH_V2` is the hardened replacement and
    preserves the required inference metadata.**
12. Is `ALL_ELIGIBLE_V1` the exact eligible population? **Yes: 313,765.**
13. Did Phase D remain frozen? **Yes.**
14. Does E.2 have hypothesis, prediction, signal, trade, or capital authority?
    **No.**
15. Is E.2 safe to freeze beneath E.3? **Yes.**

**E.2 FREEZE APPROVED**
