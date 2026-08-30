# CODEX HANDOFF — `l3h` Sovereign Live-Capital Runtime, One-Control Commissioning, and Autonomic Hardening

**MODEL MODE:** Terra Extra High / maximum available reasoning  
**REPOSITORY:** `github.com/achilles611/Trader`  
**CURRENT HANDOFF BRANCH:** `codex/l3g-ledger-epoch2-recovery`  
**PUBLISHED HANDOFF COMMIT:** `3897f46827a2f685263054a067200ea9cb3c9936`  
**NEW IMPLEMENTATION BRANCH:** `codex/l3h-sovereign-live-capital`  
**PHASE:** `l3h`  
**DATE:** Sunday, August 30, 2026  
**TARGET OPERATOR SESSION:** Monday, August 31, 2026  
**PRIMARY TERMINAL TARGET:** `LIVE_READY_DISARMED`  
**SECONDARY HONEST TERMINAL TARGET:** `PROVIDER_EVALUATION_READY_DISARMED`  
**FAIL-CLOSED TERMINAL TARGET:** `BLOCKED_<EXACT_REASON>`

---

## 0. Mission

Build the first sovereign, real-capital-capable Lane III runtime without weakening, reinterpreting, or quietly widening the existing Lane III constitution.

This pass must transform the proven `l3g` paper stack into a **separate** `l3h` live-capital capability with:

- one exact account;
- one exact concrete MNQ contract;
- one MNQ maximum exposure;
- one atomic operator start control;
- independent NinjaTrader-side risk enforcement;
- write-ahead intent persistence and strict idempotency;
- startup and continuous broker reconciliation;
- event-sourced canonical execution evidence with sealed ledger epochs;
- truthful data-authority classification;
- native NinjaTrader replay ingestion;
- calibrated latency, queue, slippage, and fill-quality evidence;
- bounded hot storage and verified Parquet/DuckDB cold storage;
- an optional QuestDB hot-data adapter that is benchmarked before activation;
- a compact operator dashboard with P&L and execution markers;
- fully automated Windows bootstrap, deployment, verification, restart, archival, and status tooling;
- an autonomous defect-discovery, ergonomic-review, repair, regression, and recommissioning loop.

Do not stop after scaffolding or documents. Implement the vertical path, deploy it locally, test it through installed NinjaTrader on `Sim101`, perform restart and fault recovery, audit the result, fix every reproducible release-blocking defect, and return only when the system truthfully reaches one of the terminal states above.

The engineering objective is to leave only these possible external actions for Joseph:

1. supply or select an already-authorized live account when no qualifying account is locally available;
2. satisfy broker, exchange-data, or prop-firm requirements that software cannot create;
3. deliberately press and hold the single **START LIVE — 1 MNQ CANARY** control.

Do not leave ordinary PowerShell, file-copying, AddOn deployment, NinjaTrader startup, workspace restoration, task scheduling, key creation, storage setup, archive maintenance, or routine verification as manual operator chores when Codex can safely perform them.

---

# 1. Governing truth

## 1.1 Existing authority

The current checked-in execution AddOn is intentionally and correctly sealed to:

- `Sim101`
- `LOCAL_SIMULATION`
- `MNQ SEP26`
- maximum quantity `1`
- `PAPER_ONLY`
- signed loopback protocol
- reconciliation before commands
- idempotent command IDs
- heartbeat watchdog
- protective-stop enforcement
- foreign-activity lockout
- exact session fences
- live capital denied

The existing paper risk artifact allows no more than:

- one MNQ;
- one pending entry;
- no averaging;
- no pyramiding;
- no same-event reversal;
- maximum planned trade risk `$50`;
- 25-point stop distance;
- maximum daily loss `$200`;
- maximum entry slippage `2` points;
- hard flat deadline `15:58 America/New_York`.

The existing operational policy is explicitly experimental, not scientifically commissioned, and identifies its data ordering as `LOCAL_CALLBACK_ORDER_ONLY` with unverified book completeness.

The current live-capital precondition record truthfully states:

```text
Live readiness architecture: PRESENT
Live execution implementation: ABSENT
Live execution registration: ABSENT
Live execution authority: DENIED
```

This pass must close those implementation gaps honestly. It must not rewrite history and claim that `l3g` already had live authority.

## 1.2 Non-negotiable invariants

1. **Never convert the paper AddOn into a live AddOn by changing constants.**  
   Preserve `BeelzebubPaperExecutionAddOn.cs` as an independently testable paper capability. Build a new live boundary, schema, port, key, manifest, runtime, and authority path.

2. **No real-capital order may be sent during autonomous implementation, testing, audit, repair, or deployment.**  
   Installed-NinjaTrader testing must use `Sim101`. Real-capital activation requires a fresh, explicit, short-lived operator authorization generated only by the one-control UI after every live preflight gate passes.

3. **`UNKNOWN` is never `FLAT`.**  
   Missing broker facts, timeouts, disconnected account streams, missing order acknowledgements, incomplete snapshots, stale balances, and ambiguous external activity are hard authority failures.

4. **Do not manufacture data authority.**  
   A local callback counter is not an exchange sequence. Keep exact provenance such as `PROVIDER_SEQUENCE`, `NINJATRADER_CALLBACK_ORDER`, `NT_REPLAY_FILE_ORDER`, and `LOCAL_RECEIPT_ORDER`. Never relabel one as another.

5. **Do not widen strategy thresholds to force a trade.**  
   The live canary policy must be identical to or narrower than the frozen experimental paper policy. Absence of a natural signal is an acceptable terminal observation.

6. **Do not claim strategy profitability or scientific validation.**  
   Mechanical live readiness, scientific commissioning, and profitable edge are separate facts.

7. **Do not add copy trading, followers, ratio matrices, or multiple accounts to the first live-capital path.**  
   One exact account and one exact MNQ contract only.

8. **No secret, full account identifier, password, API key, HMAC key, DPAPI payload, or credential-derived value may be committed, printed, returned, or placed in browser storage.**

9. **All new order-producing endpoints are loopback-only, authenticated, origin-checked, idempotent, and fail closed.**

10. **Every side effect must have durable write-ahead evidence before dispatch.**  
    A crash after durable intent but before acknowledgement creates `UNKNOWN`; it never triggers automatic resubmission.

11. **Every fix discovered during this pass requires a regression test.**

12. **Never return `COMPLETE` with an unresolved P0/P1 defect, a dirty unexplained worktree, an unverified installed AddOn, or a runtime that cannot prove flat/disarmed state.**

---

# 2. Preserve the real starting state

The handoff branch contains a published context snapshot, but the local checkout was reported to have pre-existing uncommitted work in BeezConsole, the React UI, NinjaTrader sources, control/commissioning code, tests, and launcher scripts.

Before changing anything:

1. Read:
   - `docs/CODEX_CONTEXT_HANDOFF.md`
   - `docs/commissioning/lane-iii-phase-g/live-capital-preconditions.md`
   - `docs/commissioning/lane-iii-phase-g/commissioning-report.md`
   - `docs/operations/lane-iii-commissioning-runbook.md`
   - `docs/operations/local-ledger-verification.md`
   - `docs/STORAGE_TOPOLOGY_WINDOWS.md`

2. Record:
   - exact branch and HEAD;
   - `git status --porcelain=v2 --branch`;
   - staged, unstaged, and untracked file lists;
   - complete binary-safe patch manifests;
   - SHA-256 of every modified/untracked source and configuration file;
   - active processes, listeners, runtime paths, environment overrides, fixed disks, free bytes, and NinjaTrader version;
   - active ledger path, epoch, tip, checkpoint, and verification status;
   - installed NinjaTrader source and compiled DLL hashes.

3. Create a timestamped preservation directory under the resolved audit root, not under a guessed drive letter.

4. Create a local recovery branch from the current checkout:
   - `codex/l3g-pre-l3h-preservation-<UTCSTAMP>`

5. Preserve all legitimate local source work in one explicit preservation commit without amending or squashing history. Never commit secrets, generated credentials, active SQLite/WAL files, logs, or account-private artifacts. For excluded local-only material, save a secure manifest with hashes and paths.

6. Push the preservation branch when authentication permits.

7. Create `codex/l3h-sovereign-live-capital` from the preservation commit.

8. Do not use `git reset --hard`, `git clean`, blind `git stash`, checkout-overwrite, or destructive database operations.

9. If the local dirty work conflicts with the published branch, resolve it by provenance and tests—not by discarding it.

The final report must identify:

```text
PUBLISHED_START_SHA
LOCAL_START_SHA
PRESERVATION_BRANCH
PRESERVATION_COMMIT
L3H_BRANCH_POINT
PRESERVED_MODIFIED_FILES
PRESERVED_UNTRACKED_FILES
EXCLUDED_SECRET_OR_RUNTIME_PATHS
```

---

# 3. Baseline audit before implementation

Run the complete current baseline before adding `l3h`.

## 3.1 Source and test baseline

Run:

```powershell
python -m compileall -q src main.py beez_console.py
python -m unittest discover -s tests -v
cd control-center-ui
npm ci
npm test -- --run
npm run build
npm audit --omit=dev --audit-level=high
npm audit --audit-level=high
```

Record exact counts and failures.

## 3.2 Installed runtime baseline

Using existing safe launch and autologin tooling where possible:

- stop BeezConsole and stale service owners cleanly;
- verify all relevant ports are free;
- launch NinjaTrader;
- restore the exact Beelzebub workspace;
- verify the observer and paper execution AddOn source/install/build provenance;
- launch BeezConsole;
- prove `Sim101` flat, zero working owned orders, runtime `READY_DISARMED`;
- verify observer, account, position, order, execution, and transport health separately;
- prove live capital remains denied;
- run `Auto` ledger verification, not a needless full scan when a valid checkpoint permits incremental verification;
- record baseline ingestion throughput and staleness under authentic market callbacks.

Do not place an order during baseline.

## 3.3 Baseline defect register

Create:

`docs/commissioning/lane-iii-phase-h/preimplementation-defect-register.md`

Classify every observed issue:

- `P0`: can create unintended exposure, duplicate execution, unprotected position, false flat, false authority, evidence loss, or real-account mutation.
- `P1`: can block safe execution/recovery, corrupt operational truth, make one-control operation unreliable, or seriously mislead the operator.
- `P2`: lower-risk correctness, maintainability, performance, or ergonomic issue.
- `P3`: cosmetic or future enhancement.

Fix any baseline P0 immediately before live-path development continues.

---

# 4. `l3h` architecture and package boundary

Create a new package:

```text
src/l3h_live/
    __init__.py
    contracts.py
    authority.py
    capability.py
    gateway.py
    event_store.py
    projections.py
    reconciliation.py
    risk.py
    commissioning.py
    runtime.py
    storage.py
    archive.py
    nrd_replay.py
    fill_calibration.py
    diagnostics.py
    audit.py
```

The exact partitioning may change when code ownership demands it, but the responsibilities must remain explicit.

Prefer **zero changes under `src/lane_iii`**. If a backward-compatible provenance extension there is truly unavoidable:

- explain why an adapter-local extension is insufficient;
- preserve all frozen behavior;
- add explicit schema versioning;
- produce a focused frozen-baseline diff;
- rerun every Lane III phase test;
- document the authority effect as `NONE` unless separately approved.

Integrate through explicit boundaries in `src/copytrade/control_center.py`, launch tooling, the React UI, and NinjaTrader source.

Do not blend `l3g_paper` and `l3h_live` into condition-heavy mode branches. Share only pure, authority-neutral helpers after proving that dependency direction cannot let paper or live widen the other.

---

# 5. Authority and capability manifests

## 5.1 Tracked templates

Create:

```text
docs/commissioning/lane-iii-phase-h/
    README.md
    architecture.md
    authority-matrix.md
    live-capital-specification.md
    live-policy-v0.json
    live-risk-canary-v0.json
    live-capability-template.json
    commissioning-state-machine.md
    continuous-reconciliation.md
    ledger-epochs.md
    storage-lifecycle.md
    nrd-replay-provenance.md
    fill-calibration.md
    operator-runbook.md
    rollback-and-kill.md
    adversarial-test-evidence.md
    closure-audit.md
    commissioning-record.md
    third-party-design-references.md
```

Tracked templates must contain no local account identity or credential.

## 5.2 Local live-capability artifact

Generate the actual live capability locally, outside Git, under an ACL-restricted Beelzebub authority root. It must be signed and immutable for one commissioning epoch.

It must bind:

- schema and version;
- capability ID;
- creation and expiry;
- exact sanitized account alias;
- cryptographic account-binding hash;
- account class;
- broker/provider connection identity hash;
- whether the account is:
  - `LOCAL_SIMULATION`
  - `PROVIDER_EVALUATION`
  - `PROVIDER_FUNDED`
  - `BROKERAGE_LIVE`
- exact concrete NinjaTrader instrument;
- canonical contract;
- exchange and tick economics;
- maximum quantity `1`;
- live-capital boolean;
- policy hash;
- risk hash;
- prop-rule-profile hash;
- strategy artifact hash;
- source fingerprint;
- NinjaTrader build fingerprint;
- allowed session profiles;
- one commissioning epoch;
- one operator activation nonce family;
- revocation state.

The AddOn and Python runtime must independently verify the same capability.

Never classify an account as live merely because its alias contains “live,” “funded,” or a firm name. Derive account class from independently observed provider/connection facts and local approved configuration. If the only available account is an evaluation environment, expose it as `PROVIDER_EVALUATION` and never label it “real capital.”

## 5.3 Operator authorization

The runtime may not self-grant live authority.

The single UI control must generate a short-lived, one-shot, signed operator activation containing:

- exact capability ID;
- exact current gate snapshot hash;
- exact account-binding hash;
- exact contract;
- quantity `1`;
- current session identity and generation;
- risk/profile hashes;
- commissioning epoch;
- request ID;
- issued-at and expiry;
- one-use nonce.

Use Windows-local protected storage and ACLs. Do not persist the authorization in browser local storage. It expires quickly and is consumed atomically at live arm.

---

# 6. Separate NinjaTrader live execution AddOn

Create:

`ninjatrader/NinjaScript/AddOns/BeelzebubLiveExecutionAddOn.cs`

Do not rename or repurpose the paper AddOn.

## 6.1 Separate identity

Use a separate:

- wire schema;
- protocol version;
- source fingerprint;
- port, preferably loopback `48137` if unoccupied;
- signing key;
- capability manifest;
- command namespace;
- order-name prefix such as `BZ-L3H-`;
- runtime session;
- watchdog state;
- audit event stream.

The live AddOn must start `DISARMED`, even when a live capability file exists.

## 6.2 Account and instrument isolation

At startup:

- enumerate accounts;
- find exactly one account matching the signed account-binding artifact;
- refuse zero or multiple matches;
- bind exact provider connection and exact account class;
- resolve the active concrete MNQ contract;
- compare full name, master instrument, exchange, tick size, point value, expiry, and rollover status;
- refuse stale or liquidation-only contract months;
- never accept an arbitrary account or instrument string from a command;
- never fan out to another account.

A contract roll requires a new capability and new operator authorization. Old-month positions may be closed but never reopened.

## 6.3 Command surface

Allow only:

```text
RECONCILE
HEARTBEAT
ENTER_LONG
ENTER_SHORT
EXIT
CANCEL_OWNED_ORDERS
EMERGENCY_FLATTEN
DISARM
```

Every command must have:

- signed schema;
- execution session;
- command ID;
- strictly monotonic command sequence;
- creation and expiry;
- capability ID and hashes;
- commissioning epoch;
- account-binding hash;
- exact instrument;
- exact quantity;
- causation and correlation IDs;
- idempotency identity.

Duplicate command IDs return the prior outcome and never repeat a mutation.

A reconnect never resets durable idempotency. Rehydrate currently working `BZ-L3H-*` orders and recent command correlations before accepting new authority.

## 6.4 Independent NinjaTrader risk guard

Implement a clean-room independent final risk layer inside NinjaTrader. Use the external RiskGuard project only as a list of failure modes and test ideas; do not copy unlicensed code.

The AddOn must independently deny:

- quantity other than `1` for entry;
- non-MNQ or wrong expiry;
- wrong account or class;
- entry while any exact-instrument position exists;
- entry while any owned working entry exists;
- entry while any unclassified working order exists;
- entry after daily loss latch;
- entry after first canary round trip;
- entry outside allowed session;
- entry with stale/mismatched capability;
- entry without reconciled broker state;
- entry with foreign activity;
- entry during disconnect/recovery;
- entry while prior command state is `UNKNOWN`;
- entry while storage/evidence safety gate denies;
- entry after kill latch;
- pyramiding, averaging, reversal, or simultaneous theses.

The first live commissioning epoch is narrower than paper:

```text
maximum_absolute_position = 1 MNQ
maximum_pending_entries = 1
maximum_completed_round_trips = 1
maximum_trade_risk <= $50
daily_loss_limit <= $200
averaging = false
pyramiding = false
same_event_reversal = false
copy_trading = false
```

Do not widen any checked-in paper risk value.

## 6.5 Protective-order invariant

The live position invariant is:

```text
EXPOSURE == 0
OR
exactly one accepted server-side protective stop exists for the entire exposed quantity
OR
emergency flatten is actively resolving and new entries are locked out
```

Prefer a native broker-hosted ATM/OCO bracket when the installed NinjaTrader API and account support can prove its behavior. Otherwise:

- dispatch the protective stop immediately on first fill;
- require observed broker acceptance within a stricter live SLA determined by measured Sim101 behavior, never longer than the current paper allowance;
- verify account, instrument, side, quantity, stop price, OCO identity, and working state;
- if missing, rejected, cancelled, duplicated, altered, or wrong, cancel owned entries, emergency flatten, and latch lockout;
- partial fills must receive protection for the partial quantity without waiting for final fill;
- order modification is not successful until the changed broker state is observed;
- if an order-change API silently no-ops, use a tested replace protocol that does not leave an unprotected interval;
- AddOn reload must rehydrate owned stops and enforce the invariant before authenticating.

## 6.6 Watchdog and out-of-band kill

Implement three independent controls:

1. **Python heartbeat watchdog**  
   If authenticated heartbeats stop:
   - cancel owned pending entries;
   - preserve or verify protection;
   - flatten exact-instrument exposure;
   - publish final reconciliation proof;
   - latch lockout.

2. **NinjaTrader-native kill control**  
   Add a clearly labeled NinjaTrader control or menu action:
   - `BEELZEBUB — FLATTEN MNQ & LOCK`
   - exact account/instrument only;
   - no dependence on Python or BeezConsole.

3. **Out-of-band local kill signal**  
   Add a dedicated Windows-safe kill mechanism usable by:
   - a desktop emergency shortcut;
   - `scripts/l3h_kill.ps1`;
   - a named event or ACL-protected sentinel monitored by the AddOn.

The kill path must remain functional when the Python runtime, UI, ledger writer, or normal command port is dead. Test it on `Sim101`.

Avoid broad account-wide flatten unless the operator deliberately selects a separately labeled last-resort emergency action. Normal safety logic must target the exact owned MNQ sleeve.

---

# 7. Gateway and execution-state model

Define one normalized gateway contract inspired by mature engine/gateway separation:

```text
LaneIIIExecutionGateway
    connect
    disconnect
    capability
    account_snapshot
    position_snapshot
    working_order_snapshot
    recent_order_reports
    recent_execution_reports
    submit
    cancel
    replace
    reconcile
    emergency_flatten
```

Implement distinct adapters:

- `Sim101ExecutionGateway`
- `NinjaTraderLiveExecutionGateway`
- `NinjaTraderObservationGateway`
- `ReplayExecutionGateway`

Strategy, risk, ledger, projections, and UI code must not depend directly on NinjaTrader transport details.

## 7.1 Canonical order lifecycle

Implement explicit states:

```text
INTENT_CREATED
INTENT_ADMITTED
COMMAND_SEALED
SUBMITTING
BROKER_ACKNOWLEDGED
PARTIALLY_FILLED
FILLED
PROTECTION_SUBMITTING
PROTECTED
EXIT_REQUESTED
CANCEL_PENDING
CANCELLED
REJECTED
EXPIRED
CLOSED
UNKNOWN
QUARANTINED
```

Define legal transitions and reject illegal ones.

Each transition carries:

- event ID;
- intent ID;
- decision ID;
- command ID;
- client order ID;
- broker order ID when available;
- execution ID when available;
- strategy run ID;
- commissioning epoch;
- account-binding hash;
- concrete contract;
- prior state;
- new state;
- reason;
- event/source timestamps;
- causation ID;
- correlation ID;
- payload hash.

No order is considered acknowledged, changed, cancelled, filled, protected, or closed until the corresponding provider fact is observed.

---

# 8. Canonical event ledger, projections, and epochs

Preserve the narrow existing Lane III safety store. Do not turn it into a market-data warehouse.

Create a separate canonical `l3h` execution and authority event store.

## 8.1 Event record

At minimum:

```text
global_sequence
event_id
stream_id
stream_version
event_type
schema_version
epoch_id
occurred_at
recorded_at
authority_effect
account_binding_hash
instrument
causation_id
correlation_id
payload_json
payload_hash
previous_record_hash
record_hash
```

Use:

- append-only semantics;
- exact canonical serialization;
- per-stream optimistic concurrency;
- `BEGIN IMMEDIATE` or equivalent single-writer discipline;
- WAL where safe;
- explicit fsync/durability policy for command intent;
- no update/delete API for canonical events;
- versioned upcasters for future schema evolution.

## 8.2 Write-ahead execution

Before any order-producing transport call:

1. construct immutable decision/intent;
2. run all admission and risk gates;
3. durably append `COMMAND_SEALED`;
4. commit and confirm;
5. dispatch the signed command;
6. append every observed outcome.

A process death between steps 4 and 5 leaves a sealed but undispatched command. Recovery reconciles by broker truth; it does not resend automatically.

## 8.3 Projections

Use a separate projection database or clearly non-authoritative projection tables for:

- current runtime state;
- commissioning lifecycle;
- current position;
- current owned orders;
- unresolved/unknown orders;
- risk counters and latches;
- account health;
- P&L;
- execution markers;
- archive status;
- data authority.

Projections must be rebuildable from canonical events and carry the last applied global sequence and root hash.

## 8.4 Ledger epochs

Do not repeat the single enormous SQLite failure pattern.

Rotate canonical evidence by:

- clean session/commissioning boundary; or
- bounded maximum size/row threshold;

whichever occurs first.

Each sealed epoch manifest contains:

```text
epoch_id
predecessor_epoch_id
previous_epoch_root
first_sequence
last_sequence
row_count
minimum_timestamp
maximum_timestamp
schema_versions
authority_effect_counts
final_record_hash
database_sha256
quick_check_result
sealed_at
```

A new epoch starts from the prior root but imports no mutable runtime state. Current projections recover from the sealed anchor plus active tail.

Preflight verifies:

- sealed epoch chain;
- active database quick check;
- incremental tail from last trusted checkpoint;
- projection checkpoint agreement;
- current independent broker reconciliation.

A full multi-million-row scan remains available as forensic verification, but it must not be the normal one-button startup path.

Never rotate, VACUUM, prune, archive, or replace an active ledger while armed or exposed.

---

# 9. Startup and continuous reconciliation

Implement reconciliation as a permanent runtime service, not a one-time preflight.

## 9.1 Startup reconciliation

Before `READY_DISARMED`:

- load canonical active-tail state;
- load projection checkpoint;
- obtain exact account snapshot;
- obtain exact position snapshot;
- obtain working-order snapshot;
- obtain recent order and execution reports for at least the current session and a bounded lookback;
- classify every order as:
  - `BEELZEBUB_OWNED`
  - `VENUE_EXTERNAL`
  - `RECONCILIATION_SYNTHETIC`
  - `UNKNOWN`
- correlate broker IDs to client IDs and command IDs;
- rehydrate protective orders;
- resolve only facts supported by provider evidence;
- mark unresolved state `UNKNOWN`;
- refuse readiness if position, order, account, or protection truth is ambiguous.

## 9.2 Continuous checks

Run event-driven reconciliation plus bounded polling:

- in-flight order checks approximately every 2 seconds;
- open-order checks approximately every 5 seconds;
- position checks approximately every 5 seconds;
- targeted single-order queries with retry ceilings and rate limiting;
- exact-owned-order audit against account working orders;
- protective-stop audit while exposed;
- account connection and balance freshness checks;
- session, contract, and rollover checks.

Tune intervals using observed NinjaTrader/provider behavior without loosening safety.

## 9.3 Unknown-state policy

When a command or provider fact becomes unknown:

- stop new entries;
- do not resend the order;
- reconcile by broker ID, client ID, account, instrument, side, quantity, and time;
- maintain or prove protection for possible exposure;
- if exposure cannot be disproved and protection cannot be proven, invoke emergency flatten;
- persist the ambiguity and resolution;
- require clean reconciliation before re-arm.

## 9.4 Provider-outage armor

A temporary provider condition that reports zero balances or incomplete snapshots must be marked stale/quarantined rather than treated as a real zero or flat fact.

Market-data health, account health, position health, order health, and execution health remain independent.

---

# 10. Data authority and live-policy review

The current paper policy uses `LOCAL_CALLBACK_ORDER_ONLY`. This pass must perform the “reviewed equivalent data authority” work rather than pretending local order is exchange sequencing.

## 10.1 Provenance model

Add explicit fields:

```text
ordering_authority
ordering_generation
provider_sequence
provider_event_id
callback_sequence
replay_file_sequence
exchange_time
provider_time
local_receipt_time
book_scope
book_depth_levels
snapshot_completeness
gap_status
```

Allowed authorities include:

```text
PROVIDER_SEQUENCE
NINJATRADER_CALLBACK_ORDER
NT_REPLAY_FILE_ORDER
LOCAL_RECEIPT_ORDER
UNSEQUENCED
```

## 10.2 Authentic callback review

Using installed NinjaTrader and real market callbacks:

- observe quotes, trades, and depth across clean startup;
- disconnect/reconnect;
- workspace reload;
- chart/indicator reload;
- session transition;
- contract resolution;
- intentional consumer backpressure;
- process restart.

Prove:

- callback generations reset explicitly;
- local callback sequences are monotonic only within one generation;
- gaps and reversals are detected;
- no evidence crosses a recovery boundary;
- top-of-book and depth freshness are independent;
- the top-N book scope is explicit;
- consumer throughput does not starve the observer;
- no market-data telemetry runs while holding the critical ingest lock.

If the live strategy only requires callback-ordered top-ten MBP and the review establishes that this is a sufficient **operational** authority, create a new reviewed live policy artifact that says exactly that. Do not call it provider-authoritative or scientifically eligible.

If the strategy contract requires provider-authoritative sequencing and no reviewed equivalent passes, return `BLOCKED_MARKET_DATA_AUTHORITY`. Do not silently degrade or change thresholds.

## 10.3 Policy preservation

Clone the current experimental policy into a live-canary artifact with:

- identical hypotheses;
- identical evidence families;
- identical thresholds, windows, TTLs, and dominance rules;
- equal or shorter authority lifetimes;
- narrower maximum session entries: one completed round trip;
- explicit live operational authority;
- scientific eligibility still false unless separately commissioned.

No “aggressive alpha” threshold changes belong in this pass.

---

# 11. Native NinjaTrader replay ingestion

Implement an offline `.nrd` importer inspired by `nrdtocsv`, without making undocumented replay APIs part of live authority.

## 11.1 Requirements

- discover NinjaTrader replay files safely;
- parse L1 and L2 records;
- retain timestamp plus subsecond offset;
- retain operation `ADD`, `UPDATE`, `REMOVE`;
- retain book position and market-maker field when available;
- retain exact source-file hash and parser version;
- maintain a provider-side positional book;
- translate positional updates into price-level canonical events;
- emit explicit `NT_REPLAY_FILE_ORDER`;
- never populate `provider_sequence` with a fabricated file counter;
- detect malformed records, timestamp reversals, impossible positions, crossed books, and incomplete sessions;
- support deterministic replay into the existing Lane III market pipeline;
- compare final reconstructed book hashes across repeated runs.

Do not place the importer on the live decision loop.

Add fixture-based and adversarial tests.

---

# 12. Fill, latency, queue, and slippage calibration

Implement an internal calibration layer using the concepts of realistic queue- and latency-aware replay. Do not vendor a new engine unless a small, license-compatible dependency is clearly superior.

Record for every Sim101 and later operator-authorized live command:

```text
decision_time
intent_sealed_time
command_dispatch_time
addon_receipt_time
broker_ack_time
first_partial_fill_time
final_fill_time
protective_submit_time
protective_accept_time
exit_request_time
flat_proof_time
book_state_hash_at_each_stage
best_bid_ask
spread
displayed_depth
predicted_fill
observed_fill
predicted_slippage
observed_slippage
```

Model:

- strategy computation delay;
- Python-to-AddOn transport delay;
- NinjaTrader processing delay;
- broker acknowledgement delay;
- queue-ahead estimate when limit orders are used;
- available displayed liquidity;
- feed latency;
- partial-fill behavior.

The current live canary may retain market entries, but calibration must still measure spread, adverse movement, transport, acknowledgement, protection delay, and exit quality.

Generate:

`docs/commissioning/lane-iii-phase-h/fill-calibration-report.json`

Mechanical commissioning does not require profit. It requires truthful fill/protection/recovery behavior.

---

# 13. Storage lifecycle for the 500 GB drive

Do not assume a drive letter from stale documentation. Resolve actual configured hot/cold roots, fixed-drive type, filesystem, health, free space, and current runtime bindings.

Prefer the currently commissioned NVMe hot root when healthy and eligible.

## 13.1 Managed capacity constitution

Start with a Lane III managed ceiling of approximately `300 GB`, dynamically bounded so the volume keeps at least the greater of:

- `100 GB` free; or
- `20%` free.

Initial classes:

| Class | Initial ceiling | Rule |
|---|---:|---|
| Canonical safety/execution ledgers and checkpoints | 25 GB | Never automatically delete canonical evidence |
| Recent full L1/L2 market observations | 125 GB | Rotating hot/warm tier |
| Verified Parquet sessions | 115 GB | Cold analytical tier |
| Derived features, replay, fill calibration | 20 GB | Rebuildable under pressure |
| Operational logs | 5 GB | Strict file-size and backup-count rotation |
| Archive staging/quarantine | 10 GB | Bounded and observable |

Use measured p95 bytes per session to derive actual retention. Do not promise a number of days before measuring.

## 13.2 Evidence classes

**Class A — permanent canonical evidence**

- operator commands;
- capability and activation hashes;
- commissioning transitions;
- strategy/config/risk hashes;
- intents and admission decisions;
- command dispatch evidence;
- acknowledgements;
- order states;
- fills;
- cancels/rejects;
- risk decisions;
- position snapshots;
- reconciliation events;
- rollover decisions;
- archive manifests.

**Class B — long-lived analytical evidence**

- trades;
- top-of-book quotes;
- periodic book snapshots;
- derived microstructure;
- latency/slippage;
- P&L;
- strategy/session summaries.

**Class C — rotating high-volume observations**

- every depth mutation;
- duplicate raw/normalized forms after verified archive;
- verbose diagnostics;
- temporary replay products.

Pin Class C sessions containing any trade, commissioning block, unexplained fill, reconciliation anomaly, unusual slippage, loss/profit excursion, data gap, or safety incident.

## 13.3 Atomic archive flow

At session close:

1. close the raw partition;
2. verify record integrity;
3. calculate final book hash;
4. write Parquet to a temporary path;
5. record counts, time bounds, streams, contract, session, schema, and source;
6. calculate SHA-256;
7. atomically rename;
8. read back through DuckDB;
9. replay sufficient data to verify final book hash;
10. append archive manifest to the canonical ledger;
11. delete superseded raw data only after every check passes.

## 13.4 Disk watermarks

```text
<70% used      normal
70–78%         compact/archive all closed sessions
78–85%         expire oldest verified Class C hot partitions and rotate logs
>85%           essential evidence only; deny new entries
still >85%     disarm and return BLOCKED_STORAGE_PRESSURE
```

Never delete canonical order/fill/risk/commissioning evidence in response to pressure.

## 13.5 DuckDB

Add DuckDB as the embedded cold analytical layer for:

- Parquet queries;
- P&L and execution-marker series;
- fill-quality distributions;
- confluence/session analysis;
- archive integrity checks;
- replay selection.

It must never be on the live order path.

## 13.6 QuestDB

Implement an optional `QuestDbHotSink` and benchmark harness:

- loopback only;
- partitioned time-series schema;
- quotes, trades, depth, book snapshots, derived metrics;
- health and backpressure reporting;
- open-source TTL where available;
- no canonical order ledger.

Do not make QuestDB a mandatory live-readiness dependency. If Docker/service installation is absent or its benchmark does not clearly outperform the bounded native spool for actual Lane III load, leave it disabled and document the result.

---

# 14. One-control operator experience

Rework the Lane III dashboard into an actual operator console.

## 14.1 Main dashboard

The first screen should show, without scrolling on a normal desktop:

- mode: `PAPER`, `PROVIDER EVALUATION`, or `LIVE CAPITAL`;
- sanitized account alias and account class;
- concrete MNQ contract and rollover state;
- session kind, family, trade date, generation, and entry window;
- runtime state;
- strategy state;
- position and quantity;
- owned and foreign working orders;
- protective status;
- daily and trade risk remaining;
- realized/unrealized/total P&L;
- observer/account/position/order/execution/transport health;
- ledger checkpoint/tail trust;
- storage pressure;
- current exact blockers.

Use smaller tabs and reserve secondary detail for drawers/panels.

## 14.2 P&L chart

Implement the requested Robinhood-style P&L line chart:

- paper/provider-evaluation/live selector;
- green when net positive, red when net negative;
- P&L value visually attached to the chart;
- entry, partial fill, exit, stop, flatten, and incident markers on the line;
- hover details with sanitized IDs;
- current session and selectable historical sessions;
- no implication that simulated and live P&L are the same capital class.

## 14.3 Start control

Normal live operation must use exactly one atomic start control:

**`START LIVE — 1 MNQ CANARY`**

Requirements:

- disabled until every gate is green;
- press-and-hold approximately three seconds to resist accidental activation while remaining one control;
- progress ring and plain-language consequence;
- one POST endpoint;
- one retained idempotency request ID across timeout/retry;
- server rechecks all gates under one admission lock;
- server seals the activation before arming;
- activation starts the live strategy and waits for a natural signal;
- it does not manufacture or manually force an entry;
- duplicate UI requests return the same lifecycle;
- after one round trip, any safety fault, or session cutoff, auto-disarm.

Do not expose legacy split ARM and ENTRY controls in the normal UI.

## 14.4 Emergency control

Always show a separate unmistakable:

**`FLATTEN MNQ & DISARM`**

This is not a second start step; it is the emergency control. It must work even while the start request is pending.

Also show the NinjaTrader-native/out-of-band kill status.

## 14.5 Gate drawer

Every blocker must have:

- exact machine code;
- plain-language meaning;
- current observed value;
- required value;
- whether Codex/software can repair it;
- an idempotent repair action when safe;
- no generic “not ready” message.

## 14.6 Ergonomic quality

Run automated browser and screenshot review at representative desktop sizes. Validate:

- no hidden start/kill controls;
- keyboard navigation;
- visible focus;
- no ambiguous red/green-only meaning;
- no stale status after reconnect;
- no double-submit;
- no layout overlap;
- no raw exception text;
- no account secrets;
- actionable errors;
- responsive chart;
- reasonable update rate without observer starvation.

Add Playwright or an equivalent deterministic end-to-end harness when practical, plus accessibility checks. Any discovered safety-relevant or high-friction issue must be fixed and regression-tested.

---

# 15. Operator-side automation

Implement and use these idempotent scripts, adapting names only when repository conventions require it:

```text
scripts/l3h_bootstrap.ps1
scripts/l3h_status.ps1
scripts/l3h_deploy_ninjatrader.ps1
scripts/l3h_verify_install.ps1
scripts/l3h_start.ps1
scripts/l3h_stop.ps1
scripts/l3h_kill.ps1
scripts/l3h_archive.ps1
scripts/l3h_audit.ps1
scripts/l3h_recover.ps1
```

## 15.1 Bootstrap

Automate:

- environment/root resolution;
- fixed-drive and free-space validation;
- directory creation;
- ACL-restricted key/capability locations;
- key generation without printing;
- local configuration templates;
- Python/node dependency verification;
- port ownership checks;
- desktop/start-menu shortcuts;
- single-instance launcher configuration;
- task-scheduler registration;
- log rotation;
- health/status output.

## 15.2 NinjaTrader deployment

Automate as far as safely possible:

- source fingerprint update;
- source-to-installed copy with backup and hash proof;
- no stray duplicate AddOn copies;
- NinjaTrader startup;
- unattended login using existing DPAPI-seeded tooling;
- active workspace restoration;
- compilation/recompile through existing safe UI automation when required;
- visible compile-error detection;
- compiled DLL timestamp/hash capture;
- AddOn hello/provenance verification;
- clean restart and second parity proof.

Do not accept broker agreements, market-data purchases, risk disclosures, or new financial permissions on Joseph’s behalf. Detect and report those as exact operator blockers.

## 15.3 Scheduler

Create or update scheduled tasks for:

- pre-session BeezConsole/NinjaTrader bring-up;
- safe health verification;
- closed-session archival;
- nightly incremental ledger verification;
- periodic storage audit;
- stale-process cleanup only while flat/disarmed.

A scheduled task must never arm or start live trading.

## 15.4 Status and repair

`l3h_status.ps1` must give one sanitized page with:

- process/listener ownership;
- AddOn parity;
- account class;
- contract;
- runtime;
- broker facts;
- ledger;
- storage;
- activation readiness;
- exact blockers.

Safe repairs may restart dead observation services, restore the workspace, archive closed spools, or rebuild the UI. They must never widen authority or send an order.

---

# 16. Adversarial and chaos testing

Add focused test modules such as:

```text
tests/test_l3h_authority.py
tests/test_l3h_event_store.py
tests/test_l3h_epochs.py
tests/test_l3h_reconciliation.py
tests/test_l3h_risk.py
tests/test_l3h_transport.py
tests/test_l3h_ninjascript_source.py
tests/test_l3h_storage.py
tests/test_l3h_nrd_replay.py
tests/test_l3h_fill_calibration.py
tests/test_l3h_control_center.py
tests/test_l3h_live_canary_gates.py
tests/test_l3h_chaos.py
```

Test at least:

- duplicate command;
- duplicate execution;
- reordered command;
- stale/future command;
- invalid signature;
- wrong schema;
- wrong account;
- ambiguous account;
- wrong account class;
- evaluation account mislabeled live;
- wrong instrument;
- expired contract;
- quantity two;
- pyramiding;
- averaging;
- same-event reversal;
- entry while position exists;
- entry while working order exists;
- foreign order/activity;
- market-data gap;
- callback generation reset;
- stale quote/trade/depth;
- crossed or locked malformed book;
- lost transport after command seal but before send;
- lost transport after send but before acknowledgement;
- AddOn restart with working entry;
- AddOn restart with live position and stop;
- Python crash after fill;
- heartbeat loss;
- stop rejection;
- stop cancellation;
- stop modification no-op;
- partial fill protection;
- duplicate protective order;
- cancel no-op;
- late fill after cancel;
- unknown broker order;
- balance feed zero/outage;
- account disconnect;
- sequence/timestamp reversal;
- session transition;
- rollover;
- daily loss latch;
- canary-complete latch;
- disk full and >85% watermark;
- corrupt active tail;
- stale projection checkpoint;
- archive-drive loss;
- duplicate UI request;
- browser reload during arm;
- emergency flatten during pending start;
- out-of-band kill with Python dead;
- recovery never resends an unresolved entry.

Property/invariant tests must continuously assert:

```text
absolute_position <= 1
pending_entries <= 1
no entry without durable command seal
no duplicate side effect per command ID
no authority widening
no live activation from Sim101
no live label for evaluation account
UNKNOWN never maps to FLAT
exposure implies protection or active emergency flatten
no re-arm after canary completion without a new epoch
```

Add mutation-style tests or deliberate source mutants for the highest-risk guards. A test suite that never proves it can fail is not sufficient.

---

# 17. Installed NinjaTrader commissioning on `Sim101`

The autonomous pass must perform real installed-runtime mechanical commissioning on `Sim101`; mocks alone are insufficient.

## 17.1 Negative controls

With the exact installed live AddOn but a test capability that cannot address real capital, prove refusal for all authority errors above.

## 17.2 Controlled mechanics

Complete at least:

- one controlled long `Sim101` round trip;
- one controlled short `Sim101` round trip;
- protective stop accepted for each;
- controlled exit;
- exact flat proof;
- zero owned working orders;
- no foreign mutation;
- clean disarm;
- ledger closure verification.

These are mechanical tests, not alpha tests. Use explicit commissioning commands and keep them distinguishable from natural strategy trades.

## 17.3 Natural policy rehearsal

Run the exact production validation graph in read-only rehearsal and, during an authentic session when available, allow the paper strategy to arm only through its existing atomic path. Do not alter thresholds if no natural signal occurs.

## 17.4 Restart matrix

Test clean and unclean restart:

- flat/disarmed;
- pending sealed command not dispatched;
- submitted/ack unknown;
- working order;
- filled and protected;
- exit pending.

Only safe states may return to `READY_DISARMED`. Ambiguous states become `QUARANTINED` or trigger tested safety action.

## 17.5 Performance

Measure under authentic market load:

- observer freshness;
- accepted callbacks;
- local gaps;
- queue depth;
- ledger writer latency;
- reconciliation latency;
- UI freshness;
- archive throughput.

Do not allow telemetry or verification to starve market ingestion again.

---

# 18. Live-readiness gate

The runtime may emit `LIVE_READY_DISARMED` only when every category passes.

## 18.1 Source and installation

- worktree clean;
- branch pushed;
- complete tests pass on Windows and Linux-compatible CI surfaces;
- frontend tests/build/audit pass;
- exact repository/installed AddOn source parity;
- compiled DLL provenance captured;
- no duplicate NinjaScript source;
- no secrets.

## 18.2 Account and rules

- exact single account bound;
- independently truthful account class;
- account enabled for the intended mode;
- complete broker/prop rule profile;
- rule hash frozen;
- rules enforced in both Python and AddOn;
- no unresolved agreement, credential, market-data, or permission blocker.

## 18.3 Contract and market data

- exact active MNQ expiry;
- no rollover/liquidation-only conflict;
- authentic quote/trade/depth flow;
- reviewed ordering authority;
- acceptable book scope/completeness for the live policy;
- fresh current-session evidence;
- no continuity gap.

## 18.4 Broker truth

- connection healthy;
- account snapshot fresh;
- exact position `FLAT`;
- quantity `0`;
- owned working orders `0`;
- unclassified working orders `0`;
- startup reconciliation pass;
- continuous reconciliation healthy;
- no unresolved command/order/execution.

## 18.5 Risk and protection

- one MNQ maximum;
- one round-trip canary maximum;
- trade risk no greater than `$50`;
- daily loss no greater than `$200`;
- no averaging/pyramiding/reversal;
- session/flat deadlines active;
- protective stop mechanics proven;
- heartbeat watchdog proven;
- NinjaTrader-native kill proven;
- out-of-band kill proven.

## 18.6 Evidence and storage

- canonical active-tail chain valid;
- epoch chain valid;
- projection checkpoint agrees;
- current incremental verification pass;
- no unknown authority mutation after anchor;
- hot storage healthy;
- below denial watermark;
- archive spool bounded;
- final manifests valid.

## 18.7 UI

- one-control start present;
- activation disabled until gates pass;
- emergency control present;
- idempotent atomic endpoint;
- account class and capital mode honest;
- P&L chart and executions correct;
- no P0/P1 ergonomic defect.

If the only qualifying account is a provider evaluation account, emit:

`PROVIDER_EVALUATION_READY_DISARMED`

Do not emit `LIVE_READY_DISARMED`.

---

# 19. Real-capital canary behavior

Build this path completely, but do not autonomously activate it.

When Joseph deliberately presses and holds **START LIVE — 1 MNQ CANARY**:

1. create one operator activation;
2. rerun every preflight gate under the runtime lock;
3. reserve one commissioning epoch;
4. arm the exact live strategy;
5. wait for a natural strategy signal;
6. submit no more than one MNQ;
7. enforce protection;
8. manage the existing strategy exit or controlled safety exit;
9. prove flat and zero orders;
10. auto-disarm;
11. run incremental closure verification;
12. mark:
    - `LIVE_CANARY_COMPLETE`, or
    - `LIVE_CANARY_INCOMPLETE`, or
    - `LIVE_CANARY_ABORTED`.

After the first completed round trip, no second live entry is permitted without a new reviewed authority artifact and subsequent handoff.

If no natural signal occurs, remain safely armed only within the bounded session policy, then disarm at cutoff and report `LIVE_RUNNING_NO_SIGNAL` followed by a clean closure. Never loosen policy to force the canary.

---

# 20. Autonomous audit and repair loop

After implementation and initial commissioning, run a bounded autonomous hardening loop.

## 20.1 Audit passes

1. **Authority/security audit**
   - capability isolation;
   - keys and ACLs;
   - authentication;
   - origin/CSRF;
   - account privacy;
   - dependency direction;
   - paper/live separation.

2. **Execution/reconciliation audit**
   - state machine;
   - idempotency;
   - unknown handling;
   - protection;
   - watchdog;
   - kill controls;
   - foreign activity;
   - partial fills.

3. **Ledger/recovery audit**
   - write-ahead evidence;
   - canonical hashes;
   - optimistic stream versions;
   - checkpoints;
   - epochs;
   - corrupt-tail behavior;
   - rebuild projections.

4. **Data/storage audit**
   - provenance;
   - gaps;
   - throughput;
   - replay determinism;
   - archive verification;
   - watermarks;
   - no evidence deletion.

5. **UI/ergonomic audit**
   - one-control flow;
   - emergency access;
   - status honesty;
   - chart accuracy;
   - keyboard/accessibility;
   - responsive layout;
   - exact error guidance.

6. **Installed-runtime audit**
   - source/build parity;
   - process/listener ownership;
   - clean restart;
   - Sim101 mechanics;
   - task scheduler;
   - desktop shortcuts;
   - status scripts.

## 20.2 Repair rule

For each reproducible defect:

1. assign severity and stable defect ID;
2. write a failing regression test or deterministic reproducer;
3. fix the root cause;
4. run focused tests;
5. run the entire backend and frontend suite;
6. repeat installed-runtime verification when affected;
7. update the defect register and evidence.

Repeat the audit after repairs.

Stop only when:

- zero open P0;
- zero open P1;
- no known live-safety P2;
- no known high-friction one-control ergonomic P2;
- every safe auto-repair is complete;
- final state is truthfully derived.

Use a bounded maximum of six full audit/repair cycles. If a release-blocking defect survives, return `BLOCKED_UNRESOLVED_DEFECTS` with exact IDs and evidence—not `COMPLETE`.

---

# 21. CI and structural guards

Extend CI to include:

- all current jobs;
- new `l3h` tests;
- source compilation;
- frontend test/build/audit;
- structural account/capability direction checks;
- no paper/live schema or key-path collision;
- no duplicate AddOn sources;
- no tracked secret/runtime artifacts;
- version/source-fingerprint consistency;
- no new order-producing route without authentication/idempotency tests;
- no CI battery omitted from the workflow;
- no empty mutation/adversarial battery passing vacuously.

Keep local-only installed-NinjaTrader parity checks outside hosted CI, but make them mandatory in `scripts/l3h_audit.ps1`.

---

# 22. Third-party reference policy

Use the ten surveyed projects as design references:

- NautilusTrader: reconciliation and live lifecycle;
- LEAN: order-state and transaction semantics;
- vn.py: gateway modularity;
- SocketTrader: NinjaTrader readiness/idempotency/reconnect patterns;
- nt8-riskguard: clean-room failure cases and adversarial tests only;
- nrdtocsv: replay concepts and raw field semantics;
- hftbacktest: latency/queue/fill calibration;
- eventsourcing: versioned events, projections, snapshots;
- QuestDB: optional hot time-series retention;
- DuckDB: embedded Parquet analytics.

Do not vendor wholesale engines.

Do not copy code from repositories without a compatible license. In particular:

- treat `nt8-riskguard` as clean-room requirements only;
- do not copy StockSharp;
- do not add GPL code into Beelzebub merely for convenience;
- record exact dependencies, versions, licenses, and notices for code actually used.

---

# 23. Commit discipline

Use focused commits such as:

```text
chore(l3h): preserve pre-live working state
feat(l3h): add live authority and capability contracts
feat(l3h): add canonical event store and projections
feat(l3h): add continuous NinjaTrader reconciliation
feat(l3h): add isolated live execution addon
feat(l3h): add independent live risk and kill controls
feat(l3h): add reviewed market-data authority
feat(l3h): add native replay importer and fill calibration
feat(l3h): add bounded archive and DuckDB analytics
feat(l3h): add one-control live operator dashboard
ops(l3h): automate Windows deployment and commissioning
test(l3h): add installed-runtime and chaos batteries
fix(l3h): close autonomous audit defects
docs(l3h): freeze commissioning and closure evidence
```

Never hide unsafe intermediate work in one giant commit. Do not amend published history.

Push the final branch and ensure the worktree is clean.

---

# 24. Required final artifacts

At minimum:

```text
docs/commissioning/lane-iii-phase-h/architecture.md
docs/commissioning/lane-iii-phase-h/authority-matrix.md
docs/commissioning/lane-iii-phase-h/live-capital-specification.md
docs/commissioning/lane-iii-phase-h/live-policy-v0.json
docs/commissioning/lane-iii-phase-h/live-risk-canary-v0.json
docs/commissioning/lane-iii-phase-h/live-capability-template.json
docs/commissioning/lane-iii-phase-h/adversarial-test-evidence.md
docs/commissioning/lane-iii-phase-h/fill-calibration-report.json
docs/commissioning/lane-iii-phase-h/storage-benchmark.json
docs/commissioning/lane-iii-phase-h/defect-register.md
docs/commissioning/lane-iii-phase-h/operator-runbook.md
docs/commissioning/lane-iii-phase-h/rollback-and-kill.md
docs/commissioning/lane-iii-phase-h/commissioning-record.md
docs/commissioning/lane-iii-phase-h/closure-audit.md
docs/commissioning/lane-iii-phase-h/third-party-design-references.md
```

Update `docs/CODEX_CONTEXT_HANDOFF.md` with the final `l3h` state and next exact action.

---

# 25. Required final response

Return a concise but complete evidence block in exactly this spirit:

```text
STATUS:
PHASE:
BRANCH:
PUBLISHED_START_SHA:
LOCAL_START_SHA:
PRESERVATION_BRANCH:
PRESERVATION_COMMIT:
FINAL_COMMIT:
PUSH:
WORKTREE:

AUTHORITY:
ACCOUNT_ALIAS:
ACCOUNT_CLASS:
LIVE_CAPITAL_CLASSIFICATION:
CONTRACT:
MAXIMUM_QUANTITY:
CANARY_LIMIT:
POLICY_HASH:
RISK_HASH:
PROP_RULE_HASH:
CAPABILITY_HASH:

NINJATRADER_VERSION:
REPOSITORY_ADDON_HASH:
INSTALLED_ADDON_HASH:
COMPILED_DLL_HASH:
ADDON_PROVENANCE:
LISTENER_OWNERSHIP:

DATA_AUTHORITY:
BOOK_SCOPE:
AUTHENTIC_QUOTES:
AUTHENTIC_TRADES:
AUTHENTIC_DEPTH:
GAPS:
REPLAY_DETERMINISM:

LEDGER_EPOCH:
LEDGER_CHAIN:
ACTIVE_TAIL:
PROJECTION_CHECKPOINT:
VERIFICATION:
RECOVERY_TESTS:

RECONCILIATION:
POSITION:
OWNED_WORKING_ORDERS:
FOREIGN_OR_UNKNOWN_ORDERS:
PROTECTIVE_INVARIANT:
WATCHDOG:
NATIVE_KILL:
OUT_OF_BAND_KILL:

STORAGE_ROOT:
MANAGED_CAP:
FREE_SPACE:
ARCHIVE:
DUCKDB:
QUESTDB_DECISION:

UI:
ONE_CONTROL_START:
EMERGENCY_CONTROL:
PNL_CHART:
ERGONOMIC_AUDIT:

BACKEND_TESTS:
WINDOWS_TESTS:
FRONTEND_TESTS:
NINJASCRIPT_TESTS:
CHAOS_TESTS:
SIM101_LONG:
SIM101_SHORT:
RESTART_MATRIX:
DEPENDENCY_AUDIT:

OPEN_P0:
OPEN_P1:
OPEN_LIVE_SAFETY_P2:
AUTO_REPAIR_CYCLES:

LIVE_CANARY_SENT:
LIVE_CANARY_RESULT:
TERMINAL_BLOCKERS:
NEXT_OPERATOR_ACTION:
```

`LIVE_CANARY_SENT` must remain `NO` unless Joseph himself deliberately activated the final control during the task.

The final status must be one of:

```text
LIVE_READY_DISARMED
PROVIDER_EVALUATION_READY_DISARMED
LIVE_CANARY_COMPLETE
BLOCKED_<EXACT_REASON>
```

Do not say “live ready” if only an evaluation account exists. Do not say “complete” merely because code compiles. Do not say “blocked” without exhausting every safe software-side repair first.

---

# 26. Final priority order

Work in this order so the live vertical path is never sacrificed to peripheral infrastructure:

1. preserve local state;
2. close baseline P0 defects;
3. authority/capability contracts;
4. canonical event ledger and projections;
5. isolated live NinjaTrader AddOn;
6. continuous reconciliation;
7. independent risk, protection, watchdog, and kill controls;
8. one-control backend and UI;
9. installed Sim101 commissioning and restart matrix;
10. truthful account/data/rule live-readiness classification;
11. storage/archive/DuckDB;
12. `.nrd` replay and fill calibration;
13. optional QuestDB adapter and benchmark;
14. autonomous audit/repair cycles;
15. final freeze, deployment proof, runbook, and clean push.

There is no deliberate deferral of core engineering to another handoff. The only legitimate remaining boundary is an external fact software cannot create or Joseph’s deliberate live-capital activation.

Build it like a trading machine that expects every socket to die, every callback to arrive twice or late, every UI request to retry, every disk to fill, every broker fact to become ambiguous, and every human to need one clear control under pressure.

Make the machine conservative in authority, aggressive in diagnosis, relentless in recovery, and incapable of lying about what it knows.
