# Beelzebub paper profile selector

## Scope

Slim Mode exposes four compiled, paper-only Sim101 profiles:

| Selector label | Immutable version | Behavior |
| --- | --- | --- |
| High confidence | `NY_HIGH_CONFLUENCE_COMMISSIONING_V1` | New York high-confluence profile with one entry opportunity per session. |
| Scalper | `BEELZEBUB_SCALPER_V2` | Multi-session V2 scalper. This is the initial default only when no explicit or established selection exists. |
| 5-minute session bias (legacy V1) | `BEELZEBUB_FIVE_MINUTE_BIAS_V1` | Decides once per five-minute boundary, but retains the legacy holiday, session-window, hard-flat, and maximum-age fences. |
| 5-minute perpetual position | `BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2` | Uses the latest durable non-tied completed five-minute bias, holds exactly one MNQ, and stages an exit, signed-flat reconciliation, then one opposite entry when the bias reverses. |

Every bundle contains both a policy artifact and its risk artifact. The selected
policy hash and risk hash are used by the runtime, ledger, signed AddOn session
grant, command validation, status API, and restart proof. All bundles remain
sealed to `Sim101 / LOCAL_SIMULATION / MNQ SEP26 / quantity 1`; live capital is
denied.

The perpetual V2 bundle alone removes routine strategy holidays, entry windows,
hard-flat deadlines, and position-age exits. It still denies entry during the
exchange's scheduled maintenance, weekend closure, and intraday halt, and it
retains connection/freshness, complete reconciliation, protective-stop,
quantity-one, exact `Sim101 / LOCAL_SIMULATION / MNQ SEP26`, daily-loss, and
live-capital-denial gates. A flat V2 runtime is never presented as healthy: its
Slim status is `FLAT — BLOCKED: <exact reason>` until a reconciled `+1` or `-1`
MNQ position is present.

A successfully filled, runtime-owned protective stop is an ordinary position
exit: its loss and counters are applied exactly once, signed flat/no-order
reconciliation is required, and the operation may use a later completed
30-second boundary if every remaining risk gate permits it. The stopped
position's checkpoint cannot authorize a replacement entry. Rejected,
cancelled, missing, mismatched, or unaccountable protection remains a safety
failure which locks entry authority and invokes the existing emergency path.

## Operator workflow

1. In Slim Mode, choose a profile and press **Switch & Start**.
2. The authenticated backend creates an immutable handoff manifest and a new,
   unused run directory.
3. The current runtime pauses entry authority, exits or cancels only its owned
   Sim101 MNQ work, and waits for exact reconciled flat/no-orders truth.
4. At that flat boundary, the backend seals a hash-bound risk-continuity
   artifact containing the account's exchange trade-date P&L and entry count,
   each strategy profile's loss streak and trade-date entry count, each
   profile/session entry count and P&L, accounted execution IDs, and any durable
   risk-authority lockout. Switching
   profiles therefore cannot reset the account-wide $200 loss boundary or
   count the same execution twice, while a strategy does not inherit another
   strategy's entry cap or consecutive-loss streak. One exchange trade-date
   loss budget is cumulative across Asia, London, New York RTH, NY-after, and
   every profile.
5. A legacy-V1 to perpetual-V2 switch also seals the current completed
   five-minute boundary chain. The chain begins at the latest non-tied signal,
   includes every consecutive tied boundary through the current completed
   boundary, and carries the exact observation/evidence provenance needed to
   recompute it. A missed boundary clears this authority instead of exporting
   an older direction.
6. A detached local supervisor is started, then the current process requests
   its normal lifespan-owned shutdown.
7. Shutdown seals admission, drains the writer, quiesces the verifier, proves
   the durable tip, performs the final complete checkpoint, and records the
   receipt in the handoff state.
8. Only a complete receipt plus the exact operation, source, target, and
   continuity-artifact bindings release the supervisor. It starts the selected
   profile with the prepared ledger, audit root, epoch, policy, risk, continuity,
   and Git identity.
   For V2, the supervisor first runs a bounded process-owned Full scan of the
   closed V1 ledger and requires its exact shutdown tip. Crossing into a newer
   five-minute bucket, a timeout, an incomplete seed chain, or a mismatched
   report blocks before the target launch claim.
9. The new backend must match every manifest binding. It then uses the ordinary
   paper auto-start sequence: NinjaTrader readiness, observer/authentication,
   Sim101 reconciliation, Full ledger verification, and canonical persistent
   `operational-start`.

Operational start is never routed through Atomic Commissioning.

## Automatic storage

Each selection gets a distinct run root:

```text
<runtime-root>/profiles/<profile-version>/runs/<UTC-run-id>/
  hot/lane_iii_paper.sqlite3
  audit/
```

The ledger epoch is generated with the run and cannot reuse another profile's
chain. Previous database, WAL, SHM, audit, and analysis artifacts are never
deleted, truncated, rewritten, or resequenced by the selector.

Handoff evidence is stored under:

```text
<runtime-root>/profile-switch/
  profile-selection.json
  profile-switch-audit.jsonl
  operations/<operation-id>/
    manifest.json
    risk-continuity.json
    perpetual-startup-seed.json
    perpetual-startup-seed-proof.json
    state.json
    supervisor-audit.jsonl
    target-backend.log
```

Each profile run also has a local exact-tip anchor beside its ledger. A second,
hash-chained guard is append-only under
`<runtime-root-parent>/risk-continuity-authority/`, outside the profile run tree
that an operator might restore from a run-level backup. Its location is derived
from the ledger path and run layout, not from a mutable environment setting.
An explicit selector root is accepted only when this derived guard remains
outside the root. Ambiguous nested layouts fail startup with
`PROFILE_SWITCH_RISK_GUARD_LAYOUT_UNSAFE` before the paper ledger is opened.

`profile-selection.json` separately records the latest requested target and the
last run proven to be running. A request never becomes the established startup
selection until the target binding and ordinary paper auto-start both report
`RUNNING`. Normal restart uses the exact established profile, ledger path,
audit root, ledger UUID, epoch, strict target/source-operation state, and
checkout binding. Before constructing `PaperLedger`, startup opens the existing
database read-only and verifies its stored UUID, epoch, schema, policy, risk,
and profile metadata. It also requires both the adjacent exact-tip
risk-continuity anchor and the independent hash-chained guard to equal the
database's latest execution/risk sequence and hash. Missing, empty, replaced,
truncated, or older same-UUID established ledger evidence therefore blocks
startup before SQLite can create or restore a lower allowance. Restoring a run's
database and adjacent anchor together still falls behind the independent guard;
a failed target remains only requested.

`manifest.json` and `risk-continuity.json` are hash-bound and written once.
`state.json` binds the artifact hash and is the current projection; the JSONL
files retain the append-only transition history. On later process restarts, the
target ledger's immutable risk-accounting records restore the same counters and
deduplication IDs without depending on browser state. Conflicting reuse of an
execution ID (price, quantity, direction, role, profile, context, or P&L) blocks
entry authority instead of accepting the first value. Non-daily safety lockouts
survive restart and profile changes; only `DAILY_LOSS_LIMIT` expires, and only
when the canonical exchange trade date advances. A lockout transition publishes
a durable ledger row or adjacent write-ahead pending marker before the in-memory
authority changes. A pending marker after append failure or process interruption
forces the next start into continuity lockout rather than restoring fresh entry
authority. Slim Mode remembers the
last explicit selector choice locally, while the backend remains authoritative
for the active or in-progress profile. The continuity artifact also binds the
source ledger path, identity, epoch, and final covered risk-record sequence/hash.
The supervisor requires that watermark to equal the controlled-shutdown receipt,
so a risk record appended after the snapshot blocks the handoff.

The local risk-continuity anchor and independent append-only guard advance after
every durable raw execution, accounted entry/exit, realized-P&L,
imported-continuity, or authority-lockout row. Full verification validates the
guard as well as any existing trusted checkpoint before it can publish a
replacement, so a full rescan cannot bless an older same-identity ledger image
by regressing either boundary.

A new zero-record ledger establishes its guard before its first risk record.
An existing ledger with risk history but no guard is never adopted implicitly:
startup and Full verification report `RISK_CONTINUITY_GUARD_ADOPTION_REQUIRED`
and keep entry authority locked. A separately authorized, stable-ledger Full
proof and one-time adoption procedure is therefore a deployment prerequisite
for a pre-guard operational ledger; this development handoff does not perform
that operational adoption. Once an anchor names a guard, a missing guard is
`RISK_CONTINUITY_GUARD_MISSING` and is not recreated.

## Fail-closed outcomes

No target process is launched when the current profile cannot reach exact
reconciled flat/no-orders truth, when its controlled ledger shutdown is
unproven, when the manifest hash or compiled profile hashes differ, or when the
continuity artifact is missing, altered, or bound to another operation/profile,
when the required startup seed or its closed-ledger Full proof is unavailable,
stale, incomplete, timed out, or mismatched, or when the runtime paths and Git
binding differ. While that bounded scan is running, the durable stage is
`VERIFYING_STARTUP_SEED`; it never masquerades as `STARTING_TARGET`.

Only one supervisor may create the target process: an atomic launch claim makes
duplicate or concurrent supervision fail closed. The supervisor also requires
the shutdown receipt, old process exit, and release of the loopback control port
within one bounded handoff. Late origin receipts cannot overwrite a terminal
supervisor result. Unreadable state is preserved and a blocker is appended to
the separate supervisor audit instead of rewriting damaged evidence.

If the target backend launches but NinjaTrader readiness, reconciliation, Full
ledger verification, or operational start fails, the target remains disarmed.
Slim Mode reports `BLOCKED_SAFE` and the exact blocker; there is no blind retry
and no fallback to another profile. If operation reaches `RUNNING` but durable
selection persistence fails, the distinct
`RUNNING_SELECTION_PERSISTENCE_FAILED` status prevents a false claim that the
target will be remembered after restart.

Because the old API intentionally disappears during handoff, the detached
Windows supervisor also displays a one-shot terminal failure notice containing
the requested target and exact blocker. The state and append-only audit remain
authoritative if Windows cannot display that notice; no extra resident status
service is introduced.

## Exchange-session boundary

NY-after observation starts at 16:00 ET. Its entry cutoff is 16:15 ET, where
the normal 16:15-16:30 ET equity-index halt begins. The hard flat deadline is
16:58 ET and the session ends at 17:00 ET. The daily CME
maintenance closure is 17:00-18:00 ET. Friday after 17:00, Saturday, and Sunday
before 18:00 ET are classified as weekend closed. These calendar gates are
timezone-aware and testable with fixed timestamps; they do not require an open
market or an operating paper service.
