# L3H.3 live-capital authorization boundary

Date: 2026-08-30 (America/Denver)

## 1. Terminal state and immutable references

`BLOCKED_LIVE_ACCOUNT_IDENTITY`

- Initial observed HEAD before preserving the installed L3H.2 work:
  `5307a1e96321709059f01468ff1d7b9da4d779d9`.
- Starting mechanically commissioned baseline commit:
  `e54318971b7aa03ff2adc5ac7594b709409cd50f`.
- Preservation reference:
  `codex/l3h-pre-live-authorization-preservation-20260830T2101MDT` at
  `e54318971b7aa03ff2adc5ac7594b709409cd50f`.
- Implementation branch: `codex/l3h3-live-authorization-boundary`.
- Final implementation commit:
  `e61f254a702f4c2254d19f44ab9ed02e3c86f325`.
- This evidence document is committed after the implementation commit. Its
  containing evidence-commit hash is reported in the final operator report;
  a Git commit cannot embed its own hash.
- The pre-existing dirty L3H.2 commissioning changes were reviewed, preserved
  in the starting commit, and were not discarded or rewritten.

Runtime versions observed during this pass were NinjaTrader `8.1.6.3`, Python
`3.12.10`, Node `24.14.1`, npm `11.11.0`, PowerShell `7.6.4`, and Git
`2.53.0.windows.2`.

Final safety state:

- `LIVE_ACCOUNT_IDENTITY=UNVERIFIED`
- `ACCOUNT_CLASS=UNKNOWN`
- `LIVE_AUTHORIZATION_BOUNDARY=IMPLEMENTED`
- `LIVE_AUTHORITY=DISARMED`
- `LIVE_CANARY=NOT_RUN`
- `LIVE_SEND_COUNT=0`

The new native AddOn source was deliberately not installed or compiled into
the running NinjaTrader instance. The running instance remains the frozen,
installed L3H.2 Sim101 baseline. The running BeezConsole process was also not
restarted because it owns the safety-critical `48135` listener and no
maintenance window was established. Consequently, native L3H.3 provenance and
the running schema-v3 API are explicitly unverified, not silently promoted.

## 2. Exact implementation scope

The change inserts one authorization boundary into the existing path:

`strategy/runtime -> L3H admission -> authenticated gateway -> native AddOn -> NinjaTrader`

It does not add another execution adapter or an HTTP activation endpoint. The
implementation adds:

- an in-memory live-authorization state machine and one-shot capability;
- exact native account, provider, connection, build, AddOn, gateway, runtime,
  contract, and quantity bindings;
- a signed native admission envelope above the existing signed gateway frame;
- mandatory live-envelope verification in the gateway and again in the native
  AddOn immediately before submission;
- atomic consumption, expiry, session binding, and durable transition audit;
- an independent risk-reduction classification and native exposure watchdog;
- read-only schema-v3 status projection and fail-closed UI rendering;
- a no-dispatch commissioning/audit tool and live-send sentinel; and
- focused adversarial, backend, native-source, and frontend tests.

Generic L3H `activate()` can no longer grant live authority. Live entry must
use the exact `LiveAuthorizationBoundary` ceremony. Simulation commands are
still classified explicitly as `LOCAL_SIMULATION` and carry
`live_capital=false`.

## 3. Authorization state machine

The first-class states are:

`DISARMED -> PREFLIGHT_PENDING -> PREFLIGHT_READY -> AUTHORIZATION_PENDING -> CANARY_AUTHORIZED -> CANARY_CONSUMED -> VERIFYING_FLAT -> COMMISSIONED_DISARMED`

`QUARANTINED` and `LOCKED` are fail-closed safety latches. Another preflight
cannot clear either latch in the same process. Every process constructs the
boundary as `DISARMED`; no event-ledger replay reconstructs authority. The
native AddOn also clears authentication sessions and consumed-capability state
on restart/reconnect. There is no generic Boolean that grants live entry.

## 4. Exact live-account identity

The identity contract uses only native NinjaTrader metadata observed in the
installed `8.1.6.3` API:

- `Account.Id`, `Name`, `DisplayName`, `Fcm`, `Provider`, and `AccountStatus`;
- connection name, provider, brand, type, mode, demo flag,
  order-management capability, and connection status; and
- platform provenance fixed to `NINJATRADER`.

The raw account identifier is used only to form canonical SHA-256
fingerprints. Audit and status surfaces receive a safe identifier and hashes,
not the raw identifier. Live identity requires provider values that are not
`Simulator` or `Unknown`, `IsDemo=false`, order management enabled, account
status enabled, connection connected, and exact account/connection/provider
fingerprint equality. Name, GUI selection, and “not Sim101” are never treated
as proof. No real live account satisfying this contract was observed, so the
result is `BLOCKED_LIVE_ACCOUNT_IDENTITY`.

## 5. Preflight, human acknowledgement, and capability

The canonical preflight includes the observation and expiry timestamps,
random challenge and nonce, account and connection fingerprints, exact
contract, maximum quantity, flat/working-order/foreign-activity truth,
authenticated gateway and AddOn sessions, AddOn provenance/build,
reconciliation and protection states, all three kill-readiness facts,
Beelzebub and strategy/runtime identities, quarantine/lock/unknown indicators,
and a digest of the complete record.

The local human acknowledgement must echo the exact preflight ID and digest,
challenge, safe account, `LIVE_CAPITAL` class, `MNQ SEP26`, quantity `1`,
authority type `ONE_SHOT_LIVE_CANARY`, and the generated acknowledgement text.
It must declare `LOCAL_HUMAN` over local transport. A generic confirmation is
not accepted.

The capability contains a random ID and nonce; issue and expiry times;
per-process authorization session; preflight ID/digest; account, provider, and
connection fingerprints; exact native and canonical contract identities;
quantity `1`; Beelzebub build; AddOn provenance and session; gateway session;
and authority type. It is signed with an ephemeral per-process key and is held
in memory only. Only safe hashes are persisted in transition evidence.

## 6. TTL, replay protection, and atomic admission

Preflight and authorization TTLs are `60` seconds. Account facts and
reconciliation must be no older than `15` seconds. Expiry is checked by the
server and native envelope; expiry denies and disarms without renewal.

Atomic admission rechecks all facts, validates the complete critical-facts
binding, enforces exact account/class/contract/quantity/build/AddOn/gateway and
runtime sessions, validates entry direction and resulting quantity, and
consumes the capability while holding one lock. Eight simultaneous attempts
produced exactly one local admission. Reuse, duplicate strategy signals,
session changes, copied envelopes, signature changes, and process or AddOn
restarts fail closed. Denial/replay attempts and the safe capability hash are
durably audited.

The native envelope is HMAC-SHA256 signed with the existing local gateway key
and binds the authorization, AddOn, and gateway sessions plus the command,
request, account, contract, quantity, action, build, provenance, and expiry.
The authenticated gateway rejects a bare live entry before transport. The
native AddOn independently verifies the envelope, exact native identity,
flatness, order state, foreign activity, protection, transport, expiry, and
one-shot replay set immediately before `CreateOrder`/`Submit`.

## 7. Admission invariants and action classes

Risk-increasing actions are `ENTER_LONG`, `ENTER_SHORT`, `SCALE_IN`,
`PYRAMID`, and `ATOMIC_REVERSAL`. L3H.3 admits only a one-unit long or short
entry from proven flat. Quantity mutation, scale-in, pyramiding, aliases, other
contracts, and a side/result mismatch are denied. Atomic reversal is denied;
future reversal must be `flatten -> reconcile -> fresh authorization`.

Risk-reducing actions are cancel, owned-order cancel, protection, flatten,
kill/flatten/disarm, and emergency liquidation. They do not require an entry
capability. The three native kill paths remain outside live-entry admission.
The native `exposureGuardActive` latch is explicitly risk reduction, not
authority: it keeps heartbeat-loss protection active after the one-shot live
capability is consumed.

## 8. Reconciliation, protection, and kill requirements

Authorization requires exact flat quantity zero, zero owned working entry
orders, zero unresolved protective orders, zero foreign/unknown activity,
fresh reconciliation status `PASS`, connected/fresh provider and gateway,
healthy protection, and readiness of command, NinjaTrader-menu, and
out-of-band kill paths. Any changed critical fact consumes or invalidates
authority and denies/quarantines. Foreign/unknown activity cannot be adopted.

The preserved L3H.2 Sim101 evidence still proves reconciliation, protection,
and all three kill paths. Those proofs are simulation-only and are not claimed
as live-account commissioning.

## 9. Dashboard and API representation

The read-only schema-v3 status separates mechanical commissioning, live
identity, authorization boundary, current authority, canary, exact account,
contract/quantity, reconciliation, protection, AddOn provenance, gateway,
quarantine/lock, expiry, preflight age, and live-send count. The API has no
live activation/authorization POST route and never reads keys or constructs a
capability.

The UI derives authority only from a fresh server expiry plus exact
`LIVE_CAPITAL`/`VERIFIED` identity and clear quarantine/lock state. API failure,
expired capability, Sim101, browser storage, navigation, and server-supplied
button-enable flags render `DISARMED`. Future canary and browser emergency
buttons are deliberately disabled and have no mutation handler. Viewing the
live page or chart is observational.

The source and production frontend build implement this projection. The
currently running BeezConsole process still serves its frozen schema-v2 API
until an operator-approved maintenance restart; schema-v2 lacks L3H.3 fields,
so the new frontend treats them as unverified/disarmed.

## 10. Adversarial matrix and results

All focused L3H.3 tests passed: `19 passed, 51 subtests passed`.

| Matrix items | Result and proof |
| --- | --- |
| 1-4, 32, 35 | PASS: simulation/unknown identity, wrong account, stale identity, and account A/B switch are denied; switch quarantines. |
| 5-8, 36-38 | PASS: exact `MNQ SEP26`/`MNQU6`, quantity one, entry direction, and resulting exposure are enforced; aliases, NQ, quantity two, mutation, scale/pyramid, and reversal fail. |
| 9-11, 33-34, 39 | PASS: stale preflight, expired capability, digest/challenge mismatch, replay, and duplicate strategy signal deny/disarm. |
| 12-13, 25-27 | PASS: no live mutation API exists; direct/browser-restored state cannot manufacture authority; eight concurrent consumers yield at most one local admission. |
| 14-15 | PASS: preserved gateway tests reject altered payloads, wrong keys, stale timestamps, and replay; a live envelope does not replace gateway HMAC. |
| 16, 23-24 | PASS: AddOn provenance/build/session, gateway session, runtime/build change, restart, and historical-ledger reconstruction invalidate authority. |
| 17-22, 28-31 | PASS: stale reconciliation, unknown/non-flat position, owned/protective/foreign orders, provider or gateway disconnect, unhealthy protection/kill readiness, quarantine, and lock fail closed. |
| 40 | PASS: repository-wide submission audit found only the L3H live AddOn and Sim101-only L3G paper AddOn; the live submit is behind native one-shot verification and the gateway/runtime path carries the envelope. |
| 41-45 | PASS: cancel, flatten, kill, and protection remain independent of entry authority; restart returns `DISARMED`. |
| 46-48 | PASS: preflight and human authorization alone never construct or dispatch a command; the commissioning tool has no dispatch call; the complete synthetic suite crossed no native send seam. |

The native source statically compiled against the installed NinjaTrader
assemblies. Output DLL SHA-256 was
`2DF345D99213B3A8F8DE86C760EFA85161501A39B1AB2125EC0464C370218844`;
the only compiler diagnostic was the pre-existing constant-condition
`CS0162` warning caused by the intentionally pending install fingerprint.

## 11. Regression and integrity results

- Preserved/shared L3H suite: `19 passed` (the original commissioned baseline
  was `18/18`; one read-only status separation test was added).
- Combined L3H/L3H.3 suite: `38 passed, 51 subtests passed`.
- Broad backend suite: `832 passed, 2 deselected, 280 subtests passed` in
  `444.59s`. The two fixed-port F3 listener tests were deselected because the
  already-running BeezConsole safely owns `127.0.0.1:48135`. An initial run
  showed only those two environmental bind failures (`831 passed`); no product
  assertion failed. The process was not stopped outside a maintenance window.
- Frontend: `34/34` tests passed.
- Production frontend: TypeScript and Vite build passed.
- Python compileall, `git diff --check`, and changed-file credential-pattern
  scan passed.
- PowerShell `5.1` and `7.6.4` now compute the same installed AddOn provenance
  fingerprint, which equals the running AddOn hello fingerprint
  `c38bbfd9055f0cf0596f34c52a17e535ff77600c0674786a8ec6752007e98c72`.

## 12. Static bypass audit

The audit searched native submission, order construction, flatten, entry
actions, gateway creation, port `48137`, runtime construction, commissioning
scripts, control routes, and direct AddOn messaging. Native `.Submit` calls
exist only in:

- `BeelzebubLiveExecutionAddOn.cs`, where live entry requires
  `ValidateAndConsumeLiveAuthorization`; and
- `BeelzebubPaperExecutionAddOn.cs`, whose account binding is exact Sim101
  `LOCAL_SIMULATION`.

Gateway entry dispatch requires a valid live admission envelope unless the
command is exact local simulation. Generic runtime activation always denies.
No live browser activation endpoint exists. Audit result: `PASS`; no viable
alternate live-risk path was found.

## 13. Zero-send evidence

The L3H.3 commissioning script performs deterministic fake-metadata boundary
exercise only. It never loads brokerage credentials, starts a provider,
dispatches a command, calls the gateway, or invokes the native order seam. Its
machine-readable record reports:

- `REAL_PROVIDER_INTERACTION=NOT_PERFORMED`
- `LIVE_CANARY=NOT_RUN`
- `LIVE_AUTHORITY=DISARMED`
- `LIVE_SEND_COUNT=0`

The new gateway and native AddOn contain explicit counters immediately before
live transport and native submission. They remain source/static evidence until
the L3H.3 AddOn is installed in a later maintenance window. The currently
running native AddOn is positively bound to `LOCAL_SIMULATION`/Sim101, so its
traffic is not live-capital traffic. No live-capital order was sent in this
pass.

Non-secret local evidence is at:

- `%LOCALAPPDATA%\Beelzebub\authority\l3h\events\l3h3-live-authorization-status.json`
- `.build\l3h3-commissioning-result.json`

## 14. Remaining blockers and exact next action

Blockers:

1. No exact real live account has been positively observed through the native
   identity contract; account class remains `UNKNOWN`.
2. The L3H.3 native source is not installed/visibly compiled, so live AddOn
   provenance is `L3H3_SOURCE_IMPLEMENTED_RUNTIME_UNVERIFIED`.
3. The running BeezConsole has not been restarted into schema v3 because no
   maintenance window was established.

`NEXT OPERATOR ACTION: Establish a maintenance window, then perform only a read-only install/provenance and exact native live-account identity observation; do not authorize or send a canary.`
