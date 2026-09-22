# Caelum oversight — aggressive expedited recovery

- Pass: `20260922-045541Z-aggressive-expedited-recovery`
- Opened: `2026-09-22T04:55:41Z`
- Parent integrator and sole runtime/ledger/install/reload/recovery/window operator: Sol (`/root`)
- Administrative reviewer: Caelum (`beelzebub_admin`)
- Writable scope: this record only
- Initial repository HEAD: `9da4a7003fee8ad267e438caa4be68a30ee16d0d`
- Initial repository tree: `0c82d42c8fb936ae03fb850b70a5a6f473715a61`
- Accepted repair `7ac86fac1e3d4e775df151cacd75ae220394a8be` is an ancestor of the initial HEAD.

## Objective and boundaries

Oversee points 1–6 of the operator-authorized aggressive expedited recovery,
verify attributable evidence, and HOLD only affected transitions. Caelum does
not edit application/runtime/ledger/runbook files and does not start or stop
services, install or reload the observer, recover the ledger latch, consume R2,
arm, issue native directives, or place orders. No Gates B–D or operational
paper start are authorized. Existing 8 GiB warning, 4 GiB minimum, 24-hour
runway, exact-identity, no-rerun and no-trading requirements remain unchanged.

Unrelated pre-existing worktree changes were observed and are excluded from
this pass.

## Controlling evidence

**OBSERVED**

- Prior evidence root: `N:\Beelzebub\asia-qualification-20260922`.
- `SHA256SUMS.txt` currently hashes to
  `4D3A7EB265CBE6550877F40746BB7D74D4111F314D6D3F7317875F29FB83728D`.
- Prior outcome: `ABORTED_PREFLIGHT`; the fixed window was not captured or
  moved, recovery was not called, and R2 was recorded as `0/1`.
- At `2026-09-22T04:56:17Z`, ports `8090`, `48135` and `48136` had no listener
  and no Python process was found. NinjaTrader PID `17208` remained present.
  This is a read-only process/listener observation, not loaded AddOn or source
  identity proof.

**HISTORICAL, NOT CURRENT AUTHORITY**

- The last authenticated snapshot at `2026-09-22T04:04:32Z` reported Sim101 /
  `LOCAL_SIMULATION`, flat quantity zero, zero working owned and entry orders,
  complete snapshots, current reconciliation, `DISARMED`, activation disabled
  and live capital denied.
- Supported shutdown was recorded at `04:06Z`. Current account, position,
  orders, reconciliation, loaded observer and ledger-tip state remain UNKNOWN
  until freshly authenticated.

## Immediate disposition

**`HOLD_POINT6_PREREQUISITES_1_5_UNVERIFIED`**

- **Action held:** all point 6 runtime/reload/R2/recovery/window activity.
- **Evidence:** none of points 1–5 has yet produced and passed its required
  current evidence in this pass; the native listeners are presently absent.
- **Consequence:** no fresh authenticated preflight, R2, Full recovery chain,
  or diagnostic window may begin.
- **Release condition:** Caelum verifies points 1–5 individually against the
  acceptance conditions below; Sol then captures a completely clean fresh
  authenticated preflight. Joseph alone may authorize the one R2 attempt.
- **Owner:** Sol controls the transition. Assigned specialists produce bounded
  read-only/offline evidence. Caelum reviews; Joseph retains R2 authority.
- **Independent work continuing:** points 1–5 investigation, focused offline
  tests, evidence hashing and preparation of a future predeclaration.

## Point-by-point requirements

1. **Runtime/evidence Git identity:** establish branch, HEAD, tree, status,
   dirty paths and repair ancestry; resolve launcher/CWD/command/import paths
   and hashes; classify the mismatch; freeze one attributable commit + tree +
   executable/measurement hashes + profile/topology + ledger identity/epoch
   tuple. No unexplained drift is acceptable.
2. **Authenticated native source/bridge:** inspect NinjaTrader/AddOn lifecycle,
   ports, logs, handshake, inherited environment, source session, build,
   fingerprint/generation and MNQ DEC26 identity. A process alone proves none
   of these. Use only a supported non-R2 baseline path; otherwise HOLD for
   Joseph's decision. Acceptance requires provenance-valid identity stable
   across two read-only captures while disarmed.
3. **Ledger tail:** supported read-only verification must attribute the 12,901
   rows, time span, record/reason mix, continuity, hashes and durable state, or
   identify the first failing sequence/reason. `Unverified` is not `corrupt`.
   No recovery may precede Full verification through a stable tip.
4. **Incident flood:** quantify first/last time, count/rate, reasons, producer,
   cause and ledger rows/bytes; preserve all incidents. Acceptance requires the
   causal source stopped and a bounded quiet observation without hidden retry,
   dropped records or suppressed safety incidents.
5. **Capacity/runway:** only after point 4 is quiet, measure N: free space,
   ledger/WAL/SHM, writer/checkpoint state, post-repair growth and projected
   runway. Existing floors and 24-hour policy may not change. Acceptance
   requires current `healthy` under the existing contract and a causal account
   of the prior under-24-hour warning.
6. **Earliest eligible diagnostic:** remains held until points 1–5 pass. Sol
   alone may run the exact disarmed start, authenticated sandwiches, Joseph-
   authorized R2, two Full verifications, supported recovery, immutable
   predeclaration/window and supported shutdown. Ratio remains diagnostic with
   `threshold=null` / `POLICY_NOT_DEFINED`. If Asia is no longer eligible, do
   not substitute London; preserve R2 for a separately declared continuation.

## Evidence return contract

Each owner must return commands actually run, result, path/hash, environment
and time; separate `OBSERVED`, `INFERRED` and `UNKNOWN`; identify source,
installed, loaded, exercised and accepted states independently; and state the
smallest release action. Adam is not invoked unless a formal admission decision
becomes necessary after candidate evidence exists.

## Status

Pass active. Final disposition will be appended after points 1–5 and any
authorized point 6 actions are complete or decisively held.

## Final review — 2026-09-22T05:15:45Z

### Final label

**`HOLD_R2_REQUIRED_FOR_CURRENT_NATIVE_IDENTITY`**

This pass resolved the earlier Git mismatch, proved the ledger chain through a
stable tip, and integrated an offline-tested fail-closed incident repair. It did
not establish the current authenticated native identity, exercise that repair
in a bounded runtime, restore current writer-capacity health, authorize R2, call
recovery, or admit point 6.

Sol's decisive packet is
`N:\Beelzebub\aggressive-recovery-20260922\RECOVERY_PASS.md`, SHA-256
`2A04B9CD9FB8436643FCFD8BF918C4269481B9BAEE3895E2E4A2D9FF7CA02BC7`.
Caelum independently re-hashed it and confirms its operational disposition.

### 1. Runtime/evidence Git identity — cause resolved; fresh tuple still required

**OBSERVED:** the aborted pass mismatch was a documentation-commit race, not
executable drift. Step 0 captured `f60314b` / tree `51821a3` at `04:03:35Z`;
Caelum's prior HOLD-only commit `ac451a1` / tree `7579729` followed at
`04:03:58Z`; runtime correctly reported `ac451a1` at `04:04:26Z–04:04:32Z`.
The intervening change was only the dated HOLD record. Nineteen of nineteen
runtime-import hashes matched, and executable/measurement blobs were unchanged
from accepted repair `7ac86fa`, which remains an ancestor.

The final source state is commit
`c9c2993305d8f7e97543b52e148b354843476850`, tree
`0168b1182cb7b23d9ef10e4a6fbefb358c6621f6`. Any future start must freeze this
or a later expressly approved post-integration tuple and prohibit main-worktree
commits until supported shutdown. Step 0 HEAD/tree, runtime Git SHA, loaded
module hashes and the predeclaration must all name that same tuple. The final
commit has not been loaded or exercised in runtime during this pass.

### 2. Native source/bridge identity — HOLD requiring Joseph's R2 decision

At `05:03:02Z`, ports `8090`, `48135` and `48136` had no listener. NinjaTrader
PID `17208` attempted connections to `48135` and `48136` in `SYN_SENT`; a
process and callbacks do not establish an authenticated source.

The installed AddOn source
`C:\Users\atlas\Documents\NinjaTrader 8\bin\Custom\AddOns\BeelzebubReadOnlyAddOn.cs`
hashes to
`ED4486F49CE3F8050D169418D4C74DCC458D875310A71E14662B73D85DBFA229`
and embeds obsolete source fingerprint
`051808624d0d91ac04e91cae27b7e8765d887774053192b909c7b7434f652a14`.
The installed `NinjaTrader.Custom.dll` hashes to
`E7481199AF266B16BFD15559CFBAD6E216F9C8C2B5E72021049C9C487EDEE5A3`.
The repository AddOn hashes to
`FFBB655D60A392E20DBEEA9DD8F8DB1ECBD4E68A1CD56B9C4F5A258F11E619E5`
and requires fingerprint
`17995c2795e16bf52bedd9644a865261e3a20473f1e2bb7b75b4947bc558a525`.

The last authenticated producer was session
`2b2bcac2fd8043a2a295a70989b19fc2`, build
`390e3497e1269195977f781d35418af5fe2b48a0702670d4684f4a43863b422a`,
source `051808...`, generation 1, MNQ DEC26. It is historical and does not meet
the current required fingerprint. A supported menu recheck can only reattach
the obsolete assembly; the only supported correction is the single
Joseph-authorized R2 install/reload. No such authorization or action occurred.
Release requires one R2 producing a distinct current producer with matching
build/source fingerprints, fresh signed MNQZ6 observations, and stable matching
identity across two read-only captures with zero new overflow or transport
failure. Refusal or ambiguity does not authorize a retry.

This exposes a sequencing conflict in the pass text: point 2 cannot reach its
stated acceptance without R2, while point 6 says R2 occurs only after points
1–5 pass. Caelum does not silently reinterpret that order. Joseph must
explicitly authorize the minimal exception: complete all non-R2 prerequisites,
capture the clean disarmed/account preconditions available before reload, then
spend the one R2 solely to satisfy point 2 before continuing the remaining
point 6 chain.

### 3. Ledger tail — PASS through stable tip

The historical `12,901`-row tail was an early snapshot, not corruption. A
read-only exact invariant scan later attributed `56,260` rows, sequences
`277857–334116`, spanning `04:04:31.248119Z–04:06:32.611970Z`. Sequence,
prior-hash, payload/column, canonical-hash and domain joins all passed with no
first failing row. Of these, `56,250` were matched
`INCIDENT_OBSERVATION_REJECTION` rows with
`AUTH_FAILED/ninjatrader_source_fingerprint/OFF_SESSION`; 10 were lifecycle or
reconciliation rows. Ordered tail digest:
`5e23d4085cde124e160cc6e35a7e27be8e25e3156f43df21e2eb6c77bd2bda2e`.

Sol then ran supported Full verification. It passed at `05:11:32Z` through all
`334116` rows at tip
`41d491299060463efb14c99c05c8adbbaecad507b1782f0b7eb834c0ab5402c9`,
with `quick_check=ok`, WAL zero and no storage warning. Artifact:

`N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\ledger-verification\20260922T051132251182Z-lv-aggressive-recovery-20260922T0510Z.json`

SHA-256:
`96A7D0B858A1032D9E8C4C71229E72B6FA956C6DEFD2974BE8792CE17586CF4F`.
Checkpoint SHA-256:
`E6D297A6DC8AE70B1534CF43632B3D64228809EC97065D5A07B36A98ADC37F39`.
This clears the tail-integrity HOLD only; it is not latch recovery or runtime
qualification.

### 4. Incident-rejection flood — repaired in source, not runtime-accepted

**Root cause:** strict attestation correctly rejected every mismatched source
frame, but the qualifying harness continued after invoking the rejection
callback, durably appending one incident per frame. The preserved failure was
`56,250` incidents in `121.247` seconds (about `463.93/s`), with zero accepted
or durable observations, no producer and zero overflow. Collector receipt:

`N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\pong-authentic-hour\collection-status.json`

SHA-256:
`9043D6CD8180466FDCB3F0757B989DFE85620D649DD06B331647C5C3F175AC23`.

**Implemented remedy:** commit `c9c2993` records the first strict rejection,
then raises `NINJATRADER_REQUIRED_OBSERVATION_REJECTED:<code>:<detail>` and
closes the required listener. Source SHA-256:
`89E364D61463D22A1B989DFD116084FB57F13C8DB1DFE10799FF49D4E1BC1E55`;
test SHA-256:
`BB878C1F3FD7F34977B2180665674F1F3035655CFCCC4F0B347401E3A821EED0`.

With `TEMP=TMP=N:\Beelzebub\scratch\aggressive-recovery-20260922`:

- Focused pytest set: 40 passed plus 8 subtests in 4.50 seconds. Immutable
  JUnit receipt `N:\Beelzebub\aggressive-recovery-20260922\focused-tests.xml`,
  SHA-256
  `47067B233CFDFABC44EF3D2DA01AF4D74CB17D8F2B511A1D190CD2F0C4BE7FD9`.
- Exact regression unittest: 1 passed in 0.209 seconds; terminal receipt only.
- Wider pytest set: 76 passed, 10 subtests and 4 failed in 16.27 seconds;
  terminal receipt only. One failure reflected the native telemetry allowlist
  source mismatch and three were `PROFILE_SELECTION_CHECKOUT_MISMATCH` against
  the remembered N: runtime. This was not a green full-suite result and is
  consistent with the retained identity HOLD.

The regression proves two invalid frames yield one rejection callback/record,
receiver `FAILED`, `LOCAL_BRIDGE` disconnected and port closed. The 65-minute
quiet observation occurred only while the source/runtime was stopped; it does
not exercise the repair. Therefore
**`HOLD_INCIDENT_REJECTION_REPAIR_NOT_RUNTIME_VERIFIED`** remains until the
correct AddOn is loaded and a bounded observation proves no continuing storm,
hidden retries, dropped records or suppressed safety incidents.

### 5. Ledger capacity/runway — actual destination verified; health pending

The capacity measurement used the operational ledger's actual N: parent, not
C:. At the Full scan N: had `225703780352` free bytes; at `05:16:08Z`, N: had
`225703772160` free bytes versus C: `4504956928`. The main database was
`1769041920` bytes. At the incident rate, measured growth was
`1951042.008 B/s` and cap-limited runway only `5.863` hours, so the earlier
`DEGRADED` / under-24-hour warning was correct. No migration is needed or
authorized. Floors remain 8 GiB warning / 4 GiB minimum.

The Full verifier later saw stable database size, WAL zero and no static
storage warnings, but there is no bounded post-repair runtime/writer receipt
showing `HEALTHY`, warning-free, queue zero and growth zero or projected runway
above 24 hours. **`HOLD_LEDGER_CAPACITY_POST_REPAIR_TELEMETRY_MISSING`**
therefore remains. No latch recovery was called.

### 6. Diagnostic window — not admitted

R2 remains `0/1`. No install/reload, post-repair runtime, recovery, new fixed
window, Gates B–D, paper start, arming, native directive, order, execution or
fill occurred. At `05:15:45Z`, independently, and again at `05:16:08Z` in Sol's
packet, the three relevant ports had no listener; no Python process was present,
and NinjaTrader PID `17208` remained untouched.
Current authenticated account, position, order and reconciliation state is
UNKNOWN. The clean Sim101/flat/zero-order/reconciled/disarmed observation at
`04:04:32Z` remains historical only.

### Smallest next action

Joseph decides whether to authorize the single R2 under the explicit sequencing
exception above. Before spending it, Sol must freeze the final identity tuple,
start only the exact disarmed control plane, and capture every currently
available clean account/position/order/reconciliation and ledger precondition.
If authorized, R2 is used once to install/reload the current source; then Sol
must prove point 2, exercise point 4, obtain point 5 `HEALTHY` telemetry, and
only afterward reconsider supported recovery and a separately predeclared Asia
window. No London substitution or retroactive window repair is admitted.
