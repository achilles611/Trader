# Caelum superior review — London 30-minute natural-feed qualification

- Pass: `20260922-072849Z-london-30m-qualification`
- Review opened: `2026-09-22T07:28:49Z`
- Parent integrator and sole runtime/ledger/install/reload/recovery/window operator: Sol (`/root`)
- Superior administrative reviewer: Caelum (`beelzebub_admin`)
- Subordinate admissions reviewer: Adam (`adam_admissions`)
- Writable scope: this dated Caelum directory only
- Candidate window: `2026-09-22T08:00:00Z–08:30:00Z`
  (`09:00–09:30 Europe/London`, within the configured Tuesday London session)

## Objective and authority boundary

Admit only one genuinely qualifying, continuous 30-minute natural-feed London
collection. This is not permission to relabel the earlier five-minute London
diagnostic, a replay, a partial interval, a restarted interval, or a completed
period from another session. Caelum performs administrative evidence review
only. Caelum does not edit source/runtime/ledger/runbook files; start or stop a
service; install/reload NinjaTrader; call recovery; operate R2; arm; or place an
order.

No Gates B–D, operational paper start, arming, native directives, orders,
executions, fills, live-capital authority, feed reduction, threshold weakening,
manual ledger/latch edit, database migration, or force-kill is authorized.

## Current evidence reviewed

### OBSERVED

- Current implementation commit before the administrative reports:
  `2331c744ccaa09d6644496142daa4682a0476f72`, tree
  `8b14f35e8c6bb4c3395dadf756a226803b37e6be`. The change adds a two-phase, source-preparation-only native
  commissioning path without claiming loaded identity. Sol reports 62 focused
  tests passed. Runtime exercise remains pending.
- At `2026-09-22T07:30:05Z`, ports `8090`, `48135` and `48136` had no listener,
  no Python process was present, and operator-owned NinjaTrader PID `17208`
  remained running.
- At `07:30:05Z`, before source preparation, installed observer source SHA-256 remained
  `ED4486F49CE3F8050D169418D4C74DCC458D875310A71E14662B73D85DBFA229`
  with obsolete embedded fingerprint `051808624d...`; installed compiled DLL
  SHA-256 remained
  `E7481199AF266B16BFD15559CFBAD6E216F9C8C2B5E72021049C9C487EDEE5A3`.
  Repository source SHA-256 is
  `FFBB655D60A392E20DBEEA9DD8F8DB1ECBD4E68A1CD56B9C4F5A258F11E619E5`
  and requires fingerprint
  `17995c2795e16bf52bedd9644a865261e3a20473f1e2bb7b75b4947bc558a525`.
- This historical installed state was superseded by the source-preparation
  receipt
  `N:\Beelzebub\london-qualification-20260922T080000Z\01-R2-source-preparation.json`,
  SHA-256
  `E34912EB88DB3BC68AE4305180643B81417CF0A00FF6811B966BA74F5D47D33F`.
  It proves repository validation success, installed-source parity, expected
  source `17995c...`, and a rebuilt on-disk DLL SHA-256
  `B70556DDCCE897C018255B7CFD49E2D5EC03B96CDF6A6EC6A0A06328A2CB8756`.
  It deliberately reports `verified=false`, `loaded_identity_claimed=false`
  and `distinct_producer_verified=false`; loaded/authenticated identity remains
  UNKNOWN pending the distinct post-install producer verification.
- The prior London packet at
  `N:\Beelzebub\london-diagnostic-20260922` closed at `06:50:41Z` as
  `ABORTED_R2_PREACTION_RECEIVER_STATUS_TIMEOUT`. It records `r2_used=0`, one
  attempt remaining, no evidence file, no installation change, no paper start,
  no arming and no orders. All five entries in its manifest independently
  re-hashed with zero mismatches.
- The `06:46:43Z` sandwich narrowly observed Sim101 /
  `LOCAL_SIMULATION`, MNQ DEC26/MNQZ6, flat runtime and broker quantities,
  zero working owned/entry orders, complete snapshots, current reconciliation,
  disarmed state, activation false and live capital denied. In the same receipt,
  native source identity was absent, native input/runtime bridge were blocked,
  canonical depth and all current market evidence were unavailable, warm start
  was false, and the ledger capacity latch was `DEGRADED`.
- The last Full ledger verification passed only through sequence `334116` /
  hash `41d491299060463efb14c99c05c8adbbaecad507b1782f0b7eb834c0ab5402c9`.
  The later sandwich observed tip `334136` /
  `6ccfe617e75a8cff9c789b53fc4e121f8c04c5e59cd6745dc362a9f5ac952277`.
  Full was therefore not current to the observed tip.

### UNKNOWN / not yet accepted

- Current loaded observer source/build/session/generation and distinct producer.
  Installed and compiled preparation is not loaded proof.
- Current account, flatness, orders, reconciliation and disarmed state after a
  future restart/reload; the `06:46Z` receipt is historical.
- Current Full verification through a stable tip, supported capacity recovery,
  post-recovery Full, writer `HEALTHY` state, and 24-hour runway.
- Current natural-feed source continuity, canonical depth, freshness, warm-up,
  host/collector/native queue behavior, periodic execution and durability.
- Official venue-calendar eligibility for the exact interval.

## Pre-window decision

**`HOLD_LONDON_30M_START_PENDING_CURRENT_NATIVE_FULL_RECOVERY_PREFLIGHT`**

- **Action held:** starting or claiming the `08:00Z–08:30Z` qualifying window.
- **Evidence:** installed native identity is obsolete; no current authenticated
  source exists; Full is behind the later observed tip; capacity recovery and
  post-recovery proof are absent; current safety state is historical.
- **Consequence:** the clock interval is not admitted merely because it is
  scheduled. If every release condition is not complete before `08:00:00Z`,
  this interval is missed and must be recorded as such.
- **Release condition:** Sol supplies and freezes all preconditions below before
  the boundary. Caelum review is administrative; Joseph retains the single-R2
  decision. A refusal or ambiguity consumes no second attempt.
- **Independent work allowed:** supported two-phase observer preparation/R2,
  exact read-only sandwiches, supported Full/recovery/Full, venue verification,
  predeclaration and evidence hashing by Sol. No order-producing work.

## Governing admission contract

### A. Immutable interval and London identity

1. Predeclare exactly `08:00:00Z–08:30:00Z` before it begins, naming
   `session_kind=LONDON`, `session_family=EUROPE`, timezone `Europe/London`,
   Tuesday eligibility, MNQ DEC26/MNQZ6, every frozen identity below, the
   evidence root, formulas, disqualifiers and no-rerun rule.
2. The interval is exactly 1,800 continuous wall-clock seconds. Do not slide,
   trim, restart, backfill, splice, extend, substitute a later period, or count
   warm-up/pre-start records. Any process, source session, generation, counter
   epoch, listener or collector restart inside the interval disqualifies it.
3. Verify the official venue schedule for the entire interval. A calendar or
   session mismatch is a pre-start abort, not permission to relabel the window.

### B. Frozen provenance and safety preflight

4. After every source and administrative commit, freeze one exact tuple:
   commit + tree + executable and measurement hashes + loaded artifact/module
   hashes + profile + `FULL_NATIVE_DEPTH_PERIODIC_20MS_NO_FHB_V1` architecture
   + topology/hash + Sim101 binding + ledger identity/epoch + native
   source/build/session/generation + periodic counter epoch/process. Step 0,
   runtime binding, native receipts, collector and predeclaration must agree.
5. The single supported R2, if Joseph authorizes it, must load the required
   `17995c...` source as a distinct producer and prove the matching current
   compiled build across two stable read-only captures. No partial installed or
   compiled state may be called loaded.
6. Immediately before the interval, an authenticated sandwich must prove exact
   Sim101 / `LOCAL_SIMULATION`; MNQ DEC26/MNQZ6; flat runtime and broker
   quantities; zero owned, entry, working or foreign orders; complete/current
   position and order snapshots; current reconciliation; no pending directive,
   command, order or execution; activation false; `DISARMED`; live capital
   denied; and no protection or risk-state ambiguity.
7. Require supported Full verification through a stable current tip. If the
   capacity latch remains set, recover it only through the supported endpoint
   after the clean sandwich and Full, require `recovered=true`, paper not
   started, armed false and a new Full required; then complete the second Full
   and repeat the full sandwich before `08:00Z`. Keep the 8 GiB warning, 4 GiB
   minimum and 24-hour runway rules unchanged. The actual ledger destination
   must be the measured capacity destination.

### C. Natural-feed continuity and exact retention

8. Use only newly arriving signed native market records during the fixed
   interval. Replay, retained history, synthetic timestamps, a historical
   certificate or warm-up records cannot satisfy the 30-minute collection.
9. Preserve one stable authenticated producer/source/build/session/generation,
   receiver, collector, host process, ledger identity/epoch and periodic counter
   epoch for the whole interval. All signed received frames must reconcile in
   exact order by count and digest through receiver admission, required host
   consumers, collector journal and durable closeout.
10. Require zero new producer queue overflows, producer transport failures,
    receiver/collector/host overflows, signed-record rejections, duplicates,
    DEPTH/QUOTE/TRADE sequence gaps or out-of-order events, durability lag or
    counter regression. Native sender inflight records and all host/collector
    queues must drain to zero at close.

### D. Freshness, depth, warm-up and periodic behavior

11. Persist start, end and periodic intermediate telemetry. Event/source age,
    host oldest-queue age, collector oldest-queue age and native sender inflight
    age must remain below the existing 1,000 ms ceiling throughout; the native
    heartbeat must remain continuously valid. A breach cannot be averaged away.
12. Every available DEPTH snapshot must be coherent exact 10 bids + 10 asks.
    Explicit native unavailable state is preserved honestly and never
    synthesized, but the interval must contain coherent available depth and
    cannot carry a canonical-depth loss/input latch or remain wholly
    unavailable.
13. Require nonzero authentic DEPTH, QUOTE and TRADE coverage; current London
    evidence families/warm-up must become genuinely ready from the current
    source. Do not borrow another session's warm-up. Required trade
    classification and causal source provenance must be valid.
14. The active analytical path must be ordinary periodic 20 ms, not
    event-driven protection. Require nonzero ordinary completed full analyses,
    zero protection transition/supersession, scheduler deadline and completion
    reconciliation, zero outstanding opportunities at close, and persisted
    scheduled/claimed/dispatched/completed/applied plus
    late/obsolete/intentional-no-work counters. Do not invent a zero-lateness
    gate; all outcomes must instead be complete, attributable and consistent
    with the unchanged freshness/continuity gates.
15. Persist `R = delta DEPTH / delta ordinary completed full analyses` from
    aligned start/end counters. Because numeric ratio policy remains undefined,
    record `threshold=null` / `POLICY_NOT_DEFINED`. The ratio is diagnostic and
    neither creates nor defeats this structural 30-minute qualification.
16. FHB remains absent: no profile/policy, no consumer/helper, no fallback and
    no claimed upstream feed reduction. Preserve the single ordered owner and
    all event-driven protective callbacks.

### E. Closeout and classification

17. At `08:30:00Z`, capture the exact end state without extending collection.
    Use supported stop; drain native/host/collector queues; close/checkpoint
    cleanly; run supported Full verification through the final stable tip; and
    hash the complete evidence packet. Preserve operator-owned NinjaTrader.
18. Confirm zero paper start, arming, native directives, orders, executions or
    fills. Report the final current account/position/order/reconciliation state.
19. Only if every criterion above passes may this record classify
    `QUALIFIED_LONDON_30M_NATURAL_FEED_COLLECTION`. That label is limited to
    this exact 30-minute collection. It is not the existing complete-hour Pong
    certificate, periodic-ratio policy acceptance, Gates B–D, paper readiness,
    arming or trading authority.
20. Any violation yields `HOLD_LONDON_30M_<EXACT_REASON>`. Preserve the partial
    evidence and do not repair the interval by rerun or relabel.

## Adam hierarchy

Adam's subordinate report is
`docs/administration/adam/passes/20260922-072920Z-london-30m-qualification/ADMISSIONS_REPORT.md`,
commit `8c3407b0d7bc6fe48a3e6006d38fbcd154ae9aac`, tree
`b2a5dde8083a1491d0364d56959ccba24b71a5eb`, report SHA-256
`ACB3127BB55BF26DCB08649F8E426C355EF5D05B7BDB5EAB878E77725376E492`.
Caelum independently re-hashed and reviewed it.

**Superior disposition: CONFIRM WITH MODIFICATIONS.** Caelum confirms Adam's
sole interval, no-repair rule, identity/provenance, current Full/recovery/safety
sandwich, N:-destination capacity, continuity/freshness/depth/warm-up, durable
closeout, category separation and all no-trading boundaries. Caelum adds the
explicit ordinary-periodic-path requirements in criterion 14 above: a
collection cannot receive the full qualification label while remaining on
event-driven protection, producing zero ordinary full analyses, undergoing a
protection transition/supersession, or failing scheduler/completion
reconciliation. Caelum also limits any pass label to this exact 30-minute
collection; it does not mint the runtime's separate complete-hour certificate.
These additions narrow the claim and do not weaken Adam's criteria. Adam's
report is otherwise confirmed and frozen; neither administrator may commit or
edit during the qualifying interval.

## Smallest next action

Sol must complete the current native identity/R2, fresh Full/recovery/Full and
authenticated safety preflight early enough to review before `08:00Z`. If any
item remains pending at the boundary, record a missed interval and stop; do not
start a merely diagnostic 30-minute capture under a qualification label.
