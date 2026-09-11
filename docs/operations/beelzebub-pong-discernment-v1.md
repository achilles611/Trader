# Beelzebub Pong Discernment v1 — isolated candidate

Status: **OFFLINE CANDIDATE VERIFIED — EXECUTION DISABLED — NOT REGISTERED — NOT DEPLOYED**

## Authority and provenance

This candidate implements the development scope in `pong1.md` under the
accompanying Joseph handoff's orchestration authority.  The narrower instrument
interpretation is binding: two means **two MNQ micro contracts total**, never
two NQ E-mini contracts and never two contracts added to an existing position.

Recovered authority identities:

| Authority | SHA-256 |
|---|---|
| `C:\Users\atlas\Desktop\bis\pong1.md` | `6F2A22376967270047226728CA29823229B043126D712DAC0C495544A385FAED` |
| `C:\Users\atlas\.codex\attachments\1eae3b4c-e2a9-47b1-bd63-690b9ce3c5e1\pasted-text.txt` | `3CA8F7964EE5F8E885B9ADA1B16122E25FC225C82A61DDA506CCE4B736624C68` |
| `C:\Users\atlas\Desktop\bis\Pong_Interrupted_Pass_Resume_Handoff.md` | `277E93CEB735E35991FD0660D89B90BCE3D079F71BA0DDC996123CD78D309C96` |

The interruption-recovery capture is outside watched/deployed runtime at
`D:\CodexRecovery\Pong-20260911T080000Z`; it contains the full unstaged patch,
the empty staged patch, copies of all three untracked task files, worktree/status
records, and a timestamped identity manifest.  It contains no runtime volume or
credential material.

The isolated branch `codex/pong-discernment-v1` was created from the verified
local Pong foundation
`ffd9601d2bd7eed1fdcc6faf1c199111089d15fd`, whose direct parent is the reported
design checkpoint `7d63f68a9e5fe994a78eb249c366fa606aaa141f`.
The final source-and-test review snapshot is Git tree
`4a66e7fa8a36708d39a55328ab6f48b49827c832`.
The pre-existing `Trader-pong-bias` and deployed/runtime worktrees were not
modified.

The candidate identity remains exactly:

- mode: `PAPER_SIM101`
- account: `Sim101`
- account class: `LOCAL_SIMULATION`
- native instrument: `MNQ SEP26`
- canonical contract: `MNQU6`
- candidate maximum absolute quantity: 2
- execution: disabled
- registration: `NOT_REGISTERED`
- live capital: `DENIED`

These are candidate constants and recovered historical bindings.  Current
runtime/account state is `UNKNOWN` because this implementation-only pass did
not query trading-control endpoints, account state, or a live bridge.

## Architecture decision

Pong no longer consumes Perpetual's 30-second decision stream.  A neutral pure
math kernel preserves the existing confidence calculation while Lane II keeps
its compatibility wrapper and serialized contract.  Pong supplies its own
versioned evidence semantics to a causal Discernment engine:

```text
logit(prior LONG support) + deduplicated active component effects
    -> clipped sigmoid raw LONG support
    -> reliability shrinkage about 0.5
    -> effective LONG/SHORT Discernment
```

Evidence is replaced by stable component/version identity and grouped by
dependency, so repeated rolling-window callbacks and correlated derivatives do
not accumulate as independent votes.  Required missing evidence is
`UNAVAILABLE`, not 50 percent.  Snapshots preserve event, receipt,
available-at, expiry, revision, continuity, source, policy/model, and result
hash provenance.  Every component and downstream snapshot hash also binds the
raw source-content hashes and causal envelope; changing source content changes
the snapshot and any derived switch identity.

A frozen v1 research mapper converts the symmetric aggressive-flow result,
two-sided liquidity result, and prior-only volume-regime context into versioned
Discernment evidence.  The mapper has explicit dependency groups and bounds,
retains raw/result hashes, maps blocked input to no neutral-looking evidence,
and remains execution denied.  Synthetic end-to-end tests cover raw events to
mapped evidence to a real Discernment seed and opposite switch.

The research switch policy latches LONG at 65 percent effective LONG support
and SHORT at 35 percent.  The 65/35 policy is a research fixture, not an
approved trading threshold.  First establishment seeds the latch without a
switch.  Only a valid opposite threshold transition emits one switch.  Clock,
expiry, daily-bias, fill, reporting, and same-side score callbacks cannot emit
a switch or manufacture a Pong beat.

Pong retains its enter/exit/wait philosophy.  A valid aligned switch may create
an abstract entry candidate.  The next opposite switch requests an exit, never
a reversal.  Confirmed signed flatness and zero owned orders begin the one-leg
wait; the next genuine switch completes that leg and is eligible without an
extra arbitrary skip.  Daily bias remains an independent hard entry-side gate
and is not added to the score.

Each switch is self-verifying.  Its deterministic identity binds the approved
policy and model hashes, 65/35 and strong thresholds, prior and next latch,
sequence/prior-switch chain, snapshot hash and revision, causal timestamps,
continuity domain, instrument, and contract.  Pong verifies both that identity
and its expected stream position before changing state.

Snapshot admission also verifies the policy's required component/version set,
component dependency bindings, raw source IDs and content hashes, component
evidence hashes, causal availability/revision/domain, and each component's
`valid_until` against the actual admission time.  A caller-supplied freshness
boolean can deny an entry but cannot make expired or incomplete evidence valid.
Opposite-threshold rechecks use the injected policy thresholds rather than
hard-coded 65/35 values.  The concrete microstructure mapping hash is embedded
in its generated Discernment model/policy identity, so a mapping-effect change
changes every downstream policy-bound switch identity.

## Percentage semantics and reachability

Displayed percentages are labelled `EXPERIMENTAL_DIRECTIONAL_SUPPORT`.  They
are not calibrated probabilities of a profitable trade.  Operational entry
eligibility remains disabled until the evidence mapping, reliability contract,
daily-bias inputs, and native/risk path receive separate review and
commissioning.

The inherited Lane II experimental model-confidence ceiling is 0.70.  Because
the existing sigmoid clips finite log odds at 30, its maximum raw value is
strictly below 1.0; shrinkage therefore produces an effective LONG value
strictly below 0.85 (and a mirrored SHORT value strictly below 0.85).  The
two-contract threshold is consequently unreachable under that inherited
configuration.  This revision neither rounds upward nor weakens the ceiling.
A separately justified Pong reliability contract is required to make the rule
operationally reachable.

## Conditional quantity and risk

At an otherwise eligible fresh entry, full-precision effective position-side
support below 0.85 requests one MNQ and support at or above 0.85 requests two
MNQ total.  Invalid or ineligible evidence requests zero.  This is entry-time
sizing only: it never pyramids, adds after entry, or reduces a held position
merely because support changes.

The isolated Pong quantity contract then reduces score permission by exact
identity, signed-flat/order truth, available risk, pending reservation, and
native capability.  The latest recovered historical Lane III binding permits
$50 per trade with a 25-point MNQ stop.  Nominal stop-distance risk is $50 for
one MNQ and $100 for two before costs or adverse execution.  The same recovered
historical paper AddOn evidence is sealed to quantity one.  Those conservative
values are the disabled candidate defaults; they are not a claim about current
runtime/account configuration.  Therefore the default candidate reports:

- `TWO_MNQ_EXCEEDS_POSITION_RISK_LIMIT`
- `PONG_NATIVE_QUANTITY_CAPABILITY_UNPROVEN`

and reduces an otherwise valid two-MNQ score request to at most one.  Tests may
inject an explicit two-contract fake capability to exercise the complete pure
contract; those fixtures create no policy, transport, native, or operational
authority.

Entry admission is computed inside the Pong machine from the pinned quantity
policy.  A hash-bound admission identity covers the candidate action and
switch, current Discernment snapshot, daily-bias result, admission time,
available/reserved risk calculation, and reconciliation proof.  Admission
emits a new immutable action ID from the final payload; it cannot mutate the
pre-admission candidate while retaining the old identity.  Reconciliation is
bound to `Sim101 / LOCAL_SIMULATION / MNQ SEP26 / MNQU6`, the sealed AddOn
source/build fingerprints, source-content hash, causal time, position, owned
orders, and unresolved-execution state, with a five-second freshness ceiling.
This is a pure candidate proof shape exercised with deterministic fixtures;
there is no commissioned native producer, signature verifier, bridge, or
runtime adapter for Pong in this revision.

The pure exposure tracker bounds requested entry exposure to two, deduplicates
fill identities across every lifecycle and restart, requires protection to
equal actual exposure after each partial fill, and treats fills racing any
cancellation as real.  A broker overfill is never hidden by flooring at zero:
signed position accounting records any resulting opposite exposure, latches
safety, and requests exact containment.  No active runtime or NinjaScript
source was changed.

Every working normal exit, safety exit, and protective stop is tracked by
immutable action ID, original reduced side, and remaining quantity.  A normal
exit cannot overlap a still-live protective stop: Pong first emits a targeted
protective cancellation and releases the deferred exit only after cancellation
confirmation.  A fill from a retired/cancelled reducing identity remains an
exact-once input and any race-created exposure is visible.  Partial strong-tier
entries whose evidence loses two-contract authority request cancellation of the
unfilled remainder; any racing fill is still accounted for and protected.
Safety escalation retains all still-live reducing coverage and requests only
uncovered exposure.

## Restart continuity

The pure state machine exposes an immutable, deterministic checkpoint carrying
the sealed `PAPER_SIM101` / `DISARMED` authority state, last switch chain, last
consumed switch identity, snapshot proof, daily-bias proof, quantity state,
all-lifecycle fill identities, pending/protective/reducing/deferred action
identities, cancellation state, and any safety lockout.  A strict canonical
JSON codec refuses truncated, missing, unknown, noncanonical, or hash-mismatched
envelopes.  It does not write the checkpoint or grant execution authority.
Restore verifies the checkpoint hash, exact Discernment and daily-bias proofs,
and a fresh bound reconciliation fixture.  Only verified flat state with no
pending action may resume.  A corrupt proof, unresolved action, non-flat state,
or reconciliation mismatch latches a separate recovery blocker while
preserving trusted prior lockout reasons and quarantined action/fill identities.
Restore never resubmits them.  Thus neither a consumed switch nor a historical
fill identity can become fresh merely because the process restarted.

## Read-only status projection

The candidate status includes sealed mode/account/account-class/instrument/live
authority, effective LONG/SHORT Discernment, latched logical direction,
validity/calibration state, daily bias, volume regime, score-authorized
quantity, risk-admitted quantity, signed actual quantity, waiting state, last
genuine switch, and exact blockers.  A displayed score is not an armed
indicator.  The projection also exposes the current logical checkpoint hash for
evidence binding.

## Historical audit and replay coverage

### Completed Perpetual baseline

The audited completed run is:

```text
root: N:\Beelzebub\runtime\profiles\beelzebub_five_minute_perpetual_v2\runs\20260910T232006Z-e2f1a956bdfd
profile: BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2
ledger: l3g-ledger-3bfe6ea7ecbe4dd3b5f26d9252d9d8f5
epoch: L3G-PAPER-EPOCH-BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2-20260910T232006Z-c288a48a155c
execution session: l3g-es-a91277f884de4c6183fbf490f01597b8
profile-switch: profile-switch-700c63cacf9c4d5aabecff2966bf001b
executed Git: c8b267cb81d7c9596b8363850db38b33eb6e79ae
```

The bound policy was `l3g-beelzebub-five-minute-perpetual-policy-v2`, hash
`daf3cc6daacdb32d5629fd6cbd97ef2246172426a00392f59f1246c567658fe7`.
The bound risk identity was `l3g-beelzebub-five-minute-perpetual-risk-v2`, hash
`2d50a767772a9bef9edb752cea0417e51f1df1b46ef7be63289ed2bac26dc09c`.
Account binding hash
`28ddf4acc88f1a9e35de79b8306a252e647a5a1dca0a6e9333ce814828e6841e`
proved `Sim101 / LOCAL_SIMULATION / MNQ SEP26`, maximum quantity one,
paper-only, and live capital denied.  Native provenance bound source fingerprint
`ded3309410f44aeb56018838fe1e59347c552664c4052cdab2bc56e9a71b2ef0`
and build fingerprint
`a2b889ca30929582e15ce46aaaaaf8ac430807c67eb79237dc35dc4fbfc14af6`.

The operational interval was `2026-09-10T23:20:28.092782Z` through
`2026-09-11T00:01:32.527526Z` (2464.435 seconds).  It contained 52 unique raw
30-second directional decisions: 7 genuine logical switches, 38 holds, 3
tie-while-flat blocks, and 4 direct flat-entry decisions.  Three raw decisions
reported one missed boundary each and created no durable direction checkpoint.
The 105 analyzer decision rows include 53 synthesized entry/reentry rows and
must not be misreported as 105 independent directional decisions.

There were 57 entries (44 SHORT, 13 LONG) across 35 unique source checkpoints;
22 were same-checkpoint reentries.  Exits were 50 freshness exits (44
`QUOTE_STALE`, 6 `CLASSIFIED_TRADE_STALE`), 6 actual reversal exits, and one
operator stop.  No protective-stop fill occurred.  The seven logical switches
produced six reversal-classified exits because the `23:41:30` LONG switch
arrived while a freshness exit was already in flight; its checkpoint was used
only after signed-flat reconciliation.

The ledger contains 114 unique native receipts: exactly 57 ENTRY and 57 EXIT,
all quantity one, with no evidenced partial fill, duplicate receipt, rejection,
or NQ instrument use.  Fill-to-fill gross P&L is `-$28.50`, independently equal
to the 57 `EXECUTION_REALIZED_PNL` records.  Recorded simulated fees are zero,
but actual commissions were not retained; net P&L is therefore `UNKNOWN`.
Actual fill prices already contain realized spread/slippage and were not charged
a second time.

Closure is established by signed reconciliation at ledger sequence 4754
(position zero, zero working/entry orders, complete snapshots, protective
`NONE`, foreign activity false), `READY_DISARMED` at 4756, and
`SESSION_OPERATIONAL_PAPER_STOPPED` at 4757 with closure hash
`b87a7dcc0a43b03988545a964c5332a41477fe8c483023c2df68a929ae5303d4`.
Full verifier `lv-6bbf1b4685f64913a617017fe7e320ff` passed through sequence
5258, hash `ddb44c9f5cce680061775dbabbd9977f1550be991bbae38195ac05c7b17e32f7`.

### Executed-source findings

At `c8b267…`, the exact executed blobs were:

| Path | Git blob |
|---|---|
| `src/l3g_paper/policy.py` | `05108eee6431f6d217bfe05298f7020f0bf2ddfc` |
| `src/l3g_paper/contracts.py` | `04d3d143e8f1bf9dc1219404c32c35999d25b429` |
| `src/l3g_paper/runtime.py` | `52795d397d537d1e822389e12f06226bc10498fa` |
| `src/l3g_paper/risk.py` | `1abc7bca9c341e9e91d179c7379a37a4b9104c31` |
| paper AddOn source | `5c26492de939395c7265b66b7454a361aee54c2d` |

Inspection confirms the handoff's audit targets:

- Flow generation was sell-dominant-only.  The 52 raw decisions all carried
  bearish `AGGRESSIVE_SELL_IMBALANCE`; 43 also carried bullish
  `SELLING_WITHOUT_DOWNWARD_PROGRESS`.  There was no mirrored buy-dominant
  generator.
- All 52 raw decisions carried both bullish `RANGE_RECLAIM_UP` and bearish
  `RANGE_EXPANSION_DOWN` structural evidence.
- ASK depth was validated but only BID state was scored.  Any BID removal or
  displayed-size reduction became `BID_LIQUIDITY_PULL` without requiring
  trade-match/cancellation provenance.  `BID_REPLENISHMENT` appeared in 49/52
  decisions and `BID_LIQUIDITY_PULL` in 52/52.
- Entry/retention/family/dominance margins were zero.  Any nonzero sign could
  establish direction; observed switch deltas reached `0.016666…` in the
  baseline and `0.008333…` in the later trial.
- Evidence used latest-per-label replacement with structural/flow/liquidity
  lifetimes of 90/30/20 seconds.  Three baseline SHORT switches lacked the
  bullish flow label while retaining bearish aggressive-sell evidence.
  Because raw market events were not retained, expiry-alone causality remains
  unproven.
- The executed runtime cleared the attempted checkpoint for several transient
  blockers, enabling the measured 22 same-checkpoint reentries.

These findings motivate symmetric causal research adapters; they do not prove
that the replacement mapping has a trading edge.

### Bounded correctness follow-up

Patch `e0fede22e3de3397f28dab976cbd4fc46a77eef9` changed only runtime
checkpoint-consumption/deadline behavior; policy, risk, contracts, transport,
and native-source blobs remained unchanged.  Trial
`perpetual-patch-hour-20260911t010300z` ran from
`2026-09-11T01:05:47.056699Z` through `02:05:47.198446Z`.

It contained 24 raw decisions (6 switches, 18 holds), 7 staged-entry rows, and
7 one-MNQ trades (4 SHORT, 3 LONG).  Six exits were true staged reversals, one
was `QUOTE_STALE`, and same-checkpoint reentries were zero.  Fourteen unique
native executions proved seven entries and seven exits, all quantity one.
Gross fill-to-fill P&L was `+$49.50`; true commissions/net P&L remain unknown.
After the stale exit, the trial correctly stayed flat and did not extend or
force a trade.

Sequences 6468–6470 prove signed-flat reconciliation and `READY_DISARMED`;
sequence 6475 carries closure hash
`c2b45b26e0dee9dd0472b8d470cbe34d085634d86eaa6d3e8b99ff811f8bb52a`;
6476 marks the trial `COMPLETED`; and 6477 carries final flatten/disarm hash
`cce72b67e43543a90d6f4d5fa24ab275f4f50c78b016c83e57ea56c5faf1d687`.
Full verifier `lv-a0b8a5b6872e4685b6767d8ca49e56d4` passed through 6477.
This is correctness evidence, not profitability evidence.

The canonical comparison report is
`N:\Beelzebub\runtime\profiles\beelzebub_five_minute_perpetual_v2\runs\20260910T232006Z-e2f1a956bdfd\analysis\perpetual-v2-patch-trial-comparison-20260911\report.md`,
SHA-256
`FF60285434D78A625DD2D411327F689B0AE3DDDA6BB4A539710E60A65F742E0C`.
The follow-up analyzer's separate standalone report contains stale boilerplate
claiming freshness dominance/checkpoint reuse; its JSON counts and the canonical
comparison report show one freshness exit and zero reuse, so that paragraph is
not used.

### Evidence and replay coverage manifest

| Evidence | Exact source, coverage, and digest |
|---|---|
| Profile switch | `N:\Beelzebub\runtime\profile-switch\operations\profile-switch-700c63cacf9c4d5aabecff2966bf001b\manifest.json`; file SHA-256 `0A275FDDCDCAC200AF77E69C7EDA191B87EF1632ACA201CF05B1FC3C7DF052A6`; embedded digest `3be0a4c1bf7888f179161df33dc776a83a3d514bc6f87251af3c349bd63af399` |
| Baseline analysis fence | `N:\Beelzebub\runtime\profiles\beelzebub_five_minute_perpetual_v2\runs\20260910T232006Z-e2f1a956bdfd\analysis\beelzebub-perpetual-analysis-75bed919beeaea4f2bf99765\manifest.json`; SHA-256 `CAA1805E0F71FAAC1F7A3B11BAAB08AB481C76AF0B5E881195B447622BF48F47`; source snapshot digest `75bed919beeaea4f2bf997652553adb7889f922aa02a0db51ebdfc2abeb3daf0`; verifier `lv-6bbf1b4685f64913a617017fe7e320ff`, sequence 5258, tip `ddb44c9f5cce680061775dbabbd9977f1550be991bbae38195ac05c7b17e32f7` |
| Baseline report / observations | Same directory `report.json`; SHA-256 `E7C667BB10115CBC6D6E533F20136750E603940F7B1077A1419BCBAD787EC1A1`; ORDER 910, HEALTH 480, EXECUTION 114, POSITION 114, QUOTE/TRADE/DEPTH 0 |
| Baseline trades | Same directory `trades.csv`; 57 paired lifecycles / 114 receipts; SHA-256 `A88B4E9B71B991933308A0F44285EE9E07039C7D90FEF2733B9E232D6224D40C` |
| Follow-up analysis fence | `N:\Beelzebub\runtime\profiles\beelzebub_five_minute_perpetual_v2\runs\20260910T232006Z-e2f1a956bdfd\analysis\beelzebub-perpetual-analysis-bd15895f38934bc2f68adbc0\manifest.json`; SHA-256 `03E86C8D446206E5E4AD19A1009620D60051C7028FAAC35449D550DBC27D6CB1`; source snapshot digest `bd15895f38934bc2f68adbc050405e87ac57e4c1165f5759f4d3d36c10e88663`; verifier `lv-a0b8a5b6872e4685b6767d8ca49e56d4`, sequence 6477, tip `cce72b67e43543a90d6f4d5fa24ab275f4f50c78b016c83e57ea56c5faf1d687` |
| Follow-up report / observations | Same directory `report.json`; SHA-256 `541875B4D35987D1015CA27936D58C5BCDD130BEA6E6ED0996FEECB0CD284B6B`; HEALTH 145, ORDER 112, EXECUTION 14, POSITION 14, QUOTE/TRADE/DEPTH 0 |
| Canonical comparison | `N:\Beelzebub\runtime\profiles\beelzebub_five_minute_perpetual_v2\runs\20260910T232006Z-e2f1a956bdfd\analysis\perpetual-v2-patch-trial-comparison-20260911\manifest.json`; SHA-256 `4BB2BC0AF6DAD232E4BF4783A152863713FE763927839FE4450C5BC312232E7D`; bound `report.json` SHA-256 `9C7B0ACBFE048F0A0CF70480C63C6743EA5DB14DDE77F67C32BCC5F80AC60DCE`; `report.md` SHA-256 `FF60285434D78A625DD2D411327F689B0AE3DDDA6BB4A539710E60A65F742E0C` |

The verifier IDs and tips above are immutable historical analysis fences, not
claims about the mutable `ledger-verification-latest.json` file or current
runtime state.  This pass did not query a trading-control endpoint or current
account state.

The historical Perpetual decisions/fills can be reproduced, but the full
between-boundary stream does not exist in retained evidence.  Consequently
event-driven Discernment switches, legacy-switch Pong with causal bias history,
fixed-one versus conditional-two Pong economics, MFE/MAE, and continuous
mark-to-market are `UNAVAILABLE`.  The replay module therefore emits explicit
coverage denials rather than fabricated metrics.  No profitable subset,
doubled one-contract P&L, synthetic fill, or zero-valued substitute is presented
as actual performance.

Replay metrics carry an exact identity comprising name, definition, unit,
horizon, cost basis, and fill basis.  Deltas are computed only when that full
identity matches.  A name collision with different semantics is explicitly
`UNAVAILABLE / INCOMPARABLE`.  Per-capability coverage artifacts bind their
supporting source IDs and digests and keep `ACTUAL`, `SYNTHETIC`, and
`UNAVAILABLE` states distinct.

## Independent-review finding closure

The focused result below is against source-and-test Git tree
`4a66e7fa8a36708d39a55328ab6f48b49827c832`.  The independent reviewer also
ran 23 adversarial selectors plus 10 subtests in 0.97 seconds against that exact
tree, confirmed the same ending tree, and returned `ACCEPTED` with every known
blocking finding closed.  Implementation-worker completion messages were not
treated as review closure.

| Original finding | Contract / files | Regression evidence | Surviving fix | Fresh result | Disposition |
|---|---|---|---|---|---|
| Overlapping normal/safety/protective exits or late fills could duplicate reducing coverage | `src/l3g_paper/pong.py` | `test_safety_escalation_keeps_existing_exit_coverage_without_overlap`, `test_exit_partial_fills_reprotect_and_cannot_overexit`, `test_cancelled_protective_fill_racing_exit_is_visible_and_contained` | Immutable per-action/side remainder; targeted stop cancellation before deferred exit; race fills remain exact-once and visible | Focused suite: 124/124 pass | CLOSED_WITH_EVIDENCE |
| A reducing overfill was floored to flat and hid newly opposite exposure | `src/l3g_paper/pong.py` | `test_reducing_overfill_records_opposite_exposure_exactly_once` | Signed reduction math preserves reversal exposure, latches safety, and requests containment without duplicate fill authority | Focused suite: 124/124 pass | CLOSED_WITH_EVIDENCE |
| Fill IDs were cleared between candidate lifecycles and could be reused after restart | `src/l3g_paper/pong.py` | `test_prior_lifecycle_fill_id_survives_new_candidate_and_repeated_restore` | All-lifecycle consumed fill set is retained by state and strict checkpoint/restore | Focused suite: 124/124 pass | CLOSED_WITH_EVIDENCE |
| Admission trusted unbound inputs and could revive a stale opportunity after intervening switches | `src/l3g_paper/pong.py`, `src/l3g_paper/pong_quantity.py` | `test_required_component_identity_is_rechecked_at_admission`, `test_expired_snapshot_is_not_revived_by_caller_freshness_flag`, `test_admission_rejects_an_intervening_switch_cursor_round_trip`, `test_admission_identity_changes_with_opportunity_and_proofs` | Entry-time component/expiry/threshold/current-cursor checks and a newly hashed admitted action bind the exact current opportunity | Focused suite: 124/124 pass | CLOSED_WITH_EVIDENCE |
| A partially filled two-MNQ candidate could keep its remainder after losing strong-tier authority | `src/l3g_paper/pong.py` | `test_partial_entry_confidence_loss_cancels_remainder_and_accounts_race` | Explicit pending-entry cancellation; any racing remainder fill is still accounted for and protected | Focused suite: 124/124 pass | CLOSED_WITH_EVIDENCE |
| Discernment switches were not self-verifying or pinned to one stream/policy | `src/l3g_paper/discernment.py`, `src/l3g_paper/pong.py` | `test_switch_verification_rejects_identity_policy_and_latch_tampering`, `test_switch_identity_and_threshold_contract_are_pinned`, `test_duplicate_out_of_order_gap_and_domain_change_never_enter` | Deterministic prior-ID/sequence/latch/policy/model/threshold checks plus exact current cursor | Focused suite: 124/124 pass | CLOSED_WITH_EVIDENCE |
| A hash-consistent forged snapshot could supply invented scores or dependency-group effects | `src/l3g_paper/discernment.py`, `src/l3g_paper/pong.py` | `test_hash_consistent_snapshot_score_and_group_forgeries_are_rejected` | Snapshot verification independently re-derives bounded dependency effects and raw/effective/display scores from component evidence | Focused suite: 124/124 pass | CLOSED_WITH_EVIDENCE |
| Snapshot/switch identities omitted raw source-content hashes and causal envelopes | `src/l3g_paper/discernment.py` | `test_source_content_hash_changes_snapshot_and_switch_identity`, `test_deterministic_hash_replays_exact_causal_evidence` | Component/snapshot payloads bind source IDs/hashes and causal times, then the switch binds the snapshot | Focused suite: 124/124 pass | CLOSED_WITH_EVIDENCE |
| Mapper accepted all-NQ or mixed MNQ/NQ volume-regime evidence and relabelled it MNQ | `src/l3g_paper/pong_microstructure.py` | `test_mutually_matching_wrong_instrument_is_refused`, `test_mixed_mnq_flow_and_nq_volume_regime_is_refused`, `test_window_instruments_must_match_and_bind_result_identity` | Volume windows/results bind instrument through result/group hashes; every mapped artifact must be exactly `MNQ SEP26`; mixed or wrong-instrument evidence maps to zero with an explicit blocker | Focused suite: 124/124 pass | CLOSED_WITH_EVIDENCE |
| Mode and live-authority state were absent from quantity proofs, status, and restart identity | `src/l3g_paper/pong.py`, `src/l3g_paper/pong_quantity.py` | `test_reconciliation_freshness_integrity_and_signature_fail_closed`, `test_status_projection_is_explicit_and_never_armed`, `test_checkpoint_codec_is_canonical_strict_and_hash_bound` | Sealed `PAPER_SIM101` / `DISARMED` identity is hash-bound through policy, reconciliation, status, checkpoint, and restore | Focused suite: 124/124 pass | CLOSED_WITH_EVIDENCE |
| Historical quantity defaults were described as verified current constraints and an active AddOn | `src/l3g_paper/pong_quantity.py`, this report | `test_historical_default_risk_and_native_caps_reduce_two_to_one` | Default status is explicitly `HISTORICAL_ADDON_SEALED_TO_ONE_NOT_CURRENT_AUTHORITY`; source and report state that current runtime/account were not queried | Focused suite: 124/124 pass | CLOSED_WITH_EVIDENCE |
| Restore used weak bare state and lost pending/protective/reducing identities and lockouts | `src/l3g_paper/pong.py` | `test_checkpoint_codec_is_canonical_strict_and_hash_bound`, `test_truncated_checkpoint_never_defaults_to_clean_state`, `test_nonflat_restore_quarantines_reducing_identity_across_restarts`, `test_restore_failure_preserves_existing_lockout_reason_and_proof` | Strict canonical codec; proof binding; repeated restore; quarantined action/fill identities; prior safety plus separate recovery blockers | Focused suite: 124/124 pass | CLOSED_WITH_EVIDENCE |
| No frozen microstructure-to-Discernment mapping, causal end-to-end test, or source-bound audit/replay boundary | `src/l3g_paper/pong_microstructure.py`, this report | `test_mapping_effect_changes_propagate_into_discernment_hashes`, `test_synthetic_events_map_causally_through_snapshot_and_switch`, symmetric flow/liquidity/regime tests | Mapping hash propagates into policy identity; raw synthetic events traverse mapper, Discernment seed/switch, and disabled Pong consumption; history remains digest-bound and coverage-limited | Focused suite: 124/124 pass | CLOSED_WITH_EVIDENCE |
| Replay compared metrics by name alone | `src/l3g_paper/pong_replay.py` | `test_same_metric_name_with_different_identity_is_not_compared`, `test_available_coverage_requires_a_specifically_bound_source_digest` | Exact name/definition/unit/horizon/cost/fill identity and digest-bound coverage | Focused suite: 124/124 pass | CLOSED_WITH_EVIDENCE |

## Test and evidence inventory

| Check | Environment / exact result |
|---|---|
| Frozen source/test snapshot | Git tree `4a66e7fa8a36708d39a55328ab6f48b49827c832` |
| Focused integrated Python | `.venv312\Scripts\python.exe -m unittest tests.test_shared_confidence_math tests.test_l3g_discernment tests.test_l3g_pong_microstructure tests.test_l3g_pong_replay tests.test_l3g_pong_quantity tests.test_l3g_pong tests.test_l3g_daily_bias tests.test_scientific_alpha_engine -v`; 124 passed, 0 failed/error/skipped, 5.289 s |
| Python compile / diff hygiene | `py_compile` over every changed Python source/test passed; `git diff --check` passed with only Git's non-failing LF-to-CRLF notices |
| Frontend isolated build | Worktree `node_modules` junctioned to the unchanged foundation checkout dependency tree; `npm run build` passed under Node 24.14.1 / npm 11.11.0; generated files remain ignored |
| Frontend unit tests | `npm test`; 3 files, 63 tests passed, 0 failed |
| First repository-wide diagnostic | Process-only `TEMP/TMP=D:\CodexTestTemp\Pong-resume-01a08ed2`, `BEELZEBUB_PROFILE_SWITCH_ROOT=D:\CodexTestState\Pong-resume-01a08ed2`; 1,320/1,329 passed plus 452 subtests, 9 failures, 0 errors/skips.  All nine were `WinError 1` from unsupported `os.link` on that `D:` filesystem; this is not acceptance evidence and no test was omitted. |
| Second repository-wide diagnostic | Fresh `C:` NTFS isolation collected 1,369 items; 1,226 passed and 143 failed plus 428 subtests and 2 warnings in 581 s.  The widespread failures were low-free-space/capacity cascades; this is not acceptance evidence and no failure was suppressed. |
| Final repository-wide rerun | `PASS`; unfiltered `.venv312\Scripts\python.exe -m pytest -q` collected and passed 1,347/1,347 tests plus 467 subtests, with 0 failures/errors/skips/warnings in 1,305.25 s (21m 45s), exit 0.  `TEMP/TMP=H:\CodexTestTemp\Pong-resume-final5-01a08ed2`; selector root `H:\CodexTestState\Pong-resume-final5-01a08ed2`; UTC `2026-09-11T09:19:03.6543457Z` through `2026-09-11T09:40:50.3389445Z`; pre/post tree exactly `4a66e7fa8a36708d39a55328ab6f48b49827c832`; complete log `H:\CodexTestTemp\Pong-resume-final5-01a08ed2-pytest-20260911T0919036361677Z.log` |
| Independent final review | `ACCEPTED`; 23 adversarial selectors plus 10 subtests passed in 0.97 s on isolated `H:` roots; ending tree exactly matched the frozen source/test tree |

The two diagnostic environments were deliberately rejected as acceptance
evidence instead of weakening hard-link/path-safety behavior, suppressing
failures, or skipping tests.  The final rerun uses a fresh high-capacity NTFS
path outside deployed/runtime roots.  No test process receives a deployed
selector, ledger, operation ID, or transport endpoint.

## Finish-state matrix

| Dimension | Candidate state |
|---|---|
| Implementation | Isolated pure candidate; no runtime query or mutation was performed |
| Tests | 124 focused deterministic tests pass; final isolated repository-wide suite passed 1,347/1,347 plus 467 subtests with no failures/errors/skips/warnings |
| Replay | Coverage-bounded; actual, modeled, synthetic, and unavailable remain distinct |
| Calibration | Not ready; experimental directional support only |
| Two-MNQ mathematical reachability | Rule tested; unreachable under inherited 0.70 reliability ceiling |
| Dollar-risk feasibility | Blocked by the latest recovered historical $50 cap at the 25-point stop; current state was not queried |
| Native quantity support | Unproven; conservative candidate default retains the latest recovered historical quantity-one evidence |
| Operational authorization | None; disabled, unregistered, uncommissioned; current runtime/account state UNKNOWN and not queried |

No merge, push, deployment, NinjaTrader compilation, profile registration,
commissioning, service start, order placement, or live-capital change is part
of this revision.
