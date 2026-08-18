# Phase E.5 preregistered prospective experiment specification

Date: 2026-08-18

Scientific baseline: `ee990643ea096e32f97c6177b4b1a165241d05e7`

Protocol: `e5-protocol-v1.json`

Status at this document revision: methodology implementation complete; immutable protocol identity is assigned by the final freeze commit. No E.5 acquisition has started.

## 1. Purpose and frozen history

E.5 is a new prospective experiment. It does not reinterpret, migrate, repair, or resample E.4. Phase D, E.1, E.2, E.3, and E.4 v1 retain their historical semantics. E.4 v1 remains permanently inconclusive. The E.4.1 dependence review remains the reason a new design is necessary.

The E.4.1 findings that control this design are:

- 1,697 resolved validation rows formed 208 nominal graph components, but one component contained 1,366 rows (80.495%).
- The inverse-Herfindahl effective component count was approximately 1.543.
- Wallet-only, temporal-overlap-only, and exact-endpoint-only components were locally small. Their alternating transitive links produced the giant component.
- Extending exposure to the observed 5--10 second resolution contract increased the giant to about 1,421 rows.
- Whole-component pairs resampling was not defensible because the components were emergent, extremely unequal, and nonexchangeable.
- Mature missingness depended strongly on liquidity, symbol, graph density, time, and graph position. It was not plausibly MCAR.

E.5 therefore creates its primary inference units before outcomes exist. Graph components are retained as diagnostics and fail-closed enforcement, never promoted into a population of exchangeable resampling units.

## 2. Primary experimental unit

The primary unit is one precommitted, hash-selected 30-minute market-session block inside a fixed eight-day calendar epoch. All admitted observations whose anchors occur in that 30-minute sampling interval belong to that block. Their economic exposure may extend only to anchor plus 10 seconds and is contained in the block exposure envelope.

The experiment contains exactly 60 scheduled epochs beginning at `2026-09-01T00:00:00Z` and ends at the hard stop `2027-12-25T00:00:00Z`. In each epoch, the protocol hash and fixed schedule seed select one of the first 47 half-hour UTC slots. Because the next epoch begins eight days later, adjacent exposure envelopes are separated by at least seven full days. There are no replacement blocks.

This unit is claimed to be approximately independent, not metaphysically independent. The claim is made plausible by all of the following design features:

1. primary units occur in distinct eight-day epochs across approximately sixteen months;
2. their economic resolution windows never overlap and have a minimum seven-day cooldown;
3. wallet cohorts are assigned by a preregistered salted hash;
4. a wallet may be admitted to only its cohort-matching block and to only its first admitted block in the experiment;
5. exact transaction, endpoint-family, campaign, wallet, or realized-exposure relations may not cross blocks;
6. the schedule is fixed without any effect or outcome information;
7. no single block, symbol, wallet, component, endpoint family, or local burst may dominate the design weights;
8. the final analysis contrasts predicate and complement within blocks and retains unrestricted dependence inside each block.

The remaining principal weakness is persistent market-regime dependence lasting longer than seven days. No calendar separation can prove its absence. E.5 reduces that risk through long spacing, a sixteen-month horizon, UTC-slot variation, symbol concentration gates, and fail-closed cross-block diagnostics. A known campaign or dependence mechanism spanning blocks invalidates the experiment; it is not repaired statistically.

## 3. Deterministic sampling and admission

### 3.1 Schedule

The schedule is `HASHED_SLOT_WITHIN_FIXED_8DAY_EPOCH_V1`:

- acquisition start: `2026-09-01T00:00:00Z`;
- epochs: 60;
- epoch length: 691,200 seconds (eight days);
- candidate slots: the first 47 half-hour slots in each epoch;
- sample duration: 1,800 seconds;
- schedule seed: 550017;
- minimum exposure-envelope separation: 604,800 seconds;
- hard stop: `2027-12-25T00:00:00Z`;
- replacements or extensions: zero.

The block ID hashes the protocol identity, epoch ordinal, selected times, exposure end, and wallet-cohort ordinal. The same protocol always produces the same schedule and block IDs.

### 3.2 Eligibility known at the anchor

Admission may use only predictor-side or acquisition-side fields available at the anchor. It may not use outcome availability, outcome sign, realized return, later price movement, family effect estimates, or any proxy computed from those values.

The symbol rule is `PREANCHOR_CONTINUOUS_TRADE_LIQUIDITY_V1`. It uses only authoritative Phase D market-trade timestamps before the anchor and requires at least 172,800 prints in the prior 24 hours, at least 3,600 prints in the prior 30 minutes, no interprint gap above two seconds in that 30-minute window, and no source discontinuity. These correspond to a conservative two-print-per-second operational floor, chosen from the prospective resolution requirement rather than historical effect performance. The eligibility snapshot is hashed into the observation. A missing prerequisite is an exclusion, not permission to inspect the outcome.

Wallets are assigned to one of four cohorts using a salted SHA-256 identity fixed in the protocol. Block ordinal modulo four selects the active cohort. An admitted wallet cannot be admitted to a later block. At most the first eligible event is admitted within a block and a 60-second wallet cooldown is retained. Exact source-event identity is unique.

Position or campaign lineage, when available at sampling time, must not cross primary blocks. A later-discovered cross-block transaction, endpoint family, campaign, wallet, or overlapping exposure relation causes `DEPENDENCE_GATE_FAILED`. It does not cause reassignment.

### 3.3 Late and operational evidence

Feed maintenance does not create replacement blocks. A predeclared outage can make a scheduled block unavailable, but the block remains in the stopping denominator. An unplanned outage produces missing or structurally unresolved evidence under the same final gates. Collection may not be extended to compensate.

## 4. Dependence model

Dependence is unrestricted within a primary block. The enforcement graph contains these direct relations:

- same wallet;
- same source event;
- same exact transaction;
- same endpoint family;
- same campaign or position lineage;
- overlapping same-symbol realized economic exposure from anchor through the actual allowed endpoint.

The graph uses transitive closure only to identify concentration and cross-block violations. It is never the cluster definition for resampling. Any direct graph edge crossing two primary blocks fails the dependence gate. The largest component's weight share is also capped by the concentration gate.

The maximum resolution lag is 10 seconds from the anchor, not merely the nominal five-second horizon. Thus anchors six seconds apart on the same symbol can remain linked, preventing the E.4 under-clustering error.

## 5. Concentration and effective-support gates

All gates run before evaluation-outcome values are released to the inference capability. Failure gives no numerical effect, inferential confidence interval, valid p-value, or bootstrap authority.

Minimum support:

| Measure | Minimum |
| --- | ---: |
| Admitted and finally resolved observations | 600 |
| Observations in each predicate arm | 240 |
| Primary blocks | 48 |
| Blocks containing both arms | 40 |
| Inverse-Herfindahl effective blocks, total | 40 |
| Inverse-Herfindahl effective blocks, each arm | 40 |
| Effective within-block contrast-information blocks | 40 |
| Effective symbols | 12 |

For weights `W_g`, effective support is `1 / sum_g (W_g / sum_h W_h)^2`. It is computed for total block weight, predicate block weight, complement block weight, and within-block contrast information `q_g = W1g W0g / (W1g + W0g)`. The last quantity is the design leverage of block `g` on the within-block coefficient.

Maximum shares:

| Dimension | Maximum design-weight share |
| --- | ---: |
| Primary block | 5% |
| Predicate weight in one block | 5% |
| Complement weight in one block | 5% |
| Within-block contrast information / design leverage | 5% |
| Dependence component | 5% |
| Wallet | 1% |
| Symbol | 10% |
| Exact endpoint family | 2.5% |
| Same-symbol local 30-second window | 2.5% |

The maximum-to-minimum sampling-weight ratio is 5:1. The joint gates matter: nominal count alone, effective count alone, or a maximum-share gate alone is insufficient.

The 48/40/5% envelope is deliberately stricter than the E.4.1 starting recommendation of 30/20/10%. It ensures at least 40 effective contributors to the common within-block contrast and prevents any cluster from contributing more than one twentieth of its design information. These thresholds were chosen before E.5 outcomes, independently of whether E.4 would pass, and are exercised by null, effect, unequal-cluster, few-cluster, dominant-cluster, missingness, and replay simulations. They are design guarantees, not universal claims that every 48-cluster problem is valid.

## 6. Maturity and missingness

The outcome horizon begins at anchor plus five seconds. The qualifying resolution event is the first allowed same-symbol print from anchor plus five through anchor plus ten seconds. Final maturity occurs after the ten-second economic window plus a 120-second ingestion grace period.

States are distinct:

- `IMMATURE`: final deadline has not passed; this delays analysis.
- `ADMISSIBLE_OBSERVED`: an in-window event arrived before the final deadline.
- `STRUCTURALLY_UNRESOLVED`: required source or lineage is unavailable.
- `MATURE_MISSING`: no qualifying evidence existed by the deadline.
- `MISSING`: an expected artifact is absent.
- `STALE`: evidence falls outside the economic window.
- `LATE`: qualifying economic-time evidence arrived after finalization.
- `INVALIDATING_MISSINGNESS`: an integrity rule classified the evidence as invalidating.

E.5 v1 authorizes no inverse-probability weighting, imputation, complete-case claim, or sensitivity-bound substitute. Final resolution must be 100% overall, per block, per symbol, per preregistered liquidity stratum, per graph-density stratum, and per time stratum. Any final unresolved or missing item fails the family-wide missingness gate. This strict rule avoids assuming MCAR or an untestable MAR model after E.4.1 showed strong structured missingness. It may make E.5 inconclusive; that is scientifically preferable to a post-outcome correction.

Late evidence does not reopen a finalized record. It triggers integrity review and a successor protocol if the acquisition mechanism is defective.

## 7. Frozen estimand and inference

### 7.1 Estimand

The estimand is the design-weighted common within-block predicate-minus-complement difference:

`beta = sum_g q_g (mean_w(Y | P,g) - mean_w(Y | not P,g)) / sum_g q_g`,

where `q_g = W1g W0g / (W1g + W0g)` and `W1g`, `W0g` are arm-specific design weights. This is the coefficient on the frozen predicate in a design-weighted regression with primary-block fixed effects and a common predicate slope.

It is an observational association. The predicate is not randomized, so neither the estimand nor a significant result establishes causal treatment effect or deployable alpha.

### 7.2 Studentized wild cluster bootstrap

The exact method is `RESTRICTED_STUDENTIZED_WILD_CLUSTER_BOOTSTRAP_T_LOCO_V1`:

1. fit the block-fixed-effect restricted null with predicate coefficient zero;
2. retain the full within-block residual vector;
3. draw one Webb six-point, mean-zero, unit-variance multiplier per primary block;
4. form the null bootstrap sample without splitting a block;
5. refit the common within-block predicate coefficient;
6. studentize the observed and every bootstrap coefficient using a delete-one-primary-block jackknife standard error;
7. use 9,999 deterministic replications;
8. derive the RNG seed from the protocol hash, hypothesis ID, and base seed 550017;
9. order blocks canonically by immutable block identity;
10. compute a plus-one two-sided absolute bootstrap-t p-value;
11. construct the 95% equal-tailed bootstrap-t interval with fixed type-7 quantiles.

At least 99% of requested draws must have finite positive studentization. Singular observed variance, zero within-block arm variation, an inadmissible leave-one-block fit, or excessive invalid draws refuses inference. Singleton row clusters are not inherently forbidden, but all global mixed-block, effective-support, concentration, and leverage gates still apply.

Wild bootstrap does not create independent blocks. Its validity rests on the prospective spacing, cross-block exclusion, adequate low-leverage block support, finite moments, and correct design weights.

## 8. Frozen family and multiplicity

The family contains exactly two outcome-blind E.3 control predicates:

1. `wallet_action@1 GT 0` versus its complement;
2. `wallet_action@1 LT 0` versus its complement.

They are closely related, algebraically opposite contrasts when memberships are complete. They remain two frozen members because post hoc family contraction is forbidden. Holm-Bonferroni controls familywise error at 0.05 over denominator two. An inadmissible or unevaluable member contributes p=1 and remains in the denominator. Ties use frozen ordinal then hypothesis ID.

Scientific support requires both Holm-adjusted p-value at most 0.05 and absolute effect at least 0.001. Practical relevance remains a separate gate.

## 9. Stopping rule

The experiment stops after its 60 fixed scheduled blocks and fixed hard stop. It does not stop for significance, near-significance, effect size, profitability, favorable symbols, or unfavorable results. It does not add replacement blocks.

At the hard stop:

- immature evidence transitions only to the fixed final maturity process;
- missingness failure is terminal for v1;
- fewer than 48 admissible blocks or any effective-support failure is `INSUFFICIENT_SUPPORT` and then `INCONCLUSIVE`;
- concentration or dependence failure is terminal and inconclusive;
- only a completely admissible design becomes `ELIGIBLE_FOR_INFERENCE`.

Collection cannot be extended after seeing an unfavorable or nearly significant result. A successor is a scientifically new protocol, not continuation of v1.

## 10. Leakage and capability boundary

Predictor-side membership records and outcome-side values use separate types and interfaces. `DesignObservation` has no outcome field. The registry invokes an outcome reader callback only in `ELIGIBLE_FOR_INFERENCE`; blocked attempts do not invoke the callback and are append-only audited. The successful evaluation-outcome-read counter remains zero through preregistration and design validation.

The prospective module contains no historical E.4 evidence loader and rejects any observation whose source schema is not `phase-e5-prospective-observation-v1` or whose protocol hash differs. It has no test-partition query method. Reserved test-query count is fixed to zero.

Application UI, ordinary logs, and debug tooling must receive predictor-side status or hashes only before inference eligibility. The scientific boundary cannot defend against a database or operating-system administrator deliberately bypassing process capabilities; deployment must therefore place outcome storage under a separate file/role ACL and restrict raw administrative access. Such bypass is a protocol-integrity failure, not an unofficial analysis channel.

## 11. Versioning, audit, and replay

The machine protocol records schema version, protocol version, code commit, timestamps, configuration hashes, family, unit, sampling, gates, inference, multiplicity, stopping, protected-data policy, and authority. Its ID is derived from the type-tagged SHA-256 semantic payload. The ID and hash fields are derived and excluded from the hash input; every other semantic field, including code commit and freeze timestamp, is bound.

The SQLite proof registry uses immutable protocol rows, append-only protocol events, append-only outcome-access audit events, and compare-and-swap state transitions. Any semantic change produces a different protocol hash and requires v2 or later. First admission is an irreversible boundary.

Replay canonicalizes block and observation order. Given identical raw evidence, protocol, admissibility state, and seed, replay reproduces schedule, membership, evidence classifications, graph diagnostics, gate metrics, wild multipliers, bootstrap distribution hash, interval, raw p-values, Holm results, decisions, and final replay hash.

## 12. Fail-closed states

The state vocabulary is:

`PREREGISTERING`, `FROZEN_NOT_STARTED`, `COLLECTING`, `AWAITING_MATURITY`, `INSUFFICIENT_SUPPORT`, `MISSINGNESS_GATE_FAILED`, `CONCENTRATION_GATE_FAILED`, `DEPENDENCE_GATE_FAILED`, `PROTOCOL_INTEGRITY_FAILED`, `ELIGIBLE_FOR_INFERENCE`, `INFERENCE_COMPLETED`, `INCONCLUSIVE`, `SUPPORTED`, and `REJECTED`.

Only `ELIGIBLE_FOR_INFERENCE` can release outcome values to the inferential capability. Gate failures do not report an authoritative estimate, p-value, interval, or bootstrap distribution.

## 13. Authority boundary

E.5 has no prediction, signal, execution, position-sizing, leverage, capital-allocation, or trading authority. It cannot change Phase D execution or risk controls. A supported result cannot activate a strategy. Any future bridge requires a separately reviewed and commissioned phase.

## 14. Commissioning boundary

This pass freezes methodology only. It does not admit a block, attach an E.5 outcome, query reserved test data, or run a real hypothesis. The authorized next action is a separate prospective acquisition handoff using the exact frozen protocol.
