# Phase E.4 scientific dependence review

Date: 2026-08-18

Review baseline: `21cb2e7cf602e7b7ecea9d1024f658e11ed7e7e5`

Authoritative database: `E:\Beelzebub\runtime\hot\copytrade.sqlite3`

Materialization: `e2-5f761a9f987d17003c1a20c2f7b72c12`

## Scientific conclusion

**`E.4.1 METHODOLOGY REVISION REQUIRED`**

The original commissioning result remains correctly and immutably
`INCONCLUSIVE_MISSING_EVIDENCE`. The mature-missing gate ran before the effect,
interval, p-value, or bootstrap, so no invalid numerical claim was made.

E.4 v1 is not, however, scientifically ready to produce inferential statistics
if a later evidence snapshot clears that gate. Three concrete defects are
established:

1. The support rule counts 208 graph components but ignores their mass and
   leverage. One component holds 80.495% of resolved rows and sampling weight.
   Its inverse-Herfindahl effective component count is 1.543, not 208.
2. Whole-component pairs resampling assumes the final components are defensible
   independent, approximately exchangeable sampling units. They are not sampled
   units. They are transitive closures of repeated-wallet, exact shared-endpoint,
   and local temporal-overlap relations, with a 1,366:1 largest-to-median size
   ratio. Local dependence links are converted into arbitrary dependence among
   nodes up to 15 graph edges and almost 15 minutes apart.
3. The graph labels `[anchor, anchor+5s]` as the outcome window even though the
   persisted outcome is resolved at the first print from `anchor+5s` through
   `anchor+10s` and the return exposure runs from the anchor price to that actual
   print. The frozen graph has 2,541 five-second overlap pairs; realized exposure
   has 2,736. Using the realized endpoint diagnostically produces a 1,421-row
   component (83.736%), so v1 is simultaneously capable of transitive
   over-collapse and direct under-clustering.

These are methodology defects, not a reason to rescue either current
hypothesis. E.4.1 must be preregistered against a new, independently collected
experiment. This review does not implement it because the current single-hour
corpus cannot establish an independent-block duration or supply enough
independent replications.

## Review guardrails

- All new database access was opened read-only with `PRAGMA query_only=ON`.
- Every membership query contained the literal
  `partition_name='validation'`.
- Test membership and test outcomes were not queried.
- Net-outcome values were not read. Diagnostics used resolution state,
  predictor sign, sampling design, timestamps, wallet/event lineage, symbols,
  and endpoint identity only.
- No protocol, hypothesis, threshold, result, database row, or scientific code
  was modified.
- The diagnostic program was isolated under ignored `work/` and did not persist
  scientific state.
- No wallet identity was exported into this report.

## Giant component anatomy

### Inventory and final distribution

The validation snapshot contains 2,571 rows: 1,697 resolved and 874
mature-missing. The frozen v1 graph operates on resolved rows only.

| Metric | Value |
| --- | ---: |
| Final components | 208 |
| Singletons / non-singletons | 151 / 57 |
| Median / mean component size | 1 / 8.159 |
| P90 / P95 / P99 size | 3 / 5 / 7 |
| Largest 10 | 1,366, 11, 7, 7, 6, 6, 6, 6, 5, 5 |
| Largest share | 80.495% |
| Giant wallets / source events / symbols | 330 / 1,366 / 120 |
| Giant anchor span | 894.499 seconds |
| Direct graph degree median / mean / P95 / max | 8 / 13.353 / 40 / 67 |
| Giant mean shortest path / diameter | 5.424 / 15 edges |

The giant contains 688 positive-sign and 678 negative-sign rows. Its positive
weight share is 50.366%, so it is not a one-arm component.

### Direct relation counts

Pair counts overlap when one pair has more than one relation.

| Direct relation | Pair edges | Scientific meaning |
| --- | ---: | --- |
| Same wallet | 9,079 | 236 repeated-wallet groups; largest wallet group 41 |
| Same-symbol anchors at most 5 seconds apart | 2,541 | Local overlapping/touching v1 intervals |
| Same exact outcome endpoint observation | 1,212 | 257 repeated endpoints covering 707 rows; largest group 28 |
| Same source event ID | 0 | All 1,697 anchor source-event IDs are unique |
| Same non-sentinel transaction hash | 905 | Largest group 28; all already joined by v1 relations |
| Same transaction/trade ID | 20 | All already joined by v1 relations |
| Realized anchor-to-endpoint exposure overlap | 2,736 | 195 more pairs than the v1 interval rule |
| Same-symbol temporal adjacency from 5 to 10 seconds | 1,349 | Not joined by v1 unless another relation exists |

Every exact shared-endpoint edge is also a v1 temporal-overlap edge. It adds no
unique final edge after the full overlap rule is present. Of the 2,541 temporal
edges, 2,251 cross wallets and 1,250 are not also a wallet or shared-source edge.
The graph has 11,330 unique direct v1 edges.

Valid transaction hashes do represent genuine common transaction evidence:
the largest groups occur at one timestamp, on one symbol, across multiple
wallets. They do not explain the giant because every such pair is already
joined by v1. HyperCore's all-zero hash is a sentinel and was excluded from this
diagnostic.

### Component construction and ablation

| Relations included | Components | Largest | Share |
| --- | ---: | ---: | ---: |
| Wallet only | 568 | 41 | 2.416% |
| Exact source-observation aliases only | 1,247 | 28 | 1.650% |
| Five-second overlap only | 954 | 43 | 2.534% |
| Wallet + exact source | 309 | 1,132 | 66.706% |
| Wallet + overlap | 208 | 1,366 | 80.495% |
| Source + overlap | 954 | 43 | 2.534% |
| Frozen full graph | 208 | 1,366 | 80.495% |
| Wallet + source + realized exposure overlap | 180 | 1,421 | 83.736% |

Removing source aliases from the full graph changes nothing because those edges
are a subset of overlap edges. Removing overlap leaves a 1,132-row component;
removing wallet leaves a maximum of 43. Within the original giant, wallet-only,
source-only, and overlap-only graphs fragment it into 330, 945, and 665 pieces,
with maxima 41, 28, and 43. The giant therefore is not one dense direct-dependence
body. Repeated wallets are the long-range backbone; local exact-endpoint and
same-symbol time links bridge those wallet groups.

### Percolation

Starting with wallet and exact-source links, adding only same-symbol links at
or below each diagnostic lag yields:

| Maximum lag | Largest component | Resolved share |
| --- | ---: | ---: |
| 0 seconds | 1,132 | 66.706% |
| 0.1 seconds | 1,135 | 66.883% |
| 0.5 seconds | 1,179 | 69.476% |
| 1 second | 1,245 | 73.365% |
| 2 seconds | 1,285 | 75.722% |
| 3 seconds | 1,307 | 77.018% |
| 4 seconds | 1,333 | 78.550% |
| 5 seconds | 1,366 | 80.495% |

This is percolation behavior. The largest component grows monotonically through
many local bridges rather than appearing as a single event or wallet cluster.
The giant has 138 articulation nodes and 91 graph bridges, although no one
current articulation is solely responsible: the worst single-node removal
separates only 12 rows from the remaining 1,353. It is a robust percolated core,
not a one-bad-row artifact.

### Direct dependence versus transitive closure

Same wallet, same exact endpoint, and same transaction are reasonable direct
dependence indicators. Same-symbol return exposures close in time are also
locally dependent. None implies that every node reachable through alternating
wallet and time links is exchangeable only as one indivisible sampling unit.

Connected closure is a conservative way to create a partition with no declared
edge crossing clusters. It is valid only if the scientific model permits
arbitrary covariance to propagate across the entire component and the resulting
components are the independently sampled units. Neither condition is
established. Market-return dependence should decay with temporal and graph
distance; a wallet can bridge distinct symbols and periods; and a busy local
market interval can bridge many wallets. Connectivity is therefore not
equivalent to statistical inseparability here.

## Experimental unit

The frozen E.2 sampling unit is an individual wallet-fill anchor. E.2 selected
anchors by an unstratified deterministic hash from one fixed source interval;
it did not sample graph components or independent sessions. All validation
weights equal 31.3765, so weighting does not mitigate the observed concentration.

For uncertainty about a repeated-market experiment, the defensible primary
unit is a **predeclared, independently sampled market-session/time-block
replicate**, with all dependence inside that replicate retained. Predicate and
complement should be contrasted within those blocks. Wallets, exact endpoint
prints, and transaction groups must not cross primary units; otherwise the
design needs a justified multiway method and sufficient support in every
dimension.

The current connected component is a conservative dependence envelope, not a
valid empirically sampled unit. The present 15-minute validation interval is at
most one market-session realization. The exact effective sample size cannot be
identified from graph topology alone because edge labels do not specify
covariances. The mass-based effective counts below are diagnostics, not a claim
that 1.543 is the true sampling size.

## Bootstrap review

E.4 v1 samples 208 complete components uniformly with replacement, expands all
rows in each sampled component, preserves row sampling weights, re-forms the
predicate and complement, and computes their weighted-mean difference. A
component may contain both groups; 24 do, including the giant. Whole-component
sampling therefore preserves within-component covariance and the mixed-group
contrast in form.

It does not produce defensible inference under this structure:

- The dominant component is omitted in 36.699% of resamples, drawn once in
  36.877%, and drawn at least twice in 26.424%.
- Cluster-size coefficient of variation is 11.569, and the largest-to-median
  ratio is 1,366.
- The giant holds 77.477% of all resolved positive-sign rows and 83.807% of all
  resolved negative-sign rows.
- The pairs cluster bootstrap is asymptotic in independent clusters. Its
  empirical distribution treats one 1,366-row network component and a singleton
  as draws from the same cluster population. That exchangeability assumption is
  untenable.
- Fixed-seed output is tied to component ordering. Reversing the identical row
  set preserves component membership but changes component IDs, ordered sizes,
  and the 1,000-draw sampled-mass fingerprint. Authoritative ordinal order makes
  restart replay stable, but equivalent graph construction is not invariant.

The likely bootstrap distribution is a mixture dominated by whether the giant
is absent, present, or duplicated. Increasing the number of bootstrap draws
would estimate that unsuitable empirical distribution more precisely; it would
not repair the experimental unit.

Primary references support this boundary. Cluster bootstrap theory presumes
independent groups and large-cluster asymptotics, and few/high-leverage or highly
unbalanced clusters are known failure modes:

- Djogbenou, MacKinnon, and Nielsen,
  [Asymptotic theory for clustered samples](https://doi.org/10.1016/j.jeconom.2019.02.001).
- Cameron, Gelbach, and Miller,
  [Bootstrap-Based Improvements for Inference with Clustered Errors](https://doi.org/10.3386/t0344).
- MacKinnon and Webb,
  [The wild bootstrap for few (treated) clusters](https://doi.org/10.1111/ectj.12107).
- MacKinnon, Nielsen, and Webb,
  [Leverage, influence, and the jackknife in clustered regression models](https://doi.org/10.1177/1536867X231212433).

Block methods are designed for weakly dependent ordered observations, but they
also need stationarity/mixing assumptions, many blocks, and a block-length
contract not supplied by the current corpus:

- Künsch,
  [The jackknife and the bootstrap for general stationary observations](https://doi.org/10.1214/aos/1176347265).
- Politis and Romano,
  [The Stationary Bootstrap](https://doi.org/10.1080/01621459.1994.10476870).

## Support review and effective information

| Diagnostic | Overall | Positive arm | Negative arm |
| --- | ---: | ---: | ---: |
| Nominal component count | 208 | 135 | 97 |
| Largest component mass share | 80.495% | 77.477% | 83.807% |
| Herfindahl concentration | 0.6483 | 0.6009 | 0.7027 |
| Inverse-Herfindahl effective count | 1.543 | 1.664 | 1.423 |
| Entropy effective count | 4.430 | 4.923 | 3.193 |

The row-weight Kish ESS is 1,697 only because all validation weights are equal;
it measures weight dispersion while incorrectly treating dependent rows as
independent. It is not an inferential ESS. Component count alone is plainly
insufficient, and concentration must enter E.4.1 support.

No single scalar is enough. A future gate must jointly cover actual primary
cluster count, total and arm-specific weight concentration, maximum cluster
leverage, mixed predicate/complement support, and leave-one-cluster influence.
These quantities should be computed from preregistered design weights and
predictor membership before outcome values are exposed wherever possible.

## Predicate versus complement

The contrast remains algebraically identifiable in the resolved rows: there
are 888 positive-sign and 809 negative-sign rows, and the giant contains both.
It is not independently replicated. The giant contributes most of each arm,
so its shared market and wallet structure dominates both denominators and their
covariance. Calling the sign predicate a treatment would be unjustified; it is
an observational predicate.

A future block-level score/wild bootstrap can retain the paired within-block
covariance. It must not split a shared endpoint, transaction, or primary market
block merely to increase nominal support.

## Missingness

The 874 mature-missing rows are not plausibly MCAR. Outcome resolution requires
a same-symbol trade print between 5 and 10 seconds after the anchor, so missing
probability is mechanically tied to post-anchor trading intensity.

| Diagnostic | Missing rate |
| --- | ---: |
| Overall | 33.995% |
| Positive predicate | 34.027% |
| Negative predicate | 33.959% |
| Lowest hour-print-liquidity quartile | 73.385% |
| Highest hour-print-liquidity quartile | 6.897% |
| Zero same-symbol neighbors within 5 seconds | 47.773% |
| At least five same-symbol neighbors within 5 seconds | 0.615% |
| Wallet observed once in validation | 30.380% |
| Wallet observed at least ten times | 37.200% |

The row-level correlation between log hour market-print count and missingness is
-0.550. Minute rates range from 24.757% to 48.039%. Among 56 symbols with at
least ten validation anchors, missingness rates range from 0% to 100%.

The all-validation diagnostic graph has a 2,154-row giant (83.781%). It contains
all 1,366 resolved giant rows plus 706 missing rows, for 32.776% missingness;
outside it, missingness is 40.288%. Missingness therefore correlates with
component structure and liquidity, but not materially with predicate sign: the
sign-rate difference is only 0.068 percentage points.

The data are consistent with missing-at-random only after rich observable
liquidity, symbol, time, density, and wallet-activity conditioning, but that
assumption is not proven. Trade arrival may also depend on latent price movement,
so not-missing-at-random remains plausible. Complete-case inference would be
unsafe. The v1 family-wide mature-missing gate is an adequate fail-closed action
for this run and should remain the default in E.4.1 unless a separate missingness
method is preregistered and validated.

## Candidate-method comparison

| Method | Decision for this evidence |
| --- | --- |
| Current whole-connected-component pairs bootstrap | Reject for inference; components are nonexchangeable and severely concentrated |
| Wallet-only cluster bootstrap | Reject; ignores shared endpoint, transaction, symbol-time, and market-session dependence |
| Event-only cluster bootstrap | Reject; ignores repeated-wallet and market-time dependence |
| Moving/stationary block bootstrap | Potential future method only after stationarity/mixing and block length are independently established |
| Multiway wallet × time/event clustering | Theoretically relevant for nonnested dependence, but current dimensions are too concentrated and support is not established |
| Cluster-robust variance / GEE | Not a rescue; relies on many defensible clusters and a model/working correlation |
| Hierarchical/mixed model | Not a rescue; adds distributional assumptions that the one-hour corpus cannot validate |
| Randomization/permutation | Invalid here because predicate sign was not randomized and exchangeability is unproven |
| m-out-of-n bootstrap/subsampling | Does not repair a wrong sampling unit; may be considered only after a valid block design exists |
| No numerical inference pending independent replication | Required for the current evidence |

Multiway cluster estimators can address nonnested dimensions in suitable large
samples, but not manufacture independent units:

- Cameron, Gelbach, and Miller,
  [Robust Inference With Multiway Clustering](https://doi.org/10.1198/jbes.2010.07136).

## Future data design

Simply adding more anchors from the same dense one-hour process is likely to
grow the percolated component faster than independent information. New evidence
should add independent primary replicates, not merely rows.

Before reading new outcomes, a future E.2.1 design should preregister:

1. Multiple fixed-length market-session blocks drawn across distinct days and
   sessions, with a fixed separation/cooldown justified by an outcome-neutral
   dependence pilot or external market-microstructure evidence.
2. Immutable primary block IDs persisted with membership before feature or
   outcome attachment.
3. One primary block per wallet for the experiment, or a separately justified
   multiway design with adequate wallet and time support. A deterministic
   wallet-cohort assignment is preferable to post-hoc filtering.
4. One anchor per wallet/event within a fixed cooldown, and no splitting of an
   exact transaction or endpoint group across blocks.
5. Outcome-blind event/time stratification and cluster-aware inclusion
   probabilities. Cluster-balanced sampling must change design weights, not
   silently give equal row weight to unequal selection probabilities.
6. A full required-outcome evidence plan. Pre-anchor liquidity eligibility may
   be used only if preregistered; post-anchor resolution or observed hypothesis
   direction must never select anchors.
7. A new untouched test partition separated from every discovery and
   calibration period.

Distinct days/sessions, separated wallet cohorts, and nonoverlapping outcome
exposures are the most useful new evidence. More rows from already dominant
wallets, symbols, or local time bursts are not.

## E.4.1 methodology specification

This is a preregistration design, not an implementation authorization. It must
be finalized and simulation-calibrated before any new experimental outcomes are
read.

### Defect and version boundary

- Defect: v1 mistakes a transitive dependency-graph partition for sampled,
  exchangeable clusters; omits concentration/leverage from support; uses an
  interval shorter than realized outcome exposure; and makes finite Monte Carlo
  output depend on component ordering.
- New identities: `phase-e4-evaluation-protocol-v2`, evaluator code/config v2,
  result/manifest v2, and an explicitly named E.4.1 statistical method.
- Compatibility: E.4.1 must refuse a materialization lacking preregistered
  primary block IDs and cluster-aware sampling design. No v1 row is migrated,
  replaced, or reinterpreted.

### Revised unit and dependence model

- Primary inference unit: one independently sampled E.2.1 market-session block.
- Keep every wallet, exact transaction, endpoint source, and realized
  anchor-to-endpoint exposure inside one primary block.
- If any wallet or exact source crosses primary blocks, fail closed unless a
  preregistered multiway method has adequate support in every dimension.
- Temporal edges use the actual declared exposure contract, including maximum
  resolution lag, not merely the nominal horizon.
- Local graph edges remain diagnostics inside blocks; their transitive closure
  is not treated as a new exchangeable population.

### Estimand and inference

- Preserve the E.2 sampling-weighted predicate-minus-complement mean net-outcome
  estimand and the separate practical-effect gate.
- Express that estimand as the coefficient on the frozen predicate in a
  weighted intercept-plus-predicate regression; this is algebraically the
  weighted difference in means.
- Use a restricted, studentized wild cluster bootstrap-t on primary-block score
  contributions, with one multiplier per primary block. This preserves
  within-block predicate/complement covariance without resampling a giant and a
  singleton as exchangeable pairs.
- Use at least 9,999 deterministic resamples for final commissioning. Derive the
  seed from protocol, proposal, and base seed. Canonically order blocks by their
  immutable block hash before applying random multipliers.
- Report a bootstrap-t confidence interval and two-sided null p-value. Percentile
  intervals are not retained as the primary interval.

This method still requires genuinely independent primary blocks. Wild bootstrap
does not cure dependent blocks, few effective blocks, or extreme leverage.

### Preregistered support and concentration gates

The initial v2 proposal should fail closed unless all are true:

- at least 30 primary blocks;
- at least 20 blocks containing both predicate and complement rows;
- at least 20 total-weight effective blocks,
  `1 / sum_g (W_g / sum_h W_h)^2`;
- at least 20 effective blocks separately for predicate and complement weights;
- no block holds more than 10% of total, predicate, or complement design weight;
- all required outcomes are resolved, unless a separately versioned missingness
  procedure was preregistered;
- finite studentization and at least 99% valid bootstrap draws;
- predeclared leave-one-block-out leverage/influence diagnostics do not exceed
  thresholds established by outcome-neutral Monte Carlo calibration.

The counts 30, 20, and 10% are conservative starting requirements, not claims of
universal sufficiency. Before E.4.1 is frozen, adversarial simulation must show
familywise type-I error at or below the registered alpha across the allowed
cluster-size, weight, group-balance, and dependence envelope. Calibration may
tighten these gates but may not use the two current hypotheses or reserved test
outcomes.

### Multiplicity, replay, and decisions

- Retain the exact frozen family, Holm-Bonferroni denominator, alpha, p=1 for
  unevaluable members, and no survivor-only correction.
- Preserve the practical-relevance rule as a separate, nonweakened gate.
- Canonical block identity, graph relation counts, design weights, support
  metrics, leverage diagnostics, bootstrap multiplier fingerprint, and ordered
  results enter the manifest.
- Equivalent input row ordering must yield identical blocks, multipliers,
  statistics, hashes, and decisions.
- Concentration failure is `INSUFFICIENT_EFFECTIVE_SUPPORT` / `INCONCLUSIVE`,
  never null rejection or support.

### Required adversarial tests

E.4.1 tests must cover every scenario in the review brief. In particular, 95%
and 99% dominant blocks, only three effective blocks, 100× sizes, one-arm
dominance, multiway disagreement, overlap percolation, bridge insertion/removal,
row-order permutation, restart, canonical component ordering, clustered
missingness, and attempts to rescue the current hypotheses must all fail closed
or reproduce invariantly. Holm and old-run immutability tests remain mandatory.

## Closure audit

1. **What created the 1,366-row giant component?** Alternating repeated-wallet
   links and local exact-endpoint/same-symbol time links, closed transitively.
2. **Which edge type contributes most?** Same-wallet has the most pairs and is
   the necessary long-range backbone; cross-wallet temporal/shared-endpoint
   edges bridge wallet groups. Neither alone makes a giant.
3. **Is transitive graph closure scientifically justified?** As a conservative
   no-cross-edge partition, yes; as one indivisible exchangeable bootstrap unit,
   no.
4. **Does the graph exhibit percolation?** Yes; the maximum grows from 1,132 to
   1,366 as the overlap lag increases from 0 to 5 seconds.
5. **Are distant nodes meaningfully dependent?** Local dependence is plausible;
   arbitrary covariance across up to 15 graph steps and 894.499 seconds is not
   established.
6. **What is the correct experimental unit?** A future preregistered independent
   market-session/time-block replicate, not a row or emergent connected graph.
7. **Is the current component definition conservative but valid?** Conservative
   as an envelope, not valid as the observed resampling unit.
8. **Or is it scientifically over-collapsing?** Yes for local chains, while also
   under-clustering 195 realized-exposure overlap pairs.
9. **Is whole-component bootstrap valid with 80.5% mass in one unit?** No.
10. **Are clusters sufficiently exchangeable?** No; size CV is 11.569 and the
    largest-to-median ratio is 1,366.
11. **Is component-count support adequate?** No.
12. **Should concentration enter support?** Yes, total and arm-specific.
13. **What is effective independent sample size?** Not identifiable exactly;
    inverse-Herfindahl mass count is 1.543 as a diagnostic.
14. **Can it be estimated defensibly?** Only under a specified covariance/design
    model; topology alone is insufficient.
15. **Does the contrast remain identifiable?** Algebraically yes, inferentially
    not independently replicated.
16. **Does the giant contain both groups?** Yes, 688 positive and 678 negative.
17. **Are missing outcomes concentrated non-randomly?** Yes, strongly by
    liquidity, symbol, density, time, and graph structure.
18. **Does missingness correlate with predicate membership?** Not materially;
    rates differ by only 0.068 percentage points.
19. **Does missingness correlate with component structure?** Yes; 32.776% in the
    augmented giant versus 40.288% outside.
20. **Does the five-second horizon materially create connectivity?** Yes; local
    lag expansion grows the giant, although exact shared endpoints plus wallets
    already create 1,132 rows.
21. **Is temporal overlap treated too strongly?** Yes when local links imply
    arbitrary component-wide covariance; its interval is also too short for the
    realized exposure.
22. **Are repeated-wallet rows always meaningfully dependent?** Not necessarily
    across well-separated sessions/regimes; approximate independence requires a
    preregistered separation design and evidence, not a power-driven split.
23. **Are shared-event rows always meaningfully dependent?** Exact transaction
    or endpoint sharing is genuine. Anchor source-event IDs are unique; broad
    time coincidence is not a shared event.
24. **Would more same-process rows add information?** Little if they join the
    percolated core.
25. **What future evidence increases independence?** Distinct days/sessions,
    separated wallet cohorts, and nonoverlapping exposures.
26. **Should future E.2 sampling change?** Yes, as versioned E.2.1 cluster-aware
    design; historical E.2 remains frozen.
27. **Can E.4 remain unchanged?** Its historical v1 state can; it must not be
    used for future inference.
28. **Is E.4.1 required?** Yes.
29. **What exact defect justifies it?** Wrong resampling-unit assumption,
    concentration-blind support, incomplete exposure window, and order-sensitive
    finite bootstrap mapping.
30. **What method should E.4.1 use?** Restricted studentized wild cluster
    bootstrap-t over independently sampled primary blocks, after design gates.
31. **Why is it valid?** It retains within-block covariance and does not treat
    unequal emergent components as pairs-bootstrap draws; validity still rests
    on independent, sufficiently numerous low-leverage blocks.
32. **What assumptions remain?** Block independence, valid design weights,
    adequate moments, correct exposure containment, and stable score behavior.
33. **How should severe concentration fail closed?** As
    `INSUFFICIENT_EFFECTIVE_SUPPORT`, with p=1 for Holm.
34. **How should minimum support be defined?** Joint actual-count,
    mixed-support, arm-specific effective-count, maximum-share, leverage, and
    valid-resample gates.
35. **Should ESS be used?** Yes as one diagnostic/gate, never alone.
36. **Should concentration limits be preregistered?** Yes and calibrated without
    current or test outcomes.
37. **Does Holm remain appropriate?** Yes; it controls the frozen two-member
    family after valid member-level p-values exist.
38. **Was multiple testing weakened?** No.
39. **Was reserved test untouched?** Yes; zero new test queries.
40. **Did diagnostics tune E.3?** No; no outcome values were read and predicates
    and thresholds were untouched.
41. **Did diagnostics alter E.4 protocol?** No.
42. **Does original commissioning remain immutable?** Yes, inconclusive.
43. **Can a proposed change rescue original hypotheses?** No; E.4.1 requires a
    new preregistered experiment.
44. **Did Phase D remain frozen?** Yes.
45. **Did E.1 remain frozen?** Yes.
46. **Did E.2 historical semantics remain frozen?** Yes.
47. **Did E.3 historical semantics remain frozen?** Yes.
48. **Did E.4 v1 historical semantics remain frozen?** Yes.
49. **Was prediction/signal/execution/trading authority introduced?** No.
50. **Is E.4 scientifically ready for full freeze?** No. V1 is historically
    frozen but not commissioned for inference; E.4.1 design and independent
    evidence are required.

## Findings

### Critical

- None. The fail-closed missing-evidence gate prevented numerical inference and
  no authority or trade followed.

### High

- Component-count support reports 208 while mass effective support is 1.543.
- Whole-component pairs bootstrap is invalid under the observed leverage and
  nonexchangeable emergent components.
- Transitive local/network connectivity is not a justified indivisible sampling
  unit.
- The v1 temporal edge interval omits actual endpoint-lag exposure.

### Medium

- Mature missingness is clearly not MCAR; complete-case inference would be
  biased or at least unjustified.
- Finite deterministic bootstrap output depends on component ordering even when
  component membership is equivalent.
- Shared-event naming obscures exact semantics: anchor source events are unique;
  shared endpoints and valid transaction hashes are the genuine common events.

### Low

- None added by this review.

### Informational

- All E.2 validation sampling weights are equal, so weight and row-mass
  concentration coincide in this run.
- Exact transaction links are already contained by the frozen wallet/overlap
  graph and do not cause the giant.
- Existing E.4 adversarial tests do not exercise extreme unequal components,
  concentration gates, realized-lag overlap, or row-order invariance.

## Tests and verification

- E.4 adversarial module: **16 passed**.
- Combined E.1-E.4 plus D.6/D.7 targeted suite: **91 passed, 12 subtests
  passed**.
- Full backend: **298 passed, 1 collection error, 41 subtests passed**.
- Sole full-suite error: the pre-existing pytest-9 collection of helper
  `tests/test_copytrade_suitability.py::test_config`; fixture `root` does not
  exist. This is identical to the commissioning finding.
- `python -m compileall -q src tests`: **passed**.
- `python -m pip check`: **passed**.
- Production database read-only `PRAGMA quick_check`: **ok**.
- Database bytes and modification time remained
  `1,901,522,944` and `2026-08-18T06:33:14.0342312Z` after diagnostics.
- `git diff --check`: **passed**.

## Frozen status

- D: **frozen; unchanged**.
- E.1: **frozen; unchanged**.
- E.2: **historical semantics frozen; unchanged**. E.2.1 is only a future
  design recommendation.
- E.3: **frozen; unchanged**.
- E.4 v1: **historical protocol/result frozen and immutable; not scientifically
  commissioned for numerical inference**.
- E.4.1: **methodology revision required; specification not yet implemented or
  preregistered**.
