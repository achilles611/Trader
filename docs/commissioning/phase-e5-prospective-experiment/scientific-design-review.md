# Phase E.5 scientific design review

Date: 2026-08-18

Review baseline: `ee990643ea096e32f97c6177b4b1a165241d05e7`

Companion specification: `e5-specification.md`

Machine protocol: `e5-protocol-v1.json`

## Review conclusion

**E.5 METHODOLOGY ACCEPTABLE FOR FREEZE, NOT AUTHORIZED TO ACQUIRE IN THIS PASS**

The design replaces the retrospective E.4 sampling unit with a prospective primary block: one hash-selected 30-minute market session in each fixed eight-day epoch. Sixty blocks are precommitted across approximately sixteen months, exposure envelopes are separated by at least seven days, wallets are salted-hash cohorted and admitted to only one block, and known wallet/transaction/endpoint/campaign/exposure links cannot cross blocks.

Inference is a restricted, studentized wild cluster bootstrap-t over the actual primary blocks, not graph components. It estimates a common design-weighted within-block predicate contrast with block fixed effects and delete-one-block jackknife studentization. Hard concentration, effective-support, missingness, dependence, maturity, multiplicity, and stopping rules precede outcome release.

This does not prove literal independence. It creates a substantially more defensible approximate-independence design and names the remaining long-regime and latent-coordination risks. If those risks become observed cross-block relations or cause a frozen gate to fail, v1 is inconclusive.

## Assumptions

1. A seven-day minimum separation, unique admitted wallets, nonoverlapping exposure, and fixed scheduling make residual dependence between primary blocks weak enough for cluster-level asymptotics.
2. Known transaction, endpoint, campaign, wallet, and same-symbol exposure relations cover the most important mechanically observable cross-block bridges.
3. The pre-anchor symbol-liquidity adapter can enforce the frozen 172,800-print/24-hour, 3,600-print/30-minute, two-second maximum-gap, no-discontinuity rule using only trade timestamps available at the anchor.
4. Sampling weights correctly represent the preregistered inclusion mechanism and remain within the 5:1 envelope.
5. The common within-block predicate slope is a meaningful descriptive estimand even though heterogeneity across blocks may exist.
6. Finite second moments and delete-one-block fits are adequate inside the preregistered concentration envelope.
7. Complete outcome acquisition is operationally possible. Failure to achieve it makes v1 inconclusive; no MAR/MCAR assumption is substituted.
8. Administrative controls keep raw outcomes behind a separate capability. The application-level audit cannot prevent a privileged operator from deliberately reading the backing store.

## Inferential-choice justification

### Why primary market-session blocks

The E.4 graph showed that local wallet and market relations percolate through dense same-process observations. Adding rows inside one hour did not add independent replication. Distinct, precommitted, widely separated sessions add repeated market realizations. A block contains both predicate arms and all of its internal covariance, so regime shocks are not counted as row-level information.

Eight-day epochs were selected prospectively as a conservative design separation, not because they improve E.4 results. Hash-selecting within the first day of each epoch produces at least a full seven-day cooldown while moving across weekday and UTC slots. Sixty scheduled epochs trade speed for a longer calendar span and at least 40 effective low-leverage contributors.

### Why block fixed effects and a common within-block contrast

A global row-level mean contrast could be dominated by differences in arm composition across market sessions. Block fixed effects remove each session's common level and make identification explicitly within block. The information weight `q_g = W1g W0g/(W1g+W0g)` is the exact weighted binary-regressor information inside block `g`. Its maximum share is therefore a pre-outcome design-leverage gate.

### Why restricted wild cluster bootstrap-t

The method assigns one mean-zero multiplier to every actual sampled block and never treats a graph singleton and a giant component as exchangeable draws. Restriction imposes the null, Webb six-point weights provide a symmetric mean-zero unit-variance finite-support distribution, and per-draw jackknife studentization responds to unequal cluster information. Bootstrap-t intervals use the same pivotal distribution as the test. None of this rescues dependent or high-leverage blocks; those conditions are hard gates.

### Why 9,999 draws and deterministic seeds

At 9,999 draws, the plus-one p-value grid is 0.0001 and Monte Carlo error near 0.05 is small enough for a 0.05 decision without pretending the simulation is exact. The seed is derived from protocol hash, hypothesis ID, and base seed, and blocks are canonically ordered. The exact multiplier sequence and distribution hash replay.

### Why Holm with p=1 retention

The exact two-member family was fixed before outcomes. Holm controls FWER under arbitrary dependence between its valid member p-values and is conservative for these algebraically related predicates. Assigning p=1 to an unevaluable member preserves the frozen denominator and blocks survivor-only multiplicity repair.

### Why zero missingness tolerance

E.4.1 found missingness associated with liquidity, symbol, density, time, and graph position and could not rule out outcome-related trade arrival. E.5 v1 therefore does not claim complete-case validity, fit an outcome-selected resolution model, or add IPW after observing effects. One finally missing observation fails the family. This is operationally demanding but scientifically unambiguous.

### Why the concentration envelope

The 5% block and contrast-information caps guarantee that no primary unit supplies more than one twentieth of total or coefficient information. Effective block counts of 40 make nominal support robust to moderate imbalance. The 48-block minimum leaves room for at most 12 scheduled blocks to be unavailable without changing the fixed horizon, while 40 mixed blocks ensure within-block identification. Symbol, endpoint, local-window, wallet, and component caps stop a different grouping dimension from recreating E.4 concentration. These are conservative prospective bounds exercised by simulations; they are not tuned against historical outcomes and are not asserted as universal constants.

## Known weaknesses and residual risks

- Market regimes or structural exchange changes may persist beyond seven days. A non-rejection of a serial-correlation diagnostic would not prove independence, so the protocol relies primarily on design separation and broad calendar coverage.
- Unknown coordinated wallets may evade campaign metadata. Observed relations fail closed; latent relations remain a limitation of the data source.
- The two predicates are algebraic complements under complete membership. Keeping both is conservative for historical family continuity but adds little distinct scientific content.
- A common within-block slope can conceal effect heterogeneity. The v1 claim is only the frozen weighted average; post-hoc subgroup claims are forbidden.
- Complete resolution may be operationally unattainable. If so, v1 should be inconclusive and a future protocol must redesign the measurement process before new outcomes.
- Fixed schedule dates expose the experiment to major one-off events. Excluding a bad session after it occurs is forbidden; the block remains part of the experiment.
- SQLite triggers and capabilities protect normal application paths, not a malicious privileged administrator. Deployment needs file/role separation and administrative audit.
- Synthetic calibration supports algorithm behavior only. It cannot validate the real-world independence assumption or every distributional tail.

## Synthetic validation plan and acceptance behavior

All synthetic fixtures use a namespace rejected by production admission. No fixture can be persisted as an E.5 production observation.

| Case | Construction | Frozen expected behavior |
| --- | --- | --- |
| Independent null | 48--60 independent mixed blocks, zero common within-block effect | Approximately uniform valid p-values and family type-I error near nominal; no systematic CI exclusion |
| Independent true effect | Same design with a fixed within-block shift | Increasing power, correctly signed estimate, sensible interval |
| Giant-cluster contamination | One block/component or contrast-information unit exceeds 5% | `CONCENTRATION_GATE_FAILED`; outcome reader remains sealed |
| Transitive graph bridge | Alternating wallet and overlapping-exposure edges | One large diagnostic component is detected; a cross-block edge gives `DEPENDENCE_GATE_FAILED` |
| Unequal clusters | Cluster weights/sizes vary inside and outside 5:1 and 5% envelopes | Stable bootstrap inside the envelope; hard failure outside it |
| Few clusters | Nominal/effective blocks below 48/40 | `INSUFFICIENT_SUPPORT`; no inference |
| Severe missingness | Many mature-missing records | `MISSINGNESS_GATE_FAILED` |
| Structured missingness | Missingness tied to liquidity, symbol, time, or density | Same family-wide failure; no correction is fitted |
| Outcome-correlated missingness | Synthetic missing indicator depends on hidden outcome | Same failure without reading outcome values for gate selection |
| Late outcomes | Economic-time event valid but ingestion after final deadline | `LATE`; integrity/missingness failure; record does not reopen |
| Degenerate variance | Constant outcomes or singular delete-one-block fits | `InferenceRefused` |
| Replay | Row order reversed with identical protocol and seed | Identical schedule, gates, results, bootstrap distribution hash, and replay hash |

The targeted suite implements every row above. A commissioning simulation should additionally run at least 500 independent null and effect trials with 999 or more draws across balanced, 5% boundary, 5:1 weight, heavy-tail, and heterogeneous-block scenarios. Acceptance is empirical type-I error no greater than the preregistered 5% target plus a two-standard-error Monte Carlo allowance in every admissible null scenario. Calibration may only tighten v2; it cannot mutate v1 after freeze or use E.4/test outcomes.

## Fifty-question adversarial closure

1. **What exactly is the unit that is claimed to be independent?** One hash-selected 30-minute market-session block in a fixed eight-day epoch, including every admitted observation and its complete anchor-to-resolution exposure.

2. **Why is that independence scientifically plausible?** Blocks are precommitted, separated by at least seven days, spread across about sixteen months and UTC slots, use disjoint admitted wallet cohorts, have no overlapping exposure, and fail if known causal identities cross blocks. This supports approximate independence far better than rows from one dense hour.

3. **What market mechanisms could violate it?** Multiweek regimes, exchange-wide shocks, persistent symbol trends, coordinated wallets without lineage, long campaigns, common liquidity cycles, and changes to venue mechanics can create residual cross-block dependence.

4. **How are repeated wallets across blocks handled?** Salted hash cohorts and first-admitted-block-only semantics prevent them at admission. Any repeated wallet relation detected across blocks fails the dependence gate.

5. **How are persistent positions handled?** Known position/campaign lineage must remain inside one block. Cross-block lineage fails. Unknown latent positions are a named limitation and cannot be repaired by the bootstrap.

6. **How are symbols with strong temporal autocorrelation handled?** The seven-day spacing and block fixed effects reduce short-memory dependence; one symbol is capped at 10% weight and effective symbol count must be at least 12. Dependence persisting past that remains a weakness and can invalidate the independence claim.

7. **How are adjacent market regimes handled?** Blocks cannot be temporally adjacent: exposure envelopes have at least a seven-day gap. The hash schedule moves across calendar and UTC slots and spans approximately sixteen months.

8. **How does the resolution horizon affect dependence?** Dependence exposure runs through the actual qualifying print, as late as anchor plus 10 seconds. Graph overlap uses that full interval, not the nominal five-second horizon.

9. **Could local overlap still transitively join most blocks?** It may join many observations inside a block, which is allowed and concentrated-gated. It cannot cross the seven-day gap by time; any wallet, endpoint, transaction, or campaign bridge across blocks fails rather than redefining clusters.

10. **What prevents one symbol from dominating the experiment?** A 10% maximum symbol-weight share and minimum effective symbol count of 12, checked before outcome release.

11. **What prevents one wallet from dominating it?** One admitted block per wallet, the per-block first-event/cooldown rule, a 1% wallet-weight cap, and cross-block wallet failure.

12. **What prevents one market session from dominating it?** Total, predicate, complement, and contrast-information block shares are each capped at 5%.

13. **What prevents nominal cluster count from overstating support again?** Total and arm-specific inverse-Herfindahl effective block counts, effective contrast-information blocks, maximum shares, symbol ESS, weight-ratio limits, and the mixed-block minimum all gate inference jointly.

14. **What is the effective-support requirement?** At least 40 effective total blocks, 40 in each arm, 40 effective contrast-information blocks, 48 nominal blocks, 40 mixed blocks, and 12 effective symbols.

15. **Why is the chosen threshold scientifically defensible?** It caps any block's coefficient leverage at 5%, retains at least 40 low-concentration contributors, is stricter than the E.4.1 starting envelope, and is calibrated only on synthetic admissible designs. It is a conservative design boundary, not retrospective optimization.

16. **What causes immediate experiment invalidation?** Protocol/hash mismatch, nonprospective or historical rows, out-of-schedule membership, exposure beyond the frozen window, wrong wallet cohort, cross-block dependence, final missingness, concentration failure, protected-data access, or semantic mutation after freeze.

17. **What causes mere delay rather than invalidation?** Only evidence still inside its frozen maturity and ingestion window produces `AWAITING_MATURITY`.

18. **How is mature-but-missing evidence handled?** One such record gives family-wide `MISSINGNESS_GATE_FAILED`; it is not coded as a negative outcome and is not dropped.

19. **How can missingness invalidate the experiment?** Final resolution must be 100% overall and in every block, symbol, liquidity, density, and time stratum. Any final missingness fails.

20. **Are any corrections for missingness allowed?** No. E.5 v1 authorizes no IPW, imputation, complete-case inference, or sensitivity-bound replacement.

21. **If yes, how were they preregistered?** Not applicable. A correction would require a new prospectively frozen protocol and new evidence.

22. **Could the acquisition mechanism depend indirectly on observed outcomes?** The allowed inputs are predictor/acquisition metadata at the anchor; schedule, cohorts, admission, weights, and stopping cannot use outcome availability or effect. Any outcome-derived input is an integrity failure.

23. **Could the UI expose outcomes before freeze?** The protocol forbids UI outcome exposure before inference eligibility, and no UI was added in this pass.

24. **Could logging expose outcomes before freeze?** Ordinary protocol events store hashes, counts, state, and reasons only. Outcome values are not accepted by the design record or event interface.

25. **Could debug tooling expose outcomes before freeze?** The production outcome capability is state-gated and append-only audited. Debug bypass is prohibited; raw administrative access remains a deployment threat requiring ACL separation.

26. **Can a database query bypass the scientific boundary?** A privileged direct query can bypass any in-process Python boundary. Production commissioning must store outcomes under a separate file/role capability and audit administrators. Such a query invalidates protocol integrity.

27. **Can protocol parameters change after first admission?** No. They are immutable at freeze, even before first admission; first admission also forbids replacing the protocol with a semantic successor in the same run.

28. **What happens if the implementation changes while acquisition is active?** The protocol binds a code commit. A semantic implementation change requires v2 and new evidence. Nonsemantic deployment changes must reproduce the same conformance/replay hashes before use.

29. **How does replay bind results to code/protocol versions?** Protocol hash binds code commit and every semantic field. Replay binds protocol, canonical observations, classifications, gate report, graph diagnostics, seed, bootstrap distribution, and ordered results.

30. **Can bootstrap RNG differences change scientific conclusions?** The seed derivation, Webb distribution, draw count, canonical block order, plus-one p-value, and quantile algorithm are frozen. Any RNG/distribution hash mismatch fails replay.

31. **Why is the proposed bootstrap valid for the estimand?** It perturbs independent primary-block score/residual vectors under the restricted null, preserves arbitrary covariance within each block and the paired arm contrast, and studentizes each draw. Validity still requires sufficiently numerous independent low-leverage blocks, which are hard gates.

32. **What happens with very few clusters?** Fewer than 48 nominal or 40 effective/mixed contributors refuses inference and ends inconclusively at the fixed stop.

33. **What happens with one highly leveraged cluster?** A total, arm, or contrast-information share above 5% causes concentration failure before outcome release.

34. **What happens with singular variance?** Nonfinite or zero observed jackknife standard error, invalid leave-one-block fits, or fewer than 99% valid bootstrap draws refuses inference.

35. **What happens with zero within-cluster variation?** A block containing only one arm contributes no within-block identification. Fewer than 40 mixed blocks or inadequate effective contrast support fails; global degeneracy refuses inference.

36. **What happens if Holm family membership changes?** The protocol hash changes and v1 validation fails. The two-member denominator cannot mutate.

37. **What happens if one hypothesis becomes inadmissible?** It remains in the family with correction input p=1. The other member is not given a smaller denominator.

38. **Can optional stopping occur?** No. Effect and p-value monitoring are forbidden, and the schedule has a fixed hard stop with no replacements.

39. **What forces the experiment to stop?** Completion of the 60 precomputed epochs and the `2027-12-25T00:00:00Z` hard stop, followed only by the frozen maturity grace.

40. **Can collection be extended after seeing an unfavorable result?** No. Insufficient support at the hard stop is inconclusive. Any extension is a new protocol and cannot combine with v1 as if preregistered.

41. **Can a failed experiment simply be re-run with tiny methodological changes?** A successor may be proposed, but it has a different version/hash, new evidence, and a transparent relationship to the failure. It cannot silently supersede or pool v1.

42. **How are protocol successors distinguished scientifically?** By schema/version, semantic hash, code/config hashes, timestamps, explicit predecessor reference, and nonoverlapping prospective evidence.

43. **Can old E.4 data accidentally enter E.5 inference?** E.5 requires its prospective observation schema, exact protocol hash, assigned E.5 block, and prospective classification membership. Historical E.4 schemas fail integrity before outcomes.

44. **Can synthetic fixtures accidentally contaminate production state?** Synthetic fixtures use an explicitly forbidden source namespace and temporary control databases. The production adapter must reject that namespace and exact protocol admission validation rejects nonprospective schemas.

45. **Can E.5 alter D trading authority?** No. The module does not import or invoke execution paths and its authority constants are all false.

46. **Can an E.5 result automatically enable a strategy?** No. Even `SUPPORTED` is scientific-review-only and has no prediction, signal, execution, risk, or trading transition.

47. **Are all scientific failure paths fail-closed?** Yes. Integrity, missingness, dependence, concentration, support, variance, bootstrap-validity, family, and replay failures refuse inference or end inconclusively.

48. **Can all key decisions be independently replayed?** Yes, given the same raw evidence, frozen protocol, classifications, and seed. Canonical hashes bind every material stage.

49. **Can we prove zero pre-freeze outcome reads?** The design pass never calls an outcome source. The checked-in protocol records zero, the registry successful-read counter is zero, blocked callbacks are not invoked, and reserved queries are zero. Deployment proof additionally requires separate outcome-store access logs because privileged direct access is outside the Python process.

50. **Would an external statistician consider the resulting experiment prospectively specified rather than retrospectively optimized?** The unit, schedule, calendar horizon, hypotheses, estimand, gates, missingness policy, bootstrap, seed, multiplicity, stopping, failure states, and authority are committed before any E.5 outcome exists. The report exposes residual assumptions and prohibits using E.4/test outcomes for calibration. Subject to enforcing the deployment ACL and exact acquisition adapter, this meets the substance of prospective specification.

## Freeze recommendation

Freeze v1 only after the machine protocol is updated with the implementation commit and its identity validates, the full synthetic/targeted suite passes, database integrity is read-only verified, production bytes and timestamp are unchanged, and zero successful evaluation-outcome reads and zero reserved queries are documented.

After freeze, stop. Prospective acquisition requires a separate authorization.
