# Phase E.4 closure audit

## Security/scientific-integrity answers

1. **Can E.4 alter an E.3 hypothesis after seeing outcomes? NO.** E.3 tables
   remain trigger-protected; E.4 stores separate protocol/result rows and
   replays E.3 before evaluation.
2. **Can validation/test data alter train-derived thresholds? NO.** E.4 reads
   the exact frozen E.3 predicate and threshold provenance. Test is not read.
3. **Can a hypothesis be added after family freeze? NO.** Protocol members
   insert only during `REGISTERING`; E.3 also prohibits post-freeze proposals.
4. **Can a failed hypothesis disappear from the correction family? NO.** Every
   protocol ordinal must have one result; unevaluable members use correction
   input p=1.
5. **Can missing outcomes masquerade as negative? NO.** Resolved negative,
   unresolved, mature missing, and invalid counts/states are distinct.
6. **Can immature outcomes be evaluated prematurely? NO.** They are pending;
   incomplete E.2/E.3 lineage cannot be preregistered/evaluated.
7. **Can future information enter an earlier partition? NO.** E.1/E.2 enforce
   horizon-contained partitions and causal feature windows; E.4 rechecks
   feature source time and outcome resolution bounds.
8. **Can test data affect hypothesis selection? NO.** E.3 is outcome-blind and
   E.4 evaluates the entire universe on validation only.
9. **Can the holdout be silently reused? NO.** There is no E.4 test-partition
   query operation; every contract/manifest records zero test queries.
10. **Is the correction family exactly reproducible? YES.** It is the exact
    contiguous E.3 universe and has a frozen member-list hash.
11. **Are raw and adjusted values persisted? YES.** Both are stored in result
    JSON and typed SQL projections; unevaluable raw p is null and adjusted p=1.
12. **Is effect size recorded independently of significance? YES.** Weighted
    mean difference and practical relevance are separate from p-values.
13. **Is uncertainty recorded? YES.** Percentile interval, resample counts,
    deterministic seed, and bootstrap-effects fingerprint are stored.
14. **Is insufficient support distinct from rejection? YES.** It is
    `INSUFFICIENT_SUPPORT` / `INCONCLUSIVE`.
15. **Are unresolved outcomes distinct from rejection? YES.** They are
    `PENDING_OUTCOME_MATURITY`.
16. **Are non-IID observations handled defensibly? YES, within observable
    lineage.** Whole connected wallet/event/overlap components are resampled;
    too few components fail closed. Hidden cross-wallet coordination remains a
    persisted limitation.
17. **Can caller-supplied lineage/lifecycle state be forged? NO.** Lineage is
    reconstructed from E.1/E.2/E.3; triggers and full reconciliation reject
    forged projections, hashes, events, ranks, and manifests.
18. **Can an individual wallet become an implicit search target? NO.** E.3
    prohibits identity features. E.4 uses wallets only in-memory for clustering
    and emits no identity.
19. **Do retries/process death preserve results? YES.** Family persistence is
    one immediate transaction; death rolls it all back.
20. **Does concurrent evaluation converge? YES.** SQLite serialization and a
    deterministic run ID converge on one final family.
21. **Do identical inputs reproduce identical decisions? YES.** Replay
    recomputes evidence, bootstrap, Holm ranks/values, result hashes, and
    manifest linkage.
22. **Is E.2 → E.3 → E.4 lineage complete? YES.** Results also retain D source
    and E.1 experiment/definition references.
23. **Did Phase D remain frozen? YES.** No `src/copytrade` or `trader` diff.
24. **Did E.1/E.2/E.3 remain semantically intact? YES.** Their production
    modules have no diff; only package exports gained E.4 symbols.
25. **Does E.4 have prediction authority? NO.** Explicitly false.
26. **Does E.4 have signal authority? NO.** Explicitly false.
27. **Does E.4 have trade authority? NO.** Explicitly false.
28. **Were uncommissioned real-world performance claims made? NO.** Fixture
    behavior is infrastructure evidence only.
29. **Can methodology change after results are visible? NO.** One protocol per
    E.3 run; protocol scientific fields are immutable.
30. **Can practical relevance be confused with significance? NO.** The
    decisions and thresholds are separate; both are required for scientific
    support. A statistically supported tiny-effect adversarial fixture remains
    scientifically unsupported.

## Findings

- Critical: none.
- High: none.
- Medium: no authoritative graph establishes independence among otherwise
  unlinked wallets. The protocol discloses this; observed dependence is
  component-clustered and insufficient observable independence fails closed.
- Low: `tests/test_copytrade_suitability.py::test_config` is a pre-existing
  helper whose name makes pytest 9 collect it as a test with a missing `root`
  fixture. The actual suitability test class passes; Phase E.4 did not modify
  the file.
- Informational/out-of-scope: the supplied D.7 replay regression remains:
  `worker_result["processed"]` is 0 rather than greater than 0. Phase D was not
  changed.

## Commissioning

Real E.4 commissioning did not occur. `artifacts/copytrade.sqlite3` exists but
contains no `phase_e_*` tables, including no verified
`phase_e_materializations`, complete E.3 generation run, or matured causal E.2
outcome snapshot. Tests use deterministic causal fixtures and make no real
performance claim.
