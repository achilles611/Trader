# Phase E.4 real-world commissioning report

## 1. Commissioning Status

`SOL REVIEW REQUIRED`

The first verified production-derived chain from D through E.4 was created and
replayed. The real E.3 universe was frozen without outcome access, the E.4
protocol was persisted before evaluation, every member remained in Holm, and
the resulting inconclusive decisions reproduced exactly.

E.4 is not promoted to fully commissioned. Two real-data facts prevent that:

1. All two hypotheses are `INCONCLUSIVE_MISSING_EVIDENCE`. Of 2,571 validation
   anchors, 1,697 have resolved outcomes and 874 have mature-but-missing outcome
   evidence. The frozen contract therefore computes no effect, interval, raw
   p-value, or bootstrap distribution for either member.
2. Observable dependence is extremely concentrated. One connected component
   contains 1,366 of 1,697 resolved validation observations (80.5%), spans 330
   wallets and 120 symbols, and covers nearly the full validation interval.
   The frozen support rule reports 208 components but does not account for this
   component-size concentration. The scientific suitability of whole-component
   bootstrap under this real structure requires Sol review.

No E.1, E.2, E.3, or E.4 scientific method was changed. No hypothesis was tuned,
no support or maturity rule was weakened, and no trading authority was added.

## 2. Real Data Source

- Source: official Hyperliquid HyperCore requester-pays
  `node_fills_by_block/hourly` archive.
- Source object:
  `s3://hl-mainnet-node-data/node_fills_by_block/hourly/20260817/0.lz4`.
- Verified local source artifact:
  `D:\BeelzebubData\source-cache\2026\08\17\hypercore_9142286ba0522de59fcd52a1.lz4`.
- Artifact bytes: 34,145,536.
- SHA-256:
  `0ba4159df0b3761a2cb770ffb3b70d52415845383fe252129b8f05a3b1151466`.
- Proven interval: `[2026-08-17T00:00:00Z, 2026-08-17T01:00:00Z)`.
- Object event extent: `2026-08-16T23:59:59.892000Z` through
  `2026-08-17T00:59:59.767000Z`. D.7 durably records one boundary timestamp
  anomaly; E.2 V2 independently filters and fingerprints the exact interval.
- Coverage: `PROVEN_COMPLETE`, 1/1 hour, fraction `1.0`, zero missing hours,
  zero malformed hours, and per-fill wallet attribution proven.
- Original acquisition required AWS requester-pays network access. This pass
  reused the checksum-verified D-drive source artifact and the production hot
  database; it performed no new source download.
- Persisted official observations: 398,081 wallet fills and 398,457 market
  trade prints, covering 8,080 wallets and 342 symbols before exact E.2 interval
  filtering.
- D.7 coverage records 796,076 normalized observations and 50,944 replay
  duplicates. E.2 V2 independently binds 796,514 retained in-interval rows.
  The 438-row difference reflects ingestion-attempt/duplicate accounting rather
  than E.2 membership ambiguity; E.2 uses its own full-row retained-universe
  fingerprint.
- Production provenance is distinguishable from fixtures by the official S3
  source identifier, source-object checksum, acquisition manifest, parser and
  schema versions, coverage ID, corpus fingerprint, source timestamps, and the
  production hot database path. Fixture tests use temporary databases and do
  not share these identifiers.

### Storage and initial-path audit

- Authoritative production hot DB:
  `E:\Beelzebub\runtime\hot\copytrade.sqlite3` (1,901,522,944 bytes after
  commissioning).
- Cold/source root: `D:\BeelzebubData`.
- The C checkout's YAML resolves to
  `C:\Users\atlas\Documents\Trader\runtime\hot\copytrade.sqlite3`, which did
  not exist at the start of this pass.
- The prior legacy DB
  `C:\Users\atlas\Documents\Trader\artifacts\copytrade.sqlite3` is 7.1 GB and
  contained copy-trading plus Phase-D execution tables, but no `science_*` or
  `phase_e_*` tables. It was never passed through D.6/D.7 or Phase-E
  initialization, and current configuration no longer points to it. That is
  why it contained no usable Phase-E inventory.
- The production hot DB already contained D.7, E.1, and E.2 tables plus two
  completed materializations. This pass added only the E.3/E.4 schema and
  authoritative E.3/E.4 metadata/results to that DB.
- C: had only about 0.86 GiB free; production writes remained on E: (about
  36.7 GiB free) and test tooling was isolated on D:.

## 3. Pipeline Results

### D

- Coverage ID: `coverage-fcbed592520a4335afdf7513ce7a`.
- Bound corpus fingerprint: `corpus-0a4d73730b49ec6e4a3b88c441cd`.
- Corpus observation fingerprint:
  `a133b12fc88987b9c7bee39eda531aeca6649eb122f7a5802cb761a128237d9a`.
- E.2 retained-universe count: 796,514.
- E.2 retained-universe fingerprint:
  `d511fefe0eb45b9d5f65862654bfc65cba153110bd2578a9af16cc39f7751664`.
- The older immutable corpus snapshot `corpus-b492caf3f630bec0f2268966ca95`
  remains in the ledger but is not in the commissioned lineage.

The supplied D.7 replay regression did not reproduce: the targeted suite now
passes it. Verified real D evidence and completed E.2 artifacts existed without
invoking any blocking replay repair, so Phase D remained frozen.

### E.1

- Hypotheses: 2.
- Experiments: 2.
- Accepted E.3-to-E.1 mappings: 2.
- Rejected mappings: 0.
- Experiment IDs:
  `e1-89809069f60c1ffeaf001b75e3dad125` and
  `e1-4aea6b1a72ea5904da678052daca0131`.
- Every mapping binds an immutable definition hash to the exact D corpus and
  E.2/E.3 lineage. No outcome-derived field participated in classification.

### E.2

- Commissioned materialization:
  `e2-5f761a9f987d17003c1a20c2f7b72c12`.
- Status: `COMPLETE`.
- Causal observations: 10,000.
- Unique wallets: 1,682.
- Unique source events: 10,000.
- Unique symbols: 287.
- Anchor span: `2026-08-17T00:00:00.025000Z` through
  `2026-08-17T00:54:54.923000Z`.
- Partition counts: train 5,001; validation 2,571; reserved test 2,428.
- Predictor completeness: 10,000/10,000 for `wallet_action@1`.
- Resolved outcomes: 7,086 total; 1,697 validation.
- Immature outcomes: 0.
- Mature-missing outcomes: 2,914 total; 874 validation.
- Invalid outcomes: 0.
- Membership fingerprint:
  `f56a8ddeb123954397034f3ec02f0613a8b356fe1828f2db9e1e13f3de344bd6`.
- Sampling-design fingerprint:
  `3864cb36bb0f9677cbf35844677522cd0d855152d82ece7afb75bc6afe5c531e`.
- Feature fingerprint:
  `3c451895bb15fb678afbd869f7dee4879489fb7c823996d81f0ab00f1878ae1c`.
- Outcome fingerprint:
  `0260a91c9796e4bb7cf5509f890b0b446ec266f79a48a69ec81f4ed2283de925`.
- Completed-artifact fingerprint:
  `35965007442c1952c11ac6323020aa56667d9ea0dba94354605cec0c990f21d5`.
- Authoritative verify/replay: passed exactly.

### E.3 and E.4

- E.3 generation run: `e3-356d5c5930be6269a235a89f87fdac15`.
- E.4 protocol: `e4p-d6bbe811c7c6f1eb0a1e28c0412e54c8`.
- E.4 run: `e4r-4337330f480a9ec48370170d17ed8625`.
- Full D -> E.1 -> E.2 -> E.3 -> E.4 lineage is present in both E.4 result
  records and the manifest.

## 4. Real E.3 Universe

- Family: `WALLET_ACTION_SIGN_V1@1`.
- Family fingerprint:
  `e370f65d02faa67779ee5a1e18ef71776239640def348050be609871090f6568`.
- Predictor partition: train only.
- Training population: 5,001.
- Candidate predicates considered: 2 (`wallet_action > 0` and
  `wallet_action < 0`).
- Training support: 2,504 and 2,497.
- Missing predictors: 0.
- Raw candidates: 2.
- Semantic duplicates: 0.
- Suppressions: 0.
- Budget-limited candidates: 0.
- Registered family size: 2.
- Threshold provenance: predeclared `SIGN_SPLIT_V1`, semantic zero.
- Outcome reads attempted: 0; outcome reads permitted: false.
- Predictor summary fingerprint:
  `c60a179c23d5315064023e8c250e6a387aa2db8374ab25328c9188b2cc4d6102`.
- Hypothesis-universe fingerprint:
  `39a10913bfcc16562c52e23e1207a4a340188efb0ded0d4a958981cc7a11befc`.
- Manifest hash:
  `9e0e252dac64bb072406ebb4d1d79b90bf238d4c5eb8cd6434b95635808fb963`.
- Replay: E.4's upstream verifier reverified the complete E.3 universe and
  mappings without difference.

## 5. Real E.4 Evaluation

- Protocol version: 1.
- Protocol hash:
  `d6bbe811c7c6f1eb0a1e28c0412e54c8511d1bbad1492e090c9952a29f2a3bcb`.
- Member-list hash:
  `dfb1cabe32d3d1d72aba173650c72b9c704cdb5654942edeaa3fc2b65e3fc42b`.
- Protocol registered at: `2026-08-18T06:20:40.131889Z`.
- Validation interval: `[2026-08-17T00:20:00Z,
  2026-08-17T00:35:00Z)`.
- Outcome: E.2 historical trade-return V2, five-second horizon, first same-symbol
  print from 5 through 10 seconds after anchor.
- Minimum support: 20 observations per arm, 8 components total, 2 per arm.
- Test: two-sided weighted component-bootstrap mean difference, 1,000
  resamples, base seed 17.
- Correction: Holm-Bonferroni FWER, alpha 0.05.
- Practical threshold: 0.001.
- Correction denominator: 2.
- Evaluable hypotheses: 0.
- Unevaluable hypotheses: 2, both `INCONCLUSIVE_MISSING_EVIDENCE`.
- Insufficient-support hypotheses: 0.
- Immature/pending hypotheses: 0.
- Invalid hypotheses: 0.
- Statistical support: 0.
- Practical relevance: 0 evaluated / 0 supported.
- Scientific support: 0.
- Raw p-values: null for both, as required for unevaluable members.
- Holm ranks: 1 and 2; adjusted p-values: 1.0 and 1.0.
- Result hashes:
  `3b910a29fc53ecbc8683b9ea50afbcca0ec741402c459b31de961f320dd00fec`
  and
  `c3503f65b8cd4fcc3cbab8dedfeda3994660a1cf4eeacdcf9aa5e42bf3e310ea`.
- Evidence snapshot hash:
  `4c2d45197bc4ef058f9cd84a83556a3951b0c77e1607fe29c70cbe49c0960b77`.
- E.4 manifest hash:
  `e318c67b6c4c8f4a3dbcfafa50fa1716b8653363c0193ae7533fd3d5bde140a0`.
- Test/holdout queries: 0.

This result is neither support nor null non-rejection. It is inconclusive
because mature required outcome evidence is missing. It has no tradability or
profitability meaning.

## 6. Dependence Diagnostics

The exact frozen E.4 component builder was applied to 1,697 resolved validation
observations:

- Final connected components: 208.
- Singleton components: 151.
- Non-singleton components: 57.
- Resolved observations in non-singletons: 1,546 (91.1%).
- Median component size: 1.
- P90/P95/P99 sizes: 3 / 5 / 7.
- Largest component: 1,366 rows (80.5% of resolved evidence).
- Next-largest component: 11 rows.
- Top sizes: 1,366, 11, 7, 7, 6, 6, 6, 6, 5, 5.
- Naive resolved rows per final component: 8.16.
- Repeated resolved wallets: 236 of 568; 1,365 resolved observations belong
  to repeated wallets.

Construction stages:

| Stage | Components | Largest component |
| --- | ---: | ---: |
| Independent rows | 1,697 | 1 |
| Shared wallet | 568 | 41 |
| Shared causal/outcome source | 309 | 1,132 |
| Overlapping same-symbol five-second windows | 208 | 1,366 |

The shared-source joins are 450 repeated uses of an exact E.2 outcome
observation. The final largest component spans 330 wallets, 120 symbols, and
almost the entire validation interval. No wallet identity was exported.

This proves the implementation detects observable dependence rather than
pretending 1,697 IID rows. It also challenges the frozen scientific contract:
component count alone masks extreme component-size concentration, and a
whole-component bootstrap can be dominated by whether the 1,366-row component
is selected zero, one, or multiple times. Because no bootstrap ran in this
inconclusive family, numerical stability under that structure remains
uncommissioned. Sol must decide whether the current experimental unit,
component support rule, and bootstrap are scientifically valid. Terra made no
method change.

## 7. Determinism

- E.2 authoritative verify/replay: passed; all five fingerprints reproduced.
- E.3 universe: complete and reverified upstream during E.4 registration,
  evaluation, and replay.
- E.4 fresh-process verify: passed with `reproducible: true`.
- E.4 protocol, evidence snapshot, family denominator, Holm ranks/values,
  decisions, result hashes, and manifest hash reproduced exactly.
- Operational timestamps were not part of semantic identities.
- Restart status: passed across separate E.3 generation, E.4 preregistration,
  evaluation, and verification processes. Counts remained one E.3 run, one
  E.4 protocol, one E.4 evaluation run, and one E.4 manifest.
- Real process-death injection: not performed against the authoritative DB.
  Fixture adversarial tests cover rollback.
- Real concurrent-worker execution: not performed after the Sol stop boundary.
  Fixture adversarial tests cover deterministic convergence.
- Database `PRAGMA quick_check`: `ok` (11.625 seconds).

Full provenance verification is operationally expensive. One E.2 verify took
roughly 5.5 minutes; E.3 planning and E.4 trust-boundary commands commonly took
about 10 minutes, and the E.3 run took longer because it performs multiple
upstream checks. This is a throughput/runbook concern, not a scientific result.

## 8. Scientific Integrity Audit

### Provenance

1. **Was the source data genuinely production-derived? YES.** Official S3
   source identity, checksum, manifest, and retained DB evidence verify it.
2. **Can fixture data be distinguished from real data? YES.** Production
   source/database/manifest identities are explicit; fixtures use temp DBs.
3. **Is D -> E.1 lineage complete? YES.** Both E.1 experiments bind the exact
   D corpus fingerprint and definition hashes.
4. **Is E.1 -> E.2 lineage complete? YES.** E.2 binds the D corpus and the E.1
   partition/feature/horizon contract.
5. **Is E.2 -> E.3 lineage complete? YES.** The E.3 run stores all E.2
   fingerprints and exact materialization ID.
6. **Is E.3 -> E.4 lineage complete? YES.** Protocol members/results map every
   proposal through E.1 and E.2.

### Causality

7. **Did E.2 predictors remain causal? YES.** Historical event time, same-split
   feature windows, and the V2 verifier were enforced.
8. **Did unresolved outcomes remain unresolved? YES.** There were no immature
   outcomes; none was converted to negative.
9. **Did mature-missing outcomes remain distinct? YES.** 2,914 total and 874
   validation rows remain explicitly mature-missing.
10. **Did future information enter predictor state? NO.** E.2 replay passed and
    E.4 found zero future-feature/invalid rows.

### E.3

11. **Did E.3 read any outcome values? NO.** Attempted reads: 0.
12. **Did E.3 read outcome missingness/resolution lag? NO.** Outcome relations
    were structurally forbidden by the SQLite authorizer.
13. **Were thresholds fixed from permitted train data only? YES.** The semantic
    zero sign split was predeclared and generation read train predictors only.
14. **Was the complete family frozen before E.4 evaluation? YES.** It completed
    before protocol registration.
15. **Were semantic duplicates suppressed deterministically? YES.** Duplicate
    count was zero; canonical deduplication remained active.

### E.4

16. **Was the protocol persisted before outcome evaluation? YES.** It was sealed
    at `06:20:40Z`; evaluation completed later at `06:33:07Z`.
17. **Did every registered hypothesis remain in the correction family? YES.**
    Two registered, two protocol members, two results.
18. **Did unevaluable hypotheses retain the correct correction behavior? YES.**
    Both retained adjusted p-value 1.0.
19. **Was the Holm denominator exact? YES.** Denominator 2 with contiguous ranks
    1 and 2.
20. **Were support rules unchanged? YES.** Defaults were used.
21. **Were maturation rules unchanged? YES.** Five-second horizon plus the
    frozen five-second maximum lag remained unchanged.
22. **Were practical-relevance rules unchanged? YES.** Threshold remained
    0.001.
23. **Was the statistical methodology unchanged? YES.** No production
    scientific code changed.
24. **Did real dependence components behave within the intended contract? NO.**
    Construction was deterministic, but one component contains 80.5% of
    resolved evidence. This requires Sol review.
25. **Were non-finite/extreme values handled safely? YES.** No invalid or
    non-finite value entered a statistic; no statistic was computed after the
    mature-missing gate.

### Reproducibility

26. **Did identical verified inputs reproduce the same E.3 universe? YES.**
27. **Did identical verified inputs reproduce the same E.4 statistics? N/A for
    numeric statistics; YES for the exact stored evidence classification and
    component counts.** No effect/p-value was contractually evaluable.
28. **Did they reproduce the same Holm ordering? YES.** Ranks 1 and 2.
29. **Did they reproduce the same adjusted p-values? YES.** Both 1.0.
30. **Did they reproduce the same scientific decisions? YES.** Both remained
    `INCONCLUSIVE`.
31. **Did manifests verify? YES.** E.2/E.3/E.4 hashes reconciled.

### Operations

32. **Did restart preserve authoritative state? YES.** Separate processes read
    and verified one set of deterministic artifacts.
33. **Did process interruption preserve atomicity? NOT TESTED ON REAL DATA.**
    The adversarial fixture suite passes rollback tests; no production DB was
    intentionally interrupted.
34. **Did concurrent execution converge where tested? YES IN FIXTURES; NOT
    TESTED ON REAL DATA AFTER THE SOL STOP BOUNDARY.**
35. **Were duplicate scientific artifacts prevented? YES.** Persisted counts
    remained one semantic E.3 run/family manifest and one E.4 protocol/run.

### Authority

36. **Did Phase D remain frozen? YES.**
37. **Did E.1 semantics remain frozen? YES.**
38. **Did E.2 semantics remain frozen? YES.**
39. **Did E.3 semantics remain frozen? YES.**
40. **Did E.4 scientific semantics remain frozen? YES.**
41. **Was prediction authority added? NO.**
42. **Was signal authority added? NO.**
43. **Was execution authority added? NO.**
44. **Were any trades placed? NO.**

### Scientific Interpretation

45. **Were null results accepted without tuning? N/A.** No hypothesis reached a
    null decision; the inconclusive result was accepted without tuning.
46. **Were insufficient-support results accepted without weakening rules? YES.**
    No support threshold was weakened; the stronger mature-missing gate was
    retained.
47. **Were immature outcomes accepted without shortening horizons? YES.** There
    were zero immature outcomes and horizons were unchanged.
48. **Were zero surviving hypotheses accepted as a valid possible result? YES.**
49. **Were any profitability claims made? NO.**
50. **Did commissioning reveal any issue requiring Sol review? YES.** Extreme
    observable component concentration and uncommissioned bootstrap behavior.

## 9. Findings

### Critical

- None.

### High

- **Sol scientific review: extreme dependence concentration.** One component
  contains 80.5% of resolved validation evidence after shared outcome-print
  and overlapping-window linkage. Review the experimental unit, effective
  support semantics, and whole-component bootstrap under this imbalance.

### Medium

- **No real numerical E.4 commissioning evidence.** The frozen mature-missing
  gate makes both members inconclusive, so effects, intervals, p-values, and
  bootstrap stability were not exercised on real data.
- **Verification throughput.** Full retained-universe replay takes minutes per
  trust boundary and materially lengthens operator workflows.

### Low

- **Local operator runtime mismatch.** The C checkout `.venv` is Python 3.10
  and cannot import `StrEnum`; compatible Python 3.12 has no bundled pytest.
  Tests used an isolated D-drive pytest runtime.
- **Configured/legacy DB path ambiguity.** Relative YAML config under the C
  checkout points to a nonexistent C runtime DB while authoritative data lives
  under E. The legacy C `artifacts` DB is unrelated to Phase-E commissioning.
- **Stale acquisition failure text.** The successful `INGESTED` manifest row
  retains a prior retry's failure reason. State/checksum/coverage are coherent,
  but operator diagnostics can be confusing.
- **Known pytest-9 helper collection issue.** `test_config(root)` is collected as
  a test despite being a helper.

### Informational / out of scope

- The formerly known D.7 replay regression passes in the current targeted and
  full suites; no D repair was made.
- No real crash injection or concurrent scientific worker race was attempted
  after the Sol stop condition. Existing adversarial fixtures pass both paths.

## 10. Changes

Repository changes:

- `docs/commissioning/phase-e4-real-world/wallet-action-sign-family-v1.json`
  — provenance/diagnostics artifact containing the exact already-frozen E.3
  control family used for preregistration.
- `docs/commissioning/phase-e4-real-world/commissioning-report.md` — diagnostic
  commissioning and Sol-review record.

External authoritative state:

- `E:\Beelzebub\runtime\hot\copytrade.sqlite3` — E.3/E.4 tables, one frozen
  E.3 family/run/manifest, two E.1 mappings, one E.4 protocol, two results, and
  one E.4 manifest.
- `D:\BeelzebubData\test-runtimes\phase-e4-commissioning-pytest` — isolated
  test-only pytest runtime; not production code or evidence.

Change classification:

- Operational: none in production code.
- Persistence: authoritative Phase-E rows created by existing frozen APIs.
- Diagnostics/provenance: the two tracked report artifacts above.
- Tests: no test source changed; isolated runner dependency only.
- Scientific semantic change: **none**.

## 11. Tests

- E.4 adversarial suite: **16 passed**.
- Combined E.1-E.4 plus D.6/D.7 targeted suite: **89 passed, 12 subtests
  passed**.
- Full backend: **298 passed, 1 collection error, 41 subtests passed**.
- Sole full-suite error: pre-existing pytest-9 collection of
  `tests/test_copytrade_suitability.py::test_config` because fixture `root` does
  not exist.
- The previously known D.7 replay assertion passed; there is no current D.7
  failure.
- `pip check`: passed in both the compatible system environment and deployed
  E-drive virtualenv.
- `compileall`: passed.
- `git diff --check`: passed.
- Production DB `PRAGMA quick_check`: `ok`.
- Frontend: untouched; no frontend tests/build required.

## 12. Frozen Status

- Phase D: **frozen; unchanged**.
- E.1: **frozen; unchanged; real mappings persisted**.
- E.2: **frozen; unchanged; real materialization verified**.
- E.3: **frozen; unchanged; real family generated and persisted**.
- E.4: **provisionally frozen; not promoted; Sol review required**.

## 13. Commissioning Artifacts

- DB: `E:\Beelzebub\runtime\hot\copytrade.sqlite3`.
- Source artifact:
  `D:\BeelzebubData\source-cache\2026\08\17\hypercore_9142286ba0522de59fcd52a1.lz4`.
- Source SHA-256:
  `0ba4159df0b3761a2cb770ffb3b70d52415845383fe252129b8f05a3b1151466`.
- Coverage: `coverage-fcbed592520a4335afdf7513ce7a`.
- Corpus: `corpus-0a4d73730b49ec6e4a3b88c441cd`.
- E.2: `e2-5f761a9f987d17003c1a20c2f7b72c12`.
- E.3 family fingerprint:
  `e370f65d02faa67779ee5a1e18ef71776239640def348050be609871090f6568`.
- E.3 run: `e3-356d5c5930be6269a235a89f87fdac15`.
- E.3 universe:
  `39a10913bfcc16562c52e23e1207a4a340188efb0ded0d4a958981cc7a11befc`.
- E.3 manifest:
  `9e0e252dac64bb072406ebb4d1d79b90bf238d4c5eb8cd6434b95635808fb963`.
- E.4 protocol: `e4p-d6bbe811c7c6f1eb0a1e28c0412e54c8`.
- E.4 run: `e4r-4337330f480a9ec48370170d17ed8625`.
- E.4 evidence snapshot:
  `4c2d45197bc4ef058f9cd84a83556a3951b0c77e1607fe29c70cbe49c0960b77`.
- E.4 manifest:
  `e318c67b6c4c8f4a3dbcfafa50fa1716b8653363c0193ae7533fd3d5bde140a0`.

## 14. Next Scientific Step

Do not promote E.4 or begin E.5 on the strength of this run. Request a Sol
review of the real dependence graph, especially the 1,366-row component formed
through shared outcome observations and overlapping symbol windows. Sol should
determine whether component count is an adequate effective-support measure and
whether the frozen whole-component bootstrap is valid under this concentration.

After that scientific decision, preregister a contract-preserving independent
E.4 commissioning period that can exercise numerical statistics without
post-hoc asset, wallet, horizon, lag, or outcome-completeness filtering. If Sol
validates the current method and a real period successfully commissions it,
then proceed to Phase E.5 longitudinal learning / knowledge ledger.
