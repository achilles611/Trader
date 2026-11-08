# Lane II Development Summary from October through November 8 2026

Coverage: October 1 through November 8, 2026. Dates use America/Denver unless a UTC timestamp is given. The earliest October milestone located is October 3. Status was reconciled against retained project reports and run receipts on November 8.

Lane II grew from small Kalshi signal prototypes into a prediction-market research and paper-execution system within Beelzebub. The work now includes contract discovery, wallet observation, simulated order management, shared risk accounting, reproducible captures and replays, strategy comparisons, dashboards, resource guards, and recovery tooling.

The main achievement is a much more auditable research process. Several bounded experiments completed and several interrupted captures were recovered and reproduced. The results have not established a durable trading edge or completed funded commissioning. Positive results remain small, concentrated, selected after observation, or limited by incomplete runs. The latest eight-hour follow-up failed early despite reaching its scheduled window-end state later.

This record includes work implemented and tested locally. Publishing this summary does not commit or deploy every referenced implementation. Source and evidence locations are listed at the end; raw captures, credentials, private account observations, and unrelated workspace changes are outside this publication.

## October 3 through 28 prototypes and research foundations

### Initial Kalshi prototypes

On October 3, we prepared the BTC15M proof-of-concept handoff for `KXBTC15M`: YES, NO, or WATCH signals from order-book pressure and sixty-second price momentum, automatic contract rollover, saved signals, and a bounded two-hour observation window. The delivery target was a functioning signal loop within thirty minutes, with no orders or runtime model calls. The handoff itself is the established milestone; it is not proof of a completed local run.

On October 11, we delivered a separate lightweight BTC perpetual paper-bot package targeting `KXBTCPERP1`, with $100 simulated capital, one-contract sizing, stops, targets, timed exits, and a two-hour launcher. Its contemporaneous delivery report recorded fourteen passing tests and a two-minute demo-feed check with fifteen WATCH evaluations and no errors or trades. That report explicitly said it had not yet been launched on Atlas. This perpetual prototype is separate from the later binary-contract research.

### Market structure and whale research

Beginning October 20, research covered Kalshi and Polymarket sports and short-window Bitcoin markets. We screened leaderboard candidates, examined whether attributed trade feeds were actually available, and specified admission rules for a $5 paper follower. Leaderboard profit, visible positions, anonymous market executions, and complete attributable fills were kept distinct.

The October 23 work narrowed to Kalshi Bitcoin. We checked available contract series, built price and settlement studies, and examined delay, spread, fees, depth, and expiry effects. No native five-minute expiry series was verified in that Kalshi catalog; later Polymarket five-minute studies are a separate venue/product history.

The completed October 23–24 eight-hour capture retained 56,190 contract quote observations, 13,390 spot responses, and 1,637,119 distinct anonymous execution IDs. All 42,707 compressed raw HTTP responses passed their recorded hash checks. The principal BTC15M price-model reconstruction made 17 hypothetical entries for **+$80.63**, but became **−$20.87 after removing its best winner**. The hourly rule lost **$24.64** over eight entries. These were price-model simulations, not copies of identified traders. The frozen probability model also scored worse than the market baseline on the matched observations.

The interrupted October 24 Polymarket US study recovered about two hours ten minutes of observations. It exposed substantial cache age and inconsistencies between displayed reference buckets and the declared averaging construction. Matching a last pre-boundary value to settlement did not validate averaging the displayed sixty closes. That distinction later became a central native-reference qualification issue.

We also built forward Polymarket BTC15 wallet studies. The retained second-period design preserved **eight original PRIMARY wallets and two PROBATION additions**, with observation-only candidates kept separate. Six independent policies compared pooled copying, acquisition caps, price premiums, delay, active-window timing, and recent opposite-side activity. The period-two study was stopped at the user's request on October 28; its unperformed payout tail and unresolved holdings were preserved.

### First execution engine and Bossgirl follower

The October 28 standalone Lane II implementation supplied a Polymarket US paper engine with durable intent and order journals, duplicate protection, stale-input rejection, risk limits, cancellation, reconciliation, a kill switch, a CLI, and persistent simulated accounts. The initial implementation recorded 101 passing tests.

The Bossgirl follower added source observation, a durable copy journal, lifecycle scripts, and an explicit source-to-destination equivalence gate. Its empty reviewed mapping manifest kept copies held. An inspected international/US BTC pair used different settlement references: Chainlink BTC/USD TWAP versus the US BRTI construction. A similar market name or time window could not authorize an exact copy.

Evidence: S01–S04.

## October 29 integration into Beelzebub

We bound the standalone research and engine to the Trader workspace, preserved the study methodology and roster, and produced engine/data and console integration plans. The import manifest bound 152 files, including 150 matching original files and two parent bootstrap files. The complete old workspace was separately preserved and restore-verified before its deletion on October 29 at 6:33:52 p.m. Denver, or October 30 at 00:33:52 UTC. The successor import was never treated as the complete archive.

The integrated prediction-market modules added:

- Typed binary-market contracts, source provenance, and separate permissions for research and paper execution.
- A single durable Phase D execution journal, with idempotent intents, order state, reconciliation, and retained ambiguity after an uncertain submission.
- Atomic risk reservations and a paper controller modeling the previously accepted shared $25 cumulative loss ceiling. Profits or restarts do not refill that ceiling.
- Frozen source packages, loaded-source identities, process ownership receipts, and actual crash/recovery exercises.
- A focused prediction console separating research sleeves from actual account information and distinguishing missing or stale observations from zero balances.

The composed budgeted paper path recorded **45 passing tests and seven actual child-process crash scenarios**. The broader failure harness exercised seventeen process stages. These established their stated synthetic and paper recovery behavior; they did not qualify exchange recovery or a funded adapter.

Three research efforts produced distinct results:

| Experiment | Completed work | Result and interpretation |
| --- | --- | --- |
| Two-hour, five-experiment wallet comparison | Tested a $10 acquisition cap, source-age limit, two-wallet agreement, spread limit, and expiry buffer against frozen controls. | The cap reduced exposure; its PRIMARY result was +$8.35 versus +$5.17, but became −$2.13 without the best condition. Agreement produced no trades. No strategy promotion. |
| Six-hour prospective wallet comparison | Eleven arms, including cash, with PRIMARY and PROBATION separate; full causal replay verified 2,468,058 events. | Registered screen failed. Every copying alternative lost in both populations. PRIMARY zero-premium treatment lost $125.05 versus $101.51 for its control, with unresolved basis retained. |
| Six-hour US-native observation | Captured 25 contracts and 6,672 admitted books, with separate receipt, cache, source, reference, and settlement observations. | Capture integrity passed. Only 839 books were both two-sided and within the study's source-age threshold. Rollover discovery gaps and reference semantics remained unresolved. |

These dollar amounts describe independent simulated accounts. They are not actual losses or additive portfolio returns.

Account tooling progressed from a GET-only qualifier through local credential-format and signing diagnostics. On October 29, a separate masked-input check completed **three authenticated GETs and zero exchange mutations**. Sanitized evidence established a dated schema observation. Stable principal binding, trading permission, runtime credential provisioning, and funded commissioning remained separate gaps. Private account values and credential material stayed outside the research publication.

Evidence: S05–S08.

## October 30 and 31 data quality and native strategy qualification

### Collection and quantitative research

The sports wallet capture closed with 22 frozen wallets, 8,003 forward observation fingerprints, and 7,913 sports-typed observations after metadata lookup. Twelve wallets showed activity. This was an observation study: fingerprints were not promoted to verified fill IDs or follower P&L.

The four-hour international/US BTC and sports capture retained **55,969 events, 19,258 HTTP records, and 9,845 books**. Its retained-data audit passed, while strict temporal compliance remained held because one metadata item expired before consumption and actual dispatch spacing had not been recorded.

The subsequent quantitative project combined primary-source methods, review of five supplied trading clips, empirical quote audits, binary probability and Greeks calculations, and synthetic portfolio-risk illustrations. It tested paired-token underrounds, delayed repricing, depth imbalance, related sports contracts, and wallet overlap. Of 32 gross BTC underround rows, six conditional bounds remained positive after captured fees; **none survived one adverse cent per leg**, and usable later repricing produced no fee-positive persistence. Small price associations did not establish executable returns.

### The A through G development program

We organized the remaining work into seven explicit stages and implemented several offline waves:

| Stage | Work delivered | Remaining boundary |
| --- | --- | --- |
| A — Inputs | Measured application transport-entry/end clocks, pacing, metadata revalidation, cutoff quarantine, cancellation-race repairs, and bounded public qualification. | Complete fresh coverage and sustained rollover behavior were not established by the short qualifier. |
| B — Reference | Exact identity and averaging checks; comparison of one-second and one-minute representations; retained settlement and label contracts. | Public reference representation and authoritative finality remained unresolved. |
| C — Model | Wallet-free native provenance, arithmetic averaged-binary benchmark, physical calibration/scoring tools, deterministic feature-to-fit-to-forecast producer. | Synthetic demonstrations and research scores do not supply a qualified native forecast. |
| D — Confirmation | Frozen experiment definitions, controls, chronological membership, eligibility ledgers, and an economic evaluation harness. | Untouched, adequately supported after-cost confirmation remained outstanding. |
| E — Controls | Offline native paper-service composition, shared reservations, reconciliation states, disarmed fault cases, and transport-recovery prototypes. | Synthetic lifecycle acceptance did not commission a funded service. |
| F — Visibility | Typed native paper projections, React console components, research status views, and explicit stop/cancel/flatten capability distinctions. | A stopped process or empty order display does not prove a flat account. |
| G — Commissioning | Recorded the account, adapter, controller, and operational acceptance requirements. | Funded activation remained held. |

The fourth wave exercised five genuine offline jobs and recorded 105 focused passing tests. Its retained-data readiness audit reproduced features for seventeen selected native BTC contracts but admitted zero training rows under the frozen evidence policy. A separate synthetic logistic fit reproduced deterministically; its illustrative score was worse than the midpoint baseline. The dated original acceptance checklist remained **8 of 39**, including **0 of 5 funded-stage criteria**. Later paper activity should not be mistaken for retroactive closure of that checklist.

### Native night capture and sports combinations

The October 30–31 native BTC15 night capture stopped on a total-request timeout after **2h43m44s**, about one-third of its planned interval. Its 11,276 events and 4,018 raw artifacts passed retained-data checks. Missing labels and incomplete coverage prevented empirical performance evaluation. The failed interval was retained rather than reported as a completed night.

A separate Kalshi NFL combination experiment sought all 495 eight-game groupings, comprising 126,720 tickets. This was a quote-only RFQ workflow: it did create and reconcile owned RFQs, but quote acceptance and orders were disabled. The retained final audit covered **7,358 quotes and 24 complete groups**; every complete group failed the hedge screen before fees. The other 471 groups remained incomplete. Request-budget accounting, expiry reconciliation, and clean shutdown were developed alongside the experiment; the temporary key was later revoked and its local encrypted copy removed.

A smaller Kansas City/Las Vegas/Pittsburgh candidate was calculated separately with a thirteen-cent all-in research watch ceiling. Without an actual executable combo quote, quantity, and fee evidence, it remained a research candidate. No guaranteed hedge was established.

Evidence: S09–S12.

## November 1 and 2 strategy families and comparative paper runs

We moved from isolated BTC policies to a common observer and multiple independently accounted strategies on five- and fifteen-minute Polymarket contracts. The probability display used a Coinbase-based proxy and remained uncalibrated. Its visual estimates were not proof of the venue's exact settlement probability.

The initial family included prior-close impulse, thirty-second confirmation, reversal capture, volatility continuation, and late convergence on both horizons. Later work added Patient Pullback and Conservative Convergence, replaced the P3 variants with Range Acceptance and Value Recovery, and developed selective Homerun variants. Entry timing, persistence, confluence, stake, spread/depth participation, targets, stops, and fee friction became separately identifiable comparison dimensions.

We corrected settlement parsing that had left finalized outcomes unrecognized, preloaded metadata to improve opening participation, and investigated the difference between a raw bid-price stop and a loss after fees and delayed execution. We also distinguished a doubled total payout from a 200% net-profit target.

| Run or analysis | Verified result | What it established |
| --- | --- | --- |
| Overnight paper run ending November 1 | Fifteen-minute four-entry result corrected to −$2.41; five-minute entries retained unresolved labels. | Accounting correction and audited closure, not a profitable strategy. |
| Repaired two-hour BTC15 test | Seven entries and exits, four wins and three losses; +$0.22 net after $1.78 modeled fees. All seven later entries met the thirty-second rule. | Improved opening timing; a very small, cost-sensitive result. |
| November 1–2 twelve-hour strategy research | Complete actual-night outcome audit covered 993 trades. | Enabled separate direction, exit, fee, and stop analyses without rewriting original decisions. |
| Common-feed full-roster replay | Original twelve: 992 entries and −$707.77; final revised twelve: 73 entries and −$38.64. | Lower participation greatly reduced losses on already observed data. The replay differs from actual historical launch timing and is not fresh validation. |
| November 2 morning five-hour run | 41 closed strategy trades, descriptive profile sum −$8.90; displayed cohort policy −$94.68 after separate settlement adjustment. | Completed prospective observation with costs and final accounting. It did not support broad sizing increases. |
| November 2 evening five-hour comparison | 221 strategy entries and cash exits. Earlier-only +$2.84 versus controls −$12.93; matched $10 cohort cap +$7.28 versus uncapped −$44.92. | The timing advantage depended on one Homerun trade. Additional signal relaxation did not improve on earlier-only. |

The P5/Homerun audit checked original entries, full-quantity exit depth, costs, settlement counterfactuals, and a fixed target grid. Correct eventual direction was not enough to make expensive entries profitable. Holding selected historical trades to expiry was explicitly kept separate from a strategy replay that would also change later cash availability and entries.

Wallet analysis retained all ten tracked wallets, including Bonereaper and Bossgirl. Bonereaper's selected paper copies lost before fees as well as after them, so fees alone did not explain the loss. An inverse-copy feasibility audit found insufficient causally available opposite-side quotes for a complete replay. Bossgirl sizing remained limited by sparse activity and minimum-order constraints; a wallet's historical reputation did not substitute for a fresh copied-trade sample.

The UI evolved through prediction and native-paper components, a proxy-probability observer, per-profile charts, cohort views, realized and unresolved accounting, and explicit as-of timestamps. Viewer refresh was tested separately from the worker's decision and snapshot cadence. Some native screenshot checks remained unavailable; those passes recorded their actual layout and runtime checks rather than claiming screenshot acceptance.

Evidence: S13–S16.

## November 3 and 4 capacity recovery and offline replay

The comparison framework expanded to **240 strategy profiles and 240 cohort accounts**. Those counts describe independent paper alternatives, not 480 distinct algorithms or a single funded portfolio. Resource limits became a practical constraint:

| Attempt | Observed endpoint |
| --- | --- |
| Ten-hour run on November 3 | CPU guard stopped it after 5h47m39s. The closed partial report retained all 240 profiles, with 690 entries and cash exits, and separately analyzed the cohort. |
| Five-hour qualification on November 3 | Stopped after about 2h17m because available memory stayed below the 4 GiB reserve. The capture was audited and recovered as partial. |
| Subsequent Atlas night run | Stopped after about 2h46m at the 2 GiB emergency memory floor. Sixty strategy profiles and 240 cohort accounts were reproduced in offline replay. |
| Later November 4 five-hour attempt | CPU guard stopped the run; the responsible process was not established. |
| User-paused two-hour attempt | Preserved a `CLOSED_PARTIAL_RECOVERED` report; it did not complete the requested interval. |

The longer November 3 partial comparison did not confirm the earlier timing result: earlier-only was nearly unchanged, and relaxed signal, persistence, and confluence arms lost more. Cohort caps reduced fees and peak basis but also reduced realized profit in that sample. Their overall benefit remained a research question.

Reliability work separated worker failure from supervisor/publication failure. We added atomic status publication, bounded Windows file-lock retries, monotonic timing, resource history, owned-process cleanup, and recovery that requires matching receipts, source identities, collection closure, and capture integrity. Recovery reports preserve open positions and partial duration. A fresh-account continuation is separately identified; it does not reconstruct the original account automatically.

Offline adapters reproduced **60 of 60 strategy profiles and 240 of 240 cohort accounts** on a sealed partial capture, including the retained financial events, cash, fees, inventory, unresolved basis, and terminal receipts. Subsequent friction and Bossgirl sizing comparisons used four nonoverlapping verified captures. They exposed useful tradeoffs, including missed minimum-size fills at smaller stakes, without establishing an optimal stake or a universally superior friction threshold.

Capacity experiments compared worker counts, ordered SQLite persistence, stateful profile evaluation, snapshot production, and dashboard rendering. A 240-profile stateful test met its one-hertz compute deadlines, but memory reserve failed under concurrent desktop/game load. A 480-profile short test had little timing margin; 960 exceeded the tested latency target. The earlier sixty-profile full-pipeline result and these component tests were kept distinct. No sustained higher-count deployment was certified from a short benchmark.

We implemented future completed-run archival and lifecycle coordination for CAVE: full file membership and hashes, SQLite sidecars, Windows metadata, copy verification, disposable-copy recovery, and a reversible relocation path. Native fixture testing succeeded, including recovery of data dependent on a retained WAL. This established the archive mechanism; it did not mean every historical run had been archived or that a background archiver was installed.

A separate cloud/offline research packaging path was prepared. Its single setup request returned a credit-exhaustion error; no model completion, Batch submission, or Agents execution resulted. API work was subsequently paused. It supplied no strategy results.

Evidence: S17–S20.

## November 5 through 7 slim operation and scheduled recovery

### Short Day and the slim runner

We introduced the Short Day P13 variants, then reduced the workload to a single BTC5 profile derived from the optimized P2 signal. The slim implementation removed unnecessary forecast and separate-viewer processes, used an embedded read-only dashboard, evaluated each second, and published at five-second intervals. Its $5 all-in sizing, fees, depth, and stop rules remained explicit.

The initial DOWN-only restriction was later removed at the user's request. The subsequent qualification and main run admitted the model's native Up and Down signals; historical ledgers were not relabeled.

The November 5 three-hour slim run **completed and passed its retained-capture audit**. It made nine entries and nine cash exits, with five Up and four Down entries, **+$2.01603 realized paper P&L**, **$1.40397 modeled fees**, and zero unresolved positions. Its audit checked 115,665 events and 9,371 raw HTTP files. Nine trades do not establish durable profitability.

The November 6 one-day slim-model-plus-ten-wallet attempt introduced a combined owned-process envelope and explicit capture/storage budgets. It stopped on `SLIM_HOST_CPU_PERSISTENT_5S` at 2:10 p.m., about 4h39m into the requested twenty-four hours. That was a failed-duration experiment, not a completed day.

### Eight-hour suite and positive-profile follow-up

The November 7 runner added Windows Task Scheduler execution and five-minute vitality checks, with bounded recovery and fresh-segment identities. The broad suite contained **242 profiles**—the 240-profile family plus two P13 variants—and **90 simulated cohort accounts** across ten wallets and nine policies.

An approximately 9.2-second host-clock discrepancy had initially caused venue timestamps to be rejected. A monotonic-anchored paper-process clock and new qualification restored fresh-book admission without claiming that Windows system time had been repaired. The broad run then encountered a transport timeout, continued in a separately identified fresh segment, and stopped on its CPU guard. Its two retained study intervals total about **4h56m**; the requested eight hours were not completed.

The next selection froze **38 profiles with positive realized results from those partial segments**, plus matched comparators: **42 profiles total**, including four additional controls and four controls already in the positive set. The cohort remained at ninety accounts. This was explicitly selection from a partial prior sample, with fresh virtual accounts and unchanged strategy definitions.

The November 8 review of that follow-up found:

- The intended window was November 7, **9:05:38 a.m. to 5:05:38 p.m. MST**.
- The sole study segment closed **FAILED** at approximately **11:35:47 a.m.**, with `TOTAL_HTTP_TRANSPORT_TIMEOUT`, 27 recorded entries, and about two and a half hours of study coverage.
- The operator recorded an **unstable NTP observations** exception at 12:06 p.m.
- A later recovery admission was held by the **disk admission floor** at 5:01 p.m.
- The last vitality record said `WINDOW_ENDED` but still embedded the earlier failed study and a stale cohort `RUNNING` snapshot. That snapshot is not evidence that the cohort was currently running or that either pipeline completed eight hours.

The follow-up therefore remains an incomplete experiment requiring failure and accounting closeout. Its selected positive profiles have not been prospectively confirmed by a completed eight-hour result.

Evidence: S21–S23.

## Current conclusions and remaining work

| Area | Established progress | Remaining work |
| --- | --- | --- |
| Research infrastructure | Typed contracts, preserved raw responses, hash-linked journals, frozen configurations, bounded runs, and reproducible replay paths. | Close incomplete captures and preserve clear distinctions between full runs, partial runs, and selected subsets. |
| Strategy economics | Multiple completed comparisons and detailed attribution of fees, timing, exposure, stops, and concentration. | Demonstrate a frozen candidate on untouched observations with sufficient independent conditions and realistic stressed costs. |
| Exact venue semantics | Cross-venue mismatch checks and explicit reference, outcome, and availability contracts. | Resolve applicable native reference/finality gaps and verify exact contract identity before exact copying or native commissioning. |
| Reliability and capacity | Real failure evidence, resource guards, bounded recovery, process-clock correction, and short capacity measurements. | Resolve transport, clock-calibration, disk-admission, and long-duration headroom failures; qualify the whole selected pipeline. |
| Accounting and UI | Separate realized results, fees, open basis, unknown marks, alternative accounts, and dated snapshots. | Reconcile remaining incomplete endpoints; prevent stale status pointers from implying current health or completion. |
| Funded execution | Paper risk and recovery machinery plus a dated read-only account check. | Complete actual account/principal, permissions, funded adapter/controller, operational, and strategy qualification. No reviewed result establishes funded readiness. |

The next evidence milestone is a fully closed, audited prospective run of a frozen candidate with its controls. A successful launch, a selected positive subset, an integrity PASS, and a window-end timestamp each establish different facts; none alone establishes profitable or reliable trading.

## Evidence index

Paths below identify the working project's retained evidence. Many implementations and reports are still local and are not available as GitHub files on the default branch. `operations/` below means `docs/operations/kalshi-lane-ii-integration/`; `runtime/` means the retained Beelzebub runtime evidence store. These are source identifiers, not links to public datasets. This summary does not republish private account data or raw captures.

- **S01 — Early prototypes:** project chat **lane II**, October 3 BTC15M handoff and October 11 BTC perpetual package delivery. The fourteen-test/demo result is attributed to that delivery record; an Atlas launch is not inferred.
- **S02 — Initial market research:** `integrations/kalshi/source/WHALE_COHORT_RESEARCH.md`, `BTC_ONLY_RESEARCH.md`, `BTC_8H_RESULTS.md`, and `POLYMARKET_US_RESULTS_ANALYSIS.md` in the same directory.
- **S03 — Original cohort and engine:** `integrations/kalshi/source/CURRENT_STUDY.md` and `LANE_II.md`; retained `PredictionMarketResearch/polymarket_btc15_period2` and `PredictionMarketResearch/lane_ii/bossgirl` evidence.
- **S04 — Integration design:** `operations/PLAN.md`, `ENGINE_AND_DATA_PLAN.md`, `CONSOLE_UI_PLAN.md`, and `integrations/kalshi/manifest.json`.
- **S05 — Preservation and retirement:** `operations/retirement/RETIREMENT.md` and its referenced final preservation/deletion receipts.
- **S06 — Paper integration and first experiments:** `operations/execution/EXECUTION_STATUS.md` and `COMPLETION_REPORT.md`.
- **S07 — Prospective and native observations:** `operations/viability/PROSPECTIVE_SIX_HOUR_REPORT.md` and `US_NATIVE_SIX_HOUR_REPORT.md`.
- **S08 — Account and risk progression:** `operations/viability/STATUS.md`, `PMUS_ACCOUNT_QUALIFICATION_REPORT.md`, and corresponding dated reviews under `docs/administration/caelum/passes/20261029-*`. The later successful GET receipt supersedes earlier preparation-only account status.
- **S09 — Sports and contract collection:** `operations/multi-venue/SPORTS_TWO_HOUR_REPORT.md`, `four-hour-contracts/STATUS.md`, and `FOUR_HOUR_CONTRACT_REPORT.md` in that latter directory.
- **S10 — Quantitative methods:** `operations/quant-research-20261030/RESEARCH_REPORT.md`, `EMPIRICAL_AUDIT.md`, and `METHODS_AND_SOURCES.md`.
- **S11 — A through G engineering:** `operations/phases-ag-20261030/FIRST_WAVE_REPORT.md`, `phases-ag-wave2-20261030/WAVE2_REPORT.md`, `phases-ag-wave3-20261030/WAVE3_REPORT.md`, and `phases-ag-wave4-20261030/WAVE4_REPORT.md`.
- **S12 — Night failure and sports combinations:** `operations/night-run-20261030/FAILURE_REPORT.md`, `offline-action-pass-20261031/PASS_REPORT.md`, `targeted-three-leg-20261031/STATUS.md`, and `kalshi-key-retirement-20261031/RETIREMENT_REPORT.md`.
- **S13 — Early November paper and UI:** `operations/btc-paper-night-20261031/STATUS.md`, `btc15-early30-tpsl25-20261101/STATUS.md`, and `btc-ten-profile-12h-20261101/STATUS.md`.
- **S14 — Strategy and outcome revisions:** `operations/btc-next-paper-20261102/FULL_OUTCOME_AUDIT.md`, `analysis/P5_DEEP_AUDIT.md`, `homerun-target-study/REPORT.md`, `full-roster-backtest-v6/REPORT.md`, and `FIVE_HOUR_STATUS.md` within that study directory.
- **S15 — Completed five-hour results:** `operations/btc-next-paper-20261102/results-review-20261102/REPORT.md` and `btc-comparison-recovery-20261102/results/REPORT.md`.
- **S16 — Wallet attribution:** `operations/btc-comparison-recovery-20261102/results/bonereaper-analysis/FEE_FINDINGS.md` and `INVERSE_FEASIBILITY.md`.
- **S17 — Partial runs and recovery:** `operations/overnight10h-20261103/FINAL_PARTIAL_REPORT.md`, `five-hour-qualification-20261103/REPORT.md`, and retained November 4 control-directory partial reports.
- **S18 — Replay:** `runtime/overnight10h-controls/20261103/night12h-operations-20261104T030000Z/replay-pass/REPORT.md` and `runtime/overnight10h-controls/20261104/replay-fees-sizing/REPORT.md`; implementation under `scripts/overnight_paper/replay_*.py`.
- **S19 — Capacity:** `operations/atlas-throughput-20261103/THROUGHPUT_REPORT.md`, `local-stateful-capacity-20261103/RESULTS.md`, and their detailed test receipts.
- **S20 — Archive and cloud preparation:** `operations/cave-cloud-20261103/STATUS.md`; `scripts/paper_archive/`, `scripts/paper_lifecycle/`, and `scripts/paper_cloud/`.
- **S21 — Slim work:** `operations/short-day-20261105/SEGMENT_1_REPORT.md`, `short-day-slim-3h-20261105/README.txt`, `DIRECTION_UPDATE.json` and `CURRENT_RUN.json` in that latter directory. The pointed-to `main-control/FINAL_SUMMARY.json` and `MAIN_REVIEW.json` verify the completed three-hour accounting and audit.
- **S22 — One-day attempt:** `operations/20261106T162838Z-slim-day-restart/README.txt` and `CURRENT_RUN.json`; authoritative retained `main-control/STATUS.json` records the later CPU failure rather than the launch pointer's earlier PAPER state.
- **S23 — November 7 attempts:** `runtime/paper-eight-hour/20261107T101726Z-atlas-paper8h` and `runtime/paper-eight-hour/20261107T160000Z-atlas-positive-paper8h`. Reviewed records include both initial study receipts; the follow-up `PROFILE_SELECTION.json`, `WINDOW.json`, study `receipt.json` and `COLLECTION_CLOSED.json`, `OPERATOR_ERROR.json`, `ADMISSION_HOLD.json`, and `VITALITY.json`. Implementation is under `scripts/paper_8h/`.

The implementation also resides in `src/lane_ii/prediction_markets/`, the `scripts/lane_ii_prediction_*` entry points and subsequent paper-run packages, their tests, and `control-center-ui/src/PredictionConsole.tsx` and `NativePaperConsole.tsx`. Historical test counts above belong to their dated packages and are not a claim that the entire current dirty workspace was re-tested for this documentation publication.
