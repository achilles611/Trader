# Lane II continuous research Codex handoff

Date: November 8, 2026, America/Denver. Owner: Joseph. Repository: achilles611/Trader.

## Assignment and intended result

Implement and commission a local, continuously scheduled Lane II research service on Atlas. Keep observation, paper evaluation, historical replay, and completed-run analysis moving without paid model calls in the runtime loop. Seek economically meaningful edges and a defensible wallet cohort. Produce explicit evidence for or against funding candidates; never treat activity, infrastructure tests, or selected positive paper results as profitability.

This document is the implementation assignment, not a claim that the service has been installed. The authoring session has GitHub access but no verified Atlas execution surface. Joseph explicitly requested local scheduling and paper/research work. Actual funded transactions are outside this pass; prepare a concrete capital-review packet when qualified. Keep Lane III controls and accounts separate. The new AI workstation is a separate purchase project and is not a dependency of this service.

## Verified starting point

Read [the published October–November summary](https://github.com/achilles611/Trader/blob/d911c8fb56a5ba6de6bc4f14e7ccef0decc34946/docs/operations/LANE_II_WORK_SUMMARY_2026-10_TO_2026-11-08.md). GitHub main was verified at `d911c8fb56a5ba6de6bc4f14e7ccef0decc34946` during preparation.

- November 5 slim BTC5: three hours completed, nine entries/exits, +$2.01603 realized paper P&L, $1.40397 modeled fees, no unresolved positions. This is a small exploratory sample.
- November 7 broad suite: 242 profiles and 90 alternative cohort accounts; partial intervals totaled about 4h56m after transport and CPU failures.
- Follow-up: 38 previously positive profiles plus controls, 42 total. Its only segment failed at approximately 11:35:47 a.m. MST after about 2.5 hours with `TOTAL_HTTP_TRANSPORT_TIMEOUT`. Later clock instability, disk-admission failure, and a stale cohort snapshot remain material.
- `WINDOW_ENDED` did not prove study completion. The last reviewed funded-stage checklist remained unfulfilled; dated read-only account access was not funded commissioning.
- The GitHub tree at this revision has 127 entries and no committed `AGENTS.md`. Many referenced prediction-market modules, receipts, and Windows launchers exist only in the retained local project. The README describes a separate ETH/Hyperliquid starter. Do not run its generic orchestrator as a substitute for Lane II or assume a clean clone contains Atlas's implementation.

On Atlas, locate the actual worktree, read every applicable local AGENTS.md, record HEAD, dirty paths, current owned processes, task registrations, runtime roots, and relevant source/config identities. Preserve uncommitted work. Locate the S17–S23 evidence named in the summary. Reconcile local code against the published description before modifying anything. If files are missing, report the exact dependency and continue independent queue/design work; do not fabricate receipts or overwrite local work with this older remote tree.

## Ownership and pass boundaries

The parent Codex agent integrates and owns shared configuration, runtime start/stop, queue schema, and scheduling. Spawn `beelzebub_admin` as Caelum for requirements review and evidence verification. Have Caelum read the project charter and concurrency protocol and record a dated pass assessment. If the role is unavailable, use a generic subagent with those role files and report the fallback.

Assign bounded, disjoint engineering work to: reliability/scheduler; capture and data integrity; replay/statistics; cohort research; accounting/status. Use existing Tim, Adam, LSC, and Integrity roles only where available and applicable. Do not invent role registration. No competing controllers, duplicate captures, or overlapping heavy benchmarks on Atlas. Role names do not require separate always-running model agents: the steady-state workers are ordinary local programs.

## Runtime design

Reuse the existing `scripts/paper_8h/`, `scripts/overnight_paper/`, `scripts/paper_lifecycle/`, `scripts/paper_archive/`, and `src/lane_ii/prediction_markets/` components where their local contracts support this work. Add a thin coordinator instead of a second execution engine. Proposed names below are design targets, not existing commands.

One shared public-data collector writes ordered, durable events. Read-only consumers evaluate strategies and wallet policies against that same stream. One controller owns each simulated account; replay accounts are isolated from forward-paper accounts. Do not duplicate HTTP collection per strategy. Make capture, evaluation, supervision, accounting, and reporting health separately visible.

Use a durable SQLite job registry on a verified local disk, with transactional claims, unique deduplication keys, leases, heartbeats, bounded retries, dependencies, and append-only transition history. Reuse an existing proven queue if available. Multiple consumers require fencing tokens so an expired worker cannot commit after a replacement claims the job. A single-writer design is acceptable on Atlas; retain WAL/sidecars in archive/recovery operations.

Bound the queue by job count and estimated output bytes; coalesce redundant health/discovery jobs and reserve capacity for closeout, settlement follow-up, and recovery. Persist PID plus creation time, boot identity, executable/source hash, and lease; PID alone never establishes ownership. Capture partition rotation preserves the experiment's cash, inventory, reservations, and cumulative risk. It must not reset accounts. Publish outputs and their sealed receipt atomically before acknowledging a job.

Each job carries: job and parent IDs; type; experiment and segment IDs; priority; creation/start/end clocks; due time; deadline; dependency IDs; attempt count; resource estimate; input manifest/hash; code/config hashes; owner lease/fence; checkpoint; terminal reason; artifact manifest; analysis state. Use an input-hash/type/version dedupe key. At-least-once delivery plus idempotent output is required; do not promise exactly-once process execution.

Separate three dimensions:

| Dimension | Required states or facts |
| --- | --- |
| Execution | queued, leased, running, completed, failed, cancelled, admission-held; immutable attempt history |
| Study coverage | planned and observed seconds, gaps, partial/full designation, contract and source coverage |
| Evidence closure | capture sealed, integrity audited, accounting reconciled or unresolved, analysis published |

An elapsed window is an event, not an execution success. A failed study stays failed even when analysis succeeds. A restarted fresh account gets a new segment and account identity. Stitching segments must retain cash/inventory continuity or explicitly report independent alternatives; never sum independent paper accounts into a claimed portfolio return.

## Schedule and cost policy

Use Windows Task Scheduler to launch one coordinator at startup and recover it if it exits; an independent five-minute watchdog checks ownership and heartbeat. Select the actual local service identity, venv/interpreter, working directory, task XML, log paths, and startup conditions during installation. Do not store secrets in task arguments. Set task overlap to IgnoreNew and add an application lock. Reconcile old Lane II tasks so two schedulers cannot operate the same runtime.

| Trigger | Local work |
| --- | --- |
| Continuous while admitted | Shared observation and the smallest qualified forward-paper portfolio; preserve the existing validated decision cadence |
| Each 60-second coordinator tick | Claim due work, check freshness/resources, renew leases; avoid repeated full-directory scans |
| Every five minutes | Independent vitality check, stalled-job and ownership audit, bounded recovery decision |
| Every hour | Seal a nonoverlapping capture partition, queue completeness/accounting checks and incremental replay; hourly reports remain descriptive |
| Immediately after any terminal run/segment | Queue integrity audit, reconciliation, replay verification, economic analysis, and report; include failures and cancellations |
| Overnight when measured headroom permits | One heavy replay/backtest at a time; pause new heavy admissions when collection or desktop use needs capacity |
| Daily after local closeout | Publish sanitized latest research status and a short decision digest; keep raw/private evidence local |
| Weekly frozen review | Re-rank discovery candidates, review cohort membership and stopped hypotheses, preregister the next confirmation batch |

All routine collection, arithmetic, strategy evaluation, statistical analysis, charts, and templated reporting must run with zero paid LLM/API token calls. Set model enrichment disabled by default and verify the idle/normal runtime makes zero model-provider requests. Do not invoke Codex repeatedly as a polling loop. Optional local models are noncritical and disabled until headroom is demonstrated; no cloud fallback. A manual Codex engineering/review pass may consume the user's existing session allowance. A separate low-frequency ChatGPT evidence check is not Atlas's scheduler and cannot repair an offline host.

Use a versioned, bounded hypothesis registry. Automated sweeps may draw only from preregistered parameter grids with an explicit trial budget and novelty/deduplication check. Programmatic feature/variant generation is allowed in discovery; no self-editing production strategies or automatic relaxation of guards. Put new ideas in a review queue, keep successful configurations frozen, and record negative results so the system does not rediscover them every night.

## Repair and qualify before expanding

First close the November 7 follow-up: resolve the stale cohort snapshot against owned-process and ledger evidence; reconcile the 27 recorded entries and any open holdings; record actual terminal states; seal/audit available artifacts. Missing outcomes remain unresolved, never zero-valued or presumed profitable. Preserve the original failed endpoint.

Repair transport timeout handling with per-request and total deadlines, cancellable work, bounded exponential backoff with jitter, and venue-wide pacing. Retry observations only where safe; preserve gaps and sequence identity. Simulated intents must stay idempotent. The collector must not continue trading on stale books after a network failure.

Retain monotonic scheduling and separately measured wall/exchange/reference clocks. Record calibration samples, dispersion, and accepted bounds. An unstable calibration holds the affected new decisions while allowing safe local accounting/replay. Do not relabel a process-clock workaround as a Windows time repair.

Derive storage admission from measured ingest rate, remaining run time, retention, WAL growth, analysis scratch, and a shutdown/closeout reserve. Identify the current disk and all consumers before changing floors. Reuse verified CAVE archive/recovery tooling only after matching volume identity, manifests, and restore checks; no automatic deletion or movement of active evidence. A disk hold must remain visible without a relaunch loop.

Start with the single slim candidate and its minimum matched controls. Observe all ten existing wallets if the shared collector permits it; evaluate additional policy arms offline. Retain the 42 selected profiles as a frozen historical-selection experiment, but do not assume Atlas can run them plus 90 cohorts continuously. Measure the complete selected pipeline, including journal writes, snapshots, analysis, and actual desktop load. Resource controls must throttle optional analysis first and preserve safe closeout. Never lower an existing reserve merely to pass admission.

Initial recovery policy: at most three transient retries per job, with bounded backoff; persistent transport failures, unstable clocks, CPU/memory/disk holds, or mismatched ownership require a circuit-open state and a concrete release condition. Check recovery eligibility periodically without launching new study accounts. A restart must reconcile prior ownership and state, not erase the failure. Timeouts and counts are configurable, recorded defaults, subject to local evidence.

Separately cap full-process recovery at two attempts per experiment, persisted across coordinator restarts. Use typed retryability, a stable recovery interval, and hysteresis. Deterministic defects and insufficient capacity are admission holds, not transient retry cases. Do not kill unrelated applications or silently remove treatment arms from an active experiment to recover capacity.

## Research and backtest contract

Every experiment freezes its question, candidate/control hashes, universe, point-in-time membership, entry/exit/size rules, costs, horizon, coverage requirements, primary metric, sample-size plan, allowed looks, and rejection rule before confirmation data arrive. Separate discovery, chronological validation, and untouched forward confirmation. Historical positive selection is discovery only. Once a holdout is inspected or a strategy is modified, give the next confirmation a new untouched period.

Replay event time and receipt/availability time, including actual feed delays and metadata availability. Enforce contract identity, exact venue settlement semantics, min quantity, tick size, spread, full-size depth, partial/no fill, fees, exits, liquidity, and capital contention. Keep the Coinbase proxy separate from authoritative settlement. Never invent missing opposite-side quotes. Cash is a control; compare matched timing/sizing controls on identical eligible observations. Train/fit on the past only and purge overlapping horizons across splits.

Report realized net P&L, modeled fees, open basis, unresolved exposure and marks, turnover, fill/rejection rates, max drawdown, exposure time, capacity, latency, participation, sample coverage, and matched-control differences. Stress fees and fills using current observed venue economics, including at least the historical one-adverse-cent-per-leg scenario where applicable; a stress parameter is not a universal cost estimate. Show leave-best-trade/day/market/wallet-out concentration and regime/time-of-day breakdowns. Report gross and net results together.

Do not count correlated wallets, simultaneous profiles, fills within one market, or overlapping windows as independent observations. Use market/day or campaign blocks appropriate to the dependence and a frozen resampling method. Select economically material minimum effect and calculate power/sample duration from discovery variance; retain wide intervals when data are sparse. No magic number of trades establishes an edge. Register every tested variant, control multiple comparisons at the family level, and prevent repeated hourly peeking from becoming an unplanned significance test. Any sequential promotion rule needs a predeclared alpha-spending or always-valid method.

For a candidate to enter capital review: valid complete evidence and reconciled accounting; positive after-cost performance versus cash and the matched baseline on untouched data; adequate independent support under its registered sample plan; uncertainty and multiplicity handled; no unexplained concentration; survival of plausible execution stress at the proposed size; stable repeat performance across the preregistered conditions. If not met, label insufficient evidence, rejected, or further research with the smallest discriminating next experiment. Do not tune the gate after seeing the result.

## Wallet and strategy cohorts

Maintain two registries: source-wallet quality and strategy-policy performance. Reconcile and preserve the ten tracked wallets, including Bonereaper and Bossgirl, using actual canonical IDs from local manifests. Preserve the earlier eight PRIMARY/two PROBATION lineage; do not manufacture addresses or overwrite history. Record active/inactive/source-unavailable states and retain unsuccessful members to expose survivorship bias.

Wallet admission requires attributable activity, causal availability, reproducible IDs/deduplication, correct contract mapping, observable executable destination prices, realistic delay, fees, and size. Visible positions or leaderboard profit are not sufficient fill evidence. Research-only wallets may stay observed while copying is ineligible. Keep international/US venue mismatches held unless their exact equivalence is established; do not route around eligibility or jurisdiction restrictions.

Compare a cash control, frozen existing cohort policy, equal-risk eligible cohort, and one preregistered challenger at a time in forward paper when capacity permits. Use nonnegative evidence-weighted selection only after its weights were fitted on prior data; freeze them for confirmation. Cap concentration by market, direction, correlated wallet cluster, and event window under the applicable paper limits. Correlation and common trade overlap must reduce effective breadth. A useful cohort can be small; filling a roster is not an objective. Challenger admissions and demotions occur at frozen review boundaries, with reason and evidence links.

## Capital readiness and authority

Continuously prepare a capital-candidate packet whenever the evidence gate is met. Include the precise strategy/version, venue and product, sample/provenance, estimated after-cost edge and uncertainty, drawdown/stress, executable minimum size, maximum possible loss, liquidity, account/adapter requirements, recovery/exit controls, and a proposed bounded validation plan for Joseph's decision. Until those details exist, there is no concrete capital action to approve.

A strategy research PASS and an operational readiness PASS are separate. Reconcile the existing shared $25 cumulative paper loss controller and its scope from current source; do not refill a cumulative budget with profits/restarts or multiply it across independent strategy accounts. That historical paper limit is not new live-money authorization. Do not import Lane III's $200 limit. Do not transmit funded orders, change live arming, add trading keys, deposit funds, or increase monetary limits in this pass. This research service must remain useful and keep working when no candidate qualifies.

## Completion pipeline and operator output

On every terminal event, atomically publish a manifest then queue: seal validation → integrity check → accounting/finality reconciliation → causal replay comparison → economic/control/stress analysis → candidate/cohort decision → sanitized summary. Failed captures get bounded partial analysis with missing intervals exposed. Missing dependencies become explicit blocked analysis jobs with a retry/reconciliation policy; analysis failure does not rewrite the study's terminal result. Restarting the coordinator must catch any completed study whose analysis was not yet queued.

Provide a local status surface with source and heartbeat timestamps, collector/controller/report health, current experiment, backlog/oldest age, clock confidence, resources, actual coverage, realized/unresolved accounting, and next job. A process stop does not mean a flat account. Include an operator pause-new-work control, graceful stop/closeout, resume with reconciliation, and an escalation reason. Keep reporting read-only.

Publish a small sanitized status artifact at `docs/operations/lane-ii-continuous-rd/LATEST.md` through the repository's permitted review/publication path. Fields: evidence as-of time, source/config identities, scheduled versus observed coverage, job counts/states, latest study and analysis result, experiment/cohort decisions, open blockers, links to publishable evidence, and next action. Keep stale-data age prominent. Do not publish credentials, account balances, raw/private fills, or local machine secrets. If a write credential/review path is unavailable, write locally and identify the publication block; do not claim remote visibility.

## Acceptance and handback

1. Demonstrate scheduler installation with exported task definitions, interpreter/source hashes, ownership receipts, a real scheduled launch and watchdog observation. Record installed versus actually exercised state separately.
2. Exercise duplicate dispatch, crash after claim, crash after output before acknowledgment, stale lease/fencing, reboot/restart reconciliation, and terminal-event analysis recovery. Verify no duplicate paper intents or duplicate economic reports.
3. Exercise transport outage, stale input, unstable clock, CPU/memory pressure, low disk, writer lock, missing labels, analysis error, and stale UI pointer. Verify the appropriate bounded hold, no launch storm, preserved accounting, and recovery evidence.
4. Reproduce a sealed existing capture with the applicable financial-state parity checks. Audit a fresh short full-pipeline run and automatic report, then an eight-hour run, then a full 24-hour soak. Stop expansion at a failed gate and report partial coverage honestly. The scheduled service may remain installed with held jobs while a specific defect is repaired.
5. Verify zero runtime model calls, no paid cloud fallback, one collector/controller owner per scope, resource headroom under measured load, and a bounded analysis backlog. Confirm that no funded exchange mutation was made by this pass.
6. Return code changes, exact launch/pause/stop/resume/status commands, task names/XML, configuration, passing/failing tests actually run, measured capacities, run manifests, and a dated Caelum assessment. Include an explicit status for implemented, installed, exercised, 8h qualified, 24h qualified, and research profitability. Do not mark the whole assignment complete solely because the scheduler registered successfully.

The immediate first action is local state reconciliation and closeout of the failed follow-up. The first new evidence target is a fully closed prospective slim-plus-controls run with automatic analysis and clean recovery behavior. Expansion follows measured headroom and evidence, not a target count of bots.
