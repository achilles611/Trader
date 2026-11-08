# Lane II continuous research: implementation and commissioning

This pass implements the local coordinator requested in [PR #3](https://github.com/achilles611/Trader/pull/3). Commissioning has stopped at explicit admission gates. Installation and a running status page do not establish a completed research run or profitability. See [LATEST.md](LATEST.md) for the dated observed state.

## What is implemented

- SQLite WAL job registry with persisted count/byte limits, reserved closeout capacity, immutable transition history, input/source/configuration deduplication, monotonic leases, fencing, three transient retries at most, and a separately persisted two-attempt recovery circuit. Output receipts and verified artifact copies precede acknowledgment. Dead-owner reconciliation recovers publication/acknowledgment gaps without starting another paper account.
- A single coordinator, an application lock and independent five-minute watchdog. Worker identity includes PID, creation time, boot identity and executable/source hashes. Unknown or live prior owners block competing work. Optional heavy analysis yields first under measured resource pressure.
- Frozen P13 and P2 source policies with cash as a nonexecuting control, using one existing study collector. Their existing $5/$6 entry budgets differ; this is a sizing comparison, not an equal-risk confirmation. No strategy economics were optimized in this pass.
- Safe observation retries, bounded backoff and pacing, cancellation, an enforced public-data endpoint policy and stale-book rejection. Same-boot process-clock calibration is renewable; uncertainty holds decisions. This does not repair Windows time.
- Streaming retained-capture audit, ledger reconciliation, disposable DB/WAL/SHM recovery, descriptive failed-run analysis and sanitized reporting. Study failure, partial coverage, evidence closure and analysis success remain separate facts.
- Byte partitions preserve account state and contiguous complete-line offsets. Prior manifests, payloads, source identity and the preceding source range are verified. Completed partition progress can be reconstructed after a checkpoint crash. Actual hourly capture, incremental accounting and causal replay remain unqualified.
- Versioned bounded hypothesis and wallet registries. The ten canonical wallets retain their eight PRIMARY/two PROBATION lineage. Stored eligibility expires; no fresh copying qualification or wallet collection is claimed. Capital review requires preregistered untouched evidence, source-bound power, adequate blocks, family correction, execution stress and complete accounting. Descriptive analysis cannot open that gate.
- Local read-only HTTP status, pause/stop/resume commands, daily local digests and frozen weekly review artifacts. No model client or paid cloud fallback runs in the loop. Offline child processes deny network and descendant creation; collector processes receive a reduced environment and public-data-only network policy.

## Gates that remain closed

The prospective short-run job has zero attempts. The engineering, paper-scope and replay qualification facts remain false. The current system volume is below the inherited 1 GiB forward-admission floor. Runtime storage admission retains an 80 GiB floor and adds measured/conservative ingest, retention, WAL, scratch and closeout reserves; neither floor was lowered.

The shared $25 cumulative loss kernel in `src/lane_ii/prediction_markets/campaign_risk.py` is bound to its US-market PAPER ledger and venue-specific economics. It charges gross losses, fees, exposure and reservations without replenishing the budget from profits. The retained international P13/P2 counterfactual simulator is a different account/venue scope and is not joined to that kernel. This implementation does not claim that two alternative accounts share that controller. A scope decision and validated integration are required before releasing prospective execution. Lane III and all monetary limits remain separate and unchanged.

The old strict replay adapter requires genuine closure, supervisor exit, source bindings and its original cold audit. A failed November 7 capture cannot be relabeled to satisfy it. The new collector's clock-admission holds also require a versioned replay admission-event contract before fresh-source parity can be qualified. The coordinator records these missing dependencies as holds. It does not fabricate legacy supervisor results or missing labels.

The completed-run graph queues seal, integrity, accounting, replay, descriptive analysis, decision and revised reports. Descriptive analysis depends on accounting rather than successful replay, so failed captures can still receive bounded analysis. Reports are revised when the bound closure-stage state changes. A failed study remains failed.

## Local operation

This thin coordinator depends on retained Atlas components and frozen source/evidence packages. The older remote repository is not a complete replacement for that local installation. Initialization snapshots `scripts/lane_ii_continuous`, `scripts/slim_paper_operator`, `scripts/overnight_paper` and `scripts/btc_next_paper`, binds the interpreter and configuration, and verifies hashes before each process starts. Never replace those dependencies with unrelated README orchestration.

Use the installed runtime and interpreter recorded in the private installation manifest. The exact Atlas commands are in the local operator handback; the portable equivalents are:

```powershell
$RuntimeRoot = '<installed runtime root>'
$Config = Get-Content -LiteralPath (Join-Path $RuntimeRoot 'CONFIG.json') -Raw | ConvertFrom-Json
& $Config.interpreter -I -B $Config.cli status --root $RuntimeRoot
& $Config.interpreter -I -B $Config.cli pause --root $RuntimeRoot
& $Config.interpreter -I -B $Config.cli stop --root $RuntimeRoot
& $Config.interpreter -I -B $Config.cli resume --root $RuntimeRoot
Start-ScheduledTask -TaskName 'Trader-LaneII-Continuous-Coordinator'
```

Pause stops new forward/heavy admissions while closeout/reporting remains available. Stop requests bounded owned-process closeout. Resume reconciles prior leases and preserves circuits, qualification facts and failed accounts; it does not erase risk or open a new account. The local status URL is `http://127.0.0.1:8889/`, with JSON at `/status.json`. The HTTP surface has no action endpoints. Source evidence age remains visible even when the status generator is healthy.

Task names are `Trader-LaneII-Continuous-Coordinator` and `Trader-LaneII-Continuous-Watchdog`. The preferred startup/S4U registration was denied by Windows. The installed fallback uses the current user's logon trigger and InteractiveToken; it requires that user's logged-in session. It is not a verified unattended boot service. Exported XML and actual scheduled-launch/watchdog receipts are retained locally. The six earlier Lane II tasks were exported with hashes and disabled to avoid competing controllers.

Routine remote publication is not configured: the desktop GitHub connector is used for this reviewed publication, and no GitHub credential was copied into the daemon. Daily reports are generated locally. Automatic remote daily publication remains an explicit deployment dependency.

## Evidence and acceptance

The published status distinguishes implemented, installed, exercised, short qualified, eight-hour qualified, 24-hour qualified and research profitability. Unit/fault tests, actual subprocess recovery tests, manual sealed-history replay, held-service exercise and prospective paper qualification are different evidence classes.

No fresh prospective capture, eight-hour run, 24-hour soak, live account action, funded exchange mutation, credential installation or capital promotion is authorized by a passing infrastructure check. Existing historical failures and original files remain preserved. The next experiment is the smallest fully closed prospective slim-plus-controls run after the named admission and replay/scope defects are resolved; only its audited success can support expansion.
