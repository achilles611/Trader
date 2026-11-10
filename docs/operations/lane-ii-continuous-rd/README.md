# Lane II continuous research: implementation and commissioning

This is the local paper research service requested in [PR #3](https://github.com/achilles611/Trader/pull/3). [LATEST.md](LATEST.md) records the observed commissioning state and the result of each gate. Installation, a running process and a completed timer are distinct from qualification.

## What is implemented

- SQLite WAL job registry with persisted count/byte limits, reserved closeout capacity, immutable transition history, input/source/configuration deduplication, monotonic leases, fencing, three transient retries at most, and a separately persisted two-attempt recovery circuit. Output receipts and verified artifact copies precede acknowledgment. Dead-owner reconciliation recovers publication/acknowledgment gaps without starting another paper account.
- A single coordinator, an application lock and independent five-minute watchdog. Worker identity includes PID, creation time, boot identity and executable/source hashes. Unknown or live prior owners block competing work. Optional heavy analysis yields first under measured resource pressure.
- Frozen P13 and P2 source policies with cash as a nonexecuting control, using one existing study collector. Their existing $5/$6 entry budgets differ; this is a sizing comparison, not an equal-risk confirmation. No strategy economics were optimized in this pass.
- Safe observation retries, bounded backoff and pacing, cancellation, an enforced public-data endpoint policy and stale-book rejection. Same-boot process-clock calibration is renewable; uncertainty holds decisions. This does not repair Windows time.
- Streaming capture audit, ledger reconciliation, disposable DB/WAL/SHM recovery, descriptive analysis and sanitized reporting. For prospective replay, the supervisor bridge binds the actual collector exit, PID/creation/boot/executable identity, closed-producer marker and frozen native cold audit. Replay independently rebuilds feed marks, decision admissions, paper risk actions and financial state.
- The collector flushes and seals a four-stream boundary each quarter hour. The offline consumer carries the causal replay, profile accounting and shared paper risk forward through new byte ranges. It publishes an hourly receipt after four verified quarters. On worker restart it reconstructs verified prefixes before accepting a new range.
- Versioned bounded hypothesis and wallet registries. The ten canonical wallets retain their eight PRIMARY/two PROBATION lineage. Stored eligibility expires; no fresh copying qualification or wallet collection is claimed. Capital review requires preregistered untouched evidence, source-bound power, adequate blocks, family correction, execution stress and complete accounting. Descriptive analysis cannot open that gate.
- Local read-only HTTP status, pause/stop/resume commands, daily local digests and frozen weekly review artifacts. No model client or paid cloud fallback runs in the loop. Offline child processes deny network and descendant creation; collector processes receive a reduced environment and public-data-only network policy.

## Prospective commissioning

The short is a single 900-second interval aligned to the five-minute BTC contracts, with P13, P2 and a nonexecuting cash control. Admission requires a hash-bound review, passing tests of the frozen build, retained historical parity, current capacity and an accepted renewable process clock. Storage admission keeps the 1 GiB system floor and 80 GiB runtime floor plus measured/conservative ingest, retention, WAL, scratch and closeout reserves.

P13 and P2 share one durable **$25 cumulative simulation limit** in the international paper campaign. It includes clipped gross losses, fees, open principal, pending or unknown exposure, and exit-fee reserves. Reserve before admitting an intent or amended fill; an unknown commit blocks new exposure. Profits and restarts do not refill the limit. A later segment requires the exact preceding receipt and verified account and risk continuity; balances carry forward. The existing US-market kernel in `src/lane_ii/prediction_markets/campaign_risk.py` stays in its own venue and paper ledger. Lane III is separate.

The qualification record requires all scheduled ticks, shared fresh eligible data for both arms in all three windows, normal sealed exit, raw and causal checks, reconciled flat accounts and the automatically generated report. Zero entries are allowed when the matched opportunities and abstentions are verified. A historic failed run or missing label cannot satisfy this gate. Replay and reports preserve missing intervals and unresolved positions. Multi-hour admission additionally requires qualified hourly incremental replay and accounting; sealed byte partitions alone do not satisfy that requirement.

The terminal pipeline queues seal, integrity, accounting, replay, descriptive analysis, decision and revised reports. A verified raw audit can still support bounded accounting and analysis when the replay bridge fails. A phase assessment binds the final report and its inputs and keeps the genuine study state separate from evidence failures.

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

The frozen CLI's `release-short --root $RuntimeRoot --payload <reviewed-release.json>` registers the exact review, test and historical reproduction bindings. This is a commissioning operation, not a routine restart command. Reuse the same campaign database and manifest when reconciling the service. A missing, changed or unreconciled campaign stays held.

One replacement commissioning segment can be admitted for a reviewed transport repair when the original attempt ended before paper start with zero ticks, unchanged flat accounts, an unforced owned exit and verified audit/accounting. It uses the same campaign and exact predecessor receipt. The failed attempt and its assessment remain intact. This path cannot retry a paper interval that started or chain further replacements.

A sealed completed short can be recovered from a failed offline closeout with the analysis-only `scheduled_closeout_recovery.py` task. It verifies the original failed phase, unchanged capture, unforced exit, and flat campaign; freezes the corrected offline drivers; and generates a new report only after strict replay and full coverage pass. It has no forward job or recurring trigger. The original phase record stays intact.

Task names are `Trader-LaneII-Continuous-Coordinator` and `Trader-LaneII-Continuous-Watchdog`. The preferred startup/S4U registration was denied by Windows. The installed fallback uses the current user's logon trigger and InteractiveToken; it requires that user's logged-in session. It is not a verified unattended boot service. Exported XML and actual scheduled-launch/watchdog receipts are retained locally. The six earlier Lane II tasks were exported with hashes and disabled to avoid competing controllers.

Routine remote publication is not configured: the desktop GitHub connector is used for this reviewed publication, and no GitHub credential was copied into the daemon. Daily reports are generated locally. Automatic remote daily publication remains an explicit deployment dependency.

## Long paper windows

`long_qualification.py` starts from the accepted short and its existing $25 campaign. `install_long.ps1` registers the interactive supervisor, a quarter-hour health check and a dashboard refresh one minute after each check. The local dashboard records interruption times and counts and draws a red X for each event. It retains a snapshot for each phase.

The eight-hour and 24-hour plans have fixed cutoffs. After an interruption, the supervisor checks the original owner, the durable action cursor and the financial tapes before continuing from a proven flat state. A replacement inherits the same campaign and deadline. Gaps in the paper interval block promotion. The 24-hour gate requires a sealed eight-hour assessment, complete hourly replay and accounting, flat exposure, and fresh host admission.

An optional network-isolated worker evaluates one-factor P-model variations on a separately retained tape. Its results are discovery evidence. The prospective qualification keeps its frozen P13 and P2 profiles and cash control.

## Evidence and acceptance

The published status distinguishes implemented, installed, exercised, short qualified, eight-hour qualified, 24-hour qualified and research profitability. Unit/fault tests, actual subprocess recovery tests, manual sealed-history replay, held-service exercise and prospective paper qualification are different evidence classes.

The short qualification is operational evidence. It does not establish a strategy edge or authorize capital use. Expansion follows the sealed phase assessment and the next phase's capacity and continuity gates. Historical failed captures remain unchanged.

For the longer phases, qualify an hourly consumer that binds each journal range to its WebSocket frames, HTTP proofs and profile-ledger ranges. It must carry causal and accounting state across ranges, bound memory and disk alongside the collector, and hold on a missing proof or interrupted owner. A final tail check must match the genuine terminal receipt. Demonstrate split-versus-contiguous parity and delayed, duplicate or missing proof handling before admitting the eight-hour run. The 24-hour run requires the eight-hour qualification and the same campaign continuity.
