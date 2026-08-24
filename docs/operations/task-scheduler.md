# Task Scheduler

BeezConsole's **Task Scheduler** is a durable local operations subsystem. It
stores its own SQLite database, `beelzebub_operations.sqlite3`, beside the
active application database. It does not reuse scientific or discovery jobs.

Open **Task Scheduler** from the main navigation. The header shows `LEADER`,
`STANDBY`, `DEGRADED`, or `STOPPED`, database quick-check health, timezone,
heartbeat, worker limit, next due task, and unread notifications.

## Creating and operating schedules

Use **Create schedule** to choose an allowlisted task and trigger. The backend
previews five resolved local/UTC occurrences before saving. The UI intentionally
has no command, script, executable, arbitrary-path, credential, account, or
order fields.

- `ONCE`: local date/time and timezone.
- `DAILY`: local time and timezone.
- `WEEKDAYS`: numeric weekdays (`0` Monday through `6` Sunday).
- `INTERVAL`: UTC anchor and a minimum one-minute interval.
- `SESSION_RELATIVE`: existing L3G `ASIA` or `NEW_YORK` open/close plus offset.

Spring DST gaps are recorded as `MISSED`, never shifted. Autumn overlaps use
deterministic fold 0 and execute once. The backend, not the browser, resolves
all occurrences.

The page has **Schedules** and **Runs** views. Editing creates a new immutable
revision. Pause/resume affects future scheduling; archive retains history and
cancels future scheduled runs. **Run now** goes through normal registry,
resource locks, and state handling. Cancellation requests cooperative shutdown
before a bounded forced cancellation. A retry creates a new linked run and
never rewrites a failed record.

Use status cards to filter: Scheduled (`SCHEDULED`, `QUEUED`), Active
(`ACTIVE`), Complete (`COMPLETE`), Failed (`FAILED`, `BLOCKED`, `MISSED`,
`INTERRUPTED`), or All (also `CANCELLED`). `BLOCKED` means a prerequisite was
not proven; `FAILED` is an operational or invariant failure; `MISSED` means the
window expired; `INTERRUPTED` means shutdown/lease expiry prevented a terminal
outcome. **Inspect** shows immutable configuration, events, leases, locks,
result/error, and notifications.

To create a one-time reminder, choose `operator.reminder`, supply title,
message, severity, and local time, preview, then save. Reminders become durable
notifications. Daily, weekday, and interval jobs use the same form.
`system.health_snapshot` reads existing runtimes; `system.database_quick_check`
checks only fixed configured databases; `science.run_once` invokes the existing
scientific worker with its durable resource lock.

## Lane III templates and boundaries

**Create paused** instantiates an independent template; it is not a workflow
engine. Asia and New York each have readiness, opening reminder, close-audit,
and audit-export templates.

```text
ASIA OPEN -15m  Readiness task
ASIA OPEN -2m   Reminder
ASIA OPEN       Operator manually reviews and arms outside scheduler
ASIA CLOSE      l3g owns its existing hard-flat/disarm behavior
ASIA CLOSE +2m  Close audit
ASIA CLOSE +5m  Audit export
```

The scheduler never performs manual arming. It cannot arm sessions, submit,
modify, or cancel orders, flatten accounts, change positions, enable live
execution, access Lucid/live accounts, store credentials/signing keys, execute
code, or bypass L3G fences. Lane III tasks are observe, verify, notify, or
fixed-directory export only. Session-relative schedules force `SKIP` and fail
closed when the existing L3G resolver cannot verify a valid window. Audit
export writes sanitized JSON only to `logs/scheduler-audits/<run-id>.json`.

## Health and recovery

The FastAPI lifespan owns the scheduler. Browser refreshes, routes, and
websocket clients cannot create an engine. Leader/run/resource leases plus
deterministic occurrence keys prevent duplicate work. On startup expired active
runs become `INTERRUPTED`, stale locks are reconciled, and scheduling resumes.
Unexpected loop errors are visible as `DEGRADED` notifications. The UI consumes
the existing WebSocket and polls as a fallback.
