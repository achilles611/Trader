# OPS-A Task Scheduler commissioning record

OPS-A adds an independent, durable operations scheduler to the normal
BeezConsole FastAPI lifespan. It owns no trading authority. Its SQLite database
is `beelzebub_operations.sqlite3` beside the active application database and
uses foreign keys, WAL, and a busy timeout.

Commission through normal BeezConsole startup:

1. Confirm `/api/scheduler/status` reports `LEADER` and one durable owner.
2. Confirm the existing NinjaTrader listener remains the single lifespan owner.
3. Temporarily enable commissioning probes and create a one-time
   `operator.reminder` two minutes in the future.
4. Restart before it is due; confirm exactly one run and
   `SCHEDULED → QUEUED → ACTIVE → COMPLETE`.
5. Run both configured database quick checks.
6. Create an Asia readiness template and leave it paused.
7. Confirm the scheduler cannot arm a session, change Sim101, or mutate
   Lucid/live authority.
8. Disable commissioning probes, restart, and confirm probes are absent.
9. Shut down and confirm no scheduler, listener, or paper runtime remains.

This implementation tests deterministic resolver fixtures. It does not claim
an authentic session run:

```text
Session-relative trigger computation tested with deterministic l3g resolver fixtures.
No authentic Asia or New York scheduler task execution is claimed.
```

Record branch/commit, focused and full tests, UI build, `git diff --check`,
both database checks, protected-boundary diffs, and AddOn source hashes in the
final report. Do not claim OPS-A frozen without completing that evidence.
