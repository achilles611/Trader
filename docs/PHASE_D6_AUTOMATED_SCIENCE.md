# Phase D.6 — Automated Scientific Worker

Phase D.6 turns the existing scientific object model into a durable, bounded
research loop. It is not an execution upgrade: every object remains on the
simulation/shadow-only side of the safety boundary.

## Safety boundary

- `src/eth_bot` and the Coinbase dependency were removed permanently.
- The supported runtime contains no exchange-write adapter, transfer route,
  private-key signer, or mainnet-capital authority.
- A public wallet fill becomes a `WALLET_FILL` observation. It cannot authorize
  an order or a PAPER entry directly.
- The only D.6 decision output is a `DecisionRecord` from the scientific
  decision gate. It requires validated indicators, a versioned model, positive
  cost-adjusted expected net edge, effective confidence, supported regime, and
  risk-derived size/leverage. Its execution mode is `SIMULATION_SHADOW`.
- Hot work uses the active E: SQLite database. D: is archive-only and is never
  synchronously read from an ingestion, decision, or market callback.

## Durable loop

The worker uses leased SQLite work rather than per-stage Windows scheduled
tasks. Stages are:

`OBSERVATION_INGEST → FEATURE_MATERIALIZATION → OUTCOME_LABEL →
PATTERN_DISCOVERY → HISTORICAL_EXPERIMENT → FORWARD_PREDICTION →
FORWARD_RESOLUTION → INDICATOR_PROMOTION → MODEL_BUILD → MODEL_CALIBRATION →
SHADOW_DECISION → DRIFT_EVALUATION → ARCHIVAL`.

`science_work_queue` has `PENDING`, `LEASED`, `COMPLETED`, `RETRYABLE`,
`FAILED`, `CANCELLED`, and `SUPERSEDED` semantic states. Claims use
`BEGIN IMMEDIATE`; an expired lease becomes retryable, attempts are bounded,
and terminal failures retain a redacted error class. Duplicate inputs are
idempotent. When new feature/label evidence supersedes pending expensive work,
the old row is retained as `SUPERSEDED`; a leased job is never silently
cancelled.

Research-stage work carries an exact evidence/workflow fingerprint. A stale
item completes as superseded rather than reading data that appeared after it
was scheduled. Watermarks retain materialization and resolution progress.
Restarting creates a fresh worker identity but uses the same durable queue,
observations, features, outcomes, and object state.

## Evidence and discovery

The observation bridge records reconstructed public wallet evidence and public
market mids automatically. Callbacks only persist evidence and queue work;
resampling, mining, fitting, drift checks, and archival are queued.

All D.5 wallet and market feature definitions are registered at version 1.
Unsupported inputs become explicit missing values, never zero. Feature values
retain source observation IDs and deterministic immutable fingerprints.

The initial `initial-interpretable` search family is persisted and bounded to
versioned `wallet_action`, `wallet_disagreement`, `short_term_return`, and
`local_momentum`, a 15-second horizon, interaction order 1, configured sample
floor/effect floor, and a per-cycle proposal cap. It cannot condition on wallet
identity or create arbitrary higher-order strategy searches. Registration also
enforces the daily family cap and graveyard similarity check.

Historical testing freezes the registered time range. Training precedes a
prediction-horizon purge; validation begins only afterwards. Net economics
subtract retained costs. Every eligible sibling in a declared family is
evaluated before Benjamini–Hochberg q-values are persisted. Failed hypotheses
go to the immutable graveyard; a survivor enters `FORWARD_SHADOW`, never an
indicator directly.

Forward predictions are written before their own anchor has a mature outcome.
Resolution only accepts the label for the same source observation and horizon.
Promotion requires configured forward count and positive forward net
expectancy, then produces immutable validated indicator provenance. Drift does
not degrade from a single sample; sufficient negative forward expectancy emits
a retained degradation event and changes state to `DEGRADED`.

## Models, confidence, and resources

Models are transparent weighted, versioned indicator sets with a
champion/challenger role record. A model is never modified in place. Forward
calibration records retain buckets, Brier score, source fingerprint, and an
explicit `INSUFFICIENT_FORWARD_EVIDENCE` status until the sample gate is met.
Only then can `SHADOW` advance to `ACTIVE_SIMULATION`.

The worker retains the D.5 confidence relation:

`effective_confidence = 0.5 + (trade_confidence - 0.5) * model_confidence`.

`model_confidence` is evidence quality, not a win probability. Latency records
provide count, mean, max, p50, p95, and p99. Config bounds worker/process
count, queue batch, SQLite write batch, cache/materialization size, resamples,
candidate/experiment/model budgets, and horizons. Low free space on the hot
volume pauses research safely; it does not alter execution safety.

## Operations

From `E:\Beelzebub`:

```powershell
.\.venv\Scripts\python.exe main.py science run-once
.\.venv\Scripts\python.exe main.py science run
.\.venv\Scripts\python.exe main.py science status
.\.venv\Scripts\python.exe main.py science pause --reason "maintenance"
.\.venv\Scripts\python.exe main.py science resume
.\.venv\Scripts\python.exe main.py science backfill
.\.venv\Scripts\python.exe main.py science rebuild --explicit
.\.venv\Scripts\python.exe main.py science bootstrap
.\.venv\Scripts\python.exe main.py science reproduce --experiment <id>
```

`run` is a single long-lived worker process. `run-once` is useful for a
scheduled task, CI, or a manual bounded tick. `backfill` only processes local
durable evidence. `rebuild --explicit` creates a new auditable queue generation
without deleting or mutating scientific evidence. `bootstrap` reports counts;
zero promoted indicators are a valid scientific outcome.

## Control Center

The **Automated Science** workspace reads queue truth, cursor/stage state,
discoveries, model roles/calibration, drift, journal, and E: resource status.
Its Pause/Resume buttons only set the durable worker control row. They cannot
launch live trading or change PAPER entry controls. The worker process is
started independently with the CLI, so the UI never invents a scheduler state.
