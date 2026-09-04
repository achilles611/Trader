# Beelzebub paper profile selector

## Scope

Slim Mode exposes three compiled, paper-only Sim101 profiles:

| Selector label | Immutable version | Behavior |
| --- | --- | --- |
| High confidence | `NY_HIGH_CONFLUENCE_COMMISSIONING_V1` | New York high-confluence profile with one entry opportunity per session. |
| Scalper | `BEELZEBUB_SCALPER_V2` | Multi-session V2 scalper. This remains the default when no profile is configured. |
| 5-minute perpetual position | `BEELZEBUB_FIVE_MINUTE_BIAS_V1` | Decides once per completed five-minute candle and holds or stages a reconciled reversal. |

Every bundle contains both a policy artifact and its risk artifact. The selected
policy hash and risk hash are used by the runtime, ledger, signed AddOn session
grant, command validation, status API, and restart proof. All bundles remain
sealed to `Sim101 / LOCAL_SIMULATION / MNQ SEP26 / quantity 1`; live capital is
denied.

## Operator workflow

1. In Slim Mode, choose a profile and press **Switch & Start**.
2. The authenticated backend creates an immutable handoff manifest and a new,
   unused run directory.
3. The current runtime pauses entry authority, exits or cancels only its owned
   Sim101 MNQ work, and waits for exact reconciled flat/no-orders truth.
4. A detached local supervisor is started, then the current process requests
   its normal lifespan-owned shutdown.
5. Shutdown seals admission, drains the writer, quiesces the verifier, proves
   the durable tip, performs the final complete checkpoint, and records the
   receipt in the handoff state.
6. Only a complete receipt releases the supervisor. It starts the selected
   profile with the prepared ledger, audit root, epoch, policy, risk, and Git
   identity.
7. The new backend must match every manifest binding. It then uses the ordinary
   paper auto-start sequence: NinjaTrader readiness, observer/authentication,
   Sim101 reconciliation, Full ledger verification, and canonical persistent
   `operational-start`.

Operational start is never routed through Atomic Commissioning.

## Automatic storage

Each selection gets a distinct run root:

```text
<runtime-root>/profiles/<profile-version>/runs/<UTC-run-id>/
  hot/lane_iii_paper.sqlite3
  audit/
```

The ledger epoch is generated with the run and cannot reuse another profile's
chain. Previous database, WAL, SHM, audit, and analysis artifacts are never
deleted, truncated, rewritten, or resequenced by the selector.

Handoff evidence is stored under:

```text
<runtime-root>/profile-switch/
  profile-switch-audit.jsonl
  operations/<operation-id>/
    manifest.json
    state.json
    supervisor-audit.jsonl
    target-backend.log
```

`manifest.json` is hash-bound and written once. `state.json` is the current
projection; the JSONL files retain the append-only transition history.

## Fail-closed outcomes

No target process is launched when the current profile cannot reach exact
reconciled flat/no-orders truth, when its controlled ledger shutdown is
unproven, when the manifest hash or compiled profile hashes differ, or when the
runtime paths and Git binding differ.

If the target backend launches but NinjaTrader readiness, reconciliation, Full
ledger verification, or operational start fails, the target remains disarmed.
Slim Mode reports `BLOCKED_SAFE` and the exact blocker; there is no blind retry
and no fallback to another profile.
