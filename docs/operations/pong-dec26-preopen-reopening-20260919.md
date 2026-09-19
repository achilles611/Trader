# Pong DEC26 pre-open reopening runbook — 2026-09-19

## Purpose and hard boundary

This is a disarmed, read-only reopening procedure for the versioned DEC26
topology. It does not start a listener, schedule a future launch, enable paper
orders, or grant live authority. The current historical DEC26 corpus is a
consumer-integrity artifact only; it is not current market qualification.

The only selectable identities for this runbook are:

| Field | Required value |
| --- | --- |
| Account | `Sim101` |
| Environment | `LOCAL_SIMULATION` |
| Native instrument | `MNQ DEC26` |
| Canonical contract | `MNQZ6` |
| State namespace | `pong/MNQZ6/v1` |
| Topology | `PONG_CONTRACT_TOPOLOGY_DEC26_V1` |
| Authority | `DISARMED`; live capital `DENIED` |

Any SEP26 identity, missing field, mixed hash, stale source fingerprint,
producer-session change, unresolved account state, or unverified history is a
refusal condition. Do not relabel SEP26 evidence as DEC26 and do not filter or
rehash the retained DEC26 journal to make it qualify.

## Exact reopening sequence

### 1. Confirm the machine is quiet before opening anything

Run these commands from the operating checkout. Do not proceed if an
unrelated owner holds port 8090, if a prior BeezConsole/NinjaTrader process is
still active, or if the checkout identity is not the reviewed one.

```powershell
Set-Location C:\Users\atlas\Documents\Trader
git rev-parse HEAD
git status --short
Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
  Where-Object { $_.LocalPort -in 8090,48135 } |
  Select-Object LocalAddress,LocalPort,OwningProcess
Get-Process -Name NinjaTrader*,BeezConsole*,python* -ErrorAction SilentlyContinue |
  Select-Object Id,ProcessName,Path
```

The first two commands must show the integrated reviewed commit plus only the
known preserved user edits. The listener/process checks are evidence of
absence, not proof that an account is flat.

### 2. Verify source and build without launching the trading host

```powershell
Set-Location C:\Users\atlas\Documents\Trader
pwsh -NoProfile -File .\tools\validate_ninjatrader_observer.ps1 `
  -RepositoryRoot (Get-Location).Path
```

Require bundled-Roslyn success, zero compiler errors, exact source hashes, and
the read-only observer source contract. Warnings are recorded, not silently
discarded. The existing successful offline validation was
`NINJATRADER_BUNDLED_ROSLYN_IN_MEMORY` with zero errors and 1,267 warnings.

### 3. Install only the read-only source during a maintenance window

Do this only when NinjaTrader is closed or its source watcher is intentionally
available. The installer writes source and creates timestamped `.bak` files;
it must never copy a DLL.

```powershell
Set-Location C:\Users\atlas\Documents\Trader
pwsh -NoProfile -File .\tools\commission_ninjatrader_observer.ps1 `
  -InstallSource `
  -CompileTimeoutSeconds 60 `
  -CheckpointSeconds 10 `
  -NinjaTraderDocumentsRoot 'C:\Users\atlas\Documents\NinjaTrader 8'
```

Afterward, verify canonical source parity and record the newest backup paths:

```powershell
Get-ChildItem 'C:\Users\atlas\Documents\NinjaTrader 8\bin\Custom\AddOns' `
  -Filter 'BeelzebubReadOnlyAddOn.cs.unattended-before-*.bak' |
  Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1 FullName,Length
Get-ChildItem 'C:\Users\atlas\Documents\NinjaTrader 8\bin\Custom\Indicators' `
  -Filter 'BeelzebubReadOnlyMarketObserver.cs.unattended-before-*.bak' |
  Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1 FullName,Length
Get-FileHash 'C:\Users\atlas\Documents\NinjaTrader 8\bin\Custom\NinjaTrader.Custom.dll'
```

The current offline attempt correctly wrote both source files and created
backups, but stopped before compiled identity verification because no
NinjaTrader source watcher was running. The installed DLL remained unchanged;
this is a partial safe install, not a native activation proof.

### 4. Start the native observer read-only and prove the exact source session

After the source/build checks pass, start NinjaTrader manually in the existing
user profile. Attach only `BeelzebubReadOnlyMarketObserver` to the exact
`MNQ DEC26` instrument. In the read-only AddOn configuration, select `Sim101`
and retain `LOCAL_SIMULATION`. Do not attach or enable an order-capable AddOn.

The observer must produce a new signed session whose source fingerprint and
build fingerprint match the current reviewed source/build. It must provide,
without gaps or identity changes:

- current account/position/working-order read-only snapshot;
- warm-start and history receipt with durable acknowledgement;
- depth, quote, trade, and volume-reference streams;
- one producer session for the bounded capture; and
- the exact DEC26 native/canonical identity above.

If any producer-session change occurs, stop the capture and preserve the
evidence. The protected host must fail closed with
`PONG_NATIVE_INPUT_PRODUCER_CHANGED`; do not merge sessions.

### 5. Start the host only after the observer is proven disarmed

Set the exact topology before startup and keep activation disabled:

```powershell
Set-Location C:\Users\atlas\Documents\Trader
$env:BEELZEBUB_PONG_CONTRACT_TOPOLOGY = 'DEC26'
$env:BEELZEBUB_PONG_BOUNDED_ACTIVATION = '0'
$env:BEELZEBUB_LIVE_TRADING = '0'
.\scripts\start_beezconsole.ps1
```

The first operation after startup is listener/status inspection, never an
order or activation request:

```powershell
Invoke-RestMethod http://127.0.0.1:8090/api/runtime-binding
Invoke-RestMethod http://127.0.0.1:8090/api/lane-iii/pong
```

The responses must show the DEC26 topology version/hash, `MNQ DEC26`,
`MNQZ6`, `pong/MNQZ6/v1`, the reviewed source/build, `Sim101`,
`LOCAL_SIMULATION`, disarmed authority, live denied, current account/order
reconciliation, current history/bias readiness, durable advancement, queue
age, and valid watchdog/session state. A historical path or a missing field is
not a substitute for a current response.

### 6. Run one bounded fresh capture, then protected replay

Use the actual observer session output directory selected by the current
profile. Preserve raw journal bytes, status, source/build fingerprints,
sequence bounds, and the authenticated history receipt. Before replay, verify:

1. all records are DEC26/MNQZ6;
2. the source/build fingerprint equals the new observer session;
3. exactly one producer session spans the bounded capture;
4. hash chain, signatures, sequence, stream continuity, and durability pass;
5. account/position/orders reconcile to the current Sim101 read-only snapshot;
6. queue overflow, duplicate, gap, stale-source, and unresolved-execution
   counts are zero; and
7. the host remains disarmed and emits zero native directives.

Only a corpus satisfying all seven checks may be passed to
`tools/pong_dec26_authenticated_replay.py`. The retained 2026-09-18 DEC26
corpus fails item 2 and item 3 for current qualification: it uses the old
producer fingerprint/build and contains two producer sessions. Its integrity
receipt remains valuable and must remain immutable.

### 7. Measure and decide

Report separately, with count, source span, machine/build, wall time, p50,
p95, p99, max, queue peak/final depth, overflow, and drain status:

- source-to-receipt age;
- callback-to-authoritative admission;
- first analytical attempt;
- host evaluation;
- durable acknowledgement; and
- shutdown/drain.

Natural-paced timing is the first measurement. Do not infer protective
equivalence, a 20 ms SLA, or sustained `2,111/s` whole-system capacity from a
parser rate, an owner span, a clean drain, or the retained SEP26 fixture.

### 8. Stop, rollback, or reopen later

Controlled stop is:

```powershell
Set-Location C:\Users\atlas\Documents\Trader
.\scripts\stop_beezconsole.ps1 -Port 8090
```

Verify terminal/flat/reconciled state and no listener. If native source
rollback is required, stop NinjaTrader first, identify the exact timestamped
`.unattended-before-*.bak` created by this install, hash it, and restore only
that matching source file. Never copy `NinjaTrader.Custom.dll`, use a broad
wildcard delete, or use Git reset/checkout to remove the preserved user edits.

After any stop or rollback, require a new disarmed startup and repeat steps 1,
4, and 5 before considering another bounded capture. No launch-at-open task,
automation, or future schedule is created by this runbook.

## Fresh-data gate

The following are the only remaining pre-open gates that require fresh native
market/account data: current DEC26 source/build producer authentication;
current warm-start/history and bias; current Sim101 positions, working orders,
and risk/reconciliation snapshot; one-session depth/quote/trade continuity;
current source age and latency/queue/durable-drain measurements; and restart
recovery against the current account and producer session.

The following are offline-complete: exact versioned topology migration;
mixed-bundle refusal; DEC26 risk-continuity semantics on a topology-bound
fixture; authenticated historical journal verification; SEP26 historical
regression/latency evidence; observer source compilation; safe source-only
install with backups; generated-wrapper and callback-ordering checks; and
disarmed zero-dispatch integration tests.

## 2026-09-19 execution addendum

The seven authorized reversions were resolved to the reviewed `HEAD` in the
operating checkout after immutable copies, the staged patch, and the stash
object were recorded. The only additional source repair was a narrow
compatibility correction in `src/l3g_paper/pong_production.py`: the bias helper
is static again for the retained historical certificate tool, while the live
DEC26 host passes its selected instrument explicitly. No paper or native
authority was enabled.

Use this explicit scheduler configuration for a future disarmed DEC26
observation. It keeps periodic flat evaluation enabled while retaining the
event-driven protection path:

```powershell
$env:BEELZEBUB_PONG_CONTRACT_TOPOLOGY = 'DEC26'
$env:BEELZEBUB_PONG_SCHEDULING_MODE = 'PERIODIC_FLAT_PROTECTED'
$env:BEELZEBUB_PONG_PERIODIC_INTERVAL_MS = '20'
$env:BEELZEBUB_PONG_BOUNDED_ACTIVATION = '0'
$env:BEELZEBUB_LIVE_TRADING = '0'
```

The offline periodic replay used the 20 ms configuration, selected
`PERIODIC_FLAT`, applied 7,684 periodic analytical evaluations, and retained
zero event-driven analytical evaluations because the fixture stayed flat. Its
deadline receipt recorded 9,166 scheduled opportunities, 7,805 dispatches,
7,805 completions, 1,360 obsolete skips, 121 intentional no-work deadlines,
and QPC/QueryPerformanceCounter monotonic timing. These are fixture results;
they do not certify a live 20 ms SLA, capacity, or protective equivalence.

The retained DEC26 replay is an integrity result only: 335,551/335,551 records
verified, zero contract mismatches, zero native dispatches, and
`PONG_NATIVE_INPUT_PRODUCER_CHANGED`. Its old producer fingerprint/build and
two-session split remain refusal evidence and must not be merged.

### Target-time and qualification boundary

The requested target is Sunday, 2026-09-20 at 17:00 America/Denver, which is
19:00 America/New_York and 23:00 UTC. CME's current equity-index schedule puts
the Sunday Globex reopen at 18:00 Eastern, so the target is inside the exchange
session. The local resolver selects `ASIA` beginning at 18:00 Eastern, with an
18:05 entry start; it is not an `OFF_SESSION` target. Verify the live venue
calendar and feed state again because the repository calendar is not a venue
hours feed.

Qualification must begin before 17:00 Mountain if the target is to be met. The
observer, current account snapshot, warm-start/history receipt, one coherent
DEC26 producer session, and the bounded natural capture all have to complete
before qualification can be reported. There is no safe offline way to assign a
fixed lead time: the earliest start is the first point at which NinjaTrader is
running, `Sim101` is reconciled and flat, and the fresh DEC26 feed is producing
the required streams. If that evidence is not complete by 17:00 Mountain,
qualification and any paper start move later; the scheduler must report the
shortfall rather than compressing or bypassing gates.

### Exact guarded paper-start boundary

The read-only status endpoint is:

```text
GET /api/lane-iii/paper/auto-start
```

The browser's guarded action, when all current gates are green, is the
same-origin loopback `POST /api/lane-iii/paper/operational-start` with exactly
`{"request_id":"<uuid>"}`. Do not issue it during preparation. The current
general Lane III operational-start and transport gates still require
`Sim101 / LOCAL_SIMULATION / MNQ SEP26`; that is a code-level blocker to a
DEC26 Sim101 paper start, not a missing market-data receipt. The DEC26 Pong
topology migration intentionally did not relabel or widen those generic paper
contracts. A DEC26 paper start therefore remains blocked until a separately
authorized topology-wide runtime/account integration is completed and tested.

The existing scheduled 17:00 Mountain task is reserved for fresh-data
qualification and reporting. It must not be treated as permission to repair,
arm, or start paper trading.
