# Beelzebub Asia qualification next-pass runbook — 2026-09-22

## Authority and frozen inputs

This is a manual, single-operator, disarmed Sim101 runbook. It does not authorize scheduling, autonomous startup, arming, order submission, live capital, or a retry outside the declared window.

- Operator/owner: Joseph.
- Repository: `C:\Users\atlas\Documents\Trader`.
- Repair-code head: `b4a46c2d73030f008e305a687069456c5d1bda0d`.
- Repair-code tree: `c3e5ca334e63f8332883a5f31ac9013785be905a`.
- Profile: `BEELZEBUB_PONG_DISCERNMENT_V1`.
- Account/environment: `Sim101` / `LOCAL_SIMULATION`.
- Native/canonical contract: `MNQ DEC26` / `MNQZ6`.
- Architecture: `FULL_NATIVE_DEPTH_PERIODIC_20MS_NO_FHB_V1`.
- Scheduling: `PERIODIC_FLAT_PROTECTED`, 20 ms, FHB excluded.
- Existing ledger identity: `l3g-ledger-3ec065663f4b45eeb5570a8dcba63dda`.
- Existing epoch: `L3G-PAPER-EPOCH-BEELZEBUB_PONG_DISCERNMENT_V1-20260920T211544Z-ca2ceba7da11`.
- Existing ledger: `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\hot\lane_iii_paper.sqlite3`.
- Existing audit root: `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit`.
- Evidence root: `N:\Beelzebub\asia-qualification-20260922`.
- R2 budget: zero used, one conditional attempt available.

Do not copy, move, or replace the ledger. The operational ledger is already on N: and must retain its identity, epoch, chain, and history.

## Fixed proposed window

The proposed next session is Tuesday, 2026-09-22. Recheck the [official CME trading-hours/holiday calendar](https://www.cmegroup.com/trading-hours.html) and [CME Micro E-mini hours](https://www.cmegroup.com/articles/faqs/micro-e-mini-equity-index-futures-frequently-asked-questions.html) immediately before execution; abort rather than improvise if the venue schedule differs.

| Event | America/Denver | America/New_York | UTC |
| --- | --- | --- | --- |
| Manual preflight begins | 15:30 MDT | 17:30 EDT | 21:30Z |
| Asia observation/session start | 16:00 MDT | 18:00 EDT | 22:00Z |
| Fixed diagnostic ratio window | 16:05–16:10 MDT | 18:05–18:10 EDT | 22:05–22:10Z |
| Entry cutoff from local profile | 23:30 MDT | 01:30 EDT, Sep 23 | 05:30Z, Sep 23 |
| Hard-flat deadline | 23:58 MDT | 01:58 EDT, Sep 23 | 05:58Z, Sep 23 |
| Session end | 00:00 MDT, Sep 23 | 02:00 EDT, Sep 23 | 06:00Z, Sep 23 |

The ratio window is diagnostic until Joseph approves a numeric threshold, direction, tolerance, and minimum denominator. Never slide, trim, backfill, or rerun the window after observing its result. If R2 and the preflight are not clean before 22:05Z, abort this window and predeclare a wholly new one.

## Known HOLD before natural-feed work

The final identical-input offline comparison is `NOT_QUALIFIED`: the repaired candidate retained all 10,000 records in order with zero overflow and transport-failure deltas, but achieved only 288.665 records/s with 24.643 seconds receiver lag against a 1,000 records/s offered schedule. This repairs observability and correctness; it does not establish capacity or a natural-feed qualification.

## Step 0 — create evidence root on N: and record host facts

Run from the repository in a fresh PowerShell owned by Joseph:

```powershell
$ErrorActionPreference = 'Stop'
$evidence = 'N:\Beelzebub\asia-qualification-20260922'
$env:TEMP = 'N:\Beelzebub\asia-qualification-20260922\temp'
$env:TMP = $env:TEMP
$env:TMPDIR = $env:TEMP
New-Item -ItemType Directory -Force -Path $evidence, $env:TEMP | Out-Null
git status --short | Set-Content -LiteralPath "$evidence\00-git-status.txt"
git rev-parse HEAD | Set-Content -LiteralPath "$evidence\00-head.txt"
git rev-parse 'HEAD^{tree}' | Set-Content -LiteralPath "$evidence\00-tree.txt"
Get-Volume -DriveLetter C,N | Select-Object DriveLetter,SizeRemaining,Size | ConvertTo-Json | Set-Content -LiteralPath "$evidence\00-volumes.json"
Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object LocalPort -in 8090,48135,48136 | ConvertTo-Json | Set-Content -LiteralPath "$evidence\00-listeners-before.json"
Get-Process NinjaTrader,python -ErrorAction SilentlyContinue | Select-Object Id,ProcessName,StartTime,Path | ConvertTo-Json | Set-Content -LiteralPath "$evidence\00-processes-before.json"
```

Require the repair-code commit to be an ancestor of the checked-out evidence head, preserve unrelated dirty files, require N: to remain above the unchanged 8 GiB warning floor, and do not stop or force-kill operator-owned NinjaTrader.

## Step 1 — start the disarmed control plane with exact existing storage

Set the qualifying route explicitly, leave activation disabled, and use the supported launcher with the complete maintenance binding so the existing ledger identity/history is selected rather than a new run:

```powershell
$env:BEELZEBUB_PONG_CONTRACT_TOPOLOGY = 'DEC26'
$env:BEELZEBUB_L3G_PAPER_CONTRACT_TOPOLOGY = 'DEC26'
$env:BEELZEBUB_PONG_SCHEDULING_MODE = 'PERIODIC_FLAT_PROTECTED'
$env:BEELZEBUB_PONG_PERIODIC_INTERVAL_MS = '20'
$env:BEELZEBUB_PONG_BOUNDED_ACTIVATION = '0'
$env:BEELZEBUB_LIVE_TRADING = '0'
.\scripts\start_beezconsole.ps1 `
  -LedgerPath 'N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\hot\lane_iii_paper.sqlite3' `
  -AuditRoot 'N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit' `
  -LedgerEpoch 'L3G-PAPER-EPOCH-BEELZEBUB_PONG_DISCERNMENT_V1-20260920T211544Z-ca2ceba7da11' `
  -PaperProfile 'BEELZEBUB_PONG_DISCERNMENT_V1'
```

Do not continue unless exactly one listener owns each expected port and the launcher returns the exact ledger, audit root, epoch, profile, and `127.0.0.1:8090` binding.

## Step 2 — capture the read-only preflight sandwich

```powershell
$base = 'http://127.0.0.1:8090'
$binding1 = Invoke-RestMethod "$base/api/runtime-binding"
$paper = Invoke-RestMethod "$base/api/lane-iii/paper"
$auto = Invoke-RestMethod "$base/api/lane-iii/paper/auto-start"
$pong = Invoke-RestMethod "$base/api/lane-iii/pong"
$verification = Invoke-RestMethod "$base/api/lane-iii/paper/ledger-verification"
$binding2 = Invoke-RestMethod "$base/api/runtime-binding"
[ordered]@{binding_before=$binding1;paper=$paper;auto_start=$auto;pong=$pong;ledger_verification=$verification;binding_after=$binding2} |
  ConvertTo-Json -Depth 100 | Set-Content -LiteralPath "$evidence\01-preflight-sandwich.json"
```

Abort on binding drift; any non-Sim101 account; any non-local-simulation environment; any instrument other than MNQ DEC26/MNQZ6; non-flat runtime or broker position; owned, entry, working, or foreign orders; pending directive; unresolved execution/native order; incomplete position/order snapshots; stale or false reconciliation; invalid add-on provenance; any armed/operational session; or live capital not denied.

## Step 3 — run the one conditional R2 native commission/reload

Only Joseph may initiate this step, only after Steps 0–2 are clean, and only once:

```powershell
pwsh .\tools\commission_ninjatrader_observer.ps1 `
  -InstallSource `
  -RequireDistinctProducer `
  -PreReloadReceiptPath 'H:\PongPeriodic\NoFHBQualificationCorrective-20260921\R2-commission-0518-20260921T1709Z.json' `
  -ExpectedInstrument 'MNQ DEC26' `
  -CheckpointSeconds 10 `
  -CompileTimeoutSeconds 60 `
  -EvidencePath "$evidence\02-R2-commission.json"
```

Require a distinct current producer generation/session, the repaired source fingerprint `17995c2795e16bf52bedd9644a865261e3a20473f1e2bb7b75b4947bc558a525`, a matching current build, MNQ DEC26, fresh signed observations, zero new overflow/transport failure, and the new lifecycle semantics. On any refusal or ambiguity, retain the receipt and stop; do not reinstall or reload again.

## Step 4 — capacity latch recovery, only from current proof

First request and wait for a fresh Full verification:

```powershell
Invoke-RestMethod -Method Post -ContentType 'application/json' -Uri "$base/api/lane-iii/paper/ledger-verification" -Body '{"mode":"full"}' |
  ConvertTo-Json -Depth 20 | Set-Content -LiteralPath "$evidence\03-full-verification-start.json"
do {
  Start-Sleep -Seconds 2
  $full = Invoke-RestMethod "$base/api/lane-iii/paper/ledger-verification"
} while (@('PENDING','RUNNING') -contains ([string]$full.state).ToUpperInvariant())
$full | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath "$evidence\03-full-verification-result.json"
```

Do not call recovery unless the fresh Full result passes and a new read-only sandwich proves the same ledger identity/epoch, authenticated add-on, current reconciliation, exact flat positions, zero owned/working/entry orders, zero pending directives, complete snapshots, disarmed runtime, and healthy current ledger destination. Historical flat evidence is insufficient.

```powershell
$headers = @{'X-Beelzebub-Ledger-Capacity-Recovery-Action'='lane-iii-ledger-capacity-recovery-v1'}
$body = @{request_id='asia-20260922-capacity-recovery-0001'} | ConvertTo-Json -Compress
$recovery = Invoke-RestMethod -Method Post -Headers $headers -ContentType 'application/json' -Uri "$base/api/lane-iii/paper/ledger-capacity-recovery" -Body $body
$recovery | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath "$evidence\04-capacity-recovery.json"
```

Require `recovered=true`, `operational_paper_started=false`, `armed=false`, and `new_full_verification_required=true`; then run another Full verification and repeat the sandwich before any later gate. A refusal is a HOLD, not authority to edit the database or latch files.

The traced C: refusal was a test-only temporary ledger at `C:\Users\atlas\AppData\Local\Temp\tmp0k577py1\paper.sqlite3`; `PaperLedger` correctly measures `shutil.disk_usage(self.path.parent).free`, so it measured that C: destination below the 8 GiB warning floor. The operational N: ledger is a different destination. Keep the 8 GiB warning and 4 GiB minimum unchanged.

## Step 5 — predeclare and capture the fixed periodic diagnostic window

At or before 22:04:30Z, write a signed/operator-stamped predeclaration naming exactly 22:05:00Z–22:10:00Z, the selected identity, the unchanged formula `R = ΔDEPTH / Δordinary completed full analyses`, and the no-rerun rule. At 22:05Z and 22:10Z capture the complete `/api/lane-iii/pong` response without restarting anything:

```powershell
Invoke-RestMethod "$base/api/lane-iii/pong" | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath "$evidence\05-pong-window-start.json"
# Wait manually until the fixed 22:10:00Z boundary; do not move the boundary.
Invoke-RestMethod "$base/api/lane-iii/pong" | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath "$evidence\05-pong-window-end.json"
```

The start and end `evaluation_schedule.periodic_ratio_telemetry` objects must share the exact counter epoch, reset ordinal, process, experiment, artifact, profile, topology, runtime execution, native add-on, source session/build/fingerprint, and generation identities. Require strictly increasing UTC and monotonic times; zero producer/collector/host overflows; zero transport failures; no gaps/out-of-order events; exact canonical 10x10 depth or explicit unavailable state; freshness below 1,000 ms; zero durability lag/regression; drained queues; no protection transition; no exposure/order/reconciliation disqualifier; and no FHB consumer/helper.

If the denominator is zero, the ratio must be null with its reason. If any disqualifier changes, the window is ineligible and cannot be repaired by trimming. Until policy is approved, record the computed ratio as diagnostic with `threshold=null` and `threshold_reason=POLICY_NOT_DEFINED`; do not declare periodic-ratio acceptance.

## Step 6 — stop and preserve evidence

No order-producing gate is authorized by this runbook. Stop with the supported command, then hash every artifact:

```powershell
.\scripts\stop_beezconsole.ps1 -Port 8090
Get-ChildItem -LiteralPath $evidence -File -Recurse | Get-FileHash -Algorithm SHA256 | Sort-Object Path | ForEach-Object { "$($_.Hash) $($_.Path)" } |
  Set-Content -LiteralPath "$evidence\SHA256SUMS.txt"
```

Require stopped/drained collector and host queues, clean ledger shutdown/checkpoint, zero native directives/orders/executions/fills, and preserved operator-owned NinjaTrader. Do not force-kill either process.

## Outcome labels

- `ABORTED_PREFLIGHT`: any identity, account, flatness, reconciliation, listener, storage, provenance, or venue-calendar mismatch before R2.
- `R2_REFUSED_RETAINED`: the single conditional native attempt fails or is ambiguous; no second attempt.
- `CAPACITY_RECOVERY_DEFERRED`: current authenticated flat/reconciled/Full proof is absent or recovery refuses.
- `PERIODIC_WINDOW_INELIGIBLE`: any identity drift, protection, loss, gap, stale data, overflow, failure, durability issue, or queue non-equilibrium occurs.
- `PERIODIC_DIAGNOSTIC_COMPLETE_POLICY_PENDING`: the window is structurally eligible and R is computed, but numeric policy remains unapproved.
- `NOT_QUALIFIED_CAPACITY`: the known service-rate/freshness floor remains unmet.

None of these labels authorizes Gates B–D, paper start, arming, or orders. A later full natural-feed qualification needs its own predeclared acceptance contract and evidence packet.
