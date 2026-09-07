# Beelzebub five-minute bias V1

`BEELZEBUB_FIVE_MINUTE_BIAS_V1` is an isolated Sim101 experiment. It never changes the default `BEELZEBUB_SCALPER_V2` identity and cannot share that profile's ledger epoch.

## Decision protocol

- The clock is aligned to exact five-minute UTC boundaries.
- A process that starts during a candle waits for the next boundary. It does not manufacture a mid-candle entry.
- The first admitted NinjaTrader callback at or after a boundary triggers one decision. The report retains the scheduled boundary, callback time, latency, and any missed-boundary count.
- Bias is the higher of the existing provisional bullish and bearish support scores. The profile does not apply the V2 entry threshold because this experiment asks for a directional choice each candle.
- A clear bias while flat creates one long or short entry decision.
- A clear bias matching the current position records `HOLD` and sends no order.
- An opposing bias records `REVERSE`, exits, waits for a signed flat/no-owned-orders reconciliation, then creates a new opposite-side entry decision. Same-event reversal remains forbidden.
- A tied bias holds an existing position. A tied bias while flat records `FIVE_MINUTE_BIAS_TIE_FLAT` and sends no order rather than inventing a direction.
- Every boundary decision includes both bias scores, both evidence-family summaries, the exact evidence provenance, prior/target position, and the last pre-boundary trade or quote-mid reference mark.
- The evidence is explicitly labeled `LATEST_AVAILABLE_PRE_CALLBACK_PROVISIONAL_EVIDENCE`, not a completed interval aggregate. The callback that triggers the boundary is excluded from that decision. A delayed callback therefore uses the latest already-recorded evidence before the callback and reports its latency; it does not retrospectively manufacture a completed candle.

The profile remains subordinate to all existing safety gates: exact `Sim101 / LOCAL_SIMULATION / MNQ SEP26`, quantity 1, authenticated execution bridge, fresh observations, complete reconciliation, one owned position, protective stop, daily loss, configured entry window, session close, and hard-flat deadline. Its dedicated risk artifact allows up to 128 session entries and uses an 86,400-second age ceiling, so neither the scalper's 12-entry cap nor its 540-second age limit interrupts an otherwise valid in-session five-minute hold. A safety gate may still flatten or leave the experiment flat until the next valid boundary. Live capital remains denied.

## Isolated startup

The normal operator path is Slim Mode's profile selector. **Switch & Start** automatically stops and fully reconciles the current paper runtime, proves controlled ledger closure, and creates a fresh ledger path, audit root, and epoch before this profile starts. The runtime rejects another profile's ledger epoch.

```powershell
.\scripts\start_beezconsole.ps1 `
  -PaperProfile BEELZEBUB_FIVE_MINUTE_BIAS_V1 `
  -LedgerPath N:\Beelzebub\experiments\five-minute-bias-v1\hot\lane_iii_paper.sqlite3 `
  -AuditRoot N:\Beelzebub\experiments\five-minute-bias-v1\audit `
  -LedgerEpoch L3G-PAPER-EPOCH-FIVE-MINUTE-BIAS-V1-20260904
```

The PowerShell command remains a maintenance-only fallback. An unknown profile name fails startup. With no explicit setting, ordinary startup restores the last backend-proven established run, including its exact profile, ledger, audit root, epoch, and checkout binding. V2 is only the initial default when no established selection exists.

## End-of-session analysis

After a clean operational stop reaches flat/no-owned-orders reconciliation, the runtime automatically writes immutable JSON, CSV, and Markdown artifacts beneath:

```text
<audit-root>\five-minute-session-analysis\
```

The analysis pairs each decision's pre-boundary reference mark with the next boundary's mark and classifies the interval as `WORKED`, `LOST`, `FLAT`, or `BLOCKED_OR_UNRESOLVED`. That endpoint diagnostic is explicitly pre-fee and pre-slippage. Completed position lifecycles separately retain realized P&L from authenticated entry and exit fills.

The same export can be reproduced after shutdown without modifying the ledger:

```powershell
.venv312\Scripts\python.exe -m src.l3g_paper.five_minute_analysis `
  --ledger N:\Beelzebub\experiments\five-minute-bias-v1\hot\lane_iii_paper.sqlite3 `
  --output N:\Beelzebub\experiments\five-minute-bias-v1\audit\five-minute-session-analysis
```

Pass `--session-id` to select an older closed session. The exporter opens SQLite read-only, requires a closed/stopped five-minute session, binds its identity to the closure record hash, and refuses to overwrite a same-name artifact with different content.
