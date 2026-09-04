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

The profile remains subordinate to all existing safety gates: exact `Sim101 / LOCAL_SIMULATION / MNQ SEP26`, quantity 1, authenticated execution bridge, fresh observations, complete reconciliation, one owned position, protective stop, daily loss and consecutive-loss lockouts, 12-entry cap, 540-second maximum position age, configured entry window, session close, and hard-flat deadline. A safety gate may therefore flatten or leave the experiment flat until the next valid boundary. Live capital remains denied.

## Isolated startup

Stop and fully reconcile any existing paper runtime first. Use a new ledger path and a new epoch; the runtime rejects a V2 ledger if this profile is selected.

```powershell
.\scripts\start_beezconsole.ps1 `
  -PaperProfile BEELZEBUB_FIVE_MINUTE_BIAS_V1 `
  -LedgerPath N:\Beelzebub\experiments\five-minute-bias-v1\hot\lane_iii_paper.sqlite3 `
  -AuditRoot N:\Beelzebub\experiments\five-minute-bias-v1\audit `
  -LedgerEpoch L3G-PAPER-EPOCH-FIVE-MINUTE-BIAS-V1-20260904
```

Profile selection is process-start configuration. An unknown profile name fails startup. V2 remains the default when the setting is absent.

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
