# NinjaTrader read-only observer reload evidence

Date: 2026-09-19 (America/Denver)
Status: `NATIVE_RELOAD_NOT_COMPLETED_EVIDENCED`
Execution authority: disarmed; no execution bridge, order-capable bridge, or paper-start request was activated.

## Offline source and backup proof

- Repository AddOn: `ninjatrader/NinjaScript/AddOns/BeelzebubReadOnlyAddOn.cs`, SHA-256 `82937fe51a32862bd70ba52b5b8dcfd84efa6dc15be8d84d1097a8c1349499ff`.
- Repository Indicator: `ninjatrader/NinjaScript/Indicators/BeelzebubReadOnlyMarketObserver.cs`, SHA-256 `bb5b35aaff1cc064065e093d9ec661f4f081e86ae32ee7383df1bce13c0039ff`.
- Installed AddOn: `C:\Users\atlas\Documents\NinjaTrader 8\bin\Custom\AddOns\BeelzebubReadOnlyAddOn.cs`, SHA-256 `82937fe51a32862bd70ba52b5b8dcfd84efa6dc15be8d84d1097a8c1349499ff`.
- Installed Indicator body: `C:\Users\atlas\Documents\NinjaTrader 8\bin\Custom\Indicators\BeelzebubReadOnlyMarketObserver.cs`, repository body parity verified; native generated wrapper preserved.
- Preserved backups: eight AddOn `.unattended-before-*.bak` files and five Indicator `.unattended-before-*.bak` files.
- Bundled Roslyn validation: `success=true`, `errors=0`, `warnings=1267`; no DLL was copied.

## Compiled artifact identity

Artifact: `C:\Users\atlas\Documents\NinjaTrader 8\bin\Custom\NinjaTrader.Custom.dll`

- Assembly metadata: `NinjaTrader.Custom, Version=8.1.7.0`.
- File length: `1,389,568` bytes.
- Last write: `2026-09-18T02:55:39.5916324Z`.
- SHA-256: `a9db49334f7f8ed8992eb0dcd2ab842c4397d826b52dec2f8908542133f64fd9`.
- Current expected observer source fingerprint: `7f31b43e1ba5c7e0be3fdd4e9f56ce980814cdb6bf1cbfd19fd411db5fb521c6`.
- Current fingerprint in DLL: absent in both UTF-8 and UTF-16 string scans.
- Observer implementation names `BeelzebubAutomaticMarketObserver`, `ShiftKnownPositions`, and `DepthPrefix` are present, but that is not current-source identity proof.

## Native log evidence and root cause

`C:\Users\atlas\Documents\NinjaTrader 8\log\Install.xml` records:

- `ClearedCache`: `NinjaScript editor cache cleared` at `2026-09-19 15:41:26`.
- `CompiledCustomAssembly`: `2026-04-22 09:09:50`.

The most recent non-empty native trace, `trace\trace.20260919.00000.txt`, records:

- line 32: NinjaTrader loaded the existing `NinjaTrader.Custom.dll`.
- line 76: the supported NinjaScript Editor compile command is `F5`.
- lines 125 and 268: the existing `LucidFlex25k` connection connected and later disconnected.
- line 251: the prior read-only AddOn was finalized on shutdown.

There is no compile event, compiler error row, or current observer identity load event. This is not a compiler failure: the supported native compile was never initiated. The source watcher did not rebuild the assembly, which is why the commissioning wait could only time out.

## Bounded native startup

The fixed executable `C:\Program Files\NinjaTrader 8\bin\NinjaTrader.exe` was started once while the source was already installed and execution remained disarmed. It reached only the `Welcome` window, not the authenticated Control Center/editor. It was then closed gracefully. Final process and listener checks were empty for NinjaTrader and ports `8090`, `48135`, `48136`, and `48137`.

The account context visible in the prior authenticated native run was `LucidFlex25k`; the log also reports `Global simulation mode disabled` and `Automated trading disabled`. That is not the required fresh `Sim101 / LOCAL_SIMULATION` proof.

## Supported next action

The supported procedure is intentionally visible and bounded:

1. Keep NinjaTrader closed while installing source with `tools/commission_ninjatrader_observer.ps1 -InstallSource`; preserve the timestamped backups.
2. Open NinjaTrader in the existing user profile and complete authentication manually if the Welcome window appears.
3. Open `New -> NinjaScript Editor`, select the installed observer source, and press `F5`/Compile. Require zero error rows.
4. Confirm `Install.xml` has a new `CompiledCustomAssembly` time and verify the DLL contains the current source fingerprint. Do not copy a DLL into `bin\Custom`.
5. For observation-only proof, use the exact `MNQ DEC26` configuration and `Sim101 / LOCAL_SIMULATION`; attach only `BeelzebubReadOnlyMarketObserver`. Do not attach or enable an execution bridge.
6. Capture a bounded fresh observer session and verify the loaded source/build identity, account snapshot, flat/no-order state, and current DEC26 quote/trade/depth continuity.

## Remaining blockers

- **Unfinished native implementation proof:** the visible NinjaScript Editor compile/reload and current compiled identity verification still require the manual F5 action. The local Windows Computer Use helper was not configured for this desktop, and the Welcome screen required authentication; no unsafe UI workaround was used.
- **Current account context:** the available native run auto-connected `LucidFlex25k`, not `Sim101 / LOCAL_SIMULATION`. A fresh exact Sim101 account/position/working-order/reconciliation check is still required.
- **Fresh market data:** after native reload and exact Sim101 context, a new DEC26 observer session with warm-start/history, quote/trade/depth continuity, and measured durable latency is required. Historical DEC26 replay cannot substitute for this.

No blocker is an execution bridge or a code-level observer compile error. Preparation remains disarmed.
