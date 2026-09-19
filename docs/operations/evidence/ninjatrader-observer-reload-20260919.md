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

## Post-F5 verification

The operator completed native F5 compilation after the original evidence above. The current verification was performed without starting BeezConsole or an execution bridge.

- Stable compiled copy: `C:\Users\atlas\Documents\NinjaTrader 8\bin\Custom\NinjaTrader.Custom.dll`, length `1,371,136`, last write `2026-09-19T22:30:56.7834101Z`, SHA-256 `1d749900b9f0cd8aa0eeeef7eec2cee6a0840fda2de5338f15fa4ced9d90303a`.
- Actual loaded assembly: `C:\Users\atlas\Documents\NinjaTrader 8\tmp\b090c521cffc483db90190bb46094506.dll`, length `1,371,136`, last write `2026-09-19T22:30:55.2804078Z`, SHA-256 `adfdfe544aa6748bbd4270a7aa90f42776e7f479b1e20114049ae71562272020`.
- The AddOn's runtime `AssemblyHash()` hashes the executing assembly location. The signed producer frame reported exactly `adfdfe544aa6748bbd4270a7aa90f42776e7f479b1e20114049ae71562272020`, proving the loaded identity is the temp assembly above.
- A bounded read-only receiver captured 134 frames; all 134 signatures verified. Every captured market/history frame identified `MNQ DEC26`. The producer source fingerprint was `1825e9890fe54a71f9b495e778e07c9e523ca1b36f66569b21f9ca4549102b73`, matching both the current Python expectation and the installed AddOn constant.
- The signed receiver window began after initial account attachment, so it captured only `Lucid25kflex01 / PROVIDER_EVALUATION` account frames. It did not claim a signed Sim101 frame.

## Sim101 / LOCAL_SIMULATION native state

The live NinjaTrader Control Center Accounts tab explicitly showed a connected `Sim101` row. The current AddOn maps the non-provider `Sim101` alias to `LOCAL_SIMULATION` in `Alias()`/`Class()`; this is the exact class mapping used by its account, position, and order publications.

The native Positions tab had zero data rows. The native Orders tab had zero data rows. The Sim101 Accounts row showed:

- buying power `$0.00`;
- cash value and net liquidation `$99,850.50`;
- excess intraday and initial margin `$99,850.50`;
- intraday and initial margin `$0.00`;
- gross realized, realized, unrealized, and total PnL `$0.00`.

The native log records `Automated trading disabled` and `Order entry hot keys disabled`. The temporary read-only receiver was stopped; no listener remains on `48135`, `48136`, `48137`, or `8090`, and no BeezConsole process is running.

Conclusion: current-source observer reload and actual loaded producer identity are verified. Sim101 is natively connected, flat, has no working orders, and is explicitly classified by the current observer contract as `LOCAL_SIMULATION`; execution remains disarmed. A fresh signed Sim101 account snapshot and fresh DEC26 qualification remain separate data-capture gates because the bounded receiver attached after startup.
