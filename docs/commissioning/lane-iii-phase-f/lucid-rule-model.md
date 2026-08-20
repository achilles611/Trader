# Lucid rule model and compliance diagnostics

The versioned risk profile contains program/stage, nominal size, max quantity, loss/drawdown data, news status, flat/reopen/session times, and automation-policy status. Internal limits are tighter: 1 MNQ vs firm 20, $200 daily internal ceiling vs $1,000 maximum loss, and 15:58 ET vs 16:45 ET firm flat time.

News restriction and drawdown behavior are **UNKNOWN**, so they block any later live readiness. The current source provenance is the Lucid General FAQ observed 2026-08-20; policy must be rechecked before any future authority.

`FutureExecutionRateGuard` is a diagnostic-only future rate boundary for entry/change/duplicate attempts. `microscalping_diagnostic` calculates trade duration and the fraction of profitable amount from trades at or below five seconds. Neither creates a trade, changes Trader V0, or treats results as edge evidence.
