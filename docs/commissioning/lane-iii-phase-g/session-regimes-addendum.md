# Lane III Phase G session-regimes addendum

Date: 2026-08-24
Branch: `codex/l3g-session-regimes`
Baseline: `5116a22f8a9a65eabb3709bf1e56b050f1f15a2a`

This is an addendum to, not a replacement for,
[`commissioning-report.md`](commissioning-report.md). The historical report
correctly records New York-only behavior and must not be read as evidence that
Asia was enabled or commissioned.

## Change under verification

`src/l3g_paper` now owns immutable `America/New_York` profiles for
`ASIA_GLOBEX`, `NEW_YORK_RTH`, and `OFF_SESSION`. Asia begins Sunday through
Thursday at 18:00 New York time and uses the following calendar date as its
trade date. New York RTH remains a distinct 09:30–16:00 New York regime.

Each paper observation envelope, evidence object, decision, intent, risk
grant, command, ledger record, and transport receipt carries the session kind,
session ID, trade date, profile hash, and generation. Evidence state is cleared
at every regime change. The trade-date risk state, including the $200 loss
limit and entry cap, is deliberately shared by the prior evening's Asia session
and the following New York session.

The signed NinjaTrader AddOn now rejects entry commands outside its compiled
Asia or New York entry window, including wrong session kind, session ID, trade
date, or profile hash. Exit, cancellation, reconciliation, and emergency
flatten commands remain available outside entry windows. Sim101 remains the
only mutable account; Lucid and live capital remain denied.

Holiday handling is explicit and conservative. Known US holiday and possible
early-close candidates default to `HOLIDAY_OVERRIDE_REQUIRED`, which fails
closed with `HOLIDAY_SESSION_UNVERIFIED` until an operator records a verified
override. This fence is not a claim about a particular CME schedule; no hours
are inferred from a quiet feed or from the date alone.

## Engineering evidence completed in this patch

- Event-time session classification across Asia midnight, New York cutoffs,
  weekends, holidays, and DST offsets.
- Session-isolated provisional evidence and mixed-source refusal.
- Cumulative Asia-to-New-York trade-date risk gates.
- Session-tagged hash-chain ledger filtering.
- Python transport and NinjaScript source fences for signed session identity.
- Installed AddOn source hash equals the repository hash
  `817953359DEC6AEE9326DF97FE61BE74F77A836C1A38189780507F798D79FF4C`.
- The installed AddOn source compiled as a standalone .NET Framework library
  against the local NinjaTrader assemblies. The full `NinjaTrader.Custom`
  project build remains unavailable on this machine because no .NET SDK is
  installed; no full-assembly compilation claim is made.

## Commissioning status

No authentic Asia or New York market-session commissioning pass is claimed by
this addendum. The runtime was stopped while disarmed, Sim101 was reconciled
flat with zero working orders, and Lucid mutations were zero before modification.
The next authentic pass must start flat, use fresh session-specific warmup and
arming, and record its outcome under the exact session ID:

| Scope | Status |
| --- | --- |
| Core paper execution | installed source hash and standalone AddOn compile verified; full custom-project build pending local SDK availability |
| `ASIA_GLOBEX` | `PENDING` |
| `NEW_YORK_RTH` | `PENDING` |
| All enabled sessions | `NOT_COMMISSIONED` |

An authentic natural decision is required for each regime. Thresholds and
session windows must not be changed to obtain a trade.

The 2026-09-02 first-class London successor is specified separately in
[`london-v1.md`](london-v1.md); this historical addendum and its commissioning
claims remain unchanged.
