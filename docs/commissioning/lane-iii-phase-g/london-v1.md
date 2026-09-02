# London V1 first-class session

Date: 2026-09-02

London V1 extends the existing Lane III-G Sim101 paper envelope. It does not
change the frozen Lane III science, Standard confluence policy, risk,
protection, quantity, reconciliation, gateway, or live-capital boundary.

## Canonical definition

| Field | Value |
| --- | --- |
| Session ID kind | `LONDON` |
| Parent family | `EUROPE` |
| Contract identity | `MNQU6:LONDON:<London trade date>` |
| Timezone | `Europe/London` |
| Window | `[08:00, 11:30)` |
| Entry start | `08:00:00`, inclusive |
| Entry cutoff/session end | `11:30:00`, exclusive |
| Valid start days | Monday-Friday |
| Policy/profile | `l3g-paper-policy-v0` / `STANDARD` (`STANDARD_V1` contract) |
| Effective entry threshold | `0.65` |
| Account/instrument/quantity | `Sim101` / `MNQ SEP26` / maximum 1 |
| Live capital | `DENIED` |

All boundary conversion uses the IANA `Europe/London` rules. The NinjaTrader
Windows runtime resolves the equivalent `GMT Standard Time` registry zone only
as its platform adapter; serialized identity remains `Europe/London`. No fixed
UTC or New York offset represents London.

Classification precedence is `ASIA`, `LONDON`, `NEW_YORK_RTH`, then
`NY_AFTER`. The current windows do not overlap. The explicit precedence is
nevertheless part of V1 so future timezone/calendar changes cannot silently
change identity. London uses the existing conservative market-calendar fence:
weekends are closed, and known US holiday/possible early-close candidates
require the same explicit verified override as the existing sessions.

## Isolation and compatibility

London has its own exact session kind, Europe family, ID, profile hash,
generation, observer classification, warmup latch, reset lifecycle, evidence,
P&L bucket, risk-family bucket, ledger metadata/filter, signed transport fence,
API status, Slim status, scheduler templates/selectors, and Full/Slim console
labels. A transition into or out of London closes the prior session and clears
provisional strategy and commissioning warmup state. A process restart inside
London starts cold. Asia and New York state cannot satisfy a London warmup.

The ledger schema already stores session kind/family/profile fields as
append-only text, so London requires no table rewrite or historical migration.
Existing Asia, New York, NY After, off-session, and preserved unknown rows are
not reclassified. Their compiled profile hashes remain unchanged. New London
rows use profile hash
`db211b6665e873fc3bf0b93db76210b25d154893ca1d5ca15ef0d7d6bea233cc`.

## Authentic commissioning boundary

Engineering fixtures, tests, and off-session read-only diagnostics can establish
`CODE_VERIFIED`; they cannot establish `LONDON_COMMISSIONED`. That status
requires authentic accepted observations during `[08:00, 11:30)
Europe/London` to warm all three required families in the exact London session
and generation, followed by a successful read-only commissioning rehearsal and
a fresh incremental ledger verification. No replay, clock alteration, borrowed
warmup, ARM, operational start, or order is permitted for this proof.

If authentic London evidence is unavailable, the continuation procedure is:

1. Before 08:00 London, start only the established BeezConsole/NinjaTrader
   runtime and verify exact deployed SHA/AddOn provenance, active observer,
   `READY_DISARMED`, `Sim101 / LOCAL_SIMULATION / MNQ SEP26`, `FLAT / 0`,
   complete snapshots, zero owned/working orders, and live capital `DENIED`.
2. Between 08:00 inclusive and 11:30 exclusive, keep trading disarmed and wait
   for the current `LONDON / EUROPE` generation to show authentic
   `STRUCTURAL_CONTEXT`, `ORDER_FLOW`, and `RESTING_LIQUIDITY` warmup.
3. Select **Run Read-Only Commissioning Rehearsal** in Full Console. Do not
   select ARM, Atomic Commissioning Start, or Start Paper Trading.
4. After a `READY` rehearsal, run **Verify Ledger Now - Fast / Incremental**
   and retain the PASS that covers the rehearsal-era tip with zero unverified
   tail. Reconfirm flat/order-free/disarmed state before recording
   `LONDON_COMMISSIONED`.
