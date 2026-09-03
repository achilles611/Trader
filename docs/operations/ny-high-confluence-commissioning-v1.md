# NY high-confluence commissioning V1

This profile is a one-pass `Sim101 / LOCAL_SIMULATION / MNQ SEP26 / quantity 1`
commissioning configuration. It is not an edge claim and cannot authorize live
capital.

## Immutable entry contract

- Profile: `NY_HIGH_CONFLUENCE_COMMISSIONING`
- Version: `NY_HIGH_CONFLUENCE_COMMISSIONING_V1`
- Session: `NEW_YORK_RTH` only
- Minimum support: `0.675`
- Minimum winner-over-loser dominance: `0.10`
- Required positive families: all three of `STRUCTURAL_CONTEXT`, `ORDER_FLOW`,
  and `RESTING_LIQUIDITY`
- Blocking contradictions: denied
- Maximum entries per session and trade date: `1`

Atomic Commissioning reserves exclusive ownership and waits for the first fresh
qualifying policy decision. It does not manufacture a direction or submit an
unconditional long. The originating decision ID, observation IDs, local
sequences, payload hashes, support, dominance, and family summary are copied
into the commissioning authority record.

## Position contract

- Protective stop distance: `25.00` MNQ points / `$50.00` maximum trade risk
- Maximum position age: `3,600` seconds
- Hard flat: `15:58 America/New_York`
- Strategy-generated retention exits: ignored while commissioning owns the
  position
- Still authoritative: protective stop, stale-input emergency exit, operator
  Commissioning Exit, maximum-age exit, hard-flat exit, and reconciliation
  fail-closed behavior

## Preserved predecessor

The preceding scalping profile remains available at Git ref
`archive/beelzebub-scalper-v1-20260903`. Its pre-change ledger/runtime bundle is
`D:\BeelzebubData\recovery\ny-high-confidence-prechange-20260903T123644Z`.
The bundle manifest SHA-256 is
`50DBED576EB8282C2DB8BD6D63160B5230DB7B67BA9D28587C64736AC09D551C`.
