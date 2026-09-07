# Lane III account-isolation repair deployment handoff

## Boundary

- Starting commit: `8bbbd5a845a5e22e5401370fd20a63a8f36cf791`
- Branch: `codex/lane-iii-perpetual-corrective-20260906`
- Stash `7d1a212a61f431bfa8542f38d6459fe9e44fbad5` remained untouched.
- No deployment, V2 retry, order, manual runtime-state clear, NinjaTrader
  restart, ledger repair, epoch seal, or profile switch was performed.
- Failed V2 operation `profile-switch-2c0e6dbe5d8e4524b084ac7423163a64`
  remains immutable and must never be reopened, sealed, or reused.

## Evidence conclusion

The pre-change evidence freeze is at
`C:\Users\atlas\Documents\Trader\reports\evidence\lane-iii-account-isolation-20260907T1920Z`.
Its manifest correlates the 10:11 MDT LucidFlex25k round trip, the later foreign
Sim101 orders, and signed V1 ledger sequences 8629/8631/8634-8635. The Lucid
trade did not trigger Lane III `foreign_activity`; later non-Beelzebub Sim101
orders did.

## Repair

The execution AddOn now accepts account activity only when both facts hold:

1. the event/order/position account is the exact bound `Account` object; and
2. its nonblank native name equals `Sim101` with `StringComparison.Ordinal`.

That fence is applied to callbacks, native account snapshots, rehydration,
current-position lookup, reconciliation, command admission, and every native
create/submit/cancel path. Alternate-account objects are ignored before they
can touch the Sim101 foreign latch. A non-owned Sim101 order, execution, or
position retains the existing immediate lockout behavior.

Expected AddOn source fingerprint after this repair:
`43ccc356b48dfb8380da49139434cbdbb92f4cf699179cd4bf3e2c1bc44caed9`.

## Verification

- `tests/test_l3g_ninjascript_source.py`: 41 passed.
- Source, transport, and reconciliation-probe set: 68 passed, 26 subtests passed.
- `git diff --check`: passed.

## Deployment gate

Deployment must be a separate guarded operation. Rebuild the changed AddOn and
the backend artifact that embeds the expected fingerprint. Do not arm until an
independent HELLO reports the fingerprint above, exact `Sim101 / LOCAL_SIMULATION
/ MNQ SEP26`, quantity cap one, and live capital denied. A loaded AddOn change
genuinely requires NinjaTrader to load the rebuilt assembly; restart only if
the supported AddOn load path cannot establish that congruency otherwise.

Use a completely fresh V2 ledger epoch and supported profile-switch operation.
Do not reuse or mutate the failed operation. Start only through the corrected
executable and retain the normal flat/order-free/fresh-boundary commissioning
gates. Never clear a foreign latch manually: a new correctly loaded AddOn
boundary owns its own fresh lifetime state.
