# L3H operator runbook

1. Run `scripts/l3h_status.ps1`; resolve every blocker exactly.
2. Verify installed AddOn source and DLL parity; confirm a fresh complete flat
   snapshot and no unclassified orders.
3. Generate a local capability only after an authorized account and rules are
   independently available. Do not place it in Git or browser storage.
4. Confirm the UI still reports disarmed and the one-control start is enabled
   only after the final gate refresh.
5. Joseph alone may deliberately press and hold `START LIVE — 1 MNQ CANARY`.

No script or scheduled task may arm or start L3H. `UNKNOWN` requires quarantine
and reconciliation, never a retry.
