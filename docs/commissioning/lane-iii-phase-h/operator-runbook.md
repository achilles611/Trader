# L3H operator runbook

1. Run `scripts/bootstrap-l3h.ps1`. It creates ACL-restricted local key roots
   only; it creates neither a capability/binding nor a listener/order.
2. Run `scripts/l3h_deploy_ninjatrader.ps1 -InstallSource`, then compile the
   source visibly in NinjaTrader. Do not accept broker agreements or arm it.
3. Run `scripts/l3h_verify_install.ps1` and `scripts/l3h_status.ps1`; require
   repository/installed/DLL/runtime provenance and a fresh complete flat
   native snapshot with no unclassified orders.
4. Generate a local capability and signed native binding only after the exact
   account and rules have been independently reviewed. Do not place either in
   Git, browser storage, or a shared drive.
5. Conduct the entire installed Sim101 matrix. A failure remains a blocker;
   source/unit tests do not substitute for it.
6. The permanent kill paths are BeezConsole, NinjaTrader's `KILL / FLATTEN /
   DISARM` menu item, and `scripts/l3h_kill.ps1`. Each must be proven on
   Sim101. Joseph alone may deliberately hold the enabled canary control.

No script or scheduled task may arm or start L3H. `UNKNOWN` requires quarantine
and reconciliation, never a retry.
