# L3H commissioning state machine

`BLOCKED -> READY_DISARMED -> ARMED_FLAT -> COMMAND_SEALED -> (ACKNOWLEDGED | QUARANTINED)`

`READY_DISARMED` exists only after every category gate and fresh broker flat
proof pass. `COMMAND_SEALED` is durable before dispatch. Any lost transport or
acknowledgement becomes `QUARANTINED`; it cannot transition directly to ready.
After the sole completed round trip, the epoch becomes
`LIVE_CANARY_COMPLETE` and cannot admit another entry.
