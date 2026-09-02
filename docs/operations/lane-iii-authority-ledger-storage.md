# Lane III authority-ledger storage policy

The production Lane III paper ledger is a compact permanent authority and
safety record. It retains lifecycle, authority-capable decisions, intents,
risk grants and denials, commands and receipts, owned order/fill/protection
events, position and reconciliation truth, incidents, realized P&L, integrity
metadata, and clean-session boundaries.

Raw `QUOTE`, `TRADE`, and `DEPTH` envelopes do not enter this ledger. Neither
do derived evidence items or no-effect decisions. Those values still drive the
in-memory observer, policy, warmup, mark-to-market, and classification paths.
Scientifically useful bulk persistence is disabled until a separate bounded,
time-partitioned store is commissioned; it may not fall back into the authority
ledger. Low-volume commissioning warmup attestations retain compact source IDs
and hashes, not raw payloads.

`persistence_policy` in paper-ledger health is the runtime proof. Production
must report raw market observations, derived evidence, and no-effect decisions
as `DISABLED`. The suppressed counters are diagnostic only and reset with the
process. Tests may explicitly opt into the former high-volume persistence path
only to exercise writer/checkpoint mechanics; production never does.

The authority ledger warns at 32 GiB and latches a capacity fault at 40 GiB.
It also warns below 8 GiB free, hard-fails below 4 GiB free, and warns when the
measured growth rate leaves less than 24 hours of effective runway. A warning
degrades readiness; a latched fault blocks new authority. Stop, flatten,
reconciliation, and safety evidence remain the priority over new exposure.

Clean reset genesis is one-shot and fail-closed. Run
`scripts/lane_iii_clean_reset_genesis.py` only against a nonexistent main file
and nonexistent WAL/SHM/journal sidecars, with an explicit epoch, compact reset
receipt, matching checkout/build/runtime full SHA values, and installed AddOn
fingerprints. The script proves sequence 1 is the sole genesis record and then
performs a controlled TRUNCATE shutdown before deployment.
