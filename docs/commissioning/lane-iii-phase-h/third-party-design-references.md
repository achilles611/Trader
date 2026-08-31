# L3H design references

The implementation does not vendor third-party trading engines or copied
NinjaTrader code. It uses general clean-room design patterns: durable
write-ahead events, optimistic streams, explicit order states, reconciliation
before authority, signed/replay-safe messages, and native fail-closed risk.

Before a dependency is introduced, record its exact version, license, and
notice here. No new runtime dependency was added in this pass.
