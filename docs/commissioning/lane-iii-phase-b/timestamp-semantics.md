# Timestamp and ordering semantics

`EventTimestamps` keeps clocks separate and normalizes each supplied value to ISO-8601 UTC:

1. `exchange_time` is the event-time authority when supplied.
2. `provider_time` is used only when exchange time is unavailable.
3. `local_receipt_time` is always recorded and is the final deterministic fallback.

Missing source clocks remain `null`; no precision is invented. The fallback clock is labelled in bars and session output. Receipt time makes later latency/staleness analysis possible without asserting that local arrival establishes exchange order.

Provider sequence is authoritative for per-stream order when available. Equal timestamps are ordered by sequence, not incidental wall-clock arrival. A duplicate or late sequence never alters state. A forward sequence gap is explicit and makes the affected reconstruction or flow incomplete/gapped. Without sequence, a snapshot may be retained as `INCOMPLETE`, but incremental depth is refused as non-authoritative.

Canonical capture replay uses persisted record order and re-applies these same sequence rules. This makes ties reproducible while preserving the documented limitation of an unsequenced feed.
