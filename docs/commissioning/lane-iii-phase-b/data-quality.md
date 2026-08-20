# Data quality and recovery

L3-B does not reduce data health to a boolean. `DataQuality` has `HEALTHY`, `STALE`, `GAPPED`, `RECOVERING`, `INCOMPLETE`, and `INVALID`.

`MarketDataPipeline.staleness` accepts distinct caller-selected maximum ages for trade, quote, and book. It reports each family independently and does not embed strategy thresholds. A valid but unsequenced observation is `INCOMPLETE`; a full bounded buffer is `INVALID` and raises; rejected provider input can mark its reconstructor invalid; missing sequence ranges are `GAPPED`.

Reconnect explicitly resets stream sequence guards, marks signed flow/session context incomplete, and moves depth to `RECOVERING`. Existing incrementals do not bridge the disconnected interval. A valid sequenced recovery snapshot is required before depth can be healthy. Cumulative delta is `None` once flow is unknown or gapped, and resets at the declared CME session boundary rather than stitching sessions together.

Session context uses America/Chicago CME-style 17:00 starts, 08:30 cash open, and a configurable opening range. It is DST-aware; if the local Python runtime lacks IANA zone data, a narrow America/Chicago US Central rule implementation is used rather than silently applying a fixed offset. Tape-only state leaves prior settlement as unavailable because it cannot be observed from trades alone.
