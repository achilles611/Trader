# NinjaTrader replay provenance

Replay evidence must retain its native source ordering as
`NT_REPLAY_FILE_ORDER`. It must not be relabeled provider sequence or exchange
sequence. Import records must preserve file hash, capture time, parser version,
instrument identity, and any gap/parse refusal. Replay calibration cannot prove
live market-data completeness.
