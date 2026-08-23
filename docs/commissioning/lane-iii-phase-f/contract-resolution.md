# MNQ contract resolution and rollover

`MNQ` remains the strategy root. A provider contract must be an explicit CME expiry such as `MNQU6`, with provider contract ID, expiry, CME exchange, `0.25` tick size, and point value when supplied.

The authentic native contract observed in the commissioning captures is `MNQ SEP26`; no rollover occurred during this closure pass. L3-F requires a non-secret runtime configuration value and verifies exact identity; it will not observe a continuous synthetic instrument. An expiry mismatch, stale configured contract, or ambiguous lookup is `CONTRACT_NOT_FOUND`. Contract changes are explicit/auditable and never silent rollover actions. Separately authenticated expiry/exchange/tick/point-value metadata remains a future downstream-readiness fact, not an observer-ownership freeze gate.
