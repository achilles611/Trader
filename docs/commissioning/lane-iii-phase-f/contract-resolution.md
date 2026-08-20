# MNQ contract resolution and rollover

`MNQ` remains the strategy root. A provider contract must be an explicit CME expiry such as `MNQU6`, with provider contract ID, expiry, CME exchange, `0.25` tick size, and point value when supplied.

The configured active provider contract is **UNKNOWN**. L3-F requires a non-secret runtime configuration value and verifies exact identity; it will not observe a continuous synthetic instrument. An expiry mismatch, stale configured contract, or ambiguous lookup is `CONTRACT_NOT_FOUND`. Contract changes are explicit/auditable and never silent rollover actions.
