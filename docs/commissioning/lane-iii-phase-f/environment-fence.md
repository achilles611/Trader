# Environment fence

`TradovateEnvironment` is either `TRADOVATE_DEMO` or `TRADOVATE_LIVE`. Each binds exact REST and WebSocket URLs. A session or account with the wrong environment is rejected as `ENVIRONMENT_MISMATCH`; demo failure never triggers a live attempt.

Current configured environment is `TRADOVATE_DEMO` (not a claim that an actual demo connection is established). `OBSERVE_ONLY` is the sole L3-F mode.
