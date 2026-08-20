# Authentication and secrets

Secrets are runtime-only environment values:

```text
L3F_TRADOVATE_USERNAME
L3F_TRADOVATE_PASSWORD
L3F_TRADOVATE_CID
L3F_TRADOVATE_SECRET
```

Registered Tradovate application ID, version, and device ID are non-secret deployment metadata and must be supplied separately. Tokens are redacted in representations, errors retain fixed error codes instead of provider response text, and sanitized fixtures redact token/password/secret/CID-style fields. No secret is persisted by L3-F.

Authentication status: **UNAVAILABLE** pending account-specific credentials and API entitlement. No authentication request has been made.
