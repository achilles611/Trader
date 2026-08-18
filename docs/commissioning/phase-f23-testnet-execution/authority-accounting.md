# Phase F.2/F.3 Authority Accounting

This record is additive to the frozen F.0 and F.1 manifests. It does not grant
Lane II execution, trading, or live-capital authority.

| Principal or capability | State after F.2 | State after F.3 implementation |
| --- | --- | --- |
| Lane II scientific authority | false | false |
| Lane II prediction authority | false | false |
| Trader V0 signal authority | exact F.1 version only | exact F.1 version only |
| Lane II execution authority | false | false |
| Lane II trading authority | false | false |
| Lane II live-capital authority | false | false |
| Phase D execution sovereignty | true | true |
| Phase D simulator capability | `LANE_II_SIMULATOR` | retained |
| Phase D Hyperliquid transport | false | implementation complete; external commissioning blocked |
| Hyperliquid mainnet capability | false | false |

The operational dependency direction is one-way:

```text
Trader V0 → F.2 bridge → Phase D → ExecutionAdapter → venue
```

No signing key, signer, nonce source, transport client, cancellation API, or
venue credential is present in Lane II.
