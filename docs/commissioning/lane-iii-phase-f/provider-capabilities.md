# Provider capabilities — L3-F2

| Provider path | Status | Evidence |
| --- | --- | --- |
| Direct Tradovate API | UNAVAILABLE_FOR_THIS_ACCOUNT | Operator found no API Access tab/add-on on Tradovate Prop infrastructure. No bypass/scraping attempt was made. |
| NinjaTrader 8 Desktop | AUTHENTICALLY_COMMISSIONED_OBSERVER | `MNQ SEP26` quote, trade, account, and aggregated L2 callbacks crossed the loopback bridge in the 2026-08-20 captures. |
| NinjaTrader bridge AddOn | AUTHENTICALLY_COMMISSIONED_OBSERVER | The installed AddOn and indicator compiled and produced 27,492 L1/account observations, followed by 51,464 observations including 46,214 depth frames, with zero rejections. Installed source hashes still match the repository on 2026-08-23. |

NinjaTrader is the frozen observation provider. There is no automatic fallback from an unavailable direct provider to another path and no execution capability in this boundary.
