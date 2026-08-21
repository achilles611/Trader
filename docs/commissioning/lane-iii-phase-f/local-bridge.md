# Local bridge

Transport is newline-delimited JSON over TCP with the NinjaTrader process acting only as a client. Beelzebub’s receiver binds exactly `127.0.0.1:48135`; `0.0.0.0`, LAN hosts, remote hostnames, and a reverse channel are rejected by configuration.

The AddOn writes observations to loopback and never reads from its stream. The Python receiver has no send/dispatch API. The schema recognizes observation types only—there is no command type. Frames carrying provider account IDs, passwords, tokens, secrets, or authorization fields are refused before persistence/normalization.

The receiver reads at most one configured frame allowance plus one byte before rejecting a newline-free overrun and closing that connection. It assembles only complete frames; malformed UTF-8, malformed/duplicate-key JSON, unknown top-level fields, unknown aliases, out-of-order records, incomplete disconnect tails, and oversized frames are refused. The only persistable report is a sanitized aggregate—never a raw frame, account identifier, payload, token, or credential.

`LOCAL_BRIDGE` is a separate provider-health stream. It records `CONNECTING`, `HEALTHY`, and `DISCONNECTED` as a peer connects, disconnects, or the listener stops. It cannot make account, position, order, market, or strategy state healthy, flat, or ready.
