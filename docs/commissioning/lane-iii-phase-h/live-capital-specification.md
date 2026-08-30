# L3H live-capital specification v0

The first L3H path permits exactly one account, one concrete `MNQ SEP26`
contract, maximum absolute quantity one, one pending entry, and one completed
round-trip canary per commissioning epoch. It permits no averaging, pyramiding,
or same-event reversal.

Every entry requires all of the following under the runtime admission lock:

1. a current signed capability with an eligible account class;
2. exact policy, risk, rule, source, NinjaTrader-build, account, and connection hashes;
3. a fresh complete broker snapshot proving flat and zero orders;
4. a durable command seal and idempotency reservation; and
5. explicit held operator activation from the correct nonce family.

Failure before acknowledgement moves to `QUARANTINED`; recovery reconciles and
does not resend the entry. Only a new reviewed epoch can follow a completed
canary. Mechanical readiness and a profitable strategy are separate claims.
