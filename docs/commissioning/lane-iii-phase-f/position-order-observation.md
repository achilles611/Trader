# Position and order observation

Positions normalize concrete contract, signed direction, absolute quantity, average price, alias, and timestamp. `UNKNOWN POSITION != FLAT` is structurally enforced.

Orders normalize provider order ID, contract, side, requested/filled/remaining quantity, mapped status, account, and timestamps. Existing working and partially filled orders are observed only. Neither client exposes an operation to create, modify, cancel, liquidate, flatten, reverse, bracket, or control a follower account.
