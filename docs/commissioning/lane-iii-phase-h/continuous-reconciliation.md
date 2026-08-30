# Continuous reconciliation

At start and before every authority transition, compare the capability binding
with the broker account alias/class/hash, exact `MNQ SEP26` instrument,
position, quantity, owned orders, and foreign or unknown orders. Missing,
late, disconnected, malformed, or mismatched facts are `UNKNOWN`.

The sole safe flat proof is: connection healthy, complete position/order
snapshots, `position=FLAT`, `quantity=0`, and both owned and unclassified
working-order counts zero. The projection records the raw result and carries
no inferred flatness across a restart.
