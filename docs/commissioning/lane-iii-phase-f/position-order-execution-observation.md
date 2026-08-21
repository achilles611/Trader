# Position, order, and execution observation

Positions, orders, and executions are distinct records. A position is not inferred flat from silence; an empty/flat response must be explicit. Order updates do not stand in for execution events.

The AddOn observes existing account state and callbacks only. It provides no operation to create, submit, change, cancel, flatten, reverse, attach brackets, or alter ATM strategies.
