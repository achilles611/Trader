# Order lifecycle

An admitted `LONG` or `SHORT` creates a `MARKET_ENTRY` in `WORKING` state. It becomes eligible only after configured processing, submission, and venue latency. It may then remain working, become `PARTIALLY_FILLED`, or become `FILLED`; requested, filled, remaining, and average fill price are distinct fields.

`EXIT` records `STRATEGY_EXIT_REQUESTED`, requests cancellation of a working entry, and creates a `MARKET_EXIT` only for actual filled exposure. It does not report flatness. Cancellation has its own event-time delay: `CANCEL_REQUESTED` is not `CANCELLED`, and a market observation confirms cancellation. A partial entry followed by exit therefore remains partial exposure until an exit fill actually completes.

The simulator never reverses an existing position from an opposing L3-D signal. L3-D already emits exit-only during an active opposing thesis; `l3e` honors that model by requiring confirmed flatness before any later opposite entry.
