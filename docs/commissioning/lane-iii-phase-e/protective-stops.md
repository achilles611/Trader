# Protective stops

After the first actual entry fill, `l3e` creates a `PROTECTIVE_STOP` for actual simulated exposure. Its fixed trigger is the fill price minus configured ticks for a long, or plus configured ticks for a short. Added partial exposure is covered at the original trigger; confidence and strategy signals cannot widen it.

A long stop triggers on a valid bid at or below trigger; a short stop triggers on a valid ask at or above trigger. It then fills as a marketable exit at the current obtainable bid/ask plus adverse configured exit slippage. A gap through the trigger therefore cannot receive an impossible trigger-price fill.

Protective stops have higher matching priority than strategy exits and entries. They are distinct ledger events and are retired only through a cancellation request after flatness—not by pretending an exit signal made the position safe.
