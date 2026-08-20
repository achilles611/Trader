# Risk and operator model

Minimum simulation risk gates are maximum position quantity, one directional exposure, no pyramiding, maximum open orders, working-order age, and an externally configured realized-loss ceiling. Breaching the loss ceiling blocks new entries; exit and flatten authority remains available.

Operator commands use frozen L3-A semantics: `ARM`, `DISARM`, `PAUSE_NEW_ENTRIES`, `RESUME_NEW_ENTRIES`, and `FLATTEN`. The default simulator is disarmed and paused. `FLATTEN` disarms, latches new entries, requests relevant cancellations, and creates a simulated market flatten only for filled exposure. It is not flat confirmation; a valid later market observation must fill it.

Operator and risk authority outrank strategy signals. No strategy decision can arm, unpause, clear a flatten latch, change a hard risk limit, or override loss controls.
