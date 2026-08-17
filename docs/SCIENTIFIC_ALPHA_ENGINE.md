# Beelzebub Scientific Alpha Engine

> Wallets are sensors. Indicators are knowledge. Trades are experiments. Models are accumulated evidence.

The D.5 scientific engine replaces the old wallet-score-to-copy mental model. A wallet fill is an observation with provenance. It can materialize versioned features, support a falsifiable hypothesis, and contribute to a model only after historical and forward evidence. It never has direct order authority.

## Scientific loop

`OBSERVE → FEATURES → DISCOVER → REGISTER → HISTORICAL TEST → FORWARD SHADOW → PROMOTE → MODEL → CALIBRATE → SHADOW DECIDE → DRIFT / LEARN`

Phase D.6 supplies the durable SQLite queue and one long-lived worker that
operates this loop incrementally. Public wallet and market observations are
persisted with provenance, features/outcomes are immutable, and research jobs
are fingerprinted so an old job cannot silently read future evidence. See
[the D.6 operating guide](PHASE_D6_AUTOMATED_SCIENCE.md) for queue/restart,
CLI, resource, calibration, and drift details.

Features carry an ID, version, exact definition, units, source inputs, freshness requirements, missing-data semantics, timestamp, and code SHA. Existing semantic versions cannot be changed. Wallet sensors expose measurable short-horizon behavior (holding horizon, latency survivability, MAE/MFE, specialization, alpha half-life and evidence confidence) rather than calling wallet profit a trade probability.

Hypotheses are registered immutable propositions. Their definition includes feature versions, threshold/market/regime conditions, costs and latency, temporal discovery/validation ranges, purge/embargo, FDR family, data fingerprints, code SHA, and explicit success/failure criteria. A change in any scientific semantic creates a new version. The graveyard permanently retains rejected hypotheses, effects, uncertainty, cost assumptions, fingerprints, and code provenance; similar prior rejections are surfaced at registration.

Historical tests use temporal train/validation separation. Prediction horizons plus embargo separate adjacent partitions. Net outcomes subtract fees, spread, slippage, impact, and arrival-time latency costs. Statistical evaluation supports deterministic block-aware sign resampling and Benjamini–Hochberg q-values within the declared family. Statistical significance alone cannot promote an indicator.

Forward-shadow predictions are persisted before their horizon closes, then receive a separate immutable outcome. They start with no capital authority. Indicators and models are immutable versioned objects with predecessor relationships and provenance. Indicator states are `EXPERIMENTAL`, `VALIDATED`, `ACTIVE`, `DEGRADED`, and `RETIRED`; models are candidate/shadow/simulation versions, never silently overwritten or refit online.

## Confidence and decay

`model_confidence` means evidence quality, not profitability probability. It uses a transparent geometric-style aggregation of sample strength, validation, walk-forward stability, regime coverage, provenance, calibration, FDR quality, temporal stability, and mature forward evidence. Missing/immature forward evidence applies an explicit experimental ceiling rather than forcing historical evidence to zero.

`trade_confidence` is the calibrated current probability of the defined future *net* outcome. It is updated from validated, calibrated evidence such as confirmation absence, corroboration, market change, cost deterioration, or regime state. Time is an input to learned evidence, not a fixed per-minute subtraction.

`effective_confidence = 0.5 + (trade_confidence - 0.5) * model_confidence`.

Alpha survival is `max(edge_t, 0) / max(edge_0, epsilon)`. Curves and half-life are empirical by indicator/symbol/regime/latency bucket where data exists. They do not replace live trade confidence.

## Decision and risk boundary

The decision gate requires a simulation/shadow-eligible model, validated indicator versions, positive expected net edge, sufficient effective confidence, a supported regime, and independent risk sizing. It records model/indicator versions, all three confidence values, costs, net edge, alpha survival, risk budget, notional, derived leverage, MAE/MFE estimates, reasons, and source-observation provenance.

The default maximum position life is 600 seconds and cannot be configured higher. Hard risk exits override confidence. Entry/exit thresholds use hysteresis. A source exit is evidence, not an unconditional command. Source leverage is ignored; paper leverage is derived from risk budget, adverse-move quantile, equity, exposure, and configured caps.

The engine produces only simulation/shadow decisions. The frozen `ExecutionEngine` remains `SIMULATOR_ONLY`, D.4 shadow remains public read-only context, and D.5 mainnet/live authority is not introduced.

## Modules

- `features.py`, `hypotheses.py`, `experiments.py`: scientific inputs, registrations, temporal testing and forward records.
- `indicators.py`, `scientific_models.py`, `confidence.py`, `alpha.py`, `decision.py`: promotion, versions, uncertainty, decay and gated decisions.
- `science_repository.py`, `science_storage.py`, `performance.py`: append-oriented SQLite evidence, hot/cold archival, and latency instrumentation.
- `science_read_model.py`: read-only Control Center projection.

Run `python main.py copy-storage-status` to check storage, `copy-storage-migrate --source <legacy.sqlite3>` for a verified SQLite migration, and `copy-archive-flush` from a background/archive schedule only. Never call cold archival flushing from a decision or reconciliation path.

Run `python main.py science run` for the single continuous D.6 worker or
`science run-once` for a bounded tick. These commands can produce only
simulation/shadow scientific decisions; no mode or UI control adds live
authority.
