from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
from json import dumps
from pathlib import Path
from typing import Any, Sequence

from .analytics import campaign_return_series
from .analysis import CandidateAnalysisPipeline, _config_fingerprint
from .backtest import CopyTradeBacktester
from .config import CopyTradeConfig
from .control_center import serve_control_center
from .dashboard import serve_dashboard
from .discovery import build_discovery_provider, parse_activity_age
from .hyperliquid import HyperliquidWatcher
from .market import HyperliquidMarketData
from .paper import TargetSizeClassifier
from .reporting import ObsidianExporter
from .scoring import FollowerMetrics, score_candidate, select_diverse_targets
from .service import CopyTradeService
from .science_storage import ColdArchiveSpool, StorageRoots, migrate_sqlite_to_hot
from .science_repository import ScientificRepository
from .scientific_scheduler import ScientificScheduler
from .scientific_worker import ScientificWorker, WorkerStage
from .data_ignition import DataIgnitionCommissioner, PublicObservationService


def add_copytrade_parsers(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    def command(name: str, help_text: str) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(name, help=help_text, description=help_text)
        parser.add_argument("--config", default="config/copytrade.yaml", help="Copy-trading YAML configuration path.")
        return parser

    importer = command("copy-import", "Import manually researched Hyperliquid wallets from arguments or a text/CSV file.")
    importer.add_argument("--wallet", action="append", default=[], help="0x wallet address; may be specified repeatedly.")
    importer.add_argument("--file", help="Text/CSV list whose first column contains wallet addresses.")
    importer.add_argument("--approve", action="store_true", help="Mark newly imported targets approved for research triage; this does not activate paper monitoring.")

    backfill = command("copy-backfill", "Backfill public Hyperliquid fills, account state, and portfolio history.")
    backfill.add_argument("--wallet", required=True, help="Previously imported wallet address.")
    backfill.add_argument("--start", help="Inclusive ISO-8601 start time; default is latest stored fill or 90 days ago.")
    backfill.add_argument("--end", help="Inclusive ISO-8601 end time; default is now.")

    discovery = command("copy-discover", "Discover active public HyperCore trader wallets from documented node-data files.")
    discovery.add_argument("--source", choices=("hypercore-file", "hypercore-s3"), default="hypercore-file",
                           help="Node-data transport: downloaded/local data or explicit requester-pays S3 objects.")
    discovery.add_argument("--input", action="append", default=[],
                           help="Repeatable node-trades/node-fills JSON/JSONL/LZ4 file path or exact s3://bucket/key object.")
    discovery.add_argument("--limit", type=int, default=1000, help="Maximum eligible wallets to register per run.")
    discovery.add_argument("--refresh", action="store_true", help="Request a fresh source read; recorded with the discovery run.")
    discovery.add_argument("--min-activity", type=int, default=1,
                           help="Minimum distinct observed node events for a wallet in this run (default: 1).")
    discovery.add_argument("--max-activity-age", default="30d",
                           help="Newest activity allowed for Phase A eligibility (e.g. 24h, 7d, 30d; use 'none' to disable).")
    discovery.add_argument("--output", help="Optional path for the JSON completion payload.")

    analysis = command("copy-analyze-candidates", "Run resumable Phase B public-data candidate analysis and ranking inputs.")
    analysis.add_argument("--limit", type=int, default=500, help="Maximum Phase A candidates considered in this run.")
    analysis.add_argument("--status", default="new", help="Target status to consume from the Phase A candidate universe; use 'all' for non-operator states.")
    analysis.add_argument("--resume", action="store_true", help="Resume the newest interrupted analysis run.")
    analysis.add_argument("--force", action="store_true", help="Recompute candidates already in a final analysis state.")
    analysis.add_argument("--workers", type=int, help="Bounded public-backfill workers; default comes from analysis configuration.")
    analysis.add_argument("--cheap-only", action="store_true", help="Run only local Phase A/data-quality prefiltering; defer public backfills.")
    analysis.add_argument("--output", help="Optional path for machine-readable analysis results.")

    analysis_status = command("copy-analysis-status", "Show persisted Phase B runs and GUI-ready candidate-analysis rows.")
    analysis_status.add_argument("--limit", type=int, default=1000, help="Maximum candidate rows to return.")

    score = command("copy-score", "Reconstruct, analyze, simulate, and score copy-trading candidates.")
    score.add_argument("--wallet", action="append", default=[], help="Wallet to score; default scores every imported target.")
    score.add_argument("--no-latency-grid", action="store_true", help="Use only the baseline follower simulation.")

    rank = command("copy-rank", "Rank eligible candidates and choose a lower-correlation target set.")
    rank.add_argument("--count", type=int, default=7, help="Maximum selected targets (default: 7).")
    rank.add_argument("--output", help="Optional path for canonical finalist JSON.")

    suitability = command("copy-suitability-report", "Show the stored deterministic suitability evidence for one wallet.")
    suitability.add_argument("--wallet", required=True, help="Discovered public wallet to inspect; this never changes status.")
    suitability.add_argument("--output", help="Optional path for the report JSON.")

    approve = command("copy-approve", "Approve a target for research triage; approval does not activate paper monitoring.")
    approve.add_argument("--wallet", required=True)
    reject = command("copy-reject", "Reject a target from research triage; it cannot open new paper sleeves.")
    reject.add_argument("--wallet", required=True)

    watch = command("copy-watch", "Watch Active entry wallets and exit-only wallets with open paper sleeves.")
    watch.add_argument("--duration", type=float, help="Optional bounded duration in seconds for smoke tests.")

    backtest = command("copy-backtest", "Replay stored public fills through deterministic paper-copy simulation.")
    backtest.add_argument("--wallet", action="append", default=[], help="Wallet to replay; default replays all stored fills.")
    backtest.add_argument("--walk-forward", action="store_true", help="Run rolling train/forward windows without lookahead.")
    backtest.add_argument("--export", action="store_true", help="Write an Obsidian backtest note.")
    backtest.add_argument("--market-price-proxy", action="store_true", help="Use public candle-close historical price proxies instead of only target fill prices.")

    report = command("copy-report", "Generate Obsidian target notes, dashboard, SVG charts, and a research report.")
    report.add_argument("--wallet", action="append", default=[], help="Wallet to export; default exports every target.")
    export = command("copy-export-obsidian", "Alias for copy-report, producing the Obsidian research tree.")
    export.add_argument("--wallet", action="append", default=[], help="Wallet to export; default exports every target.")

    dashboard = command("copy-dashboard", "Start the local paper-copy FastAPI dashboard.")
    dashboard.add_argument("--host", help="Override dashboard host.")
    dashboard.add_argument("--port", type=int, help="Override dashboard port.")

    control_center = command("copy-control-center", "Start the local paper-only copy-trading control center.")
    control_center.add_argument("--host", help="Override control-center host.")
    control_center.add_argument("--port", type=int, help="Override control-center port.")
    control_center.add_argument("--with-watcher", action="store_true", help="Run the paper watcher in the control-center lifecycle.")

    sizing = command("copy-size-demo", "Show the configured 5/10/20 percent sizing classification.")
    sizing.add_argument("--fractions", default="0.03,0.10,0.20", help="Comma-separated target entry fractions to classify against a 10% prior-median demo history.")
    sizing.add_argument("--equity", type=float, default=1000.0, help="Illustrative target equity.")

    storage_status = command("copy-storage-status", "Report scientific hot/cold storage health without changing execution state.")
    storage_migrate = command("copy-storage-migrate", "Safely snapshot and migrate a legacy SQLite database to the hot root.")
    storage_migrate.add_argument("--source", required=True, help="Legacy SQLite database path; it is never deleted.")
    storage_migrate.add_argument("--destination", help="Optional hot destination; defaults to configured active database.")
    archive_flush = command("copy-archive-flush", "Flush the local hot archival spool to cold storage; never use in a decision path.")

    science = command("science", "Operate the durable, simulation/shadow-only automated scientific worker.")
    science_subparsers = science.add_subparsers(dest="science_command", required=True)
    science_run = science_subparsers.add_parser("run", help="Run the worker continuously until interrupted.")
    science_run.add_argument("--max-items", type=int, help="Optional bounded items per scheduler tick.")
    science_once = science_subparsers.add_parser("run-once", help="Process currently available scientific work and exit.")
    science_once.add_argument("--max-items", type=int, help="Maximum queue items for this invocation.")
    science_subparsers.add_parser("status", help="Show truthful queue, cursor, storage, and scientific-object state.")
    science_pause = science_subparsers.add_parser("pause", help="Fail-safe pause; durable work remains queued.")
    science_pause.add_argument("--reason", default="operator requested scientific pause")
    science_subparsers.add_parser("resume", help="Resume durable scientific work.")
    science_backfill = science_subparsers.add_parser("backfill", help="Explicitly materialize available local observations and outcomes.")
    science_backfill.add_argument("--max-cycles", type=int, default=128)
    science_rebuild = science_subparsers.add_parser("rebuild", help="Explicitly rerun bounded research over immutable local evidence.")
    science_rebuild.add_argument("--explicit", action="store_true", help="Required acknowledgement; rebuild never deletes evidence.")
    science_rebuild.add_argument("--max-cycles", type=int, default=128)
    science_bootstrap = science_subparsers.add_parser("bootstrap", help="Run the bounded local-data bootstrap and report exact counts.")
    science_bootstrap.add_argument("--max-cycles", type=int, default=128)
    science_reproduce = science_subparsers.add_parser("reproduce", help="Print the immutable inputs and result for one historical experiment.")
    science_reproduce.add_argument("--experiment", required=True)
    science_source = science_subparsers.add_parser("source-status", help="Show official historical-source readiness and the D.7 evidence capability audit.")
    science_source.add_argument("--test-access", action="store_true", help="Perform one bounded requester-pays source probe when credentials are configured.")
    science_plan = science_subparsers.add_parser("plan-history", help="Persist deterministic, bounded UTC historical-source hour slots.")
    science_plan.add_argument("--start", help="Inclusive UTC ISO-8601 start; defaults to commissioning.historical_start.")
    science_plan.add_argument("--end", help="Exclusive UTC ISO-8601 end; defaults to commissioning.historical_end.")
    science_acquire = science_subparsers.add_parser("acquire-history", help="Resolve, forecast, verify, ingest, and archive a bounded historical range.")
    science_acquire.add_argument("--start", help="Inclusive UTC ISO-8601 start; defaults to commissioning.historical_start.")
    science_acquire.add_argument("--end", help="Exclusive UTC ISO-8601 end; defaults to commissioning.historical_end.")
    science_subparsers.add_parser("cancel-history", help="Request durable cancellation between historical source objects.")
    science_coverage = science_subparsers.add_parser("coverage", help="Calculate first-class D.7 historical coverage and quality evidence.")
    science_coverage.add_argument("--start", help="Inclusive UTC ISO-8601 start; defaults to commissioning.historical_start.")
    science_coverage.add_argument("--end", help="Exclusive UTC ISO-8601 end; defaults to commissioning.historical_end.")
    science_commission = science_subparsers.add_parser("commission", help="Run bounded D.7 acquisition and the existing D.6 science loop.")
    science_commission.add_argument("--start", help="Inclusive UTC ISO-8601 start; defaults to commissioning.historical_start.")
    science_commission.add_argument("--end", help="Exclusive UTC ISO-8601 end; defaults to commissioning.historical_end.")
    science_commission.add_argument("--max-cycles", type=int, default=128)
    science_observe = science_subparsers.add_parser("observe", help="Run the public allMids/userFills observer; it only persists scientific observations.")
    science_observe.add_argument("--duration", type=float, help="Optional bounded observer duration in seconds for smoke tests.")


def run_copytrade_command(args: argparse.Namespace) -> int:
    config = CopyTradeConfig.from_yaml(args.config)
    command = args.command
    roots = StorageRoots(home=Path.cwd(), hot_root=config.artifacts.database_path.parent, cold_root=config.storage.cold_root)
    spool = ColdArchiveSpool(roots, max_bytes=config.storage.archive_spool_max_bytes, max_age_seconds=config.storage.archive_spool_max_age_seconds)
    if command == "copy-storage-status":
        _print({**roots.cold_status(), "hot_database": str(config.artifacts.database_path), "spool": spool.backlog()})
        return 0
    if command == "copy-storage-migrate":
        _print(migrate_sqlite_to_hot(source=Path(args.source), destination=Path(args.destination) if args.destination else config.artifacts.database_path, roots=roots))
        return 0
    if command == "copy-archive-flush":
        _print(spool.flush_once())
        return 0
    if command == "science":
        repository = ScientificRepository(config.artifacts.database_path, archive_spool=spool)
        worker = ScientificWorker(repository, config)
        ignition = DataIgnitionCommissioner(repository, worker, config)
        science_command = args.science_command
        if science_command == "run":
            scheduler = ScientificScheduler(worker, poll_interval_seconds=config.scientific_worker.poll_interval_seconds)
            try:
                scheduler.run_forever(max_items=args.max_items)
            except KeyboardInterrupt:
                scheduler.stop()
                _print({"state": "STOPPED", "reason": "keyboard_interrupt", "queue": repository.work_queue_status(now="operator-interrupt")})
            return 0
        if science_command == "run-once":
            _print(worker.run_once(max_items=args.max_items))
            return 0
        if science_command == "status":
            _print({**_science_status(repository, roots, spool), "data_ignition": ignition.status()})
            return 0
        if science_command == "pause":
            worker.pause(args.reason)
            _print(_science_status(repository, roots, spool))
            return 0
        if science_command == "resume":
            worker.resume()
            _print(_science_status(repository, roots, spool))
            return 0
        if science_command == "backfill":
            _print({"mode": "BACKFILL", "summary": worker.run_until_idle(max_cycles=args.max_cycles), "status": _science_status(repository, roots, spool)})
            return 0
        if science_command == "rebuild":
            if not args.explicit:
                raise ValueError("science rebuild requires --explicit; it never deletes immutable evidence.")
            _queue_explicit_rebuild(worker)
            _print({"mode": "EXPLICIT_REBUILD", "summary": worker.run_until_idle(max_cycles=args.max_cycles), "status": _science_status(repository, roots, spool)})
            return 0
        if science_command == "bootstrap":
            _print({"mode": "BOUNDED_LOCAL_BOOTSTRAP", "summary": worker.run_until_idle(max_cycles=args.max_cycles), "counts": _science_counts(repository)})
            return 0
        if science_command == "reproduce":
            experiment = next((item for item in repository.list_experiments(kind="HISTORICAL") if item["experiment_id"] == args.experiment), None)
            if experiment is None:
                raise ValueError("Unknown historical experiment ID.")
            hypothesis = next((item for item in repository.list_hypotheses() if item["hypothesis_id"] == experiment["hypothesis_id"] and int(item["version"]) == int(experiment["hypothesis_version"])), None)
            _print({"experiment": experiment, "hypothesis": hypothesis, "reproducible": True, "execution_mode": "SIMULATION_SHADOW_ONLY"})
            return 0
        if science_command == "source-status":
            _print(ignition.source_status(test_access=args.test_access))
            return 0
        if science_command == "plan-history":
            _print(ignition.plan_history(args.start, args.end))
            return 0
        if science_command == "acquire-history":
            _print(ignition.acquire_history(args.start, args.end))
            return 0
        if science_command == "cancel-history":
            _print(ignition.cancel_history())
            return 0
        if science_command == "coverage":
            start, end = args.start or config.commissioning.historical_start, args.end or config.commissioning.historical_end
            _print(ignition.coverage.calculate(str(start).replace("+00:00", "Z"), str(end).replace("+00:00", "Z")))
            return 0
        if science_command == "commission":
            _print(ignition.commission(args.start, args.end, max_cycles=args.max_cycles))
            return 0
        if science_command == "observe":
            observer = PublicObservationService(ignition)
            try:
                _print(asyncio.run(observer.run(duration_seconds=args.duration)))
            except KeyboardInterrupt:
                observer.stop()
                _print({"state": "STOPPED", "reason": "keyboard_interrupt", "paper_only": True})
            return 0
    service = CopyTradeService(config)
    if command == "copy-import":
        targets = service.import_wallets(args.wallet)
        if args.file:
            targets.extend(service.import_wallet_file(args.file))
        if not targets:
            raise ValueError("Provide --wallet and/or --file.")
        if args.approve:
            for target in targets:
                service.set_status(target.wallet, "approved")
        _print({"imported": [target.wallet for target in targets], "approved": args.approve})
        return 0
    if command == "copy-backfill":
        _print(service.backfill(args.wallet, start=args.start, end=args.end))
        return 0
    if command == "copy-discover":
        provider = build_discovery_provider(args.source, args.input)
        max_activity_age = parse_activity_age(args.max_activity_age)
        summary = service.discover_candidates(
            provider, limit=args.limit, min_activity=args.min_activity, refresh=args.refresh,
            max_activity_age=max_activity_age,
            configuration={"source": args.source, "inputs": list(args.input), "max_activity_age": args.max_activity_age},
        )
        payload: dict[str, Any] = {
            "message": "Discovery complete", "run_id": summary.run_id, "status": summary.status,
            "sources": summary.sources, "wallets_observed": summary.wallets_seen,
            "eligible_wallets": summary.eligible_wallets, "limit_deferred_wallets": summary.limit_deferred_wallets,
            "new_candidates": summary.new_wallets, "existing_refreshed": summary.existing_wallets_refreshed,
            "filtered": summary.filtered_wallets, "queued_for_analysis": summary.queued_for_analysis,
            "valid_events": summary.valid_events, "normalized_observations": summary.normalized_observations,
            "duplicate_events": summary.duplicate_events, "invalid_wallets": summary.invalid_wallets,
            "malformed_events": summary.malformed_events, "unsupported_records": summary.unsupported_records,
            "fatal_source_errors": summary.fatal_source_errors,
            "errors": summary.errors,
        }
        if args.output:
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(dumps(payload, indent=2, default=str), encoding="utf-8")
            payload["output"] = str(output)
        _print(payload)
        return 0
    if command == "copy-analyze-candidates":
        pipeline = CandidateAnalysisPipeline(service)
        selected_status = None if args.status == "all" else args.status
        payload = pipeline.run(
            limit=args.limit, status=selected_status, resume=args.resume, force=args.force,
            workers=args.workers, cheap_only=args.cheap_only,
        )
        if args.output:
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(dumps(payload, indent=2, default=str), encoding="utf-8")
            payload["output"] = str(output)
        _print(payload)
        return 0
    if command == "copy-analysis-status":
        _print(CandidateAnalysisPipeline(service).status(limit=args.limit))
        return 0
    if command == "copy-score":
        wallets = args.wallet or [target.wallet for target in service.database.list_targets()]
        reports = []
        for wallet in wallets:
            reconstructed = service.reconstruct(wallet)
            metrics = reconstructed["metrics"]
            events = reconstructed["events"]
            coverage = service.database.latest_backfill_coverage(wallet) or {}
            baseline = CopyTradeBacktester(config).run(events=events, coverage_metadata=coverage)  # type: ignore[arg-type]
            latency = () if args.no_latency_grid else tuple(CopyTradeBacktester(config).latency_decay_curve(events=events))  # type: ignore[arg-type]
            follower_net = float(baseline.summary["net_pnl"])
            follower = FollowerMetrics(
                net_pnl=follower_net, expectancy=follower_net / max(int(baseline.summary["filled_attempts"]), 1),
                missed_trade_rate=float(baseline.summary["skipped_attempts"]) / max(int(baseline.summary["attempts"]), 1),
                latency_curve=latency,
                latency_status="available" if latency else "unavailable",
            )
            score = score_candidate(metrics, config.candidates, follower)  # type: ignore[arg-type]
            service.database.upsert_candidate_score(score)
            reports.append({"wallet": wallet.lower(), "score": score.total_score, "eligible": score.eligible, "reasons": score.reasons})
        _print({"scores": reports})
        return 0
    if command == "copy-rank":
        pipeline = CandidateAnalysisPipeline(service)
        fingerprint = _config_fingerprint(config.research_snapshot())
        phase_b_scores = service.database.phase_b_qualified_scores(config_fingerprint=fingerprint)
        # Ranking is an explicit operator command, unlike analysis status.
        finalists = pipeline.shadow_finalists(count=args.count, persist=True)
        payload = {
            "ranked_phase_b": [_score_payload(score) for score in phase_b_scores],
            "selected": finalists,
            "shadow_finalists": finalists,
            "current_config_fingerprint": fingerprint,
            "stale_qualified_candidates": service.database.count_stale_qualified_candidates(fingerprint),
            "legacy_scores": [_score_payload(score) for score in service.database.latest_legacy_scores()],
            "legacy_scores_label": "research_compatibility_only",
        }
        if getattr(args, "output", None):
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(dumps(payload, indent=2, default=str), encoding="utf-8")
            payload["output"] = str(output)
        _print(payload)
        return 0
    if command == "copy-suitability-report":
        payload = CandidateAnalysisPipeline(service).suitability_report(args.wallet)
        if args.output:
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(dumps(payload, indent=2, default=str), encoding="utf-8")
            payload["output"] = str(output)
        _print(payload)
        return 0
    if command in {"copy-approve", "copy-reject"}:
        service.set_status(args.wallet, "approved" if command == "copy-approve" else "rejected")
        _print({"wallet": args.wallet.lower(), "status": "approved" if command == "copy-approve" else "rejected"})
        return 0
    if command == "copy-watch":
        watcher = HyperliquidWatcher(service.adapter)
        async def watch() -> dict[str, int]:
            return await watcher.run(service.monitored_execution_wallets(), service.ingest_watched_fills, service.ingest_watched_state,
                                     service.ingest_market_update, service.reconcile_monitored_wallets,
                                     duration_seconds=args.duration)
        reconciled = asyncio.run(watch())
        _print({"health": watcher.health.as_dict(), "mode": config.mode, "reconciled_fills": reconciled})
        return 0
    if command == "copy-backtest":
        wallets = args.wallet or None
        fills = []
        events = []
        coverages = []
        if wallets:
            for wallet in wallets:
                fills.extend(service.database.list_raw_fills(wallet))
                events.extend(service.reconstruct(wallet)["events"])
                coverage = service.database.latest_backfill_coverage(wallet)
                if coverage:
                    coverages.append(coverage)
        else:
            fills = service.database.list_raw_fills()
            for wallet in sorted({fill.target_wallet for fill in fills}):
                events.extend(service.reconstruct(wallet)["events"])
                coverage = service.database.latest_backfill_coverage(wallet)
                if coverage:
                    coverages.append(coverage)
        market_data = HyperliquidMarketData(service.adapter) if args.market_price_proxy else None
        backtester = CopyTradeBacktester(config, service.database, market_data)
        if args.walk_forward:
            _print({"walk_forward": backtester.walk_forward(events=events)})
        else:
            run = backtester.run(events=events, coverage_metadata={"wallet_coverages": coverages})
            payload: dict[str, Any] = {"run_id": run.run_id, "summary": run.summary}
            if args.export:
                payload["obsidian_note"] = str(ObsidianExporter(config, service.database).export_backtest(run))
            _print(payload)
        return 0
    if command in {"copy-report", "copy-export-obsidian"}:
        exporter = ObsidianExporter(config, service.database, lambda wallet: service.reconstruct(wallet)["events"])
        paths = [str(exporter.export_target(wallet)) for wallet in (args.wallet or [target.wallet for target in service.database.list_targets()])]
        _print({"target_notes": paths, "dashboard": str(exporter.export_dashboard()), "report": str(exporter.export_report())})
        return 0
    if command == "copy-dashboard":
        if args.host or args.port:
            config = replace(config, artifacts=replace(config.artifacts, dashboard_host=args.host or config.artifacts.dashboard_host, dashboard_port=args.port or config.artifacts.dashboard_port))
        serve_dashboard(config, service.database)
        return 0
    if command == "copy-control-center":
        serve_control_center(config, service.database, host=args.host, port=args.port,
                             with_watcher=args.with_watcher, service=service)
        return 0
    if command == "copy-size-demo":
        fractions = [float(item.strip()) for item in args.fractions.split(",") if item.strip()]
        classifier = TargetSizeClassifier(config.sizing)
        classifier.seed("0x0000000000000000000000000000000000000000", [0.10] * config.sizing.min_history)
        output = []
        for fraction in fractions:
            decision = classifier.classify("0x0000000000000000000000000000000000000000", fraction * args.equity, args.equity)
            output.append({"target_size_fraction": fraction, "bucket": decision.bucket, "allocation_fraction": decision.allocation_fraction, "size_ratio": decision.size_ratio})
        _print({"allocations_use_available_capital": True, "prior_median_fraction": 0.10, "classification": output})
        return 0
    raise ValueError(f"Unsupported copy-trading command: {command}")


def _science_counts(repository: ScientificRepository) -> dict[str, int]:
    hypotheses = repository.list_hypotheses()
    forward = repository.list_forward_records()
    observations = repository.observation_counts()
    return {
        "observations": sum(observations.values()),
        "features": len(repository.list_feature_values()),
        "candidate_patterns": len(repository.list_discoveries()),
        "proposals": len(repository.list_discoveries()),
        "registered_hypotheses": sum(item["state"] == "REGISTERED" for item in hypotheses),
        "historical_rejects": len(repository.list_graveyard()),
        "historical_survivors": sum(item["state"] == "FORWARD_SHADOW" for item in hypotheses),
        "forward_predictions": len(forward),
        "resolved_forward_predictions": sum(item["outcome"] is not None for item in forward),
        "promoted_indicators": len(repository.list_indicators()),
        "candidate_models": len(repository.list_models()),
    }


def _science_status(repository: ScientificRepository, roots: StorageRoots, spool: ColdArchiveSpool) -> dict[str, Any]:
    return {
        "execution_mode": "SIMULATION_SHADOW_ONLY",
        "worker_control": repository.worker_control(),
        "queue": repository.work_queue_status(now="status"),
        "watermarks": repository.list_watermarks(),
        "stage_health": repository.stage_health(),
        "counts": _science_counts(repository),
        "storage": {**roots.cold_status(), "spool": spool.backlog(), "hot_database": str(repository.path)},
    }


def _queue_explicit_rebuild(worker: ScientificWorker) -> None:
    """Create a new, auditable queue generation without deleting evidence."""
    fingerprint = worker._workflow_fingerprint()
    for stage, subject_type, subject_id in (
        (WorkerStage.PATTERN_DISCOVERY, "family", "initial-interpretable"),
        (WorkerStage.HISTORICAL_EXPERIMENT, "family", "initial-interpretable"),
        (WorkerStage.FORWARD_RESOLUTION, "prediction-scan", "all"),
        (WorkerStage.INDICATOR_PROMOTION, "promotion-scan", "all"),
        (WorkerStage.MODEL_BUILD, "model-scan", "all"),
        (WorkerStage.MODEL_CALIBRATION, "model-calibration", "all"),
        (WorkerStage.DRIFT_EVALUATION, "drift-scan", "all"),
    ):
        worker._enqueue(stage, subject_type, subject_id, 2, fingerprint)


def _score_payload(score: Any) -> dict[str, Any]:
    return {
        "wallet": score.target_wallet, "score": score.total_score, "eligible": score.eligible,
        "reasons": score.reasons, "provenance": getattr(score, "provenance", "legacy"),
        "analysis_run_id": getattr(score, "analysis_run_id", None),
        "config_fingerprint": getattr(score, "config_fingerprint", None),
        "confidence_score": getattr(score, "confidence_score", 0.0),
        "hard_gates": getattr(score, "hard_gates", ()),
        "score_version": getattr(score, "score_version", None),
    }


def _print(payload: object) -> None:
    print(dumps(payload, indent=2, default=str))


def build_parser() -> argparse.ArgumentParser:
    """Build the sole supported Beelzebub command surface.

    The legacy ETH/Coinbase CLI was intentionally removed in Phase D.6.  This
    entry point owns only public-data copy-trade research and its paper/shadow
    scientific controls.
    """
    parser = argparse.ArgumentParser(prog="beelzebub", description="Beelzebub paper-only scientific research controls.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    add_copytrade_parsers(subparsers)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return run_copytrade_command(args)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    return 2  # pragma: no cover - argparse.error exits
