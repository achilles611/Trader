from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
from json import dumps
from pathlib import Path
from typing import Any

from .analytics import campaign_return_series
from .analysis import CandidateAnalysisPipeline, _config_fingerprint
from .backtest import CopyTradeBacktester
from .config import CopyTradeConfig
from .dashboard import serve_dashboard
from .discovery import build_discovery_provider, parse_activity_age
from .hyperliquid import HyperliquidWatcher
from .market import HyperliquidMarketData
from .paper import TargetSizeClassifier
from .reporting import ObsidianExporter
from .scoring import FollowerMetrics, score_candidate, select_diverse_targets
from .service import CopyTradeService


def add_copytrade_parsers(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    def command(name: str, help_text: str) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(name, help=help_text, description=help_text)
        parser.add_argument("--config", default="config/copytrade.yaml", help="Copy-trading YAML configuration path.")
        return parser

    importer = command("copy-import", "Import manually researched Hyperliquid wallets from arguments or a text/CSV file.")
    importer.add_argument("--wallet", action="append", default=[], help="0x wallet address; may be specified repeatedly.")
    importer.add_argument("--file", help="Text/CSV list whose first column contains wallet addresses.")
    importer.add_argument("--approve", action="store_true", help="Mark newly imported targets approved after import.")

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

    approve = command("copy-approve", "Approve a target for paper-mode websocket monitoring.")
    approve.add_argument("--wallet", required=True)
    reject = command("copy-reject", "Reject a target from paper-mode websocket monitoring.")
    reject.add_argument("--wallet", required=True)

    watch = command("copy-watch", "Watch approved Hyperliquid wallets and paper-copy new public fills.")
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

    sizing = command("copy-size-demo", "Show the configured 5/10/20 percent sizing classification.")
    sizing.add_argument("--fractions", default="0.03,0.10,0.20", help="Comma-separated target entry fractions to classify against a 10% prior-median demo history.")
    sizing.add_argument("--equity", type=float, default=1000.0, help="Illustrative target equity.")


def run_copytrade_command(args: argparse.Namespace) -> int:
    config = CopyTradeConfig.from_yaml(args.config)
    service = CopyTradeService(config)
    command = args.command
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
        fingerprint = _config_fingerprint(config.snapshot())
        phase_b_scores = service.database.phase_b_qualified_scores(config_fingerprint=fingerprint)
        finalists = pipeline.shadow_finalists(count=args.count)
        _print({
            "ranked_phase_b": [_score_payload(score) for score in phase_b_scores],
            "selected": finalists,
            "shadow_finalists": finalists,
            "current_config_fingerprint": fingerprint,
            "stale_qualified_candidates": service.database.count_stale_qualified_candidates(fingerprint),
            "legacy_scores": [_score_payload(score) for score in service.database.latest_scores()],
            "legacy_scores_label": "research_compatibility_only",
        })
        return 0
    if command in {"copy-approve", "copy-reject"}:
        service.set_status(args.wallet, "approved" if command == "copy-approve" else "rejected")
        _print({"wallet": args.wallet.lower(), "status": "approved" if command == "copy-approve" else "rejected"})
        return 0
    if command == "copy-watch":
        watcher = HyperliquidWatcher(service.adapter)
        async def watch() -> dict[str, int]:
            return await watcher.run(service.approved_wallets(), service.ingest_watched_fills, service.ingest_watched_state,
                                     service.ingest_market_update, service.reconcile_approved_wallets,
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


def _score_payload(score: Any) -> dict[str, Any]:
    return {
        "wallet": score.target_wallet, "score": score.total_score, "eligible": score.eligible,
        "reasons": score.reasons, "provenance": getattr(score, "provenance", "legacy"),
        "analysis_run_id": getattr(score, "analysis_run_id", None),
        "config_fingerprint": getattr(score, "config_fingerprint", None),
    }


def _print(payload: object) -> None:
    print(dumps(payload, indent=2, default=str))
