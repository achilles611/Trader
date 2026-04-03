from __future__ import annotations

import argparse
import json
import logging
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from .analysis.openai_client import OpenAIAnalysisClient, OpenAIAnalysisError
from .analysis.patch_request_builder import build_patch_request, build_patch_request_markdown
from .analysis.prompt_builder import build_analysis_input, load_system_prompt
from .analysis.schema import load_analysis_schema
from .git.branch_ops import capture_diff, create_experiment_branch
from .git.repo_sync import DirtyWorktreeError, RepoSyncError, get_repo_state, sync_to_production
from .profile_loader import load_bot_definitions, load_runtime_settings
from .result_normalizer import normalize_cycle
from .safety.kill_switch import global_kill_switch_reason
from .safety.run_lock import RunLock
from .storage.artifacts import (
    build_analysis_markdown,
    build_cycle_markdown,
    prepare_artifact_layout,
    write_json,
    write_markdown,
)
from .storage.db import SwarmDatabase
from .storage.models import AIAnalysisRecord, PatchAttemptRecord, RunCycleRecord
from .swarm_runner import SwarmRunner
from .validation.validate_patch import validate_patch
from .validation.validate_run import RunValidationReport, validate_cycle_preconditions


LOGGER = logging.getLogger("trader_swarm")


def _expected_boundary(now: datetime, interval_minutes: int) -> datetime:
    minute = (now.minute // interval_minutes) * interval_minutes
    boundary = now.replace(minute=minute, second=0, microsecond=0)
    if boundary > now:
        boundary -= timedelta(minutes=interval_minutes)
    return boundary


def _setup_logging(layout, bot_ids: list[str]) -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    if not root_logger.handlers:
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)

    def attach_file_logger(logger_name: str, path: Path) -> None:
        logger = logging.getLogger(logger_name)
        if any(getattr(handler, "baseFilename", None) == str(path) for handler in logger.handlers):
            return
        handler = logging.FileHandler(path, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    attach_file_logger("trader_swarm", layout.shared_log_dir / "orchestrator.log")
    attach_file_logger("trader_swarm.analysis", layout.shared_log_dir / "analysis.log")
    attach_file_logger("trader_swarm.patching", layout.shared_log_dir / "patching.log")
    attach_file_logger("trader_swarm.validation", layout.shared_log_dir / "validation.log")
    for bot_id in bot_ids:
        attach_file_logger(f"eth_bot.{bot_id}", layout.bot_log_path(bot_id))


def _tail(path: Path, lines: int) -> list[str]:
    if not path.exists():
        return []
    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return content[-lines:]


def _collect_log_excerpts(layout, lines: int) -> dict[str, list[str]]:
    return {
        "orchestrator": _tail(layout.shared_log_dir / "orchestrator.log", lines),
        "analysis": _tail(layout.shared_log_dir / "analysis.log", lines),
        "validation": _tail(layout.shared_log_dir / "validation.log", lines),
        "patching": _tail(layout.shared_log_dir / "patching.log", lines),
    }


def _notify(settings, title: str, message: str) -> None:
    if not settings.notifications.discord_webhook_url:
        return
    try:
        requests.post(
            settings.notifications.discord_webhook_url,
            json={"content": f"**{title}**\n{message}"},
            timeout=10,
        ).raise_for_status()
    except requests.RequestException as exc:
        logging.getLogger("trader_swarm").warning("Notification failed: %s", exc)


def _write_analysis_skip(layout, reason: str) -> None:
    payload = {"status": "skipped", "reason": reason}
    write_json(layout.analysis_json_path, payload)
    write_markdown(layout.analysis_summary_path, f"# AI Analysis\n\n- Status: `skipped`\n- Reason: {reason}")


def _run_patch_pipeline(settings, layout, root_dir: Path, bundle, analysis_result: dict[str, Any], bot_definitions, cycle_db_id: int | None, database: SwarmDatabase) -> str:
    patch_logger = logging.getLogger("trader_swarm.patching")
    branch_name = f"{settings.patching.experiment_branch_prefix}/{bundle.cycle_id.lower()}"
    patch_payload = build_patch_request(
        bundle,
        analysis_result,
        branch_name=branch_name,
        diff_line_limit=settings.patching.diff_line_limit,
    )
    write_markdown(layout.patch_request_path, build_patch_request_markdown(patch_payload))

    if not settings.patching.enabled:
        return str(layout.patch_request_path)

    validation_status = "not_applied"
    notes = "patching enabled but no patch command configured"
    if settings.patching.codex_patch_command:
        create_experiment_branch(root_dir, branch_name, base_ref=settings.patching.base_branch)
        patch_logger.info("patch_branch_created branch=%s", branch_name)
        result = subprocess.run(
            settings.patching.codex_patch_command,
            cwd=root_dir,
            shell=True,
            check=False,
            capture_output=True,
            text=True,
        )
        notes = (result.stdout + "\n" + result.stderr).strip()[-4000:]
        if result.returncode == 0:
            validation_report = validate_patch(root_dir, settings, bot_definitions, base_ref=settings.patching.base_branch)
            write_json(layout.validation_report_path, validation_report.to_dict())
            validation_status = validation_report.overall_status
            capture_diff(root_dir, layout.patch_diff_path, base_ref=settings.patching.base_branch)
            patch_logger.info("patch_validation_%s branch=%s", validation_status, branch_name)
        else:
            validation_status = "patch_command_failed"
            patch_logger.error("patch command failed for branch=%s", branch_name)

    if cycle_db_id is not None:
        database.insert_patch_attempt(
            cycle_db_id,
            PatchAttemptRecord(
                branch_name=branch_name,
                diff_artifact_path=str(layout.patch_diff_path),
                validation_status=validation_status,
                merged_to_develop=False,
                promoted_to_main=False,
                notes=notes,
            ),
        )
    return str(layout.patch_request_path)


def run_cycle(root_dir: Path, config_path: Path | None = None) -> int:
    settings = load_runtime_settings(root_dir, config_path)
    database = SwarmDatabase(settings.db_path)
    database.initialize()

    triggered_at = datetime.now(timezone.utc)
    boundary = _expected_boundary(triggered_at, settings.scheduler.interval_minutes)
    cycle_id = f"cycle_{boundary.strftime('%Y%m%dT%H%M%SZ')}_{triggered_at.strftime('%H%M%S')}"
    layout = prepare_artifact_layout(settings.artifact_root, triggered_at, cycle_id)

    try:
        bot_definitions = load_bot_definitions(settings)
    except Exception:
        bot_definitions = []
    _setup_logging(layout, [definition.bot_id for definition in bot_definitions])

    lock = RunLock(
        settings.lock_path,
        metadata={"cycle_id": cycle_id, "triggered_at": triggered_at.isoformat()},
    )
    if not lock.acquire():
        overlap_metadata = lock.read_metadata()
        message = f"overlap detected; active lock={overlap_metadata}"
        LOGGER.warning(message)
        _notify(settings, "Trader swarm overlap detected", message)
        record = RunCycleRecord(
            cycle_id=cycle_id,
            started_at=triggered_at.isoformat(),
            expected_trigger_at=boundary.isoformat(),
            actual_trigger_at=triggered_at.isoformat(),
            git_sha="",
            status="skipped_due_to_overlap",
            summary_artifact_path=str(layout.cycle_summary_path),
            error_message=message,
        )
        database.insert_cycle(record)
        write_markdown(layout.cycle_summary_path, f"# Cycle {cycle_id}\n\n- Status: `skipped_due_to_overlap`\n- Reason: {message}")
        return 0

    cycle_db_id: int | None = None
    try:
        repo_state = sync_to_production(
            settings.root_dir,
            remote=settings.git.remote,
            branch=settings.git.production_branch,
        )
        settings = load_runtime_settings(root_dir, config_path)
        bot_definitions = load_bot_definitions(settings)
        _setup_logging(layout, [definition.bot_id for definition in bot_definitions])

        kill_reason = global_kill_switch_reason(
            settings.safety.global_kill_switch,
            settings.safety.global_kill_switch_path,
        )
        if kill_reason:
            record = RunCycleRecord(
                cycle_id=cycle_id,
                started_at=triggered_at.isoformat(),
                expected_trigger_at=boundary.isoformat(),
                actual_trigger_at=triggered_at.isoformat(),
                git_sha=repo_state.git_sha,
                status="global_kill_switch",
                summary_artifact_path=str(layout.cycle_summary_path),
                error_message=kill_reason,
            )
            database.insert_cycle(record)
            write_markdown(layout.cycle_summary_path, f"# Cycle {cycle_id}\n\n- Status: `global_kill_switch`\n- Reason: {kill_reason}")
            _notify(settings, "Trader swarm kill switch engaged", kill_reason)
            return 0

        validation_report = validate_cycle_preconditions(settings, bot_definitions, database, repo_state)
        write_json(layout.cycle_root / "preflight_validation.json", validation_report.to_dict())
        if validation_report.should_skip:
            reason = "; ".join(check.message for check in validation_report.checks if check.status == "skip")
            record = RunCycleRecord(
                cycle_id=cycle_id,
                started_at=triggered_at.isoformat(),
                expected_trigger_at=boundary.isoformat(),
                actual_trigger_at=triggered_at.isoformat(),
                git_sha=repo_state.git_sha,
                status="skipped",
                summary_artifact_path=str(layout.cycle_summary_path),
                error_message=reason,
            )
            database.insert_cycle(record)
            write_markdown(layout.cycle_summary_path, f"# Cycle {cycle_id}\n\n- Status: `skipped`\n- Reason: {reason}")
            _notify(settings, "Trader swarm cycle skipped", reason)
            return 0
        if not validation_report.can_run:
            reason = "; ".join(check.message for check in validation_report.checks if check.status == "fail")
            record = RunCycleRecord(
                cycle_id=cycle_id,
                started_at=triggered_at.isoformat(),
                expected_trigger_at=boundary.isoformat(),
                actual_trigger_at=triggered_at.isoformat(),
                git_sha=repo_state.git_sha,
                status="failed_preflight",
                summary_artifact_path=str(layout.cycle_summary_path),
                error_message=reason,
            )
            database.insert_cycle(record)
            write_markdown(layout.cycle_summary_path, f"# Cycle {cycle_id}\n\n- Status: `failed_preflight`\n- Reason: {reason}")
            _notify(settings, "Trader swarm preflight failed", reason)
            return 1

        started_at = datetime.now(timezone.utc)
        cycle_db_id = database.insert_cycle(
            RunCycleRecord(
                cycle_id=cycle_id,
                started_at=started_at.isoformat(),
                expected_trigger_at=boundary.isoformat(),
                actual_trigger_at=triggered_at.isoformat(),
                git_sha=repo_state.git_sha,
                status="running",
                summary_artifact_path=str(layout.cycle_summary_path),
                drift_seconds=(triggered_at - boundary).total_seconds(),
            )
        )

        swarm_result = SwarmRunner(
            settings=settings,
            bot_definitions=bot_definitions,
            artifact_layout=layout,
        ).run_session(settings.scheduler.session_minutes)

        finished_at = datetime.now(timezone.utc)
        timing_context = {
            "cycle_id": cycle_id,
            "expected_trigger_at": boundary.isoformat(),
            "actual_trigger_at": triggered_at.isoformat(),
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "drift_seconds": (triggered_at - boundary).total_seconds(),
            "duration_seconds": (finished_at - started_at).total_seconds(),
        }
        recent_cycles = database.get_recent_cycles(settings.analysis.rolling_cycles)
        previous_hashes = database.get_latest_bot_hashes()
        log_excerpts = _collect_log_excerpts(layout, settings.analysis.log_excerpt_lines)
        bundle, bot_records = normalize_cycle(
            settings=settings,
            repo_state=repo_state,
            artifact_layout=layout,
            timing_context=timing_context,
            swarm_result=swarm_result,
            bot_definitions=bot_definitions,
            recent_cycles=recent_cycles,
            previous_hashes=previous_hashes,
            validation_report=validation_report,
            log_excerpts=log_excerpts,
        )
        write_json(layout.cycle_bundle_path, bundle.to_dict())
        cycle_markdown = build_cycle_markdown(bundle)
        write_markdown(layout.cycle_summary_path, cycle_markdown)
        write_markdown(layout.cycle_report_path, cycle_markdown)
        database.insert_bot_runs(cycle_db_id, bot_records)

        analysis_artifact_path = str(layout.analysis_json_path)
        patch_request_artifact_path = ""
        if settings.analysis.enabled and settings.analysis.api_key:
            try:
                schema_document = load_analysis_schema(settings.analysis.schema_path)
                instructions = load_system_prompt(settings.analysis.prompt_path)
                client = OpenAIAnalysisClient(settings.analysis)
                analysis_response = client.analyze_cycle(
                    instructions=instructions,
                    user_input=build_analysis_input(
                        bundle,
                        max_signal_events=settings.analysis.max_signal_events,
                        log_excerpts=log_excerpts,
                    ),
                    schema_document=schema_document,
                    metadata={"cycle_id": cycle_id, "git_sha": repo_state.git_sha},
                )
                write_json(layout.analysis_json_path, analysis_response.parsed_json)
                write_markdown(layout.analysis_summary_path, build_analysis_markdown(analysis_response.parsed_json))
                patch_request_artifact_path = _run_patch_pipeline(
                    settings,
                    layout,
                    settings.root_dir,
                    bundle,
                    analysis_response.parsed_json,
                    bot_definitions,
                    cycle_db_id,
                    database,
                )
                database.insert_ai_analysis(
                    cycle_db_id,
                    AIAnalysisRecord(
                        model=analysis_response.model,
                        prompt_cache_key=analysis_response.prompt_cache_key,
                        request_tokens_est=analysis_response.request_tokens_est,
                        response_tokens_est=analysis_response.response_tokens_est,
                        json_result=analysis_response.parsed_json,
                        recommendation_grade=str(analysis_response.parsed_json.get("cycle_verdict", "hold")),
                        patch_request_artifact_path=patch_request_artifact_path,
                        response_id=analysis_response.response_id,
                        request_id=analysis_response.request_id,
                    ),
                )
            except OpenAIAnalysisError as exc:
                logging.getLogger("trader_swarm.analysis").warning("analysis skipped after error: %s", exc)
                _write_analysis_skip(layout, f"analysis error: {exc}")
                _notify(settings, "Trader swarm analysis degraded", str(exc))
        else:
            reason = "analysis disabled" if not settings.analysis.enabled else "OPENAI_API_KEY missing"
            _write_analysis_skip(layout, reason)

        database.update_cycle(
            cycle_db_id,
            RunCycleRecord(
                cycle_id=cycle_id,
                started_at=started_at.isoformat(),
                expected_trigger_at=boundary.isoformat(),
                actual_trigger_at=triggered_at.isoformat(),
                git_sha=repo_state.git_sha,
                status="completed",
                summary_artifact_path=str(layout.cycle_summary_path),
                analysis_artifact_path=analysis_artifact_path,
                finished_at=finished_at.isoformat(),
                total_pnl=bundle.total_pnl,
                total_drawdown=bundle.total_drawdown,
                total_trades=bundle.total_trades,
                drift_seconds=(triggered_at - boundary).total_seconds(),
                duration_seconds=(finished_at - started_at).total_seconds(),
            ),
        )
        return 0
    except (DirtyWorktreeError, RepoSyncError) as exc:
        LOGGER.exception("cycle failed: %s", exc)
        write_markdown(layout.cycle_summary_path, f"# Cycle {cycle_id}\n\n- Status: `failed`\n- Reason: {exc}")
        if cycle_db_id is not None:
            database.update_cycle(
                cycle_db_id,
                RunCycleRecord(
                    cycle_id=cycle_id,
                    started_at=triggered_at.isoformat(),
                    expected_trigger_at=boundary.isoformat(),
                    actual_trigger_at=triggered_at.isoformat(),
                    git_sha="",
                    status="failed",
                    summary_artifact_path=str(layout.cycle_summary_path),
                    finished_at=datetime.now(timezone.utc).isoformat(),
                    error_message=str(exc),
                ),
            )
        else:
            database.insert_cycle(
                RunCycleRecord(
                    cycle_id=cycle_id,
                    started_at=triggered_at.isoformat(),
                    expected_trigger_at=boundary.isoformat(),
                    actual_trigger_at=triggered_at.isoformat(),
                    git_sha="",
                    status="failed",
                    summary_artifact_path=str(layout.cycle_summary_path),
                    finished_at=datetime.now(timezone.utc).isoformat(),
                    error_message=str(exc),
                )
            )
        _notify(settings, "Trader swarm cycle failed", str(exc))
        return 1
    finally:
        lock.release()


def health_check(root_dir: Path, config_path: Path | None = None) -> dict[str, Any]:
    settings = load_runtime_settings(root_dir, config_path)
    database = SwarmDatabase(settings.db_path)
    database.initialize()
    repo_state = get_repo_state(settings.root_dir)
    latest_cycle = database.latest_cycle()
    lock = RunLock(settings.lock_path)
    return {
        "root_dir": str(settings.root_dir),
        "db_path": str(settings.db_path),
        "artifact_root": str(settings.artifact_root),
        "repo": {
            "branch": repo_state.branch,
            "git_sha": repo_state.git_sha,
            "is_dirty": repo_state.is_dirty,
            "dirty_files": repo_state.dirty_files,
        },
        "latest_cycle": latest_cycle,
        "lock": lock.read_metadata(),
    }


def replay_analysis(root_dir: Path, bundle_path: Path, config_path: Path | None = None) -> int:
    settings = load_runtime_settings(root_dir, config_path)
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    schema_document = load_analysis_schema(settings.analysis.schema_path)
    instructions = load_system_prompt(settings.analysis.prompt_path)
    client = OpenAIAnalysisClient(settings.analysis)
    response = client.analyze_cycle(
        instructions=instructions,
        user_input=json.dumps(bundle, indent=2, sort_keys=True),
        schema_document=schema_document,
        metadata={"replay": "true", "cycle_id": bundle.get("cycle_id", "unknown")},
    )
    print(json.dumps(response.parsed_json, indent=2))
    return 0


def init_db(root_dir: Path, config_path: Path | None = None) -> int:
    settings = load_runtime_settings(root_dir, config_path)
    database = SwarmDatabase(settings.db_path)
    database.initialize()
    print(json.dumps({"db_path": str(settings.db_path), "status": "ok"}, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Trader swarm orchestration entrypoint.")
    parser.add_argument("--config", type=str, default="", help="Optional path to global YAML config.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("run-cycle", help="Run one full orchestration cycle.")
    subparsers.add_parser("health-check", help="Print current orchestration health.")
    subparsers.add_parser("init-db", help="Initialize the SQLite database.")
    replay = subparsers.add_parser("replay-analysis", help="Replay AI analysis for a stored cycle bundle.")
    replay.add_argument("--bundle", type=str, required=True, help="Path to a saved cycle_bundle.json file.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    root_dir = Path(__file__).resolve().parents[1]
    config_path = Path(args.config).resolve() if args.config else None

    if args.command == "run-cycle":
        return run_cycle(root_dir, config_path)
    if args.command == "health-check":
        print(json.dumps(health_check(root_dir, config_path), indent=2))
        return 0
    if args.command == "init-db":
        return init_db(root_dir, config_path)
    if args.command == "replay-analysis":
        return replay_analysis(root_dir, Path(args.bundle).resolve(), config_path)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
