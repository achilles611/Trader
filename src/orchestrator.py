from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from .analysis.openai_client import OpenAIAnalysisClient, OpenAIAnalysisError
from .analysis.patch_request_builder import build_patch_request, build_patch_request_markdown
from .analysis.prompt_builder import build_analysis_input, load_system_prompt
from .analysis.schema import AnalysisSchemaError, load_analysis_schema
from .config.env_validator import validate_environment, validate_writable_targets
from .failure import FailureCode, OrchestratorFailure
from .git.branch_ops import capture_diff, create_experiment_branch
from .git.repo_sync import DirtyWorktreeError, RepoSyncError, get_repo_state, resolve_ref_sha, resolve_remote_head_sha, sync_to_production
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


def _run_mode(settings) -> str:
    if settings.dry_run:
        return "dry_run"
    if settings.live_trading_enabled:
        return "live"
    return "paper"


def _failure_markdown(cycle_id: str, failure: OrchestratorFailure) -> str:
    lines = [
        f"# Cycle {cycle_id}",
        "",
        "- Status: `failed`",
        f"- Failure code: `{failure.code}`",
        f"- Reason: {failure.message}",
    ]
    if failure.details:
        lines.extend(["", "## Details", ""])
        for key, value in failure.details.items():
            lines.append(f"- {key}: `{value}`")
    return "\n".join(lines)


def _lock_status(lock_path: Path) -> dict[str, Any]:
    probe = RunLock(lock_path, metadata={"health_check": True})
    if probe.acquire():
        probe.release()
        return {"status": "free", "metadata": {}}
    return {"status": "held", "metadata": probe.read_metadata()}


def _build_health_report(root_dir: Path, config_path: Path | None = None) -> tuple[dict[str, Any], int]:
    repo_state = get_repo_state(root_dir)
    settings = None
    config_valid = False
    config_error = ""
    bot_definitions = []
    try:
        settings = load_runtime_settings(root_dir, config_path)
        bot_definitions = load_bot_definitions(settings)
        config_valid = True
    except Exception as exc:
        config_error = str(exc)

    production_branch = settings.git.production_branch if settings is not None else "main"
    production_ref = settings.git.remote if settings is not None else "origin"
    origin_sha = resolve_remote_head_sha(root_dir, production_ref, production_branch) or resolve_ref_sha(root_dir, f"{production_ref}/{production_branch}")
    lock_info = _lock_status((settings.lock_path if settings is not None else (root_dir / "artifacts" / "run.lock")))

    db_status = {"ok": False, "message": "settings unavailable", "path": ""}
    artifact_status = {"ok": False, "message": "settings unavailable", "path": ""}
    env_report = None
    latest_cycle = None
    if settings is not None:
        database = SwarmDatabase(settings.db_path)
        try:
            database.initialize()
            database.ping()
            latest_cycle = database.latest_cycle()
            db_status = {"ok": True, "message": "writable", "path": str(settings.db_path)}
        except Exception as exc:
            db_status = {"ok": False, "message": str(exc), "path": str(settings.db_path)}
        writable = validate_writable_targets(settings)
        artifact_status = writable["artifact_root"]
        env_report = validate_environment(settings, root_dir=root_dir)

    ready_for_dry_run = all(
        [
            config_valid,
            settings is not None,
            not repo_state.is_dirty,
            db_status["ok"],
            artifact_status["ok"],
            lock_info["status"] == "free",
            env_report.valid if env_report is not None else False,
        ]
    )
    report = {
        "root_dir": str(root_dir),
        "ready_for_dry_run": ready_for_dry_run,
        "repo_clean": not repo_state.is_dirty,
        "repo": {
            "branch": repo_state.branch,
            "git_sha": repo_state.git_sha,
            "origin_production_sha": origin_sha,
            "production_branch": production_branch,
            "dirty_files": repo_state.dirty_files,
        },
        "environment": env_report.to_dict() if env_report is not None else {"valid": False, "missing_keys": [], "error": "settings unavailable"},
        "db_writable": db_status,
        "artifact_root_writable": artifact_status,
        "openai_api_key_present": bool(settings and settings.analysis.api_key),
        "analysis_model_configured": bool(settings and settings.analysis.model),
        "live_trading_enabled": bool(settings and settings.live_trading_enabled),
        "patching_enabled": bool(settings and settings.patching.enabled),
        "scheduler_timezone": settings.scheduler.timezone if settings is not None else "",
        "lock_status": lock_info,
        "bot_profile_count_loaded": len(bot_definitions),
        "config_validation": {
            "valid": config_valid,
            "error": config_error,
        },
        "latest_cycle": latest_cycle,
    }
    return report, 0 if ready_for_dry_run else 1


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


def _persist_cycle_failure(
    *,
    database: SwarmDatabase,
    cycle_db_id: int | None,
    cycle_id: str,
    layout,
    triggered_at: datetime,
    boundary: datetime,
    git_sha: str,
    run_mode: str,
    dry_run: bool,
    failure: OrchestratorFailure,
) -> None:
    write_markdown(layout.cycle_summary_path, _failure_markdown(cycle_id, failure))
    record = RunCycleRecord(
        cycle_id=cycle_id,
        started_at=triggered_at.isoformat(),
        expected_trigger_at=boundary.isoformat(),
        actual_trigger_at=triggered_at.isoformat(),
        git_sha=git_sha,
        status="failed",
        dry_run=dry_run,
        run_mode=run_mode,
        summary_artifact_path=str(layout.cycle_summary_path),
        finished_at=datetime.now(timezone.utc).isoformat(),
        failure_code=failure.code,
        failure_details=failure.details,
        error_message=failure.message,
    )
    if cycle_db_id is not None:
        database.update_cycle(cycle_db_id, record)
    else:
        database.insert_cycle(record)


def run_cycle(root_dir: Path, config_path: Path | None = None, *, dry_run_override: bool | None = None) -> int:
    if dry_run_override is not None:
        os.environ["DRY_RUN"] = "true" if dry_run_override else "false"

    try:
        settings = load_runtime_settings(root_dir, config_path)
        bot_definitions = load_bot_definitions(settings)
    except Exception as exc:
        raise_failure = OrchestratorFailure(
            FailureCode.CONFIG_INVALID,
            "Unable to load runtime settings or bot profiles.",
            details={"error": str(exc)},
        )
        print(json.dumps(raise_failure.to_dict(), indent=2))
        return raise_failure.exit_code

    database = SwarmDatabase(settings.db_path)
    database.initialize()

    triggered_at = datetime.now(timezone.utc)
    boundary = _expected_boundary(triggered_at, settings.scheduler.interval_minutes)
    cycle_id = f"cycle_{boundary.strftime('%Y%m%dT%H%M%SZ')}_{triggered_at.strftime('%H%M%S')}"
    layout = prepare_artifact_layout(settings.artifact_root, triggered_at, cycle_id)
    _setup_logging(layout, [definition.bot_id for definition in bot_definitions])
    run_mode = _run_mode(settings)

    lock = RunLock(
        settings.lock_path,
        metadata={"cycle_id": cycle_id, "triggered_at": triggered_at.isoformat(), "run_mode": run_mode},
    )
    if not lock.acquire():
        overlap_metadata = lock.read_metadata()
        failure = OrchestratorFailure(
            FailureCode.LOCK_HELD,
            "overlap detected; active run lock is already held",
            details={"active_lock": overlap_metadata},
            exit_code=0,
        )
        LOGGER.warning("%s", failure)
        _notify(settings, "Trader swarm overlap detected", failure.message)
        database.insert_cycle(
            RunCycleRecord(
                cycle_id=cycle_id,
                started_at=triggered_at.isoformat(),
                expected_trigger_at=boundary.isoformat(),
                actual_trigger_at=triggered_at.isoformat(),
                git_sha="",
                status="skipped_due_to_overlap",
                dry_run=settings.dry_run,
                run_mode=run_mode,
                summary_artifact_path=str(layout.cycle_summary_path),
                failure_code=failure.code,
                failure_details=failure.details,
                error_message=failure.message,
            )
        )
        write_markdown(layout.cycle_summary_path, _failure_markdown(cycle_id, failure))
        return failure.exit_code

    cycle_db_id: int | None = None
    repo_state = None
    try:
        env_report = validate_environment(settings, root_dir=root_dir)
        if not env_report.valid:
            raise OrchestratorFailure(
                FailureCode.MISSING_ENV,
                "Missing required environment variables for this run mode.",
                details={"missing_keys": env_report.missing_keys},
            )

        writable_targets = validate_writable_targets(settings)
        if not writable_targets["artifact_root"]["ok"]:
            raise OrchestratorFailure(
                FailureCode.ARTIFACT_PATH_UNWRITABLE,
                "Artifact root is not writable.",
                details=writable_targets["artifact_root"],
            )
        if not writable_targets["db_path"]["ok"]:
            raise OrchestratorFailure(
                FailureCode.DB_UNWRITABLE,
                "SQLite path is not writable.",
                details=writable_targets["db_path"],
            )

        repo_state = sync_to_production(
            settings.root_dir,
            remote=settings.git.remote,
            branch=settings.git.production_branch,
        )
        settings = load_runtime_settings(root_dir, config_path)
        bot_definitions = load_bot_definitions(settings)
        _setup_logging(layout, [definition.bot_id for definition in bot_definitions])
        run_mode = _run_mode(settings)

        kill_reason = global_kill_switch_reason(
            settings.safety.global_kill_switch,
            settings.safety.global_kill_switch_path,
        )
        if kill_reason:
            failure = OrchestratorFailure(
                FailureCode.CONFIG_INVALID,
                kill_reason,
            )
            database.insert_cycle(
                RunCycleRecord(
                    cycle_id=cycle_id,
                    started_at=triggered_at.isoformat(),
                    expected_trigger_at=boundary.isoformat(),
                    actual_trigger_at=triggered_at.isoformat(),
                    git_sha=repo_state.git_sha,
                    status="global_kill_switch",
                    dry_run=settings.dry_run,
                    run_mode=run_mode,
                    summary_artifact_path=str(layout.cycle_summary_path),
                    failure_code=failure.code,
                    failure_details=failure.details,
                    error_message=failure.message,
                )
            )
            write_markdown(layout.cycle_summary_path, _failure_markdown(cycle_id, failure))
            _notify(settings, "Trader swarm kill switch engaged", kill_reason)
            return 0

        validation_report = validate_cycle_preconditions(settings, bot_definitions, database, repo_state)
        write_json(layout.cycle_root / "preflight_validation.json", validation_report.to_dict())
        if validation_report.should_skip:
            reason = "; ".join(check.message for check in validation_report.checks if check.status == "skip")
            database.insert_cycle(
                RunCycleRecord(
                    cycle_id=cycle_id,
                    started_at=triggered_at.isoformat(),
                    expected_trigger_at=boundary.isoformat(),
                    actual_trigger_at=triggered_at.isoformat(),
                    git_sha=repo_state.git_sha,
                    status="skipped",
                    dry_run=settings.dry_run,
                    run_mode=run_mode,
                    summary_artifact_path=str(layout.cycle_summary_path),
                    error_message=reason,
                )
            )
            write_markdown(layout.cycle_summary_path, f"# Cycle {cycle_id}\n\n- Status: `skipped`\n- Reason: {reason}")
            _notify(settings, "Trader swarm cycle skipped", reason)
            return 0
        if not validation_report.can_run:
            raise OrchestratorFailure(
                FailureCode.CONFIG_INVALID,
                "Preflight validation failed.",
                details={"checks": [check.to_dict() for check in validation_report.checks if check.status == "fail"]},
            )

        started_at = datetime.now(timezone.utc)
        cycle_db_id = database.insert_cycle(
            RunCycleRecord(
                cycle_id=cycle_id,
                started_at=started_at.isoformat(),
                expected_trigger_at=boundary.isoformat(),
                actual_trigger_at=triggered_at.isoformat(),
                git_sha=repo_state.git_sha,
                status="running",
                dry_run=settings.dry_run,
                run_mode=run_mode,
                summary_artifact_path=str(layout.cycle_summary_path),
                drift_seconds=(triggered_at - boundary).total_seconds(),
            )
        )

        session_minutes = settings.scheduler.dry_run_session_minutes if settings.dry_run else settings.scheduler.session_minutes
        try:
            swarm_result = SwarmRunner(
                settings=settings,
                bot_definitions=bot_definitions,
                artifact_layout=layout,
            ).run_session(session_minutes)
        except Exception as exc:
            raise OrchestratorFailure(
                FailureCode.SWARM_EXECUTION_FAILED,
                "Swarm execution failed.",
                details={"error": str(exc)},
            ) from exc

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
        cycle_status = "completed"
        failure_code = None
        failure_details: dict[str, Any] = {}
        if settings.analysis.enabled and settings.analysis.api_key:
            try:
                schema_document = load_analysis_schema(settings.analysis.schema_path)
                instructions = load_system_prompt(settings.analysis.prompt_path)
                client = OpenAIAnalysisClient(settings.analysis)
                request_payload = build_analysis_input(
                    bundle,
                    max_signal_events=settings.analysis.max_signal_events,
                    log_excerpts=log_excerpts,
                )
                write_json(
                    layout.analysis_request_path,
                    {"metadata": {"cycle_id": cycle_id, "git_sha": repo_state.git_sha}, "input": json.loads(request_payload)},
                )
                analysis_response = client.analyze_cycle(
                    instructions=instructions,
                    user_input=request_payload,
                    schema_document=schema_document,
                    metadata={"cycle_id": cycle_id, "git_sha": repo_state.git_sha},
                )
                write_json(layout.analysis_json_path, analysis_response.parsed_json)
                write_json(layout.analysis_raw_response_path, analysis_response.raw_response)
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
                        request_size_bytes=analysis_response.request_size_bytes,
                        response_size_bytes=analysis_response.response_size_bytes,
                        latency_ms=analysis_response.latency_ms,
                        schema_validation_result=analysis_response.schema_validation_result,
                        analysis_status="ok",
                        response_id=analysis_response.response_id,
                        request_id=analysis_response.request_id,
                    ),
                )
            except AnalysisSchemaError as exc:
                failure_code = FailureCode.SCHEMA_VALIDATION_FAILED
                failure_details = {"error": str(exc)}
                cycle_status = "completed_with_analysis_failure"
                logging.getLogger("trader_swarm.analysis").warning("schema validation failed: %s", exc)
                _write_analysis_skip(layout, f"schema validation error: {exc}")
                database.insert_ai_analysis(
                    cycle_db_id,
                    AIAnalysisRecord(
                        model=settings.analysis.model,
                        prompt_cache_key=settings.analysis.prompt_cache_key,
                        request_tokens_est=0,
                        response_tokens_est=0,
                        json_result={},
                        recommendation_grade="analysis_failed",
                        patch_request_artifact_path="",
                        request_size_bytes=0,
                        response_size_bytes=0,
                        latency_ms=0.0,
                        schema_validation_result="invalid",
                        analysis_status="schema_validation_failed",
                    ),
                )
            except OpenAIAnalysisError as exc:
                failure_code = FailureCode.OPENAI_ANALYSIS_FAILED
                failure_details = {"error": str(exc)}
                cycle_status = "completed_with_analysis_failure"
                logging.getLogger("trader_swarm.analysis").warning("analysis skipped after error: %s", exc)
                _write_analysis_skip(layout, f"analysis error: {exc}")
                _notify(settings, "Trader swarm analysis degraded", str(exc))
                database.insert_ai_analysis(
                    cycle_db_id,
                    AIAnalysisRecord(
                        model=settings.analysis.model,
                        prompt_cache_key=settings.analysis.prompt_cache_key,
                        request_tokens_est=0,
                        response_tokens_est=0,
                        json_result={},
                        recommendation_grade="analysis_failed",
                        patch_request_artifact_path="",
                        request_size_bytes=0,
                        response_size_bytes=0,
                        latency_ms=0.0,
                        schema_validation_result="not_run",
                        analysis_status="request_failed",
                    ),
                )
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
                status=cycle_status,
                dry_run=settings.dry_run,
                run_mode=run_mode,
                summary_artifact_path=str(layout.cycle_summary_path),
                analysis_artifact_path=analysis_artifact_path,
                finished_at=finished_at.isoformat(),
                total_pnl=bundle.total_pnl,
                total_drawdown=bundle.total_drawdown,
                total_trades=bundle.total_trades,
                drift_seconds=(triggered_at - boundary).total_seconds(),
                duration_seconds=(finished_at - started_at).total_seconds(),
                failure_code=failure_code,
                failure_details=failure_details,
            ),
        )
        return 0
    except DirtyWorktreeError as exc:
        failure = OrchestratorFailure(
            FailureCode.DIRTY_REPO,
            "Production branch checkout aborted because the worktree is dirty.",
            details={
                "dirty_files": exc.dirty_files,
                "remediation": ["commit", "stash intentionally", "reset explicitly"],
            },
        )
        LOGGER.exception("%s", failure)
        _persist_cycle_failure(
            database=database,
            cycle_db_id=cycle_db_id,
            cycle_id=cycle_id,
            layout=layout,
            triggered_at=triggered_at,
            boundary=boundary,
            git_sha=repo_state.git_sha if repo_state is not None else "",
            run_mode=run_mode,
            dry_run=settings.dry_run,
            failure=failure,
        )
        _notify(settings, "Trader swarm cycle failed", failure.message)
        return failure.exit_code
    except OrchestratorFailure as failure:
        LOGGER.exception("%s", failure)
        _persist_cycle_failure(
            database=database,
            cycle_db_id=cycle_db_id,
            cycle_id=cycle_id,
            layout=layout,
            triggered_at=triggered_at,
            boundary=boundary,
            git_sha=repo_state.git_sha if repo_state is not None else "",
            run_mode=run_mode,
            dry_run=settings.dry_run,
            failure=failure,
        )
        _notify(settings, "Trader swarm cycle failed", failure.message)
        return failure.exit_code
    except RepoSyncError as exc:
        failure = OrchestratorFailure(
            FailureCode.CONFIG_INVALID,
            "Repository synchronization failed.",
            details={"error": str(exc)},
        )
        LOGGER.exception("%s", failure)
        _persist_cycle_failure(
            database=database,
            cycle_db_id=cycle_db_id,
            cycle_id=cycle_id,
            layout=layout,
            triggered_at=triggered_at,
            boundary=boundary,
            git_sha=repo_state.git_sha if repo_state is not None else "",
            run_mode=run_mode,
            dry_run=settings.dry_run,
            failure=failure,
        )
        _notify(settings, "Trader swarm cycle failed", failure.message)
        return failure.exit_code
    finally:
        lock.release()


def health_check(root_dir: Path, config_path: Path | None = None) -> dict[str, Any]:
    report, _ = _build_health_report(root_dir, config_path)
    return report


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
    run_cycle_parser = subparsers.add_parser("run-cycle", help="Run one full orchestration cycle.")
    run_cycle_parser.add_argument("--dry-run", action="store_true", help="Force dry-run mode for this invocation.")
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
        return run_cycle(root_dir, config_path, dry_run_override=bool(args.dry_run))
    if args.command == "health-check":
        report, exit_code = _build_health_report(root_dir, config_path)
        print(json.dumps(report, indent=2))
        return exit_code
    if args.command == "init-db":
        return init_db(root_dir, config_path)
    if args.command == "replay-analysis":
        return replay_analysis(root_dir, Path(args.bundle).resolve(), config_path)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
