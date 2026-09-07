from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.l3f_provider.tradovate_observation import StreamHealth
from src.l3g_paper.contracts import (
    FIVE_MINUTE_PERPETUAL_POLICY,
    FIVE_MINUTE_PERPETUAL_RISK_PROFILE,
    ExecutionAction,
    PaperRuntimeState,
)
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import (
    ADDON_PROTOCOL_VERSION,
    PaperExecutionTransport,
    expected_addon_source_fingerprint,
)
from src.l3g_paper.perpetual_startup_seed import (
    write_perpetual_startup_seed_proof,
)
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.l3g_paper.verification import LocalLedgerVerifier
from tests import test_l3g_perpetual_runtime as _runtime_fixtures
from tests.test_l3g_perpetual_runtime import _CommandCapture
from tests.test_l3g_perpetual_startup_seed import _SeedFixture


_SIGNAL_KIND = "RISK_EVENT_FIVE_MINUTE_DIRECTION_CHECKPOINT"


class PerpetualSeedIntegrationRegressionTests(unittest.TestCase):
    @staticmethod
    def _make_imported_target(
        directory: str,
        *,
        artifact: dict[str, object],
        proof: dict[str, object],
        operation_id: str,
        at: str,
    ) -> tuple[PaperLedger, LaneIIIPaperRuntime, _CommandCapture]:
        ledger = PaperLedger(
            Path(directory) / "regression-target.sqlite3",
            epoch_id="L3G-PAPER-EPOCH-SEED-INTEGRATION-REGRESSION",
            policy=FIVE_MINUTE_PERPETUAL_POLICY,
            risk=FIVE_MINUTE_PERPETUAL_RISK_PROFILE,
        )
        runtime = LaneIIIPaperRuntime(ledger)
        runtime.import_perpetual_startup_seed(
            artifact,
            proof,
            operation_id=operation_id,
            manifest_sha256="4" * 64,
            expected_at=at,
        )
        transport = PaperExecutionTransport(
            ledger,
            port=48188,
            policy=FIVE_MINUTE_PERPETUAL_POLICY,
            risk=FIVE_MINUTE_PERPETUAL_RISK_PROFILE,
        )
        runtime.bind_transport(transport)
        with transport._lock:
            transport._state = "AUTHENTICATED"
            transport._authenticated = True
            transport._reconciled = True
            transport._client = object()  # type: ignore[assignment]
            transport._execution_session_id = "l3g-es-seed-chain-regression"
            transport._addon_protocol_version = ADDON_PROTOCOL_VERSION
            transport._addon_source_fingerprint = expected_addon_source_fingerprint()
        runtime._snapshot = _runtime_fixtures.PerpetualRuntimeTests._healthy_snapshot(
            at, runtime._session_context,
        )
        runtime._state = PaperRuntimeState.READY_DISARMED
        runtime._last_quote = (Decimal("100"), Decimal("100.25"), at)
        runtime._last_trade = (Decimal("100.25"), at)
        commands = _CommandCapture()
        runtime._adapter = commands  # type: ignore[assignment]
        _runtime_fixtures.PerpetualRuntimeTests._ready(runtime)
        return ledger, runtime, commands

    def test_long_two_tie_seed_imports_full_chain_and_entry_provenance(self) -> None:
        at = "2026-09-08T14:10:30Z"
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=at,
        ):
            fixture = _SeedFixture(Path(directory))
            core, artifact = fixture.complete()
            proof = write_perpetual_startup_seed_proof(
                Path(directory) / "regression-proof.json",
                artifact=artifact,
                manifest_sha256="4" * 64,
                shutdown_receipt=fixture.shutdown_receipt(artifact),
                verification_report=fixture.verification_report(artifact),
                created_at="2026-09-08T14:10:05Z",
            )
            ledger, runtime, commands = self._make_imported_target(
                directory,
                artifact=artifact,
                proof=proof,
                operation_id=fixture.operation_id,
                at=at,
            )
            try:
                checkpoints = list(reversed(ledger.recent_kind_records(
                    (_SIGNAL_KIND,), limit=10,
                )))
                self.assertEqual(len(checkpoints), 3)
                self.assertEqual(
                    [
                        row["record"]["payload"]["boundary_bias"]
                        for row in checkpoints
                    ],
                    ["LONG", "TIE", "TIE"],
                )
                self.assertEqual(
                    [
                        row["record"]["payload"]["candle_close_utc"]
                        for row in checkpoints
                    ],
                    [
                        "2026-09-08T14:00:00Z",
                        "2026-09-08T14:05:00Z",
                        "2026-09-08T14:10:00Z",
                    ],
                )

                started = runtime.operational_paper_start(
                    "seed-chain-flat-start-regression",
                )
                self.assertTrue(started["started"])
                self.assertEqual(
                    _runtime_fixtures.PerpetualRuntimeTests._entry_actions(commands),
                    [ExecutionAction.ENTER_LONG],
                )
                entry = runtime._last_decision
                self.assertIsNotNone(entry)
                self.assertEqual(
                    entry.reason_code,  # type: ignore[union-attr]
                    "FIVE_MINUTE_PERPETUAL_ENTER_LONG",
                )
                summary = entry.family_summary  # type: ignore[union-attr]
                self.assertEqual(summary["continuity_chain_length"], 3)
                self.assertEqual(
                    summary["directional_source_candle_close_utc"],
                    "2026-09-08T14:00:00Z",
                )
                self.assertEqual(
                    summary["continuity_tip_candle_close_utc"],
                    "2026-09-08T14:10:00Z",
                )
                entry_sources = set(entry.source_observation_ids)  # type: ignore[union-attr]
                root_sources = set(
                    core["boundary_chain"][0]["decision"]["source_observation_ids"]
                )
                tip_sources = set(
                    core["boundary_chain"][-1]["decision"]["source_observation_ids"]
                )
                self.assertTrue(root_sources <= entry_sources)
                self.assertTrue(tip_sources <= entry_sources)
            finally:
                ledger.close()

    def test_routine_session_transition_preserves_non_tied_root_for_tied_seed_start(self) -> None:
        """A resolver-label rollover is not a sequence gap or lost direction."""
        clock = {"at": "2026-09-01T10:29:57Z"}
        operation_id = "profile-switch-" + "c" * 32
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", side_effect=lambda: clock["at"],
        ):
            source_ledger, source_runtime, source_commands = (
                _runtime_fixtures.PerpetualRuntimeTests._runtime(
                    directory,
                    at=clock["at"],
                    perpetual=False,
                )
            )
            source_path = source_ledger.path
            target_ledger: PaperLedger | None = None
            try:
                source_runtime.on_observation_transport_state(StreamHealth.HEALTHY)
                shadow = source_runtime._perpetual_seed_shadow
                self.assertIsNotNone(shadow)
                sequence, _ = (
                    _runtime_fixtures.PerpetualRuntimeTests._warm_bullish_market_evidence(
                        source_runtime,
                        first_sequence=1,
                        first_at="2026-09-01T10:29:57Z",
                        connect=True,
                    )
                )
                # The first callback after London's 11:30 local close must
                # first seal the 10:30 completed boundary from the continuous
                # pre-close evidence domain.  It then opens the adjacent 10:35
                # boundary under the newly resolved OFF_SESSION label.
                source_runtime.ingest(
                    _runtime_fixtures.PerpetualRuntimeTests._quote(
                        sequence,
                        "2026-09-01T10:30:00.100000Z",
                        "101",
                    )
                )
                sequence += 1
                root = source_runtime.status()["perpetual_startup_seed"]
                self.assertEqual(root["latest_completed_boundary"], "2026-09-01T10:30:00Z")
                self.assertEqual(root["latest_completed_bias"], "SHORT")

                clock["at"] = "2026-09-01T10:35:00.500000Z"

                # London ends at 10:30Z on this date. These entirely fresh
                # callbacks are classified by the resolver as OFF_SESSION, but
                # CME is tradeable; the V2 profile treats that change as an
                # evidence-domain rollover, not loss of the sealed direction.
                sequence, _ = (
                    _runtime_fixtures.PerpetualRuntimeTests._warm_bullish_market_evidence(
                        source_runtime,
                        first_sequence=sequence,
                        first_at="2026-09-01T10:34:57Z",
                        connect=False,
                    )
                )
                # The helper's final 10 -> 5 bid reduction contributes the
                # bearish liquidity family. Restoring it to 10 completes an
                # equally strong bullish replenishment cycle, making this
                # authentic closed-boundary score an exact tie.
                source_runtime.ingest(
                    _runtime_fixtures.PerpetualRuntimeTests._depth(
                        sequence,
                        "2026-09-01T10:34:58.100000Z",
                        "UPDATE",
                        10,
                    )
                )
                sequence += 1
                source_runtime.ingest(
                    _runtime_fixtures.PerpetualRuntimeTests._depth(
                        sequence,
                        "2026-09-01T10:34:58.200000Z",
                        "UPDATE",
                        5,
                    )
                )
                sequence += 1
                source_runtime.ingest(
                    _runtime_fixtures.PerpetualRuntimeTests._depth(
                        sequence,
                        "2026-09-01T10:34:58.300000Z",
                        "UPDATE",
                        10,
                    )
                )
                sequence += 1
                source_runtime.ingest(
                    _runtime_fixtures.PerpetualRuntimeTests._quote(
                        sequence,
                        "2026-09-01T10:35:00.100000Z",
                        "101",
                    )
                )
                seeded = source_runtime.status()["perpetual_startup_seed"]
                self.assertEqual(seeded["latest_completed_boundary"], "2026-09-01T10:35:00Z")
                self.assertEqual(seeded["latest_completed_bias"], "TIE")
                self.assertEqual(seeded["latest_non_tied_boundary"], "2026-09-01T10:30:00Z")
                self.assertEqual(seeded["boundary_chain_length"], 2)

                artifact = source_runtime.export_perpetual_startup_seed(
                    operation_id,
                    Path(directory) / "rollover-seed.json",
                )
                chain = artifact["core"]["boundary_chain"]
                self.assertEqual([item["bias"] for item in chain], ["SHORT", "TIE"])
                self.assertEqual(
                    chain[0]["candle_close_utc"],
                    chain[1]["candle_open_utc"],
                )
                self.assertNotEqual(
                    chain[0]["session_context"]["session_id"],
                    chain[1]["session_context"]["session_id"],
                )
                self.assertEqual(
                    artifact["core"]["latest_completed"]["session_context"]["session_kind"],
                    "OFF_SESSION",
                )
                self.assertEqual(source_commands.commands, [])

                shutdown = {
                    **source_ledger.close(),
                    "verifier_shutdown": {"completed": True},
                    "runtime_watchdog_shutdown": {"completed": True},
                }
                audit_root = Path(directory) / "rollover-source-audit"
                verification = LocalLedgerVerifier(
                    source_path,
                    audit_root,
                    requested_mode="full",
                ).run()
                self.assertEqual(verification["status"], "PASS")
                proof = write_perpetual_startup_seed_proof(
                    Path(directory) / "rollover-proof.json",
                    artifact=artifact,
                    manifest_sha256="4" * 64,
                    shutdown_receipt=shutdown,
                    verification_report=verification,
                )

                target_ledger, target_runtime, target_commands = self._make_imported_target(
                    directory,
                    artifact=artifact,
                    proof=proof,
                    operation_id=operation_id,
                    at="2026-09-01T10:35:00.500000Z",
                )
                started = target_runtime.operational_paper_start(
                    "rollover-tie-seed-start",
                )
                self.assertTrue(started["started"])
                self.assertEqual(
                    _runtime_fixtures.PerpetualRuntimeTests._entry_actions(target_commands),
                    [ExecutionAction.ENTER_SHORT],
                )
                checkpoint = target_runtime._latest_five_minute_direction_checkpoint
                self.assertIsNotNone(checkpoint)
                self.assertEqual(checkpoint["boundary_bias"], "TIE")  # type: ignore[index]
                self.assertEqual(checkpoint["direction"], "SHORT")  # type: ignore[index]
                self.assertEqual(
                    target_runtime._session_context.session_kind.value,
                    "OFF_SESSION",
                )
            finally:
                if target_ledger is not None:
                    target_ledger.close()
                if source_ledger.shutdown_status() is None:
                    source_ledger.close()

    def test_gap_clears_seed_authority_until_fresh_directional_boundary(self) -> None:
        clock = {"at": "2026-09-01T14:05:00.500000Z"}
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", side_effect=lambda: clock["at"],
        ):
            ledger, runtime, commands = _runtime_fixtures.PerpetualRuntimeTests._runtime(
                directory,
                at=clock["at"],
                perpetual=False,
            )
            try:
                runtime.on_observation_transport_state(StreamHealth.HEALTHY)
                sequence, _ = _runtime_fixtures.PerpetualRuntimeTests._warm_bullish_market_evidence(
                    runtime,
                    first_sequence=1,
                    first_at="2026-09-01T14:04:57Z",
                    connect=True,
                )
                runtime.ingest(_runtime_fixtures.PerpetualRuntimeTests._quote(
                    sequence, "2026-09-01T14:05:00.100000Z", "101",
                ))
                sequence += 1
                before_gap = runtime.status()["perpetual_startup_seed"]
                self.assertEqual(before_gap["boundary_chain_length"], 1)
                self.assertIn(
                    before_gap["latest_completed_bias"], {"LONG", "SHORT"},
                )
                pre_gap_observation_ids = set(
                    runtime._perpetual_seed_observations,
                )
                self.assertTrue(pre_gap_observation_ids)

                # Warm the 14:15 evaluation from entirely post-14:10 facts
                # without ever delivering a callback that could claim 14:10.
                # The resulting decision therefore has missed_boundary_count=1.
                clock["at"] = "2026-09-01T14:14:57Z"
                sequence, _ = _runtime_fixtures.PerpetualRuntimeTests._warm_bullish_market_evidence(
                    runtime,
                    first_sequence=sequence,
                    first_at="2026-09-01T14:14:57Z",
                    connect=False,
                )
                clock["at"] = "2026-09-01T14:15:00.500000Z"
                runtime.ingest(_runtime_fixtures.PerpetualRuntimeTests._quote(
                    sequence, "2026-09-01T14:15:00.100000Z", "101",
                ))
                sequence += 1
                after_gap = runtime.status()["perpetual_startup_seed"]
                self.assertEqual(after_gap["boundary_chain_length"], 0)
                self.assertIsNone(after_gap["latest_completed_boundary"])
                self.assertIsNone(after_gap["latest_non_tied_boundary"])
                self.assertEqual(after_gap["captured_observations"], 0)
                with self.assertRaisesRegex(
                    RuntimeError,
                    "CONTINUITY|CURRENT_BOUNDARY_UNAVAILABLE|NON_TIED_UNAVAILABLE",
                ):
                    runtime.export_perpetual_startup_seed(
                        "profile-switch-" + "a" * 32,
                        Path(directory) / "gap-must-refuse.json",
                    )

                sequence, _ = _runtime_fixtures.PerpetualRuntimeTests._warm_bullish_market_evidence(
                    runtime,
                    first_sequence=sequence,
                    first_at="2026-09-01T14:19:57Z",
                    connect=False,
                )
                clock["at"] = "2026-09-01T14:20:00.500000Z"
                runtime.ingest(_runtime_fixtures.PerpetualRuntimeTests._quote(
                    sequence, "2026-09-01T14:20:00.100000Z", "101",
                ))
                recovered = runtime.status()["perpetual_startup_seed"]
                self.assertEqual(recovered["boundary_chain_length"], 1)
                self.assertEqual(
                    recovered["latest_completed_boundary"],
                    "2026-09-01T14:20:00Z",
                )
                self.assertIn(
                    recovered["latest_completed_bias"], {"LONG", "SHORT"},
                )
                artifact = runtime.export_perpetual_startup_seed(
                    "profile-switch-" + "b" * 32,
                    Path(directory) / "fresh-post-gap.json",
                )
                exported_ids = {
                    item["wire"]["observation_id"]
                    for item in artifact["core"]["observations"]
                }
                self.assertTrue(pre_gap_observation_ids.isdisjoint(exported_ids))
                self.assertEqual(
                    len(artifact["core"]["boundary_chain"]), 1,
                )
                self.assertEqual(commands.commands, [])
            finally:
                ledger.close()

    def test_backward_timestamp_immediately_revokes_seed_authority(self) -> None:
        clock = {"at": "2026-09-01T14:05:00.500000Z"}
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", side_effect=lambda: clock["at"],
        ):
            ledger, runtime, commands = _runtime_fixtures.PerpetualRuntimeTests._runtime(
                directory,
                at=clock["at"],
                perpetual=False,
            )
            try:
                runtime.on_observation_transport_state(StreamHealth.HEALTHY)
                sequence, _ = _runtime_fixtures.PerpetualRuntimeTests._warm_bullish_market_evidence(
                    runtime,
                    first_sequence=1,
                    first_at="2026-09-01T14:04:57Z",
                    connect=True,
                )
                runtime.ingest(_runtime_fixtures.PerpetualRuntimeTests._quote(
                    sequence, "2026-09-01T14:05:00.100000Z", "101",
                ))
                sequence += 1
                sealed = runtime.status()["perpetual_startup_seed"]
                self.assertEqual(sealed["boundary_chain_length"], 1)

                # A queue-later callback with an older receipt timestamp is a
                # temporal continuity break. It must revoke export authority
                # immediately; waiting for a later sequence-gap callback leaves
                # a window in which the stale chain could be handed to V2.
                runtime.ingest(_runtime_fixtures.PerpetualRuntimeTests._quote(
                    sequence, "2026-09-01T14:04:59.900000Z", "101",
                ))
                revoked = runtime.status()["perpetual_startup_seed"]
                self.assertEqual(revoked["boundary_chain_length"], 0)
                self.assertIsNone(revoked["latest_completed_boundary"])
                self.assertIsNone(revoked["latest_non_tied_boundary"])
                self.assertEqual(revoked["captured_observations"], 0)
                with self.assertRaisesRegex(
                    RuntimeError,
                    "CONTINUITY|CURRENT_BOUNDARY_UNAVAILABLE|NON_TIED_UNAVAILABLE",
                ):
                    runtime.export_perpetual_startup_seed(
                        "profile-switch-" + "d" * 32,
                        Path(directory) / "backward-timestamp-must-refuse.json",
                    )
                self.assertEqual(commands.commands, [])
            finally:
                ledger.close()

    def test_over_nine_thousand_callbacks_preserve_shadow_liveness_and_bounds(self) -> None:
        at = "2026-09-01T14:05:30Z"
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=at,
        ):
            ledger, runtime, commands = _runtime_fixtures.PerpetualRuntimeTests._runtime(
                directory,
                at=at,
                perpetual=False,
            )
            try:
                runtime.on_observation_transport_state(StreamHealth.HEALTHY)
                shadow = runtime._perpetual_seed_shadow
                self.assertIsNotNone(shadow)
                reset_count = shadow.reset_count()  # type: ignore[union-attr]
                moment = datetime(2026, 9, 1, 14, 5, 1, tzinfo=timezone.utc)
                callback_count = 9_001
                for sequence in range(1, callback_count + 1):
                    runtime.ingest(_runtime_fixtures.PerpetualRuntimeTests._quote(
                        sequence,
                        moment.isoformat().replace("+00:00", "Z"),
                        "101",
                    ))
                    moment += timedelta(milliseconds=1)

                self.assertEqual(
                    shadow.reset_count(), reset_count,  # type: ignore[union-attr]
                )
                status = runtime.status()["perpetual_startup_seed"]
                self.assertLessEqual(status["captured_observations"], 64)
                self.assertLess(
                    status["captured_observations"], callback_count,
                )
                self.assertNotIn(
                    "startup-observation-1",
                    runtime._perpetual_seed_observations,
                )
                self.assertIn(
                    f"startup-observation-{callback_count}",
                    runtime._perpetual_seed_observations,
                )
                self.assertEqual(commands.commands, [])
            finally:
                ledger.close()


if __name__ == "__main__":
    unittest.main()
