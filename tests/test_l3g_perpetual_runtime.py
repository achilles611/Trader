from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from itertools import permutations
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation
from src.l3f_provider.tradovate_observation import StreamHealth
from src.lane_iii.contracts import canonical_hash
from src.l3g_paper.contracts import (
    ACCOUNT_BINDING,
    FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
    FIVE_MINUTE_PERPETUAL_POLICY,
    FIVE_MINUTE_PERPETUAL_PROFILE,
    FIVE_MINUTE_PERPETUAL_RISK_PROFILE,
    FIVE_MINUTE_POLICY,
    FIVE_MINUTE_PROFILE,
    FIVE_MINUTE_RISK_PROFILE,
    BookCompleteness,
    ExecutionAccountBinding,
    ExecutionAction,
    FiveMinutePerpetualPaperRiskProfile,
    HypothesisKind,
    PaperDecision,
    PaperDecisionKind,
    PaperDirection,
    PaperEntryOwner,
    PaperRuntimeState,
    SequenceAuthority,
    resolve_paper_profile,
)
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import (
    ADDON_PROTOCOL_VERSION,
    PaperExecutionTransport,
    expected_addon_source_fingerprint,
)
from src.l3g_paper.risk import PaperRiskAuthority, PaperRiskSnapshot
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.l3g_paper.perpetual_startup_seed import (
    PERPETUAL_STARTUP_SEED_EXPORT_KIND,
    build_perpetual_observation_proof,
    write_perpetual_startup_seed_proof,
)
from src.l3g_paper.sessions import PaperSessionResolver
from src.l3g_paper.slim_status import derive_slim_paper_status
from src.l3g_paper.verification import LocalLedgerVerifier


# Shared V2 fixture time. Tests of legacy session cutoffs use their own exact
# RTH timestamps so profile-window changes cannot silently alter intent.
NOW = "2026-09-01T20:54:00Z"
HOLIDAY_NOW = "2026-09-07T14:00:00Z"


def _dotnet_roundtrip_utc(moment: datetime) -> str:
    """Match DateTime.ToString("o") for the AddOn's authentic UTC frames."""
    return (
        moment.strftime("%Y-%m-%dT%H:%M:%S.")
        + f"{moment.microsecond:06d}7Z"
    )


class _CommandCapture:
    def __init__(self) -> None:
        self.commands: list[object] = []
        self.grants: list[object] = []

    def submit(self, command: object, grant: object) -> None:
        self.commands.append(command)
        self.grants.append(grant)


class PerpetualRuntimeTests(unittest.TestCase):
    @staticmethod
    def _healthy_snapshot(
        at: str,
        context: object,
        *,
        direction: PaperDirection = PaperDirection.FLAT,
        position_opened_at: str | None = None,
    ) -> PaperRiskSnapshot:
        quantity = 0 if direction is PaperDirection.FLAT else 1
        return PaperRiskSnapshot(
            at,
            account_name="Sim101",
            account_class="LOCAL_SIMULATION",
            instrument="MNQ SEP26",
            canonical_contract="MNQU6",
            current_position=direction,
            current_position_quantity=quantity,
            working_owned_orders=0,
            working_entry_orders=0,
            foreign_activity=False,
            position_snapshot_complete=True,
            order_snapshot_complete=True,
            reconciliation_current=True,
            local_bridge_healthy=True,
            market_price_connected=True,
            execution_bridge_healthy=True,
            evidence_warmed=True,
            depth_reset_recovery=False,
            quote_observed_at=at,
            classified_trade_observed_at=at,
            depth_mutation_observed_at=at,
            position_opened_at=position_opened_at,
            session_kind=context.session_kind,  # type: ignore[attr-defined]
            session_id=context.session_id,  # type: ignore[attr-defined]
            trade_date=context.trade_date,  # type: ignore[attr-defined]
            session_profile_hash=context.session_profile_hash,  # type: ignore[attr-defined]
            session_generation=context.session_generation,  # type: ignore[attr-defined]
        )

    @staticmethod
    def _runtime(
        directory: str,
        *,
        at: str = NOW,
        perpetual: bool = True,
    ) -> tuple[PaperLedger, LaneIIIPaperRuntime, _CommandCapture]:
        profile = FIVE_MINUTE_PERPETUAL_PROFILE if perpetual else FIVE_MINUTE_PROFILE
        path = Path(directory) / ("perpetual.sqlite3" if perpetual else "legacy.sqlite3")
        ledger = PaperLedger(
            path,
            epoch_id=(
                "L3G-PAPER-EPOCH-TEST-PERPETUAL"
                if perpetual else "L3G-PAPER-EPOCH-TEST-LEGACY"
            ),
            policy=profile.policy,
            risk=profile.risk,
        )
        runtime = LaneIIIPaperRuntime(ledger)
        transport = PaperExecutionTransport(
            ledger,
            port=48178 if perpetual else 48179,
            policy=profile.policy,
            risk=profile.risk,
        )
        runtime.bind_transport(transport)
        with transport._lock:
            transport._state = "AUTHENTICATED"
            transport._authenticated = True
            transport._reconciled = True
            transport._client = object()  # type: ignore[assignment]
            transport._execution_session_id = "l3g-es-perpetual-runtime-test"
            transport._addon_protocol_version = ADDON_PROTOCOL_VERSION
            transport._addon_source_fingerprint = expected_addon_source_fingerprint()
        context = PaperSessionResolver().resolve(at, generation=4).context
        runtime._session_context = context
        runtime.policy._activate_session(context)
        ledger.set_session_context(context)
        runtime._snapshot = PerpetualRuntimeTests._healthy_snapshot(at, context)
        runtime._state = PaperRuntimeState.READY_DISARMED
        runtime._last_quote = (Decimal("100"), Decimal("100.25"), at)
        runtime._last_trade = (Decimal("100.25"), at)
        capture = _CommandCapture()
        runtime._adapter = capture  # type: ignore[assignment]
        return ledger, runtime, capture

    @staticmethod
    def _decision(
        runtime: LaneIIIPaperRuntime,
        direction: PaperDirection,
        *,
        created_at: str,
        candle_close_utc: str,
        shape: str = "ENTER",
        suffix: str = "signal",
    ) -> PaperDecision:
        candle_close = datetime.fromisoformat(candle_close_utc.replace("Z", "+00:00"))
        candle_open_utc = (
            candle_close
            - timedelta(seconds=runtime.policy.artifact.decision_interval_seconds)
        ).isoformat().replace("+00:00", "Z")
        if shape == "ENTER":
            decision_kind = (
                PaperDecisionKind.LONG
                if direction is PaperDirection.LONG
                else PaperDecisionKind.SHORT
            )
            reason = f"FIVE_MINUTE_ENTER_{direction.value}"
            action = "ENTER"
        elif shape == "HOLD":
            decision_kind = PaperDecisionKind.NO_TRADE
            reason = f"FIVE_MINUTE_HOLD_{direction.value}"
            action = "HOLD"
        elif shape == "REVERSE":
            decision_kind = PaperDecisionKind.EXIT
            reason = f"FIVE_MINUTE_REVERSE_TO_{direction.value}"
            action = "REVERSE"
        elif shape == "PENDING":
            decision_kind = PaperDecisionKind.NO_TRADE
            reason = f"FIVE_MINUTE_PENDING_ORDER_{direction.value}"
            action = "PENDING"
        else:
            raise AssertionError(f"unsupported fixture shape: {shape}")
        hypothesis = (
            HypothesisKind.BULLISH_REVERSAL
            if direction is PaperDirection.LONG
            else HypothesisKind.BEARISH_CONTINUATION
        )
        return PaperDecision(
            paper_decision_id="l3g-pd-" + canonical_hash({
                "suffix": suffix,
                "direction": direction.value,
                "shape": shape,
                "candle_close": candle_close_utc,
            })[:32],
            paper_policy_id=runtime.policy.artifact.policy_id,
            paper_policy_hash=runtime.policy.artifact.configuration_hash,
            decision=decision_kind,
            created_at=created_at,
            expires_at=(
                datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                + timedelta(seconds=30)
            ).isoformat().replace("+00:00", "Z"),
            hypothesis_kind=hypothesis,
            direction=(
                direction
                if decision_kind in {PaperDecisionKind.LONG, PaperDecisionKind.SHORT}
                else PaperDirection.FLAT
            ),
            relative_support=Decimal("0.25"),
            family_summary={
                "action": action,
                "bias": direction.value,
                "prior_position": (
                    PaperDirection.FLAT.value if shape == "ENTER"
                    else direction.value if shape == "HOLD"
                    else (PaperDirection.SHORT if direction is PaperDirection.LONG else PaperDirection.LONG).value
                ),
                "target_position": direction.value,
                "candle_open_utc": candle_open_utc,
                "candle_close_utc": candle_close_utc,
                "missed_boundary_count": 0,
            },
            source_observation_ids=(f"observation-{suffix}",),
            source_local_sequences=(1,),
            source_payload_hashes=(canonical_hash({"source": suffix}),),
            sequence_authority=SequenceAuthority.LOCAL_CALLBACK_ORDER_ONLY,
            book_completeness=BookCompleteness.UNVERIFIED,
            scientific_eligibility=False,
            reason_code=reason,
            session_kind=runtime._session_context.session_kind,
            session_id=runtime._session_context.session_id,
            trade_date=runtime._session_context.trade_date,
            session_profile_hash=runtime._session_context.session_profile_hash,
            session_generation=runtime._session_context.session_generation,
            commissioning=False,
            strategy_generated=True,
            scientific_evidence=False,
        )

    @staticmethod
    def _commit_signal(runtime: LaneIIIPaperRuntime, decision: PaperDecision) -> None:
        runtime.ledger.append(
            "DECISION",
            decision.payload(),
            identity=decision.paper_decision_id,
            occurred_at=decision.created_at,
            execution_session_id=runtime._execution_session_id(),
        )
        if not runtime._record_five_minute_direction_checkpoint_locked(decision):
            raise AssertionError("test signal did not produce a durable checkpoint")

    @staticmethod
    def _ready(runtime: LaneIIIPaperRuntime) -> None:
        runtime.operational_paper_readiness = lambda _preflight=None: {  # type: ignore[method-assign]
            "result": "READY",
            "blocking_reasons": [],
            "deferred_entry_reasons": [],
            "ledger": {"status": "PASS"},
        }

    @staticmethod
    def _entry_actions(capture: _CommandCapture) -> list[ExecutionAction]:
        return [
            command.action  # type: ignore[attr-defined]
            for command in capture.commands
            if command.action in {ExecutionAction.ENTER_LONG, ExecutionAction.ENTER_SHORT}  # type: ignore[attr-defined]
        ]

    @staticmethod
    def _observation(
        sequence: int,
        kind: str,
        at: str,
        payload: dict[str, object],
    ) -> NinjaTraderObservation:
        return NinjaTraderObservation(
            f"startup-observation-{sequence}",
            "authentic-startup-market-session",
            kind,
            at,
            sequence,
            payload,
            provider_timestamp=at,
        )

    @classmethod
    def _connection(cls, sequence: int, at: str) -> NinjaTraderObservation:
        return cls._observation(
            sequence,
            "CONNECTION",
            at,
            {"scope": "MARKET_DATA", "price_status": "Connected"},
        )

    @classmethod
    def _quote(cls, sequence: int, at: str, bid: str = "100") -> NinjaTraderObservation:
        price = Decimal(bid)
        return cls._observation(
            sequence,
            "QUOTE",
            at,
            {
                "contract_id": "MNQ SEP26",
                "bid": str(price),
                "ask": str(price + Decimal("0.25")),
                "bid_size": 10,
                "ask_size": 10,
            },
        )

    @classmethod
    def _trade(
        cls,
        sequence: int,
        at: str,
        quote: NinjaTraderObservation,
        price: str,
    ) -> NinjaTraderObservation:
        return cls._observation(
            sequence,
            "TRADE",
            at,
            {
                "contract_id": "MNQ SEP26",
                "price": price,
                "size": 2,
                "aggressor_side": "UNKNOWN",
                "aggressor_source": "BID_ASK_CLASSIFICATION",
                "bid_at_trade": quote.payload["bid"],
                "ask_at_trade": quote.payload["ask"],
                "derivation_quote_observation_id": quote.observation_id,
            },
        )

    @classmethod
    def _depth(
        cls,
        sequence: int,
        at: str,
        operation: str,
        volume: int,
    ) -> NinjaTraderObservation:
        return cls._observation(
            sequence,
            "DEPTH",
            at,
            {
                "contract_id": "MNQ SEP26",
                "bids": [{"price": "98", "size": volume}],
                "asks": [{"price": "101", "size": 10}],
                "operation": operation,
                "side": "Bid",
                "mutation_price": "98",
                "mutation_volume": volume,
                "mutation_position": 0,
                "is_reset": False,
            },
        )

    @classmethod
    def _warm_bullish_market_evidence(
        cls,
        runtime: LaneIIIPaperRuntime,
        *,
        first_sequence: int,
        first_at: str,
        connect: bool,
    ) -> tuple[int, NinjaTraderObservation]:
        """Admit authentic quote/trade/depth frames for all three families."""
        sequence = first_sequence
        moment = datetime.fromisoformat(first_at.replace("Z", "+00:00"))

        def next_at() -> str:
            nonlocal moment
            value = moment.isoformat().replace("+00:00", "Z")
            moment += timedelta(milliseconds=50)
            return value

        if connect:
            runtime.ingest(cls._connection(sequence, next_at()))
            sequence += 1
        last_trade: NinjaTraderObservation | None = None
        # One complete retained flow/structure window makes this helper valid
        # both for a fresh policy and for refreshing evidence five minutes
        # later without depending on expired observations from the prior bar.
        for price in ("100", "99", "100", "100", "99", "100", "99", "100"):
            quote = cls._quote(sequence, next_at(), price)
            runtime.ingest(quote)
            sequence += 1
            last_trade = cls._trade(sequence, next_at(), quote, price)
            runtime.ingest(last_trade)
            sequence += 1
        # The bid reduction creates authentic resting-liquidity evidence and
        # clears depth-reset recovery. It is deliberately the final warm-up
        # frame so no pre-boundary callback can prematurely claim a boundary.
        for operation, volume in (
            ("ADD", 10),
            ("UPDATE", 5),
        ):
            runtime.ingest(cls._depth(sequence, next_at(), operation, volume))
            sequence += 1
        assert last_trade is not None
        return sequence, last_trade

    @staticmethod
    def _flat_reconciliation(receipt_id: str, at: str) -> dict[str, object]:
        return {
            "message_type": "RECONCILIATION",
            "receipt_id": receipt_id,
            "account_name": "Sim101",
            "account_class": "LOCAL_SIMULATION",
            "instrument": "MNQ SEP26",
            "position_quantity": 0,
            "working_order_count": 0,
            "working_entry_count": 0,
            "position_snapshot_complete": True,
            "order_snapshot_complete": True,
            "foreign_activity": False,
            "protective_stop_state": "NONE",
            "timestamp": at,
        }

    @staticmethod
    def _filled_order(
        role: str,
        native_order_id: str,
        *,
        command_id: str | None = None,
    ) -> dict[str, object]:
        return {
            "message_type": "ORDER_EVENT",
            "order_role": role,
            "order_state": "FILLED",
            "native_order_id": native_order_id,
            "command_id": command_id,
        }

    def _start_and_fill_long(
        self,
        runtime: LaneIIIPaperRuntime,
        capture: _CommandCapture,
        *,
        request_id: str,
        suffix: str,
        at: str = NOW,
    ) -> None:
        signal = self._decision(
            runtime,
            PaperDirection.LONG,
            created_at="2026-09-01T20:54:00Z",
            candle_close_utc="2026-09-01T20:54:00Z",
            suffix=suffix,
        )
        self._commit_signal(runtime, signal)
        self._ready(runtime)
        self.assertTrue(runtime.operational_paper_start(request_id)["started"])
        runtime.on_execution_message({
            "message_type": "EXECUTION_EVENT",
            "order_role": "ENTRY",
            "direction": "LONG",
            "price": "100.25",
            "quantity": 1,
            "native_execution_id": suffix + "-entry-execution",
            "native_order_id": suffix + "-entry-order",
            "account_name": "Sim101",
            "instrument": "MNQ SEP26",
            "timestamp": at,
        })
        # Isolate protective-fill settlement from the separately tested V2
        # positioned-reconciliation handshake. This models the already-owned
        # protective order proven by that handshake.
        runtime._protective_order_id = suffix + "-protective-order"
        runtime._snapshot = replace(
            runtime._snapshot,
            working_owned_orders=1,
            protective_stop_state="WORKING",
        )
        self.assertEqual(runtime.state, PaperRuntimeState.LONG)
        self.assertEqual(self._entry_actions(capture), [ExecutionAction.ENTER_LONG])

    def test_v2_identity_is_sealed_and_v1_hashes_remain_unchanged(self) -> None:
        self.assertEqual(
            FIVE_MINUTE_POLICY.configuration_hash,
            "9d94d10feb963a1815b9928893868ddc793d5ec42b6835148d65ed0c9956bdac",
        )
        self.assertEqual(
            FIVE_MINUTE_RISK_PROFILE.configuration_hash,
            "c6f84af61e5769ab7285330e6e092aaf5b801c6d38e101cbf908ee00e224e0a1",
        )
        self.assertEqual(
            FIVE_MINUTE_PERPETUAL_POLICY.configuration_hash,
            "daf3cc6daacdb32d5629fd6cbd97ef2246172426a00392f59f1246c567658fe7",
        )
        self.assertEqual(
            FIVE_MINUTE_PERPETUAL_RISK_PROFILE.configuration_hash,
            "2d50a767772a9bef9edb752cea0417e51f1df1b46ef7be63289ed2bac26dc09c",
        )
        self.assertNotEqual(
            FIVE_MINUTE_PERPETUAL_POLICY.configuration_hash,
            FIVE_MINUTE_POLICY.configuration_hash,
        )
        self.assertNotEqual(
            FIVE_MINUTE_PERPETUAL_RISK_PROFILE.configuration_hash,
            FIVE_MINUTE_RISK_PROFILE.configuration_hash,
        )
        self.assertIs(
            resolve_paper_profile(FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION),
            FIVE_MINUTE_PERPETUAL_PROFILE,
        )
        self.assertEqual(FIVE_MINUTE_PROFILE.display_name, "5-minute session bias (legacy V1)")

    def test_operational_start_outside_legacy_window_stays_red_and_exactly_blocked_when_flat(self) -> None:
        # 09:00 America/New_York is an intentional gap between the London
        # and New York profiles, while the exchange itself is tradeable.
        outside_at = "2026-09-01T13:00:00Z"
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=outside_at,
        ):
            ledger, runtime, capture = self._runtime(directory, at=outside_at)
            try:
                legacy_risk = PaperRiskAuthority(
                    profile=FIVE_MINUTE_RISK_PROFILE,
                    policy=FIVE_MINUTE_POLICY,
                )
                legacy_reasons = legacy_risk.preflight_reasons(
                    runtime._snapshot, at=outside_at,
                )
                self.assertIn("OFF_SESSION", legacy_reasons)

                self._ready(runtime)
                result = runtime.operational_paper_start("perpetual-outside-window")
                self.assertTrue(result["started"])
                self.assertEqual(runtime.state, PaperRuntimeState.PAPER_RUNNING)
                self.assertEqual(capture.commands, [])

                status = runtime.status()
                requirement = status["position_requirement"]
                self.assertEqual(requirement["state"], "BLOCKED_FLAT")  # type: ignore[index]
                self.assertEqual(
                    requirement["primary_blocker"],  # type: ignore[index]
                    "NO_COMPLETED_FIVE_MINUTE_SIGNAL",
                )
                slim = derive_slim_paper_status(
                    status,
                    {
                        "status": "PASS",
                        "chain_valid": True,
                        "checkpoint_valid": True,
                        "full_scan_required": False,
                        "quick_check": "ok",
                        "completed_at": outside_at,
                    },
                    {"market_observer_active": True, "market_observer_state": "ACTIVE"},
                    {"result": "READY", "blocking_reasons": []},
                    now=datetime.fromisoformat(outside_at.replace("Z", "+00:00")),
                )
                self.assertEqual(slim["light"], "RED")
                self.assertEqual(
                    slim["label"],
                    "FLAT — BLOCKED: NO_COMPLETED_FIVE_MINUTE_SIGNAL",
                )
            finally:
                ledger.close()

    def test_start_uses_latest_durable_non_tied_signal_and_submits_exactly_once(self) -> None:
        with TemporaryDirectory() as directory, patch("src.l3g_paper.runtime._now", return_value=NOW):
            ledger, runtime, capture = self._runtime(directory)
            try:
                older = self._decision(
                    runtime,
                    PaperDirection.LONG,
                    created_at="2026-09-01T20:46:00Z",
                    candle_close_utc="2026-09-01T20:45:00Z",
                    suffix="older-long",
                )
                latest = self._decision(
                    runtime,
                    PaperDirection.SHORT,
                    created_at="2026-09-01T20:54:00Z",
                    candle_close_utc="2026-09-01T20:54:00Z",
                    suffix="latest-short",
                )
                self._commit_signal(runtime, older)
                self._commit_signal(runtime, latest)
                self._ready(runtime)

                result = runtime.operational_paper_start("perpetual-latest-signal")
                self.assertTrue(result["started"])
                self.assertEqual(runtime.state, PaperRuntimeState.ENTRY_PENDING)
                self.assertEqual(self._entry_actions(capture), [ExecutionAction.ENTER_SHORT])
                self.assertEqual(
                    runtime.status()["position_requirement"]["desired_position"],  # type: ignore[index]
                    "SHORT",
                )

                runtime._maintain_perpetual_position_locked("DUPLICATE_CALLBACK")
                runtime._maintain_perpetual_position_locked("DUPLICATE_CALLBACK")
                self.assertEqual(self._entry_actions(capture), [ExecutionAction.ENTER_SHORT])
            finally:
                ledger.close()

    def test_missed_boundary_cannot_submit_entry_or_reversal_side_effects(self) -> None:
        """A continuity-rejected decision is evidence only, never authority."""
        for initially_positioned in (False, True):
            with self.subTest(initially_positioned=initially_positioned):
                with TemporaryDirectory() as directory, patch(
                    "src.l3g_paper.runtime._now", return_value=NOW,
                ):
                    ledger, runtime, capture = self._runtime(directory)
                    try:
                        runtime._session_generation = 4
                        self._ready(runtime)
                        started = runtime.operational_paper_start(
                            "perpetual-missed-boundary-regression",
                        )
                        self.assertTrue(started["started"])
                        self.assertEqual(capture.commands, [])

                        target = (
                            PaperDirection.SHORT
                            if initially_positioned else PaperDirection.LONG
                        )
                        shape = "REVERSE" if initially_positioned else "ENTER"
                        decision = self._decision(
                            runtime,
                            target,
                            created_at=NOW,
                            candle_close_utc="2026-09-01T20:54:00Z",
                            shape=shape,
                            suffix=f"missed-{shape.lower()}",
                        )
                        decision = replace(
                            decision,
                            family_summary={
                                **decision.family_summary,
                                "missed_boundary_count": 1,
                            },
                        )
                        runtime.policy.ingest_runtime = (  # type: ignore[method-assign]
                            lambda *_args, **_kwargs: decision
                        )

                        exit_calls: list[tuple[object, ...]] = []
                        if initially_positioned:
                            runtime._position = PaperDirection.LONG
                            runtime._position_quantity = 1
                            runtime._state = PaperRuntimeState.LONG
                            runtime._entry_owner = PaperEntryOwner.STRATEGY
                            runtime._snapshot = replace(
                                self._healthy_snapshot(
                                    NOW,
                                    runtime._session_context,
                                    direction=PaperDirection.LONG,
                                    position_opened_at=NOW,
                                ),
                                working_owned_orders=1,
                                protective_stop_state="WORKING",
                            )
                            runtime._request_exit = (  # type: ignore[method-assign]
                                lambda *args, **_kwargs: exit_calls.append(args) or True
                            )

                        runtime.ingest(self._quote(1, NOW))

                        self.assertEqual(capture.commands, [])
                        self.assertEqual(exit_calls, [])
                        self.assertIsNone(
                            runtime._latest_five_minute_direction_checkpoint,
                        )
                        self.assertEqual(
                            runtime._perpetual_flat_blocker,
                            "FIVE_MINUTE_BOUNDARY_CONTINUITY_UNPROVEN",
                        )
                    finally:
                        ledger.close()

    def test_tie_does_not_replace_latest_non_tied_signal_on_startup(self) -> None:
        with TemporaryDirectory() as directory, patch("src.l3g_paper.runtime._now", return_value=NOW):
            ledger, runtime, capture = self._runtime(directory)
            try:
                signal = self._decision(
                    runtime,
                    PaperDirection.LONG,
                    created_at="2026-09-01T20:53:30Z",
                    candle_close_utc="2026-09-01T20:53:30Z",
                    suffix="non-tied-long",
                )
                self._commit_signal(runtime, signal)
                prior = dict(runtime._latest_five_minute_direction_checkpoint or {})
                tie = PaperDecision(
                    "l3g-pd-" + "7" * 32,
                    runtime.policy.artifact.policy_id,
                    runtime.policy.artifact.configuration_hash,
                    PaperDecisionKind.NO_TRADE,
                    "2026-09-01T20:54:00Z",
                    "2026-09-01T20:54:30Z",
                    None,
                    PaperDirection.FLAT,
                    Decimal("0"),
                    {
                        "action": "BLOCKED",
                        "bias": "TIE",
                        "target_position": "FLAT",
                        "candle_open_utc": "2026-09-01T20:53:30Z",
                        "candle_close_utc": "2026-09-01T20:54:00Z",
                        "missed_boundary_count": 0,
                    },
                    ("observation-tie",),
                    (2,),
                    (canonical_hash({"source": "tie"}),),
                    SequenceAuthority.LOCAL_CALLBACK_ORDER_ONLY,
                    BookCompleteness.UNVERIFIED,
                    False,
                    "FIVE_MINUTE_BIAS_TIE_FLAT",
                    runtime._session_context.session_kind,
                    runtime._session_context.session_id,
                    runtime._session_context.trade_date,
                    runtime._session_context.session_profile_hash,
                    runtime._session_context.session_generation,
                )
                ledger.append(
                    "DECISION", tie.payload(), identity=tie.paper_decision_id,
                    occurred_at=tie.created_at,
                )
                self.assertTrue(runtime._record_five_minute_direction_checkpoint_locked(tie))
                checkpoint = runtime._latest_five_minute_direction_checkpoint
                self.assertIsNotNone(checkpoint)
                self.assertNotEqual(checkpoint, prior)
                self.assertEqual(checkpoint["direction"], PaperDirection.LONG.value)  # type: ignore[index]
                self.assertEqual(checkpoint["boundary_bias"], "TIE")  # type: ignore[index]
                self.assertEqual(
                    checkpoint["prior_signal_hash"], prior["signal_hash"],  # type: ignore[index]
                )

                self._ready(runtime)
                runtime.operational_paper_start("perpetual-tie-startup")
                self.assertEqual(self._entry_actions(capture), [ExecutionAction.ENTER_LONG])
            finally:
                ledger.close()

    def test_missed_boundary_invalidates_tie_chain_until_fresh_direction(self) -> None:
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=NOW,
        ):
            ledger, runtime, _ = self._runtime(directory)
            try:
                prior = self._decision(
                    runtime,
                    PaperDirection.SHORT,
                    created_at="2026-09-01T20:53:30Z",
                    candle_close_utc="2026-09-01T20:53:30Z",
                    suffix="continuity-prior",
                )
                self._commit_signal(runtime, prior)

                missed = self._decision(
                    runtime,
                    PaperDirection.SHORT,
                    created_at="2026-09-01T20:54:00Z",
                    candle_close_utc="2026-09-01T20:54:00Z",
                    suffix="continuity-missed",
                )
                missed = replace(
                    missed,
                    family_summary={
                        **missed.family_summary,
                        "missed_boundary_count": 1,
                    },
                )
                ledger.append(
                    "DECISION", missed.payload(),
                    identity=missed.paper_decision_id,
                    occurred_at=missed.created_at,
                )
                self.assertFalse(
                    runtime._record_five_minute_direction_checkpoint_locked(
                        missed,
                    ),
                )
                self.assertIsNone(
                    runtime._latest_five_minute_direction_checkpoint,
                )
                self.assertFalse(runtime._perpetual_signal_ledger_verified)

                tied_source = self._decision(
                    runtime,
                    PaperDirection.LONG,
                    created_at="2026-09-01T20:54:30Z",
                    candle_close_utc="2026-09-01T20:54:30Z",
                    suffix="continuity-tie",
                )
                tie = replace(
                    tied_source,
                    decision=PaperDecisionKind.NO_TRADE,
                    direction=PaperDirection.FLAT,
                    family_summary={
                        **tied_source.family_summary,
                        "action": "BLOCKED",
                        "bias": "TIE",
                        "target_position": "FLAT",
                        "missed_boundary_count": 0,
                    },
                    reason_code="FIVE_MINUTE_BIAS_TIE_FLAT",
                )
                ledger.append(
                    "DECISION", tie.payload(), identity=tie.paper_decision_id,
                    occurred_at=tie.created_at,
                )
                self.assertTrue(
                    runtime._record_five_minute_direction_checkpoint_locked(
                        tie,
                    ),
                )
                self.assertIsNone(
                    runtime._latest_five_minute_direction_checkpoint,
                )
                self.assertEqual(
                    runtime._perpetual_flat_blocker,
                    "FIVE_MINUTE_TIE_WITHOUT_PRIOR_NON_TIED_SIGNAL",
                )
                self.assertIsNone(runtime._perpetual_signal_fault)

                recovered = self._decision(
                    runtime,
                    PaperDirection.LONG,
                    created_at="2026-09-01T20:55:00Z",
                    candle_close_utc="2026-09-01T20:55:00Z",
                    suffix="continuity-recovered",
                )
                self._commit_signal(runtime, recovered)
                checkpoint = runtime._latest_five_minute_direction_checkpoint
                self.assertIsNotNone(checkpoint)
                self.assertEqual(
                    checkpoint["direction"], PaperDirection.LONG.value,  # type: ignore[index]
                )
                self.assertTrue(runtime._perpetual_signal_ledger_verified)
                self.assertIsNone(runtime._perpetual_flat_blocker)
            finally:
                ledger.close()

    def test_discontinuous_tie_is_refused_before_append_and_fresh_direction_recovers(self) -> None:
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=NOW,
        ):
            ledger, runtime, _ = self._runtime(directory)
            path = ledger.path
            try:
                prior = self._decision(
                    runtime,
                    PaperDirection.SHORT,
                    created_at="2026-09-01T20:53:30Z",
                    candle_close_utc="2026-09-01T20:53:30Z",
                    suffix="discontinuous-prior",
                )
                self._commit_signal(runtime, prior)
                prior_checkpoint = dict(
                    runtime._latest_five_minute_direction_checkpoint or {},
                )

                tied_source = self._decision(
                    runtime,
                    PaperDirection.SHORT,
                    created_at="2026-09-01T20:55:00Z",
                    candle_close_utc="2026-09-01T20:55:00Z",
                    suffix="discontinuous-tie",
                )
                tie = replace(
                    tied_source,
                    decision=PaperDecisionKind.NO_TRADE,
                    direction=PaperDirection.FLAT,
                    family_summary={
                        **tied_source.family_summary,
                        "action": "BLOCKED",
                        "bias": "TIE",
                        "target_position": "FLAT",
                        # This mirrors the incident: the process-local scheduler
                        # reported no miss even though the recovered checkpoint
                        # did not end at this boundary's open.
                        "missed_boundary_count": 0,
                    },
                    reason_code="FIVE_MINUTE_BIAS_TIE_FLAT",
                )
                ledger.append(
                    "DECISION", tie.payload(), identity=tie.paper_decision_id,
                    occurred_at=tie.created_at,
                )
                before = len(ledger.recent_kind_records(
                    ("RISK_EVENT_FIVE_MINUTE_DIRECTION_CHECKPOINT",),
                ))

                self.assertFalse(
                    runtime._record_five_minute_direction_checkpoint_locked(tie),
                )
                self.assertEqual(
                    len(ledger.recent_kind_records(
                        ("RISK_EVENT_FIVE_MINUTE_DIRECTION_CHECKPOINT",),
                    )),
                    before,
                )
                self.assertIsNone(
                    runtime._latest_five_minute_direction_checkpoint,
                )
                self.assertFalse(runtime._perpetual_signal_ledger_verified)
                self.assertEqual(
                    runtime._perpetual_flat_blocker,
                    "FIVE_MINUTE_BOUNDARY_CONTINUITY_UNPROVEN",
                )
                self.assertIsNone(runtime._perpetual_signal_fault)
                self.assertFalse(runtime.risk.status()["locked_out"])
                self.assertIsNone(ledger.record_by_identity(
                    "l3g-five-minute-direction-" + canonical_hash({
                        "paper_policy_hash": runtime.policy.artifact.configuration_hash,
                        "candle_close_utc": "2026-09-01T20:55:00Z",
                    })[:32],
                ))

                recovered = self._decision(
                    runtime,
                    PaperDirection.LONG,
                    created_at="2026-09-01T20:55:30Z",
                    candle_close_utc="2026-09-01T20:55:30Z",
                    suffix="discontinuous-recovered",
                )
                self._commit_signal(runtime, recovered)
                checkpoint = dict(
                    runtime._latest_five_minute_direction_checkpoint or {},
                )
                self.assertEqual(checkpoint["direction"], "LONG")
                self.assertGreater(
                    int(checkpoint["ledger_sequence"]),
                    int(prior_checkpoint["ledger_sequence"]),
                )
            finally:
                ledger.close()

            reopened = PaperLedger(
                path,
                epoch_id="L3G-PAPER-EPOCH-TEST-PERPETUAL",
                policy=FIVE_MINUTE_PERPETUAL_PROFILE.policy,
                risk=FIVE_MINUTE_PERPETUAL_PROFILE.risk,
            )
            try:
                recovered_runtime = LaneIIIPaperRuntime(reopened)
                checkpoint = recovered_runtime._latest_five_minute_direction_checkpoint
                self.assertIsNotNone(checkpoint)
                self.assertEqual(checkpoint["direction"], "LONG")  # type: ignore[index]
                self.assertEqual(
                    checkpoint["candle_close_utc"],  # type: ignore[index]
                    "2026-09-01T20:55:30Z",
                )
                self.assertTrue(
                    recovered_runtime._perpetual_signal_requires_start_verification,
                )
                self.assertFalse(
                    recovered_runtime._perpetual_signal_ledger_verified,
                )
            finally:
                reopened.close()

    def test_flat_faulted_operational_stop_completes_once_and_preserves_lockout(self) -> None:
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=NOW,
        ):
            ledger, runtime, capture = self._runtime(directory)
            path = ledger.path
            try:
                self._ready(runtime)
                started = runtime.operational_paper_start(
                    "faulted-stop-regression",
                )
                self.assertTrue(started["started"])
                self.assertEqual(runtime.state, PaperRuntimeState.PAPER_RUNNING)

                runtime._fail_closed_without_ledger_locked(
                    "FIVE_MINUTE_SIGNAL_CHECKPOINT_DURABILITY_FAILED",
                )
                self.assertEqual(runtime.state, PaperRuntimeState.FAULTED)
                self.assertTrue(runtime.risk.status()["locked_out"])
                self.assertTrue(
                    runtime.status()["operational_paper_session"]["stopping"],  # type: ignore[index]
                )

                first = runtime.flatten_and_disarm()
                replay = runtime.flatten_and_disarm()

                self.assertTrue(first["flat_confirmed"])
                self.assertFalse(first["stopping"])
                self.assertTrue(replay["flat_confirmed"])
                self.assertEqual(runtime.state, PaperRuntimeState.FAULTED)
                self.assertIsNone(runtime.status()["operational_paper_session"])
                self.assertTrue(runtime._entries_paused)
                self.assertTrue(runtime.risk.status()["locked_out"])
                self.assertEqual(
                    runtime.status()["lockout_or_fault_reason"],
                    "FIVE_MINUTE_SIGNAL_CHECKPOINT_DURABILITY_FAILED",
                )
                self.assertEqual(capture.commands, [])
                stopped = ledger.recent_kind_records(
                    ("SESSION_OPERATIONAL_PAPER_STOPPED",),
                )
                self.assertEqual(len(stopped), 1)
                self.assertEqual(
                    stopped[0]["record"]["payload"]["reason"],  # type: ignore[index]
                    "FIVE_MINUTE_SIGNAL_CHECKPOINT_DURABILITY_FAILED",
                )
            finally:
                ledger.close()

            reopened = PaperLedger(
                path,
                epoch_id="L3G-PAPER-EPOCH-TEST-PERPETUAL",
                policy=FIVE_MINUTE_PERPETUAL_PROFILE.policy,
                risk=FIVE_MINUTE_PERPETUAL_PROFILE.risk,
            )
            try:
                recovered_runtime = LaneIIIPaperRuntime(reopened)
                self.assertIsNone(
                    recovered_runtime.status()["operational_paper_session"],
                )
                self.assertTrue(recovered_runtime.risk.status()["locked_out"])
                self.assertEqual(
                    recovered_runtime.status()["lockout_or_fault_reason"],
                    "FIVE_MINUTE_SIGNAL_CHECKPOINT_DURABILITY_FAILED",
                )
                self.assertEqual(
                    len(reopened.recent_kind_records(
                        ("SESSION_OPERATIONAL_PAPER_STOPPED",),
                    )),
                    1,
                )
            finally:
                reopened.close()

    def test_v2_start_calculates_just_completed_bias_from_retained_authentic_observations(self) -> None:
        """A fresh V2 derives the latest complete bar before it starts flat.

        The policy is warmed entirely by admitted pre-boundary market
        callbacks. Operational start itself evaluates 14:05 and consumes the
        resulting durable checkpoint rather than waiting for another callback.
        """
        start_at = "2026-09-01T14:05:00.500000Z"
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=start_at,
        ):
            ledger, runtime, capture = self._runtime(directory, at=start_at)
            try:
                runtime.policy.on_transport_state(StreamHealth.HEALTHY)

                def bullish_score(_at: str, hypothesis: object) -> tuple[Decimal, dict[str, object]]:
                    value = (
                        Decimal("0.60")
                        if hypothesis is HypothesisKind.BULLISH_REVERSAL
                        else Decimal("0.40")
                    )
                    return value, {
                        "positive_family_count": 1,
                        "blocking_contradiction": False,
                    }

                runtime.policy.score = bullish_score  # type: ignore[method-assign]
                _, last_directional_input = self._warm_bullish_market_evidence(
                    runtime,
                    first_sequence=1,
                    first_at="2026-09-01T14:04:57Z",
                    connect=True,
                )
                self.assertIsNone(runtime._latest_five_minute_direction_checkpoint)

                runtime._snapshot = self._healthy_snapshot(
                    start_at, runtime._session_context,
                )
                self._ready(runtime)
                result = runtime.operational_paper_start(
                    "perpetual-calculate-completed-startup-bar",
                )

                self.assertTrue(result["started"])
                self.assertEqual(runtime.state, PaperRuntimeState.ENTRY_PENDING)
                self.assertEqual(self._entry_actions(capture), [ExecutionAction.ENTER_LONG])
                checkpoint = runtime._latest_five_minute_direction_checkpoint
                self.assertIsNotNone(checkpoint)
                self.assertEqual(checkpoint["direction"], "LONG")  # type: ignore[index]
                self.assertEqual(
                    checkpoint["candle_close_utc"],  # type: ignore[index]
                    "2026-09-01T14:05:00Z",
                )
                source = checkpoint["source_decision"]  # type: ignore[index]
                self.assertTrue(
                    source["family_summary"]["startup_reconstruction"],  # type: ignore[index]
                )
                self.assertEqual(source["created_at"], start_at)  # type: ignore[index]
                self.assertTrue(all(
                    str(observation_id).startswith("startup-observation-")
                    for observation_id in source["source_observation_ids"]  # type: ignore[index]
                ))
                self.assertEqual(
                    source["family_summary"]["decision_reference_observation_id"],  # type: ignore[index]
                    last_directional_input.observation_id,
                )
                self.assertTrue(
                    source["family_summary"]["decision_reference_before_scheduled_boundary"]  # type: ignore[index]
                )
            finally:
                ledger.close()

        # The V1 profile retains its WAIT_FOR_NEXT_BOUNDARY startup behavior.
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=start_at,
        ):
            ledger, legacy, capture = self._runtime(
                directory, at=start_at, perpetual=False,
            )
            try:
                legacy.policy.on_transport_state(StreamHealth.HEALTHY)
                legacy.policy.score = bullish_score  # type: ignore[method-assign]
                self._warm_bullish_market_evidence(
                    legacy,
                    first_sequence=1,
                    first_at="2026-09-01T14:04:57Z",
                    connect=True,
                )
                legacy._snapshot = self._healthy_snapshot(
                    start_at, legacy._session_context,
                )
                self._ready(legacy)
                self.assertTrue(
                    legacy.operational_paper_start(
                        "legacy-waits-for-post-start-boundary-callback",
                    )["started"]
                )
                self.assertEqual(legacy.state, PaperRuntimeState.PAPER_RUNNING)
                self.assertEqual(capture.commands, [])
            finally:
                ledger.close()

    def test_v2_start_does_not_consume_latest_boundary_until_all_evidence_families_are_authentic(self) -> None:
        start_at = "2026-09-01T14:05:00.500000Z"
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=start_at,
        ):
            ledger, runtime, capture = self._runtime(directory, at=start_at)
            try:
                runtime.policy.on_transport_state(StreamHealth.HEALTHY)
                runtime.policy.score = lambda _at, hypothesis: (  # type: ignore[method-assign]
                    (
                        Decimal("0.60")
                        if hypothesis is HypothesisKind.BULLISH_REVERSAL
                        else Decimal("0.40")
                    ),
                    {"positive_family_count": 1, "blocking_contradiction": False},
                )
                runtime.ingest(self._connection(1, "2026-09-01T14:04:58Z"))
                runtime.ingest(self._quote(2, "2026-09-01T14:04:59Z"))
                runtime._snapshot = self._healthy_snapshot(
                    start_at, runtime._session_context,
                )
                self._ready(runtime)

                result = runtime.operational_paper_start(
                    "perpetual-incomplete-startup-evidence",
                )

                self.assertTrue(result["started"])
                self.assertEqual(runtime.state, PaperRuntimeState.PAPER_RUNNING)
                self.assertIsNone(runtime._latest_five_minute_direction_checkpoint)
                self.assertIsNone(runtime.policy.status()["five_minute_schedule"]["last_boundary"])
                self.assertEqual(capture.commands, [])
                runtime.ingest(self._quote(
                    3, "2026-09-01T14:05:00.600000Z", "100",
                ))
                self.assertIsNone(runtime._latest_five_minute_direction_checkpoint)
                self.assertIsNone(runtime.policy.status()["five_minute_schedule"]["last_boundary"])
                self.assertEqual(capture.commands, [])
                self.assertEqual(
                    runtime.status()["position_requirement"]["state"],  # type: ignore[index]
                    "BLOCKED_FLAT",
                )
            finally:
                ledger.close()

    def test_v2_start_evaluates_latest_tie_but_uses_prior_non_tied_checkpoint(self) -> None:
        start_at = "2026-09-01T14:05:30.500000Z"
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=start_at,
        ):
            ledger, runtime, capture = self._runtime(directory, at=start_at)
            try:
                runtime.policy.on_transport_state(StreamHealth.HEALTHY)

                def scheduled_score(at: str, hypothesis: object) -> tuple[Decimal, dict[str, object]]:
                    tied = datetime.fromisoformat(at.replace("Z", "+00:00")) >= datetime(
                        2026, 9, 1, 14, 5, 30, tzinfo=timezone.utc,
                    )
                    value = (
                        Decimal("0.50")
                        if tied
                        else Decimal("0.62")
                        if hypothesis is HypothesisKind.BULLISH_REVERSAL
                        else Decimal("0.38")
                    )
                    return value, {
                        "positive_family_count": 1,
                        "blocking_contradiction": False,
                    }

                runtime.policy.score = scheduled_score  # type: ignore[method-assign]
                next_sequence, _ = self._warm_bullish_market_evidence(
                    runtime,
                    first_sequence=1,
                    first_at="2026-09-01T14:04:57Z",
                    connect=True,
                )
                runtime.ingest(self._quote(
                    next_sequence, "2026-09-01T14:05:00.100000Z", "101",
                ))
                next_sequence += 1
                first_checkpoint = dict(
                    runtime._latest_five_minute_direction_checkpoint or {},
                )
                self.assertEqual(first_checkpoint.get("direction"), "LONG")
                self.assertEqual(
                    first_checkpoint.get("candle_close_utc"),
                    "2026-09-01T14:05:00Z",
                )
                self._warm_bullish_market_evidence(
                    runtime,
                    first_sequence=next_sequence,
                    first_at="2026-09-01T14:05:27Z",
                    connect=False,
                )
                self.assertEqual(
                    runtime._latest_five_minute_direction_checkpoint,
                    first_checkpoint,
                )
                runtime._snapshot = self._healthy_snapshot(
                    start_at, runtime._session_context,
                )
                self._ready(runtime)

                result = runtime.operational_paper_start(
                    "perpetual-tied-latest-completed-bar",
                )
                self.assertTrue(result["started"])
                self.assertEqual(self._entry_actions(capture), [ExecutionAction.ENTER_LONG])
                checkpoint = runtime._latest_five_minute_direction_checkpoint
                self.assertIsNotNone(checkpoint)
                self.assertEqual(
                    checkpoint["direction"], PaperDirection.LONG.value,  # type: ignore[index]
                )
                self.assertEqual(
                    checkpoint["candle_close_utc"],  # type: ignore[index]
                    "2026-09-01T14:05:30Z",
                )
                self.assertEqual(checkpoint["boundary_bias"], "TIE")  # type: ignore[index]
                self.assertEqual(
                    checkpoint["prior_signal_hash"],  # type: ignore[index]
                    first_checkpoint["signal_hash"],
                )
                decisions = [
                    record["payload"]
                    for record in ledger.recent(200)
                    if record["kind"] == "DECISION"
                ]
                tied = [
                    payload for payload in decisions
                    if payload.get("reason_code") == "FIVE_MINUTE_BIAS_TIE_FLAT"
                    and payload.get("family_summary", {}).get("candle_close_utc")
                    == "2026-09-01T14:05:30Z"
                ]
                self.assertEqual(len(tied), 1)
            finally:
                ledger.close()

    def test_live_tied_boundary_is_checkpointed_and_recovers_across_cold_restart(self) -> None:
        """A live tie must durably advance continuity while retaining direction."""
        boundary_at = "2026-09-01T14:05:00.500000Z"
        path: Path
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=boundary_at,
        ):
            path = Path(directory) / "perpetual.sqlite3"
            ledger, runtime, _ = self._runtime(directory, at=boundary_at)
            try:
                prior = self._decision(
                    runtime,
                    PaperDirection.LONG,
                    created_at="2026-09-01T14:04:30Z",
                    candle_close_utc="2026-09-01T14:04:30Z",
                    suffix="cold-restart-tie-root",
                )
                self._commit_signal(runtime, prior)
                prior_checkpoint = dict(
                    runtime._latest_five_minute_direction_checkpoint or {},
                )
                runtime.policy.on_transport_state(StreamHealth.HEALTHY)
                runtime.policy.score = lambda _at, _hypothesis: (  # type: ignore[method-assign]
                    Decimal("0.50"),
                    {
                        "positive_family_count": 1,
                        "blocking_contradiction": False,
                    },
                )
                next_sequence, _ = self._warm_bullish_market_evidence(
                    runtime,
                    first_sequence=1,
                    first_at="2026-09-01T14:04:57Z",
                    connect=True,
                )

                # This is an ordinary live callback boundary, not the explicit
                # startup reconstruction path exercised by the neighboring test.
                runtime.ingest(self._quote(
                    next_sequence,
                    "2026-09-01T14:05:00.100000Z",
                    "101",
                ))
                checkpoint = runtime._latest_five_minute_direction_checkpoint
                self.assertIsNotNone(checkpoint)
                self.assertEqual(checkpoint["boundary_bias"], "TIE")  # type: ignore[index]
                self.assertEqual(checkpoint["direction"], "LONG")  # type: ignore[index]
                self.assertEqual(  # type: ignore[index]
                    checkpoint["candle_close_utc"],
                    "2026-09-01T14:05:00Z",
                )
                self.assertEqual(  # type: ignore[index]
                    checkpoint["prior_signal_hash"],
                    prior_checkpoint["signal_hash"],
                )
            finally:
                ledger.close()

            reopened = PaperLedger(
                path,
                policy=FIVE_MINUTE_PERPETUAL_POLICY,
                risk=FIVE_MINUTE_PERPETUAL_RISK_PROFILE,
            )
            recovered = LaneIIIPaperRuntime(reopened)
            try:
                checkpoint = recovered._latest_five_minute_direction_checkpoint
                self.assertIsNotNone(checkpoint)
                self.assertEqual(checkpoint["boundary_bias"], "TIE")  # type: ignore[index]
                self.assertEqual(checkpoint["direction"], "LONG")  # type: ignore[index]
                self.assertEqual(  # type: ignore[index]
                    checkpoint["candle_close_utc"],
                    "2026-09-01T14:05:00Z",
                )
                chain = recovered._perpetual_direction_chain_locked(checkpoint)  # type: ignore[arg-type]
                self.assertEqual(
                    [item["boundary_bias"] for item in chain],
                    ["LONG", "TIE"],
                )
            finally:
                reopened.close()

    def test_v1_passive_shadow_exports_full_verified_seed_for_fresh_v2(self) -> None:
        switch_at = "2026-09-01T14:05:00.500000Z"
        operation_id = "profile-switch-" + "6" * 32
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=switch_at,
        ):
            source_ledger, source_runtime, source_commands = self._runtime(
                directory, at=switch_at, perpetual=False,
            )
            source_path = source_ledger.path
            target_ledger: PaperLedger | None = None
            try:
                source_runtime.on_observation_transport_state(StreamHealth.HEALTHY)
                next_sequence, _ = self._warm_bullish_market_evidence(
                    source_runtime,
                    first_sequence=1,
                    first_at="2026-09-01T14:04:57Z",
                    connect=True,
                )
                source_runtime.ingest(self._quote(
                    next_sequence,
                    "2026-09-01T14:05:00.100000Z",
                    "101",
                ))
                shadow = source_runtime.status()["perpetual_startup_seed"]
                self.assertEqual(
                    shadow["latest_completed_boundary"],  # type: ignore[index]
                    "2026-09-01T14:05:00Z",
                )
                exported_direction = shadow["latest_completed_bias"]  # type: ignore[index]
                self.assertIn(exported_direction, {"LONG", "SHORT"})
                self.assertEqual(source_commands.commands, [])

                artifact_path = Path(directory) / "seed.json"
                artifact = source_runtime.export_perpetual_startup_seed(
                    operation_id, artifact_path,
                )
                exports = source_ledger.recent_kind_records(
                    (PERPETUAL_STARTUP_SEED_EXPORT_KIND,), limit=10,
                )
                self.assertEqual(len(exports), 1)
                self.assertEqual(
                    exports[0]["ledger_sequence"],
                    artifact["export_record"]["ledger_sequence"],  # type: ignore[index]
                )

                shutdown = {
                    **source_ledger.close(),
                    "verifier_shutdown": {"completed": True},
                    "runtime_watchdog_shutdown": {"completed": True},
                }
                audit_root = Path(directory) / "source-audit"
                verification = LocalLedgerVerifier(
                    source_path, audit_root, requested_mode="full",
                ).run()
                self.assertEqual(verification["status"], "PASS")
                proof = write_perpetual_startup_seed_proof(
                    Path(directory) / "proof.json",
                    artifact=artifact,
                    manifest_sha256="4" * 64,
                    shutdown_receipt=shutdown,
                    verification_report=verification,
                )

                target_ledger = PaperLedger(
                    Path(directory) / "target.sqlite3",
                    epoch_id="L3G-PAPER-EPOCH-PERPETUAL-SEED-TARGET",
                    policy=FIVE_MINUTE_PERPETUAL_POLICY,
                    risk=FIVE_MINUTE_PERPETUAL_RISK_PROFILE,
                )
                target_runtime = LaneIIIPaperRuntime(target_ledger)
                imported = target_runtime.import_perpetual_startup_seed(
                    artifact,
                    proof,
                    operation_id=operation_id,
                    manifest_sha256="4" * 64,
                    expected_at=switch_at,
                )
                self.assertEqual(imported["artifact_sha256"], artifact["artifact_sha256"])
                checkpoint = target_runtime._latest_five_minute_direction_checkpoint
                self.assertIsNotNone(checkpoint)
                self.assertEqual(checkpoint["direction"], exported_direction)  # type: ignore[index]
                self.assertEqual(
                    checkpoint["candle_close_utc"],  # type: ignore[index]
                    "2026-09-01T14:05:00Z",
                )
                self.assertTrue(target_runtime._perpetual_signal_ledger_verified)
                self.assertEqual(
                    (
                        target_runtime._snapshot.session_kind,
                        target_runtime._snapshot.session_id,
                        target_runtime._snapshot.trade_date,
                        target_runtime._snapshot.session_profile_hash,
                        target_runtime._snapshot.session_generation,
                    ),
                    (
                        target_runtime._session_context.session_kind,
                        target_runtime._session_context.session_id,
                        target_runtime._session_context.trade_date,
                        target_runtime._session_context.session_profile_hash,
                        target_runtime._session_context.session_generation,
                    ),
                )
            finally:
                if target_ledger is not None:
                    target_ledger.close()
                if source_ledger.shutdown_status() is None:
                    source_ledger.close()

    def test_v1_shadow_exports_authentic_dotnet_roundtrip_timestamps(self) -> None:
        switch_at = "2026-09-07T11:45:00.500000Z"
        operation_id = "profile-switch-" + "7" * 32
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=switch_at,
        ):
            ledger, runtime, commands = self._runtime(
                directory, at=switch_at, perpetual=False,
            )
            try:
                runtime.on_observation_transport_state(StreamHealth.HEALTHY)
                sequence = 1
                moment = datetime(2026, 9, 7, 11, 44, 57, tzinfo=timezone.utc)

                def next_at() -> str:
                    nonlocal moment
                    value = _dotnet_roundtrip_utc(moment)
                    moment += timedelta(milliseconds=50)
                    return value

                runtime.ingest(self._connection(sequence, next_at()))
                sequence += 1
                last_trade: NinjaTraderObservation | None = None
                for price in ("100", "99", "100", "100", "99", "100", "99", "100"):
                    quote = self._quote(sequence, next_at(), price)
                    runtime.ingest(quote)
                    sequence += 1
                    last_trade = self._trade(sequence, next_at(), quote, price)
                    runtime.ingest(last_trade)
                    sequence += 1
                for operation, volume in (("ADD", 10), ("UPDATE", 5)):
                    runtime.ingest(self._depth(sequence, next_at(), operation, volume))
                    sequence += 1
                assert last_trade is not None
                self.assertEqual(
                    last_trade.ninja_receipt_time,
                    "2026-09-07T11:44:57.8000007Z",
                )

                runtime.ingest(self._quote(
                    sequence, "2026-09-07T11:45:00.1000007Z", "101",
                ))
                status = runtime.status()["perpetual_startup_seed"]
                self.assertIsNone(status["source_shadow_fault"])
                self.assertEqual(status["latest_completed_boundary"], "2026-09-07T11:45:00Z")
                bundle = runtime._perpetual_seed_latest_bundle
                self.assertIsNotNone(bundle)
                summary = bundle["decision"]["family_summary"]  # type: ignore[index]
                self.assertEqual(
                    summary["decision_reference_observed_at"],
                    "2026-09-07T11:44:57.800000Z",
                )

                artifact = runtime.export_perpetual_startup_seed(
                    operation_id, Path(directory) / "dotnet-roundtrip-seed.json",
                )
                proofs = artifact["core"]["observations"]  # type: ignore[index]
                reference = next(
                    item for item in proofs
                    if item["wire"]["observation_id"] == last_trade.observation_id
                )
                self.assertEqual(
                    reference["wire"]["ninja_receipt_time"],
                    "2026-09-07T11:44:57.800000Z",
                )
                self.assertEqual(
                    reference["wire"]["provider_timestamp"],
                    "2026-09-07T11:44:57.800000Z",
                )
                self.assertEqual(commands.commands, [])
            finally:
                ledger.close()

    def test_seed_normalizes_exchange_timestamp_without_mutating_observation(self) -> None:
        at = "2026-09-01T14:04:58Z"
        raw = "2026-09-01T14:04:57.1234567Z"
        normalized = "2026-09-01T14:04:57.123456Z"
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=at,
        ):
            ledger, runtime, _ = self._runtime(
                directory, at=at, perpetual=False,
            )
            try:
                runtime.on_observation_transport_state(StreamHealth.HEALTHY)
                observation = replace(
                    self._quote(1, raw), exchange_timestamp=raw,
                )
                runtime.ingest(observation)

                self.assertEqual(observation.ninja_receipt_time, raw)
                self.assertEqual(observation.provider_timestamp, raw)
                self.assertEqual(observation.exchange_timestamp, raw)
                retained = runtime._perpetual_seed_observations[
                    observation.observation_id
                ]
                self.assertIs(retained, observation)
                envelope = runtime._perpetual_seed_source_envelopes[
                    observation.observation_id
                ]
                wire = runtime._perpetual_seed_wire(observation)
                proof = build_perpetual_observation_proof(
                    wire=wire, source_envelope=envelope,
                )
                for field in (
                    "ninja_receipt_time", "provider_timestamp",
                    "exchange_timestamp",
                ):
                    self.assertEqual(wire[field], normalized)
                    self.assertEqual(envelope[field], normalized)
                    self.assertEqual(proof["wire"][field], normalized)  # type: ignore[index]
            finally:
                ledger.close()

    def test_same_bias_keeps_one_position_without_another_entry_or_exit(self) -> None:
        clock = {"at": NOW}
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", side_effect=lambda: clock["at"],
        ):
            ledger, runtime, capture = self._runtime(directory)
            try:
                self._start_and_fill_long(
                    runtime,
                    capture,
                    request_id="perpetual-hold-long",
                    suffix="same-bias",
                )
                capture.commands.clear()

                clock["at"] = "2026-09-01T20:54:30Z"
                held = self._decision(
                    runtime,
                    PaperDirection.LONG,
                    created_at="2026-09-01T20:54:30Z",
                    candle_close_utc="2026-09-01T20:54:30Z",
                    shape="HOLD",
                    suffix="held-long",
                )
                self._commit_signal(runtime, held)
                runtime._maintain_perpetual_position_locked("SAME_BIAS")

                self.assertEqual(runtime.state, PaperRuntimeState.LONG)
                self.assertEqual(runtime._position, PaperDirection.LONG)
                self.assertEqual(runtime._position_quantity, 1)
                self.assertEqual(capture.commands, [])
                self.assertEqual(
                    runtime.status()["position_requirement"]["desired_position"],  # type: ignore[index]
                    "LONG",
                )
            finally:
                ledger.close()

    def test_opposite_bias_exits_reconciles_flat_then_enters_opposite_once(self) -> None:
        clock = {"at": NOW}
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", side_effect=lambda: clock["at"],
        ):
            ledger, runtime, capture = self._runtime(directory)
            try:
                self._start_and_fill_long(
                    runtime,
                    capture,
                    request_id="perpetual-reversal",
                    suffix="reversal",
                )

                clock["at"] = "2026-09-01T20:54:30Z"
                runtime._snapshot = replace(
                    runtime._snapshot,
                    quote_observed_at=clock["at"],
                    classified_trade_observed_at=clock["at"],
                    depth_mutation_observed_at=clock["at"],
                )
                reversal = self._decision(
                    runtime,
                    PaperDirection.SHORT,
                    created_at="2026-09-01T20:54:30Z",
                    candle_close_utc="2026-09-01T20:54:30Z",
                    shape="REVERSE",
                    suffix="reverse-short",
                )
                self._commit_signal(runtime, reversal)
                runtime._last_decision = reversal
                runtime._pending_five_minute_reversal = reversal
                self.assertTrue(runtime._request_exit("FIVE_MINUTE_REVERSE_TO_SHORT"))
                self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
                self.assertEqual(self._entry_actions(capture), [ExecutionAction.ENTER_LONG])

                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT",
                    "order_role": "EXIT",
                    "direction": "FLAT",
                    "price": "100.00",
                    "quantity": 1,
                    "native_execution_id": "reversal-exit-execution",
                    "native_order_id": "reversal-exit-order",
                    "account_name": "Sim101",
                    "instrument": "MNQ SEP26",
                    "timestamp": clock["at"],
                })
                runtime.on_execution_message({
                    "message_type": "POSITION_EVENT",
                    "quantity": 0,
                    "timestamp": clock["at"],
                })
                self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
                self.assertEqual(
                    [command.action for command in capture.commands].count(  # type: ignore[attr-defined]
                        ExecutionAction.RECONCILE,
                    ),
                    0,
                )
                runtime.on_execution_message(self._filled_order(
                    "EXIT", "reversal-exit-order",
                    command_id=runtime._pending_exit_command_id,
                ))
                self.assertEqual(runtime.state, PaperRuntimeState.RECONCILING)
                # No opposite entry may exist before the independently signed
                # flat/no-order snapshot is processed.
                self.assertEqual(self._entry_actions(capture), [ExecutionAction.ENTER_LONG])

                runtime.on_execution_message({
                    "message_type": "RECONCILIATION",
                    "receipt_id": "reversal-flat-reconciliation",
                    "account_name": "Sim101",
                    "account_class": "LOCAL_SIMULATION",
                    "instrument": "MNQ SEP26",
                    "position_quantity": 0,
                    "working_order_count": 0,
                    "working_entry_count": 0,
                    "position_snapshot_complete": True,
                    "order_snapshot_complete": True,
                    "foreign_activity": False,
                    "protective_stop_state": "NONE",
                    "timestamp": clock["at"],
                })
                self.assertEqual(runtime.state, PaperRuntimeState.ENTRY_PENDING)
                self.assertEqual(
                    self._entry_actions(capture),
                    [ExecutionAction.ENTER_LONG, ExecutionAction.ENTER_SHORT],
                )
                actions = [command.action for command in capture.commands]  # type: ignore[attr-defined]
                self.assertEqual(actions.count(ExecutionAction.EXIT), 1)
                self.assertEqual(actions.count(ExecutionAction.RECONCILE), 1)
                self.assertEqual(actions.count(ExecutionAction.ENTER_SHORT), 1)
                runtime._maintain_perpetual_position_locked("DUPLICATE_FLAT_CALLBACK")
                self.assertEqual(
                    [command.action for command in capture.commands].count(  # type: ignore[attr-defined]
                        ExecutionAction.ENTER_SHORT,
                    ),
                    1,
                )
            finally:
                ledger.close()

    def test_exit_callback_permutations_require_all_three_settlement_facts(self) -> None:
        for callback_order in permutations(("execution", "position", "order")):
            with self.subTest(callback_order=callback_order), TemporaryDirectory() as directory, patch(
                "src.l3g_paper.runtime._now", return_value=NOW,
            ):
                ledger, runtime, capture = self._runtime(directory)
                try:
                    suffix = "-".join(callback_order)
                    self._start_and_fill_long(
                        runtime,
                        capture,
                        request_id="callback-permutation-" + suffix,
                        suffix="callback-permutation-" + suffix,
                    )
                    self.assertTrue(runtime._request_exit("FIVE_MINUTE_REVERSE_TO_SHORT"))
                    command_id = runtime._pending_exit_command_id
                    events = {
                        "execution": {
                            "message_type": "EXECUTION_EVENT",
                            "order_role": "EXIT",
                            "direction": "FLAT",
                            "price": "100.00",
                            "quantity": 1,
                            "native_execution_id": "exit-execution-" + suffix,
                            "native_order_id": "exit-order-" + suffix,
                            "command_id": command_id,
                            "account_name": "Sim101",
                            "instrument": "MNQ SEP26",
                            "timestamp": NOW,
                        },
                        "position": {
                            "message_type": "POSITION_EVENT",
                            "quantity": 0,
                            "timestamp": NOW,
                        },
                        "order": self._filled_order(
                            "EXIT", "exit-order-" + suffix,
                            command_id=command_id,
                        ),
                    }
                    for index, event_name in enumerate(callback_order):
                        runtime.on_execution_message(events[event_name])
                        reconcile_count = [
                            command.action for command in capture.commands  # type: ignore[attr-defined]
                        ].count(ExecutionAction.RECONCILE)
                        self.assertEqual(reconcile_count, 1 if index == 2 else 0)
                    self.assertEqual(runtime.state, PaperRuntimeState.RECONCILING)
                finally:
                    ledger.close()

    def test_early_submitted_protective_waits_for_acceptance_then_reconciles_once(self) -> None:
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=NOW,
        ):
            ledger, runtime, capture = self._runtime(directory)
            try:
                suffix = "submitted-before-entry-fill"
                signal = self._decision(
                    runtime,
                    PaperDirection.LONG,
                    created_at=NOW,
                    candle_close_utc=NOW,
                    suffix=suffix,
                )
                self._commit_signal(runtime, signal)
                self._ready(runtime)
                self.assertTrue(
                    runtime.operational_paper_start(
                        "early-protective-submitted-regression",
                    )["started"],
                )
                self.assertEqual(runtime.state, PaperRuntimeState.ENTRY_PENDING)
                entry_command = capture.commands[0]
                protective = {
                    "message_type": "ORDER_EVENT",
                    "order_role": "PROTECTIVE",
                    "order_state": "SUBMITTED",
                    "native_order_id": suffix + "-protective-order",
                    "command_id": entry_command.command_id,  # type: ignore[attr-defined]
                    "account_name": "Sim101",
                    "instrument": "MNQ SEP26",
                    "quantity": 1,
                    "timestamp": NOW,
                }

                # The native stop exists, but SUBMITTED is not protection.
                # Preserve the owned callback without competing with the
                # AddOn's independent acceptance watchdog.
                runtime.on_execution_message(protective)
                self.assertEqual(runtime.state, PaperRuntimeState.ENTRY_PENDING)
                self.assertIsNotNone(runtime._early_protective_order_event)
                self.assertEqual(
                    runtime.status()["protective_stop_state"], "SUBMITTED",
                )
                self.assertEqual(
                    [command.action for command in capture.commands],  # type: ignore[attr-defined]
                    [ExecutionAction.ENTER_LONG],
                )

                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT",
                    "order_role": "ENTRY",
                    "direction": "LONG",
                    "price": "100.25",
                    "quantity": 1,
                    "native_execution_id": suffix + "-entry-execution",
                    "native_order_id": suffix + "-entry-order",
                    "account_name": "Sim101",
                    "instrument": "MNQ SEP26",
                    "timestamp": NOW,
                })
                self.assertEqual(runtime.state, PaperRuntimeState.LONG)
                self.assertIsNone(runtime._early_protective_order_event)
                self.assertFalse(runtime.risk.status()["locked_out"])
                actions = [command.action for command in capture.commands]  # type: ignore[attr-defined]
                self.assertEqual(actions.count(ExecutionAction.RECONCILE), 0)
                self.assertEqual(actions.count(ExecutionAction.EMERGENCY_FLATTEN), 0)

                protective["order_state"] = "ACCEPTED"
                runtime.on_execution_message(protective)
                runtime.on_execution_message(protective)
                actions = [command.action for command in capture.commands]  # type: ignore[attr-defined]
                self.assertEqual(actions.count(ExecutionAction.RECONCILE), 1)
                self.assertEqual(actions.count(ExecutionAction.EMERGENCY_FLATTEN), 0)

                runtime.on_execution_message({
                    "message_type": "RECONCILIATION",
                    "receipt_id": suffix + "-positioned-reconciliation",
                    "account_name": "Sim101",
                    "account_class": "LOCAL_SIMULATION",
                    "instrument": "MNQ SEP26",
                    "position_quantity": 1,
                    "working_order_count": 1,
                    "working_entry_count": 0,
                    "position_snapshot_complete": True,
                    "order_snapshot_complete": True,
                    "foreign_activity": False,
                    "protective_stop_state": "ACCEPTED",
                    "timestamp": NOW,
                })
                status = runtime.status()
                self.assertEqual(runtime.state, PaperRuntimeState.LONG)
                self.assertFalse(runtime.risk.status()["locked_out"])
                self.assertEqual(
                    status["position_requirement"]["blocking_reasons"],  # type: ignore[index]
                    [],
                )
                self.assertEqual(
                    len(ledger.recent_kind_records(
                        ("RISK_EVENT_POSITIONED_RECONCILIATION",),
                    )),
                    1,
                )
            finally:
                ledger.close()

    def test_protective_fill_reconciles_and_waits_for_a_later_boundary(self) -> None:
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=NOW,
        ):
            ledger, runtime, capture = self._runtime(directory)
            try:
                self._start_and_fill_long(
                    runtime,
                    capture,
                    request_id="perpetual-protective-fill-reentry",
                    suffix="protective-reentry",
                )
                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT",
                    "order_role": "PROTECTIVE",
                    "direction": "FLAT",
                    "price": "99.75",
                    "quantity": 1,
                    "native_execution_id": "protective-reentry-exit-execution",
                    "native_order_id": "protective-reentry-protective-order",
                    "account_name": "Sim101",
                    "instrument": "MNQ SEP26",
                    "timestamp": NOW,
                })
                self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
                self.assertFalse(runtime._entries_paused)
                self.assertFalse(runtime._retain_safety_lockout_after_flat)
                self.assertFalse(runtime.risk.status()["locked_out"])
                self.assertEqual(self._entry_actions(capture), [ExecutionAction.ENTER_LONG])
                # The protective order already supplied the physical exit;
                # the runtime must not send a second exit command.
                self.assertEqual(
                    [command.action for command in capture.commands].count(  # type: ignore[attr-defined]
                        ExecutionAction.EXIT,
                    ),
                    0,
                )

                runtime.on_execution_message({
                    "message_type": "POSITION_EVENT",
                    "quantity": 0,
                    "timestamp": NOW,
                })
                runtime.on_execution_message(self._filled_order(
                    "PROTECTIVE", "protective-reentry-protective-order",
                ))
                self.assertEqual(runtime.state, PaperRuntimeState.RECONCILING)
                actions = [command.action for command in capture.commands]  # type: ignore[attr-defined]
                self.assertEqual(actions.count(ExecutionAction.RECONCILE), 1)
                self.assertEqual(self._entry_actions(capture), [ExecutionAction.ENTER_LONG])

                # This callback models a frame which already passed the
                # authenticated transport's schema/HMAC/session checks.
                reconciliation = self._flat_reconciliation(
                    "protective-reentry-flat", NOW,
                )
                runtime.on_execution_message(reconciliation)
                self.assertEqual(runtime.state, PaperRuntimeState.PAPER_RUNNING)
                self.assertIsNotNone(runtime.status()["operational_paper_session"])
                self.assertEqual(self._entry_actions(capture), [ExecutionAction.ENTER_LONG])

                # Replayed maintenance and the stopped position's still-valid
                # checkpoint cannot duplicate an entry.
                runtime._maintain_perpetual_position_locked("DUPLICATE_PROTECTIVE_SETTLEMENT")
                self.assertEqual(self._entry_actions(capture), [ExecutionAction.ENTER_LONG])

                later = "2026-09-01T20:54:30Z"
                next_signal = self._decision(
                    runtime,
                    PaperDirection.LONG,
                    created_at=later,
                    candle_close_utc=later,
                    suffix="protective-reentry-later-boundary",
                )
                with patch("src.l3g_paper.runtime._now", return_value=later):
                    runtime._snapshot = self._healthy_snapshot(
                        later, runtime._session_context,
                    )
                    self._commit_signal(runtime, next_signal)
                    runtime._maintain_perpetual_position_locked("LATER_BOUNDARY")
                self.assertEqual(
                    self._entry_actions(capture),
                    [ExecutionAction.ENTER_LONG, ExecutionAction.ENTER_LONG],
                )
            finally:
                ledger.close()

    def test_protective_fill_health_recovery_cannot_reuse_stopped_checkpoint(self) -> None:
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=NOW,
        ):
            ledger, runtime, capture = self._runtime(directory)
            try:
                self._start_and_fill_long(
                    runtime,
                    capture,
                    request_id="perpetual-protective-health-fence",
                    suffix="protective-health",
                )
                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT",
                    "order_role": "PROTECTIVE",
                    "direction": "FLAT",
                    "price": "99.75",
                    "quantity": 1,
                    "native_execution_id": "protective-health-exit-execution",
                    "native_order_id": "protective-health-protective-order",
                    "account_name": "Sim101",
                    "instrument": "MNQ SEP26",
                    "timestamp": NOW,
                })
                runtime.on_execution_message({
                    "message_type": "POSITION_EVENT",
                    "quantity": 0,
                    "timestamp": NOW,
                })
                runtime.on_execution_message(self._filled_order(
                    "PROTECTIVE", "protective-health-protective-order",
                ))
                self.assertEqual(runtime.state, PaperRuntimeState.RECONCILING)
                runtime._snapshot = replace(
                    runtime._snapshot,
                    local_bridge_healthy=False,
                    market_price_connected=False,
                    evidence_warmed=False,
                )
                runtime.on_execution_message(
                    self._flat_reconciliation("protective-health-flat", NOW),
                )

                self.assertEqual(runtime.state, PaperRuntimeState.PAPER_RUNNING)
                self.assertEqual(self._entry_actions(capture), [ExecutionAction.ENTER_LONG])
                self.assertFalse(runtime.risk.status()["locked_out"])

                runtime._snapshot = self._healthy_snapshot(
                    NOW, runtime._session_context,
                )
                runtime._maintain_perpetual_position_locked("HEALTH_RECOVERED")
                runtime._maintain_perpetual_position_locked("HEALTH_RECOVERED_REPLAY")
                self.assertEqual(
                    self._entry_actions(capture),
                    [ExecutionAction.ENTER_LONG],
                )
            finally:
                ledger.close()

    def test_historical_stop_fill_lockout_has_one_scoped_durable_recovery(self) -> None:
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=NOW,
        ):
            ledger, runtime, capture = self._runtime(directory)
            path = ledger.path
            try:
                self._start_and_fill_long(
                    runtime,
                    capture,
                    request_id="perpetual-historical-protective-lockout",
                    suffix="historical-protective",
                )
                ledger.append(
                    "EXECUTION",
                    {
                        "message_type": "EXECUTION_EVENT",
                        "order_role": "ENTRY",
                        "direction": "LONG",
                        "price": "100.25",
                        "quantity": 1,
                        "native_execution_id": "historical-protective-entry-execution",
                        "native_order_id": "historical-protective-entry-order",
                        "account_name": "Sim101",
                        "instrument": "MNQ SEP26",
                        "timestamp": NOW,
                    },
                    identity="historical-protective-entry-transport-receipt",
                )
                original_apply = runtime._apply_execution

                def apply_with_retired_lockout(
                    message: object,
                    *,
                    durable_receipt_unavailable: bool = False,
                ) -> None:
                    if isinstance(message, dict) and message.get("order_role") == "PROTECTIVE":
                        ledger.append(
                            "EXECUTION",
                            message,
                            identity="historical-protective-exit-transport-receipt",
                        )
                        runtime._retain_safety_lockout_after_flat = True
                        runtime._fault_reason = "PROTECTIVE_STOP_FILLED"
                        runtime._risk_continuity_fault = "PROTECTIVE_STOP_FILLED"
                        runtime._entries_paused = True
                        runtime.risk.lock_out("PROTECTIVE_STOP_FILLED")
                        runtime._request_operational_stop_locked("PROTECTIVE_STOP_FILLED")
                    original_apply(
                        message,  # type: ignore[arg-type]
                        durable_receipt_unavailable=durable_receipt_unavailable,
                    )

                runtime._apply_execution = apply_with_retired_lockout  # type: ignore[method-assign]
                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT",
                    "order_role": "PROTECTIVE",
                    "direction": "FLAT",
                    "price": "99.75",
                    "quantity": 1,
                    "native_execution_id": "historical-protective-exit-execution",
                    "native_order_id": "historical-protective-protective-order",
                    "account_name": "Sim101",
                    "instrument": "MNQ SEP26",
                    "timestamp": NOW,
                })
                runtime.on_execution_message({
                    "message_type": "POSITION_EVENT",
                    "quantity": 0,
                    "timestamp": NOW,
                })
                runtime.on_execution_message(self._filled_order(
                    "PROTECTIVE", "historical-protective-protective-order",
                ))
                runtime.on_execution_message(
                    self._flat_reconciliation("historical-protective-flat", NOW),
                )
                self.assertEqual(runtime.state, PaperRuntimeState.LOCKED_OUT)
                before = runtime.risk_continuity_snapshot()
                def recovery_readiness(_preflight: object = None) -> dict[str, object]:
                    tip = int(ledger.health_status()["highest_sequence"])
                    locked = bool(runtime.risk.status()["locked_out"])
                    return {
                        "result": "BLOCKED" if locked else "READY",
                        "blocking_reasons": ["PROTECTIVE_STOP_FILLED"] if locked else [],
                        "deferred_entry_reasons": [],
                        "ledger": {
                            "ledger_trust_state": "VERIFIED_TO_ARM_SNAPSHOT_TIP",
                            "verified_through_sequence": tip,
                            "arm_snapshot_tip": tip,
                            "unverified_tail_rows": 0,
                        },
                    }

                runtime.operational_paper_readiness = recovery_readiness  # type: ignore[method-assign]
                started = runtime.operational_paper_start(
                    "historical-protective-recovery",
                )
                self.assertTrue(started["started"], started)
                self.assertEqual(runtime.state, PaperRuntimeState.PAPER_RUNNING)
                self.assertFalse(runtime.risk.status()["locked_out"])
                self.assertEqual(
                    runtime.risk_continuity_snapshot()["trade_dates"],
                    before["trade_dates"],
                )
                self.assertEqual(
                    runtime.risk_continuity_snapshot()["profile_trade_dates"],
                    before["profile_trade_dates"],
                )
                self.assertEqual(self._entry_actions(capture), [ExecutionAction.ENTER_LONG])
                clear = ledger.recent_kind_records(
                    ("RISK_EVENT_AUTHORITY_LOCKOUT_CLEARED",), 1,
                )[0]
                payload = clear["record"]["payload"]  # type: ignore[index]
                self.assertEqual(
                    payload["effect"],  # type: ignore[index]
                    "ONLY_SUCCESSFUL_PROTECTIVE_EXIT_LOCKOUT_CLEARED",
                )
            finally:
                ledger.close()

            reopened = PaperLedger(
                path,
                epoch_id="L3G-PAPER-EPOCH-TEST-PERPETUAL",
                policy=FIVE_MINUTE_PERPETUAL_PROFILE.policy,
                risk=FIVE_MINUTE_PERPETUAL_PROFILE.risk,
            )
            try:
                recovered_runtime = LaneIIIPaperRuntime(reopened)
                self.assertFalse(
                    recovered_runtime.risk.status()["locked_out"],
                    recovered_runtime.risk.status(),
                )
                trade_date = runtime._session_context.trade_date
                self.assertEqual(
                    recovered_runtime._trade_date_risk[trade_date].realized_pnl,
                    Decimal("-1.00"),
                )
                self.assertEqual(
                    recovered_runtime._trade_date_risk[trade_date].entry_count,
                    1,
                )
                self.assertEqual(
                    recovered_runtime._profile_trade_date_risk[
                        (trade_date, FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION)
                    ].consecutive_losses,
                    1,
                )
                recovered_runtime.risk.lock_out("PROTECTIVE_STOP_REJECTED")
                refused, _ = recovered_runtime._recover_completed_protective_exit_locked(
                    "must-not-clear-a-protection-failure", {},
                )
                self.assertFalse(refused)
                self.assertEqual(
                    recovered_runtime.risk.status()["lockout_reason"],
                    "PROTECTIVE_STOP_REJECTED",
                )
            finally:
                reopened.close()

    def test_legacy_protective_fill_settles_without_perpetual_reentry(self) -> None:
        at = "2026-09-01T14:00:00Z"
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=at,
        ):
            ledger, runtime, capture = self._runtime(
                directory, at=at, perpetual=False,
            )
            try:
                self._ready(runtime)
                self.assertTrue(
                    runtime.operational_paper_start(
                        "legacy-protective-settlement",
                    )["started"]
                )
                context = runtime._session_context
                runtime._state = PaperRuntimeState.LONG
                runtime._position = PaperDirection.LONG
                runtime._position_quantity = 1
                runtime._entry_owner = PaperEntryOwner.STRATEGY
                runtime._entry_fill_price = Decimal("100.25")
                runtime._entry_fill_quantity = 1
                runtime._entry_direction = PaperDirection.LONG
                runtime._entry_session_context = context
                runtime._entry_execution = {
                    "native_execution_id": "legacy-entry-execution",
                    "native_order_id": "legacy-entry-order",
                    "price": "100.25",
                    "quantity": 1,
                    "timestamp": at,
                }
                runtime._protective_order_id = "legacy-protective-order"
                runtime._snapshot = self._healthy_snapshot(
                    at, context, direction=PaperDirection.LONG,
                    position_opened_at=at,
                )

                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT",
                    "order_role": "PROTECTIVE",
                    "direction": "FLAT",
                    "price": "99.75",
                    "quantity": 1,
                    "native_execution_id": "legacy-protective-execution",
                    "native_order_id": "legacy-protective-order",
                    "account_name": "Sim101",
                    "instrument": "MNQ SEP26",
                    "timestamp": at,
                })
                self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
                runtime.on_execution_message({
                    "message_type": "POSITION_EVENT",
                    "quantity": 0,
                    "timestamp": at,
                })
                runtime.on_execution_message(self._filled_order(
                    "PROTECTIVE", "legacy-protective-order",
                ))
                self.assertEqual(runtime.state, PaperRuntimeState.RECONCILING)
                self.assertEqual(self._entry_actions(capture), [])
                runtime.on_execution_message(
                    self._flat_reconciliation("legacy-protective-flat", at),
                )
                self.assertEqual(runtime.state, PaperRuntimeState.PAPER_RUNNING)
                self.assertIsNotNone(runtime.status()["operational_paper_session"])
                self.assertEqual(self._entry_actions(capture), [])
            finally:
                ledger.close()

    def test_session_rollover_is_evidence_only_and_never_flattens_or_pauses_v2(self) -> None:
        old_at = "2026-09-01T19:59:00Z"
        new_at = "2026-09-01T20:31:00Z"
        with TemporaryDirectory() as directory, patch("src.l3g_paper.runtime._now", return_value=new_at):
            ledger, runtime, capture = self._runtime(directory, at=old_at)
            try:
                self._ready(runtime)
                with patch("src.l3g_paper.runtime._now", return_value=old_at):
                    runtime.operational_paper_start("perpetual-rollover")
                runtime._position = PaperDirection.LONG
                runtime._position_quantity = 1
                runtime._state = PaperRuntimeState.LONG
                runtime._snapshot = replace(
                    runtime._snapshot,
                    current_position=PaperDirection.LONG,
                    current_position_quantity=1,
                    position_opened_at=old_at,
                )
                capture.commands.clear()

                new_context = PaperSessionResolver().resolve(new_at, generation=5).context
                runtime._set_session_context(new_context, reason="MARKET_EVENT_SESSION")

                self.assertEqual(runtime.state, PaperRuntimeState.LONG)
                self.assertEqual(runtime._position, PaperDirection.LONG)
                self.assertEqual(runtime._position_quantity, 1)
                self.assertFalse(runtime._entries_paused)
                self.assertIsNotNone(runtime._operational_session)
                self.assertEqual(capture.commands, [])
                self.assertEqual(
                    [row["kind"] for row in ledger.recent(30)].count("SESSION_CLOSED"),
                    1,
                )
            finally:
                ledger.close()

    def test_v2_bypasses_holiday_hard_flat_and_max_age_only_for_this_profile(self) -> None:
        holiday_context = PaperSessionResolver().resolve(HOLIDAY_NOW, generation=4).context
        holiday_snapshot = self._healthy_snapshot(HOLIDAY_NOW, holiday_context)
        perpetual_risk = PaperRiskAuthority(
            profile=FIVE_MINUTE_PERPETUAL_RISK_PROFILE,
            policy=FIVE_MINUTE_PERPETUAL_POLICY,
        )
        legacy_risk = PaperRiskAuthority(
            profile=FIVE_MINUTE_RISK_PROFILE,
            policy=FIVE_MINUTE_POLICY,
        )
        self.assertNotIn(
            "HOLIDAY_SESSION_UNVERIFIED",
            perpetual_risk.preflight_reasons(holiday_snapshot, at=HOLIDAY_NOW),
        )
        self.assertIn(
            "HOLIDAY_SESSION_UNVERIFIED",
            legacy_risk.preflight_reasons(holiday_snapshot, at=HOLIDAY_NOW),
        )

        age_at = "2026-09-01T14:00:00Z"
        opened = "2026-08-31T14:00:00Z"
        aged = replace(
            self._healthy_snapshot(
                age_at,
                PaperSessionResolver().resolve(age_at, generation=4).context,
                direction=PaperDirection.LONG,
                position_opened_at=opened,
            ),
            current_position=PaperDirection.LONG,
            current_position_quantity=1,
        )
        self.assertFalse(perpetual_risk.maximum_age_due(aged, age_at))
        self.assertTrue(legacy_risk.maximum_age_due(aged, age_at))

        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=age_at,
        ):
            perpetual_ledger, perpetual_runtime, _ = self._runtime(
                str(Path(directory) / "v2"), at=age_at, perpetual=True,
            )
            legacy_ledger, legacy_runtime, _ = self._runtime(
                str(Path(directory) / "v1"), at=age_at, perpetual=False,
            )
            try:
                for runtime in (perpetual_runtime, legacy_runtime):
                    runtime._position = PaperDirection.LONG
                    runtime._position_quantity = 1
                    runtime._state = PaperRuntimeState.LONG
                    runtime._snapshot = aged
                perpetual_calls: list[tuple[object, ...]] = []
                legacy_calls: list[tuple[object, ...]] = []
                perpetual_runtime._request_exit = lambda *args, **kwargs: perpetual_calls.append((args, kwargs)) or True  # type: ignore[method-assign]
                legacy_runtime._request_exit = lambda *args, **kwargs: legacy_calls.append((args, kwargs)) or True  # type: ignore[method-assign]

                perpetual_runtime._evaluate_risk_exit(age_at)
                legacy_runtime._evaluate_risk_exit(age_at)

                self.assertEqual(perpetual_calls, [])
                self.assertEqual(legacy_calls[0][0], ("MAXIMUM_POSITION_AGE",))
            finally:
                perpetual_ledger.close()
                legacy_ledger.close()

        # Exercise the hard-flat switch separately at 15:59 New York, one
        # minute after the stable RTH hard-flat deadline and before the RTH
        # session domain closes. V2 keeps the existing position; V1 exits.
        hard_flat_at = "2026-09-01T19:59:00Z"
        hard_flat_context = PaperSessionResolver().resolve(
            hard_flat_at, generation=4,
        ).context
        hard_flat_snapshot = self._healthy_snapshot(
            hard_flat_at,
            hard_flat_context,
            direction=PaperDirection.LONG,
            position_opened_at=hard_flat_at,
        )
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=hard_flat_at,
        ):
            perpetual_ledger, perpetual_runtime, _ = self._runtime(
                str(Path(directory) / "hard-flat-v2"), at=hard_flat_at,
            )
            legacy_ledger, legacy_runtime, _ = self._runtime(
                str(Path(directory) / "hard-flat-v1"),
                at=hard_flat_at,
                perpetual=False,
            )
            try:
                for runtime in (perpetual_runtime, legacy_runtime):
                    runtime._position = PaperDirection.LONG
                    runtime._position_quantity = 1
                    runtime._state = PaperRuntimeState.LONG
                    runtime._snapshot = hard_flat_snapshot
                perpetual_calls = []
                legacy_calls = []
                perpetual_runtime._request_exit = lambda *args, **kwargs: perpetual_calls.append((args, kwargs)) or True  # type: ignore[method-assign]
                legacy_runtime._request_exit = lambda *args, **kwargs: legacy_calls.append((args, kwargs)) or True  # type: ignore[method-assign]

                perpetual_runtime._evaluate_risk_exit(hard_flat_at)
                legacy_runtime._evaluate_risk_exit(hard_flat_at)

                self.assertEqual(perpetual_calls, [])
                self.assertEqual(legacy_calls[0][0], ("HARD_FLAT_DEADLINE",))
            finally:
                perpetual_ledger.close()
                legacy_ledger.close()

    def test_recovered_checkpoint_requires_full_chain_coverage_before_entry_use(self) -> None:
        with TemporaryDirectory() as directory, patch("src.l3g_paper.runtime._now", return_value=NOW):
            ledger, runtime, _ = self._runtime(directory)
            signal = self._decision(
                runtime,
                PaperDirection.SHORT,
                created_at="2026-09-01T20:54:00Z",
                candle_close_utc="2026-09-01T20:54:00Z",
                suffix="restart-short",
            )
            self._commit_signal(runtime, signal)
            checkpoint_sequence = int(
                runtime._latest_five_minute_direction_checkpoint["ledger_sequence"]  # type: ignore[index]
            )
            ledger.close()

            reopened = PaperLedger(
                Path(directory) / "perpetual.sqlite3",
                policy=FIVE_MINUTE_PERPETUAL_POLICY,
                risk=FIVE_MINUTE_PERPETUAL_RISK_PROFILE,
            )
            recovered = LaneIIIPaperRuntime(reopened)
            try:
                self.assertIsNotNone(recovered._latest_five_minute_direction_checkpoint)
                self.assertFalse(recovered._perpetual_signal_ledger_verified)
                recovered.commissioning_rehearsal = lambda _preflight=None: {  # type: ignore[method-assign]
                    "result": "READY",
                    "blocking_reasons": [],
                    "ledger": {
                        "status": "PASS",
                        "verified_through_sequence": checkpoint_sequence - 1,
                    },
                }
                insufficient = recovered.operational_paper_readiness()
                self.assertIn(
                    "FIVE_MINUTE_SIGNAL_LEDGER_UNVERIFIED",
                    insufficient["deferred_entry_reasons"],
                )
                self.assertFalse(recovered._perpetual_signal_ledger_verified)

                recovered.commissioning_rehearsal = lambda _preflight=None: {  # type: ignore[method-assign]
                    "result": "READY",
                    "blocking_reasons": [],
                    "ledger": {
                        "status": "PASS",
                        "verified_through_sequence": checkpoint_sequence,
                    },
                }
                covered = recovered.operational_paper_readiness()
                self.assertNotIn(
                    "FIVE_MINUTE_SIGNAL_LEDGER_UNVERIFIED",
                    covered["deferred_entry_reasons"],
                )
                self.assertTrue(recovered._perpetual_signal_ledger_verified)
            finally:
                reopened.close()

    def test_malformed_and_dangling_checkpoints_fail_closed(self) -> None:
        with TemporaryDirectory() as directory:
            malformed_path = Path(directory) / "malformed.sqlite3"
            malformed = PaperLedger(
                malformed_path,
                policy=FIVE_MINUTE_PERPETUAL_POLICY,
                risk=FIVE_MINUTE_PERPETUAL_RISK_PROFILE,
            )
            malformed.append(
                "RISK_EVENT_FIVE_MINUTE_DIRECTION_CHECKPOINT",
                {"schema": "malformed"},
                identity="malformed-five-minute-checkpoint",
            )
            malformed.close()
            reopened = PaperLedger(
                malformed_path,
                policy=FIVE_MINUTE_PERPETUAL_POLICY,
                risk=FIVE_MINUTE_PERPETUAL_RISK_PROFILE,
            )
            malformed_runtime = LaneIIIPaperRuntime(reopened)
            try:
                self.assertTrue(malformed_runtime._entries_paused)
                self.assertEqual(
                    malformed_runtime.status()["risk_continuity_fault"],
                    "FIVE_MINUTE_SIGNAL_CHECKPOINT_INVALID",
                )
            finally:
                reopened.close()

            dangling_path = Path(directory) / "dangling.sqlite3"
            dangling = PaperLedger(
                dangling_path,
                policy=FIVE_MINUTE_PERPETUAL_POLICY,
                risk=FIVE_MINUTE_PERPETUAL_RISK_PROFILE,
            )
            context = PaperSessionResolver().resolve(NOW, generation=4).context
            dangling.set_session_context(context)
            fake_runtime = LaneIIIPaperRuntime(dangling)
            source = self._decision(
                fake_runtime,
                PaperDirection.LONG,
                created_at="2026-09-01T20:54:00Z",
                candle_close_utc="2026-09-01T20:54:00Z",
                suffix="missing-source",
            ).payload()
            base: dict[str, object] = {
                "schema": "lane-iii-five-minute-perpetual-signal-v1",
                "paper_policy_id": FIVE_MINUTE_PERPETUAL_POLICY.policy_id,
                "paper_policy_hash": FIVE_MINUTE_PERPETUAL_POLICY.configuration_hash,
                "risk_profile_hash": FIVE_MINUTE_PERPETUAL_RISK_PROFILE.configuration_hash,
                "entry_profile_version": FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
                "direction": "LONG",
                "candle_open_utc": "2026-09-01T20:53:30Z",
                "candle_close_utc": "2026-09-01T20:54:00Z",
                "source_decision_id": source["paper_decision_id"],
                "source_decision_ledger_sequence": 1,
                "source_decision_record_hash": "0" * 64,
                "source_decision": source,
            }
            dangling.append(
                "RISK_EVENT_FIVE_MINUTE_DIRECTION_CHECKPOINT",
                {**base, "signal_hash": canonical_hash(base)},
                identity="dangling-five-minute-checkpoint",
                occurred_at="2026-09-01T20:54:00Z",
            )
            dangling.close()
            reopened_dangling = PaperLedger(
                dangling_path,
                policy=FIVE_MINUTE_PERPETUAL_POLICY,
                risk=FIVE_MINUTE_PERPETUAL_RISK_PROFILE,
            )
            dangling_runtime = LaneIIIPaperRuntime(reopened_dangling)
            try:
                self.assertTrue(dangling_runtime._entries_paused)
                self.assertEqual(
                    dangling_runtime.status()["risk_continuity_fault"],
                    "FIVE_MINUTE_SIGNAL_CHECKPOINT_INVALID",
                )
            finally:
                reopened_dangling.close()

    def test_sim101_mnq_quantity_one_and_live_denial_are_not_configurable(self) -> None:
        self.assertEqual(ACCOUNT_BINDING.account_name, "Sim101")
        self.assertEqual(ACCOUNT_BINDING.account_class, "LOCAL_SIMULATION")
        self.assertEqual(ACCOUNT_BINDING.instrument, "MNQ SEP26")
        self.assertEqual(ACCOUNT_BINDING.maximum_quantity, 1)
        self.assertTrue(ACCOUNT_BINDING.paper_only)
        self.assertFalse(ACCOUNT_BINDING.live_capital)
        self.assertEqual(FIVE_MINUTE_PERPETUAL_RISK_PROFILE.maximum_absolute_position, 1)
        self.assertEqual(FIVE_MINUTE_PERPETUAL_RISK_PROFILE.maximum_entry_quantity, 1)
        self.assertTrue(FIVE_MINUTE_PERPETUAL_RISK_PROFILE.paper_only)
        self.assertFalse(FIVE_MINUTE_PERPETUAL_RISK_PROFILE.approved_for_live)

        with self.assertRaisesRegex(ValueError, "No configurable or live account binding"):
            ExecutionAccountBinding(live_capital=True)
        with self.assertRaisesRegex(ValueError, "one Sim101 MNQ contract"):
            FiveMinutePerpetualPaperRiskProfile(maximum_entry_quantity=2)

        context = PaperSessionResolver().resolve(NOW, generation=4).context
        bad_account = replace(
            self._healthy_snapshot(NOW, context),
            account_name="LiveAccount",
            account_class="BROKERAGE_LIVE",
        )
        authority = PaperRiskAuthority(
            profile=FIVE_MINUTE_PERPETUAL_RISK_PROFILE,
            policy=FIVE_MINUTE_PERPETUAL_POLICY,
        )
        self.assertIn("EXACT_SIM101_BINDING_REQUIRED", authority.preflight_reasons(bad_account, at=NOW))

        with TemporaryDirectory() as directory, patch("src.l3g_paper.runtime._now", return_value=NOW):
            ledger, runtime, _ = self._runtime(directory)
            try:
                status = runtime.status()
                self.assertEqual(
                    (
                        status["mode"], status["paper_account"], status["account_class"],
                        status["market_instrument"], status["maximum_quantity"],
                        status["live_capital"],
                    ),
                    ("PAPER_SIM101", "Sim101", "LOCAL_SIMULATION", "MNQ SEP26", 1, "DENIED"),
                )
            finally:
                ledger.close()

    def test_signed_oversize_position_is_preserved_and_permanently_blocks_entry(self) -> None:
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=NOW,
        ):
            ledger, runtime, capture = self._runtime(directory)
            try:
                runtime._state = PaperRuntimeState.LONG
                runtime._position = PaperDirection.LONG
                runtime._position_quantity = 1
                runtime._snapshot = self._healthy_snapshot(
                    NOW, runtime._session_context, direction=PaperDirection.LONG,
                )

                runtime.on_execution_message({
                    "message_type": "POSITION_EVENT",
                    "quantity": 2,
                    "timestamp": NOW,
                })

                status = runtime.status()
                self.assertEqual(runtime.state, PaperRuntimeState.LOCKED_OUT)
                self.assertEqual(status["current_position"], "LONG")
                self.assertEqual(status["current_quantity"], 2)
                self.assertEqual(status["lockout_or_fault_reason"], "MAXIMUM_QUANTITY_BREACH")
                self.assertTrue(runtime._snapshot.foreign_activity)
                self.assertTrue(runtime._retain_safety_lockout_after_flat)
                self.assertEqual(capture.commands, [])
            finally:
                ledger.close()


if __name__ == "__main__":
    unittest.main()
