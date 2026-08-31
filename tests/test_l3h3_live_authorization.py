from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest

from src.l3h_live.contracts import canonical_hash
from src.l3h_live.event_store import LiveEventStore
from src.l3h_live.gateway import AuthenticatedLoopbackGateway, GatewayDispatchError
from src.l3h_live.live_authorization import (
    ActionClass, AuthorizationAccountClass, AuthorizationFacts, HumanAuthorization,
    LiveAuthorizationBoundary, LiveAuthorizationState, LiveEntryRequest,
    classify_action, identity_from_native_metadata, verify_native_admission_envelope,
)


HASH = "a" * 64
NATIVE_KEY = b"n" * 32


def timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def metadata(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "account_id": "native-101", "account_name": "ExactLive", "display_name": "Exact Live",
        "fcm": "FCM", "account_provider": "Tradovate", "account_status": "Enabled",
        "connection_name": "ExactConnection", "connection_provider": "Tradovate",
        "connection_brand": "NinjaTrader", "connection_type": "Tradovate",
        "connection_mode": "Live", "connection_is_demo": False,
        "connection_can_manage_orders": True, "connection_status": "Connected",
    }
    value.update(overrides)
    return value


class L3H3LiveAuthorizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "l3h3.sqlite3"
        self.now = datetime.now(timezone.utc).replace(microsecond=0)

    def facts(self, **overrides: object) -> AuthorizationFacts:
        observed = timestamp(self.now)
        account = identity_from_native_metadata(
            metadata(), safe_account_id="LIVE-ACCOUNT-VERIFIED",
            account_class=AuthorizationAccountClass.LIVE_CAPITAL, observed_at=observed,
        )
        values: dict[str, object] = {
            "observed_at": observed, "account": account, "native_instrument": "MNQ SEP26",
            "canonical_contract": "MNQU6", "maximum_quantity": 1, "position": "FLAT", "quantity": 0,
            "owned_working_entry_orders": 0, "unresolved_owned_protective_orders": 0,
            "foreign_or_unknown_activity": 0, "gateway_authenticated": True,
            "gateway_session_id": "l3h3-gateway-session-unit", "addon_session_id": "l3h3-addon-session-unit",
            "addon_provenance": HASH, "addon_build_identity": HASH, "provider_connected": True,
            "connection_fresh": True, "reconciliation_status": "PASS", "reconciliation_observed_at": observed,
            "protection_status": "PASS", "command_kill_ready": True, "native_menu_kill_ready": True,
            "out_of_band_kill_ready": True, "beelzebub_build_identity": HASH,
            "strategy_runtime_identity": "unit-strategy-runtime", "runtime_session_id": "unit-runtime-session",
        }
        values.update(overrides)
        return AuthorizationFacts(**values)  # type: ignore[arg-type]

    def boundary(self, *, store: LiveEventStore | None = None) -> LiveAuthorizationBoundary:
        return LiveAuthorizationBoundary(
            store or LiveEventStore(self.path), native_hmac_key=NATIVE_KEY,
            beelzebub_build_identity=HASH, expected_addon_provenance=HASH,
        )

    def ceremony(self, *, facts: AuthorizationFacts | None = None) -> tuple[LiveAuthorizationBoundary, str, AuthorizationFacts]:
        exact = facts or self.facts()
        boundary = self.boundary()
        preflight = boundary.start_preflight(exact, now=timestamp(self.now))
        self.assertEqual(preflight.blockers, ())
        challenge = boundary.begin_authorization(
            preflight_id=preflight.preflight_id, preflight_digest=preflight.digest, now=timestamp(self.now),
        )
        capability_id = boundary.authorize(HumanAuthorization(
            preflight_id=challenge.preflight_id, preflight_digest=challenge.preflight_digest,
            challenge=challenge.challenge, safe_account_id=challenge.safe_account_id,
            account_class=challenge.account_class, native_instrument=challenge.native_instrument,
            quantity=challenge.quantity, authority_type=challenge.authority_type,
            acknowledgement=challenge.acknowledgement, actor_type="LOCAL_HUMAN", local_transport=True,
        ), now=timestamp(self.now))
        return boundary, capability_id, exact

    def request(self, facts: AuthorizationFacts, **overrides: object) -> LiveEntryRequest:
        values: dict[str, object] = {
            "request_id": "l3h3-request-unit", "strategy_signal_id": "l3h3-signal-unit",
            "action": "ENTER_LONG", "account_fingerprint": facts.account.account_fingerprint,
            "account_class": "LIVE_CAPITAL", "native_instrument": "MNQ SEP26",
            "canonical_contract": "MNQU6", "quantity": 1, "resulting_position_quantity": 1,
        }
        values.update(overrides)
        return LiveEntryRequest(**values)  # type: ignore[arg-type]

    def test_exact_state_machine_starts_and_finishes_disarmed_without_a_send(self) -> None:
        boundary = self.boundary()
        self.assertEqual(boundary.state, LiveAuthorizationState.DISARMED)
        preflight = boundary.start_preflight(self.facts(), now=timestamp(self.now))
        self.assertEqual(boundary.state, LiveAuthorizationState.PREFLIGHT_READY)
        challenge = boundary.begin_authorization(
            preflight_id=preflight.preflight_id, preflight_digest=preflight.digest, now=timestamp(self.now),
        )
        self.assertEqual(boundary.state, LiveAuthorizationState.AUTHORIZATION_PENDING)
        authorization = HumanAuthorization(
            preflight_id=challenge.preflight_id, preflight_digest=challenge.preflight_digest,
            challenge=challenge.challenge, safe_account_id=challenge.safe_account_id,
            account_class=challenge.account_class, native_instrument=challenge.native_instrument,
            quantity=1, authority_type=challenge.authority_type, acknowledgement=challenge.acknowledgement,
            actor_type="LOCAL_HUMAN", local_transport=True,
        )
        boundary.authorize(authorization, now=timestamp(self.now))
        self.assertEqual(boundary.state, LiveAuthorizationState.CANARY_AUTHORIZED)
        boundary.disarm("TEST_STOP_BEFORE_ADMISSION")
        self.assertEqual(boundary.status()["live_authority"], "DISARMED")
        self.assertEqual(LiveEventStore(self.path).verify(), (True, "PASS"))

    def test_simulation_unknown_and_ambiguous_identity_never_preflight_as_live(self) -> None:
        for account_class in (AuthorizationAccountClass.LOCAL_SIMULATION, AuthorizationAccountClass.UNKNOWN):
            with self.subTest(account_class=account_class):
                account = replace(self.facts().account, account_class=account_class)
                boundary = self.boundary(store=LiveEventStore(Path(self.directory.name) / f"{account_class}.sqlite3"))
                preflight = boundary.start_preflight(self.facts(account=account), now=timestamp(self.now))
                self.assertIn("BLOCKED_LIVE_ACCOUNT_IDENTITY", preflight.blockers)
                self.assertEqual(boundary.state, LiveAuthorizationState.DISARMED)

    def test_account_switch_after_preflight_and_account_a_capability_on_b_are_quarantined(self) -> None:
        boundary, capability_id, facts = self.ceremony()
        other = identity_from_native_metadata(
            metadata(account_id="native-202", account_name="OtherLive"), safe_account_id="OTHER-LIVE",
            account_class=AuthorizationAccountClass.LIVE_CAPITAL, observed_at=timestamp(self.now),
        )
        switched = replace(facts, account=other)
        with self.assertRaisesRegex(ValueError, "CHANGED|BINDING_MISMATCH"):
            boundary.atomic_admit(capability_id, self.request(facts), switched, command_id="l3h-cmd-switch")
        self.assertEqual(boundary.state, LiveAuthorizationState.QUARANTINED)

    def test_wrong_contract_alias_and_instrument_family_are_denied(self) -> None:
        for native, canonical in (("MNQ 09-26", "MNQU6"), ("MNQ SEP26", "NQU6"), ("NQ SEP26", "NQU6")):
            with self.subTest(native=native, canonical=canonical):
                facts = self.facts(native_instrument=native, canonical_contract=canonical)
                boundary = self.boundary(store=LiveEventStore(Path(self.directory.name) / canonical_hash((native, canonical))))
                preflight = boundary.start_preflight(facts, now=timestamp(self.now))
                self.assertIn("WRONG_CONTRACT", preflight.blockers)

    def test_quantity_two_mutation_scale_in_pyramiding_and_reversal_are_denied(self) -> None:
        facts = self.facts()
        for request_overrides in (
            {"quantity": 2}, {"action": "SCALE_IN", "resulting_position_quantity": 2},
            {"action": "PYRAMID", "resulting_position_quantity": 2},
            {"action": "ATOMIC_REVERSAL", "resulting_position_quantity": -1},
            {"action": "ENTER_LONG", "resulting_position_quantity": -1},
            {"action": "ENTER_SHORT", "resulting_position_quantity": 1},
        ):
            with self.subTest(request_overrides=request_overrides):
                boundary, capability_id, _ = self.ceremony(facts=facts)
                with self.assertRaises(ValueError):
                    boundary.atomic_admit(
                        capability_id, self.request(facts, **request_overrides), facts,
                        command_id="l3h-cmd-risk-mutation",
                    )
                self.assertEqual(boundary.status()["live_authority"], "DISARMED")

    def test_stale_preflight_stale_account_observation_and_expired_capability_disarm(self) -> None:
        stale_at = timestamp(self.now - timedelta(seconds=16))
        stale_account = replace(self.facts().account, observed_at=stale_at)
        stale_facts = self.facts(observed_at=stale_at, reconciliation_observed_at=stale_at, account=stale_account)
        preflight = self.boundary().start_preflight(stale_facts, now=timestamp(self.now))
        self.assertTrue({"STALE_ACCOUNT_OBSERVATION", "STALE_PREFLIGHT_FACTS", "BLOCKED_RECONCILIATION"}.issubset(preflight.blockers))
        boundary, capability_id, facts = self.ceremony()
        with self.assertRaisesRegex(ValueError, "EXPIRED"):
            boundary.atomic_admit(
                capability_id, self.request(facts), facts, command_id="l3h-cmd-expired",
                now=timestamp(self.now + timedelta(seconds=61)),
            )
        self.assertEqual(boundary.state, LiveAuthorizationState.DISARMED)

    def test_preflight_digest_nonce_and_exact_human_acknowledgement_must_match(self) -> None:
        mutations = {"preflight_digest": "b" * 64, "challenge": "wrong-nonce", "quantity": 2, "local_transport": False}
        for field, mutation in mutations.items():
            with self.subTest(field=field):
                boundary = self.boundary(store=LiveEventStore(Path(self.directory.name) / f"human-{field}.sqlite3"))
                preflight = boundary.start_preflight(self.facts(), now=timestamp(self.now))
                challenge = boundary.begin_authorization(
                    preflight_id=preflight.preflight_id, preflight_digest=preflight.digest, now=timestamp(self.now),
                )
                values = {
                    "preflight_id": challenge.preflight_id, "preflight_digest": challenge.preflight_digest,
                    "challenge": challenge.challenge, "safe_account_id": challenge.safe_account_id,
                    "account_class": challenge.account_class, "native_instrument": challenge.native_instrument,
                    "quantity": challenge.quantity, "authority_type": challenge.authority_type,
                    "acknowledgement": challenge.acknowledgement, "actor_type": "LOCAL_HUMAN", "local_transport": True,
                }
                values[field] = mutation
                with self.assertRaises(ValueError):
                    boundary.authorize(HumanAuthorization(**values), now=timestamp(self.now))  # type: ignore[arg-type]
                self.assertEqual(boundary.state, LiveAuthorizationState.DISARMED)

    def test_reconciliation_position_orders_foreign_transport_protection_and_kill_fail_closed(self) -> None:
        mutations = (
            {"reconciliation_status": "STALE"}, {"position": "UNKNOWN", "quantity": None},
            {"position": "LONG", "quantity": 1}, {"owned_working_entry_orders": 1},
            {"unresolved_owned_protective_orders": 1}, {"foreign_or_unknown_activity": 1},
            {"provider_connected": False}, {"gateway_authenticated": False},
            {"connection_fresh": False}, {"protection_status": "FAIL"},
            {"command_kill_ready": False}, {"native_menu_kill_ready": False},
            {"out_of_band_kill_ready": False}, {"stale_or_unknown_state": True},
            {"quarantine_reason": "UNIT_QUARANTINE"}, {"lock_reason": "UNIT_LOCK"},
        )
        for index, mutation in enumerate(mutations):
            with self.subTest(mutation=mutation):
                boundary = self.boundary(store=LiveEventStore(Path(self.directory.name) / f"block-{index}.sqlite3"))
                preflight = boundary.start_preflight(self.facts(**mutation), now=timestamp(self.now))
                self.assertTrue(preflight.blockers)
                self.assertNotEqual(boundary.state, LiveAuthorizationState.PREFLIGHT_READY)

    def test_changed_addon_build_gateway_or_runtime_session_invalidates_atomic_admission(self) -> None:
        for mutation, expected in (
            ({"addon_provenance": "b" * 64}, "BLOCKED_PROVENANCE"),
            ({"beelzebub_build_identity": "b" * 64}, "BUILD_OR_RUNTIME_IDENTITY_INVALID"),
        ):
            with self.subTest(preflight_mutation=mutation):
                boundary = self.boundary(store=LiveEventStore(Path(self.directory.name) / (expected + ".sqlite3")))
                preflight = boundary.start_preflight(self.facts(**mutation), now=timestamp(self.now))
                self.assertIn(expected, preflight.blockers)
                self.assertEqual(boundary.state, LiveAuthorizationState.DISARMED)
        mutations = (
            {"addon_provenance": "b" * 64}, {"addon_session_id": "restarted-addon"},
            {"gateway_session_id": "restarted-gateway"}, {"beelzebub_build_identity": "b" * 64},
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                boundary, capability_id, facts = self.ceremony()
                with self.assertRaisesRegex(ValueError, "CHANGED|BINDING_MISMATCH"):
                    boundary.atomic_admit(
                        capability_id, self.request(facts), replace(facts, **mutation),
                        command_id="l3h-cmd-session-change",
                    )

    def test_quarantine_and_lock_cannot_be_cleared_by_requesting_another_preflight(self) -> None:
        for mutation in ({"foreign_or_unknown_activity": 1}, {"lock_reason": "UNIT_LOCK"}):
            with self.subTest(mutation=mutation):
                boundary = self.boundary(store=LiveEventStore(Path(self.directory.name) / canonical_hash(mutation)))
                boundary.start_preflight(self.facts(**mutation), now=timestamp(self.now))
                with self.assertRaisesRegex(ValueError, "SAFETY_LATCH"):
                    boundary.start_preflight(self.facts(), now=timestamp(self.now))

    def test_one_capability_has_at_most_one_concurrent_atomic_admission(self) -> None:
        boundary, capability_id, facts = self.ceremony()
        barrier = threading.Barrier(8)
        outcomes: list[str] = []
        lock = threading.Lock()

        def attempt(index: int) -> None:
            barrier.wait()
            try:
                boundary.atomic_admit(
                    capability_id, self.request(facts, request_id=f"l3h3-request-{index}"), facts,
                    command_id=f"l3h-cmd-concurrent-{index}",
                )
                result = "ADMITTED"
            except ValueError as error:
                result = str(error)
            with lock:
                outcomes.append(result)

        threads = [threading.Thread(target=attempt, args=(index,)) for index in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(outcomes.count("ADMITTED"), 1)
        self.assertEqual(boundary.state, LiveAuthorizationState.CANARY_CONSUMED)

    def test_duplicate_strategy_signal_and_capability_replay_cannot_create_second_admission(self) -> None:
        boundary, capability_id, facts = self.ceremony()
        request = self.request(facts, strategy_signal_id="duplicate-signal")
        boundary.atomic_admit(capability_id, request, facts, command_id="l3h-cmd-first")
        with self.assertRaisesRegex(ValueError, "DISARMED"):
            boundary.atomic_admit(capability_id, request, facts, command_id="l3h-cmd-second")
        audit = repr([event.payload for event in boundary.store.stream(boundary._stream_id)])
        self.assertIn("CAPABILITY_REPLAY_OR_AUTHORITY_DISARMED", audit)
        self.assertIn(canonical_hash(capability_id), audit)

    def test_cancel_flatten_kill_and_protection_do_not_require_entry_authority(self) -> None:
        boundary = self.boundary()
        for action in ("CANCEL", "CANCEL_OWNED_ORDERS", "PROTECT", "FLATTEN", "KILL_FLATTEN_DISARM", "EMERGENCY_LIQUIDATE"):
            with self.subTest(action=action):
                self.assertEqual(boundary.admit_risk_reducing(action), (True, "RISK_REDUCTION_INDEPENDENT_OF_ENTRY_AUTHORITY"))
        self.assertEqual(boundary.admit_risk_reducing("ATOMIC_REVERSAL")[0], False)
        self.assertEqual(boundary.state, LiveAuthorizationState.DISARMED)

    def test_action_classification_never_mistakes_reversal_for_flatten(self) -> None:
        self.assertEqual(classify_action("FLATTEN"), ActionClass.RISK_REDUCING)
        self.assertEqual(classify_action("ATOMIC_REVERSAL"), ActionClass.RISK_INCREASING)
        self.assertEqual(classify_action("UNKNOWN"), ActionClass.PROHIBITED)

    def test_restart_and_historical_ledger_never_restore_authority(self) -> None:
        store = LiveEventStore(self.path)
        first = self.boundary(store=store)
        preflight = first.start_preflight(self.facts(), now=timestamp(self.now))
        challenge = first.begin_authorization(preflight_id=preflight.preflight_id, preflight_digest=preflight.digest, now=timestamp(self.now))
        first.authorize(HumanAuthorization(
            preflight_id=challenge.preflight_id, preflight_digest=challenge.preflight_digest,
            challenge=challenge.challenge, safe_account_id=challenge.safe_account_id,
            account_class=challenge.account_class, native_instrument=challenge.native_instrument,
            quantity=1, authority_type=challenge.authority_type, acknowledgement=challenge.acknowledgement,
            actor_type="LOCAL_HUMAN", local_transport=True,
        ), now=timestamp(self.now))
        self.assertEqual(first.state, LiveAuthorizationState.CANARY_AUTHORIZED)
        restarted = self.boundary(store=store)
        self.assertEqual(restarted.state, LiveAuthorizationState.DISARMED)
        self.assertIsNone(restarted.capability_id)
        self.assertEqual(store.verify(), (True, "PASS"))

    def test_gateway_refuses_bare_live_entry_before_transport_or_hmac_can_imply_authority(self) -> None:
        gateway = AuthenticatedLoopbackGateway(NATIVE_KEY, expected_addon_fingerprint=HASH, expected_capability_hash=HASH)
        bare_live = {
            "command_id": "l3h-cmd-bare-live", "request_id": "l3h3-request-bare-live",
            "client_order_id": "BZ-L3H-BARE-LIVE", "action": "ENTER_LONG",
            "account_class": "LIVE_CAPITAL", "live_capital": True,
        }
        with self.assertRaisesRegex(GatewayDispatchError, "LIVE_AUTHORIZATION_REQUIRED"):
            gateway.dispatch(bare_live)
        bare_sim = {**bare_live, "account_class": "LOCAL_SIMULATION", "live_capital": False}
        with self.assertRaisesRegex(GatewayDispatchError, "NOT_AUTHENTICATED"):
            gateway.dispatch(bare_sim)
        self.assertEqual(gateway.live_send_count, 0)

    def test_native_envelope_is_hmac_session_command_account_contract_and_quantity_bound(self) -> None:
        boundary, capability_id, facts = self.ceremony()
        request = self.request(facts)
        command_id = "l3h-cmd-envelope"
        envelope = boundary.atomic_admit(capability_id, request, facts, command_id=command_id, now=timestamp(self.now))
        command = {
            "command_id": command_id, "request_id": request.request_id, "action": request.action,
            "account_fingerprint": request.account_fingerprint, "account_class": request.account_class,
            "native_instrument": request.native_instrument, "canonical_contract": request.canonical_contract,
            "quantity": request.quantity,
        }
        verify_native_admission_envelope(
            envelope.as_mapping(), NATIVE_KEY,
            authorization_session_id=boundary.authorization_session_id,
            addon_session_id=facts.addon_session_id, gateway_session_id=facts.gateway_session_id,
            command=command, now=timestamp(self.now),
        )
        tampered_signature = envelope.as_mapping(); tampered_signature["signature"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "SIGNATURE"):
            verify_native_admission_envelope(
                tampered_signature, NATIVE_KEY,
                authorization_session_id=boundary.authorization_session_id,
                addon_session_id=facts.addon_session_id, gateway_session_id=facts.gateway_session_id,
                command=command, now=timestamp(self.now),
            )
        for session_field, session_value in (
            ("authorization_session_id", "l3h3-auth-session-restarted"),
            ("addon_session_id", "l3h3-addon-session-restarted"),
            ("gateway_session_id", "l3h3-gateway-session-restarted"),
        ):
            with self.subTest(session_field=session_field):
                arguments = {
                    "authorization_session_id": boundary.authorization_session_id,
                    "addon_session_id": facts.addon_session_id,
                    "gateway_session_id": facts.gateway_session_id,
                }
                arguments[session_field] = session_value
                with self.assertRaisesRegex(ValueError, "SESSION"):
                    verify_native_admission_envelope(
                        envelope.as_mapping(), NATIVE_KEY, command=command, now=timestamp(self.now), **arguments,
                    )
        for field, mutation in (("quantity", 2), ("native_instrument", "NQ SEP26"), ("account_fingerprint", "b" * 64)):
            with self.subTest(field=field):
                altered = dict(command); altered[field] = mutation
                with self.assertRaisesRegex(ValueError, "MISMATCH"):
                    verify_native_admission_envelope(
                        envelope.as_mapping(), NATIVE_KEY,
                        authorization_session_id=boundary.authorization_session_id,
                        addon_session_id=facts.addon_session_id, gateway_session_id=facts.gateway_session_id,
                        command=altered, now=timestamp(self.now),
                    )

    def test_native_metadata_requires_exact_fields_and_returns_hashes_not_raw_account_id(self) -> None:
        identity = identity_from_native_metadata(
            metadata(), safe_account_id="SAFE-LIVE", account_class=AuthorizationAccountClass.LIVE_CAPITAL,
            observed_at=timestamp(self.now),
        )
        self.assertEqual(len(identity.account_fingerprint), 64)
        self.assertNotIn("native-101", repr(identity))
        incomplete = metadata(); incomplete.pop("connection_is_demo")
        with self.assertRaisesRegex(ValueError, "FIELDS"):
            identity_from_native_metadata(
                incomplete, safe_account_id="SAFE-LIVE", account_class=AuthorizationAccountClass.LIVE_CAPITAL,
                observed_at=timestamp(self.now),
            )

    def test_audit_events_contain_transition_evidence_but_no_bearer_signature_or_raw_account_id(self) -> None:
        boundary, _, _ = self.ceremony()
        text = repr([event.payload for event in boundary.store.stream(boundary._stream_id)])
        for required in ("previous_state", "next_state", "preflight_digest", "account_fingerprint", "actor_type"):
            self.assertIn(required, text)
        self.assertNotIn("signature", text.lower())
        self.assertNotIn("native-101", text)


if __name__ == "__main__":
    unittest.main()
