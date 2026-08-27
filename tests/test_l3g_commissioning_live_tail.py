from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
from itertools import chain
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.lane_iii.contracts import canonical_hash
from src.l3g_paper import ledger as ledger_module
from src.l3g_paper.commissioning import (
    CommissioningLedgerGateError,
    evaluate_commissioning_ledger_gate,
    evaluate_commissioning_post_run_verification,
)
from src.l3g_paper.contracts import POLICY, PaperRuntimeState
from src.l3g_paper.health import ledger_health_projection
from src.l3g_paper.ledger import (
    COMMISSIONING_NO_AUTHORITY_EFFECT,
    COMMISSIONING_READINESS_RECORD_SEMANTICS,
    COMMISSIONING_READINESS_RECORD_SEMANTICS_VERSION,
    COMMISSIONING_TAIL_POLICY_VERSION,
    COMMISSIONING_WARMUP_POLICY_HASH,
    COMMISSIONING_WARMUP_REQUIRED_FAMILIES,
    CommissioningTailCategory,
    PaperLedger,
    commissioning_tail_classification,
    is_commissioning_safe_unverified_tail_record,
)
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.l3g_paper.sessions import PaperSessionResolver, UNSPECIFIED_OFF_SESSION_CONTEXT


NOW = "2026-08-26T17:30:00Z"
HASH = canonical_hash({"fixture": "commissioning-live-tail"})
OFF_CONTEXT = UNSPECIFIED_OFF_SESSION_CONTEXT.payload()
RTH_CONTEXT = PaperSessionResolver().resolve("2026-08-26T17:00:00Z", generation=1).context.payload()


def observation(number: int = 1, kind: str = "QUOTE") -> dict[str, object]:
    return {
        **OFF_CONTEXT,
        "observation_id": f"nt-passive-{number}",
        "observation_type": kind,
        "observed_at": NOW,
        "ninja_receipt_time": NOW,
        "provider_timestamp": None,
        "exchange_timestamp": NOW,
        "local_monotonic_sequence": number,
        "source_payload_hash": canonical_hash({"observation": number, "kind": kind}),
    }


def informational_account_observation(number: int = 1) -> dict[str, object]:
    return {
        **observation(number, "ACCOUNT"),
        "authority_effect": COMMISSIONING_NO_AUTHORITY_EFFECT,
        "observation_semantics": "INFORMATIONAL_ACCOUNT_ITEM",
        "observation_payload_keys": ["item", "value"],
        "observation_account_alias": "Sim101",
        "observation_account_class": "LOCAL_SIMULATION",
    }


def evidence(number: int = 1) -> dict[str, object]:
    return {
        "evidence_id": f"l3g-pe-{number}",
        "hypothesis_kind": "BULLISH_REVERSAL",
        "family": "ORDER_FLOW",
        "label": "PROVISIONAL_BUY_IMBALANCE",
        "strength": "0.75",
        "supports": True,
        "observed_at": NOW,
        "expires_at": "2026-08-26T17:30:05Z",
        "source_observation_ids": [f"nt-passive-{number}"],
        "source_local_sequences": [number],
        "source_payload_hashes": [canonical_hash({"source": number})],
        "quality": "PROVISIONAL_CONTIGUOUS_LOCAL_CALLBACKS",
        "sequence_authority": "LOCAL_CALLBACK_ORDER_ONLY",
        "book_completeness": "UNVERIFIED",
        "scientific_eligibility": False,
        "blocking": False,
        "session_kind": OFF_CONTEXT["session_kind"],
        "session_id": OFF_CONTEXT["session_id"],
        "trade_date": OFF_CONTEXT["trade_date"],
        "session_profile_hash": OFF_CONTEXT["session_profile_hash"],
        "session_generation": OFF_CONTEXT["session_generation"],
        "source_session_ids": [OFF_CONTEXT["session_id"]],
        "session_family": OFF_CONTEXT["session_family"],
    }


def decision(number: int = 1, value: str = "NO_TRADE", *, authority_effect: bool = True) -> dict[str, object]:
    payload: dict[str, object] = {
        "paper_decision_id": f"l3g-pd-{number}",
        "paper_policy_id": POLICY.policy_id,
        "paper_policy_hash": POLICY.configuration_hash,
        "decision": value,
        "created_at": NOW,
        "expires_at": "2026-08-26T17:30:05Z",
        "hypothesis_kind": None if value in {"NO_TRADE", "EXIT"} else "BULLISH_REVERSAL",
        "direction": {"NO_TRADE": "FLAT", "LONG": "LONG", "SHORT": "SHORT", "EXIT": "FLAT"}[value],
        "relative_support": "0.50",
        "family_summary": {"ORDER_FLOW": "SUPPORT"},
        "source_observation_ids": [f"nt-passive-{number}"],
        "source_local_sequences": [number],
        "source_payload_hashes": [canonical_hash({"decision-source": number})],
        "sequence_authority": "LOCAL_CALLBACK_ORDER_ONLY",
        "book_completeness": "UNVERIFIED",
        "scientific_eligibility": False,
        "reason_code": "FIXTURE",
        "session_kind": OFF_CONTEXT["session_kind"],
        "session_id": OFF_CONTEXT["session_id"],
        "trade_date": OFF_CONTEXT["trade_date"],
        "session_profile_hash": OFF_CONTEXT["session_profile_hash"],
        "session_generation": OFF_CONTEXT["session_generation"],
        "commissioning": False,
        "strategy_generated": True,
        "scientific_evidence": False,
        "session_family": OFF_CONTEXT["session_family"],
    }
    if authority_effect:
        payload["authority_effect"] = COMMISSIONING_NO_AUTHORITY_EFFECT
    return payload


def warmup_attestation(kind: str) -> dict[str, object]:
    common = {
        **RTH_CONTEXT,
        "authority_effect": COMMISSIONING_NO_AUTHORITY_EFFECT,
        "record_semantics": COMMISSIONING_READINESS_RECORD_SEMANTICS,
        "record_semantics_version": COMMISSIONING_READINESS_RECORD_SEMANTICS_VERSION,
        "commissioning_warmup_state": "WARMED" if kind == "COMMISSIONING_SESSION_WARMED" else "NOT_WARMED",
        "policy_hash": COMMISSIONING_WARMUP_POLICY_HASH,
        "required_families": list(COMMISSIONING_WARMUP_REQUIRED_FAMILIES),
    }
    if kind == "COMMISSIONING_SESSION_WARMED":
        return {
            **common,
            "warmed_at": NOW,
            "reason": "ALL_REQUIRED_FAMILIES_GENUINELY_OBSERVED",
            "evidence_provenance": {
                family: {
                    "evidence_id": f"l3g-pe-{index}",
                    "observed_at": NOW,
                    "source_observation_ids": [f"nt-warmup-{index}"],
                    "source_local_sequences": [index],
                }
                for index, family in enumerate(COMMISSIONING_WARMUP_REQUIRED_FAMILIES, start=1)
            },
        }
    return {
        **common,
        "reset_at": NOW,
        "reason": "INVALID_OR_CROSSED_QUOTE",
        "seen_families": ["ORDER_FLOW"],
        "warmed_at": NOW,
    }


def legacy_unmarked_warmup(kind: str = "COMMISSIONING_SESSION_WARMED") -> dict[str, object]:
    payload = warmup_attestation(kind)
    for key in (
        "authority_effect", "record_semantics", "record_semantics_version", "commissioning_warmup_state",
    ):
        payload.pop(key)
    return payload


def runtime_snapshot() -> dict[str, object]:
    return {
        "commissioning_id": "l3g-commissioning-test",
        "account": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "instrument": "MNQ SEP26",
        "current_position": "FLAT",
        "current_position_quantity": 0,
        "broker_snapshot_position": "FLAT",
        "broker_snapshot_position_quantity": 0,
        "working_owned_orders": 0,
        "working_entry_orders": 0,
        "position_snapshot_complete": True,
        "order_snapshot_complete": True,
        "reconciliation_current": True,
        "unresolved_command": False,
        "unresolved_native_order": False,
        "unresolved_execution": False,
        "entry_owner": "NONE",
        "commissioning_ownership_active": False,
        "live_capital": "DENIED",
        "runtime_state": "READY_DISARMED",
        "session_kind": "NEW_YORK_RTH",
        "session_family": "NEW_YORK",
        "session_id": "MNQU6:NEW_YORK_RTH:2026-08-26",
        "trade_date": "2026-08-26",
        "session_profile_hash": RTH_CONTEXT["session_profile_hash"],
        "session_generation": 1,
        "transport": {
            "reconciled": True,
            "account": "Sim101",
            "account_class": "LOCAL_SIMULATION",
            "instrument": "MNQ SEP26",
            "live_capital": "DENIED",
        },
    }


def write_v2_watermark(
    path: Path, *, classified: int, mutation_sequence: int, safe_last: dict[str, int],
) -> None:
    with closing(sqlite3.connect(path)) as connection, connection:
        row = None if mutation_sequence == 0 else connection.execute(
            "SELECT kind, domain, record_hash FROM lane_iii_paper_audit WHERE ledger_sequence=?",
            (mutation_sequence,),
        ).fetchone()
        watermark = {
            "policy_version": "l3g-commissioning-passive-tail-v2",
            "classified_through_sequence": classified,
            "last_authority_mutation_sequence": mutation_sequence,
            "last_authority_mutation_kind": None if row is None else row[0],
            "last_authority_mutation_domain": None if row is None else row[1],
            "last_authority_mutation_hash": None if row is None else row[2],
            "safe_classification_last_sequences": safe_last,
            "updated_at": NOW,
        }
        connection.execute(
            "UPDATE lane_iii_paper_ledger_metadata SET metadata_value=? WHERE metadata_key=?",
            (json.dumps(watermark, sort_keys=True, separators=(",", ":")), "commissioning_authority_watermark"),
        )


class CommissioningLiveTailTests(unittest.TestCase):
    def prepared_gate(
        self, ledger: PaperLedger, anchor: int, *, full_anchor: int | None = None,
    ) -> tuple[dict[str, object], dict[str, object]]:
        full_sequence = anchor if full_anchor is None else full_anchor
        tail = ledger.commissioning_tail_snapshot(anchor, last_full_verified_sequence=full_sequence)
        anchor_hash = tail["verified_anchor_record_hash"]
        verification = {
            "status": "PASS",
            "chain_valid": True,
            "checkpoint_valid": True,
            "full_scan_required": False,
            "quick_check": "inherited_from_full",
            "completed_at": NOW,
            "verification_id": "lv-incremental-test",
            "verification_mode": "incremental" if full_sequence < anchor else "full",
            "verified_through_sequence": anchor,
            "tip_hash": anchor_hash,
            "last_full_verified_sequence": full_sequence,
            "last_full_verified_hash": tail["last_full_anchor_record_hash"],
            "last_full_verification_id": "lv-full-test",
            "last_full_quick_check_at": NOW,
            "ledger_identity": tail["ledger_identity"],
            "ledger_epoch": tail["ledger_epoch"],
            "ledger_schema_version": tail["ledger_schema_version"],
        }
        return verification, tail

    def evaluate(self, ledger: PaperLedger, anchor: int) -> dict[str, object]:
        verification, tail = self.prepared_gate(ledger, anchor)
        return evaluate_commissioning_ledger_gate(
            verification,
            tail,
            runtime_snapshot(),
            checkpoint_matches_report=True,
            freshness_seconds=900,
            now=datetime.fromisoformat(NOW.replace("Z", "+00:00")),
        )

    def test_explicit_taxonomy_is_strict_and_fail_closed(self) -> None:
        cases = (
            ("OBSERVATION", "OBSERVATION_ENVELOPE", observation(), CommissioningTailCategory.PASSIVE_DATA),
            ("OBSERVATION", "OBSERVATION_ENVELOPE", informational_account_observation(), CommissioningTailCategory.AUTHORITY_OBSERVATION),
            ("EVIDENCE", "EVIDENCE", evidence(), CommissioningTailCategory.PASSIVE_DATA),
            ("DECISION", "DECISION", decision(), CommissioningTailCategory.PASSIVE_DATA),
            ("DECISION", "DECISION", decision(authority_effect=False), CommissioningTailCategory.AUTHORITY_MUTATION),
            ("COMMAND", "COMMAND", {"command_id": "command-1"}, CommissioningTailCategory.AUTHORITY_MUTATION),
            ("ORDER_EVENT", "ORDER_EVENT_ACCEPTED", {"order_id": "order-1"}, CommissioningTailCategory.AUTHORITY_MUTATION),
            ("EXECUTION", "EXECUTION_FILL", {"execution_id": "execution-1"}, CommissioningTailCategory.AUTHORITY_MUTATION),
            ("RISK_EVENT", "RISK_EVENT_MUTATION", {"risk_authority": "changed"}, CommissioningTailCategory.AUTHORITY_MUTATION),
            ("SESSION", "SESSION_AUTHORITY", {"reason": "anchor"}, CommissioningTailCategory.AUTHORITY_MUTATION),
            ("INCIDENT", "INCIDENT_TRANSPORT_AMBIGUITY", {"reason": "UNKNOWN"}, CommissioningTailCategory.UNKNOWN),
        )
        for domain, kind, payload, expected in cases:
            with self.subTest(domain=domain, kind=kind, expected=expected):
                self.assertEqual(commissioning_tail_classification(domain, kind, payload).category, expected)

        for domain, kind, payload in (
            ("OBSERVATION", "OBSERVATION_ENVELOPE", {**observation(), "future": True}),
            ("OBSERVATION", "OBSERVATION_ENVELOPE", {**informational_account_observation(), "future": True}),
            ("EVIDENCE", "EVIDENCE", {**evidence(), "future": True}),
            ("DECISION", "DECISION", {**decision(), "future": True}),
            ("OBSERVATION", "OBSERVATION_ENVELOPE", {**observation(), "session_generation": True}),
        ):
            with self.subTest(extra_field=(domain, kind)):
                self.assertEqual(
                    commissioning_tail_classification(domain, kind, payload).category,
                    CommissioningTailCategory.UNKNOWN,
                )

    def test_classifier_is_total_for_malformed_json_compatible_values(self) -> None:
        malformed = (
            ("OBSERVATION", "OBSERVATION_ENVELOPE", {**observation(), "observation_type": []}),
            (
                "OBSERVATION", "OBSERVATION_ENVELOPE",
                {**informational_account_observation(), "observation_account_alias": []},
            ),
            ("EVIDENCE", "EVIDENCE", {**evidence(), "family": {}}),
            ("DECISION", "DECISION", {**decision(), "decision": []}),
            (
                "INCIDENT", "COMMISSIONING_SESSION_WARMUP_RESET",
                {**warmup_attestation("COMMISSIONING_SESSION_WARMUP_RESET"), "seen_families": [{}]},
            ),
        )
        for domain, kind, payload in malformed:
            with self.subTest(domain=domain, kind=kind):
                classification = commissioning_tail_classification(domain, kind, payload)
                self.assertEqual(classification.category, CommissioningTailCategory.UNKNOWN)

        self.assertEqual(
            commissioning_tail_classification([], "DECISION", decision()).category,  # type: ignore[arg-type]
            CommissioningTailCategory.UNKNOWN,
        )

    def test_exact_warmup_rows_are_observations_but_legacy_or_malformed_rows_are_unknown(self) -> None:
        for kind in ("COMMISSIONING_SESSION_WARMED", "COMMISSIONING_SESSION_WARMUP_RESET"):
            exact = warmup_attestation(kind)
            self.assertEqual(
                commissioning_tail_classification("INCIDENT", kind, exact).category,
                CommissioningTailCategory.AUTHORITY_OBSERVATION,
            )
            self.assertEqual(
                commissioning_tail_classification("INCIDENT", kind, legacy_unmarked_warmup(kind)).category,
                CommissioningTailCategory.UNKNOWN,
            )
            self.assertEqual(
                commissioning_tail_classification("INCIDENT", kind, {**exact, "future": True}).category,
                CommissioningTailCategory.UNKNOWN,
            )
            self.assertEqual(
                commissioning_tail_classification(
                    "INCIDENT", kind, {**exact, "record_semantics_version": True},
                ).category,
                CommissioningTailCategory.UNKNOWN,
            )

    def test_passive_tail_is_accepted(self) -> None:
        with TemporaryDirectory() as directory, PaperLedger(Path(directory) / "paper.sqlite3") as ledger:
            ledger.append("SESSION_AUTHORITY", {"reason": "verified anchor"})
            anchor = int(ledger.health_status()["highest_sequence"])
            for number, kind in enumerate(("QUOTE", "TRADE", "DEPTH"), start=1):
                ledger.append("OBSERVATION_ENVELOPE", observation(number, kind))
            ledger.append("EVIDENCE", evidence())
            for number, value in enumerate(("NO_TRADE", "LONG", "SHORT", "EXIT"), start=1):
                ledger.append("DECISION", decision(number, value))
            result = self.evaluate(ledger, anchor)
            self.assertEqual(result["ledger_trust_state"], "VERIFIED_ANCHOR_WITH_PASSIVE_LIVE_TAIL")
            self.assertEqual(result["tail_authority_classification"], "PASSIVE_ONLY")
            self.assertEqual(result["last_authority_mutation_sequence"], anchor)
            self.assertEqual(result["last_unknown_sequence"], 0)
            self.assertEqual(result["unverified_tail_rows"], 8)

    def test_account_and_exact_warmup_attestations_are_accepted_observations(self) -> None:
        with TemporaryDirectory() as directory, PaperLedger(Path(directory) / "paper.sqlite3") as ledger:
            ledger.append("SESSION_AUTHORITY", {"reason": "verified anchor"})
            anchor = int(ledger.health_status()["highest_sequence"])
            ledger.append("OBSERVATION_ENVELOPE", informational_account_observation())
            ledger.append("COMMISSIONING_SESSION_WARMED", warmup_attestation("COMMISSIONING_SESSION_WARMED"))
            ledger.append("COMMISSIONING_SESSION_WARMUP_RESET", warmup_attestation("COMMISSIONING_SESSION_WARMUP_RESET"))
            result = self.evaluate(ledger, anchor)
            self.assertEqual(result["ledger_trust_state"], "VERIFIED_ANCHOR_WITH_ACCEPTED_LIVE_TAIL")
            self.assertEqual(result["tail_authority_classification"], "AUTHORITY_OBSERVATIONS_ONLY")
            self.assertEqual(result["last_authority_observation_sequence"], anchor + 3)
            self.assertEqual(result["last_unknown_sequence"], 0)
            self.assertEqual(result["tail_record_categories"], ["AUTHORITY_OBSERVATION"])

    def test_mutation_and_unknown_tails_both_deny(self) -> None:
        forbidden = (
            ("DECISION", decision(authority_effect=False), "AUTHORITY_MUTATION"),
            ("ORDER_EVENT_ACCEPTED", {"order_id": "order-1"}, "AUTHORITY_MUTATION"),
            ("COMMISSIONING_OWNERSHIP_RESERVED", {"commissioning_id": "commissioning-1"}, "AUTHORITY_MUTATION"),
            ("INCIDENT_TRANSPORT_AMBIGUITY", {"reason": "UNKNOWN"}, "UNKNOWN"),
            ("COMMISSIONING_SESSION_WARMED", legacy_unmarked_warmup(), "UNKNOWN"),
        )
        for kind, payload, classification in forbidden:
            with self.subTest(kind=kind), TemporaryDirectory() as directory:
                with PaperLedger(Path(directory) / "paper.sqlite3") as ledger:
                    ledger.append("SESSION_AUTHORITY", {"reason": "verified anchor"})
                    anchor = int(ledger.health_status()["highest_sequence"])
                    ledger.append(kind, payload)
                    tail = ledger.commissioning_tail_snapshot(anchor, last_full_verified_sequence=anchor)
                    self.assertEqual(tail["last_blocking_classification"], classification)
                    with self.assertRaises(CommissioningLedgerGateError) as raised:
                        self.evaluate(ledger, anchor)
                    self.assertEqual(raised.exception.code, "COMMISSIONING_LEDGER_TAIL_UNTRUSTED")
                    self.assertTrue(raised.exception.launch_auto)

    def test_unknown_observation_shapes_are_not_blanket_allowed(self) -> None:
        for payload in (
            {**observation(1), "observation_type": "ORDER"},
            {**observation(2), "observation_type": "ACCOUNT"},
        ):
            record = {"domain": "OBSERVATION", "kind": "OBSERVATION_ENVELOPE", "payload": payload}
            self.assertFalse(is_commissioning_safe_unverified_tail_record(record))

    def test_malformed_v2_safe_map_row_rebuilds_unknown_instead_of_raising(self) -> None:
        cases = (
            (
                "OBSERVATION_ENVELOPE", {**observation(), "observation_type": []},
                "OBSERVATION:OBSERVATION_ENVELOPE:QUOTE",
            ),
            (
                "OBSERVATION_ENVELOPE",
                {**informational_account_observation(), "observation_account_alias": []},
                "OBSERVATION:OBSERVATION_ENVELOPE:ACCOUNT_ITEM_INFORMATIONAL:AUTHORITY_EFFECT_NONE",
            ),
            ("EVIDENCE", {**evidence(), "family": {}}, "EVIDENCE:EVIDENCE"),
            (
                "DECISION", {**decision(), "decision": []},
                "DECISION:DECISION:NO_TRADE:AUTHORITY_EFFECT_NONE",
            ),
        )
        for kind, payload, claimed_shape in cases:
            with self.subTest(kind=kind), TemporaryDirectory() as directory:
                path = Path(directory) / "paper.sqlite3"
                with PaperLedger(path) as ledger:
                    ledger.append(kind, payload)
                    sequence = int(ledger.health_status()["highest_sequence"])
                write_v2_watermark(
                    path, classified=sequence, mutation_sequence=0,
                    safe_last={claimed_shape: sequence},
                )
                with PaperLedger(path) as reopened:
                    watermark = reopened.health_status()["authority_watermark"]
                    self.assertEqual(watermark["last_unknown_sequence"], sequence)
                    self.assertEqual(watermark["classified_through_sequence"], sequence)

    def test_malformed_tail_categories_fail_closed_with_stable_gate_error(self) -> None:
        with TemporaryDirectory() as directory, PaperLedger(Path(directory) / "paper.sqlite3") as ledger:
            ledger.append("SESSION_AUTHORITY", {"reason": "verified anchor"})
            anchor = int(ledger.health_status()["highest_sequence"])
            verification, tail = self.prepared_gate(ledger, anchor)
            tail["tail_record_categories"] = [{}]
            with self.assertRaises(CommissioningLedgerGateError) as raised:
                evaluate_commissioning_ledger_gate(
                    verification, tail, runtime_snapshot(), checkpoint_matches_report=True,
                    freshness_seconds=900, now=datetime.fromisoformat(NOW.replace("Z", "+00:00")),
                )
            self.assertEqual(raised.exception.code, "COMMISSIONING_LEDGER_TAIL_UNCLASSIFIED")

    def test_mixed_hundred_thousand_passive_recurring_observations_and_one_mutation_denies(self) -> None:
        passive = {"domain": "OBSERVATION", "kind": "OBSERVATION_ENVELOPE", "payload": observation()}
        observations = (
            {
                "domain": "OBSERVATION", "kind": "OBSERVATION_ENVELOPE",
                "payload": informational_account_observation(),
            },
            {
                "domain": "INCIDENT", "kind": "COMMISSIONING_SESSION_WARMED",
                "payload": warmup_attestation("COMMISSIONING_SESSION_WARMED"),
            },
            {
                "domain": "INCIDENT", "kind": "COMMISSIONING_SESSION_WARMUP_RESET",
                "payload": warmup_attestation("COMMISSIONING_SESSION_WARMUP_RESET"),
            },
        )
        mutation = {
            "domain": "ORDER_EVENT", "kind": "ORDER_EVENT_ACCEPTED", "payload": {"order_id": "real-order"},
        }

        def traffic():
            for number in range(50_000):
                yield passive
                if number % 5_000 == 0:
                    yield from observations

        records = chain(traffic(), (mutation,), traffic())
        self.assertEqual(sum(not is_commissioning_safe_unverified_tail_record(record) for record in records), 1)
        anchor = 10
        mutation_sequence = anchor + 50_031
        tip = anchor + 100_061
        observation_sequence = tip - 4_999
        verification = {
            "status": "PASS", "chain_valid": True, "checkpoint_valid": True,
            "full_scan_required": False, "quick_check": "inherited_from_full",
            "completed_at": NOW, "verification_id": "lv-large-mixed-tail",
            "verified_through_sequence": anchor, "tip_hash": HASH,
            "last_full_verified_sequence": anchor, "last_full_verified_hash": HASH,
            "last_full_verification_id": "lv-large-mixed-tail-full",
            "last_full_quick_check_at": NOW, "ledger_identity": "ledger-large-tail",
            "ledger_epoch": "L3G-PAPER-EPOCH-002", "ledger_schema_version": "fixture",
        }
        tail = {
            "policy_version": COMMISSIONING_TAIL_POLICY_VERSION,
            "ledger_identity": "ledger-large-tail", "ledger_epoch": "L3G-PAPER-EPOCH-002",
            "ledger_schema_version": "fixture", "verified_through_sequence": anchor,
            "verified_anchor_record_hash": HASH, "last_full_verified_sequence": anchor,
            "last_full_anchor_record_hash": HASH, "arm_snapshot_tip": tip,
            "arm_snapshot_tip_hash": HASH, "classified_through_sequence": tip,
            "classified_through_hash": HASH, "unverified_tail_rows": tip - anchor,
            "tail_record_kinds": [
                "OBSERVATION:OBSERVATION_ENVELOPE:QUOTE",
                "OBSERVATION:OBSERVATION_ENVELOPE:ACCOUNT_ITEM_INFORMATIONAL:AUTHORITY_EFFECT_NONE",
                "AUTHORITY_OBSERVATION:COMMISSIONING_SESSION_WARMED:AUTHORITY_EFFECT_NONE",
                "AUTHORITY_OBSERVATION:COMMISSIONING_SESSION_WARMUP_RESET:AUTHORITY_EFFECT_NONE",
            ],
            "tail_record_categories": [
                "PASSIVE_DATA", "AUTHORITY_OBSERVATION", "AUTHORITY_MUTATION",
            ],
            "last_authority_mutation_sequence": mutation_sequence,
            "last_authority_mutation_kind": "ORDER_EVENT_ACCEPTED",
            "last_authority_mutation_domain": "ORDER_EVENT",
            "last_authority_mutation_hash": HASH,
            "last_authority_observation_sequence": observation_sequence,
            "last_authority_observation_kind": "COMMISSIONING_SESSION_WARMUP_RESET",
            "last_authority_observation_domain": "INCIDENT",
            "last_authority_observation_hash": HASH,
            "last_unknown_sequence": 0, "last_unknown_kind": None,
            "last_unknown_domain": None, "last_unknown_hash": None,
            "last_blocking_sequence": mutation_sequence,
            "last_blocking_kind": "ORDER_EVENT_ACCEPTED",
            "last_blocking_domain": "ORDER_EVENT", "last_blocking_hash": HASH,
            "last_blocking_classification": "AUTHORITY_MUTATION",
        }
        with self.assertRaises(CommissioningLedgerGateError) as raised:
            evaluate_commissioning_ledger_gate(
                verification, tail, runtime_snapshot(), checkpoint_matches_report=True,
                freshness_seconds=900, now=datetime.fromisoformat(NOW.replace("Z", "+00:00")),
            )
        self.assertEqual(raised.exception.code, "COMMISSIONING_LEDGER_TAIL_UNTRUSTED")

    def test_tampered_account_position_snapshot_and_runtime_reconciliation_shapes_deny(self) -> None:
        wrong_account = {
            **informational_account_observation(),
            "observation_account_alias": "Lucid25kflex01",
            "observation_account_class": "LOCAL_SIMULATION",
        }
        wrong_effect = {**informational_account_observation(), "authority_effect": "MUTATES"}
        for payload in (wrong_account, wrong_effect):
            self.assertEqual(
                commissioning_tail_classification(
                    "OBSERVATION", "OBSERVATION_ENVELOPE", payload,
                ).category,
                CommissioningTailCategory.UNKNOWN,
            )
        self.assertEqual(
            commissioning_tail_classification(
                "POSITION_SNAPSHOT", "POSITION_SNAPSHOT", {
                    "account": "Sim101", "instrument": "NQ SEP26",
                    "position_snapshot_complete": False, "authority_effect": "NONE",
                },
            ).category,
            CommissioningTailCategory.AUTHORITY_MUTATION,
        )

        with TemporaryDirectory() as directory, PaperLedger(Path(directory) / "paper.sqlite3") as ledger:
            ledger.append("SESSION_AUTHORITY", {"reason": "verified anchor"})
            anchor = int(ledger.health_status()["highest_sequence"])
            verification, tail = self.prepared_gate(ledger, anchor)
            for changes in (
                {"position_snapshot_complete": False},
                {"order_snapshot_complete": False},
                {"instrument": "NQ SEP26"},
                {"account": "Lucid25kflex01"},
            ):
                with self.subTest(runtime_changes=changes), self.assertRaises(
                    CommissioningLedgerGateError,
                ) as raised:
                    evaluate_commissioning_ledger_gate(
                        verification, tail, {**runtime_snapshot(), **changes},
                        checkpoint_matches_report=True, freshness_seconds=900,
                        now=datetime.fromisoformat(NOW.replace("Z", "+00:00")),
                    )
                self.assertEqual(raised.exception.code, "COMMISSIONING_RUNTIME_NOT_RECONCILED")

            ledger.append("POSITION_SNAPSHOT", {
                "account": "Sim101", "instrument": "MNQ SEP26",
                "position_snapshot_complete": True, "order_snapshot_complete": True,
                "authority_effect": "NONE",
            })
            with self.assertRaises(CommissioningLedgerGateError) as raised:
                self.evaluate(ledger, anchor)
            self.assertEqual(raised.exception.code, "COMMISSIONING_LEDGER_TAIL_UNTRUSTED")

    def test_deferred_batches_preserve_separate_watermarks_and_restart(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            with PaperLedger(path) as ledger:
                ledger.append("SESSION_AUTHORITY", {"reason": "verified anchor"})
                anchor = int(ledger.health_status()["highest_sequence"])
                for number in range(1, 521):
                    ledger.append_deferred("OBSERVATION_ENVELOPE", observation(number))
                ledger.append_deferred("DECISION", {**decision(800), "future": True})
                ledger.append_deferred("OBSERVATION_ENVELOPE", informational_account_observation(900))
                for number in range(901, 1421):
                    ledger.append_deferred("OBSERVATION_ENVELOPE", observation(number))
                ledger.flush_deferred()
                snapshot = ledger.commissioning_tail_snapshot(anchor, last_full_verified_sequence=anchor)
                self.assertEqual(snapshot["last_authority_mutation_sequence"], anchor)
                self.assertEqual(snapshot["last_unknown_sequence"], anchor + 521)
                self.assertEqual(snapshot["last_authority_observation_sequence"], anchor + 522)
                self.assertEqual(snapshot["classified_through_sequence"], snapshot["arm_snapshot_tip"])
                expected = {
                    key: snapshot[key]
                    for key in (
                        "last_authority_mutation_sequence", "last_authority_observation_sequence",
                        "last_unknown_sequence", "classified_through_sequence", "classified_through_hash",
                    )
                }
            with PaperLedger(path) as reopened:
                snapshot = reopened.commissioning_tail_snapshot(anchor, last_full_verified_sequence=anchor)
                for key, value in expected.items():
                    self.assertEqual(snapshot[key], value)
                with self.assertRaises(CommissioningLedgerGateError):
                    self.evaluate(reopened, anchor)

    def test_valid_v2_metadata_migrates_without_reclassifying_legacy_unsafe_boundary(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            with PaperLedger(path) as ledger:
                ledger.append("SESSION_AUTHORITY", {"reason": "verified anchor"})
                ledger.append("OBSERVATION_ENVELOPE", observation())
                ledger.append("COMMISSIONING_SESSION_WARMED", legacy_unmarked_warmup())
                tip = int(ledger.health_status()["highest_sequence"])
            write_v2_watermark(path, classified=tip, mutation_sequence=tip, safe_last={
                "OBSERVATION:OBSERVATION_ENVELOPE:QUOTE": tip - 1,
            })
            with PaperLedger(path) as reopened:
                watermark = reopened.health_status()["authority_watermark"]
                self.assertEqual(watermark["policy_version"], COMMISSIONING_TAIL_POLICY_VERSION)
                self.assertEqual(watermark["last_authority_mutation_sequence"], 0)
                self.assertEqual(watermark["last_unknown_sequence"], tip)
                self.assertEqual(
                    watermark["safe_classification_last_sequences"]["OBSERVATION:OBSERVATION_ENVELOPE:QUOTE"],
                    tip - 1,
                )

    def test_v2_suffix_migration_is_bounded_and_conservative(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            with PaperLedger(path) as ledger:
                ledger.append("SESSION_AUTHORITY", {"reason": "verified anchor"})
                anchor = int(ledger.health_status()["highest_sequence"])
                for number in range(1, 6):
                    ledger.append("OBSERVATION_ENVELOPE", observation(number))
                tip = int(ledger.health_status()["highest_sequence"])
            write_v2_watermark(path, classified=anchor, mutation_sequence=anchor, safe_last={})
            with patch.object(ledger_module, "_COMMISSIONING_WATERMARK_SCAN_LIMIT", 3):
                with PaperLedger(path) as reopened:
                    watermark = reopened.health_status()["authority_watermark"]
                    self.assertEqual(watermark["classified_through_sequence"], tip)
                    self.assertEqual(watermark["last_unknown_sequence"], tip)
                    self.assertEqual(watermark["safe_classification_last_sequences"], {})
                    with self.assertRaises(CommissioningLedgerGateError):
                        self.evaluate(reopened, anchor)

    def test_v2_subset_safe_row_is_covered_by_unknown_migration_umbrella(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            with PaperLedger(path) as ledger:
                ledger.append("SESSION_AUTHORITY", {"reason": "verified anchor"})
                anchor = int(ledger.health_status()["highest_sequence"])
                ledger.append("OBSERVATION_ENVELOPE", {**observation(), "future": True})
                tip = int(ledger.health_status()["highest_sequence"])
            # v2 accepted this subset-plus-extra shape. v3 must retain the map
            # for bounded migration, but may not let it cross the older anchor.
            write_v2_watermark(
                path,
                classified=tip,
                mutation_sequence=anchor,
                safe_last={"OBSERVATION:OBSERVATION_ENVELOPE:QUOTE": tip},
            )
            with PaperLedger(path) as reopened:
                watermark = reopened.health_status()["authority_watermark"]
                self.assertEqual(watermark["last_unknown_sequence"], tip)
                self.assertEqual(
                    watermark["safe_classification_last_sequences"][
                        "OBSERVATION:OBSERVATION_ENVELOPE:QUOTE"
                    ],
                    tip,
                )
                with self.assertRaises(CommissioningLedgerGateError) as raised:
                    self.evaluate(reopened, anchor)
                self.assertEqual(raised.exception.code, "COMMISSIONING_LEDGER_TAIL_UNTRUSTED")
            with PaperLedger(path) as restarted:
                watermark = restarted.health_status()["authority_watermark"]
                self.assertEqual(watermark["last_unknown_sequence"], tip)
                self.assertEqual(watermark["classified_through_sequence"], tip)

    def test_auto_live_traffic_mutation_and_incremental_coverage_lifecycle(self) -> None:
        with TemporaryDirectory() as directory, PaperLedger(Path(directory) / "paper.sqlite3") as ledger:
            ledger.append("SESSION_AUTHORITY", {"reason": "initial Auto Full PASS anchor"})
            full_anchor = int(ledger.health_status()["highest_sequence"])

            initial = self.evaluate(ledger, full_anchor)
            self.assertEqual(initial["ledger_trust_state"], "VERIFIED_TO_ARM_SNAPSHOT_TIP")

            ledger.append("OBSERVATION_ENVELOPE", observation())
            ledger.append("OBSERVATION_ENVELOPE", informational_account_observation(2))
            ledger.append(
                "COMMISSIONING_SESSION_WARMED",
                warmup_attestation("COMMISSIONING_SESSION_WARMED"),
            )
            continued = self.evaluate(ledger, full_anchor)
            self.assertEqual(
                continued["ledger_trust_state"], "VERIFIED_ANCHOR_WITH_ACCEPTED_LIVE_TAIL",
            )
            self.assertEqual(
                continued["tail_authority_classification"],
                "PASSIVE_AND_AUTHORITY_OBSERVATIONS",
            )

            ledger.append("ORDER_EVENT_ACCEPTED", {"order_id": "verified-after-next-Auto"})
            mutation_sequence = int(ledger.health_status()["highest_sequence"])
            with self.assertRaises(CommissioningLedgerGateError) as denied:
                self.evaluate(ledger, full_anchor)
            self.assertEqual(denied.exception.code, "COMMISSIONING_LEDGER_TAIL_UNTRUSTED")

            ledger.append("OBSERVATION_ENVELOPE", observation(3, "TRADE"))
            verification, tail = self.prepared_gate(
                ledger, mutation_sequence, full_anchor=full_anchor,
            )
            covered = evaluate_commissioning_ledger_gate(
                verification, tail, runtime_snapshot(), checkpoint_matches_report=True,
                freshness_seconds=900, now=datetime.fromisoformat(NOW.replace("Z", "+00:00")),
            )
            self.assertEqual(verification["verification_mode"], "incremental")
            self.assertEqual(covered["verified_through_sequence"], mutation_sequence)
            self.assertEqual(
                covered["ledger_trust_state"], "VERIFIED_ANCHOR_WITH_PASSIVE_LIVE_TAIL",
            )

    def test_repeated_commissioning_rehearsal_is_authority_free(self) -> None:
        with TemporaryDirectory() as directory, PaperLedger(Path(directory) / "paper.sqlite3") as ledger:
            runtime = LaneIIIPaperRuntime(ledger)
            try:
                runtime._state = PaperRuntimeState.READY_DISARMED
                before_health = ledger.health_status()
                before_counts = ledger.counts()
                before_risk = dict(runtime.risk.status())
                before_status = runtime.status()
                with patch("src.l3g_paper.runtime._now", return_value=NOW):
                    results = [
                        runtime.commissioning_rehearsal(
                            lambda commissioning_id, snapshot: {
                                "ledger_trust_state": "VERIFIED_ANCHOR_WITH_ACCEPTED_LIVE_TAIL",
                                "verified_through_sequence": 10,
                                "arm_snapshot_tip": 12,
                                "unverified_tail_rows": 2,
                            },
                        )
                        for _ in range(25)
                    ]
                ledger.flush_deferred()
                after_status = runtime.status()
                self.assertTrue(all(result["result"] == "BLOCKED" for result in results))
                self.assertEqual(ledger.health_status()["highest_sequence"], before_health["highest_sequence"])
                self.assertEqual(ledger.health_status()["final_record_hash"], before_health["final_record_hash"])
                self.assertEqual(ledger.counts(), before_counts)
                self.assertEqual(runtime.risk.status()["arm_attempts"], before_risk["arm_attempts"])
                self.assertEqual(after_status["entry_owner"], "NONE")
                self.assertFalse(after_status["commissioning_lifecycle"]["active"])
                self.assertEqual(after_status["working_owned_orders"], before_status["working_owned_orders"])
                for domain in (
                    "RISK_GRANT", "INTENT", "COMMAND", "ORDER_EVENT", "EXECUTION",
                ):
                    self.assertEqual(ledger.counts().get(domain, 0), 0)
            finally:
                runtime.stop()

    def test_stale_anchor_and_broker_reconciliation_mismatch_deny(self) -> None:
        with TemporaryDirectory() as directory, PaperLedger(Path(directory) / "paper.sqlite3") as ledger:
            ledger.append("SESSION_AUTHORITY", {"reason": "verified anchor"})
            anchor = int(ledger.health_status()["highest_sequence"])
            verification, tail = self.prepared_gate(ledger, anchor)
            verification["completed_at"] = "2026-08-26T16:00:00Z"
            with self.assertRaises(CommissioningLedgerGateError) as stale:
                evaluate_commissioning_ledger_gate(
                    verification, tail, runtime_snapshot(), checkpoint_matches_report=True,
                    freshness_seconds=900, now=datetime.fromisoformat(NOW.replace("Z", "+00:00")),
                )
            self.assertEqual(stale.exception.code, "COMMISSIONING_LEDGER_ANCHOR_STALE")
            verification["completed_at"] = NOW
            mismatched = {**runtime_snapshot(), "current_position_quantity": 1}
            with self.assertRaises(CommissioningLedgerGateError) as broker:
                evaluate_commissioning_ledger_gate(
                    verification, tail, mismatched, checkpoint_matches_report=True,
                    freshness_seconds=900,
                    now=datetime.now(timezone.utc).replace(
                        year=2026, month=8, day=26, hour=17, minute=30, second=0, microsecond=0,
                    ),
                )
            self.assertEqual(broker.exception.code, "COMMISSIONING_RUNTIME_NOT_RECONCILED")

    def test_health_projection_distinguishes_observation_mutation_and_unknown_tails(self) -> None:
        verification = {"status": "PASS", "chain_valid": True, "verified_through_sequence": 100}

        def projected(*, mutation: int = 99, observation: int = 80, unknown: int = 0) -> dict[str, object]:
            return ledger_health_projection(
                {
                    "highest_sequence": 150,
                    "final_record_hash": HASH,
                    "authority_watermark": {
                        "last_authority_mutation_sequence": mutation,
                        "last_authority_observation_sequence": observation,
                        "last_unknown_sequence": unknown,
                        "classified_through_sequence": 150,
                        "classified_through_hash": HASH,
                    },
                },
                verification,
            )

        self.assertEqual(
            projected()["commissioning_ledger_state"], "VERIFIED_ANCHOR_WITH_PASSIVE_LIVE_TAIL",
        )
        observed = projected(observation=125)
        self.assertEqual(observed["commissioning_ledger_state"], "VERIFIED_ANCHOR_WITH_ACCEPTED_LIVE_TAIL")
        self.assertEqual(observed["last_authority_observation_sequence"], 125)
        mutated = projected(mutation=125)
        self.assertEqual(mutated["commissioning_ledger_state"], "UNVERIFIED_AUTHORITY_MUTATION_TAIL")
        self.assertEqual(mutated["last_blocking_classification"], "AUTHORITY_MUTATION")
        unknown = projected(unknown=130)
        self.assertEqual(unknown["commissioning_ledger_state"], "UNVERIFIED_UNKNOWN_TAIL")
        self.assertEqual(unknown["last_blocking_classification"], "UNKNOWN")
        hash_mismatch = ledger_health_projection(
            {
                "highest_sequence": 150,
                "final_record_hash": canonical_hash({"different": True}),
                "authority_watermark": {
                    "last_authority_mutation_sequence": 99,
                    "last_authority_observation_sequence": 125,
                    "last_unknown_sequence": 0,
                    "classified_through_sequence": 150,
                    "classified_through_hash": HASH,
                },
            },
            verification,
        )
        self.assertEqual(hash_mismatch["commissioning_ledger_state"], "UNTRUSTED")

    def test_post_run_pass_requires_clean_disarmed_economics_and_incremental_coverage(self) -> None:
        closure = {
            "classification": "EXPLICIT_PAPER_COMMISSIONING",
            "commissioning_id": "l3g-commissioning-post-run",
            "entry_direction": "LONG",
            "entry_price": "100.25",
            "entry_quantity": 1,
            "exit_price": "101",
            "exit_quantity": 1,
            "contract_value_per_point": "2",
            "realized_pnl": "1.50",
            "final_position": "FLAT",
            "final_quantity": 0,
            "final_working_order_count": 0,
            "reconciliation_state": "CLEAN",
            "lock_disarm_state": "READY_DISARMED",
            "closure_ledger_sequence": 120,
            "lucid_mutation_count": 0,
            "incidents": [],
        }
        verification = {
            "status": "PASS", "chain_valid": True, "checkpoint_valid": True,
            "full_scan_required": False, "errors": [], "verification_mode": "incremental",
            "verification_id": "lv-post-run", "verified_through_sequence": 125,
        }
        passed = evaluate_commissioning_post_run_verification(
            closure, verification, checkpoint_matches_report=True,
        )
        self.assertEqual(passed["result"], "PASS")
        attacks = {
            "flat alone is insufficient": ({**closure, "lock_disarm_state": "ARMED_FLAT"}, verification, True),
            "pnl mismatch": ({**closure, "realized_pnl": "1.00"}, verification, True),
            "verifier failure": (closure, {**verification, "status": "FAIL"}, True),
            "wrong mode": (closure, {**verification, "verification_mode": "full"}, True),
            "lifecycle uncovered": (closure, {**verification, "verified_through_sequence": 119}, True),
            "checkpoint mismatch": (closure, verification, False),
        }
        for name, (candidate, report, checkpoint) in attacks.items():
            with self.subTest(name=name):
                result = evaluate_commissioning_post_run_verification(
                    candidate, report, checkpoint_matches_report=checkpoint,
                )
                self.assertEqual(result["result"], "COMMISSIONING_INCOMPLETE")
                self.assertTrue(result["blocking_reasons"])


if __name__ == "__main__":
    unittest.main()
