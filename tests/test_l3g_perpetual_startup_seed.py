from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.l3f_provider.ninjatrader_observation import L3F2_SCHEMA
from src.lane_iii.contracts import canonical_hash
from src.l3g_paper.contracts import (
    ACCOUNT_BINDING,
    FIVE_MINUTE_PERPETUAL_PROFILE,
    FIVE_MINUTE_PROFILE,
    BookCompleteness,
    EvidenceFamily,
    HypothesisKind,
    PaperDecision,
    PaperDecisionKind,
    PaperDirection,
    PaperEvidence,
    SequenceAuthority,
    deterministic_id,
)
from src.l3g_paper.perpetual_startup_seed import (
    PERPETUAL_STARTUP_SEED_EXPORT_KIND,
    build_perpetual_boundary_bundle,
    build_perpetual_observation_proof,
    build_perpetual_startup_seed_core,
    perpetual_startup_seed_export_identity,
    perpetual_startup_seed_export_payload,
    read_perpetual_startup_seed_artifact,
    read_perpetual_startup_seed_proof,
    validate_perpetual_startup_seed_core,
    write_perpetual_startup_seed_artifact,
    write_perpetual_startup_seed_proof,
)
from src.l3g_paper.sessions import PaperSessionResolver


def _digest(value: object) -> str:
    return sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")).hexdigest()


def _rehash(value: dict[str, object], field: str) -> None:
    value[field] = _digest({key: item for key, item in value.items() if key != field})


class _SeedFixture:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.ledger_path = str((root / "source.sqlite3").resolve())
        self.operation_id = "profile-switch-" + "1" * 32
        self.ledger_identity = "l3g-ledger-" + "2" * 32
        self.ledger_epoch = "L3G-PAPER-EPOCH-SEED-SOURCE"
        resolution = PaperSessionResolver().resolve(
            "2026-09-08T14:05:01Z", generation=3,
        )
        self.context = resolution.context
        self.next_sequence = 1
        self.previous_hash: str | None = None
        self.next_local_sequence = 1
        self.proofs: list[dict[str, object]] = []

    def _record(
        self, kind: str, payload: dict[str, object], *, identity: str,
        occurred_at: str,
    ) -> dict[str, object]:
        policy = FIVE_MINUTE_PROFILE.policy
        risk = FIVE_MINUTE_PROFILE.risk
        session_identity = {
            key: self.context.payload()[key] for key in (
                "session_kind", "session_family", "session_id", "trade_date",
                "session_profile_hash", "session_generation",
            )
        }
        record: dict[str, object] = {
            "schema": "lane-iii-phase-g-paper-record-v1",
            "kind": kind,
            "occurred_at": occurred_at,
            "execution_session_id": None,
            "paper_policy_hash": policy.configuration_hash,
            "risk_profile_hash": risk.configuration_hash,
            "entry_profile": policy.entry_profile,
            "entry_profile_version": policy.entry_profile_version,
            "effective_confidence_threshold": str(policy.entry_support_threshold),
            "entry_dominance_margin": str(policy.entry_dominance_margin),
            "entry_family_count": policy.entry_family_count,
            "retention_confidence_threshold": str(policy.retention_support_threshold),
            "account_binding_hash": ACCOUNT_BINDING.binding_hash,
            "scientific_eligibility": False,
            "paper_only": True,
            "live_capital": False,
            **session_identity,
            "payload": payload,
            "identity": identity,
            "previous_record_hash": self.previous_hash,
        }
        record_hash = canonical_hash(record)
        record["record_hash"] = record_hash
        result = {
            "ledger_sequence": self.next_sequence,
            "record_hash": record_hash,
            "record": record,
        }
        self.next_sequence += 1
        self.previous_hash = record_hash
        return result

    def observation(
        self, identifier: str, kind: str, at: str, payload: dict[str, object],
    ) -> dict[str, object]:
        local_sequence = self.next_local_sequence
        self.next_local_sequence += 1
        wire = {
            "schema": L3F2_SCHEMA,
            "observation_id": identifier,
            "session_id": "authentic-seed-market-session",
            "observation_type": kind,
            "ninja_receipt_time": at,
            "local_monotonic_sequence": local_sequence,
            "provider_timestamp": at,
            "provider_sequence": None,
            "exchange_timestamp": at,
            "account": None,
            "payload": payload,
        }
        envelope = {
            **self.context.payload(),
            "observation_id": identifier,
            "observation_type": kind,
            "observed_at": at,
            "ninja_receipt_time": at,
            "provider_timestamp": at,
            "exchange_timestamp": at,
            "local_monotonic_sequence": local_sequence,
            "source_payload_hash": canonical_hash(payload),
        }
        proof = build_perpetual_observation_proof(
            wire=wire, source_envelope=envelope,
        )
        self.proofs.append(proof)
        return proof

    @staticmethod
    def _score(
        evidence: list[dict[str, object]], hypothesis: HypothesisKind,
    ) -> tuple[Decimal, dict[str, object]]:
        family_summary: dict[str, object] = {}
        balance = Decimal("0")
        positive = 0
        blocking = False
        for family in EvidenceFamily:
            values = [
                item for item in evidence
                if item["hypothesis_kind"] == hypothesis.value
                and item["family"] == family.value
            ]
            supports = max(
                (Decimal(str(item["strength"])) for item in values if item["supports"] is True),
                default=Decimal("0"),
            )
            contradictions = max(
                (Decimal(str(item["strength"])) for item in values if item["supports"] is False),
                default=Decimal("0"),
            )
            if supports > 0:
                positive += 1
            blocking = blocking or any(
                item["blocking"] is True and item["supports"] is False for item in values
            )
            balance += supports - contradictions
            family_summary[family.value] = {
                "support": str(supports),
                "contradiction": str(contradictions),
                "labels": sorted(str(item["label"]) for item in values),
            }
        score = max(Decimal("0"), min(Decimal("1"), Decimal("0.5") + balance / Decimal("10")))
        family_summary["positive_family_count"] = positive
        family_summary["blocking_contradiction"] = blocking
        return score, family_summary

    def _evidence(
        self, *, hypothesis: HypothesisKind, family: EvidenceFamily,
        source: dict[str, object], label: str, supports: bool,
    ) -> dict[str, object]:
        wire = source["wire"]
        observed_at = str(wire["ninja_receipt_time"])
        identifier = str(wire["observation_id"])
        sequence = int(wire["local_monotonic_sequence"])
        payload_hash = str(source["payload_sha256"])
        strength = (
            str(FIVE_MINUTE_PERPETUAL_PROFILE.policy.structural_strength)
            if family in {
                EvidenceFamily.STRUCTURAL_CONTEXT,
                EvidenceFamily.RESTING_LIQUIDITY,
            }
            else "0.5"
        )
        identity = {
            "policy_hash": FIVE_MINUTE_PERPETUAL_PROFILE.policy.configuration_hash,
            "hypothesis": hypothesis.value,
            "family": family.value,
            "label": label,
            "strength": strength,
            "supports": supports,
            "observed_at": observed_at,
            "sources": (identifier,),
            "sequences": (sequence,),
            "hashes": (payload_hash,),
            "session_kind": self.context.session_kind.value,
            "session_id": self.context.session_id,
            "trade_date": self.context.trade_date,
            "session_profile_hash": self.context.session_profile_hash,
            "session_generation": self.context.session_generation,
        }
        lifetime = {
            EvidenceFamily.STRUCTURAL_CONTEXT:
                FIVE_MINUTE_PERPETUAL_PROFILE.policy.structural_evidence_lifetime_seconds,
            EvidenceFamily.ORDER_FLOW:
                FIVE_MINUTE_PERPETUAL_PROFILE.policy.flow_evidence_lifetime_seconds,
            EvidenceFamily.RESTING_LIQUIDITY:
                FIVE_MINUTE_PERPETUAL_PROFILE.policy.liquidity_evidence_lifetime_seconds,
        }[family]
        expiry = (
            datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
            + timedelta(seconds=lifetime)
        ).isoformat().replace("+00:00", "Z")
        return PaperEvidence(
            deterministic_id("l3g-pe-", identity), hypothesis, family, label,
            Decimal(strength), supports, observed_at, expiry,
            (identifier,), (sequence,), (payload_hash,),
            session_kind=self.context.session_kind,
            session_id=self.context.session_id,
            trade_date=self.context.trade_date,
            session_profile_hash=self.context.session_profile_hash,
            session_generation=self.context.session_generation,
            source_session_ids=(self.context.session_id,),
        ).payload()

    def boundary_bundle(
        self, *, close: str, bias: str, prefix: str,
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        closed = datetime.fromisoformat(close.replace("Z", "+00:00"))
        opened = closed - timedelta(
            seconds=FIVE_MINUTE_PERPETUAL_PROFILE.policy.decision_interval_seconds,
        )
        quote_at = (closed - timedelta(seconds=3)).isoformat().replace("+00:00", "Z")
        trade_at = (closed - timedelta(seconds=2)).isoformat().replace("+00:00", "Z")
        depth_at = (closed - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
        quote = self.observation(
            f"{prefix}-quote", "QUOTE", quote_at,
            {
                "contract_id": "MNQ SEP26", "bid": "100", "ask": "100.25",
                "bid_size": 10, "ask_size": 10,
            },
        )
        trade = self.observation(
            f"{prefix}-trade", "TRADE", trade_at,
            {
                "contract_id": "MNQ SEP26", "price": "100.25", "size": 2,
                "aggressor_side": "UNKNOWN",
                "aggressor_source": "BID_ASK_CLASSIFICATION",
                "bid_at_trade": "100", "ask_at_trade": "100.25",
                "derivation_quote_observation_id": f"{prefix}-quote",
            },
        )
        depth = self.observation(
            f"{prefix}-depth", "DEPTH", depth_at,
            {
                "contract_id": "MNQ SEP26",
                "bids": [{"price": "100", "size": 10}],
                "asks": [{"price": "100.25", "size": 10}],
                "operation": "ADD", "side": "Bid", "mutation_price": "100",
                "mutation_volume": 10, "mutation_position": 0, "is_reset": False,
            },
        )
        evidence_specs = [
            (
                HypothesisKind.BULLISH_REVERSAL,
                EvidenceFamily.STRUCTURAL_CONTEXT,
                "RANGE_RECLAIM_UP", True, trade,
            ),
            (
                HypothesisKind.BEARISH_CONTINUATION,
                EvidenceFamily.STRUCTURAL_CONTEXT,
                "RANGE_RECLAIM_UP", False, trade,
            ),
            (
                HypothesisKind.BULLISH_REVERSAL,
                EvidenceFamily.ORDER_FLOW,
                "SELLING_WITHOUT_DOWNWARD_PROGRESS", True, trade,
            ),
            (
                HypothesisKind.BEARISH_CONTINUATION,
                EvidenceFamily.ORDER_FLOW,
                "AGGRESSIVE_SELL_IMBALANCE", True, trade,
            ),
            (
                HypothesisKind.BULLISH_REVERSAL,
                EvidenceFamily.RESTING_LIQUIDITY,
                "BID_REPLENISHMENT", True, depth,
            ),
        ]
        if bias == "TIE":
            evidence_specs.extend((
                (
                    HypothesisKind.BEARISH_CONTINUATION,
                    EvidenceFamily.STRUCTURAL_CONTEXT,
                    "RANGE_EXPANSION_DOWN", True, trade,
                ),
                (
                    HypothesisKind.BULLISH_REVERSAL,
                    EvidenceFamily.STRUCTURAL_CONTEXT,
                    "RANGE_EXPANSION_DOWN", False, trade,
                ),
                (
                    HypothesisKind.BEARISH_CONTINUATION,
                    EvidenceFamily.RESTING_LIQUIDITY,
                    "BID_LIQUIDITY_PULL", True, depth,
                ),
            ))
        evidence = [
            self._evidence(
                hypothesis=hypothesis, family=family, source=source,
                label=label, supports=supports,
            )
            for hypothesis, family, label, supports, source in evidence_specs
        ]
        bull, bull_families = self._score(evidence, HypothesisKind.BULLISH_REVERSAL)
        bear, bear_families = self._score(evidence, HypothesisKind.BEARISH_CONTINUATION)
        decision_at = (closed + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
        summary: dict[str, object] = {
            "candle_open_utc": opened.isoformat().replace("+00:00", "Z"),
            "candle_close_utc": close,
            "decision_observed_at": decision_at,
            "decision_latency_ms": 1000,
            "missed_boundary_count": 0,
            "decision_interval_seconds": (
                FIVE_MINUTE_PERPETUAL_PROFILE.policy.decision_interval_seconds
            ),
            "decision_clock": FIVE_MINUTE_PERPETUAL_PROFILE.policy.decision_clock,
            "startup_reconstruction": True,
            "decision_protocol": "EXIT_RECONCILE_THEN_ENTER",
            "prior_position": "FLAT",
            "signal_basis": "LATEST_AVAILABLE_PRE_CALLBACK_PROVISIONAL_EVIDENCE",
            "completed_interval_aggregate": False,
            "decision_reference_price": "100.25",
            "decision_reference_kind": "LAST_TRADE_BEFORE_BOUNDARY",
            "decision_reference_observation_id": f"{prefix}-trade",
            "decision_reference_observed_at": trade_at,
            "decision_reference_before_scheduled_boundary": True,
            "bullish_support": str(bull),
            "bearish_support": str(bear),
            "score_delta": str(bull - bear),
            "bullish_families": bull_families,
            "bearish_families": bear_families,
            "bias": bias,
            "action": "ENTER" if bias == "LONG" else "BLOCKED",
            "target_position": bias if bias == "LONG" else "FLAT",
        }
        if bias == "LONG":
            decision_kind = PaperDecisionKind.LONG
            direction = PaperDirection.LONG
            hypothesis: HypothesisKind | None = HypothesisKind.BULLISH_REVERSAL
            reason = "FIVE_MINUTE_ENTER_LONG"
            decision_sources = [trade, depth]
            relative_support = bull
        else:
            decision_kind = PaperDecisionKind.NO_TRADE
            direction = PaperDirection.FLAT
            hypothesis = None
            reason = "FIVE_MINUTE_BIAS_TIE_FLAT"
            decision_sources = [depth]
            relative_support = bull
        decision_sources.sort(key=lambda item: int(item["wire"]["local_monotonic_sequence"]))
        source_ids = tuple(str(item["wire"]["observation_id"]) for item in decision_sources)
        source_sequences = tuple(int(item["wire"]["local_monotonic_sequence"]) for item in decision_sources)
        source_hashes = tuple(str(item["payload_sha256"]) for item in decision_sources)
        expires = (closed + timedelta(seconds=31)).isoformat().replace("+00:00", "Z")
        decision_identity = {
            "policy_id": FIVE_MINUTE_PERPETUAL_PROFILE.policy.policy_id,
            "policy_hash": FIVE_MINUTE_PERPETUAL_PROFILE.policy.configuration_hash,
            "decision": decision_kind.value,
            "created_at": decision_at,
            "expires_at": expires,
            "hypothesis_kind": None if hypothesis is None else hypothesis.value,
            "direction": direction.value,
            "relative_support": str(relative_support),
            "family_summary": summary,
            "source_observation_ids": source_ids,
            "source_local_sequences": source_sequences,
            "source_payload_hashes": source_hashes,
            "sequence_authority": SequenceAuthority.LOCAL_CALLBACK_ORDER_ONLY.value,
            "book_completeness": BookCompleteness.UNVERIFIED.value,
            "scientific_eligibility": False,
            "reason_code": reason,
            "session_kind": self.context.session_kind.value,
            "session_id": self.context.session_id,
            "trade_date": self.context.trade_date,
            "session_profile_hash": self.context.session_profile_hash,
            "session_generation": self.context.session_generation,
        }
        decision = PaperDecision(
            deterministic_id("l3g-pd-", decision_identity),
            FIVE_MINUTE_PERPETUAL_PROFILE.policy.policy_id,
            FIVE_MINUTE_PERPETUAL_PROFILE.policy.configuration_hash,
            decision_kind, decision_at, expires, hypothesis, direction, relative_support,
            summary, source_ids, source_sequences, source_hashes,
            SequenceAuthority.LOCAL_CALLBACK_ORDER_ONLY, BookCompleteness.UNVERIFIED,
            False, reason, self.context.session_kind, self.context.session_id,
            self.context.trade_date, self.context.session_profile_hash,
            self.context.session_generation,
        ).payload()
        bundle_ids = sorted((
            str(quote["wire"]["observation_id"]),
            str(trade["wire"]["observation_id"]),
            str(depth["wire"]["observation_id"]),
        ))
        bundle = build_perpetual_boundary_bundle(
            candle_open_utc=opened.isoformat().replace("+00:00", "Z"),
            candle_close_utc=close, decision_observed_at=decision_at, bias=bias,
            fallback_observation_id=str(depth["wire"]["observation_id"]),
            session_context=self.context.payload(), evidence=evidence,
            decision=decision, source_observation_ids=bundle_ids,
        )
        return bundle, [quote, trade, depth]

    def complete(self) -> tuple[dict[str, object], dict[str, object]]:
        prior, _ = self.boundary_bundle(
            close="2026-09-08T14:09:00Z", bias="LONG", prefix="prior",
        )
        middle, _ = self.boundary_bundle(
            close="2026-09-08T14:09:30Z", bias="TIE", prefix="middle",
        )
        latest, _ = self.boundary_bundle(
            close="2026-09-08T14:10:00Z", bias="TIE", prefix="latest",
        )
        core = build_perpetual_startup_seed_core(
            operation_id=self.operation_id,
            created_at="2026-09-08T14:10:01Z",
            source_ledger={
                "path": self.ledger_path,
                "ledger_identity": self.ledger_identity,
                "ledger_epoch": self.ledger_epoch,
            },
            current_five_minute_boundary_utc="2026-09-08T14:10:00Z",
            latest_completed=latest,
            latest_non_tied=prior,
            boundary_chain=[prior, middle, latest],
            observations=self.proofs,
        )
        export_payload = {
            **perpetual_startup_seed_export_payload(core),
            "session_family": self.context.session_family.value,
        }
        export = self._record(
            PERPETUAL_STARTUP_SEED_EXPORT_KIND, export_payload,
            identity=perpetual_startup_seed_export_identity(core),
            occurred_at="2026-09-08T14:10:02Z",
        )
        artifact = write_perpetual_startup_seed_artifact(
            self.root / "seed.json", core=core, export_record=export,
        )
        return core, artifact

    def shutdown_receipt(self, artifact: dict[str, object]) -> dict[str, object]:
        export = artifact["export_record"]
        return {
            "schema": "l3g-ledger-controlled-shutdown-v1",
            "closed_at": "2026-09-08T14:10:03Z",
            "clean_shutdown": True,
            "admission_sealed": True,
            "writer_stopped": True,
            "checkpoint": {"complete": True},
            "expected_tip_sequence": export["ledger_sequence"],
            "expected_tip_hash": export["record_hash"],
            "durable_tip_sequence": export["ledger_sequence"],
            "durable_tip_hash": export["record_hash"],
            "risk_continuity_boundary": {
                "path": self.ledger_path,
                "ledger_identity": self.ledger_identity,
                "ledger_epoch": self.ledger_epoch,
                "risk_boundary_sequence": 0,
                "risk_boundary_hash": None,
            },
            "verifier_shutdown": {"completed": True},
            "runtime_watchdog_shutdown": {"completed": True},
        }

    def verification_report(self, artifact: dict[str, object]) -> dict[str, object]:
        export = artifact["export_record"]
        return {
            "verification_id": "lv-" + "3" * 32,
            "completed_at": "2026-09-08T14:10:04Z",
            "status": "PASS",
            "verification_mode": "full",
            "chain_valid": True,
            "quick_check": "ok",
            "ledger_path": self.ledger_path,
            "ledger_identity": self.ledger_identity,
            "ledger_epoch": self.ledger_epoch,
            "verified_through_sequence": export["ledger_sequence"],
            "tip_hash": export["record_hash"],
        }


class PerpetualStartupSeedTests(unittest.TestCase):
    def test_seed_and_shutdown_proof_round_trip_exclusively(self) -> None:
        with TemporaryDirectory() as folder:
            fixture = _SeedFixture(Path(folder))
            core, artifact = fixture.complete()
            self.assertEqual(artifact["core"], core)
            loaded = read_perpetual_startup_seed_artifact(
                fixture.root / "seed.json", operation_id=fixture.operation_id,
                expected_at="2026-09-08T14:10:29Z",
            )
            self.assertEqual(loaded, artifact)
            self.assertEqual(loaded["core"]["latest_completed"]["bias"], "TIE")
            self.assertEqual(loaded["core"]["latest_non_tied"]["bias"], "LONG")
            self.assertEqual(
                [item["bias"] for item in loaded["core"]["boundary_chain"]],
                ["LONG", "TIE", "TIE"],
            )
            self.assertEqual(
                loaded["core"]["latest_non_tied"],
                loaded["core"]["boundary_chain"][0],
            )
            self.assertEqual(
                loaded["core"]["latest_completed"],
                loaded["core"]["boundary_chain"][-1],
            )
            export_payload = perpetual_startup_seed_export_payload(loaded["core"])
            self.assertEqual(export_payload["boundary_chain_count"], 3)
            self.assertEqual(
                export_payload["boundary_chain_sha256"],
                _digest([
                    item["bundle_sha256"]
                    for item in loaded["core"]["boundary_chain"]
                ]),
            )
            self.assertEqual(len(loaded["core"]["observations"]), 9)
            with self.assertRaises(FileExistsError):
                write_perpetual_startup_seed_artifact(
                    fixture.root / "seed.json", core=core,
                    export_record=artifact["export_record"],
                )

            proof = write_perpetual_startup_seed_proof(
                fixture.root / "proof.json",
                artifact=artifact,
                manifest_sha256="4" * 64,
                shutdown_receipt=fixture.shutdown_receipt(artifact),
                verification_report=fixture.verification_report(artifact),
                created_at="2026-09-08T14:10:05Z",
            )
            self.assertEqual(
                proof["bound_source_max_sequence"],
                artifact["export_record"]["ledger_sequence"],
            )
            self.assertEqual(
                read_perpetual_startup_seed_proof(
                    fixture.root / "proof.json", artifact=artifact,
                    operation_id=fixture.operation_id,
                    expected_at="2026-09-08T14:10:20Z",
                ),
                proof,
            )
            with self.assertRaises(FileExistsError):
                write_perpetual_startup_seed_proof(
                    fixture.root / "proof.json", artifact=artifact,
                    manifest_sha256="4" * 64,
                    shutdown_receipt=fixture.shutdown_receipt(artifact),
                    verification_report=fixture.verification_report(artifact),
                    created_at="2026-09-08T14:10:05Z",
                )

    def test_core_refuses_stale_identity_tamper_and_missing_transitive_quote(self) -> None:
        with TemporaryDirectory() as folder:
            fixture = _SeedFixture(Path(folder))
            core, _artifact = fixture.complete()
            with self.assertRaisesRegex(RuntimeError, "BOUNDARY_STALE"):
                validate_perpetual_startup_seed_core(
                    core, expected_at="2026-09-08T14:10:30Z",
                )

            wrong_profile = deepcopy(core)
            wrong_profile["target_profile"]["policy_sha256"] = "9" * 64
            _rehash(wrong_profile, "seed_core_sha256")
            with self.assertRaisesRegex(RuntimeError, "CORE_INVALID"):
                validate_perpetual_startup_seed_core(wrong_profile)

            wrong_account = deepcopy(core)
            wrong_account["account_binding"]["account_name"] = "Live"
            _rehash(wrong_account, "seed_core_sha256")
            with self.assertRaisesRegex(RuntimeError, "CORE_INVALID"):
                validate_perpetual_startup_seed_core(wrong_account)

            bad_evidence = deepcopy(core)
            bad_latest = deepcopy(bad_evidence["latest_completed"])
            bad_latest["evidence"][0]["evidence_id"] = (
                "l3g-pe-" + "a" * 32
            )
            _rehash(bad_latest, "bundle_sha256")
            bad_evidence["latest_completed"] = bad_latest
            bad_evidence["boundary_chain"][-1] = deepcopy(bad_latest)
            _rehash(bad_evidence, "seed_core_sha256")
            with self.assertRaisesRegex(RuntimeError, "EVIDENCE_ID_INVALID"):
                validate_perpetual_startup_seed_core(bad_evidence)

            missing_quote = deepcopy(core)
            missing_quote["observations"] = [
                item for item in missing_quote["observations"]
                if item["wire"]["observation_id"] != "latest-quote"
            ]
            incomplete_latest = deepcopy(missing_quote["latest_completed"])
            incomplete_latest["source_observation_ids"].remove("latest-quote")
            _rehash(incomplete_latest, "bundle_sha256")
            missing_quote["latest_completed"] = incomplete_latest
            missing_quote["boundary_chain"][-1] = deepcopy(incomplete_latest)
            _rehash(missing_quote, "seed_core_sha256")
            with self.assertRaisesRegex(RuntimeError, "PROVENANCE_INCOMPLETE"):
                validate_perpetual_startup_seed_core(missing_quote)

    def test_core_requires_complete_ordered_boundary_chain(self) -> None:
        with TemporaryDirectory() as folder:
            fixture = _SeedFixture(Path(folder))
            core, _artifact = fixture.complete()

            missing_chain = deepcopy(core)
            del missing_chain["boundary_chain"]
            _rehash(missing_chain, "seed_core_sha256")
            with self.assertRaisesRegex(RuntimeError, "CORE_INVALID"):
                validate_perpetual_startup_seed_core(missing_chain)

            gapped = deepcopy(core)
            gapped["boundary_chain"] = [
                gapped["boundary_chain"][0], gapped["boundary_chain"][-1],
            ]
            _rehash(gapped, "seed_core_sha256")
            with self.assertRaisesRegex(RuntimeError, "BOUNDARY_CHAIN_INVALID"):
                validate_perpetual_startup_seed_core(gapped)

            reordered = deepcopy(core)
            reordered["boundary_chain"][1:] = reversed(
                reordered["boundary_chain"][1:]
            )
            _rehash(reordered, "seed_core_sha256")
            with self.assertRaisesRegex(RuntimeError, "BOUNDARY_CHAIN_INVALID"):
                validate_perpetual_startup_seed_core(reordered)

            wrong_non_tied_endpoint = deepcopy(core)
            wrong_non_tied_endpoint["latest_non_tied"] = deepcopy(
                wrong_non_tied_endpoint["boundary_chain"][1]
            )
            _rehash(wrong_non_tied_endpoint, "seed_core_sha256")
            with self.assertRaisesRegex(RuntimeError, "BOUNDARY_CHAIN_INVALID"):
                validate_perpetual_startup_seed_core(wrong_non_tied_endpoint)

            missing_middle_provenance = deepcopy(core)
            missing_middle_provenance["observations"] = [
                item for item in missing_middle_provenance["observations"]
                if item["wire"]["observation_id"] != "middle-quote"
            ]
            _rehash(missing_middle_provenance, "seed_core_sha256")
            with self.assertRaisesRegex(RuntimeError, "PROVENANCE_INCOMPLETE"):
                validate_perpetual_startup_seed_core(missing_middle_provenance)

            directional_fixture = _SeedFixture(Path(folder) / "directional")
            first, _ = directional_fixture.boundary_bundle(
                close="2026-09-08T14:04:30Z", bias="LONG", prefix="first",
            )
            later, _ = directional_fixture.boundary_bundle(
                close="2026-09-08T14:05:00Z", bias="LONG", prefix="later",
            )
            with self.assertRaisesRegex(RuntimeError, "BOUNDARY_CHAIN_INVALID"):
                build_perpetual_startup_seed_core(
                    operation_id=directional_fixture.operation_id,
                    created_at="2026-09-08T14:05:01Z",
                    source_ledger={
                        "path": directional_fixture.ledger_path,
                        "ledger_identity": directional_fixture.ledger_identity,
                        "ledger_epoch": directional_fixture.ledger_epoch,
                    },
                    current_five_minute_boundary_utc="2026-09-08T14:05:00Z",
                    latest_completed=later,
                    latest_non_tied=first,
                    boundary_chain=[first, later],
                    observations=directional_fixture.proofs,
                )

    def test_artifact_and_proof_fail_closed_on_integrity_or_unproven_tip(self) -> None:
        with TemporaryDirectory() as folder:
            fixture = _SeedFixture(Path(folder))
            _core, artifact = fixture.complete()
            damaged = deepcopy(artifact)
            damaged["artifact_sha256"] = "0" * 64
            (fixture.root / "damaged.json").write_text(
                json.dumps(damaged), encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "ARTIFACT_INTEGRITY_FAILED"):
                read_perpetual_startup_seed_artifact(fixture.root / "damaged.json")

            wrong_export = deepcopy(artifact)
            export_wrapper = wrong_export["export_record"]
            export_record = export_wrapper["record"]
            export_record["payload"]["target_policy_sha256"] = "7" * 64
            export_record["record_hash"] = canonical_hash({
                key: item for key, item in export_record.items() if key != "record_hash"
            })
            export_wrapper["record_hash"] = export_record["record_hash"]
            _rehash(wrong_export, "artifact_sha256")
            (fixture.root / "wrong-export.json").write_text(
                json.dumps(wrong_export), encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "EXPORT_RECORD_INVALID"):
                read_perpetual_startup_seed_artifact(fixture.root / "wrong-export.json")

            bad_shutdown = fixture.shutdown_receipt(artifact)
            bad_shutdown["durable_tip_sequence"] = (
                int(artifact["export_record"]["ledger_sequence"]) - 1
            )
            with self.assertRaisesRegex(RuntimeError, "SHUTDOWN_UNPROVEN"):
                write_perpetual_startup_seed_proof(
                    fixture.root / "bad-shutdown.json", artifact=artifact,
                    manifest_sha256="4" * 64, shutdown_receipt=bad_shutdown,
                    verification_report=fixture.verification_report(artifact),
                    created_at="2026-09-08T14:10:05Z",
                )

            incremental = fixture.verification_report(artifact)
            incremental["verification_mode"] = "incremental"
            with self.assertRaisesRegex(RuntimeError, "SOURCE_VERIFICATION_FAILED"):
                write_perpetual_startup_seed_proof(
                    fixture.root / "bad-verification.json", artifact=artifact,
                    manifest_sha256="4" * 64,
                    shutdown_receipt=fixture.shutdown_receipt(artifact),
                    verification_report=incremental,
                    created_at="2026-09-08T14:10:05Z",
                )

            wrong_tip = fixture.verification_report(artifact)
            wrong_tip["tip_hash"] = "8" * 64
            with self.assertRaisesRegex(RuntimeError, "SOURCE_VERIFICATION_FAILED"):
                write_perpetual_startup_seed_proof(
                    fixture.root / "wrong-tip.json", artifact=artifact,
                    manifest_sha256="4" * 64,
                    shutdown_receipt=fixture.shutdown_receipt(artifact),
                    verification_report=wrong_tip,
                    created_at="2026-09-08T14:10:05Z",
                )

            proof = write_perpetual_startup_seed_proof(
                fixture.root / "valid-proof.json", artifact=artifact,
                manifest_sha256="4" * 64,
                shutdown_receipt=fixture.shutdown_receipt(artifact),
                verification_report=fixture.verification_report(artifact),
                created_at="2026-09-08T14:10:05Z",
            )
            proof["proof_sha256"] = "0" * 64
            (fixture.root / "damaged-proof.json").write_text(
                json.dumps(proof), encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "PROOF_INTEGRITY_FAILED"):
                read_perpetual_startup_seed_proof(
                    fixture.root / "damaged-proof.json", artifact=artifact,
                )


if __name__ == "__main__":
    unittest.main()
