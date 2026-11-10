from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from scripts.lane_ii_continuous import cohorts, hypotheses, research


def registration(stage="discovery"):
    result = {"hypothesis_id": "slim-sizing", "version": 1, "family_id": "slim",
        "question": "Does the registered slim sizing improve after-cost block returns?", "stage": stage,
        "registered_at_utc": "2026-11-08T12:00:00+00:00", "candidate_sha256": "a" * 64,
        "universe_sha256": "b" * 64, "membership_sha256": "c" * 64, "config_sha256": "d" * 64,
        "control_sha256": {"cash": "e" * 64, "matched": "f" * 64}, "trial_budget": 2,
        "parameter_grid": {"budget": [5.0, 6.0]}}
    if stage == "confirmation":
        result.update(confirmation_start_utc="2026-11-09T00:00:00+00:00", confirmation_end_utc="2026-11-10T00:00:00+00:00",
            entry_exit_size_rules="frozen source", cost_model="recorded fees and depth", contract_semantics="exact typed terms",
            primary_metric="net per independent campaign", coverage_requirements="full registered interval", rejection_rule="registered lower-bound rule",
            sample_plan={"minimum_independent_blocks": 5, "unit": "campaign",
                "discovery_variance_sha256": "1" * 64, "minimum_effect": .1, "look_policy": "FIXED_END_ONLY",
                "multiplicity": "BONFERRONI", "family_comparisons": 4, "alpha": .05,
                "tested_contrasts": hypotheses.SUPPORTED_CONTRASTS.copy(),
                "family_parameter_sha256": hypotheses.registered_parameter_hashes(result),
                "resampling_method": "INDEPENDENT_BLOCK_BOOTSTRAP", "seed": 42, "resamples": 199})
        result["sample_plan"]["power_calculation"] = {
            "schema": "LANE_II_REGISTERED_POWER_V1", "method": "INDEPENDENT_BLOCK_BOOTSTRAP_POWER", "status": "PASS",
            "result_sha256": "2" * 64, "source_variance_sha256": "1" * 64,
            "target_power": .8, "estimated_power": .85, "required_independent_blocks": 5,
            "required_window_seconds": 86400, "minimum_effect": .1, "alpha": .05,
            "family_comparisons": 4, "inference_resamples": 199}
    return result


def review_evidence(spec):
    blocks = [{"block_id": f"campaign-{i}", "market_ids": [f"market-{i}"],
               "net": 3.0, "baseline_net": 1.0, "stress_net": 1.0} for i in range(6)]
    analysis = research.analyze_blocks(blocks, spec["sample_plan"], registration_sha256=hypotheses.document_hash(spec), resamples=199)
    result = {key: True for key in ("complete_evidence", "accounting_reconciled", "full_planned_coverage", "untouched_confirmation",
        "causal_inputs_verified", "contract_semantics_verified", "block_independence_verified", "family_trial_inventory_complete",
        "stress_at_proposed_size_verified", "concentration_review_passed", "registered_regime_coverage_met", "source_and_config_unchanged",
        "registered_power_plan_verified")}
    result.update(registration_sha256=hypotheses.document_hash(spec), historical_positive_selection=False, execution_state="completed",
        unresolved_basis=0, unresolved_positions=0, allowed_look="REGISTERED_FINAL_LOOK", look_number=1,
        observed_start_utc=spec["confirmation_start_utc"], observed_end_utc=spec["confirmation_end_utc"],
        family_parameter_sha256=spec["sample_plan"]["family_parameter_sha256"],
        power_result_sha256=spec["sample_plan"]["power_calculation"]["result_sha256"],
        block_analysis=analysis, operational_readiness_pass=True)
    return result


class RegistryTests(unittest.TestCase):
    def test_duplicate_dispatch_does_not_spend_another_trial(self):
        reg = hypotheses.register_hypothesis(hypotheses.new_registry(), registration())
        first = hypotheses.register_variant(reg, "slim-sizing", 1, {"budget": 5.0}, created_at_utc="2026-11-08T12:01:00Z")
        duplicate = hypotheses.register_variant(first, "slim-sizing", 1, {"budget": 5.0}, created_at_utc="2026-11-08T12:02:00Z")
        self.assertEqual(first, duplicate)
        self.assertEqual(len(first["trials"]), 1)
        with self.assertRaisesRegex(ValueError, "UNREGISTERED_PARAMETER"):
            hypotheses.register_variant(first, "slim-sizing", 1, {"budget": 7.0}, created_at_utc="2026-11-08T12:03:00Z")

    def test_frozen_design_and_negative_result_cannot_be_rewritten(self):
        spec = registration()
        reg = hypotheses.register_hypothesis(hypotheses.new_registry(), spec)
        changed = deepcopy(spec); changed["parameter_grid"]["budget"] = [5.0, 7.0]
        with self.assertRaisesRegex(ValueError, "CANNOT_CHANGE"):
            hypotheses.register_hypothesis(reg, changed)
        renamed = deepcopy(spec); renamed.update(hypothesis_id="renamed", family_id="renamed-family")
        with self.assertRaisesRegex(ValueError, "DUPLICATE_RESEARCH_DESIGN"):
            hypotheses.register_hypothesis(reg, renamed)
        reg = hypotheses.register_variant(reg, "slim-sizing", 1, {"budget": 5.0}, created_at_utc="2026-11-08T12:01:00Z")
        trial = next(iter(reg["trials"]))
        reg = hypotheses.record_result(reg, trial, "rejected", "2" * 64, as_of_utc="2026-11-08T14:00:00Z")
        with self.assertRaisesRegex(ValueError, "RESULT_IS_IMMUTABLE"):
            hypotheses.record_result(reg, trial, "research_pass", "3" * 64, as_of_utc="2026-11-08T14:01:00Z")
        self.assertEqual(reg["trials"][trial]["status"], "rejected")

    def test_grid_and_registry_limits(self):
        spec = registration(); spec["trial_budget"] = 1
        with self.assertRaisesRegex(ValueError, "GRID_EXCEEDS"):
            hypotheses.validate_registration(spec)
        reg = hypotheses.register_hypothesis(hypotheses.new_registry(1), registration())
        other = registration(); other.update(hypothesis_id="different", candidate_sha256="9" * 64)
        with self.assertRaisesRegex(ValueError, "REGISTRY_FULL"):
            hypotheses.register_hypothesis(reg, other)

    def test_confirmation_is_frozen_before_first_observation(self):
        spec = registration("confirmation")
        spec["registered_at_utc"] = spec["confirmation_start_utc"]
        with self.assertRaisesRegex(ValueError, "PRECEDE"):
            hypotheses.validate_registration(spec)
        spec = registration("confirmation"); spec["sample_plan"].pop("power_calculation")
        with self.assertRaisesRegex(ValueError, "POWER"):
            hypotheses.validate_registration(spec)

    def test_large_grid_requires_complete_family_multiplicity(self):
        spec = registration("confirmation")
        spec.update(parameter_grid={"budget": [5, 6], "delay": list(range(50))}, trial_budget=100)
        spec["sample_plan"]["family_parameter_sha256"] = hypotheses.registered_parameter_hashes(spec)
        spec["sample_plan"]["family_comparisons"] = 2
        with self.assertRaisesRegex(ValueError, "FAMILY_CORRECTION"):
            hypotheses.validate_registration(spec)
        spec["sample_plan"]["family_comparisons"] = 200
        spec["sample_plan"]["resamples"] = 4999
        spec["sample_plan"]["power_calculation"].update(family_comparisons=200, inference_resamples=4999)
        hypotheses.validate_registration(spec)
        spec["sample_plan"]["family_parameter_sha256"].pop()
        with self.assertRaisesRegex(ValueError, "COMPLETE_FROZEN_PARAMETER_INVENTORY"):
            hypotheses.validate_registration(spec)

    def test_confirmation_requires_structured_power_and_resolvable_tail(self):
        spec = registration("confirmation")
        spec["sample_plan"]["power_calculation"] = "unverified rationale"
        with self.assertRaisesRegex(ValueError, "STRUCTURED_SOURCE_BOUND_POWER"):
            hypotheses.validate_registration(spec)
        spec = registration("confirmation")
        spec["sample_plan"]["power_calculation"]["required_independent_blocks"] = 100
        with self.assertRaisesRegex(ValueError, "POWER_DERIVED_SUPPORT"):
            hypotheses.validate_registration(spec)
        spec = registration("confirmation")
        spec["sample_plan"]["alpha"] = .001
        with self.assertRaisesRegex(ValueError, "INSUFFICIENT_BOOTSTRAP_TAIL_RESOLUTION"):
            hypotheses.validate_registration(spec)


class EvidenceTests(unittest.TestCase):
    def test_capital_review_positive_fixture_still_has_no_live_authority(self):
        spec = registration("confirmation")
        result = research.assess_capital_candidate(spec, review_evidence(spec))
        self.assertTrue(result["capital_review_eligible"])
        self.assertFalse(result["funded_authority"])

    def test_sparse_selected_failed_or_unresolved_samples_cannot_promote(self):
        spec = registration("confirmation")
        for key, value in (("historical_positive_selection", True), ("execution_state", "failed"),
                           ("unresolved_basis", 1), ("look_number", 2), ("block_independence_verified", False)):
            with self.subTest(key=key):
                evidence = review_evidence(spec); evidence[key] = value
                self.assertFalse(research.assess_capital_candidate(spec, evidence)["capital_review_eligible"])
        spec["sample_plan"]["minimum_independent_blocks"] = 20
        self.assertFalse(research.assess_capital_candidate(spec, review_evidence(spec))["capital_review_eligible"])

    def test_stress_loss_and_analysis_tampering_fail_gate(self):
        spec = registration("confirmation")
        evidence = review_evidence(spec)
        evidence["block_analysis"]["realized_net"] += 5
        self.assertIn("FROZEN_BLOCK_ANALYSIS_BINDING_REQUIRED", research.assess_capital_candidate(spec, evidence)["reasons"])
        evidence = review_evidence(spec)
        evidence["block_analysis"]["stress_net"] = -1
        evidence["block_analysis"].pop("analysis_sha256")
        evidence["block_analysis"]["analysis_sha256"] = hypotheses.document_hash(evidence["block_analysis"])
        self.assertFalse(research.assess_capital_candidate(spec, evidence)["capital_review_eligible"])

    def test_confirmation_requires_exact_fixed_window(self):
        spec = registration("confirmation")
        for start, end in (("2026-11-09T01:00:00Z", "2026-11-10T01:00:00Z"),
                           ("2026-11-09T00:00:00Z", "2026-11-11T00:00:00Z"),
                           ("2026-11-10T00:00:00Z", "2026-11-09T00:00:00Z"),
                           ("2026-11-09T00:00:00", "2026-11-10T00:00:00Z")):
            with self.subTest(start=start, end=end):
                evidence = review_evidence(spec)
                evidence.update(observed_start_utc=start, observed_end_utc=end)
                self.assertFalse(research.assess_capital_candidate(spec, evidence)["capital_review_eligible"])

    def test_frozen_resampling_and_inventory_cannot_be_overridden(self):
        spec = registration("confirmation")
        blocks = [{"block_id": str(i), "market_ids": [str(i)], "net": 2, "baseline_net": 0, "stress_net": 1} for i in range(5)]
        with self.assertRaisesRegex(ValueError, "REGISTERED_RESAMPLING_COUNT"):
            research.analyze_blocks(blocks, spec["sample_plan"], registration_sha256="a" * 64, resamples=299)
        for key, value in (("resamples", 299), ("family_comparisons", 2)):
            evidence = review_evidence(spec)
            evidence["block_analysis"][key] = value
            evidence["block_analysis"].pop("analysis_sha256")
            evidence["block_analysis"]["analysis_sha256"] = hypotheses.document_hash(evidence["block_analysis"])
            self.assertIn("FROZEN_INFERENCE_PARAMETERS_REQUIRED", research.assess_capital_candidate(spec, evidence)["reasons"])
        evidence = review_evidence(spec)
        evidence["family_parameter_sha256"] = evidence["family_parameter_sha256"][:1]
        self.assertIn("FROZEN_FAMILY_INVENTORY_BINDING_REQUIRED", research.assess_capital_candidate(spec, evidence)["reasons"])

    def test_power_result_requires_external_verification_and_matching_source(self):
        spec = registration("confirmation")
        for key, value in (("registered_power_plan_verified", False), ("power_result_sha256", "9" * 64)):
            evidence = review_evidence(spec)
            evidence[key] = value
            self.assertFalse(research.assess_capital_candidate(spec, evidence)["capital_review_eligible"])

    def test_shared_market_blocks_refused_and_resampling_reproducible(self):
        spec = registration("confirmation")
        blocks = [{"block_id": str(i), "market_ids": [str(i)], "net": i + 1, "baseline_net": 0, "stress_net": 1} for i in range(5)]
        one = research.analyze_blocks(blocks, spec["sample_plan"], registration_sha256="a" * 64, resamples=199)
        self.assertEqual(one, research.analyze_blocks(blocks, spec["sample_plan"], registration_sha256="a" * 64, resamples=199))
        blocks[1]["market_ids"] = ["0"]
        with self.assertRaisesRegex(ValueError, "NONOVERLAPPING"):
            research.analyze_blocks(blocks, spec["sample_plan"], registration_sha256="a" * 64, resamples=199)

    def test_replay_financial_counts_do_not_override_missing_decision_parity(self):
        result = {"source_state": "STOPPED", "source_profile_count": 2, "replayed_original_profile_count": 2,
                  "source_profiles_with_financial_parity": 2, "source_profiles_with_recorded_decision_parity": 2,
                  "parity": {"a": {"pass": True}, "b": {"pass": True}},
                  "recorded_decision_parity": {"a": {"pass": True}, "b": {"pass": False}}}
        self.assertFalse(research.assess_replay_results(result)["all_requested_parity_passed"])
        result["recorded_decision_parity"]["b"]["pass"] = True
        checked = research.assess_replay_results(result)
        self.assertTrue(checked["all_requested_parity_passed"])
        self.assertEqual(checked["source_execution_state"], "STOPPED")
        self.assertFalse(checked["planned_window_completion_inferred"])

    def test_replay_planner_does_not_relabel_failed_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); candidate = root / "candidates/c1"; study = root / "runs/r1/study"
            candidate.mkdir(parents=True); (study / "capture").mkdir(parents=True)
            (candidate / "SOURCE_MANIFEST.json").write_text("{}")
            (study / "PAPER_RUN_RESULT.json").write_text(json.dumps({"capture_audit": {"status": "PASS_RETAINED_NEXT12_CAPTURE_INTEGRITY_ONLY", "problems": []}}))
            (study / "exit.json").write_text("{}")
            (study / "operator.json").write_text(json.dumps({"command": [str(root / "python.exe")]}))
            (study / "capture/receipt.json").write_text(json.dumps({"state": "FAILED", "reason": "TOTAL_HTTP_TRANSPORT_TIMEOUT"}))
            (study / "capture/COLLECTION_CLOSED.json").write_text("{}")
            plan = research.build_replay_commands(root, root / "python.exe", candidate=candidate, study=study, output=root / "output")
            self.assertEqual(plan["state"], "BLOCKED")
            self.assertEqual(plan["commands"], [])
            self.assertIn("EXISTING_REPLAY_ADAPTER_REFUSES_FAILED_OR_UNSEALED_CAPTURE", plan["reasons"])


class CohortTests(unittest.TestCase):
    def test_import_preserves_canonical_ids_and_inactive_members_without_admission(self):
        rows = [{"wallet": "0x" + format(i, "040x"), "name": "Bossgirl" if i == 0 else "Bonereaper" if i == 1 else f"wallet-{i}",
                 "cohort": "PRIMARY" if i < 8 else "PROBATION", "active": i != 1} for i in range(10)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "roster.json"
            raw = json.dumps({"wallets": rows, "scope": "btc15"}).encode(); path.write_bytes(raw)
            reg = cohorts.freeze_wallet_registry(path, hashlib.sha256(raw).hexdigest())
            self.assertEqual([r["wallet"] for r in reg["members"]], [r["wallet"] for r in rows])
            self.assertEqual(len(reg["members"]), 10)
            self.assertFalse(any(r["copying_eligible"] for r in reg["members"]))
            self.assertFalse(reg["members"][1]["historical_active"])
            with self.assertRaisesRegex(ValueError, "HASH_MISMATCH"):
                cohorts.freeze_wallet_registry(path, "f" * 64)

    def test_cross_venue_copy_requires_exact_equivalence(self):
        evidence = {key: True for key in cohorts.REQUIRED_EVIDENCE}
        evidence.update(observation_state="ACTIVE", source_manifest_sha256="a" * 64, cross_venue=True,
                        observed_at_utc="2026-11-08T12:00:00Z", evaluated_at_utc="2026-11-08T12:00:10Z", maximum_source_age_seconds=30)
        self.assertFalse(cohorts.evaluate_wallet_eligibility(evidence)["paper_copying_eligible"])
        evidence["exact_equivalence_verified"] = True
        self.assertTrue(cohorts.evaluate_wallet_eligibility(evidence)["paper_copying_eligible"])
        evidence["observation_state"] = "SOURCE_UNAVAILABLE"
        self.assertFalse(cohorts.evaluate_wallet_eligibility(evidence)["paper_copying_eligible"])

    def test_stale_or_missing_wallet_clock_cannot_admit_copying(self):
        evidence = {key: True for key in cohorts.REQUIRED_EVIDENCE}
        evidence.update(observation_state="ACTIVE", source_manifest_sha256="a" * 64)
        self.assertIn("current_observation_clock", cohorts.evaluate_wallet_eligibility(evidence)["missing_requirements"])
        evidence.update(observed_at_utc="2026-11-08T12:00:00Z", evaluated_at_utc="2026-11-08T12:02:00Z", maximum_source_age_seconds=30)
        self.assertFalse(cohorts.evaluate_wallet_eligibility(evidence)["paper_copying_eligible"])


class CloseoutAnalysisTests(unittest.TestCase):
    def artifacts(self, directory):
        root = Path(directory)
        source = root / "capture"
        source.mkdir()
        def write(path, document):
            raw = json.dumps(document, sort_keys=True).encode()
            path.write_bytes(raw)
            return {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
        account = {"id": "5m-P13", "entries": 1, "cash_exits": 1, "settlements": 0,
                   "realized_pnl": -2, "modeled_fees": 3, "unresolved_basis": 0, "unresolved_positions": 0}
        receipt = {"state": "FAILED", "reason": "TOTAL_HTTP_TRANSPORT_TIMEOUT", "profiles": [account],
                   "paper_start_utc": "2026-11-08T12:00:00Z", "cutoff_utc": "2026-11-08T13:00:00Z",
                   "actual_end_utc": "2026-11-08T12:30:00Z", "real_orders": 0, "model_api_calls": 0,
                   "authenticated_account_calls": 0}
        terminal = write(source / "receipt.json", receipt)
        audit = {"schema": "LANE_II_READONLY_CAPTURE_AUDIT_V1", "status": "PASS_RETAINED_CAPTURE_INTEGRITY_ONLY",
                 "capture_dir": str(source), "input_bindings": [terminal], "problem_counts": {}, "execution": {"state": "FAILED"}}
        cohort_account = {"wallet": "0x" + "1" * 40, "policy": "fixed", "entries": 1, "cash_exits": 0,
                          "settlements": 0, "realized_pnl": 0, "modeled_fees": .1,
                          "unresolved_basis": 5, "unresolved_positions": 1}
        accounting = {"schema": "LANE_II_RETAINED_CLOSEOUT_V1", "source_root": str(source), "input_bindings": [terminal],
                      "components": {
                          "study": {"execution": {"state": "failed"}, "coverage": {},
                                    "accounting": {"status": "PASS_RETAINED_PROFILE_ACCOUNTING", "accounts": [account]}},
                          "cohort": {"execution": {"state": "failed", "reason": "INTERRUPTED_WITHOUT_TERMINAL_RECEIPT"},
                                     "coverage": {"planned_seconds": 3600, "observed_seconds": 1600},
                                     "accounting": {"status": "PASS_RETAINED_COHORT_ACCOUNTING", "accounts": [cohort_account]}}}}
        evidence = {"artifacts": {"terminal_receipt": terminal,
                     "raw_audit": write(root / "audit.json", audit), "accounting": write(root / "accounting.json", accounting)},
                    "source_sha256": "a" * 64, "config_sha256": "b" * 64}
        return evidence, write

    def test_bound_failed_closeout_preserves_alternatives_and_open_basis(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence, _ = self.artifacts(directory)
            output = Path(directory) / "analysis"
            delivered = research.analyze_closeout(evidence, output)
            report = delivered["result"]
            self.assertEqual(report["components"]["study"]["execution"]["state"], "failed")
            self.assertEqual(report["components"]["study"]["coverage"]["observed_seconds"], 1800)
            self.assertEqual(report["decision"], "insufficient_evidence")
            self.assertEqual(report["accounts"][0]["gross_net"], 1)
            self.assertIsNone(report["accounts"][1]["gross_net"])
            self.assertEqual(report["accounts"][1]["unresolved_basis"], 5)
            self.assertFalse(report["capital_review_eligible"])
            self.assertFalse(report["replay_eligibility_inferred"])
            self.assertNotIn("0x" + "1" * 40, json.dumps(report))
            self.assertNotIn(directory, json.dumps(report))
            self.assertEqual(delivered, research.analyze_closeout(evidence, output))
            for item in delivered["artifacts"]:
                self.assertEqual(hashlib.sha256(Path(item["path"]).read_bytes()).hexdigest(), item["sha256"])

    def test_receipt_accounting_mismatch_rejects_evidence_package(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence, write = self.artifacts(directory)
            path = Path(evidence["artifacts"]["accounting"]["path"])
            accounting = json.loads(path.read_bytes())
            accounting["components"]["study"]["accounting"]["accounts"][0]["realized_pnl"] = 99
            evidence["artifacts"]["accounting"] = write(path, accounting)
            report = research.analyze_closeout(evidence, Path(directory) / "analysis")["result"]
            self.assertEqual(report["decision"], "rejected")
            self.assertFalse(report["accounts"][0]["accounting_verified"])
            self.assertIn("STUDY_ACCOUNTING_NOT_VERIFIED", report["rejected_reasons"])

    def test_hash_tampering_and_output_inside_capture_are_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence, _ = self.artifacts(directory)
            with self.assertRaisesRegex(ValueError, "OUTSIDE_CAPTURE"):
                research.analyze_closeout(evidence, Path(directory) / "capture/analysis")
            Path(evidence["artifacts"]["raw_audit"]["path"]).write_text("{}")
            with self.assertRaisesRegex(ValueError, "HASH_MISMATCH"):
                research.analyze_closeout(evidence, Path(directory) / "analysis")

    def test_missing_audit_stays_descriptive_and_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence, _ = self.artifacts(directory)
            evidence["artifacts"].pop("raw_audit")
            report = research.analyze_closeout(evidence, Path(directory) / "analysis")["result"]
            self.assertEqual(report["decision"], "insufficient_evidence")
            self.assertIn("MISSING_RAW_AUDIT", report["holds"])
            self.assertEqual(report["components"]["study"]["closure"]["integrity"], "UNVERIFIED")

    def test_window_event_is_not_a_genuine_terminal_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence, write = self.artifacts(directory)
            path = Path(evidence["artifacts"]["terminal_receipt"]["path"])
            receipt = json.loads(path.read_bytes()); receipt["state"] = "WINDOW_ENDED"
            evidence["artifacts"]["terminal_receipt"] = write(path, receipt)
            report = research.analyze_closeout(evidence, Path(directory) / "analysis")["result"]
            self.assertEqual(report["decision"], "rejected")
            self.assertFalse(report["components"]["study"]["closure"]["terminal_receipt_verified"])
            self.assertIn("GENUINE_TERMINAL_RECEIPT_REQUIRED", report["rejected_reasons"])


if __name__ == "__main__":
    unittest.main()
