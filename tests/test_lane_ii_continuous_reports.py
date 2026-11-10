from copy import deepcopy
import json
import unittest

from scripts.lane_ii_continuous import reports


class StatusTests(unittest.TestCase):
    def sample(self):
        return {"as_of_utc": "2026-11-08T12:00:00Z", "source_sha256": "a" * 64, "config_sha256": "b" * 64,
            "paper_only": True, "execution": {"state": "WINDOW_ENDED"},
            "coverage": {"planned_seconds": 28800, "observed_seconds": 9000, "gap_seconds": 19800},
            "closure": {"seal": "SEALED", "integrity": "PASS", "accounting": "RECONCILED", "analysis": "PUBLISHED"},
            "components": {"study": {"state": "FAILED", "utc": "2026-11-08T09:30:00Z", "reason": "TOTAL_HTTP_TRANSPORT_TIMEOUT"},
                           "cohort": {"state": "RUNNING", "utc": "2026-11-08T09:30:00Z"}},
            "paper_accounts": [{"id": "P13", "realized_net": 2, "modeled_fees": 1, "unresolved_basis": 5, "unresolved_positions": 1,
                                "virtual_cash": 3000, "raw_fills": [{"secret": "PRIVATE"}]}],
            "runtime_model_calls_observed": 0, "queue": {"counts": {"failed": 1, "completed": 4}, "bounded": True}}

    def test_elapsed_window_and_published_analysis_do_not_make_failed_study_complete(self):
        status = reports.build_status(self.sample(), now_utc="2026-11-08T12:05:00Z")
        self.assertEqual(status["execution"]["state"], "failed")
        self.assertTrue(status["execution"]["window_ended"])
        self.assertEqual(status["coverage"]["designation"], "PARTIAL_OR_UNKNOWN")
        self.assertEqual(status["closure"]["analysis"], "PUBLISHED")
        self.assertFalse(status["components"]["cohort"]["currently_running_verified"])
        rendered = reports.render_latest(status)
        self.assertIn("STALE OR UNVERIFIED", rendered)
        self.assertIn("not a study-completion", rendered)
        self.assertIn("independent alternatives", rendered)
        self.assertNotIn("virtual_cash", rendered)

    def test_public_allowlist_drops_secrets_paths_balances_and_raw_fills(self):
        state = self.sample()
        state.update(api_key="sk-proj-secret", account_balance=100, raw_fills=[{"user": "PRIVATE"}],
                     blockers=[r"N:\private\user\data.json", "Bearer secret-value", "DISK_ADMISSION_HOLD"])
        state["paper_accounts"][0]["id"] = "0x" + "1" * 40
        state["evidence_links"] = ["https://example.com/private", "https://github.com/achilles611/Trader/pull/3"]
        result = reports.build_status(state, now_utc="2026-11-08T12:05:00Z")
        encoded = json.dumps(result)
        for text in ("sk-proj-secret", "account_balance", "private", "secret-value", "raw_fills", "virtual_cash", "0x" + "1" * 40):
            self.assertNotIn(text, encoded)
        self.assertIn("DISK_ADMISSION_HOLD", encoded)
        self.assertEqual(len(result["evidence_links"]), 1)

    def test_clock_lead_is_not_fresh_and_regeneration_does_not_refresh_source(self):
        state = self.sample()
        one = reports.build_status(state, now_utc="2026-11-08T12:05:00Z")
        two = reports.build_status(state, now_utc="2026-11-08T12:06:00Z")
        self.assertEqual(one["evidence_as_of_utc"], two["evidence_as_of_utc"])
        self.assertEqual(two["evidence_age_seconds"] - one["evidence_age_seconds"], 60)
        future = reports.build_status(state, now_utc="2026-11-08T11:00:00Z")
        self.assertTrue(future["future_clock"])
        self.assertTrue(future["stale"])

    def test_full_coverage_requires_explicit_coverage_and_completed_execution(self):
        state = self.sample(); state["execution"] = {"state": "completed"}
        state["components"] = {}
        state["coverage"] = {"planned_seconds": 100, "observed_seconds": 100, "gap_seconds": 0}
        self.assertNotEqual(reports.build_status(state)["coverage"]["designation"], "FULL")
        state["coverage"]["requirements_met"] = True
        self.assertEqual(reports.build_status(state)["coverage"]["designation"], "FULL")

    def test_fresh_outer_status_cannot_hide_stale_failed_source(self):
        state = self.sample()
        state["execution"] = {"state": "completed"}
        result = reports.build_status(state, now_utc="2026-11-08T12:00:10Z")
        self.assertTrue(result["stale"])
        self.assertEqual(result["execution"]["state"], "failed")
        self.assertEqual(result["evidence_age_seconds"], 9010)

    def test_failed_cohort_cannot_be_hidden_by_completed_aggregate(self):
        state = self.sample()
        state["execution"] = {"state": "completed"}
        state["components"] = {"cohort": {"state": "FAILED", "utc": "2026-11-08T12:00:00Z"}}
        state["coverage"] = {"planned_seconds": 100, "observed_seconds": 100, "gap_seconds": 0, "requirements_met": True}
        result = reports.build_status(state, now_utc="2026-11-08T12:00:10Z")
        self.assertEqual(result["execution"]["state"], "failed")
        self.assertEqual(result["coverage"]["designation"], "PARTIAL_OR_UNKNOWN")

    def test_complete_evidence_requires_replay_pass(self):
        state = self.sample()
        for replay in (None, "UNKNOWN", "FAIL", "NOT_APPLICABLE"):
            state["closure"]["replay"] = replay
            self.assertFalse(reports.build_status(state)["closure"]["complete_evidence"])
        state["closure"]["replay"] = "PASS"
        self.assertTrue(reports.build_status(state)["closure"]["complete_evidence"])

    def test_daily_and_weekly_digests_never_mutate_roster_or_admit_capital(self):
        state = reports.build_status(self.sample(), now_utc="2026-11-08T12:05:00Z")
        self.assertFalse(reports.daily_digest(state)["automatic_strategy_change"])
        registry = {"trials": {"a": {"trial_id": "a", "status": "rejected"}, "b": {"trial_id": "b", "status": "registered"}}}
        wallets = {"members": [{"name": "Bossgirl", "cohort": "PRIMARY", "observation_state": "INACTIVE", "copying_eligible": False}]}
        before = deepcopy(wallets)
        review = reports.weekly_review(registry, wallets, as_of_utc="2026-11-08T12:05:00Z")
        self.assertEqual(wallets, before)
        self.assertEqual(review["negative_results_retained"], 1)
        self.assertEqual(review["automatic_admissions"], [])

    def test_weekly_digest_does_not_reuse_expired_wallet_eligibility(self):
        wallets = {"members": [{"cohort": "PRIMARY", "observation_state": "ACTIVE", "copying_eligible": True,
            "eligibility": {"paper_copying_eligible": True, "evaluated_at_utc": "2026-11-08T12:00:00Z",
                            "valid_until_utc": "2026-11-08T12:00:30Z"}}]}
        self.assertEqual(reports.weekly_review({}, wallets, as_of_utc="2026-11-08T12:00:15Z")["eligible_paper_wallets"], 1)
        self.assertEqual(reports.weekly_review({}, wallets, as_of_utc="2026-11-08T12:05:00Z")["eligible_paper_wallets"], 0)

    def test_descriptive_closeout_never_sums_accounts_or_infers_capital(self):
        report = reports.closeout_summary({"schema": "LANE_II_DESCRIPTIVE_CLOSEOUT_V1", "decision": "profitable",
            "capital_review_eligible": True, "source_root": "N:/private/path", "accounts": [
                {"account_id": "P13", "component": "study", "realized_net": 1, "virtual_cash": 3000,
                 "unresolved_basis": 5, "unresolved_positions": 1, "gross_net": None}],
            "holds": ["OPEN_EXPOSURE_UNRESOLVED"]})
        self.assertFalse(report["capital_review_eligible"])
        self.assertEqual(report["decision"], "insufficient_evidence")
        self.assertNotIn("virtual_cash", json.dumps(report))
        rendered = reports.render_closeout(report)
        self.assertIn("returns are not added together", rendered)
        self.assertIn("gross stays unknown", rendered)
        self.assertNotIn("N:/private/path", rendered)


if __name__ == "__main__":
    unittest.main()
