from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from src.copytrade.config import CopyTradeConfig
from src.copytrade.drift import assess_forward_drift
from src.copytrade.science_repository import ScientificRepository
from src.copytrade.science_read_model import ScientificReadModel
from src.copytrade.scientific_worker import ScientificWorker


class ScientificWorkerIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        settings = replace(
            CopyTradeConfig().scientific_worker,
            minimum_hot_free_bytes=1,
            historical_resamples=20,
            minimum_sample=8,
            maximum_q_value=0.80,  # synthetic statistical fixture only
            minimum_forward_observations=3,
            drift_minimum_observations=20,
        )
        self.config = replace(CopyTradeConfig(), scientific_worker=settings)
        self.repository = ScientificRepository(Path(self.temp.name) / "science.sqlite3")
        self.worker = ScientificWorker(self.repository, self.config, worker_id="d6-test-worker")
        self.start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _observation(self, kind: str, event_id: str, at: datetime, payload: dict[str, object], *, wallet: str | None = None) -> None:
        self.worker.ingest_observation(
            kind=kind, source="d6-synthetic", source_event_id=event_id, event_at=at,
            symbol="BTC", wallet=wallet, payload=payload,
        )

    def test_outcome_waits_for_mature_horizon_and_retains_cost_adjusted_provenance(self) -> None:
        self._observation("MARKET_PRICE", "price-start", self.start, {"price": 100.0})
        self._observation("WALLET_FILL", "wallet-start", self.start + timedelta(seconds=1), {"side": "buy", "price": 100.0, "estimated_cost": 0.001}, wallet="0x" + "1" * 40)
        self.worker.run_until_idle(max_cycles=8)
        self.assertEqual(self.repository.list_outcome_labels(), [])

        self._observation("MARKET_PRICE", "price-mature", self.start + timedelta(seconds=20), {"price": 101.0})
        self.worker.run_until_idle(max_cycles=8)
        labels = self.repository.list_outcome_labels(horizon_seconds=15)
        self.assertEqual(len(labels), 1)
        self.assertAlmostEqual(float(labels[0]["payload"]["net_outcome"]), 0.009)
        self.assertEqual(labels[0]["payload"]["market_observation_id"].startswith("obs-"), True)

    def test_full_loop_is_durable_and_shadow_only(self) -> None:
        price = 100.0
        # The historical fixture is deliberately bounded and frozen before
        # forward anchors are added. Buys have positive net outcomes; sells
        # have insufficient edge after cost, creating an interpretable signal.
        for index in range(19):
            at = self.start + timedelta(seconds=index * 30)
            side = "buy" if index < 13 else "sell"
            self._observation("MARKET_PRICE", f"history-price-{index}", at, {"price": price})
            self._observation("WALLET_FILL", f"history-wallet-{index}", at + timedelta(seconds=1), {
                "side": side, "price": price, "notional": 100.0, "estimated_cost": 0.001,
            }, wallet="0x" + "1" * 40)
            price *= 1.01 if side == "buy" else 1.0002
        self._observation("MARKET_PRICE", "history-final", self.start + timedelta(seconds=19 * 30), {"price": price})
        history = self.worker.run_until_idle(max_cycles=24)
        self.assertEqual(history["failures"], 0)
        self.assertTrue(any(item["state"] == "FORWARD_SHADOW" for item in self.repository.list_hypotheses()))

        # All forward anchors share a timestamp so none has a future market
        # label when its prediction is persisted.
        forward_at = self.start + timedelta(seconds=600)
        self._observation("MARKET_PRICE", "forward-start", forward_at, {"price": price})
        for index in range(3):
            self._observation("WALLET_FILL", f"forward-wallet-{index}", forward_at + timedelta(seconds=1), {
                "side": "buy", "price": price, "estimated_cost": 0.001,
            }, wallet="0x" + str(index + 2) * 40)
        before_outcomes = self.worker.run_until_idle(max_cycles=24)
        self.assertEqual(before_outcomes["failures"], 0)
        predictions = self.repository.list_forward_records()
        self.assertEqual(len(predictions), 3)
        self.assertTrue(all(item["outcome"] is None for item in predictions))

        price *= 1.01
        self._observation("MARKET_PRICE", "forward-mature", forward_at + timedelta(seconds=20), {"price": price})
        after_outcomes = self.worker.run_until_idle(max_cycles=40)
        self.assertEqual(after_outcomes["failures"], 0)
        predictions = self.repository.list_forward_records()
        self.assertTrue(all(item["outcome"] is not None for item in predictions))
        self.assertEqual(len(self.repository.list_indicators()), 1)
        self.assertEqual(len(self.repository.list_models()), 1)
        self.assertEqual(self.repository.list_models()[0]["state"], "ACTIVE_SIMULATION")
        decision = self.repository.list_decisions()[0]
        self.assertEqual(decision["payload"]["provenance"]["execution_mode"], "SIMULATION_SHADOW")
        self.assertGreaterEqual(len(self.repository.list_model_calibrations()), 1)
        queue = self.repository.work_queue_status(now="test")
        self.assertEqual(queue["states"].get("FAILED", 0), 0)

    def test_single_forward_outcome_cannot_degrade_an_indicator(self) -> None:
        assessment = assess_forward_drift(
            [{"net_outcome": -1.0, "trade_confidence": 0.99}],
            minimum_observations=20,
            net_expectancy_floor=0.0,
        )
        self.assertEqual(assessment.state, "INSUFFICIENT_EVIDENCE")

    def test_automated_read_model_exposes_truthful_control_and_queue_state(self) -> None:
        projection = ScientificReadModel(self.config, self.repository.path)
        before = projection.automated()
        self.assertEqual(before["execution_mode"], "SIMULATION_SHADOW_ONLY")
        self.assertFalse(before["worker_control"]["paused"])
        paused = projection.pause_automated_worker("test maintenance")
        self.assertTrue(paused["worker_control"]["paused"])
        resumed = projection.resume_automated_worker()
        self.assertFalse(resumed["worker_control"]["paused"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
