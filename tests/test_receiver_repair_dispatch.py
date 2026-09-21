"""TEST ONLY: required-consumer regression for the qualifying receiver route."""

from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from src.copytrade.control_center import _dispatch_pong_observation, _ObservationIngressMetrics
from src.l3f_provider.ninjatrader_observation import AccountClass
from src.l3g_paper.contracts import (
    PONG_ENTRY_PROFILE_VERSION, paper_policy_for_topology,
    paper_risk_for_topology, resolve_paper_profile,
)
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.pong_contract import DEC26_TOPOLOGY
from src.l3g_paper.pong_qualifying import PONG_NO_FHB_QUALIFYING_ARCHITECTURE
from src.l3g_paper.runtime import LaneIIIPaperRuntime, ObservationFanout
from tests.l3g_helpers import ObservationFactory


class ReceiverRepairDispatchTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.failures = []
        self.metrics = _ObservationIngressMetrics()
        self.paper_callback = lambda record: self.events.append(("paper", record))
        self.fanout = ObservationFanout(
            shadow_observation=lambda record: self.events.append(("shadow", record)),
            shadow_transport=lambda state: self.events.append(("shadow_transport", state)),
            shadow_rejection=lambda error: self.events.append(("shadow_rejection", error)),
            shadow_duplicate=lambda: self.events.append(("shadow_duplicate", None)),
            paper_observation=lambda record: self.paper_callback(record),
            paper_transport=lambda state: self.events.append(("paper_transport", state)),
            paper_rejection=lambda error: self.events.append(("paper_rejection", error)),
            paper_duplicate=lambda: self.events.append(("paper_duplicate", None)),
            record_failure=lambda *values: self.failures.append(values),
        )

    def dispatch(self, observation, **overrides):
        options = dict(
            collector=SimpleNamespace(ingest=lambda record: self.events.append(("collector", record))),
            pong_host=SimpleNamespace(on_observation=lambda record: self.events.append(("host", record))),
            fanout=self.fanout, metrics=self.metrics, independent=False,
            profile=PONG_ENTRY_PROFILE_VERSION, activation_authorized=False,
            qualifying_architecture=PONG_NO_FHB_QUALIFYING_ARCHITECTURE,
        )
        options.update(overrides)
        _dispatch_pong_observation(observation, **options)

    def test_mixed_streams_preserve_required_consumers_identity_and_order(self):
        kinds = ("QUOTE", "ACCOUNT", "TRADE", "POSITION", "DEPTH", "ORDER", "HEALTH",
                 "EXECUTION", "CONNECTION", "INSTRUMENT", "DEPTH")
        for independent in (False, True):
            with self.subTest(independent=independent):
                self.events.clear()
                records = [SimpleNamespace(observation_type=kind, sequence=i)
                           for i, kind in enumerate(kinds)]
                for record in records:
                    self.dispatch(record, independent=independent)
                for sink in ("collector", "host", "paper"):
                    self.assertEqual([record for owner, record in self.events if owner == sink], records)
                self.assertEqual(
                    [record for owner, record in self.events if owner == "shadow"],
                    [record for record in records if record.observation_type not in {"QUOTE", "TRADE", "DEPTH"}],
                )
                self.assertEqual(self.failures, [])

    def test_unselected_architecture_and_activation_routes_are_unchanged(self):
        record = SimpleNamespace(observation_type="DEPTH")
        cases = (
            ({"qualifying_architecture": None}, ["collector", "host", "shadow", "paper"]),
            ({"qualifying_architecture": "UNKNOWN"}, ["collector", "host", "shadow", "paper"]),
            ({"profile": "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2"}, ["collector", "host", "shadow", "paper"]),
            ({"activation_authorized": True}, ["collector", "host", "shadow", "paper"]),
            ({"activation_authorized": True, "independent": True}, ["collector", "host", "paper"]),
            ({"qualifying_architecture": None, "independent": True}, ["collector", "host"]),
        )
        for settings, expected in cases:
            with self.subTest(settings=settings):
                self.events.clear()
                self.dispatch(record, **settings)
                self.assertEqual([owner for owner, _ in self.events], expected)

    def test_required_paper_failure_is_recorded_and_propagated(self):
        def fail(_):
            raise RuntimeError("required consumer refused")
        self.paper_callback = fail
        for kind in ("DEPTH", "ACCOUNT"):
            with self.subTest(kind=kind):
                with self.assertRaisesRegex(RuntimeError, "required consumer refused"):
                    self.dispatch(SimpleNamespace(observation_type=kind))
        self.assertEqual(self.failures, [("EXPERIMENTAL_PAPER", "OBSERVATION", "RuntimeError")] * 2)
        self.assertEqual(self.metrics.snapshot()["routes"], {})

    def test_collector_and_host_admission_exceptions_propagate(self):
        def fail(_):
            raise RuntimeError("admission refused")
        for sink, override, expected in (
            ("collector", {"collector": SimpleNamespace(ingest=fail)}, []),
            ("host", {"pong_host": SimpleNamespace(on_observation=fail)}, ["collector"]),
        ):
            with self.subTest(sink=sink):
                self.events.clear()
                with self.assertRaisesRegex(RuntimeError, "admission refused"):
                    self.dispatch(SimpleNamespace(observation_type="DEPTH"), **override)
                self.assertEqual([owner for owner, _ in self.events], expected)

    def test_qualifying_market_refuses_startup_pause_without_buffering(self):
        self.fanout.begin_startup_paper_observation_pause()
        with self.assertRaises(RuntimeError):
            self.dispatch(SimpleNamespace(observation_type="DEPTH"))
        result = self.fanout.end_startup_paper_observation_pause()
        self.assertEqual(result["replayed_observations"], 0)
        self.assertFalse(any(owner == "paper" for owner, _ in self.events))

    def test_existing_unselected_startup_pause_drains_paper_cross_stream_order(self):
        records = [SimpleNamespace(observation_type=kind) for kind in ("QUOTE", "ACCOUNT", "DEPTH")]
        self.fanout.begin_startup_paper_observation_pause()
        for record in records:
            self.dispatch(record, qualifying_architecture=None)
        self.assertFalse(any(owner == "paper" for owner, _ in self.events))
        result = self.fanout.end_startup_paper_observation_pause()
        self.assertEqual(result["replayed_observations"], 3)
        self.assertEqual([record for owner, record in self.events if owner == "paper"], records)

    def test_required_paper_ledger_and_freshness_are_retained(self):
        profile = resolve_paper_profile(PONG_ENTRY_PROFILE_VERSION)
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(
                Path(directory) / "paper.sqlite3",
                policy=paper_policy_for_topology(profile.policy, DEC26_TOPOLOGY),
                risk=paper_risk_for_topology(profile.risk, DEC26_TOPOLOGY),
            )
            try:
                runtime = LaneIIIPaperRuntime(ledger)
                self.paper_callback = runtime.ingest
                factory = ObservationFactory()
                quote = factory.make("QUOTE", {
                    "contract_id": "MNQ DEC26", "bid": "25000", "ask": "25000.25",
                    "bid_size": 10, "ask_size": 10,
                })
                self.dispatch(quote)
                self.assertEqual(self.failures, [])
                self.assertEqual(runtime._snapshot.quote_observed_at, quote.ninja_receipt_time)
                # Existing policy suppresses raw market envelopes in the paper
                # ledger; the collector is their durable owner. Control truth
                # must still enter the authority ledger through the same sink.
                account = replace(
                    quote, observation_id="account-control-2", observation_type="ACCOUNT",
                    local_monotonic_sequence=2, payload={"unknown_control_field": "retain"},
                    account_alias="Sim101", account_class=AccountClass.LOCAL_SIMULATION,
                )
                self.dispatch(account)
                ledger.flush_deferred()
                envelopes = ledger.recent_kinds(("OBSERVATION_ENVELOPE",))
                self.assertEqual(len(envelopes), 1)
                self.assertEqual(envelopes[0]["payload"]["observation_id"], account.observation_id)
                self.assertEqual(envelopes[0]["payload"]["local_monotonic_sequence"], account.local_monotonic_sequence)
                persistence = ledger.health_status()["persistence_policy"]
                self.assertEqual(persistence["suppressed_records_by_domain"]["OBSERVATION"], 1)
                self.assertEqual([owner for owner, _ in self.events],
                                 ["collector", "host", "collector", "host", "shadow"])
            finally:
                ledger.close()


if __name__ == "__main__":
    unittest.main()
