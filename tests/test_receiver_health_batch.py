from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from src.l3f_provider import ninjatrader_observation as observation_module
from src.l3f_provider.ninjatrader_commission import NinjaTraderCommissioningHarness
from src.l3f_provider.ninjatrader_observation import (
    AccountClass,
    L3F2_SCHEMA,
    NinjaTraderHealthStream,
    NinjaTraderObservation,
)
from src.l3f_provider.tradovate_observation import StreamHealth


TIME = "2026-09-22T02:00:00+00:00"
NORMALIZED_TIME = "2026-09-22T02:00:00Z"


def observation(
    kind: str,
    *,
    payload: dict[str, object] | None = None,
    account_alias: str | None = None,
) -> NinjaTraderObservation:
    return NinjaTraderObservation(
        observation_id=f"health-{kind.lower()}",
        session_id="health-session",
        observation_type=kind,
        ninja_receipt_time=TIME,
        local_monotonic_sequence=1,
        payload=payload or {"contract_id": "MNQ DEC26"},
        account_alias=account_alias,
        account_class=(
            AccountClass.PROVIDER_EVALUATION if account_alias is not None else None
        ),
    )


def frame(number: int) -> bytes:
    return json.dumps(
        {
            "schema": L3F2_SCHEMA,
            "observation_id": f"health-{number}",
            "session_id": "health-session",
            "observation_type": "QUOTE",
            "ninja_receipt_time": NORMALIZED_TIME,
            "local_monotonic_sequence": number,
            "provider_timestamp": NORMALIZED_TIME,
            "provider_sequence": None,
            "exchange_timestamp": None,
            "account": None,
            "payload": {"contract_id": "MNQ DEC26"},
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


class ReceiverHealthBatchTests(unittest.TestCase):
    def test_observation_health_normalizes_once_and_materializes_one_snapshot(self) -> None:
        harness = NinjaTraderCommissioningHarness()
        quote = observation("QUOTE")

        with (
            patch.object(
                observation_module,
                "normalized_utc",
                wraps=observation_module.normalized_utc,
            ) as normalize,
            patch.object(
                harness.health,
                "snapshot",
                wraps=harness.health.snapshot,
            ) as snapshot,
        ):
            harness._record_observation_health(quote)

        self.assertEqual(normalize.call_count, 1)
        self.assertEqual(snapshot.call_count, 1)
        health = harness.health.snapshot()
        for stream in (
            NinjaTraderHealthStream.NINJATRADER_PROCESS,
            NinjaTraderHealthStream.MARKET_DATA_STREAM,
        ):
            self.assertIs(health.streams[stream], StreamHealth.HEALTHY)
            self.assertEqual(health.updated_at[stream], NORMALIZED_TIME)
        self.assertIs(
            health.streams[NinjaTraderHealthStream.DEPTH_STREAM],
            StreamHealth.UNKNOWN,
        )

    def test_observation_types_preserve_exact_independent_stream_marks(self) -> None:
        cases = (
            (observation("TRADE"), NinjaTraderHealthStream.MARKET_DATA_STREAM),
            (observation("DEPTH"), NinjaTraderHealthStream.DEPTH_STREAM),
            (
                observation("ACCOUNT", account_alias="Lucid25kflex01"),
                NinjaTraderHealthStream.ACCOUNT_STREAM,
            ),
            (
                observation("POSITION", account_alias="Lucid25kflex01"),
                NinjaTraderHealthStream.POSITION_STREAM,
            ),
            (
                observation("ORDER", account_alias="Lucid25kflex01"),
                NinjaTraderHealthStream.ORDER_STREAM,
            ),
            (
                observation(
                    "SNAPSHOT_COMPLETE",
                    payload={"scope": "POSITION", "open_count": 0},
                    account_alias="Lucid25kflex01",
                ),
                NinjaTraderHealthStream.POSITION_STREAM,
            ),
            (
                observation(
                    "SNAPSHOT_COMPLETE",
                    payload={"scope": "ORDER", "working_count": 0},
                    account_alias="Lucid25kflex01",
                ),
                NinjaTraderHealthStream.ORDER_STREAM,
            ),
            (
                observation(
                    "CONNECTION",
                    payload={"state": "ACCOUNT_BOUND"},
                    account_alias="Lucid25kflex01",
                ),
                NinjaTraderHealthStream.LUCID_CONNECTION,
            ),
        )
        for item, specific_stream in cases:
            with self.subTest(kind=item.observation_type, stream=specific_stream):
                harness = NinjaTraderCommissioningHarness()
                harness._record_observation_health(item)
                health = harness.health.snapshot()
                healthy = {
                    stream
                    for stream, state in health.streams.items()
                    if state is StreamHealth.HEALTHY
                }
                self.assertEqual(
                    healthy,
                    {
                        NinjaTraderHealthStream.NINJATRADER_PROCESS,
                        specific_stream,
                    },
                )
                self.assertEqual(
                    health.updated_at[NinjaTraderHealthStream.NINJATRADER_PROCESS],
                    NORMALIZED_TIME,
                )
                self.assertEqual(health.updated_at[specific_stream], NORMALIZED_TIME)

    def test_ingest_preserves_callbacks_duplicate_rejection_and_report_shape(self) -> None:
        local_states: list[StreamHealth] = []
        accepted_ids: list[str] = []
        duplicate_calls: list[None] = []
        rejection_details: list[str | None] = []
        harness = NinjaTraderCommissioningHarness(
            on_local_bridge_state=local_states.append,
            on_observation=lambda item: accepted_ids.append(item.observation_id),
            on_duplicate=lambda: duplicate_calls.append(None),
            on_rejection=lambda error: rejection_details.append(error.detail),
        )
        first = frame(1)
        second = frame(2)

        summary = harness.ingest(
            (first, first, b'{"schema":"wrong"}', second),
            source_connection=object(),
        )

        self.assertEqual(accepted_ids, ["health-1", "health-2"])
        self.assertEqual(len(duplicate_calls), 1)
        self.assertEqual(rejection_details, ["bridge_schema"])
        self.assertEqual(local_states, [StreamHealth.HEALTHY])
        report = summary.safe_report()
        self.assertEqual(report["accepted_observations"], 2)
        self.assertEqual(report["duplicate_observations"], 1)
        self.assertEqual(report["rejected_observations"], 1)
        self.assertEqual(
            report["provider_health"]["local_bridge_transitions"],
            [StreamHealth.UNKNOWN.value, StreamHealth.HEALTHY.value],
        )
        self.assertEqual(
            set(report["provider_health"]["streams"]),
            {stream.value for stream in NinjaTraderHealthStream},
        )


if __name__ == "__main__":
    unittest.main()
