from __future__ import annotations

from pathlib import Path
import unittest


class NinjaScriptSourceTests(unittest.TestCase):
    def test_execution_source_is_exact_sim101_closed_action_only(self) -> None:
        source = (Path(__file__).parents[1] / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubPaperExecutionAddOn.cs").read_text(encoding="utf-8")
        for required in (
            'ExactAccountName = "Sim101"', 'ExactInstrumentName = "MNQ SEP26"',
            "MaximumQuantity = 1", "HMACSHA256", "Account.All", "CreateOrder(",
            ".Submit(", ".Cancel(", ".Flatten(", "BZ-L3G-E-", "BZ-L3G-S-",
        ):
            self.assertIn(required, source)
        for forbidden in (
            "Lucid25kflex01", "AccountSelector", "GetEnvironmentVariable", "FlattenEverything",
            "CancelAllOrders", "AtmStrategy", "quantity > 1", "fallbackPort",
            "LiveExecutionMode", "NinjaTraderLiveAdapter", "LucidExecutionAdapter",
        ):
            self.assertNotIn(forbidden, source)
        for denial in (
            "INVALID_SIGNATURE_OR_SCHEMA", "DUPLICATE_JSON_KEY", "OVERSIZED_FRAME",
            "WRONG_EXECUTION_SESSION", "STALE_OR_FUTURE_COMMAND", "COMMAND_EXPIRED",
            "AUTHORITY_HASH_MISMATCH", "ACCOUNT_MISMATCH", "ACCOUNT_CLASS_MISMATCH",
            "INSTRUMENT_MISMATCH", "INSTRUMENT_BINDING_LOST", "QUANTITY_REFUSED",
            "UNSUPPORTED_ACTION", "POSITION_OR_ORDER_PRECONDITION", "FOREIGN_ACTIVITY_LOCKOUT",
            "REORDERED_COMMAND", "RECONCILIATION_REQUIRED",
            "HEARTBEAT_WATCHDOG", "PROTECTIVE_STOP_ACCEPTANCE_TIMEOUT",
            "FLATTEN_ACCEPTANCE_TIMEOUT",
        ):
            self.assertIn(denial, source)

    def test_expected_protective_cancellation_is_scoped_to_an_exact_flatten(self) -> None:
        source = (Path(__file__).parents[1] / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubPaperExecutionAddOn.cs").read_text(encoding="utf-8")
        self.assertIn("flattenInProgress = true", source)
        self.assertIn("order.OrderState == OrderState.Cancelled && !flattenInProgress", source)
        self.assertIn("ExpectedFlattenOrder(order)", source)
        self.assertIn('"Close", "EXIT", order', source)
        self.assertIn("flattenFailed) LockAndProtect(\"FLATTEN_ACCEPTANCE_TIMEOUT\")", source)

    def test_read_only_addon_remains_without_order_or_inbound_authority(self) -> None:
        source = (Path(__file__).parents[1] / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubReadOnlyAddOn.cs").read_text(encoding="utf-8")
        for forbidden in ("CreateOrder(", ".Submit(", ".Cancel(", ".Flatten(", "Stream.Read"):
            self.assertNotIn(forbidden, source)

    def test_observer_bounds_unverified_book_frames_and_stays_observation_only(self) -> None:
        source = (Path(__file__).parents[1] / "ninjatrader" / "NinjaScript" / "Indicators" / "BeelzebubReadOnlyMarketObserver.cs").read_text(encoding="utf-8")
        self.assertIn("MaximumPublishedBookLevelsPerSide = 10", source)
        self.assertIn("TrimBook(book)", source)
        self.assertIn("mutationPrice = book.ElementAt(e.Position).Key", source)
        self.assertIn("MARKET_OBSERVER_UNRESOLVED_DEPTH_REMOVE", source)
        self.assertIn("bidAtTrade < askAtTrade", source)
        self.assertIn("bestBid < bestAsk", source)
        self.assertIn("MARKET_OBSERVER_REALTIME_STRICT_SPREAD_V1", source)
        self.assertNotIn("bidAtTrade <= askAtTrade", source)
        self.assertNotIn("bestBid <= bestAsk", source)
        for forbidden in ("CreateOrder(", ".Submit(", ".Cancel(", ".Flatten(", "NetworkStream.Read"):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
