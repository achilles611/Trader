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

    def test_watchdog_requires_a_correlated_settled_reconciliation(self) -> None:
        source = (Path(__file__).parents[1] / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubPaperExecutionAddOn.cs").read_text(encoding="utf-8")
        self.assertIn("pendingWatchdogSafetyEventId", source)
        self.assertIn('incident["safety_event_id"] = safetyEventId', source)
        self.assertIn('message["safety_event_id"] = safetyEventId', source)
        self.assertIn("TryPublishWatchdogSafetyReconciliation()", source)
        self.assertIn("requireWatchdogFlat", source)
        self.assertIn("safety_settlement_final", source)
        self.assertIn("safety_settlement_sequence", source)
        self.assertIn("WatchdogSettlementSeconds", source)
        self.assertIn("MaximumWatchdogFinalProofAttempts", source)
        self.assertIn("pendingWatchdogSafetyFinalProofInFlight", source)
        self.assertIn("authenticatedSessionGeneration", source)
        self.assertIn("settlementSequence = pendingWatchdogSafetySettlementSequence + 1", source)
        self.assertIn("if (sent)", source)
        self.assertIn('"PROCESS_STOP_OPEN_POSITION"', source)
        self.assertIn('BeginWatchdogSafetyCorrelation("EMERGENCY_FLATTEN_ACCEPTED")', source)
        self.assertIn("RepublishPendingWatchdogSafetyCorrelation()", source)
        self.assertIn("private bool SendSigned", source)

    def test_foreign_lockout_cannot_disable_owned_entry_watchdog_cancellation(self) -> None:
        """Foreign Sim101 activity must not turn heartbeat loss into a no-op."""
        source = (Path(__file__).parents[1] / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubPaperExecutionAddOn.cs").read_text(encoding="utf-8")
        watchdog = source[source.index("        private void LockAndProtect"):source.index("        private void Acknowledge")]
        self.assertNotIn('if (lockedOut && reason == "HEARTBEAT_WATCHDOG") return;', watchdog)
        self.assertIn("watchdogSafetyDispatchStarted", watchdog)
        self.assertIn("watchdogSafetyActionInFlight", watchdog)
        self.assertIn("CancelOwnedOrders();", watchdog)
        self.assertIn("if (!foreign && position != null && position.Quantity != 0)", watchdog)
        self.assertIn("FOREIGN_ACTIVITY_EMERGENCY_FLATTEN_REFUSED", source)

    def test_restart_rehydrates_exact_owned_work_before_session_and_watchdog(self) -> None:
        """A reload must not lose cancellation ownership of a working BZ-L3G order."""
        source = (Path(__file__).parents[1] / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubPaperExecutionAddOn.cs").read_text(encoding="utf-8")
        helper = source[
            source.index("        private void RehydrateOwnedWorkingOrders"):
            source.index("        private static bool Working")
        ]
        self.assertIn("lock (paperAccount.Orders)", helper)
        self.assertIn("Working(order.OrderState)", helper)
        self.assertIn("ExactInstrumentName", helper)
        self.assertIn("IsOwnedName(order.Name)", helper)
        self.assertIn("OwnedOrder.Restored(order)", helper)
        self.assertNotIn(".Submit(", helper)
        self.assertNotIn(".Flatten(", helper)
        self.assertNotIn(".Cancel(", helper)
        accept = source[
            source.index("        private void AcceptSession"):
            source.index("        private static bool HashText")
        ]
        self.assertLess(accept.index("RehydrateOwnedWorkingOrders();"), accept.index("authenticated = true"))
        watchdog = source[
            source.index("        private void WatchdogLoop"):
            source.index("        private void LockAndProtect")
        ]
        self.assertLess(watchdog.index("RehydrateOwnedWorkingOrders();"), watchdog.index("lock (stateLock)"))

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
        self.assertIn('PublicationPolicy = "BOUNDED_LATEST_STATE_2HZ"', source)
        self.assertIn("MinimumPublicationTicks", source)
        self.assertIn("TryReservePublication(ref lastQuotePublicationTicks)", source)
        self.assertIn("TryReservePublication(ref lastTradePublicationTicks)", source)
        self.assertIn("TryReservePublication(ref lastDepthPublicationTicks)", source)
        self.assertIn("publication_policy", source)

        addon = (Path(__file__).parents[1] / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubReadOnlyAddOn.cs").read_text(encoding="utf-8")
        self.assertIn("private const int MaximumQueuedFrames = 8;", addon)
        self.assertNotIn("private const int MaximumQueuedFrames = 20000;", addon)
        self.assertIn("bidAtTrade < askAtTrade", source)
        self.assertIn("bestBid < bestAsk", source)
        self.assertIn("MARKET_OBSERVER_REALTIME_STRICT_SPREAD_V1", source)
        self.assertNotIn("bidAtTrade <= askAtTrade", source)
        self.assertNotIn("bestBid <= bestAsk", source)
        for forbidden in ("CreateOrder(", ".Submit(", ".Cancel(", ".Flatten(", "NetworkStream.Read"):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
