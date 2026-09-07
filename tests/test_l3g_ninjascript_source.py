from __future__ import annotations

from pathlib import Path
import re
import unittest

from src.l3g_paper.contracts import FIVE_MINUTE_PERPETUAL_POLICY
from src.l3g_paper.ninjatrader_transport import (
    EXPECTED_ADDON_SOURCE_FINGERPRINT,
    expected_addon_source_fingerprint,
)


class NinjaScriptSourceTests(unittest.TestCase):
    @staticmethod
    def _execution_source() -> str:
        return (
            Path(__file__).parents[1]
            / "ninjatrader"
            / "NinjaScript"
            / "AddOns"
            / "BeelzebubPaperExecutionAddOn.cs"
        ).read_text(encoding="utf-8")

    def test_execution_source_is_exact_sim101_closed_action_only(self) -> None:
        source = (Path(__file__).parents[1] / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubPaperExecutionAddOn.cs").read_text(encoding="utf-8")
        for required in (
            'ExactAccountName = "Sim101"', 'ExactInstrumentName = "MNQ SEP26"',
            "MaximumQuantity = 1", "HMACSHA256", "Account.All", "CreateOrder(",
            ".Submit(", ".Cancel(", "BZ-L3G-E-", "BZ-L3G-S-", "BZ-L3G-X-",
            "SubmitOwnedFlattenOrder(",
        ):
            self.assertIn(required, source)
        for forbidden in (
            "Lucid25kflex01", "AccountSelector", "GetEnvironmentVariable", "FlattenEverything",
            "CancelAllOrders", "AtmStrategy", "quantity > 1", "fallbackPort",
            "LiveExecutionMode", "NinjaTraderLiveAdapter", "LucidExecutionAdapter",
            ".Flatten(",
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

    def test_manual_activity_on_another_account_cannot_reach_sim101_foreign_latch(self) -> None:
        source = self._execution_source()
        callbacks = (
            ("OnOrderUpdate", "OnExecutionUpdate", "e.Order.Account"),
            ("OnExecutionUpdate", "OnPositionUpdate", "e.Execution.Order.Account"),
            ("OnPositionUpdate", "OnAccountItemUpdate", "e.Position.Account"),
        )
        for start, end, candidate in callbacks:
            callback = source[
                source.index(f"        private void {start}"):
                source.index(f"        private void {end}")
            ]
            ingress = callback[:callback.index("BeginNativeObservationCallback();")]
            self.assertIn(f"!ExactBoundAccount({candidate})", ingress)
            self.assertIn("return;", ingress)
            self.assertNotIn("foreignActivity", ingress)
            self.assertNotIn("LockAndProtect", ingress)

        probe = source[
            source.index("        private ProbeNativeSample CaptureProbeNativeSample"):
            source.index("        private static bool ExactFields")
        ]
        self.assertIn("!ExactBoundAccount(position.Account)", probe)
        self.assertIn("!ExactBoundAccount(order.Account)", probe)
        reconcile = source[
            source.index("        private bool SendReconciliation"):
            source.index("        private void TryPublishWatchdogSafetyReconciliation")
        ]
        self.assertIn("ExactBoundAccount(order.Account)", reconcile)
        self.assertIn("ExactBoundAccount(item.Account)", reconcile)

        # LucidFlex25k is the concrete regression case: a same-instrument event
        # from any non-bound Account instance exits before state observation.
        self.assertNotEqual("LucidFlex25k", "Sim101")

    def test_foreign_sim101_order_still_latches_and_blocks_immediately(self) -> None:
        source = self._execution_source()
        callback = source[
            source.index("        private void OnOrderUpdate"):
            source.index("        private void OnExecutionUpdate")
        ]
        foreign = callback[
            callback.index("else if (UnresolvedNativeOrderState(eventState) || eventFilled != 0)"):
            callback.index("if (!exact && (UnresolvedNativeOrderState(eventState) || eventFilled != 0)")
        ]
        self.assertIn("foreignActivity = true", foreign)
        self.assertIn("lockedOut = true", foreign)
        self.assertIn("reconciled = false", foreign)
        self.assertIn('safetyReason = "FOREIGN_ORDER_ACTIVITY"', foreign)
        self.assertLess(
            callback.index('safetyReason = "FOREIGN_ORDER_ACTIVITY"'),
            callback.index("LockAndProtect(safetyReason)"),
        )

    def test_account_identity_is_present_exact_ordinal_and_reference_bound(self) -> None:
        source = self._execution_source()
        binding = source[
            source.index("        private bool ExactBoundAccount"):
            source.index("        private void ConnectionLoop")
        ]
        self.assertIn("Object.ReferenceEquals(candidate, paperAccount)", binding)
        self.assertIn("!String.IsNullOrWhiteSpace(candidate.Name)", binding)
        self.assertIn(
            "String.Equals(candidate.Name, ExactAccountName, StringComparison.Ordinal)",
            binding,
        )
        self.assertNotIn("OrdinalIgnoreCase", binding)
        self.assertNotIn(".Trim(", binding)
        start = source[
            source.index("        private void StartPaperBoundary"):
            source.index("        private void StopPaperBoundary")
        ]
        self.assertIn(
            "Account.All.Where(a => String.Equals(a.Name, ExactAccountName, StringComparison.Ordinal))",
            start,
        )
        self.assertIn("if (matches.Count != 1)", start)

    def test_every_native_command_mutation_remains_on_exact_bound_sim101(self) -> None:
        source = self._execution_source()
        validation = source[
            source.index("        private string ValidateReservedCommand"):
            source.index("        private static string CommandOutcomeKey")
        ]
        account_match = validation.index(
            'String.Equals(Text(command, "account_name"), ExactAccountName, StringComparison.Ordinal)'
        )
        binding_match = validation.index("if (!ExactBoundAccount(paperAccount))", account_match)
        self.assertLess(account_match, binding_match)
        self.assertIn('return "ACCOUNT_BINDING_LOST"', validation[binding_match:])

        native_mutations = re.findall(
            r"\b([A-Za-z_][A-Za-z0-9_]*)\.(CreateOrder|Submit|Cancel)\(", source
        )
        self.assertTrue(native_mutations)
        self.assertTrue(all(receiver == "paperAccount" for receiver, _ in native_mutations))
        self.assertEqual(source.count(".CreateOrder("), source.count("paperAccount.CreateOrder("))
        self.assertEqual(source.count(".Submit("), source.count("paperAccount.Submit("))
        self.assertEqual(source.count(".Cancel("), source.count("paperAccount.Cancel("))
        for mutation in (
            "paperAccount.CreateOrder(",
            "paperAccount.Submit(",
            "paperAccount.Cancel(",
        ):
            self.assertIn(mutation, source)
        self.assertNotIn('Account.All.First', source)

    def test_expected_protective_cancellation_is_scoped_to_a_preowned_exit(self) -> None:
        source = self._execution_source()
        self.assertIn("flattenInProgress = true", source)
        callback = source[
            source.index("        private void OnOrderUpdate"):
            source.index("        private void OnExecutionUpdate")
        ]
        for exact_fence in (
            "eventState == OrderState.Cancelled",
            "eventFilled == 0",
            "Object.ReferenceEquals(pendingFlattenProtectiveOrder, order)",
            "String.Equals(pendingFlattenProtectiveName, order.Name, StringComparison.Ordinal)",
            "String.Equals(pendingFlattenProtectiveOco, order.Oco, StringComparison.Ordinal)",
            "String.Equals(pendingFlattenProtectiveOco, activeFlattenOwner.Order.Oco, StringComparison.Ordinal)",
        ):
            self.assertIn(exact_fence, callback)
        self.assertLess(
            callback.index("if (protectiveNoFillTerminal)"),
            callback.index("pendingFlattenProtectiveCancellationObserved = true"),
        )
        terminal_no_fill = callback[
            callback.index("bool protectiveNoFillTerminal"):
            callback.index("if (protectiveNoFillTerminal)")
        ]
        self.assertIn("eventState == OrderState.Cancelled", terminal_no_fill)
        self.assertIn("eventState == OrderState.Rejected", terminal_no_fill)
        self.assertIn("eventFilled == 0", terminal_no_fill)
        self.assertNotIn(
            "pendingFlattenProtectiveCancellationExpected = false",
            callback[
                callback.index("if (protectiveNoFillTerminal)"):
                callback.index("if (!Working(eventState)")
            ],
        )
        self.assertIn(
            "eventState == OrderState.Rejected\n"
            "                        || (eventState == OrderState.Cancelled && !expectedOcoCancellation)",
            callback,
        )
        rehydrate = source[
            source.index("        private void RehydrateOwnedWorkingOrders"):
            source.index("        private static bool Working")
        ]
        self.assertIn("if (adoptedExit != null && restoredExits == 1 && restoredProtectives == 1)", rehydrate)
        self.assertNotIn("pendingFlattenProtectiveCancellationExpected = restoredProtectives", rehydrate)
        self.assertIn("SubmitOwnedFlattenOrder(", source)
        self.assertIn('string name = "BZ-L3G-X-" + Fragment(commandId)', source)
        self.assertIn('commandId, intentId, decisionId, name, "EXIT", order', source)
        self.assertNotIn("ExpectedFlattenOrder", source)
        self.assertNotIn('String.Equals(order.Name, "Close"', source)
        self.assertIn("flattenFailed) LockAndProtect(\"FLATTEN_ACCEPTANCE_TIMEOUT\")", source)

    def test_entry_execution_protection_is_idempotent_and_conflicts_fail_closed(self) -> None:
        source = self._execution_source()
        callback = source[
            source.index("        private void OnExecutionUpdate"):
            source.index("        private void OnPositionUpdate")
        ]
        self.assertIn("protectedEntryExecutionFacts.TryGetValue(executionId, out priorFact)", callback)
        self.assertIn("String.IsNullOrWhiteSpace(commandId)", callback)
        self.assertIn("failedEntryProtectionCommands.Contains(commandId)", callback)
        self.assertIn("failedEntryProtectionCommands.Add(commandId)", callback)
        self.assertIn("protectedEntryCommandExecutions.TryGetValue(commandId, out priorExecution)", callback)
        self.assertIn("protectedEntryExecutionFacts[executionId] = executionFact", callback)
        self.assertIn("protectedEntryCommandExecutions[commandId] = executionId", callback)
        self.assertIn('LockAndProtect("CONFLICTING_OR_UNIDENTIFIED_ENTRY_EXECUTION")', callback)
        self.assertEqual(callback.count("SubmitProtectiveStop(order, eventQuantity, eventPrice, owner)"), 1)
        self.assertLess(
            callback.index("protectedEntryExecutionFacts[executionId] = executionFact"),
            callback.index("SubmitProtectiveStop(order, eventQuantity, eventPrice, owner)"),
        )
        self.assertIn("PROTECTIVE_STOP_SUBMISSION_FAILED_", callback)
        self.assertNotIn("protectedEntryExecutionFacts.Remove", source)
        self.assertNotIn("protectedEntryCommandExecutions.Remove", source)
        self.assertNotIn("protectedEntryExecutionFacts.Clear", source)
        self.assertNotIn("protectedEntryCommandExecutions.Clear", source)
        self.assertNotIn("failedEntryProtectionCommands.Remove", source)
        self.assertNotIn("failedEntryProtectionCommands.Clear", source)

    def test_semantically_rejected_exact_sequence_is_consumed_for_later_safety_commands(self) -> None:
        source = self._execution_source()
        reservation = source[
            source.index("        private string ReserveCommandSequence"):
            source.index("        private string ValidateReservedCommand")
        ]
        exact_sequence = reservation.index("sequence != lastCommandSequence + 1")
        pending_claim = reservation.index(
            'commandOutcomes[outcomeKey] = new CommandOutcome(fingerprint, "PENDING", null)'
        )
        sequence_claim = reservation.index("lastCommandSequence = sequence")
        self.assertLess(exact_sequence, sequence_claim)
        self.assertLess(exact_sequence, pending_claim)
        self.assertLess(pending_claim, sequence_claim)
        for non_consuming_fence in (
            'return "NOT_AUTHENTICATED"',
            'return "WRONG_EXECUTION_SESSION"',
            'return "MISSING_COMMAND_ID"',
            'return "IDEMPOTENCY_CONFLICT"',
        ):
            self.assertLess(reservation.index(non_consuming_fence), pending_claim)

        validator = source[
            source.index("        private string ValidateReservedCommand"):
            source.index("        private static string CommandOutcomeKey")
        ]
        self.assertIn('pending.Status != "PENDING"', validator)
        self.assertIn('return "COMMAND_OUTCOME_NOT_PENDING"', validator)
        self.assertIn('return "LOCKED_OUT"', validator)
        self.assertIn('return "RECONCILIATION_REQUIRED"', validator)
        self.assertIn('return "AUTHORITY_HASH_MISMATCH"', validator)
        self.assertIn(
            '!reconciled && (action == "ENTER_LONG" || action == "ENTER_SHORT")',
            validator,
        )
        self.assertNotIn(
            'action != "RECONCILE" && action != "HEARTBEAT"',
            validator,
        )
        executor = source[
            source.index("        private void ExecuteCommand"):
            source.index("        private string ReserveCommandSequence")
        ]
        self.assertNotIn("lastCommandSequence = sequence", executor)
        self.assertIn('SetCommandOutcome(command, "REJECTED", refusal)', executor)
        self.assertIn('LockAndProtect("COMMAND_REJECTED_" + refusal)', executor)
        self.assertLess(
            executor.index('LockAndProtect("COMMAND_REJECTED_" + refusal)'),
            executor.index("Reject(command, refusal, commandId)"),
        )

        command_loop = source[
            source.index("        private void CommandLoop"):
            source.index("        private void ExecuteCommand")
        ]
        self.assertLess(
            command_loop.index("ReserveCommandSequence(command"),
            command_loop.index("RandomDispatcher.BeginInvoke"),
        )

    def test_pending_duplicate_is_never_replayed_as_an_acknowledgement(self) -> None:
        source = self._execution_source()
        enqueue = source[
            source.index("        private void EnqueueCommand"):
            source.index("        private void CommandLoop")
        ]
        self.assertRegex(
            enqueue,
            r'else\s+Diagnostic\("DUPLICATE_COMMAND_OUTCOME_PENDING"\);\s+return;',
        )
        pending_diagnostic = enqueue.index('Diagnostic("DUPLICATE_COMMAND_OUTCOME_PENDING")')
        pending_return = enqueue.index("return;", pending_diagnostic)
        self.assertNotIn("Acknowledge(", enqueue[pending_diagnostic:pending_return])
        self.assertNotIn("Reject(", enqueue[pending_diagnostic:pending_return])

        reservation = source[
            source.index("        private string ReserveCommandSequence"):
            source.index("        private string ValidateReservedCommand")
        ]
        self.assertIn(
            'if (prior.Status == "PENDING") return "COMMAND_OUTCOME_PENDING"',
            reservation,
        )
        self.assertLess(
            reservation.index('prior.Status == "PENDING"'),
            reservation.index('prior.Status == "ACCEPTED"'),
        )

        command_loop = source[
            source.index("        private void CommandLoop"):
            source.index("        private void ExecuteCommand")
        ]
        pending_branch = command_loop[
            command_loop.index('if (reservation == "COMMAND_OUTCOME_PENDING")'):
            command_loop.index('if (reservation == "DUPLICATE_ACCEPTED")')
        ]
        self.assertIn("continue;", pending_branch)
        self.assertNotIn("Acknowledge(", pending_branch)
        self.assertNotIn("Reject(", pending_branch)

    def test_command_outcomes_are_session_scoped_and_payload_conflicts_fail_closed(self) -> None:
        source = self._execution_source()
        outcome_helpers = source[
            source.index("        private static string CommandOutcomeKey"):
            source.index("        private bool HasOwnedActivity")
        ]
        self.assertIn('Text(command, "execution_session_id")', outcome_helpers)
        self.assertIn('Text(command, "command_id")', outcome_helpers)
        self.assertIn(' + "|" + ', outcome_helpers)
        self.assertIn("unsigned.Remove(\"signature\")", outcome_helpers)
        self.assertIn("SHA256.Create()", outcome_helpers)
        self.assertIn("Canonical(unsigned)", outcome_helpers)

        for section in (
            source[
                source.index("        private void EnqueueCommand"):
                source.index("        private void CommandLoop")
            ],
            source[
                source.index("        private string ReserveCommandSequence"):
                source.index("        private string ValidateReservedCommand")
            ],
        ):
            conflict = section.index("prior.Fingerprint")
            terminal_status = section.index("prior.Status")
            self.assertLess(conflict, terminal_status)
            self.assertIn("IDEMPOTENCY_CONFLICT", section[conflict:terminal_status])

    def test_socket_loss_cannot_disable_native_owned_activity_watchdog(self) -> None:
        source = self._execution_source()
        accept = source[
            source.index("        private void AcceptSession"):
            source.index("        private static bool HashText")
        ]
        connection = source[
            source.index("        private void ConnectionLoop"):
            source.index("        private void ReadFrames")
        ]
        stop = source[
            source.index("        private void StopPaperBoundary"):
            source.index("        private static Thread NewThread")
        ]
        watchdog = source[
            source.index("        private void WatchdogLoop"):
            source.index("        private void LockAndProtect")
        ]
        self.assertIn("watchdogSafetyAuthorityEstablished = true", accept)
        self.assertNotIn("watchdogSafetyAuthorityEstablished = false", connection)
        self.assertIn("watchdogSafetyAuthorityEstablished = false", stop)
        self.assertIn(
            "heartbeatLost = watchdogSafetyAuthorityEstablished && ownedActivity",
            watchdog,
        )
        self.assertNotIn("heartbeatLost = authenticated && ownedActivity", watchdog)
        self.assertIn('LockAndProtect("HEARTBEAT_WATCHDOG")', watchdog)

    def test_reconciliation_probe_is_signed_status_only_and_stably_double_sampled(self) -> None:
        source = self._execution_source()
        process = source[
            source.index("        private void ProcessFrame"):
            source.index("        private void AcceptSession")
        ]
        probe_dispatch = process.index(
            'String.Equals(type, "RECONCILIATION_PROBE_GRANT", StringComparison.Ordinal)'
        )
        normal_authority = process.index("if (!authenticated", probe_dispatch)
        self.assertLess(probe_dispatch, normal_authority)
        self.assertIn("AcceptReconciliationProbeGrant(message);", process)

        probe = source[
            source.index("        private void AcceptReconciliationProbeGrant"):
            source.index("        private ProbeNativeSample CaptureProbeNativeSample")
        ]
        expected_grant_fields = {
            "schema", "message_type", "probe_session_id", "server_nonce",
            "account_binding_hash", "mode", "live_capital", "timestamp", "signature",
        }
        grant_array = probe[
            probe.index("string[] exactFields"):
            probe.index("DateTime timestamp")
        ]
        self.assertEqual(set(re.findall(r'"([a-z_]+)"', grant_array)), expected_grant_fields)
        self.assertIn(
            'ExactAccountBindingHash = "28ddf4acc88f1a9e35de79b8306a252e647a5a1dca0a6e9333ce814828e6841e"',
            source,
        )
        self.assertIn('Text(grant, "account_binding_hash"), ExactAccountBindingHash', probe)
        self.assertIn('Text(grant, "mode"), "PAPER_SIM101"', probe)
        self.assertIn('Boolean(grant, "live_capital") != false', probe)
        self.assertIn("lock (nativeMutationGate)", probe)
        self.assertEqual(probe.count("CaptureProbeNativeSample();"), 2)
        for fence in (
            "nativeObservationCallbacksInFlight",
            "generationBefore = Interlocked.Read(ref nativeObservationGeneration)",
            "generationAfter = Interlocked.Read(ref nativeObservationGeneration)",
            "generationBefore == generationAfter",
            "String.Equals(first.Hash, second.Hash, StringComparison.Ordinal)",
            "!first.NativeMutationPending",
            "!second.NativeMutationPending",
        ):
            self.assertIn(fence, probe)

        exact_result_fields = {
            "schema", "message_type", "probe_session_id", "server_nonce",
            "observation_generation", "first_sample_hash", "second_sample_hash",
            "snapshot_stable", "timestamp", "receipt_id", "account_name",
            "account_class", "instrument", "position_quantity",
            "working_order_count", "working_entry_count",
            "position_snapshot_complete", "order_snapshot_complete",
            "foreign_activity", "protective_stop_state",
        }
        result_block = probe[
            probe.index("Dictionary<string, object> result"):
            probe.index("SendSigned(result);")
        ]
        self.assertEqual(
            set(re.findall(r'result\["([a-z_]+)"\]', result_block)),
            exact_result_fields,
        )
        self.assertIn('result["message_type"] = "RECONCILIATION_PROBE_RESULT"', result_block)
        self.assertIn('result["position_snapshot_complete"] = stable', result_block)
        self.assertIn('result["order_snapshot_complete"] = stable', result_block)
        self.assertNotIn("AcceptSession(", probe)
        self.assertNotIn("RehydrateOwnedWorkingOrders", probe)
        self.assertNotIn("EnsureCurrentPositionOwnershipProven", probe)
        self.assertNotIn("LockAndProtect", probe)
        self.assertNotIn("CreateOrder", probe)
        self.assertNotIn(".Submit(", probe)
        self.assertNotIn(".Cancel(", probe)
        for forbidden_assignment in (
            "authenticated =", "reconciled =", "executionSessionId =",
            "watchdogSafetyAuthorityEstablished =", "lastHeartbeatUtc =",
            "paperPolicyHash =", "riskProfileHash =", "accountBindingHash =",
        ):
            self.assertNotIn(forbidden_assignment, probe)

        capture = source[
            source.index("        private ProbeNativeSample CaptureProbeNativeSample"):
            source.index("        private static bool ExactFields")
        ]
        self.assertIn("lock (paperAccount.Positions)", capture)
        self.assertIn("lock (paperAccount.Orders)", capture)
        self.assertIn("UnresolvedNativeOrderState(order.OrderState)", capture)
        self.assertIn('hashPayload["native_mutation_pending"]', capture)
        self.assertIn("Canonical(hashPayload)", capture)

        callback_fence = source[
            source.index("        private void BeginNativeObservationCallback"):
            source.index("        private void OnOrderUpdate")
        ]
        self.assertIn("Interlocked.Increment(ref nativeObservationCallbacksInFlight)", callback_fence)
        self.assertEqual(callback_fence.count("Interlocked.Increment(ref nativeObservationGeneration)"), 2)
        self.assertIn("Interlocked.Decrement(ref nativeObservationCallbacksInFlight)", callback_fence)
        order_callback = source[
            source.index("        private void OnOrderUpdate"):
            source.index("        private void OnExecutionUpdate")
        ]
        execution_callback = source[
            source.index("        private void OnExecutionUpdate"):
            source.index("        private void OnPositionUpdate")
        ]
        position_callback = source[
            source.index("        private void OnPositionUpdate"):
            source.index("        private void OnAccountItemUpdate")
        ]
        self.assertEqual(order_callback.count("BeginNativeObservationCallback();"), 1)
        self.assertEqual(order_callback.count("EndNativeObservationCallback();"), 1)
        self.assertEqual(execution_callback.count("BeginNativeObservationCallback();"), 1)
        self.assertEqual(execution_callback.count("EndNativeObservationCallback();"), 2)
        self.assertEqual(position_callback.count("BeginNativeObservationCallback();"), 1)
        self.assertEqual(position_callback.count("EndNativeObservationCallback();"), 3)

    def test_native_quantity_breach_immediately_uses_independent_safety_flatten(self) -> None:
        source = self._execution_source()
        callback = source[
            source.index("        private void OnPositionUpdate"):
            source.index("        private void OnAccountItemUpdate")
        ]
        breach = callback.index("Math.Abs(quantity) > MaximumQuantity")
        safety = callback.index('LockAndProtect("MAXIMUM_QUANTITY_BREACH")')
        publish = callback.index('SessionMessage("POSITION_EVENT")')
        self.assertLess(breach, safety)
        self.assertLess(safety, publish)
        reconcile = source[
            source.index("        private bool SendReconciliation"):
            source.index("        private void TryPublishWatchdogSafetyReconciliation")
        ]
        self.assertIn("Math.Abs(quantity) > MaximumQuantity", reconcile)
        self.assertIn('LockAndProtect("MAXIMUM_QUANTITY_BREACH")', reconcile)
        watchdog = source[
            source.index("        private void WatchdogLoop"):
            source.index("        private void Acknowledge")
        ]
        self.assertIn("position.Quantity > MaximumQuantity", watchdog)
        self.assertIn('if (quantityBreach) LockAndProtect("MAXIMUM_QUANTITY_BREACH")', watchdog)
        self.assertIn("CancelOwnedOrders();", watchdog)
        self.assertNotIn('CancelOwnedOrders("EXIT")', watchdog)
        self.assertIn("SubmitOwnedFlattenOrder(", watchdog)
        self.assertNotIn(".Flatten(", watchdog)

    def test_native_safety_exit_is_claimed_once_and_preowned_before_submit(self) -> None:
        source = self._execution_source()
        begin_correlation = source[
            source.index("        private void BeginWatchdogSafetyCorrelation"):
            source.index("        private bool PublishWatchdogSafetyEvent")
        ]
        # If a commanded EXIT is already pending, the safety correlation adopts
        # that one mutation instead of creating a competing exit.
        self.assertIn(
            "watchdogSafetyFlattenSubmitted = flattenInProgress",
            begin_correlation,
        )
        self.assertIn(
            "pendingWatchdogSafetyExitRequired = flattenInProgress",
            begin_correlation,
        )

        watchdog = source[
            source.index("        private void LockAndProtect"):
            source.index("        private void Acknowledge")
        ]
        already_submitted = watchdog.index(
            "alreadySubmitted = watchdogSafetyFlattenSubmitted || flattenInProgress"
        )
        guard = watchdog.index("&& !alreadySubmitted", already_submitted)
        submit_helper = watchdog.index("SubmitOwnedFlattenOrder(", guard)
        self.assertLess(already_submitted, guard)
        self.assertLess(guard, submit_helper)
        self.assertEqual(watchdog.count("SubmitOwnedFlattenOrder("), 1)
        self.assertIn('commandId, "NATIVE_SAFETY", decisionId, true', watchdog)
        self.assertNotIn("watchdogSafetyFlattenSubmitted = false", watchdog)
        self.assertNotIn(".Flatten(", watchdog)

        helper = source[
            source.index("        private string SubmitOwnedFlattenOrder"):
            source.index("        private void CancelOwnedOrders")
        ]
        progress_fence = helper.index(
            'if (flattenInProgress) return "FLATTEN_ALREADY_IN_PROGRESS"'
        )
        mutation_claim = helper.index("flattenInProgress = true", progress_fence)
        safety_claim = helper.index("watchdogSafetyFlattenSubmitted = true", mutation_claim)
        create = helper.index("paperAccount.CreateOrder(", mutation_claim)
        owner = helper.index('name, "EXIT", order', create)
        register = helper.index("ownedByName[name] = owner", owner)
        submit = helper.index("paperAccount.Submit(new[] { order })", register)
        self.assertLess(progress_fence, mutation_claim)
        self.assertLess(mutation_claim, create)
        self.assertLess(safety_claim, create)
        self.assertLess(create, owner)
        self.assertLess(owner, register)
        self.assertLess(register, submit)
        self.assertEqual(helper.count("paperAccount.Submit(new[] { order })"), 1)
        self.assertIn('string name = "BZ-L3G-X-" + Fragment(commandId)', helper)

    def test_foreign_activity_is_sticky_across_reconciliation_snapshots(self) -> None:
        source = self._execution_source()
        reconcile = source[
            source.index("        private bool SendReconciliation"):
            source.index("        private void TryPublishWatchdogSafetyReconciliation")
        ]
        latch = reconcile.index("foreignActivity = foreignActivity || foreign")
        read_sticky = reconcile.index("foreign = foreignActivity", latch)
        authority = reconcile.index("reconciled = !foreign", read_sticky)
        receipt = reconcile.index('message["foreign_activity"] = foreign', authority)
        self.assertLess(latch, read_sticky)
        self.assertLess(read_sticky, authority)
        self.assertLess(authority, receipt)
        self.assertNotIn("foreignActivity = foreign;", reconcile)
        # The lifetime fact is reset only when a new AddOn boundary starts, not
        # by reconnect or by a later clean-looking account snapshot.
        self.assertEqual(source.count("foreignActivity = false;"), 1)

    def test_position_before_exit_callbacks_cannot_release_flatten_settlement(self) -> None:
        source = self._execution_source()
        settlement = source[
            source.index("        private void SettleFlattenOwnershipIfPossible"):
            source.index("        private Position CurrentPosition")
        ]
        open_guard = settlement.index("if (!flattenInProgress || positionOpen) return")
        callback_facts = settlement.index("if (!pendingFlattenExecutionObserved")
        self.assertIn("!pendingFlattenOrderTerminalObserved", settlement[callback_facts:])
        self.assertIn("pendingFlattenProtectiveCancellationExpected", settlement[callback_facts:])
        clear = settlement.index("flattenInProgress = false")
        self.assertLess(open_guard, callback_facts)
        self.assertLess(callback_facts, clear)

        position_callback = source[
            source.index("        private void OnPositionUpdate"):
            source.index("        private void OnAccountItemUpdate")
        ]
        self.assertEqual(
            position_callback.count("SettleFlattenOwnershipIfPossible(quantity != 0)"),
            1,
        )
        self.assertLess(
            position_callback.index("if (authenticated)"),
            position_callback.index("SettleFlattenOwnershipIfPossible(quantity != 0)"),
        )

        order_callback = source[
            source.index("        private void OnOrderUpdate"):
            source.index("        private void OnExecutionUpdate")
        ]
        terminal_fact = order_callback.index(
            "pendingFlattenOrderTerminalObserved = true"
        )
        order_settlement = order_callback.index(
            "SettleFlattenOwnershipIfPossible", terminal_fact
        )
        self.assertLess(terminal_fact, order_settlement)

        execution_callback = source[
            source.index("        private void OnExecutionUpdate"):
            source.index("        private void OnPositionUpdate")
        ]
        execution_fact = execution_callback.index(
            "pendingFlattenExecutionObserved = true"
        )
        execution_settlement = execution_callback.index(
            "SettleFlattenOwnershipIfPossible", execution_fact
        )
        self.assertLess(execution_fact, execution_settlement)
        self.assertIn("pendingFlattenExecutionIds.Add(executionId)", execution_callback)
        self.assertIn(
            "pendingFlattenFilledQuantity == pendingFlattenExpectedQuantity",
            execution_callback,
        )
        self.assertIn(
            "pendingFlattenFilledQuantity + eventQuantity > pendingFlattenExpectedQuantity",
            execution_callback,
        )

        reconciliation = source[
            source.index("        private bool SendReconciliation"):
            source.index("        private void TryPublishWatchdogSafetyReconciliation")
        ]
        exit_required = reconciliation.index("pendingWatchdogSafetyExitRequired")
        complete_execution = reconciliation.index(
            "!pendingFlattenExecutionObserved", exit_required
        )
        terminal_order = reconciliation.index(
            "!pendingFlattenOrderTerminalObserved", complete_execution
        )
        final_receipt = reconciliation.index(
            'message["safety_settlement_final"]', terminal_order
        )
        self.assertLess(exit_required, complete_execution)
        self.assertLess(complete_execution, terminal_order)
        self.assertLess(terminal_order, final_receipt)

    def test_preowned_exit_is_not_counted_as_a_working_entry(self) -> None:
        source = self._execution_source()
        reconcile = source[
            source.index("        private bool SendReconciliation"):
            source.index("        private void TryPublishWatchdogSafetyReconciliation")
        ]
        resolve_owner = reconcile.index(
            "ownedByName.TryGetValue(order.Name ?? String.Empty, out owner)"
        )
        owned = reconcile.index("bool owned = owner != null", resolve_owner)
        working = reconcile.index("if (exact) working++", owned)
        entry_working = reconcile.index(
            'owner != null && owner.Role == "ENTRY"', working
        )
        self.assertLess(resolve_owner, owned)
        self.assertLess(owned, working)
        self.assertLess(working, entry_working)
        self.assertNotIn('owner == null || owner.Role == "ENTRY"', reconcile)

        helper = source[
            source.index("        private string SubmitOwnedFlattenOrder"):
            source.index("        private void CancelOwnedOrders")
        ]
        self.assertIn('name, "EXIT", order', helper)
        self.assertLess(
            helper.index("ownedByName[name] = owner"),
            helper.index("paperAccount.Submit(new[] { order })"),
        )

    def test_unidentified_entry_command_is_tombstoned_before_any_later_claim(self) -> None:
        source = self._execution_source()
        callback = source[
            source.index("        private void OnExecutionUpdate"):
            source.index("        private void OnPositionUpdate")
        ]
        missing_id = callback.index(
            "String.IsNullOrWhiteSpace(executionId) || String.IsNullOrWhiteSpace(commandId)"
        )
        tombstone_check = callback.index("failedEntryProtectionCommands.Contains(commandId)")
        execution_fact_check = callback.index(
            "protectedEntryExecutionFacts.TryGetValue(executionId, out priorFact)"
        )
        execution_claim = callback.index("protectedEntryExecutionFacts[executionId] = executionFact")
        protective_submit = callback.index(
            "SubmitProtectiveStop(order, eventQuantity, eventPrice, owner)"
        )
        self.assertIn(
            "conflictingExecution = true;",
            callback[missing_id:tombstone_check],
        )
        self.assertIn(
            "conflictingExecution = true;",
            callback[tombstone_check:execution_fact_check],
        )
        self.assertLess(missing_id, tombstone_check)
        self.assertLess(tombstone_check, execution_fact_check)
        self.assertLess(execution_fact_check, execution_claim)
        self.assertLess(execution_claim, protective_submit)
        conflict_start = callback.index("if (conflictingExecution)\n                    {")
        conflict_tombstone = callback[
            conflict_start:
            callback.index("if (conflictingExecution)", conflict_start + 1)
        ]
        self.assertIn("owner.OutcomeUnknown = true", conflict_tombstone)
        self.assertIn("failedEntryProtectionCommands.Add(commandId)", conflict_tombstone)

    def test_embedded_addon_and_perpetual_policy_fingerprints_are_exact(self) -> None:
        source = self._execution_source()
        source_fingerprint = re.search(
            r'private const string AddonSourceFingerprint = "([0-9a-f]{64})";',
            source,
        )
        perpetual_policy = re.search(
            r'private const string PerpetualPolicyHash = "([0-9a-f]{64})";',
            source,
        )
        self.assertIsNotNone(source_fingerprint)
        self.assertIsNotNone(perpetual_policy)
        self.assertEqual(expected_addon_source_fingerprint(), EXPECTED_ADDON_SOURCE_FINGERPRINT)
        self.assertEqual(source_fingerprint.group(1), EXPECTED_ADDON_SOURCE_FINGERPRINT)  # type: ignore[union-attr]
        self.assertEqual(perpetual_policy.group(1), FIVE_MINUTE_PERPETUAL_POLICY.configuration_hash)  # type: ignore[union-attr]

    def test_native_order_rejection_diagnostics_are_signed_into_order_events(self) -> None:
        source = self._execution_source()
        callback = source[
            source.index("        private void OnOrderUpdate"):
            source.index("        private void OnExecutionUpdate")
        ]
        self.assertIn('message["native_error_code"] = e.Error.ToString().ToUpperInvariant()', callback)
        self.assertIn('message["native_error_comment"] = e.Comment ?? String.Empty', callback)
        self.assertLess(callback.index('message["native_error_code"]'), callback.index("SendSigned(message)"))

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
        self.assertNotIn('CancelOwnedOrders("EXIT")', watchdog)
        self.assertIn("if (foreign)", watchdog)
        self.assertIn("else if (position != null && position.Quantity != 0", watchdog)
        self.assertIn("SubmitOwnedFlattenOrder(", watchdog)
        self.assertNotIn(".Flatten(", watchdog)
        flatten = source[
            source.index("        private string FlattenOwnedInstrument"):
            source.index("        private string SubmitOwnedFlattenOrder")
        ]
        self.assertIn("FOREIGN_ACTIVITY_FLATTEN_REFUSED", flatten)
        self.assertIn('return "FOREIGN_ACTIVITY_LOCKOUT"', flatten)
        executor = source[
            source.index("        private void ExecuteCommand"):
            source.index("        private string ReserveCommandSequence")
        ]
        self.assertIn("actionRefusal = FlattenOwnedInstrument", executor)
        self.assertIn('SetCommandOutcome(command, "REJECTED", actionRefusal)', executor)
        self.assertNotIn('Acknowledge(command, "ACCEPTED", false)', flatten)

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

    def test_all_nonterminal_ninjatrader_order_states_remain_active(self) -> None:
        source = self._execution_source()
        working = source[
            source.index("        private static bool Working"):
            source.index("        private void SettleFlattenOwnershipIfPossible")
        ]
        self.assertIn("return !Order.IsTerminalState(state);", working)
        for brittle_state in (
            "OrderState.Initialized",
            "OrderState.Submitted",
            "OrderState.Accepted",
            "OrderState.Working",
            "OrderState.Suspended",
            "OrderState.AcceptedByRisk",
        ):
            self.assertNotIn(brittle_state, working)

        rehydrate = source[
            source.index("        private void RehydrateOwnedWorkingOrders"):
            source.index("        private static bool Working")
        ]
        self.assertIn("UnresolvedNativeOrderState(order.OrderState)", rehydrate)
        self.assertIn("owner.OutcomeUnknown = true", rehydrate)
        self.assertIn('LockAndProtect("RESTORED_OWNED_ORDER_OUTCOME_UNKNOWN")', rehydrate)
        unresolved = source[
            source.index("        private static bool UnresolvedNativeOrderState"):
            source.index("        private void SettleFlattenOwnershipIfPossible")
        ]
        self.assertIn("Working(state) || state == OrderState.Unknown", unresolved)

    def test_native_callbacks_use_immutable_event_snapshots(self) -> None:
        source = self._execution_source()
        order_callback = source[
            source.index("        private void OnOrderUpdate"):
            source.index("        private void OnExecutionUpdate")
        ]
        for binding in (
            "OrderState eventState = e.OrderState",
            "int eventFilled = e.Filled",
            "int eventQuantity = e.Quantity",
            "string eventOrderId = e.OrderId",
        ):
            self.assertIn(binding, order_callback)
        for mutable_fact in ("order.OrderState", "order.Filled", "order.OrderId"):
            self.assertNotIn(mutable_fact, order_callback)

        execution_callback = source[
            source.index("        private void OnExecutionUpdate"):
            source.index("        private void OnPositionUpdate")
        ]
        for binding in (
            "string executionId = e.ExecutionId",
            "string eventOrderId = e.OrderId",
            "int eventQuantity = e.Quantity",
            "double eventPrice = e.Price",
        ):
            self.assertIn(binding, execution_callback)
        for mutable_fact in (
            "e.Execution.ExecutionId",
            "e.Execution.Quantity",
            "e.Execution.Price",
            "order.OrderId",
        ):
            self.assertNotIn(mutable_fact, execution_callback)

        position_callback = source[
            source.index("        private void OnPositionUpdate"):
            source.index("        private void OnAccountItemUpdate")
        ]
        for binding in (
            "int eventQuantity = e.Quantity",
            "MarketPosition eventMarketPosition = e.MarketPosition",
            "double eventAveragePrice = e.AveragePrice",
        ):
            self.assertIn(binding, position_callback)
        for mutable_fact in (
            "position.Quantity",
            "position.MarketPosition",
            "position.AveragePrice",
        ):
            self.assertNotIn(mutable_fact, position_callback)

    def test_foreign_or_flat_safety_cancels_every_owned_role(self) -> None:
        source = self._execution_source()
        self.assertNotIn('CancelOwnedOrders("EXIT")', source)
        cancellation = source[
            source.index("        private void CancelOwnedOrders"):
            source.index("        private List<Order> OwnedWorkingOrders")
        ]
        self.assertNotIn("exceptRole", cancellation)
        self.assertIn("UnresolvedNativeOrderState(value.Order.OrderState)", cancellation)

        termination = source[
            source.index("        private bool ProtectBoundaryTermination"):
            source.index("        private static Thread NewThread")
        ]
        foreign = termination.index("if (foreign)")
        foreign_cancel = termination.index("CancelOwnedOrders();", foreign)
        open_submit = termination.index("else if (positionOpen && !submitted)", foreign_cancel)
        flat = termination.index("else if (!positionOpen)", open_submit)
        flat_cancel = termination.index("CancelOwnedOrders();", flat)
        self.assertLess(foreign, foreign_cancel)
        self.assertLess(foreign_cancel, open_submit)
        self.assertLess(open_submit, flat)
        self.assertLess(flat, flat_cancel)

        watchdog = source[
            source.index("        private void LockAndProtect"):
            source.index("        private void Acknowledge")
        ]
        foreign = watchdog.index("if (foreign)")
        foreign_cancel = watchdog.index("CancelOwnedOrders();", foreign)
        open_submit = watchdog.index("else if (position != null && position.Quantity != 0", foreign_cancel)
        flat = watchdog.index("else if (position == null || position.Quantity == 0)", open_submit)
        flat_cancel = watchdog.index("CancelOwnedOrders();", flat)
        self.assertLess(foreign_cancel, open_submit)
        self.assertLess(open_submit, flat_cancel)

    def test_unknown_owned_outcomes_are_tombstoned_for_every_role(self) -> None:
        source = self._execution_source()
        callback = source[
            source.index("        private void OnOrderUpdate"):
            source.index("        private void OnExecutionUpdate")
        ]
        self.assertIn("eventState == OrderState.Unknown", callback)
        self.assertIn("owner.OutcomeUnknown = true", callback)
        for reason in (
            "OWNED_ENTRY_ORDER_OUTCOME_UNKNOWN",
            "OWNED_PROTECTIVE_ORDER_OUTCOME_UNKNOWN",
            "OWNED_EXIT_ORDER_OUTCOME_UNKNOWN",
        ):
            self.assertIn(reason, callback)
        self.assertIn("LockAndProtect(safetyReason)", callback)
        termination = source[
            source.index("        private bool TerminationNeedsCallbackRetention"):
            source.index("        private static Thread NewThread")
        ]
        self.assertIn("HasAmbiguousOwnedOutcomeUnderLock()", termination)
        ambiguity = source[
            source.index("        private bool HasAmbiguousOwnedOutcomeUnderLock"):
            source.index("        private static void TrySettleEntryLifecycleUnderLock")
        ]
        self.assertIn("owner.OutcomeUnknown", ambiguity)
        self.assertIn("owner.Order.OrderState == OrderState.Unknown", ambiguity)

        flatten = source[
            source.index("        private string SubmitOwnedFlattenOrder"):
            source.index("        private void CancelOwnedOrders")
        ]
        self.assertIn('return "OWNED_ORDER_OUTCOME_UNKNOWN"', flatten)
        cancellation = source[
            source.index("        private void CancelOwnedOrders"):
            source.index("        private List<Order> OwnedWorkingOrders")
        ]
        self.assertIn("UnresolvedNativeOrderState(value.Order.OrderState)", cancellation)
        for safety in (
            source[
                source.index("        private bool ProtectBoundaryTermination"):
                source.index("        private static Thread NewThread")
            ],
            source[
                source.index("        private void LockAndProtect"):
                source.index("        private void Acknowledge")
            ],
        ):
            ambiguous = safety.index("if (ambiguousOwned)")
            cancel = safety.index("CancelOwnedOrders();", ambiguous)
            foreign = safety.index("else if (foreign)", cancel)
            submit = safety.index("SubmitOwnedFlattenOrder(", foreign)
            self.assertLess(ambiguous, cancel)
            self.assertLess(cancel, foreign)
            self.assertLess(foreign, submit)

    def test_entry_lifecycle_blocks_pristine_shutdown_until_exposure_is_handled(self) -> None:
        source = self._execution_source()
        submit = source[
            source.index("        private void SubmitEntry"):
            source.index("        private void SubmitProtectiveStop")
        ]
        claim = submit.index('OwnedOrder.FromCommand(command, name, "ENTRY", null)')
        register = submit.index("ownedByName[name] = owner", claim)
        create = submit.index("paperAccount.CreateOrder", register)
        native_submit = submit.index("paperAccount.Submit", create)
        self.assertLess(claim, register)
        self.assertLess(register, create)
        self.assertLess(create, native_submit)
        self.assertIn("reconciled = false", submit[register:create])

        settlement = source[
            source.index("        private static void TrySettleEntryLifecycleUnderLock"):
            source.index("        private void MarkPendingEntryExposureHandledByExitUnderLock")
        ]
        for prerequisite in (
            "EntryOrderFilledTerminalObserved",
            "EntryExecutionObserved",
            "EntryPositionObserved",
            "EntryExposureHandled",
        ):
            self.assertIn(prerequisite, settlement)
        self.assertIn("EntryNoFillTerminalObserved", settlement)

        stop = source[
            source.index("        private void StopPaperBoundary"):
            source.index("        private static Thread NewThread")
        ]
        self.assertIn("HasUnsettledEntryLifecycleUnderLock()", stop)
        self.assertIn("terminationCallbacksRetained = retainCallbacks", stop)
        self.assertIn("if (!retainCallbacks)\n                DetachAccountCallbacks();", stop)
        self.assertIn("TryFinalizeRetainedTermination()", stop)

        order_callback = source[
            source.index("        private void OnOrderUpdate"):
            source.index("        private void OnExecutionUpdate")
        ]
        filled = order_callback.index("eventState == OrderState.Filled")
        filled_terminal = order_callback.index("owner.EntryOrderFilledTerminalObserved = true", filled)
        self.assertNotIn("EntrySettlementComplete = true", order_callback[filled:filled_terminal + 80])
        self.assertIn("TrySettleEntryLifecycleUnderLock(owner)", order_callback[filled_terminal:])

        execution_callback = source[
            source.index("        private void OnExecutionUpdate"):
            source.index("        private void OnPositionUpdate")
        ]
        self.assertIn("owner.EntryExecutionObserved = true", execution_callback)
        self.assertIn("terminationExitClaimed = owner.EntryExposureHandled", execution_callback)
        self.assertIn("stopping && activeFlattenOwner != null", execution_callback)
        self.assertIn("activeFlattenOwner.Order != null", execution_callback)
        self.assertIn("if (!terminationExitClaimed)", execution_callback)
        self.assertNotIn("PROTECTIVE_STOP_SUPERSEDED_BY_TERMINATION", execution_callback)

        flatten = source[
            source.index("        private string SubmitOwnedFlattenOrder"):
            source.index("        private void CancelOwnedOrders")
        ]
        exit_create = flatten.index("paperAccount.CreateOrder")
        exit_register = flatten.index("ownedByName[name] = owner", exit_create)
        exposure_claim = flatten.index(
            "MarkPendingEntryExposureHandledByExitUnderLock(signedPositionQuantity)",
            exit_register,
        )
        exit_submit = flatten.index("paperAccount.Submit", exposure_claim)
        self.assertLess(exit_create, exit_register)
        self.assertLess(exit_register, exposure_claim)
        self.assertLess(exposure_claim, exit_submit)
        self.assertNotIn("EntryExposureHandled = false", source)

        position_callback = source[
            source.index("        private void OnPositionUpdate"):
            source.index("        private void OnAccountItemUpdate")
        ]
        self.assertIn("EntryPositionObserved = true", position_callback)
        self.assertIn("lock (stateLock) terminating = stopping", position_callback)
        self.assertIn("else if (terminating && quantity != 0)", position_callback)
        self.assertIn('LockAndProtect("ADDON_TERMINATING_LATE_ENTRY_FILL")', position_callback)

    def test_nonzero_position_requires_current_cycle_signed_ownership(self) -> None:
        source = self._execution_source()
        ownership = source[
            source.index("        private bool EnsureCurrentPositionOwnershipProven"):
            source.index("        private bool HasUnsettledEntryLifecycle")
        ]
        self.assertIn("ownedPositionEstablished", ownership)
        self.assertIn("ownedPositionSignedQuantity == signedQuantity", ownership)
        self.assertIn('!String.Equals(owner.CommandId, "RESTORED"', ownership)
        self.assertIn("owner.EntryExecutionObserved || owner.EntryOrderFilledTerminalObserved", ownership)
        self.assertIn("EntryDirectionMatchesPosition(owner, signedQuantity)", ownership)
        self.assertIn("!owner.EntryPositionOwnershipInvalidated", ownership)
        self.assertIn("owner.EntryPositionGeneration == positionOwnershipGeneration", ownership)
        self.assertIn("foreignActivity = true", ownership)
        self.assertIn('Diagnostic("POSITION_OWNERSHIP_UNPROVEN")', ownership)

        accept = source[
            source.index("        private void AcceptSession"):
            source.index("        private static bool HashText")
        ]
        self.assertLess(
            accept.index("EnsureCurrentPositionOwnershipProven();"),
            accept.index("authenticated = true"),
        )
        reconcile = source[
            source.index("        private bool SendReconciliation"):
            source.index("        private void TryPublishWatchdogSafetyReconciliation")
        ]
        self.assertIn("EnsureCurrentPositionOwnershipProven();", reconcile)
        flatten = source[
            source.index("        private string SubmitOwnedFlattenOrder"):
            source.index("        private void TryRearmProtectionAfterDefinitiveFlattenNoFill")
        ]
        mutation_fence = flatten.index(
            "if (!EnsureCurrentPositionOwnershipProven(signedPositionQuantity))"
        )
        native_create = flatten.index("paperAccount.CreateOrder", mutation_fence)
        self.assertLess(mutation_fence, native_create)
        self.assertIn('return "POSITION_OWNERSHIP_UNPROVEN"', flatten)

        position = source[
            source.index("        private void OnPositionUpdate"):
            source.index("        private void OnAccountItemUpdate")
        ]
        self.assertIn("EntryDirectionMatchesPosition(entry, quantity)", position)
        self.assertIn("entry.EntryPositionSignedQuantity = quantity", position)
        self.assertIn("positionOwnershipGeneration++", position)
        self.assertIn("entry.EntryPositionOwnershipInvalidated = true", position)
        self.assertIn("entry.EntryPositionGeneration = positionOwnershipGeneration", position)
        self.assertIn("entry.EntryPositionOwnershipInvalidated", position)
        self.assertIn("ownedPositionSignedQuantity = 0", position)
        self.assertIn('"POSITION_OWNERSHIP_QUANTITY_CHANGED"', position)
        self.assertIn('"POSITION_OWNERSHIP_DIRECTION_CHANGED"', position)

    def test_callbacks_require_exact_instrument_and_preowned_order_reference(self) -> None:
        source = self._execution_source()
        order_callback = source[
            source.index("        private void OnOrderUpdate"):
            source.index("        private void OnExecutionUpdate")
        ]
        self.assertIn("if (!exact || !ownedByName.TryGetValue", order_callback)
        self.assertIn("claimed.Order == null", order_callback)
        self.assertIn("!Object.ReferenceEquals(claimed.Order, order)", order_callback)
        self.assertIn('safetyReason = "FOREIGN_ORDER_IDENTITY_COLLISION"', order_callback)
        self.assertNotIn("OwnedOrder.Restored(order)", order_callback)
        self.assertNotIn("owner.Order = order", order_callback)

        execution_callback = source[
            source.index("        private void OnExecutionUpdate"):
            source.index("        private void OnPositionUpdate")
        ]
        self.assertIn("if (exact && claimed != null && claimed.Order != null", execution_callback)
        self.assertIn("Object.ReferenceEquals(claimed.Order, order)", execution_callback)
        self.assertIn("order.OrderAction != OrderAction.Buy", execution_callback)
        self.assertIn("order.OrderAction != OrderAction.SellShort", execution_callback)
        self.assertEqual(
            execution_callback.count("SubmitProtectiveStop(order, eventQuantity, eventPrice, owner)"),
            1,
        )

        rehydrate = source[
            source.index("        private void RehydrateOwnedWorkingOrders"):
            source.index("        private static bool Working")
        ]
        self.assertIn("owner.Order != null && !Object.ReferenceEquals(owner.Order, order)", rehydrate)
        self.assertIn("restoredUnknownOutcome = true", rehydrate)

    def test_oco_terminal_permutations_are_fail_closed_and_order_independent(self) -> None:
        source = self._execution_source()
        callback = source[
            source.index("        private void OnOrderUpdate"):
            source.index("        private void OnExecutionUpdate")
        ]
        cancellation = callback.index("pendingFlattenProtectiveCancellationObserved = true")
        exit_no_fill = callback.index("pendingFlattenExitNoFillTerminalObserved = true")
        self.assertNotIn(
            "pendingFlattenProtectiveOrder = null",
            callback[cancellation:exit_no_fill],
        )
        self.assertIn("pendingFlattenCompletedByProtective = true", callback)
        self.assertIn("pendingFlattenExitFilledTerminalObserved = true", callback)
        self.assertIn("pendingFlattenProtectiveFilledTerminalObserved = true", callback)
        self.assertGreaterEqual(callback.count('safetyReason = "MULTIPLE_FLATTEN_LEGS_FILLED"'), 2)
        self.assertIn("pendingFlattenExitExecutionQuantity == 0", callback)
        self.assertIn(
            "if (!pendingFlattenCompletedByProtective\n"
            "                                && (!pendingFlattenProtectiveCancellationExpected",
            callback,
        )

        settlement = source[
            source.index("        private void SettleFlattenOwnershipIfPossible"):
            source.index("        private bool DeferReconciliationUntilFlattenSettled")
        ]
        self.assertIn("pendingFlattenCompletedByProtective", settlement)
        self.assertIn("pendingFlattenExitNoFillTerminalObserved", settlement)
        self.assertIn("pendingFlattenProtectiveCancellationObserved", settlement)
        self.assertIn("HasAmbiguousOwnedOutcomeUnderLock()", settlement)

        execution = source[
            source.index("        private void OnExecutionUpdate"):
            source.index("        private void OnPositionUpdate")
        ]
        self.assertIn('else if (owner.Role == "PROTECTIVE")', execution)
        self.assertIn("pendingFlattenExecutionIds.Add(executionId)", execution)
        self.assertIn("pendingFlattenExitExecutionQuantity += eventQuantity", execution)
        self.assertIn("pendingFlattenProtectiveExecutionQuantity += eventQuantity", execution)
        self.assertIn("pendingFlattenExitNoFillTerminalObserved", execution)

    def test_terminal_no_fill_rearms_one_stop_but_never_retries_exit(self) -> None:
        source = self._execution_source()
        helper = source[
            source.index("        private void TryRearmProtectionAfterDefinitiveFlattenNoFill"):
            source.index("        private void CancelOwnedOrders")
        ]
        self.assertIn("pendingFlattenRecoveryProtectionSubmitted = true", helper)
        self.assertIn("pendingFlattenRecoveryProtectionDispatchPending = true", helper)
        self.assertIn("!HasEntryCapableOfAddingExposureUnderLock()", helper)
        self.assertIn("!EntryExposureFullyAccountedUnderLock(value)", helper)
        self.assertIn('string name = "BZ-L3G-S-" + Fragment(commandId) + "-R"', helper)
        self.assertIn('"PROTECTIVE", stop, DateTime.UtcNow', helper)
        self.assertIn("paperAccount.Submit(new[] { stop })", helper)
        self.assertNotIn("SubmitOwnedFlattenOrder", helper)
        self.assertNotIn('"EXIT"', helper)
        create_failure = helper[
            helper.index('Diagnostic("FLATTEN_NO_FILL_PROTECTION_CREATE_FAILED_') - 220:
            helper.index('Diagnostic("FLATTEN_NO_FILL_PROTECTION_CREATE_FAILED_') + 180
        ]
        self.assertIn("pendingFlattenRecoveryProtectionDispatchPending = false", create_failure)
        owner_register = helper.index("ownedByName[name] = protection")
        durable_claim = helper.index(
            "pendingFlattenRecoveryProtectionSubmitted = true", owner_register
        )
        submit = helper.index("paperAccount.Submit(new[] { stop })", durable_claim)
        submit_unknown = helper.index("FLATTEN_NO_FILL_PROTECTION_SUBMIT_UNKNOWN_", submit)
        self.assertLess(owner_register, durable_claim)
        self.assertLess(durable_claim, submit)
        self.assertNotIn(
            "pendingFlattenRecoveryProtectionSubmitted = false",
            helper[submit:submit_unknown],
        )

        watchdog = source[
            source.index("        private void WatchdogLoop"):
            source.index("        private void LockAndProtect")
        ]
        self.assertIn("TryRearmProtectionAfterDefinitiveFlattenNoFill();", watchdog)
        position = source[
            source.index("        private void OnPositionUpdate"):
            source.index("        private void OnAccountItemUpdate")
        ]
        self.assertIn("flatWithOwnedReducingOrder", position)
        self.assertIn('owner.Role == "PROTECTIVE" || owner.Role == "EXIT"', position)
        self.assertIn('LockAndProtect("FLAT_WITH_OWNED_REDUCING_ORDER")', position)
        accounting = source[
            source.index("        private bool EntryExposureFullyAccountedUnderLock"):
            source.index("        private bool HasAmbiguousOwnedOutcomeUnderLock")
        ]
        for fact in (
            "owner.EntryExecutionObserved",
            "owner.EntryPositionObserved",
            "owner.EntryExposureHandled",
            "!owner.EntryPositionOwnershipInvalidated",
            "owner.EntryPositionGeneration == positionOwnershipGeneration",
            "ownedPositionEstablished",
            "EntryDirectionMatchesPosition(owner, ownedPositionSignedQuantity)",
        ):
            self.assertIn(fact, accounting)

        flatten = source[
            source.index("        private string SubmitOwnedFlattenOrder"):
            source.index("        private void TryRearmProtectionAfterDefinitiveFlattenNoFill")
        ]
        quantity_guard = flatten.index("if (positionQuantity != MaximumQuantity)")
        create = flatten.index("paperAccount.CreateOrder", quantity_guard)
        self.assertLess(quantity_guard, create)
        self.assertIn('return "POSITION_QUANTITY_BREACH"', flatten)
        self.assertEqual(flatten.count("paperAccount.Submit(new[] { order })"), 1)

    def test_proven_create_failures_release_only_pre_submit_claims(self) -> None:
        source = self._execution_source()
        entry = source[
            source.index("        private void SubmitEntry"):
            source.index("        private void SubmitProtectiveStop")
        ]
        create = entry.index("paperAccount.CreateOrder")
        no_fill = entry.index("owner.EntryNoFillTerminalObserved = true", create)
        submit = entry.index("paperAccount.Submit(new[] { order })", no_fill)
        self.assertLess(create, no_fill)
        self.assertLess(no_fill, submit)
        self.assertIn("TrySettleEntryLifecycleUnderLock(owner)", entry[no_fill:submit])

        flatten = source[
            source.index("        private string SubmitOwnedFlattenOrder"):
            source.index("        private void TryRearmProtectionAfterDefinitiveFlattenNoFill")
        ]
        create = flatten.index("paperAccount.CreateOrder")
        release = flatten.index("ReleasePreSubmitFlattenClaimUnderLock(commandId)", create)
        refusal = flatten.index('return "FLATTEN_CREATE_FAILED_"', release)
        register = flatten.index("ownedByName[name] = owner", refusal)
        submit = flatten.index("paperAccount.Submit(new[] { order })", register)
        self.assertLess(create, release)
        self.assertLess(release, refusal)
        self.assertLess(refusal, register)
        self.assertLess(register, submit)
        self.assertNotIn("flattenInProgress = false", flatten[register:submit])

        release_helper = source[
            source.index("        private void ReleasePreSubmitFlattenClaimUnderLock"):
            source.index("        private string SubmitOwnedFlattenOrder")
        ]
        self.assertIn("activeFlattenOwner != null", release_helper)
        self.assertIn("flattenInProgress = false", release_helper)
        self.assertIn("pendingFlattenCommandId = null", release_helper)

    def test_protective_and_exit_submits_recheck_signed_position_after_create(self) -> None:
        source = self._execution_source()
        protection = source[
            source.index("        private bool EntryProtectionPositionMatches"):
            source.index("        private string FlattenOwnedInstrument")
        ]
        self.assertIn("signedQuantity != expectedSignedQuantity", protection)
        self.assertIn("entry.EntryPositionGeneration != positionOwnershipGeneration", protection)
        create = protection.index("paperAccount.CreateOrder")
        after_create = protection.index(
            "EntryProtectionPositionMatches(entry, expectedSignedQuantity)", create
        )
        register = protection.index("ownedByName[name] = protection", after_create)
        before_submit = protection.index(
            "EntryProtectionPositionMatches(entry, expectedSignedQuantity)", register
        )
        submit = protection.index("paperAccount.Submit(new[] { stop })", before_submit)
        self.assertLess(create, after_create)
        self.assertLess(after_create, register)
        self.assertLess(register, before_submit)
        self.assertLess(before_submit, submit)

        flatten = source[
            source.index("        private string SubmitOwnedFlattenOrder"):
            source.index("        private void TryRearmProtectionAfterDefinitiveFlattenNoFill")
        ]
        create = flatten.index("paperAccount.CreateOrder")
        post_create_position = flatten.index("Position preSubmitPosition = CurrentPosition()", create)
        state_admission = flatten.index("lock (stateLock)", post_create_position)
        owned_latch = flatten.index("!ownedPositionEstablished", state_admission)
        signed_latch = flatten.index(
            "ownedPositionSignedQuantity != signedPositionQuantity", owned_latch
        )
        register = flatten.index("ownedByName[name] = owner", post_create_position)
        submit = flatten.index("paperAccount.Submit(new[] { order })", register)
        self.assertLess(create, post_create_position)
        self.assertLess(post_create_position, state_admission)
        self.assertLess(state_admission, owned_latch)
        self.assertLess(owned_latch, signed_latch)
        self.assertLess(signed_latch, register)
        self.assertLess(post_create_position, register)
        self.assertLess(register, submit)

        rearm = source[
            source.index("        private void TryRearmProtectionAfterDefinitiveFlattenNoFill"):
            source.index("        private void CancelOwnedOrders")
        ]
        create = rearm.index("paperAccount.CreateOrder")
        confirmed = rearm.index("Position confirmedPosition = CurrentPosition()", create)
        submit = rearm.index("paperAccount.Submit(new[] { stop })", confirmed)
        self.assertLess(create, confirmed)
        self.assertLess(confirmed, submit)

    def test_flat_callback_fences_every_preowned_reducing_order(self) -> None:
        source = self._execution_source()
        position = source[
            source.index("        private void OnPositionUpdate"):
            source.index("        private void OnAccountItemUpdate")
        ]
        for invariant in (
            'owner.Role == "PROTECTIVE" || owner.Role == "EXIT"',
            "owner.Order != null && !owner.Terminal",
            "flatExpectedDuringFlatten = flattenInProgress",
            "DispatchOwnedOrderCancellationAfterExpectedFlat()",
            'LockAndProtect("FLAT_WITH_OWNED_REDUCING_ORDER")',
        ):
            self.assertIn(invariant, position)
        expected = position.index("if (flatExpectedDuringFlatten)")
        cancel = position.index(
            "DispatchOwnedOrderCancellationAfterExpectedFlat()", expected
        )
        unexpected = position.index(
            'LockAndProtect("FLAT_WITH_OWNED_REDUCING_ORDER")', cancel
        )
        self.assertLess(expected, cancel)
        self.assertLess(cancel, unexpected)

        dispatch = source[
            source.index(
                "        private void DispatchOwnedOrderCancellationAfterExpectedFlat"
            ):
            source.index("        private List<Order> OwnedWorkingOrders")
        ]
        claim = dispatch.index("flatOwnedOrderCancellationDispatchPending = true")
        gate = dispatch.index("lock (nativeMutationGate)", claim)
        native_cancel = dispatch.index("CancelOwnedOrders();", gate)
        release = dispatch.index(
            "flatOwnedOrderCancellationDispatchPending = false", native_cancel
        )
        self.assertLess(claim, gate)
        self.assertLess(gate, native_cancel)
        self.assertLess(native_cancel, release)

    def test_termination_retains_callbacks_only_for_owned_exposure(self) -> None:
        source = self._execution_source()
        retention = source[
            source.index("        private bool TerminationNeedsCallbackRetention"):
            source.index("        private void TryFinalizeRetainedTermination")
        ]
        self.assertNotIn(
            "if (position != null && position.Quantity != 0) return true", retention
        )
        self.assertIn("signedQuantity != 0 && ownedPositionEstablished", retention)
        self.assertIn("ownedPositionSignedQuantity == signedQuantity", retention)
        self.assertIn("owner.Order != null && !owner.Terminal", retention)
        self.assertIn("HasUnsettledEntryLifecycleUnderLock()", retention)
        self.assertNotIn("return flattenInProgress", retention)
        self.assertNotIn("pendingFlattenReconciliationCommand != null", retention)

        finalize = source[
            source.index("        private void TryFinalizeRetainedTermination"):
            source.index("        private bool ProtectBoundaryTermination")
        ]
        self.assertNotIn(
            "if (position != null && position.Quantity != 0) return", finalize
        )
        self.assertIn("signedQuantity != 0 && ownedPositionEstablished", finalize)
        self.assertIn("ownedPositionSignedQuantity == signedQuantity", finalize)
        self.assertIn("owner.Order != null && !owner.Terminal", finalize)
        self.assertIn('pending.Status == "PENDING"', finalize)
        self.assertIn('"BOUNDARY_STOPPED_BEFORE_RECONCILIATION"', finalize)
        self.assertIn("terminationCallbacksRetained = false", finalize)
        self.assertIn("DetachAccountCallbacks()", finalize)

    def test_retained_termination_retries_only_before_native_exit_ownership(self) -> None:
        source = self._execution_source()
        stop = source[
            source.index("        private void StopPaperBoundary"):
            source.index("        private void DetachAccountCallbacks")
        ]
        retain = stop.index("terminationCallbacksRetained = retainCallbacks")
        retry = stop.index("StartRetainedTerminationSafetyRetryLoop()", retain)
        self.assertLess(retain, retry)

        loop = source[
            source.index("        private void RetainedTerminationSafetyRetryLoop"):
            source.index("        private static Thread NewThread")
        ]
        for fence in (
            "while (true)",
            "Thread.Sleep(250)",
            "TryFinalizeRetainedTermination()",
            "if (!stopping || !terminationCallbacksRetained) return",
            "activeFlattenOwner != null",
            "foreignActivity",
            "HasAmbiguousOwnedOutcomeUnderLock()",
            "TryRearmProtectionAfterDefinitiveFlattenNoFill()",
            "if (pendingFlattenRecoveryProtectionSubmitted) return",
            "if (exitAlreadyOwned) continue",
            'LockAndProtect("RETAINED_TERMINATION_RETRY")',
        ):
            self.assertIn(fence, loop)
        self.assertNotIn("SubmitOwnedFlattenOrder(", loop)
        prohibited = loop.index("if (retryProhibited) return")
        rearm = loop.index("TryRearmProtectionAfterDefinitiveFlattenNoFill()")
        owned_exit = loop.index("if (exitAlreadyOwned) continue", rearm)
        dispatch = loop.index('LockAndProtect("RETAINED_TERMINATION_RETRY")')
        self.assertLess(prohibited, rearm)
        self.assertLess(rearm, owned_exit)
        self.assertLess(owned_exit, dispatch)

        flatten = source[
            source.index("        private string SubmitOwnedFlattenOrder"):
            source.index("        private void TryRearmProtectionAfterDefinitiveFlattenNoFill")
        ]
        create = flatten.index("paperAccount.CreateOrder")
        proven_release = flatten.index(
            "ReleasePreSubmitFlattenClaimUnderLock(commandId)", create
        )
        register = flatten.index("activeFlattenOwner = owner", proven_release)
        submit = flatten.index("paperAccount.Submit(new[] { order })", register)
        self.assertLess(create, proven_release)
        self.assertLess(proven_release, register)
        self.assertLess(register, submit)
        self.assertNotIn(
            "ReleasePreSubmitFlattenClaimUnderLock", flatten[register:submit]
        )

    def test_signing_key_lifetime_is_serialized_with_retained_callbacks(self) -> None:
        source = self._execution_source()
        stop = source[
            source.index("        private void StopPaperBoundary"):
            source.index("        private void DetachAccountCallbacks")
        ]
        wipe_lock = stop.index("lock (sendLock)")
        wipe = stop.index("Array.Clear(signingKey", wipe_lock)
        key_null = stop.index("signingKey = null", wipe)
        self.assertLess(wipe_lock, wipe)
        self.assertLess(wipe, key_null)

        send = source[
            source.index("        private bool SendSigned"):
            source.index("        private bool Verify")
        ]
        send_lock = send.index("lock (sendLock)")
        key_check = send.index("if (signingKey == null) return false", send_lock)
        sign = send.index("Sign(message)", key_check)
        write = send.index("current.Write", sign)
        self.assertLess(send_lock, key_check)
        self.assertLess(key_check, sign)
        self.assertLess(sign, write)

        verify = source[
            source.index("        private bool Verify"):
            source.index("        private string Sign")
        ]
        verify_lock = verify.index("lock (sendLock)")
        verify_key = verify.index("if (signingKey == null) return false", verify_lock)
        verify_sign = verify.index("Sign(message)", verify_key)
        self.assertLess(verify_lock, verify_key)
        self.assertLess(verify_key, verify_sign)

    def test_read_only_addon_remains_without_order_or_inbound_authority(self) -> None:
        source = (Path(__file__).parents[1] / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubReadOnlyAddOn.cs").read_text(encoding="utf-8")
        for forbidden in (
            "CreateOrder(", ".Submit(", ".Cancel(", ".Flatten(", "Stream.Read",
            "SendKeys", "Mouse", "Cursor", "Indicators.Add(",
        ):
            self.assertNotIn(forbidden, source)

    def test_read_only_addon_uses_native_headless_subscription_with_optional_chart_focus(self) -> None:
        root = Path(__file__).parents[1] / "ninjatrader" / "NinjaScript"
        addon = (root / "AddOns" / "BeelzebubReadOnlyAddOn.cs").read_text(encoding="utf-8")
        observer = (root / "Indicators" / "BeelzebubReadOnlyMarketObserver.cs").read_text(encoding="utf-8")
        self.assertIn("window as NinjaTrader.Gui.Chart.Chart", addon)
        self.assertIn("ActiveChartControl.Instrument.FullName", addon)
        self.assertIn("correct.Activate()", addon)
        self.assertIn("instrument.MarketData.Update += OnMarketData", addon)
        self.assertIn("instrument.MarketDepth.Update += OnMarketDepth", addon)
        self.assertIn('"NATIVE_ADDON"', addon)
        self.assertIn("BeelzebubAutomaticMarketObserver", addon)
        self.assertIn("MARKET_OBSERVER_ATTACHMENT", addon)
        self.assertIn("NATIVE_ADDON_OBSERVER_ACTIVE", addon)
        self.assertIn("beelzebub-observer.local.config", addon)
        self.assertIn("L3F_NT_MARKET_INSTRUMENT", addon)
        self.assertIn("ResolveConfiguredInstrument()", observer)
        self.assertIn("PublishObserverAttachment(", observer)
        self.assertNotIn("MNQ SEP26", observer)
        self.assertNotIn("MNQ September 2026", observer)

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
        self.assertIn("private readonly object bookLock = new object();", source)
        self.assertIn("lock (bookLock)", source)
        self.assertIn("Interlocked.Exchange(ref lastQuotePublicationTicks", source)
        self.assertIn("A sampled Last event must retain its exact same-callback", source)
        self.assertIn("publication_policy", source)

        addon = (Path(__file__).parents[1] / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubReadOnlyAddOn.cs").read_text(encoding="utf-8")
        self.assertIn("private const int MaximumQueuedFrames = 8;", addon)
        self.assertIn("private readonly object bookLock = new object();", addon)
        self.assertIn("lock (bookLock)", addon)
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
