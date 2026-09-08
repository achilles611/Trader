// Lane III-G experimental paper execution. This AddOn is compiled to one
// account (Sim101), one instrument (MNQ SEP26), and one-contract authority.
// It has no configurable account, instrument, quantity, or capital mode.
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Web.Script.Serialization;
using NinjaTrader.Cbi;
using NinjaTrader.NinjaScript;

namespace NinjaTrader.NinjaScript.AddOns
{
    public sealed class BeelzebubPaperExecutionAddOn : AddOnBase
    {
        private const string WireSchema = "lane-iii-phase-g-paper-execution-v1";
        private const string AddonProtocolVersion = "l3g-paper-addon-provenance-v1";
        // Updated from the checked-in source before a NinjaTrader build.  The
        // Python bridge independently fingerprints the same source, so an old
        // compiled AddOn cannot be armed merely because its DLL timestamp is new.
        private const string AddonSourceFingerprint = "5573d0b78ddc094e8200f157e6eed38f367881bc2fdf88992054ca54e2d22cab";
        private const string ExactAccountName = "Sim101";
        private const string ExactAccountClass = "LOCAL_SIMULATION";
        private const string ExactInstrumentName = "MNQ SEP26";
        private const string ExactCapability = "PAPER_ONLY";
        private const string ExactAccountBindingHash = "28ddf4acc88f1a9e35de79b8306a252e647a5a1dca0a6e9333ce814828e6841e";
        private const int Port = 48136;
        private const int MaximumFrameBytes = 65536;
        private const int MaximumQuantity = 1;
        private const double ExactTickSize = 0.25;
        private const double ProtectiveStopDistance = 25.0;
        private const int ProtectiveAcceptanceSeconds = 3;
        private const int WatchdogSettlementSeconds = 1;
        private const int MinimumWatchdogFinalProofs = 2;
        private const int MaximumWatchdogFinalProofAttempts = 3;
        private const string PaperTimezone = "America/New_York";
        private const string LondonTimezone = "Europe/London";
        private const string AsiaProfileHash = "55225b35ccdb289d179bb23afd7f3fdb2c5ab193d53aba21603f17ff9f6d43aa";
        private const string LondonProfileHash = "db211b6665e873fc3bf0b93db76210b25d154893ca1d5ca15ef0d7d6bea233cc";
        private const string NewYorkProfileHash = "8b8560a08ff41963a7a78d09bc977fbc1faf10f4a11ce58d05f47cacd89e0814";
        private const string NyAfterProfileHash = "f4dc8790d7cce0b26a4cedf43bc891907a1fef0a7fd97b4b66f19883907b8c97";
        private const string OffSessionProfileHash = "168f289a5847781ccb7a09f2556c4b3aa03e6f767071dc061dc5e3211d3834eb";
        private const string PerpetualPolicyHash = "35666cbb8744689d159706ea027b42d289a9e2e73622c9e663e1eab6c88cdff9";

        private readonly object stateLock = new object();
        // Serializes the final stopping latch/account snapshot with every
        // dispatcher-owned native mutation.
        private readonly object nativeMutationGate = new object();
        private readonly object sendLock = new object();
        private readonly object queueLock = new object();
        private readonly Queue<Dictionary<string, object>> commandQueue = new Queue<Dictionary<string, object>>();
        private readonly Dictionary<string, OwnedOrder> ownedByName = new Dictionary<string, OwnedOrder>(StringComparer.Ordinal);
        // Command identity is scoped to the authenticated execution session.
        // PENDING is never replayed as success, and UNKNOWN is a terminal
        // fail-closed outcome after a native mutation may have started.
        private readonly Dictionary<string, CommandOutcome> commandOutcomes = new Dictionary<string, CommandOutcome>(StringComparer.Ordinal);
        private readonly Dictionary<string, string> protectedEntryExecutionFacts = new Dictionary<string, string>(StringComparer.Ordinal);
        private readonly Dictionary<string, string> protectedEntryCommandExecutions = new Dictionary<string, string>(StringComparer.Ordinal);
        private readonly HashSet<string> failedEntryProtectionCommands = new HashSet<string>(StringComparer.Ordinal);
        // A probe snapshot is accepted only when no account callback overlaps
        // either immutable sample. Odd/even generations alone are insufficient
        // because NinjaTrader may invoke different callback types concurrently.
        private long nativeObservationGeneration;
        private int nativeObservationCallbacksInFlight;
        private Account paperAccount;
        private Instrument paperInstrument;
        private bool accountCallbacksAttached;
        private bool terminationCallbacksRetained;
        private TcpClient client;
        private NetworkStream stream;
        private Thread connectionThread;
        private Thread commandThread;
        private Thread watchdogThread;
        private Thread terminationSafetyRetryThread;
        private byte[] signingKey;
        private bool stopping;
        private bool lockedOut;
        private bool authenticated;
        // Once Python has established exact Sim101 authority, a dropped socket
        // must not disable the native heartbeat watchdog. This latch survives
        // reconnect attempts and is cleared only when the AddOn terminates.
        private bool watchdogSafetyAuthorityEstablished;
        private bool reconciled;
        private bool foreignActivity;
        private string executionSessionId;
        private string paperPolicyHash;
        private string riskProfileHash;
        private string accountBindingHash;
        private string paperSessionKind;
        private string paperSessionId;
        private string paperTradeDate;
        private string paperSessionProfileHash;
        private long paperSessionGeneration;
        private long lastCommandSequence;
        private DateTime lastHeartbeatUtc = DateTime.MinValue;
        private DateTime protectiveDeadlineUtc = DateTime.MaxValue;
        // A commanded exact-instrument flatten must first cancel any working
        // protective stop.  That expected cancellation is not a protective
        // failure, but it must still resolve to flat promptly.
        private bool flattenInProgress;
        private DateTime flattenDeadlineUtc = DateTime.MaxValue;
        private string pendingFlattenCommandId;
        private string pendingFlattenIntentId;
        private string pendingFlattenDecisionId;
        private OwnedOrder activeFlattenOwner;
        private int pendingFlattenExpectedQuantity;
        private int pendingFlattenFilledQuantity;
        private int pendingFlattenExitExecutionQuantity;
        private int pendingFlattenProtectiveExecutionQuantity;
        private bool pendingFlattenExecutionObserved;
        private bool pendingFlattenOrderTerminalObserved;
        private bool pendingFlattenExitFilledTerminalObserved;
        private bool pendingFlattenProtectiveFilledTerminalObserved;
        private bool pendingFlattenProtectiveCancellationExpected;
        private bool pendingFlattenProtectiveCancellationObserved;
        private bool pendingFlattenCompletedByProtective;
        private bool pendingFlattenExitNoFillTerminalObserved;
        private bool pendingFlattenRecoveryProtectionSubmitted;
        private bool pendingFlattenRecoveryProtectionDispatchPending;
        private bool flatOwnedOrderCancellationDispatchPending;
        private OrderAction pendingFlattenRecoveryProtectionAction;
        private int pendingFlattenRecoveryProtectionQuantity;
        private double pendingFlattenRecoveryProtectionStopPrice;
        // The OCO cancellation fence belongs to one exact protective order.
        // Periodic account snapshots may rediscover orders, but they must never
        // clear or retarget an in-flight callback correlation.
        private Order pendingFlattenProtectiveOrder;
        private string pendingFlattenProtectiveName;
        private string pendingFlattenProtectiveOco;
        private readonly HashSet<string> pendingFlattenExecutionIds = new HashSet<string>(StringComparer.Ordinal);
        // Hold one normal reversal reconciliation until flat POSITION, complete
        // EXIT execution, and terminal EXIT order facts have all arrived.
        private Dictionary<string, object> pendingFlattenReconciliationCommand;
        // A watchdog safety event is not complete until the AddOn publishes a
        // matching, exact-account/instrument reconciliation that proves both
        // position and owned working orders are flat.  Python uses this id to
        // reject stale pre-watchdog snapshots during controlled shutdown.
        private string pendingWatchdogSafetyEventId;
        private string pendingWatchdogSafetyReason;
        private DateTime pendingWatchdogSafetyActivatedUtc = DateTime.MinValue;
        private bool pendingWatchdogSafetyEventPublished;
        private long pendingWatchdogSafetySettlementSequence;
        private int pendingWatchdogSafetyFinalProofAttempts;
        // A final proof is only counted after its exact-flat receipt has
        // actually been written.  Keep one reservation per authenticated
        // socket generation so an in-flight write on a dead connection cannot
        // consume a later session's bounded retry budget.
        private bool pendingWatchdogSafetyFinalProofInFlight;
        private long authenticatedSessionGeneration;
        // ``lockedOut`` also represents foreign-account activity. Keep the
        // independently owned watchdog action separate so that a pre-existing
        // foreign lockout cannot suppress cancellation of a still-working
        // owned entry after Python heartbeats stop.
        private bool watchdogSafetyDispatchStarted;
        private bool watchdogSafetyActionInFlight;
        // An owned market EXIT is still an ambiguous native mutation if Submit
        // returns without a durable callback. Claim it once per safety
        // correlation and never retry blindly while the position remains open.
        private bool watchdogSafetyFlattenSubmitted;
        private bool pendingWatchdogSafetyExitRequired;
        // A nonzero exact-instrument position is owned only after this AddOn
        // observes both the exact pre-owned ENTRY execution and its matching
        // signed position transition. Account/instrument identity and an order
        // name prefix are not position provenance.
        private bool ownedPositionEstablished;
        private int ownedPositionSignedQuantity;
        private long positionOwnershipGeneration;
        private DateTime watchdogSafetyLastActionUtc = DateTime.MinValue;
        private Order protectiveOrder;
        private string pendingProtectionCommandId;
        private readonly string bridgeInstanceId = Guid.NewGuid().ToString("N");
        private readonly string ninjaTraderSessionId = Guid.NewGuid().ToString("N");

        protected override void OnStateChange()
        {
            if (State == State.Active)
                StartPaperBoundary();
            else if (State == State.Terminated)
                StopPaperBoundary();
        }

        private void StartPaperBoundary()
        {
            lock (stateLock)
            {
                List<Account> matches;
                lock (Account.All)
                    matches = Account.All.Where(a => String.Equals(a.Name, ExactAccountName, StringComparison.Ordinal)).ToList();
                if (matches.Count != 1)
                {
                    Diagnostic("ACCOUNT_BINDING_REFUSED");
                    return;
                }
                paperAccount = matches[0];
                paperInstrument = Instrument.GetInstrument(ExactInstrumentName);
                if (!ExactInstrument(paperInstrument))
                {
                    paperAccount = null;
                    paperInstrument = null;
                    Diagnostic("INSTRUMENT_BINDING_REFUSED");
                    return;
                }
                try
                {
                    signingKey = File.ReadAllBytes(KeyPath());
                    if (signingKey.Length < 32)
                        throw new InvalidDataException();
                }
                catch (IOException) { Diagnostic("LOCAL_KEY_UNAVAILABLE"); return; }
                catch (UnauthorizedAccessException) { Diagnostic("LOCAL_KEY_UNAVAILABLE"); return; }
                catch (InvalidDataException) { Diagnostic("LOCAL_KEY_INVALID"); return; }

                paperAccount.OrderUpdate += OnOrderUpdate;
                paperAccount.ExecutionUpdate += OnExecutionUpdate;
                paperAccount.PositionUpdate += OnPositionUpdate;
                paperAccount.AccountItemUpdate += OnAccountItemUpdate;
                accountCallbacksAttached = true;
                terminationCallbacksRetained = false;
                terminationSafetyRetryThread = null;
                stopping = false;
                lockedOut = false;
                foreignActivity = false;
                watchdogSafetyAuthorityEstablished = false;
                watchdogSafetyDispatchStarted = false;
                watchdogSafetyActionInFlight = false;
                watchdogSafetyFlattenSubmitted = false;
                watchdogSafetyLastActionUtc = DateTime.MinValue;
                flattenInProgress = false;
                flattenDeadlineUtc = DateTime.MaxValue;
                pendingFlattenCommandId = null;
                pendingFlattenIntentId = null;
                pendingFlattenDecisionId = null;
                activeFlattenOwner = null;
                pendingFlattenExpectedQuantity = 0;
                pendingFlattenFilledQuantity = 0;
                pendingFlattenExitExecutionQuantity = 0;
                pendingFlattenProtectiveExecutionQuantity = 0;
                pendingFlattenExecutionObserved = false;
                pendingFlattenOrderTerminalObserved = false;
                pendingFlattenExitFilledTerminalObserved = false;
                pendingFlattenProtectiveFilledTerminalObserved = false;
                pendingFlattenProtectiveCancellationExpected = false;
                pendingFlattenProtectiveCancellationObserved = false;
                pendingFlattenCompletedByProtective = false;
                pendingFlattenExitNoFillTerminalObserved = false;
                pendingFlattenRecoveryProtectionSubmitted = false;
                pendingFlattenRecoveryProtectionDispatchPending = false;
                flatOwnedOrderCancellationDispatchPending = false;
                pendingFlattenRecoveryProtectionAction = OrderAction.Sell;
                pendingFlattenRecoveryProtectionQuantity = 0;
                pendingFlattenRecoveryProtectionStopPrice = 0;
                pendingFlattenProtectiveOrder = null;
                pendingFlattenProtectiveName = null;
                pendingFlattenProtectiveOco = null;
                pendingFlattenExecutionIds.Clear();
                pendingFlattenReconciliationCommand = null;
                pendingWatchdogSafetyExitRequired = false;
                ownedPositionEstablished = false;
                ownedPositionSignedQuantity = 0;
                positionOwnershipGeneration = 0;
                connectionThread = NewThread(ConnectionLoop, "BeelzebubPaperConnection");
                commandThread = NewThread(CommandLoop, "BeelzebubPaperCommands");
                watchdogThread = NewThread(WatchdogLoop, "BeelzebubPaperWatchdog");
                connectionThread.Start();
                commandThread.Start();
                watchdogThread.Start();
                Diagnostic("PAPER_BOUNDARY_ACTIVE");
            }
        }

        private void StopPaperBoundary()
        {
            // Quiesce command admission before inspecting account state. A
            // queued dispatcher ticket re-checks this latch before executing.
            lock (stateLock)
            {
                stopping = true;
                lockedOut = true;
                reconciled = false;
            }
            lock (queueLock) Monitor.PulseAll(queueLock);
            bool safeTermination;
            bool retainCallbacks;
            lock (nativeMutationGate)
            {
                safeTermination = ProtectBoundaryTermination();
                // Account callbacks are the only remaining authority capable of
                // resolving a cancel/fill race after State.Terminated returns.
                // Retain them whenever an owned native lifecycle can still
                // mutate exposure; a later exact callback drains them.
                retainCallbacks = !safeTermination && TerminationNeedsCallbackRetention();
                lock (stateLock)
                    terminationCallbacksRetained = retainCallbacks;
            }
            if (retainCallbacks)
                StartRetainedTerminationSafetyRetryLoop();
            lock (stateLock)
            {
                authenticated = false;
                reconciled = false;
                if (safeTermination)
                {
                    watchdogSafetyAuthorityEstablished = false;
                    pendingWatchdogSafetyEventId = null;
                    pendingWatchdogSafetyReason = null;
                    pendingWatchdogSafetyActivatedUtc = DateTime.MinValue;
                    pendingWatchdogSafetyEventPublished = false;
                    pendingWatchdogSafetySettlementSequence = 0;
                    pendingWatchdogSafetyFinalProofAttempts = 0;
                    pendingWatchdogSafetyFinalProofInFlight = false;
                    authenticatedSessionGeneration = 0;
                    watchdogSafetyDispatchStarted = false;
                    watchdogSafetyActionInFlight = false;
                    watchdogSafetyFlattenSubmitted = false;
                    pendingWatchdogSafetyExitRequired = false;
                    watchdogSafetyLastActionUtc = DateTime.MinValue;
                }
                else
                    lockedOut = true;
                CloseTransport();
            }
            if (!retainCallbacks)
                DetachAccountCallbacks();
            Join(connectionThread);
            Join(commandThread);
            Join(watchdogThread);
            // A retained/in-flight account callback may have observed the old
            // authenticated latch. Serialize key lifetime with signing so the
            // callback either completes with the intact key or sees no key.
            lock (sendLock)
            {
                if (signingKey != null)
                    Array.Clear(signingKey, 0, signingKey.Length);
                signingKey = null;
            }
            if (retainCallbacks)
                TryFinalizeRetainedTermination();
            Diagnostic(
                safeTermination
                    ? "PAPER_BOUNDARY_TERMINATED_SAFE"
                    : "PAPER_BOUNDARY_TERMINATED_UNRESOLVED_ACTIVITY"
            );
        }

        private void DetachAccountCallbacks()
        {
            Account account;
            lock (stateLock)
            {
                if (!accountCallbacksAttached) return;
                accountCallbacksAttached = false;
                account = paperAccount;
            }
            if (account == null) return;
            account.OrderUpdate -= OnOrderUpdate;
            account.ExecutionUpdate -= OnExecutionUpdate;
            account.PositionUpdate -= OnPositionUpdate;
            account.AccountItemUpdate -= OnAccountItemUpdate;
        }

        private bool TerminationNeedsCallbackRetention()
        {
            Position position = CurrentPosition();
            int signedQuantity = position == null || position.MarketPosition == MarketPosition.Flat
                ? 0 : position.MarketPosition == MarketPosition.Short
                    ? -position.Quantity : position.Quantity;
            lock (stateLock)
                return (signedQuantity != 0 && ownedPositionEstablished
                        && ownedPositionSignedQuantity == signedQuantity)
                    // Callback-owned terminal state is authoritative here. The
                    // mutable OrderState may run ahead of its callback and must
                    // not detach the last handler before that exact fact lands.
                    || ownedByName.Values.Any(owner =>
                        owner.Order != null && !owner.Terminal)
                    || HasUnsettledEntryLifecycleUnderLock()
                    || HasAmbiguousOwnedOutcomeUnderLock();
        }

        private void TryFinalizeRetainedTermination()
        {
            lock (stateLock)
                if (!stopping || !terminationCallbacksRetained) return;
            Position position = CurrentPosition();
            int signedQuantity = position == null || position.MarketPosition == MarketPosition.Flat
                ? 0 : position.MarketPosition == MarketPosition.Short
                    ? -position.Quantity : position.Quantity;
            lock (stateLock)
            {
                if (!terminationCallbacksRetained
                    || (signedQuantity != 0 && ownedPositionEstablished
                        && ownedPositionSignedQuantity == signedQuantity)
                    || ownedByName.Values.Any(owner =>
                        owner.Order != null && !owner.Terminal)
                    || HasUnsettledEntryLifecycleUnderLock()
                    || HasAmbiguousOwnedOutcomeUnderLock())
                    return;
                // Bare flatten/reconciliation bookkeeping has no remaining
                // native fill authority once every exact owner is callback-
                // terminal. Do not leave a terminated AddOn subscribed forever
                // merely because a foreign aggregate can never satisfy it.
                if (pendingFlattenReconciliationCommand != null)
                {
                    string key = CommandOutcomeKey(pendingFlattenReconciliationCommand);
                    CommandOutcome pending;
                    if (commandOutcomes.TryGetValue(key, out pending)
                        && pending.Status == "PENDING")
                        commandOutcomes[key] = new CommandOutcome(
                            pending.Fingerprint, "UNKNOWN",
                            "BOUNDARY_STOPPED_BEFORE_RECONCILIATION"
                        );
                    pendingFlattenReconciliationCommand = null;
                    Diagnostic("DEFERRED_RECONCILIATION_UNRESOLVED_AT_STOP");
                }
                terminationCallbacksRetained = false;
            }
            DetachAccountCallbacks();
            Diagnostic("PAPER_BOUNDARY_TERMINATION_CALLBACKS_DRAINED");
        }

        private bool ProtectBoundaryTermination()
        {
            if (paperAccount == null || paperInstrument == null) return true;
            RehydrateOwnedWorkingOrders();
            EnsureCurrentPositionOwnershipProven();
            Position position = CurrentPosition();
            bool positionOpen = position != null && position.Quantity != 0;
            bool ownedActivity = HasOwnedActivity();
            bool pristineIdle;
            lock (stateLock)
                pristineIdle = !ownedActivity
                    && !foreignActivity
                    && !flattenInProgress
                    && pendingFlattenReconciliationCommand == null
                    && String.IsNullOrWhiteSpace(pendingWatchdogSafetyEventId)
                    && !HasUnsettledEntryLifecycleUnderLock()
                    && !HasAmbiguousOwnedOutcomeUnderLock()
                    && !commandOutcomes.Values.Any(outcome =>
                        outcome.Status == "PENDING" || outcome.Status == "UNKNOWN");
            if (pristineIdle) return true;
            bool beginCorrelation;
            lock (stateLock)
            {
                lockedOut = true;
                reconciled = false;
                beginCorrelation = !watchdogSafetyDispatchStarted;
                if (beginCorrelation) watchdogSafetyDispatchStarted = true;
            }
            if (beginCorrelation)
                BeginWatchdogSafetyCorrelation("ADDON_TERMINATING_WITH_ACTIVITY");
            bool foreign;
            bool ambiguousOwned;
            bool submitted;
            string safetyEventId;
            lock (stateLock)
            {
                foreign = foreignActivity;
                ambiguousOwned = HasAmbiguousOwnedOutcomeUnderLock();
                submitted = watchdogSafetyFlattenSubmitted || flattenInProgress;
                safetyEventId = pendingWatchdogSafetyEventId;
            }
            try
            {
                if (ambiguousOwned)
                {
                    // Unknown can mean a native order is still capable of a
                    // late fill. Attempt cancellation, but never compete with
                    // it using a newly submitted EXIT.
                    CancelOwnedOrders();
                }
                else if (foreign)
                {
                    // The aggregate position is ambiguous, but our exact named
                    // orders are not. Cancel all of them and never submit an
                    // exit that could flatten somebody else's Sim101 exposure.
                    CancelOwnedOrders();
                }
                else if (positionOpen && !submitted)
                {
                    string refusal = SubmitOwnedFlattenOrder(
                        safetyEventId, "NATIVE_SAFETY", "ADDON_TERMINATING_WITH_ACTIVITY", true
                    );
                    if (refusal != null)
                        Diagnostic("TERMINATION_FLATTEN_REFUSED_" + refusal);
                }
                else if (!positionOpen)
                    // A working EXIT can open the opposite position after the
                    // account is already flat. Cancel every owned role.
                    CancelOwnedOrders();
                // A nonforeign open position with an already-claimed EXIT keeps
                // only that native EXIT/OCO pair alive to finish settlement.
            }
            catch (Exception error)
            {
                string reason = "TERMINATION_SAFETY_ACTION_FAILED_" + error.GetType().Name;
                Diagnostic(reason);
                // The first cancellation/exit attempt was ambiguous. Preserve
                // one dispatcher-owned retry path after callback retention is
                // latched; never call a competing mutation inline.
                LockAndProtect(reason);
            }
            TryPublishWatchdogSafetyReconciliation();
            Position current = CurrentPosition();
            bool open = current != null && current.Quantity != 0;
            bool working = OwnedWorkingOrders(null).Count != 0;
            bool exitSettled;
            bool foreignNow;
            bool correlatedFinalProofs;
            lock (stateLock)
            {
                exitSettled = !pendingWatchdogSafetyExitRequired
                    || (pendingFlattenExecutionObserved && pendingFlattenOrderTerminalObserved
                        && (pendingFlattenCompletedByProtective
                            ? pendingFlattenExitNoFillTerminalObserved
                            : (!pendingFlattenProtectiveCancellationExpected
                                || pendingFlattenProtectiveCancellationObserved)));
                foreignNow = foreignActivity;
                correlatedFinalProofs = pendingWatchdogSafetyEventPublished
                    && pendingWatchdogSafetySettlementSequence >= MinimumWatchdogFinalProofs;
                exitSettled = exitSettled && !flattenInProgress
                    && pendingFlattenReconciliationCommand == null
                    && !HasUnsettledEntryLifecycleUnderLock()
                    && !HasAmbiguousOwnedOutcomeUnderLock()
                    && !commandOutcomes.Values.Any(outcome =>
                        outcome.Status == "PENDING" || outcome.Status == "UNKNOWN");
            }
            // State.Terminated can run on NinjaTrader's dispatcher. Never wait
            // here for callbacks that need that dispatcher. If the exact facts
            // were not already durable, terminate truthfully as unresolved.
            return !open && !working && !foreignNow && exitSettled && correlatedFinalProofs;
        }

        private void StartRetainedTerminationSafetyRetryLoop()
        {
            Thread retry;
            lock (stateLock)
            {
                if (!stopping || !terminationCallbacksRetained) return;
                if (terminationSafetyRetryThread != null
                    && terminationSafetyRetryThread.IsAlive)
                    return;
                retry = NewThread(
                    RetainedTerminationSafetyRetryLoop,
                    "BeelzebubPaperTerminationSafety"
                );
                terminationSafetyRetryThread = retry;
            }
            retry.Start();
        }

        private void RetainedTerminationSafetyRetryLoop()
        {
            while (true)
            {
                Thread.Sleep(250);
                // A callback may have completed the last exact native
                // lifecycle since the previous pass.
                TryFinalizeRetainedTermination();
                bool retryProhibited;
                bool exitAlreadyOwned;
                lock (stateLock)
                {
                    if (!stopping || !terminationCallbacksRetained) return;
                    // Unknown/foreign ownership likewise permits cancellation
                    // only; callback paths retain that authority without a new
                    // EXIT retry loop.
                    retryProhibited = foreignActivity
                        || HasAmbiguousOwnedOutcomeUnderLock();
                    exitAlreadyOwned = activeFlattenOwner != null;
                }
                if (retryProhibited) return;
                // A terminal no-fill EXIT deliberately remains as a tombstone.
                // Keep servicing its one-stop recovery path: its dispatcher or
                // CreateOrder can fail before Submit without generating another
                // callback. The recovery helper's durable Submitted latch makes
                // an ambiguous Submit non-retryable.
                TryRearmProtectionAfterDefinitiveFlattenNoFill();
                lock (stateLock)
                {
                    if (!stopping || !terminationCallbacksRetained) return;
                    if (pendingFlattenRecoveryProtectionSubmitted) return;
                    exitAlreadyOwned = activeFlattenOwner != null;
                }
                if (exitAlreadyOwned) continue;
                // LockAndProtect supplies the one-second throttle, in-flight
                // coalescing, dispatcher ownership, and final mutation gate.
                // A proven pre-Submit refusal leaves no activeFlattenOwner, so
                // the next pass may safely make exactly one fresh attempt.
                LockAndProtect("RETAINED_TERMINATION_RETRY");
            }
        }

        private static Thread NewThread(ThreadStart action, string name)
        {
            Thread thread = new Thread(action);
            thread.IsBackground = true;
            thread.Name = name;
            return thread;
        }

        private static void Join(Thread thread)
        {
            if (thread != null && thread != Thread.CurrentThread)
                thread.Join(TimeSpan.FromSeconds(3));
        }

        private static string KeyPath()
        {
            return Path.Combine(System.Environment.GetFolderPath(System.Environment.SpecialFolder.MyDocuments), "NinjaTrader 8", "l3g.paper.local.key");
        }

        private static bool ExactInstrument(Instrument instrument)
        {
            return instrument != null
                && String.Equals(instrument.FullName, ExactInstrumentName, StringComparison.Ordinal)
                && instrument.MasterInstrument != null
                && String.Equals(instrument.MasterInstrument.Name, "MNQ", StringComparison.Ordinal)
                && Math.Abs(instrument.MasterInstrument.TickSize - ExactTickSize) < 0.0000001;
        }

        private bool ExactBoundAccount(Account candidate)
        {
            // The account object and its native name are both authority facts.
            // Never normalize, default, or infer a missing account name, and
            // never accept another Account instance merely because it is active.
            return candidate != null
                && paperAccount != null
                && Object.ReferenceEquals(candidate, paperAccount)
                && !String.IsNullOrWhiteSpace(candidate.Name)
                && String.Equals(candidate.Name, ExactAccountName, StringComparison.Ordinal);
        }

        private void ConnectionLoop()
        {
            while (!stopping)
            {
                try
                {
                    TcpClient next = new TcpClient();
                    next.Connect(IPAddress.Loopback, Port);
                    lock (stateLock)
                    {
                        if (stopping) { next.Close(); return; }
                        client = next;
                        stream = next.GetStream();
                        authenticated = false;
                        reconciled = false;
                        executionSessionId = null;
                        lastCommandSequence = 0;
                    }
                    SendHello();
                    ReadFrames(next.GetStream());
                }
                catch (SocketException) { }
                catch (IOException) { }
                catch (ObjectDisposedException) { }
                catch (Exception error) { Diagnostic("TRANSPORT_FAULT_" + error.GetType().Name); }
                finally
                {
                    lock (stateLock)
                    {
                        authenticated = false;
                        reconciled = false;
                        executionSessionId = null;
                        CloseTransport();
                    }
                }
                if (!stopping)
                    Thread.Sleep(1000);
            }
        }

        private void ReadFrames(NetworkStream current)
        {
            byte[] read = new byte[4096];
            List<byte> buffer = new List<byte>();
            while (!stopping)
            {
                int count = current.Read(read, 0, read.Length);
                if (count <= 0)
                    return;
                for (int index = 0; index < count; index++)
                {
                    byte value = read[index];
                    if (value == 10)
                    {
                        if (buffer.Count > 0)
                        {
                            ProcessFrame(Encoding.UTF8.GetString(buffer.ToArray()));
                            buffer.Clear();
                        }
                    }
                    else
                    {
                        buffer.Add(value);
                        if (buffer.Count > MaximumFrameBytes)
                        {
                            Reject(null, "OVERSIZED_FRAME", null);
                            return;
                        }
                    }
                }
            }
        }

        private void ProcessFrame(string text)
        {
            Dictionary<string, object> message;
            try
            {
                if (HasDuplicateTopLevelKeys(text)) { Reject(null, "DUPLICATE_JSON_KEY", null); return; }
                JavaScriptSerializer serializer = new JavaScriptSerializer();
                serializer.MaxJsonLength = MaximumFrameBytes;
                message = serializer.DeserializeObject(text) as Dictionary<string, object>;
                if (message == null) throw new InvalidOperationException();
            }
            catch (Exception) { Reject(null, "MALFORMED_JSON", null); return; }
            if (!String.Equals(Text(message, "schema"), WireSchema, StringComparison.Ordinal) || !Verify(message))
            {
                Reject(message, "INVALID_SIGNATURE_OR_SCHEMA", null);
                return;
            }
            string type = Text(message, "message_type");
            if (String.Equals(type, "SESSION_GRANT", StringComparison.Ordinal))
            {
                AcceptSession(message);
                return;
            }
            if (String.Equals(type, "RECONCILIATION_PROBE_GRANT", StringComparison.Ordinal))
            {
                AcceptReconciliationProbeGrant(message);
                return;
            }
            lock (stateLock)
            {
                if (!authenticated || !String.Equals(Text(message, "execution_session_id"), executionSessionId, StringComparison.Ordinal))
                {
                    Reject(message, "WRONG_EXECUTION_SESSION", null);
                    return;
                }
            }
            if (String.Equals(type, "HEARTBEAT", StringComparison.Ordinal))
            {
                if (!ExactHashes(message)) { Reject(message, "HEARTBEAT_AUTHORITY_MISMATCH", null); return; }
                DateTime timestamp;
                if (!ValidTime(Text(message, "timestamp"), 5, out timestamp)) { Reject(message, "HEARTBEAT_TIME", null); return; }
                lock (stateLock) lastHeartbeatUtc = DateTime.UtcNow;
                return;
            }
            if (!String.Equals(type, "COMMAND", StringComparison.Ordinal))
            {
                Reject(message, "UNSUPPORTED_MESSAGE", null);
                return;
            }
            EnqueueCommand(message);
        }

        private void AcceptSession(Dictionary<string, object> grant)
        {
            DateTime timestamp;
            if (!ValidTime(Text(grant, "timestamp"), 10, out timestamp)
                || !String.Equals(Text(grant, "mode"), "PAPER_SIM101", StringComparison.Ordinal)
                || Boolean(grant, "live_capital") != false)
            {
                Reject(grant, "SESSION_GRANT_REFUSED", null);
                return;
            }
            string session = Text(grant, "execution_session_id");
            string policy = Text(grant, "paper_policy_hash");
            string risk = Text(grant, "risk_profile_hash");
            string binding = Text(grant, "account_binding_hash");
            if (String.IsNullOrWhiteSpace(session) || !HashText(policy) || !HashText(risk) || !HashText(binding))
            {
                Reject(grant, "SESSION_AUTHORITY_MISSING", null);
                return;
            }
            // An AddOn reload can leave a previously submitted BZ-L3G order
            // working without producing another OrderUpdate for this instance.
            // Restore only its local cancel/watchdog identity before this
            // authenticated session can evaluate commands or missed heartbeats.
            RehydrateOwnedWorkingOrders();
            EnsureCurrentPositionOwnershipProven();
            lock (stateLock)
            {
                executionSessionId = session;
                paperPolicyHash = policy;
                riskProfileHash = risk;
                accountBindingHash = binding;
                authenticated = true;
                watchdogSafetyAuthorityEstablished = true;
                reconciled = false;
                lastCommandSequence = 0;
                lastHeartbeatUtc = DateTime.UtcNow;
                // A reconnect may have lost receipts that were successfully
                // written only to the prior local socket.  Retain the event
                // correlation and monotonic settlement sequence, but give the
                // newly authenticated connection its own bounded proof budget.
                authenticatedSessionGeneration++;
                pendingWatchdogSafetyFinalProofAttempts = 0;
                pendingWatchdogSafetyFinalProofInFlight = false;
            }
            RepublishPendingWatchdogSafetyCorrelation();
            SendReconciliation();
        }

        private static bool HashText(string value)
        {
            if (value == null || value.Length != 64)
                return false;
            for (int index = 0; index < value.Length; index++)
                if (!Uri.IsHexDigit(value[index])) return false;
            return true;
        }

        private void AcceptReconciliationProbeGrant(Dictionary<string, object> grant)
        {
            string[] exactFields = {
                "schema", "message_type", "probe_session_id", "server_nonce",
                "account_binding_hash", "mode", "live_capital", "timestamp", "signature"
            };
            DateTime timestamp;
            bool normalSessionActive;
            lock (stateLock)
                normalSessionActive = authenticated || !String.IsNullOrWhiteSpace(executionSessionId);
            if (!ExactFields(grant, exactFields)
                || !ValidTime(Text(grant, "timestamp"), 10, out timestamp)
                || String.IsNullOrWhiteSpace(Text(grant, "probe_session_id"))
                || String.IsNullOrWhiteSpace(Text(grant, "server_nonce"))
                || !String.Equals(Text(grant, "account_binding_hash"), ExactAccountBindingHash, StringComparison.Ordinal)
                || !String.Equals(Text(grant, "mode"), "PAPER_SIM101", StringComparison.Ordinal)
                || Boolean(grant, "live_capital") != false
                || normalSessionActive)
            {
                Diagnostic("RECONCILIATION_PROBE_GRANT_REFUSED");
                return;
            }

            ProbeNativeSample first;
            ProbeNativeSample second;
            long generationBefore;
            long generationAfter;
            int callbacksBefore;
            int callbacksAfter;
            // This fence blocks every locally dispatched Submit/Cancel while
            // both immutable account samples are captured. Account callbacks
            // do not take this gate, so their separate generation/in-flight
            // fence must also remain unchanged and empty across both samples.
            lock (nativeMutationGate)
            {
                callbacksBefore = Interlocked.CompareExchange(
                    ref nativeObservationCallbacksInFlight, 0, 0
                );
                generationBefore = Interlocked.Read(ref nativeObservationGeneration);
                first = CaptureProbeNativeSample();
                Thread.MemoryBarrier();
                second = CaptureProbeNativeSample();
                generationAfter = Interlocked.Read(ref nativeObservationGeneration);
                callbacksAfter = Interlocked.CompareExchange(
                    ref nativeObservationCallbacksInFlight, 0, 0
                );
            }
            bool stable = callbacksBefore == 0
                && callbacksAfter == 0
                && generationBefore == generationAfter
                && (generationAfter & 1L) == 0L
                && String.Equals(first.Hash, second.Hash, StringComparison.Ordinal)
                && first.PositionSnapshotComplete
                && first.OrderSnapshotComplete
                && second.PositionSnapshotComplete
                && second.OrderSnapshotComplete
                && !first.NativeMutationPending
                && !second.NativeMutationPending;

            Dictionary<string, object> result = new Dictionary<string, object>();
            result["schema"] = WireSchema;
            result["message_type"] = "RECONCILIATION_PROBE_RESULT";
            result["probe_session_id"] = Text(grant, "probe_session_id");
            result["server_nonce"] = Text(grant, "server_nonce");
            result["observation_generation"] = generationAfter;
            result["first_sample_hash"] = first.Hash;
            result["second_sample_hash"] = second.Hash;
            result["snapshot_stable"] = stable;
            result["timestamp"] = UtcNow();
            result["receipt_id"] = "l3g-reconciliation-probe-" + Guid.NewGuid().ToString("N");
            result["account_name"] = ExactAccountName;
            result["account_class"] = ExactAccountClass;
            result["instrument"] = ExactInstrumentName;
            result["position_quantity"] = second.PositionQuantity;
            result["working_order_count"] = second.WorkingOrderCount;
            result["working_entry_count"] = second.WorkingEntryCount;
            result["position_snapshot_complete"] = stable && second.PositionSnapshotComplete;
            result["order_snapshot_complete"] = stable && second.OrderSnapshotComplete;
            result["foreign_activity"] = first.ForeignActivity || second.ForeignActivity;
            result["protective_stop_state"] = second.ProtectiveStopState;
            SendSigned(result);
        }

        private ProbeNativeSample CaptureProbeNativeSample()
        {
            ProbeNativeSample sample = new ProbeNativeSample();
            sample.PositionSnapshotComplete = ExactBoundAccount(paperAccount) && ExactInstrument(paperInstrument);
            sample.OrderSnapshotComplete = sample.PositionSnapshotComplete;
            sample.ProtectiveStopState = "NONE";
            List<string> positionFacts = new List<string>();
            List<string> orderFacts = new List<string>();
            int exactPositionCount = 0;
            if (ExactBoundAccount(paperAccount))
            {
                lock (paperAccount.Positions)
                {
                    foreach (Position position in paperAccount.Positions)
                    {
                        if (position == null || !ExactBoundAccount(position.Account)) continue;
                        string instrument = position.Instrument == null
                            ? String.Empty : position.Instrument.FullName ?? String.Empty;
                        int signedQuantity = position.MarketPosition == MarketPosition.Short
                            ? -position.Quantity
                            : position.MarketPosition == MarketPosition.Flat ? 0 : position.Quantity;
                        positionFacts.Add(String.Join("|", new[] {
                            instrument,
                            position.MarketPosition.ToString(),
                            signedQuantity.ToString(CultureInfo.InvariantCulture)
                        }));
                        if (String.Equals(instrument, ExactInstrumentName, StringComparison.Ordinal))
                        {
                            exactPositionCount++;
                            sample.PositionQuantity += signedQuantity;
                        }
                        else if (signedQuantity != 0)
                            sample.ForeignActivity = true;
                    }
                }
                lock (paperAccount.Orders)
                {
                    foreach (Order order in paperAccount.Orders)
                    {
                        if (order == null || !ExactBoundAccount(order.Account)
                            || !UnresolvedNativeOrderState(order.OrderState)) continue;
                        string instrument = order.Instrument == null
                            ? String.Empty : order.Instrument.FullName ?? String.Empty;
                        string name = order.Name ?? String.Empty;
                        bool exact = String.Equals(instrument, ExactInstrumentName, StringComparison.Ordinal);
                        orderFacts.Add(String.Join("|", new[] {
                            instrument,
                            name,
                            order.OrderId ?? String.Empty,
                            order.OrderState.ToString(),
                            order.Quantity.ToString(CultureInfo.InvariantCulture),
                            order.Filled.ToString(CultureInfo.InvariantCulture)
                        }));
                        if (exact)
                        {
                            sample.WorkingOrderCount++;
                            if (name.StartsWith("BZ-L3G-E-", StringComparison.Ordinal))
                                sample.WorkingEntryCount++;
                        }
                        if (!exact || !IsOwnedName(name)) sample.ForeignActivity = true;
                    }
                }
            }
            if (exactPositionCount > 1) sample.ForeignActivity = true;
            positionFacts.Sort(StringComparer.Ordinal);
            orderFacts.Sort(StringComparer.Ordinal);
            lock (queueLock)
                if (commandQueue.Count != 0) sample.NativeMutationPending = true;
            lock (stateLock)
            {
                sample.ForeignActivity = sample.ForeignActivity || foreignActivity;
                sample.ProtectiveStopState = protectiveOrder == null
                    ? "NONE" : protectiveOrder.OrderState.ToString().ToUpperInvariant();
                sample.NativeMutationPending = sample.NativeMutationPending
                    || flattenInProgress
                    || watchdogSafetyActionInFlight
                    || pendingFlattenRecoveryProtectionDispatchPending
                    || flatOwnedOrderCancellationDispatchPending
                    || HasUnsettledEntryLifecycleUnderLock()
                    || HasAmbiguousOwnedOutcomeUnderLock()
                    || commandOutcomes.Values.Any(outcome =>
                        outcome.Status == "PENDING" || outcome.Status == "UNKNOWN");
            }
            Dictionary<string, object> hashPayload = new Dictionary<string, object>();
            hashPayload["position_facts"] = positionFacts;
            hashPayload["order_facts"] = orderFacts;
            hashPayload["position_quantity"] = sample.PositionQuantity;
            hashPayload["working_order_count"] = sample.WorkingOrderCount;
            hashPayload["working_entry_count"] = sample.WorkingEntryCount;
            hashPayload["position_snapshot_complete"] = sample.PositionSnapshotComplete;
            hashPayload["order_snapshot_complete"] = sample.OrderSnapshotComplete;
            hashPayload["foreign_activity"] = sample.ForeignActivity;
            hashPayload["protective_stop_state"] = sample.ProtectiveStopState;
            hashPayload["native_mutation_pending"] = sample.NativeMutationPending;
            using (SHA256 sha = SHA256.Create())
                sample.Hash = BitConverter.ToString(
                    sha.ComputeHash(Encoding.UTF8.GetBytes(Canonical(hashPayload)))
                ).Replace("-", String.Empty).ToLowerInvariant();
            return sample;
        }

        private static bool ExactFields(
            Dictionary<string, object> message, IEnumerable<string> expected)
        {
            if (message == null) return false;
            string[] names = expected.ToArray();
            return message.Count == names.Length && names.All(message.ContainsKey);
        }

        private void EnqueueCommand(Dictionary<string, object> command)
        {
            string commandId = Text(command, "command_id");
            string outcomeKey = CommandOutcomeKey(command);
            string fingerprint = CommandFingerprint(command);
            CommandOutcome prior = null;
            lock (stateLock)
                commandOutcomes.TryGetValue(outcomeKey, out prior);
            if (prior != null)
            {
                if (!String.Equals(prior.Fingerprint, fingerprint, StringComparison.Ordinal))
                {
                    if (HasOwnedActivity()) LockAndProtect("COMMAND_IDEMPOTENCY_CONFLICT");
                    Reject(command, "IDEMPOTENCY_CONFLICT", commandId);
                }
                else if (prior.Status == "ACCEPTED")
                    Acknowledge(command, "DUPLICATE_IDEMPOTENT", true);
                else if (prior.Status == "REJECTED")
                    Reject(command, prior.Reason, commandId);
                else if (prior.Status == "UNKNOWN")
                {
                    if (HasOwnedActivity()) LockAndProtect("COMMAND_OUTCOME_UNKNOWN");
                    Reject(command, "COMMAND_OUTCOME_UNKNOWN", commandId);
                }
                else
                    Diagnostic("DUPLICATE_COMMAND_OUTCOME_PENDING");
                return;
            }
            lock (queueLock)
            {
                commandQueue.Enqueue(command);
                Monitor.Pulse(queueLock);
            }
        }

        private void CommandLoop()
        {
            while (!stopping)
            {
                Dictionary<string, object> command = null;
                lock (queueLock)
                {
                    while (!stopping && commandQueue.Count == 0)
                        Monitor.Wait(queueLock, 250);
                    if (stopping) return;
                    command = commandQueue.Dequeue();
                }
                string commandId;
                string action;
                long sequence;
                string reservation = ReserveCommandSequence(command, out commandId, out action, out sequence);
                if (reservation != null)
                {
                    if (reservation == "COMMAND_OUTCOME_PENDING")
                        continue;
                    if (reservation == "DUPLICATE_ACCEPTED")
                        Acknowledge(command, "DUPLICATE_IDEMPOTENT", true);
                    else if (reservation.StartsWith("DUPLICATE_REJECTED:", StringComparison.Ordinal))
                        Reject(command, reservation.Substring("DUPLICATE_REJECTED:".Length), commandId);
                    else if (reservation == "DUPLICATE_UNKNOWN")
                    {
                        if (HasOwnedActivity()) LockAndProtect("COMMAND_OUTCOME_UNKNOWN");
                        Reject(command, "COMMAND_OUTCOME_UNKNOWN", commandId);
                    }
                    else
                    {
                        if ((reservation == "REORDERED_COMMAND" || reservation == "IDEMPOTENCY_CONFLICT")
                            && HasOwnedActivity())
                            LockAndProtect("COMMAND_REJECTED_" + reservation);
                        Reject(command, reservation, commandId);
                    }
                    continue;
                }

                CommandDispatchTicket ticket = new CommandDispatchTicket();
                try
                {
                    NinjaTrader.Core.Globals.RandomDispatcher.BeginInvoke(new Action(delegate
                    {
                        if (!ticket.TryStart()) return;
                        try { ExecuteCommand(command, commandId, action, sequence); }
                        catch (Exception error) { ticket.Failure = error; }
                        finally { ticket.Complete(); }
                    }));
                }
                catch (Exception error)
                {
                    ticket.Failure = error;
                    ticket.Complete();
                }
                if (!ticket.Completed.WaitOne(TimeSpan.FromSeconds(5)))
                {
                    if (ticket.TryAbandon())
                    {
                        SetCommandOutcome(command, "REJECTED", "COMMAND_DISPATCH_TIMEOUT");
                        LockAndProtect("COMMAND_DISPATCH_TIMEOUT");
                        Reject(command, "COMMAND_DISPATCH_TIMEOUT", commandId);
                        ticket.Completed.Dispose();
                        continue;
                    }
                    // Once dispatcher execution has begun, timing out cannot
                    // safely prove whether a native mutation occurred. Keep
                    // serial command authority blocked until its exact result
                    // is known; never launch a competing call.
                    ticket.Completed.WaitOne();
                }
                if (ticket.Failure != null)
                {
                    string reason = "COMMAND_DISPATCH_FAILURE_" + ticket.Failure.GetType().Name;
                    SetCommandOutcome(command, "UNKNOWN", reason);
                    LockAndProtect("COMMAND_DISPATCH_FAILURE");
                    Reject(command, reason, commandId);
                }
                ticket.Completed.Dispose();
            }
        }

        private void ExecuteCommand(Dictionary<string, object> command, string commandId, string action, long sequence)
        {
            lock (nativeMutationGate)
                ExecuteCommandUnderMutationGate(command, commandId, action, sequence);
        }

        private void ExecuteCommandUnderMutationGate(
            Dictionary<string, object> command, string commandId, string action, long sequence)
        {
            string refusal = ValidateReservedCommand(command, commandId, action, sequence);
            if (refusal != null)
            {
                SetCommandOutcome(command, "REJECTED", refusal);
                // A gap or a consumed semantic refusal means Python and the
                // native bridge no longer share safe command authority. If any
                // owned activity remains, the AddOn must cancel/flatten itself;
                // waiting for another Python command can strand exposure.
                if (HasOwnedActivity())
                    LockAndProtect("COMMAND_REJECTED_" + refusal);
                Reject(command, refusal, commandId);
                return;
            }
            lock (stateLock)
            {
                paperSessionKind = Text(command, "session_kind");
                paperSessionId = Text(command, "session_id");
                paperTradeDate = Text(command, "trade_date");
                paperSessionProfileHash = Text(command, "session_profile_hash");
                paperSessionGeneration = Integer64(command, "session_generation", 0);
            }
            string actionRefusal = null;
            if (action == "ENTER_LONG" || action == "ENTER_SHORT")
                SubmitEntry(command, action == "ENTER_LONG");
            else if (action == "EXIT")
                actionRefusal = FlattenOwnedInstrument(command, false);
            else if (action == "EMERGENCY_FLATTEN")
                actionRefusal = FlattenOwnedInstrument(command, true);
            else if (action == "CANCEL_OWNED_ORDERS")
                CancelOwnedOrders();
            else if (action == "RECONCILE")
            {
                // A flat POSITION callback can precede the matching EXIT fill
                // and terminal order callbacks. Hold the broker snapshot and
                // its ACK until all three exact facts have settled.
                if (DeferReconciliationUntilFlattenSettled(command)) return;
                if (!SendReconciliation())
                    actionRefusal = "RECONCILIATION_SEND_FAILED";
            }
            if (actionRefusal != null)
            {
                SetCommandOutcome(command, "REJECTED", actionRefusal);
                if (HasOwnedActivity()) LockAndProtect("COMMAND_REJECTED_" + actionRefusal);
                Reject(command, actionRefusal, commandId);
                return;
            }
            SetCommandOutcome(command, "ACCEPTED", "ACCEPTED");
            Acknowledge(command, "ACCEPTED", false);
        }

        private string ReserveCommandSequence(
            Dictionary<string, object> command, out string commandId, out string action,
            out long sequence)
        {
            commandId = Text(command, "command_id");
            action = Text(command, "action");
            sequence = Integer64(command, "command_sequence", -1);
            string outcomeKey = CommandOutcomeKey(command);
            string fingerprint = CommandFingerprint(command);
            lock (stateLock)
            {
                if (stopping) return "BOUNDARY_STOPPING";
                if (!authenticated) return "NOT_AUTHENTICATED";
                if (!String.Equals(Text(command, "execution_session_id"), executionSessionId, StringComparison.Ordinal)) return "WRONG_EXECUTION_SESSION";
                if (String.IsNullOrWhiteSpace(commandId)) return "MISSING_COMMAND_ID";
                CommandOutcome prior;
                if (commandOutcomes.TryGetValue(outcomeKey, out prior))
                {
                    if (!String.Equals(prior.Fingerprint, fingerprint, StringComparison.Ordinal))
                        return "IDEMPOTENCY_CONFLICT";
                    if (prior.Status == "PENDING") return "COMMAND_OUTCOME_PENDING";
                    if (prior.Status == "ACCEPTED") return "DUPLICATE_ACCEPTED";
                    if (prior.Status == "REJECTED") return "DUPLICATE_REJECTED:" + prior.Reason;
                    return "DUPLICATE_UNKNOWN";
                }
                if (sequence != lastCommandSequence + 1) return "REORDERED_COMMAND";
                // The authenticated sender advances its sequence when it
                // durably creates a command. Once an exact-session command has
                // the next sequence and a usable id, consume that sequence even
                // when a later semantic fence rejects the action. Otherwise a
                // rejected RECONCILE/EXIT permanently wedges every subsequent
                // safety command as REORDERED_COMMAND.
                commandOutcomes[outcomeKey] = new CommandOutcome(fingerprint, "PENDING", null);
                lastCommandSequence = sequence;
            }
            return null;
        }

        private string ValidateReservedCommand(
            Dictionary<string, object> command, string commandId, string action, long sequence)
        {
            DateTime timestamp;
            DateTime expiry;
            lock (stateLock)
            {
                if (stopping) return "BOUNDARY_STOPPING";
                if (!authenticated) return "NOT_AUTHENTICATED";
                if (!String.Equals(Text(command, "execution_session_id"), executionSessionId, StringComparison.Ordinal)) return "WRONG_EXECUTION_SESSION";
                CommandOutcome pending;
                if (!commandOutcomes.TryGetValue(CommandOutcomeKey(command), out pending)
                    || pending.Status != "PENDING"
                    || !String.Equals(pending.Fingerprint, CommandFingerprint(command), StringComparison.Ordinal))
                    return "COMMAND_OUTCOME_NOT_PENDING";
                if (lockedOut && (action == "ENTER_LONG" || action == "ENTER_SHORT")) return "LOCKED_OUT";
                if (!reconciled && (action == "ENTER_LONG" || action == "ENTER_SHORT")) return "RECONCILIATION_REQUIRED";
                if ((action == "ENTER_LONG" || action == "ENTER_SHORT")
                    && HasUnsettledEntryLifecycleUnderLock()) return "ENTRY_LIFECYCLE_PENDING";
                // A position callback can report flat before NinjaTrader emits
                // the owned EXIT order/execution. Do not admit the next entry
                // until that native exit has durably bound and settled.
                if (flattenInProgress && (action == "ENTER_LONG" || action == "ENTER_SHORT")) return "FLATTEN_SETTLEMENT_PENDING";
            }
            if (!ValidTime(Text(command, "created_at"), 5, out timestamp)) return "STALE_OR_FUTURE_COMMAND";
            if (!DateTime.TryParse(Text(command, "expires_at"), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out expiry) || expiry.ToUniversalTime() < DateTime.UtcNow) return "COMMAND_EXPIRED";
            if (!ExactHashes(command)) return "AUTHORITY_HASH_MISMATCH";
            string sessionRefusal = ValidatePaperSessionFence(command, action);
            if (sessionRefusal != null) return sessionRefusal;
            if (!String.Equals(Text(command, "account_name"), ExactAccountName, StringComparison.Ordinal)) return "ACCOUNT_MISMATCH";
            if (!String.Equals(Text(command, "account_class"), ExactAccountClass, StringComparison.Ordinal)) return "ACCOUNT_CLASS_MISMATCH";
            if (!String.Equals(Text(command, "instrument"), ExactInstrumentName, StringComparison.Ordinal)) return "INSTRUMENT_MISMATCH";
            if (!ExactBoundAccount(paperAccount)) return "ACCOUNT_BINDING_LOST";
            if (!ExactInstrument(paperInstrument)) return "INSTRUMENT_BINDING_LOST";
            int quantity = Integer32(command, "quantity", Int32.MinValue);
            bool noQuantity = action == "HEARTBEAT" || action == "RECONCILE" || action == "CANCEL_OWNED_ORDERS";
            if (quantity != (noQuantity ? 0 : MaximumQuantity)) return "QUANTITY_REFUSED";
            string[] allowed = { "ENTER_LONG", "ENTER_SHORT", "EXIT", "EMERGENCY_FLATTEN", "CANCEL_OWNED_ORDERS", "HEARTBEAT", "RECONCILE" };
            if (!allowed.Contains(action, StringComparer.Ordinal)) return "UNSUPPORTED_ACTION";
            Position current = CurrentPosition();
            int positionQuantity = current == null ? 0 : current.Quantity;
            int workingEntries = OwnedWorkingOrders("ENTRY").Count;
            if ((action == "ENTER_LONG" || action == "ENTER_SHORT") && (positionQuantity != 0 || workingEntries != 0 || OwnedWorkingOrders(null).Count != 0)) return "POSITION_OR_ORDER_PRECONDITION";
            if ((action == "ENTER_LONG" || action == "ENTER_SHORT") && foreignActivity) return "FOREIGN_ACTIVITY_LOCKOUT";
            if ((action == "EXIT" || action == "EMERGENCY_FLATTEN") && positionQuantity == 0) return "EXIT_WHILE_FLAT";
            return null;
        }

        private static string CommandOutcomeKey(Dictionary<string, object> command)
        {
            return (Text(command, "execution_session_id") ?? String.Empty) + "|" + (Text(command, "command_id") ?? String.Empty);
        }

        private static string CommandFingerprint(Dictionary<string, object> command)
        {
            Dictionary<string, object> unsigned = new Dictionary<string, object>(command ?? new Dictionary<string, object>(), StringComparer.Ordinal);
            unsigned.Remove("signature");
            using (SHA256 sha = SHA256.Create())
                return BitConverter.ToString(sha.ComputeHash(Encoding.UTF8.GetBytes(Canonical(unsigned)))).Replace("-", String.Empty).ToLowerInvariant();
        }

        private void SetCommandOutcome(Dictionary<string, object> command, string status, string reason)
        {
            string key = CommandOutcomeKey(command);
            string fingerprint = CommandFingerprint(command);
            lock (stateLock)
                commandOutcomes[key] = new CommandOutcome(fingerprint, status, reason);
        }

        private bool HasOwnedActivity()
        {
            Position position = CurrentPosition();
            return (position != null && position.Quantity != 0)
                || OwnedWorkingOrders(null).Count != 0
                || HasUnsettledEntryLifecycle()
                || HasAmbiguousOwnedOutcome();
        }

        private bool EnsureCurrentPositionOwnershipProven()
        {
            Position position = CurrentPosition();
            int signedQuantity = position == null || position.MarketPosition == MarketPosition.Flat
                ? 0 : position.MarketPosition == MarketPosition.Short
                    ? -position.Quantity : position.Quantity;
            return EnsureCurrentPositionOwnershipProven(signedQuantity);
        }

        private bool EnsureCurrentPositionOwnershipProven(int signedQuantity)
        {
            if (signedQuantity == 0) return true;
            bool proven;
            lock (stateLock)
            {
                // Permit the short callback-order window for an ENTRY this
                // exact AddOn instance pre-owned. A RESTORED name is not
                // equivalent: it lacks current-cycle execution provenance.
                bool currentEntryLifecycle = ownedByName.Values.Any(owner =>
                    owner.Role == "ENTRY" && !owner.EntrySettlementComplete
                    && owner.Order != null
                    && !String.Equals(owner.CommandId, "RESTORED", StringComparison.Ordinal)
                    && (owner.EntryExecutionObserved || owner.EntryOrderFilledTerminalObserved)
                    && (!owner.EntryPositionObserved
                        || (!owner.EntryPositionOwnershipInvalidated
                            && owner.EntryPositionGeneration == positionOwnershipGeneration))
                    && EntryDirectionMatchesPosition(owner, signedQuantity));
                proven = (ownedPositionEstablished
                        && ownedPositionSignedQuantity == signedQuantity)
                    || currentEntryLifecycle;
                if (!proven)
                {
                    // A naked Sim101/MNQ position may belong to an operator or
                    // another strategy. Never adopt or flatten it merely because
                    // its account/instrument and a BZ-looking name happen to fit.
                    foreignActivity = true;
                    lockedOut = true;
                    reconciled = false;
                }
            }
            if (!proven) Diagnostic("POSITION_OWNERSHIP_UNPROVEN");
            return proven;
        }

        private static bool EntryDirectionMatchesPosition(OwnedOrder owner, int signedQuantity)
        {
            if (owner == null || owner.Order == null || Math.Abs(signedQuantity) != MaximumQuantity)
                return false;
            return (owner.Order.OrderAction == OrderAction.Buy && signedQuantity == MaximumQuantity)
                || (owner.Order.OrderAction == OrderAction.SellShort && signedQuantity == -MaximumQuantity);
        }

        private void TryEstablishOwnedPositionUnderLock(OwnedOrder owner)
        {
            if (owner == null || owner.Role != "ENTRY") return;
            if (owner.EntryExecutionObserved && owner.EntryPositionObserved
                && !owner.EntryPositionOwnershipInvalidated
                && owner.EntryPositionGeneration == positionOwnershipGeneration
                && EntryDirectionMatchesPosition(owner, owner.EntryPositionSignedQuantity))
            {
                ownedPositionEstablished = true;
                ownedPositionSignedQuantity = owner.EntryPositionSignedQuantity;
            }
        }

        private bool HasUnsettledEntryLifecycle()
        {
            lock (stateLock)
                return HasUnsettledEntryLifecycleUnderLock();
        }

        private bool HasUnsettledEntryLifecycleUnderLock()
        {
            return ownedByName.Values.Any(owner =>
                owner.Role == "ENTRY" && !owner.EntrySettlementComplete);
        }

        private bool EntryExposureFullyAccountedUnderLock(OwnedOrder owner)
        {
            return owner != null && owner.Role == "ENTRY"
                && !owner.OutcomeUnknown
                && owner.EntryExecutionObserved
                && owner.EntryPositionObserved
                && owner.EntryExposureHandled
                && !owner.EntryPositionOwnershipInvalidated
                && owner.EntryPositionGeneration == positionOwnershipGeneration
                && ownedPositionEstablished
                && owner.EntryPositionSignedQuantity == ownedPositionSignedQuantity
                && EntryDirectionMatchesPosition(owner, ownedPositionSignedQuantity);
        }

        private bool HasEntryCapableOfAddingExposureUnderLock()
        {
            return ownedByName.Values.Any(owner => owner.Role == "ENTRY"
                && !owner.EntrySettlementComplete
                && !EntryExposureFullyAccountedUnderLock(owner));
        }

        private bool HasAmbiguousOwnedOutcomeUnderLock()
        {
            return ownedByName.Values.Any(owner => owner.OutcomeUnknown
                || (owner.Order != null && owner.Order.OrderState == OrderState.Unknown));
        }

        private bool HasAmbiguousOwnedOutcome()
        {
            lock (stateLock)
                return HasAmbiguousOwnedOutcomeUnderLock();
        }

        private static void TrySettleEntryLifecycleUnderLock(OwnedOrder owner)
        {
            if (owner == null || owner.Role != "ENTRY" || owner.EntrySettlementComplete) return;
            if (owner.EntryNoFillTerminalObserved
                || (owner.EntryOrderFilledTerminalObserved
                    && owner.EntryExecutionObserved
                    && owner.EntryPositionObserved
                    && owner.EntryExposureHandled))
                owner.EntrySettlementComplete = true;
        }

        private void MarkPendingEntryExposureHandledByExitUnderLock(int signedPositionQuantity)
        {
            foreach (OwnedOrder owner in ownedByName.Values.Where(value =>
                value.Role == "ENTRY" && !value.EntrySettlementComplete))
            {
                // SubmitOwnedFlattenOrder is called only after CurrentPosition
                // proves exposure. Claiming its one pre-owned EXIT is therefore
                // an exact native handling path even if ENTRY callbacks arrive
                // in the opposite order.
                if (!owner.EntryPositionOwnershipInvalidated
                    && EntryDirectionMatchesPosition(owner, signedPositionQuantity))
                {
                    owner.EntryPositionObserved = true;
                    owner.EntryPositionSignedQuantity = signedPositionQuantity;
                    owner.EntryPositionGeneration = positionOwnershipGeneration;
                    TryEstablishOwnedPositionUnderLock(owner);
                }
                owner.EntryExposureHandled = true;
                TrySettleEntryLifecycleUnderLock(owner);
            }
        }

        private static TimeZoneInfo NewYorkTimezone()
        {
            // NinjaTrader runs on Windows where the registry name is used;
            // .NET installations with IANA data accept America/New_York.
            try { return TimeZoneInfo.FindSystemTimeZoneById(PaperTimezone); }
            catch (TimeZoneNotFoundException) { return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
            catch (InvalidTimeZoneException) { return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
        }

        private static TimeZoneInfo LondonTimezoneInfo()
        {
            // Preserve the IANA identity on the wire. NinjaTrader's Windows
            // .NET Framework runtime uses the registry-equivalent timezone.
            try { return TimeZoneInfo.FindSystemTimeZoneById(LondonTimezone); }
            catch (TimeZoneNotFoundException) { return TimeZoneInfo.FindSystemTimeZoneById("GMT Standard Time"); }
            catch (InvalidTimeZoneException) { return TimeZoneInfo.FindSystemTimeZoneById("GMT Standard Time"); }
        }

        private static bool AsiaStartDay(DateTime local)
        {
            return local.DayOfWeek == DayOfWeek.Sunday || local.DayOfWeek == DayOfWeek.Monday
                || local.DayOfWeek == DayOfWeek.Tuesday || local.DayOfWeek == DayOfWeek.Wednesday
                || local.DayOfWeek == DayOfWeek.Thursday;
        }

        private static bool NewYorkStartDay(DateTime local)
        {
            return local.DayOfWeek >= DayOfWeek.Monday && local.DayOfWeek <= DayOfWeek.Friday;
        }

        private static bool LondonStartDay(DateTime local)
        {
            return local.DayOfWeek >= DayOfWeek.Monday && local.DayOfWeek <= DayOfWeek.Friday;
        }

        private static string IsoDate(DateTime local)
        {
            return local.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        private string ValidatePaperSessionFence(Dictionary<string, object> command, string action)
        {
            if (action != "ENTER_LONG" && action != "ENTER_SHORT") return null;
            string kind = Text(command, "session_kind");
            string family = Text(command, "session_family");
            string sessionId = Text(command, "session_id");
            string tradeDate = Text(command, "trade_date");
            string profileHash = Text(command, "session_profile_hash");
            if (String.IsNullOrWhiteSpace(kind) || String.IsNullOrWhiteSpace(family) || String.IsNullOrWhiteSpace(sessionId)
                || String.IsNullOrWhiteSpace(tradeDate) || String.IsNullOrWhiteSpace(profileHash))
                return "MISSING_SESSION_IDENTITY";
            DateTime nowUtc = DateTime.UtcNow;
            DateTime local = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, NewYorkTimezone());
            DateTime londonLocal = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, LondonTimezoneInfo());
            DateTime parsedDate;
            if (!DateTime.TryParseExact(tradeDate, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out parsedDate))
                return "TRADE_DATE_MISMATCH";
            TimeSpan clock = local.TimeOfDay;
            TimeSpan londonClock = londonLocal.TimeOfDay;
            bool perpetual = String.Equals(
                Text(command, "policy_hash"), PerpetualPolicyHash,
                StringComparison.Ordinal
            ) && Boolean(command, "commissioning") == false;
            string expectedKind;
            string expectedFamily;
            string expectedDate;
            string expectedHash;
            bool insideEntry;
            if (perpetual)
            {
                bool dailyMaintenance = local.DayOfWeek >= DayOfWeek.Monday
                    && local.DayOfWeek <= DayOfWeek.Thursday
                    && clock >= new TimeSpan(17, 0, 0)
                    && clock < new TimeSpan(18, 0, 0);
                bool weekendClosed = (local.DayOfWeek == DayOfWeek.Friday && clock >= new TimeSpan(17, 0, 0))
                    || local.DayOfWeek == DayOfWeek.Saturday
                    || (local.DayOfWeek == DayOfWeek.Sunday && clock < new TimeSpan(18, 0, 0));
                bool equityIndexHalt = local.DayOfWeek >= DayOfWeek.Monday
                    && local.DayOfWeek <= DayOfWeek.Friday
                    && clock >= new TimeSpan(16, 15, 0)
                    && clock < new TimeSpan(16, 30, 0);
                if (dailyMaintenance) return "EXCHANGE_DAILY_MAINTENANCE";
                if (weekendClosed) return "EXCHANGE_WEEKEND_CLOSED";
                if (equityIndexHalt) return "EXCHANGE_INTRADAY_HALT";

                if ((clock >= new TimeSpan(18, 0, 0) && AsiaStartDay(local))
                    || (clock < new TimeSpan(2, 0, 0) && AsiaStartDay(local.AddDays(-1))))
                {
                    expectedKind = "ASIA";
                    expectedFamily = "ASIA";
                    expectedDate = IsoDate(clock >= new TimeSpan(18, 0, 0) ? local.AddDays(1) : local);
                    expectedHash = AsiaProfileHash;
                }
                else if (londonClock >= new TimeSpan(8, 0, 0) && londonClock < new TimeSpan(11, 30, 0)
                    && LondonStartDay(londonLocal))
                {
                    expectedKind = "LONDON";
                    expectedFamily = "EUROPE";
                    expectedDate = IsoDate(londonLocal);
                    expectedHash = LondonProfileHash;
                }
                else if (clock >= new TimeSpan(9, 30, 0) && clock < new TimeSpan(16, 0, 0)
                    && NewYorkStartDay(local))
                {
                    expectedKind = "NEW_YORK_RTH";
                    expectedFamily = "NEW_YORK";
                    expectedDate = IsoDate(local);
                    expectedHash = NewYorkProfileHash;
                }
                else if (clock >= new TimeSpan(16, 0, 0) && clock < new TimeSpan(17, 0, 0)
                    && local.DayOfWeek >= DayOfWeek.Monday && local.DayOfWeek <= DayOfWeek.Thursday)
                {
                    expectedKind = "NY_AFTER";
                    expectedFamily = "NEW_YORK";
                    expectedDate = IsoDate(local);
                    expectedHash = NyAfterProfileHash;
                }
                else
                {
                    expectedKind = "OFF_SESSION";
                    expectedFamily = "OFF_SESSION";
                    expectedDate = IsoDate(local);
                    expectedHash = OffSessionProfileHash;
                }
                if (!String.Equals(kind, expectedKind, StringComparison.Ordinal)) return "COMMAND_SESSION_MISMATCH";
                if (!String.Equals(family, expectedFamily, StringComparison.Ordinal)) return "COMMAND_SESSION_FAMILY_MISMATCH";
                if (!String.Equals(profileHash, expectedHash, StringComparison.Ordinal)) return "SESSION_PROFILE_HASH_MISMATCH";
                if (!String.Equals(tradeDate, expectedDate, StringComparison.Ordinal)
                    || !String.Equals(sessionId, "MNQU6:" + expectedKind + ":" + expectedDate, StringComparison.Ordinal))
                    return "TRADE_DATE_MISMATCH";
                if (Integer64(command, "session_generation", -1) < 0) return "SESSION_GENERATION_MISMATCH";
                return null;
            }
            if ((clock >= new TimeSpan(18, 0, 0) && AsiaStartDay(local))
                || (clock < new TimeSpan(2, 0, 0) && AsiaStartDay(local.AddDays(-1))))
            {
                expectedKind = "ASIA";
                expectedFamily = "ASIA";
                expectedDate = IsoDate(clock >= new TimeSpan(18, 0, 0) ? local.AddDays(1) : local);
                expectedHash = AsiaProfileHash;
                insideEntry = (clock >= new TimeSpan(18, 5, 0)) || (clock < new TimeSpan(1, 30, 0));
            }
            else if (londonClock >= new TimeSpan(8, 0, 0) && londonClock < new TimeSpan(11, 30, 0)
                && LondonStartDay(londonLocal))
            {
                expectedKind = "LONDON";
                expectedFamily = "EUROPE";
                expectedDate = IsoDate(londonLocal);
                expectedHash = LondonProfileHash;
                insideEntry = true;
            }
            else if (clock >= new TimeSpan(9, 30, 0) && clock < new TimeSpan(16, 0, 0) && NewYorkStartDay(local))
            {
                expectedKind = "NEW_YORK_RTH";
                expectedFamily = "NEW_YORK";
                expectedDate = IsoDate(local);
                expectedHash = NewYorkProfileHash;
                insideEntry = clock >= new TimeSpan(9, 35, 0) && clock < new TimeSpan(15, 30, 0);
            }
            else if (clock >= new TimeSpan(16, 0, 0) && clock < new TimeSpan(17, 0, 0)
                && local.DayOfWeek >= DayOfWeek.Monday && local.DayOfWeek <= DayOfWeek.Thursday)
            {
                expectedKind = "NY_AFTER";
                expectedFamily = "NEW_YORK";
                expectedDate = IsoDate(local);
                expectedHash = NyAfterProfileHash;
                // Intersect the strategy window with CME's normal 16:15-16:30
                // ET equity-index futures halt. No entry is admitted in it.
                insideEntry = clock >= new TimeSpan(16, 5, 0) && clock < new TimeSpan(16, 15, 0);
            }
            else
                return "SESSION_OFF_SESSION";
            if (!String.Equals(kind, expectedKind, StringComparison.Ordinal)) return "COMMAND_SESSION_MISMATCH";
            if (!String.Equals(family, expectedFamily, StringComparison.Ordinal)) return "COMMAND_SESSION_FAMILY_MISMATCH";
            if (!String.Equals(profileHash, expectedHash, StringComparison.Ordinal)) return "SESSION_PROFILE_HASH_MISMATCH";
            if (!String.Equals(tradeDate, expectedDate, StringComparison.Ordinal)
                || !String.Equals(sessionId, "MNQU6:" + expectedKind + ":" + expectedDate, StringComparison.Ordinal))
                return "TRADE_DATE_MISMATCH";
            if (!insideEntry) return "ENTRY_CUTOFF_PASSED";
            return null;
        }

        private bool ExactHashes(Dictionary<string, object> message)
        {
            lock (stateLock)
                return String.Equals(Text(message, "paper_policy_hash") ?? Text(message, "policy_hash"), paperPolicyHash, StringComparison.Ordinal)
                    && String.Equals(Text(message, "risk_profile_hash"), riskProfileHash, StringComparison.Ordinal)
                    && String.Equals(Text(message, "account_binding_hash"), accountBindingHash, StringComparison.Ordinal);
        }

        private void SubmitEntry(Dictionary<string, object> command, bool enterLong)
        {
            if (!ExactBoundAccount(paperAccount))
                throw new InvalidOperationException("ACCOUNT_BINDING_LOST");
            string fragment = Fragment(Text(command, "command_id"));
            string name = "BZ-L3G-E-" + fragment;
            OrderAction orderAction = enterLong ? OrderAction.Buy : OrderAction.SellShort;
            // Claim the entry lifecycle before CreateOrder. A thrown or
            // ambiguous native call remains an unresolved tombstone and cannot
            // become a pristine shutdown or a second entry opportunity.
            OwnedOrder owner = OwnedOrder.FromCommand(command, name, "ENTRY", null);
            lock (stateLock)
            {
                ownedByName[name] = owner;
                reconciled = false;
            }
            Order order;
            try
            {
                order = paperAccount.CreateOrder(paperInstrument, orderAction, OrderType.Market, OrderEntry.Automated, TimeInForce.Day, MaximumQuantity, 0, 0, String.Empty, name, NinjaTrader.Core.Globals.MaxDate, null);
                if (order == null)
                    throw new InvalidOperationException("CREATE_ORDER_RETURNED_NULL");
                if (!ExactBoundAccount(order.Account))
                    throw new InvalidOperationException("ACCOUNT_BINDING_LOST");
            }
            catch
            {
                // CreateOrder did not cross the native Submit boundary. Close
                // the entry lifecycle as a proven no-fill while the command
                // outcome itself remains fail-closed at the dispatcher.
                lock (stateLock)
                {
                    owner.EntryNoFillTerminalObserved = true;
                    owner.Terminal = true;
                    TrySettleEntryLifecycleUnderLock(owner);
                }
                throw;
            }
            lock (stateLock) owner.Order = order;
            if (!ExactBoundAccount(paperAccount) || !ExactBoundAccount(order.Account))
                throw new InvalidOperationException("ACCOUNT_BINDING_LOST");
            paperAccount.Submit(new[] { order });
        }

        private bool EntryProtectionPositionMatches(OwnedOrder entry, int expectedSignedQuantity)
        {
            Position position = CurrentPosition();
            int signedQuantity = position == null || position.MarketPosition == MarketPosition.Flat
                ? 0 : position.MarketPosition == MarketPosition.Short
                    ? -position.Quantity : position.Quantity;
            if (signedQuantity != expectedSignedQuantity) return false;
            lock (stateLock)
            {
                if (entry == null || entry.OutcomeUnknown || entry.EntryExposureHandled
                    || entry.EntryPositionOwnershipInvalidated)
                    return false;
                if (entry.EntryPositionObserved
                    && (entry.EntryPositionSignedQuantity != expectedSignedQuantity
                        || entry.EntryPositionGeneration != positionOwnershipGeneration))
                    return false;
                entry.EntryPositionObserved = true;
                entry.EntryPositionSignedQuantity = expectedSignedQuantity;
                entry.EntryPositionGeneration = positionOwnershipGeneration;
                TryEstablishOwnedPositionUnderLock(entry);
                return ownedPositionEstablished
                    && ownedPositionSignedQuantity == expectedSignedQuantity;
            }
        }

        private void SubmitProtectiveStop(
            Order entryOrder, int executionQuantity, double executionPrice, OwnedOrder entry)
        {
            if (!ExactBoundAccount(paperAccount)
                || entryOrder == null || !ExactBoundAccount(entryOrder.Account))
            {
                LockAndProtect("ACCOUNT_BINDING_LOST");
                return;
            }
            if (entryOrder == null || executionQuantity != MaximumQuantity || executionPrice <= 0)
            {
                LockAndProtect("INVALID_ENTRY_FILL");
                return;
            }
            bool longPosition = entryOrder.OrderAction == OrderAction.Buy;
            int expectedSignedQuantity = longPosition ? executionQuantity : -executionQuantity;
            if (!EntryProtectionPositionMatches(entry, expectedSignedQuantity))
            {
                LockAndProtect("ENTRY_PROTECTION_POSITION_CHANGED_BEFORE_CREATE");
                return;
            }
            double raw = longPosition ? executionPrice - ProtectiveStopDistance : executionPrice + ProtectiveStopDistance;
            double stopPrice = Math.Round(raw / ExactTickSize, MidpointRounding.AwayFromZero) * ExactTickSize;
            OrderAction stopAction = longPosition ? OrderAction.Sell : OrderAction.BuyToCover;
            string name = "BZ-L3G-S-" + Fragment(entry.CommandId);
            string oco = "BZ-L3G-OCO-" + Fragment(entry.CommandId);
            Order stop = paperAccount.CreateOrder(paperInstrument, stopAction, OrderType.StopMarket, OrderEntry.Automated, TimeInForce.Gtc, executionQuantity, 0, stopPrice, oco, name, NinjaTrader.Core.Globals.MaxDate, null);
            if (stop == null)
                throw new InvalidOperationException("CREATE_ORDER_RETURNED_NULL");
            if (!ExactBoundAccount(stop.Account))
                throw new InvalidOperationException("ACCOUNT_BINDING_LOST");
            if (!EntryProtectionPositionMatches(entry, expectedSignedQuantity))
            {
                LockAndProtect("ENTRY_PROTECTION_POSITION_CHANGED_AFTER_CREATE");
                return;
            }
            OwnedOrder protection = new OwnedOrder(entry.CommandId, entry.IntentId, entry.DecisionId, name, "PROTECTIVE", stop, DateTime.UtcNow);
            lock (stateLock)
            {
                ownedByName[name] = protection;
                protectiveOrder = stop;
                pendingProtectionCommandId = entry.CommandId;
                protectiveDeadlineUtc = DateTime.UtcNow.AddSeconds(ProtectiveAcceptanceSeconds);
            }
            if (!EntryProtectionPositionMatches(entry, expectedSignedQuantity))
            {
                lock (stateLock)
                {
                    protection.Terminal = true;
                    ownedByName.Remove(name);
                    if (Object.ReferenceEquals(protectiveOrder, stop)) protectiveOrder = null;
                    protectiveDeadlineUtc = DateTime.MaxValue;
                }
                LockAndProtect("ENTRY_PROTECTION_POSITION_CHANGED_BEFORE_SUBMIT");
                return;
            }
            if (!ExactBoundAccount(paperAccount) || !ExactBoundAccount(stop.Account))
            {
                LockAndProtect("ACCOUNT_BINDING_LOST");
                return;
            }
            paperAccount.Submit(new[] { stop });
        }

        private void TrySubmitPendingEntryProtection(OwnedOrder owner)
        {
            if (owner == null || owner.Role != "ENTRY") return;
            string commandId = owner.CommandId ?? String.Empty;
            try
            {
                lock (nativeMutationGate)
                {
                    Order entryOrder;
                    int executionQuantity;
                    double executionPrice;
                    bool terminationExitClaimed;
                    lock (stateLock)
                    {
                        // NinjaTrader may publish the immutable execution before
                        // its matching PositionUpdate. Neither callback alone is
                        // authority to create a reducing stop. Keep one claimed
                        // pending submission until both exact facts agree.
                        if (!owner.EntryProtectionPending
                            || !owner.EntryExecutionObserved
                            || !owner.EntryPositionObserved)
                            return;
                        owner.EntryProtectionPending = false;
                        entryOrder = owner.Order;
                        executionQuantity = owner.EntryExecutionQuantity;
                        executionPrice = owner.EntryExecutionPrice;
                        // The EXIT may already have fully settled before a
                        // delayed ENTRY callback pair completes. Retain the
                        // per-entry handling tombstone so a new stop cannot
                        // reverse an already-flat account.
                        terminationExitClaimed = owner.EntryExposureHandled
                            || (stopping && activeFlattenOwner != null
                                && activeFlattenOwner.Order != null);
                        if (terminationExitClaimed)
                        {
                            owner.EntryExposureHandled = true;
                            TrySettleEntryLifecycleUnderLock(owner);
                        }
                    }
                    // If termination already owns one EXIT, a new stop with a
                    // different OCO could reverse the flat account. Otherwise
                    // the exact execution receives protection only after its
                    // matching signed position callback has been observed.
                    if (!terminationExitClaimed)
                        SubmitProtectiveStop(
                            entryOrder, executionQuantity, executionPrice, owner
                        );
                }
            }
            catch (Exception error)
            {
                lock (stateLock)
                {
                    owner.EntryProtectionPending = false;
                    failedEntryProtectionCommands.Add(commandId);
                }
                LockAndProtect("PROTECTIVE_STOP_SUBMISSION_FAILED_" + error.GetType().Name);
            }
        }

        private string FlattenOwnedInstrument(Dictionary<string, object> command, bool emergency)
        {
            bool shutdownWatchdogCorrelation = emergency
                && String.Equals(Text(command, "reason_code"), "PROCESS_STOP_OPEN_POSITION", StringComparison.Ordinal);
            bool foreign;
            lock (stateLock)
            {
                foreign = foreignActivity;
                if (foreign)
                {
                    lockedOut = true;
                    reconciled = false;
                }
            }
            if (foreign)
            {
                // Owned cancellation is still safe, but an EXIT could close an
                // unowned Sim101 position. Report one truthful
                // terminal rejection; ExecuteCommand must not ACK afterward.
                if (emergency) CancelOwnedOrders();
                Diagnostic("FOREIGN_ACTIVITY_FLATTEN_REFUSED");
                return "FOREIGN_ACTIVITY_LOCKOUT";
            }
            // Process shutdown submits a durable EMERGENCY_FLATTEN before
            // heartbeats stop. It may settle before the heartbeat watchdog
            // observes activity, so give that path the same correlated final
            // reconciliation proof as the independent watchdog. Other
            // operator/risk emergency flattens retain their normal lifecycle
            // semantics and are not mislabeled as process-stop watchdogs.
            if (shutdownWatchdogCorrelation)
                BeginWatchdogSafetyCorrelation("EMERGENCY_FLATTEN_ACCEPTED");
            string refusal = SubmitOwnedFlattenOrder(
                Text(command, "command_id"), Text(command, "intent_id"),
                Text(command, "decision_id"), shutdownWatchdogCorrelation
            );
            if (refusal != null) return refusal;
            if (shutdownWatchdogCorrelation)
                TryPublishWatchdogSafetyReconciliation();
            return null;
        }

        private void ReleasePreSubmitFlattenClaimUnderLock(string commandId)
        {
            if (activeFlattenOwner != null
                || !String.Equals(pendingFlattenCommandId, commandId, StringComparison.Ordinal))
                return;
            flattenInProgress = false;
            flattenDeadlineUtc = DateTime.MaxValue;
            pendingFlattenCommandId = null;
            pendingFlattenIntentId = null;
            pendingFlattenDecisionId = null;
            pendingFlattenProtectiveCancellationExpected = false;
            pendingFlattenProtectiveCancellationObserved = false;
            pendingFlattenProtectiveOrder = null;
            pendingFlattenProtectiveName = null;
            pendingFlattenProtectiveOco = null;
            watchdogSafetyFlattenSubmitted = false;
            pendingWatchdogSafetyExitRequired = false;
        }

        private string SubmitOwnedFlattenOrder(
            string commandId, string intentId, string decisionId, bool watchdogSafety)
        {
            if (!ExactBoundAccount(paperAccount)) return "ACCOUNT_BINDING_LOST";
            if (String.IsNullOrWhiteSpace(commandId))
                return "FLATTEN_CORRELATION_ID_MISSING";
            Position position = CurrentPosition();
            if (position == null || position.Quantity == 0 || position.MarketPosition == MarketPosition.Flat)
                return "EXIT_WHILE_FLAT";
            int positionQuantity = position.Quantity;
            MarketPosition positionDirection = position.MarketPosition;
            double positionAveragePrice = position.AveragePrice;
            int signedPositionQuantity = positionDirection == MarketPosition.Short
                ? -positionQuantity : positionQuantity;
            // This guard is deliberately repeated at the mutation boundary.
            // No caller (shutdown, watchdog, or command) may flatten a naked
            // manual/foreign exact-instrument position by skipping an earlier
            // reconciliation pass.
            if (!EnsureCurrentPositionOwnershipProven(signedPositionQuantity))
                return "POSITION_OWNERSHIP_UNPROVEN";
            // The Lane III boundary may never emit a native order larger than
            // one contract, even as a safety action. An oversize aggregate is
            // an ownership/reconciliation incident for operator handling.
            if (positionQuantity != MaximumQuantity)
                return "POSITION_QUANTITY_BREACH";
            Position confirmedPosition = CurrentPosition();
            int confirmedSignedQuantity = confirmedPosition == null
                || confirmedPosition.MarketPosition == MarketPosition.Flat
                ? 0 : confirmedPosition.MarketPosition == MarketPosition.Short
                    ? -confirmedPosition.Quantity : confirmedPosition.Quantity;
            if (confirmedSignedQuantity != signedPositionQuantity)
                return "POSITION_CHANGED_DURING_FLATTEN_CLAIM";
            OrderAction action;
            if (positionDirection == MarketPosition.Long)
                action = OrderAction.Sell;
            else if (positionDirection == MarketPosition.Short)
                action = OrderAction.BuyToCover;
            else
                return "POSITION_DIRECTION_UNRESOLVED";
            int quantity = positionQuantity;
            string name = "BZ-L3G-X-" + Fragment(commandId);
            List<Order> workingProtectiveOrders = OwnedWorkingOrders("PROTECTIVE");
            if (workingProtectiveOrders.Count > 1)
                return "PROTECTIVE_OWNERSHIP_AMBIGUOUS";
            string oco = workingProtectiveOrders.Count == 1
                ? workingProtectiveOrders[0].Oco : String.Empty;
            if (workingProtectiveOrders.Count == 1 && String.IsNullOrWhiteSpace(oco))
                return "PROTECTIVE_OCO_UNAVAILABLE";
            OrderAction recoveryProtectionAction = action;
            double rawRecoveryStop = positionDirection == MarketPosition.Long
                ? positionAveragePrice - ProtectiveStopDistance
                : positionAveragePrice + ProtectiveStopDistance;
            double recoveryProtectionStopPrice = Math.Round(
                rawRecoveryStop / ExactTickSize, MidpointRounding.AwayFromZero) * ExactTickSize;
            if (workingProtectiveOrders.Count == 1)
            {
                Order candidate = workingProtectiveOrders[0];
                if (candidate.Quantity != MaximumQuantity || candidate.StopPrice <= 0
                    || candidate.OrderAction != recoveryProtectionAction)
                    return "PROTECTIVE_SHAPE_INVALID";
                recoveryProtectionStopPrice = candidate.StopPrice;
            }
            if (recoveryProtectionStopPrice <= 0)
                return "PROTECTIVE_STOP_PRICE_UNAVAILABLE";
            lock (stateLock)
            {
                if (HasAmbiguousOwnedOutcomeUnderLock())
                    return "OWNED_ORDER_OUTCOME_UNKNOWN";
                if (foreignActivity) return "FOREIGN_ACTIVITY_LOCKOUT";
                if (flattenInProgress) return "FLATTEN_ALREADY_IN_PROGRESS";
                // Claim the one native mutation before CreateOrder/Submit. A
                // thrown or ambiguous call leaves this attempt tombstoned;
                // duplicate callbacks and watchdog passes cannot blind-retry.
                flattenInProgress = true;
                flattenDeadlineUtc = DateTime.UtcNow.AddSeconds(ProtectiveAcceptanceSeconds);
                protectiveDeadlineUtc = DateTime.MaxValue;
                pendingFlattenCommandId = commandId;
                pendingFlattenIntentId = intentId;
                pendingFlattenDecisionId = decisionId;
                pendingFlattenExpectedQuantity = quantity;
                pendingFlattenFilledQuantity = 0;
                pendingFlattenExitExecutionQuantity = 0;
                pendingFlattenProtectiveExecutionQuantity = 0;
                pendingFlattenExecutionObserved = false;
                pendingFlattenOrderTerminalObserved = false;
                pendingFlattenExitFilledTerminalObserved = false;
                pendingFlattenProtectiveFilledTerminalObserved = false;
                pendingFlattenProtectiveCancellationExpected = workingProtectiveOrders.Count == 1;
                pendingFlattenProtectiveCancellationObserved = false;
                pendingFlattenCompletedByProtective = false;
                pendingFlattenExitNoFillTerminalObserved = false;
                pendingFlattenRecoveryProtectionSubmitted = false;
                pendingFlattenRecoveryProtectionDispatchPending = false;
                pendingFlattenRecoveryProtectionAction = recoveryProtectionAction;
                pendingFlattenRecoveryProtectionQuantity = MaximumQuantity;
                pendingFlattenRecoveryProtectionStopPrice = recoveryProtectionStopPrice;
                pendingFlattenProtectiveOrder = workingProtectiveOrders.Count == 1
                    ? workingProtectiveOrders[0] : null;
                pendingFlattenProtectiveName = pendingFlattenProtectiveOrder == null
                    ? null : pendingFlattenProtectiveOrder.Name;
                pendingFlattenProtectiveOco = pendingFlattenProtectiveOrder == null
                    ? null : pendingFlattenProtectiveOrder.Oco;
                pendingFlattenExecutionIds.Clear();
                activeFlattenOwner = null;
                if (watchdogSafety)
                {
                    watchdogSafetyFlattenSubmitted = true;
                    pendingWatchdogSafetyExitRequired = true;
                }
            }
            Order order;
            try
            {
                order = paperAccount.CreateOrder(
                    paperInstrument, action, OrderType.Market, OrderEntry.Automated,
                    TimeInForce.Day, quantity, 0, 0, oco, name,
                    NinjaTrader.Core.Globals.MaxDate, null
                );
                if (order == null)
                    throw new InvalidOperationException("CREATE_ORDER_RETURNED_NULL");
                if (!ExactBoundAccount(order.Account))
                    throw new InvalidOperationException("ACCOUNT_BINDING_LOST");
            }
            catch (Exception error)
            {
                // No Order was submitted, so releasing this claim cannot create
                // a duplicate native EXIT. It lets the correlated safety path
                // make one independently pre-owned attempt instead of remaining
                // permanently suppressed by a phantom flatten.
                lock (stateLock)
                    ReleasePreSubmitFlattenClaimUnderLock(commandId);
                return "FLATTEN_CREATE_FAILED_" + error.GetType().Name;
            }
            Position preSubmitPosition = CurrentPosition();
            int preSubmitSignedQuantity = preSubmitPosition == null
                || preSubmitPosition.MarketPosition == MarketPosition.Flat
                ? 0 : preSubmitPosition.MarketPosition == MarketPosition.Short
                    ? -preSubmitPosition.Quantity : preSubmitPosition.Quantity;
            if (preSubmitSignedQuantity != signedPositionQuantity
                || !EnsureCurrentPositionOwnershipProven(preSubmitSignedQuantity))
            {
                lock (stateLock)
                    ReleasePreSubmitFlattenClaimUnderLock(commandId);
                return "POSITION_CHANGED_BEFORE_FLATTEN_SUBMIT";
            }
            OwnedOrder owner = new OwnedOrder(
                commandId, intentId, decisionId, name, "EXIT", order, DateTime.UtcNow
            );
            lock (stateLock)
            {
                if (foreignActivity || HasAmbiguousOwnedOutcomeUnderLock()
                    || !flattenInProgress || activeFlattenOwner != null
                    || !ownedPositionEstablished
                    || ownedPositionSignedQuantity != signedPositionQuantity
                    || !String.Equals(pendingFlattenCommandId, commandId, StringComparison.Ordinal))
                {
                    ReleasePreSubmitFlattenClaimUnderLock(commandId);
                    return "FLATTEN_AUTHORITY_CHANGED_BEFORE_SUBMIT";
                }
                activeFlattenOwner = owner;
                ownedByName[name] = owner;
                // CreateOrder succeeded and the exact EXIT is now pre-owned.
                // Only this point may suppress protection for a delayed ENTRY
                // execution; the earlier flatten tombstone alone is not proof
                // that any exit order exists.
                MarkPendingEntryExposureHandledByExitUnderLock(signedPositionQuantity);
            }
            // The working stop and market EXIT share a native OCO group. Keep
            // the stop live through Submit; NinjaTrader cancels the losing leg
            // when either exit fills, avoiding an unprotected submit window.
            if (!ExactBoundAccount(paperAccount) || !ExactBoundAccount(order.Account))
                return "ACCOUNT_BINDING_LOST";
            paperAccount.Submit(new[] { order });
            return null;
        }

        private void TryRearmProtectionAfterDefinitiveFlattenNoFill()
        {
            Position observedPosition = CurrentPosition();
            if (observedPosition == null || observedPosition.Quantity == 0
                || observedPosition.MarketPosition == MarketPosition.Flat)
                return;
            bool schedule;
            lock (stateLock)
            {
                schedule = flattenInProgress
                    && activeFlattenOwner != null
                    && activeFlattenOwner.Terminal
                    && pendingFlattenExitNoFillTerminalObserved
                    && pendingFlattenFilledQuantity == 0
                    && !pendingFlattenCompletedByProtective
                    && (!pendingFlattenProtectiveCancellationExpected
                        || pendingFlattenProtectiveCancellationObserved)
                    && !pendingFlattenRecoveryProtectionSubmitted
                    && !pendingFlattenRecoveryProtectionDispatchPending
                    && !HasEntryCapableOfAddingExposureUnderLock()
                    && !foreignActivity
                    && !HasAmbiguousOwnedOutcomeUnderLock()
                    && !ownedByName.Values.Any(value =>
                        value != activeFlattenOwner
                        && !Object.ReferenceEquals(value.Order, pendingFlattenProtectiveOrder)
                        && value.Order != null && !value.Terminal
                        && !EntryExposureFullyAccountedUnderLock(value));
                // Queueing is not native submission. Keep a separate in-flight
                // claim so a proven dispatcher/CreateOrder failure may be
                // retried without granting a second stop Submit attempt.
                if (schedule) pendingFlattenRecoveryProtectionDispatchPending = true;
            }
            if (!schedule) return;
            try
            {
                NinjaTrader.Core.Globals.RandomDispatcher.BeginInvoke(new Action(delegate
                {
                    try
                    {
                        lock (nativeMutationGate)
                        {
                            Position position = CurrentPosition();
                            int quantity = position == null ? 0 : position.Quantity;
                            MarketPosition direction = position == null
                                ? MarketPosition.Flat : position.MarketPosition;
                            OrderAction action;
                            int protectionQuantity;
                            double stopPrice;
                            string commandId;
                            string intentId;
                            string decisionId;
                            lock (stateLock)
                            {
                                bool stillEligible = flattenInProgress
                                    && activeFlattenOwner != null
                                    && activeFlattenOwner.Terminal
                                    && pendingFlattenExitNoFillTerminalObserved
                                    && pendingFlattenFilledQuantity == 0
                                    && !pendingFlattenCompletedByProtective
                                    && (!pendingFlattenProtectiveCancellationExpected
                                        || pendingFlattenProtectiveCancellationObserved)
                                    && pendingFlattenRecoveryProtectionDispatchPending
                                    && !pendingFlattenRecoveryProtectionSubmitted
                                    && !foreignActivity
                                    && !HasAmbiguousOwnedOutcomeUnderLock();
                                if (!stillEligible || quantity == 0 || direction == MarketPosition.Flat)
                                {
                                    pendingFlattenRecoveryProtectionDispatchPending = false;
                                    return;
                                }
                                action = pendingFlattenRecoveryProtectionAction;
                                protectionQuantity = pendingFlattenRecoveryProtectionQuantity;
                                stopPrice = pendingFlattenRecoveryProtectionStopPrice;
                                bool shapeMatches = quantity == MaximumQuantity
                                    && protectionQuantity == MaximumQuantity
                                    && stopPrice > 0
                                    && ownedPositionEstablished
                                    && ownedPositionSignedQuantity
                                        == (direction == MarketPosition.Short ? -quantity : quantity)
                                    && ((direction == MarketPosition.Long && action == OrderAction.Sell)
                                        || (direction == MarketPosition.Short && action == OrderAction.BuyToCover));
                                bool otherUnresolvedOwner = ownedByName.Values.Any(value =>
                                    value != activeFlattenOwner
                                    && !Object.ReferenceEquals(value.Order, pendingFlattenProtectiveOrder)
                                    && value.Order != null && !value.Terminal
                                    && !EntryExposureFullyAccountedUnderLock(value));
                                if (otherUnresolvedOwner)
                                {
                                    pendingFlattenRecoveryProtectionDispatchPending = false;
                                    Diagnostic("FLATTEN_NO_FILL_PROTECTION_REARM_DEFERRED");
                                    return;
                                }
                                if (!shapeMatches)
                                {
                                    foreignActivity = true;
                                    lockedOut = true;
                                    reconciled = false;
                                    pendingFlattenRecoveryProtectionDispatchPending = false;
                                    Diagnostic("FLATTEN_NO_FILL_PROTECTION_REARM_REFUSED");
                                    return;
                                }
                                commandId = pendingFlattenCommandId;
                                intentId = pendingFlattenIntentId;
                                decisionId = pendingFlattenDecisionId;
                            }
                            string name = "BZ-L3G-S-" + Fragment(commandId) + "-R";
                            string oco = "BZ-L3G-OCO-R-" + Fragment(commandId);
                            Order stop;
                            try
                            {
                                stop = paperAccount.CreateOrder(
                                    paperInstrument, action, OrderType.StopMarket,
                                    OrderEntry.Automated, TimeInForce.Gtc,
                                    protectionQuantity, 0, stopPrice, oco, name,
                                    NinjaTrader.Core.Globals.MaxDate, null
                                );
                                if (stop == null)
                                    throw new InvalidOperationException("CREATE_ORDER_RETURNED_NULL");
                                if (!ExactBoundAccount(stop.Account))
                                    throw new InvalidOperationException("ACCOUNT_BINDING_LOST");
                            }
                            catch (Exception error)
                            {
                                // CreateOrder failed before any native Submit.
                                // Release only the dispatcher claim; a later
                                // watchdog pass may safely try to create the one
                                // replacement again.
                                lock (stateLock)
                                    pendingFlattenRecoveryProtectionDispatchPending = false;
                                Diagnostic("FLATTEN_NO_FILL_PROTECTION_CREATE_FAILED_" + error.GetType().Name);
                                return;
                            }
                            OwnedOrder protection = new OwnedOrder(
                                commandId, intentId, decisionId, name,
                                "PROTECTIVE", stop, DateTime.UtcNow
                            );
                            Position confirmedPosition = CurrentPosition();
                            int confirmedSignedQuantity = confirmedPosition == null
                                || confirmedPosition.MarketPosition == MarketPosition.Flat
                                ? 0 : confirmedPosition.MarketPosition == MarketPosition.Short
                                    ? -confirmedPosition.Quantity : confirmedPosition.Quantity;
                            int expectedSignedQuantity = direction == MarketPosition.Short
                                ? -quantity : quantity;
                            lock (stateLock)
                            {
                                if (!pendingFlattenRecoveryProtectionDispatchPending
                                    || pendingFlattenRecoveryProtectionSubmitted
                                    || foreignActivity || HasAmbiguousOwnedOutcomeUnderLock()
                                    || confirmedSignedQuantity != expectedSignedQuantity
                                    || !ownedPositionEstablished
                                    || ownedPositionSignedQuantity != confirmedSignedQuantity)
                                {
                                    pendingFlattenRecoveryProtectionDispatchPending = false;
                                    Diagnostic("FLATTEN_NO_FILL_PROTECTION_POSITION_CHANGED_BEFORE_SUBMIT");
                                    return;
                                }
                                ownedByName[name] = protection;
                                pendingFlattenRecoveryProtectionSubmitted = true;
                                pendingFlattenRecoveryProtectionDispatchPending = false;
                                protectiveOrder = stop;
                                pendingProtectionCommandId = commandId;
                                protectiveDeadlineUtc = DateTime.UtcNow.AddSeconds(ProtectiveAcceptanceSeconds);
                                // Retarget only after the old EXIT is definitively
                                // terminal with zero fill and the replacement is
                                // pre-owned. This new stop is not an OCO sibling
                                // of any live EXIT.
                                pendingFlattenProtectiveOrder = stop;
                                pendingFlattenProtectiveName = name;
                                pendingFlattenProtectiveOco = oco;
                                pendingFlattenProtectiveCancellationExpected = false;
                                pendingFlattenProtectiveCancellationObserved = false;
                            }
                            try
                            {
                                if (!ExactBoundAccount(paperAccount) || !ExactBoundAccount(stop.Account))
                                {
                                    Diagnostic("ACCOUNT_BINDING_LOST");
                                    return;
                                }
                                paperAccount.Submit(new[] { stop });
                                Diagnostic("FLATTEN_NO_FILL_PROTECTION_REARMED");
                            }
                            catch (Exception error)
                            {
                                // Submit may have crossed the native boundary.
                                // Retain the pre-owned tombstone and never retry.
                                Diagnostic("FLATTEN_NO_FILL_PROTECTION_SUBMIT_UNKNOWN_" + error.GetType().Name);
                            }
                        }
                    }
                    catch (Exception error)
                    {
                        lock (stateLock)
                            if (!pendingFlattenRecoveryProtectionSubmitted)
                                pendingFlattenRecoveryProtectionDispatchPending = false;
                        Diagnostic("FLATTEN_NO_FILL_PROTECTION_REARM_FAILED_" + error.GetType().Name);
                    }
                }));
            }
            catch (Exception error)
            {
                lock (stateLock)
                    pendingFlattenRecoveryProtectionDispatchPending = false;
                Diagnostic("FLATTEN_NO_FILL_PROTECTION_DISPATCH_FAILED_" + error.GetType().Name);
            }
        }

        private void CancelOwnedOrders()
        {
            if (!ExactBoundAccount(paperAccount))
            {
                Diagnostic("ACCOUNT_BINDING_LOST");
                return;
            }
            List<Order> orders;
            lock (stateLock)
                orders = ownedByName.Values.Where(value =>
                    value.Order != null
                    && ExactBoundAccount(value.Order.Account)
                    && UnresolvedNativeOrderState(value.Order.OrderState)
                ).Select(value => value.Order).Distinct().ToList();
            if (orders.Count > 0 && ExactBoundAccount(paperAccount)
                && orders.All(order => ExactBoundAccount(order.Account)))
                paperAccount.Cancel(orders);
        }

        private void DispatchOwnedOrderCancellationAfterExpectedFlat()
        {
            lock (stateLock)
            {
                if (flatOwnedOrderCancellationDispatchPending) return;
                flatOwnedOrderCancellationDispatchPending = true;
            }
            try
            {
                NinjaTrader.Core.Globals.RandomDispatcher.BeginInvoke(new Action(delegate
                {
                    Exception failure = null;
                    try
                    {
                        // A PositionUpdate can interleave after a reducing order
                        // is pre-owned but before its native Submit. Queue behind
                        // that submit's mutation gate, then cancel the exact
                        // owned handle before a stale EXIT/stop can reverse flat.
                        lock (nativeMutationGate)
                            CancelOwnedOrders();
                    }
                    catch (Exception error)
                    {
                        failure = error;
                    }
                    finally
                    {
                        lock (stateLock)
                            flatOwnedOrderCancellationDispatchPending = false;
                    }
                    if (failure != null)
                        LockAndProtect("FLAT_OWNED_ORDER_CANCELLATION_FAILED_" + failure.GetType().Name);
                }));
            }
            catch (Exception error)
            {
                lock (stateLock)
                    flatOwnedOrderCancellationDispatchPending = false;
                LockAndProtect("FLAT_OWNED_ORDER_CANCELLATION_DISPATCH_FAILED_" + error.GetType().Name);
            }
        }

        private List<Order> OwnedWorkingOrders(string role)
        {
            lock (stateLock)
                return ownedByName.Values.Where(value => (role == null || value.Role == role) && value.Order != null && Working(value.Order.OrderState)).Select(value => value.Order).Distinct().ToList();
        }

        private void RehydrateOwnedWorkingOrders()
        {
            if (!ExactBoundAccount(paperAccount) || paperInstrument == null) return;
            // Do not hold the account collection lock while updating AddOn
            // state: NinjaTrader can deliver OrderUpdate concurrently. The
            // snapshot is used only to restore names this instance already
            // owns; it never submits, changes, or flattens an order.
            List<Order> restored;
            lock (paperAccount.Orders)
            {
                restored = paperAccount.Orders
                    .Where(order => order != null
                        && ExactBoundAccount(order.Account)
                        && UnresolvedNativeOrderState(order.OrderState)
                        && order.Instrument != null
                        && String.Equals(order.Instrument.FullName, ExactInstrumentName, StringComparison.Ordinal)
                        && IsOwnedName(order.Name))
                    .ToList();
            }
            if (restored.Count == 0)
            {
                lock (stateLock)
                    if (protectiveOrder != null && !Working(protectiveOrder.OrderState))
                        protectiveOrder = null;
                return;
            }
            bool restoredUnknownOutcome = false;
            lock (stateLock)
            {
                int restoredExits = 0;
                int restoredProtectives = 0;
                Order adoptedExit = null;
                Order adoptedProtective = null;
                foreach (Order order in restored)
                {
                    string name = order.Name ?? String.Empty;
                    OwnedOrder owner;
                    if (!ownedByName.TryGetValue(name, out owner))
                    {
                        owner = OwnedOrder.Restored(order);
                        ownedByName[name] = owner;
                    }
                    else if (owner.Order != null && !Object.ReferenceEquals(owner.Order, order))
                    {
                        owner.OutcomeUnknown = true;
                        restoredUnknownOutcome = true;
                        foreignActivity = true;
                        lockedOut = true;
                        reconciled = false;
                        continue;
                    }
                    else
                    {
                        owner.Order = order;
                    }
                    owner.Terminal = !Working(order.OrderState);
                    if (order.OrderState == OrderState.Unknown)
                    {
                        owner.OutcomeUnknown = true;
                        restoredUnknownOutcome = true;
                        lockedOut = true;
                        reconciled = false;
                        continue;
                    }
                    if (owner.Role == "EXIT")
                    {
                        restoredExits++;
                        if (!flattenInProgress)
                        {
                            flattenInProgress = true;
                            flattenDeadlineUtc = DateTime.UtcNow.AddSeconds(ProtectiveAcceptanceSeconds);
                            pendingFlattenCommandId = owner.CommandId;
                            pendingFlattenIntentId = owner.IntentId;
                            pendingFlattenDecisionId = owner.DecisionId;
                            pendingFlattenExpectedQuantity = Math.Max(1, order.Quantity);
                            pendingFlattenFilledQuantity = 0;
                            pendingFlattenExitExecutionQuantity = 0;
                            pendingFlattenProtectiveExecutionQuantity = 0;
                            pendingFlattenExecutionObserved = false;
                            pendingFlattenOrderTerminalObserved = false;
                            pendingFlattenExitFilledTerminalObserved = false;
                            pendingFlattenProtectiveFilledTerminalObserved = false;
                            pendingFlattenProtectiveCancellationObserved = false;
                            pendingFlattenCompletedByProtective = false;
                            pendingFlattenExitNoFillTerminalObserved = false;
                            pendingFlattenRecoveryProtectionSubmitted = false;
                            pendingFlattenRecoveryProtectionDispatchPending = false;
                            pendingFlattenExecutionIds.Clear();
                            activeFlattenOwner = owner;
                            adoptedExit = order;
                        }
                    }
                    else if (owner.Role == "PROTECTIVE")
                    {
                        restoredProtectives++;
                        if (adoptedProtective == null)
                            adoptedProtective = order;
                        protectiveOrder = order;
                        pendingProtectionCommandId = owner.CommandId;
                        protectiveDeadlineUtc = DateTime.MaxValue;
                    }
                }
                if (restoredExits > 1 || restoredProtectives > 1)
                {
                    foreignActivity = true;
                    lockedOut = true;
                    reconciled = false;
                }
                // Only a newly adopted, previously unknown EXIT may initialize
                // this correlation. A watchdog poll must never retarget or
                // clear a fence that is waiting for its exact Cancelled event.
                if (adoptedExit != null && restoredExits == 1)
                    MarkPendingEntryExposureHandledByExitUnderLock(0);
                if (adoptedExit != null && restoredExits == 1 && restoredProtectives == 1)
                {
                    string exitOco = adoptedExit.Oco;
                    string protectiveOco = adoptedProtective == null ? null : adoptedProtective.Oco;
                    if (!String.IsNullOrWhiteSpace(exitOco)
                        && String.Equals(exitOco, protectiveOco, StringComparison.Ordinal))
                    {
                        pendingFlattenProtectiveCancellationExpected = true;
                        pendingFlattenProtectiveOrder = adoptedProtective;
                        pendingFlattenProtectiveName = adoptedProtective.Name;
                        pendingFlattenProtectiveOco = protectiveOco;
                        pendingFlattenRecoveryProtectionAction = adoptedProtective.OrderAction;
                        pendingFlattenRecoveryProtectionQuantity = adoptedProtective.Quantity;
                        pendingFlattenRecoveryProtectionStopPrice = adoptedProtective.StopPrice;
                    }
                    else
                    {
                        foreignActivity = true;
                        lockedOut = true;
                        reconciled = false;
                    }
                }
            }
            if (restoredUnknownOutcome)
                LockAndProtect("RESTORED_OWNED_ORDER_OUTCOME_UNKNOWN");
        }

        private static bool Working(OrderState state)
        {
            // NinjaTrader may introduce or surface active intermediary states
            // such as Suspended and AcceptedByRisk. Enumerating only familiar
            // states can make a live order disappear from safety decisions.
            return !Order.IsTerminalState(state);
        }

        private static bool UnresolvedNativeOrderState(OrderState state)
        {
            return Working(state) || state == OrderState.Unknown;
        }

        private void SettleFlattenOwnershipIfPossible(bool positionOpen)
        {
            Dictionary<string, object> deferred = null;
            if (OwnedWorkingOrders(null).Count != 0) return;
            lock (stateLock)
            {
                if (!flattenInProgress || positionOpen) return;
                if (HasAmbiguousOwnedOutcomeUnderLock()) return;
                // The uniquely named EXIT is registered before Submit, so
                // callback order cannot lose ownership. Do not release entry
                // authority until its complete execution and terminal order
                // are both observed; a missing callback is an unresolved
                // settlement, never permission to reverse.
                bool losingLegTerminal = pendingFlattenCompletedByProtective
                    ? pendingFlattenExitNoFillTerminalObserved
                    : (!pendingFlattenProtectiveCancellationExpected
                        || pendingFlattenProtectiveCancellationObserved);
                if (!pendingFlattenExecutionObserved || !pendingFlattenOrderTerminalObserved
                    || !losingLegTerminal) return;
                flattenInProgress = false;
                flattenDeadlineUtc = DateTime.MaxValue;
                pendingFlattenCommandId = null;
                pendingFlattenIntentId = null;
                pendingFlattenDecisionId = null;
                activeFlattenOwner = null;
                pendingFlattenProtectiveOrder = null;
                pendingFlattenProtectiveName = null;
                pendingFlattenProtectiveOco = null;
                pendingFlattenProtectiveCancellationExpected = false;
                pendingFlattenProtectiveCancellationObserved = false;
                deferred = pendingFlattenReconciliationCommand;
                pendingFlattenReconciliationCommand = null;
            }
            if (deferred != null) CompleteDeferredFlattenReconciliation(deferred);
        }

        private bool DeferReconciliationUntilFlattenSettled(
            Dictionary<string, object> command)
        {
            lock (stateLock)
            {
                if (!flattenInProgress) return false;
                if (pendingFlattenReconciliationCommand == null)
                    pendingFlattenReconciliationCommand = new Dictionary<string, object>(command);
                else if (!String.Equals(
                    CommandOutcomeKey(pendingFlattenReconciliationCommand),
                    CommandOutcomeKey(command), StringComparison.Ordinal))
                {
                    // The authenticated sender must wait for the pending ACK.
                    // Preserve the first reconciliation and fail this second
                    // reservation closed instead of replacing it.
                    SetCommandOutcome(command, "REJECTED", "FLATTEN_RECONCILIATION_ALREADY_PENDING");
                    Reject(command, "FLATTEN_RECONCILIATION_ALREADY_PENDING", Text(command, "command_id"));
                }
                return true;
            }
        }

        private void CompleteDeferredFlattenReconciliation(
            Dictionary<string, object> command)
        {
            bool available;
            lock (stateLock) available = authenticated && !stopping;
            if (!available)
            {
                SetCommandOutcome(command, "UNKNOWN", "BOUNDARY_STOPPED_BEFORE_RECONCILIATION");
                Diagnostic("DEFERRED_RECONCILIATION_UNRESOLVED_AT_STOP");
                return;
            }
            if (!SendReconciliation())
            {
                lock (stateLock) reconciled = false;
                SetCommandOutcome(command, "UNKNOWN", "RECONCILIATION_SEND_FAILED");
                LockAndProtect("DEFERRED_RECONCILIATION_SEND_FAILED");
                Reject(command, "RECONCILIATION_SEND_FAILED", Text(command, "command_id"));
                return;
            }
            SetCommandOutcome(command, "ACCEPTED", "ACCEPTED");
            Acknowledge(command, "ACCEPTED", false);
        }

        private Position CurrentPosition()
        {
            if (!ExactBoundAccount(paperAccount) || paperInstrument == null) return null;
            lock (paperAccount.Positions)
                return paperAccount.Positions.FirstOrDefault(position => position != null
                    && ExactBoundAccount(position.Account)
                    && position.Instrument != null
                    && String.Equals(position.Instrument.FullName, ExactInstrumentName, StringComparison.Ordinal));
        }

        private void SendHello()
        {
            Dictionary<string, object> hello = new Dictionary<string, object>();
            hello["schema"] = WireSchema;
            hello["message_type"] = "HELLO";
            hello["bridge_instance_id"] = bridgeInstanceId;
            hello["ninjatrader_session_id"] = ninjaTraderSessionId;
            hello["addon_protocol_version"] = AddonProtocolVersion;
            hello["addon_source_fingerprint"] = AddonSourceFingerprint;
            hello["addon_build_fingerprint"] = AssemblyHash();
            hello["addon_build_timestamp"] = AssemblyBuildTimestamp();
            hello["account_name"] = ExactAccountName;
            hello["account_class"] = ExactAccountClass;
            hello["instrument"] = ExactInstrumentName;
            hello["capability"] = ExactCapability;
            hello["timestamp"] = UtcNow();
            hello["nonce"] = Guid.NewGuid().ToString("N");
            SendSigned(hello);
        }

        private bool SendReconciliation(
            string safetyEventId = null, bool requireWatchdogFlat = false,
            bool safetySettlementFinal = false, long safetySettlementSequence = 0)
        {
            if (!ExactBoundAccount(paperAccount)) return false;
            EnsureCurrentPositionOwnershipProven();
            Position position = CurrentPosition();
            int quantity = 0;
            if (position != null)
            {
                if (position.MarketPosition == MarketPosition.Long) quantity = position.Quantity;
                else if (position.MarketPosition == MarketPosition.Short) quantity = -position.Quantity;
            }
            int working = 0;
            int entryWorking = 0;
            bool foreign = false;
            List<Order> orderSnapshot;
            lock (paperAccount.Orders)
                orderSnapshot = paperAccount.Orders.Where(order =>
                    order != null && ExactBoundAccount(order.Account)
                    && UnresolvedNativeOrderState(order.OrderState)).ToList();
            // Process the snapshot outside NinjaTrader's account collection
            // lock. ExpectedFlattenOwner takes stateLock, while watchdog paths
            // can inspect account collections from state-locked code.
            foreach (Order order in orderSnapshot)
            {
                bool exact = order.Instrument != null && String.Equals(order.Instrument.FullName, ExactInstrumentName, StringComparison.Ordinal);
                OwnedOrder owner = null;
                lock (stateLock)
                    ownedByName.TryGetValue(order.Name ?? String.Empty, out owner);
                bool owned = owner != null || IsOwnedName(order.Name);
                if (!exact || !owned) foreign = true;
                if (exact) working++;
                if (exact && owned && owner != null && owner.Role == "ENTRY") entryWorking++;
            }
            lock (paperAccount.Positions)
                if (paperAccount.Positions.Any(item => item != null
                    && ExactBoundAccount(item.Account) && item.Quantity != 0
                    && (item.Instrument == null || !String.Equals(item.Instrument.FullName, ExactInstrumentName, StringComparison.Ordinal)))) foreign = true;
            // Position and order collections have separate account locks. A
            // fill can arrive while the order scan runs, so a safety proof
            // must reject a mixed-time snapshot rather than calling it whole.
            Position stablePosition = CurrentPosition();
            int stableQuantity = 0;
            if (stablePosition != null)
            {
                if (stablePosition.MarketPosition == MarketPosition.Long) stableQuantity = stablePosition.Quantity;
                else if (stablePosition.MarketPosition == MarketPosition.Short) stableQuantity = -stablePosition.Quantity;
            }
            if (requireWatchdogFlat && stableQuantity != quantity)
                return false;
            if (requireWatchdogFlat)
            {
                lock (stateLock)
                    if (pendingWatchdogSafetyExitRequired
                        && (!pendingFlattenExecutionObserved || !pendingFlattenOrderTerminalObserved
                            || (pendingFlattenCompletedByProtective
                                ? !pendingFlattenExitNoFillTerminalObserved
                                : (pendingFlattenProtectiveCancellationExpected
                                    && !pendingFlattenProtectiveCancellationObserved))))
                        return false;
            }
            lock (stateLock)
            {
                if (HasUnsettledEntryLifecycleUnderLock()
                    || HasAmbiguousOwnedOutcomeUnderLock())
                {
                    reconciled = false;
                    return false;
                }
                // Foreign ownership is a lifetime safety fact. A later clean
                // scan cannot prove a disappeared foreign order did not fill.
                foreignActivity = foreignActivity || foreign;
                foreign = foreignActivity;
                reconciled = !foreign && Math.Abs(quantity) <= MaximumQuantity;
                if (foreign) lockedOut = true;
            }
            if (Math.Abs(quantity) > MaximumQuantity)
                LockAndProtect("MAXIMUM_QUANTITY_BREACH");
            if (requireWatchdogFlat && (quantity != 0 || working != 0 || entryWorking != 0 || foreign))
                return false;
            Dictionary<string, object> message = SessionMessage("RECONCILIATION");
            message["receipt_id"] = "l3g-reconcile-" + Guid.NewGuid().ToString("N");
            message["account_name"] = ExactAccountName;
            message["account_class"] = ExactAccountClass;
            message["instrument"] = ExactInstrumentName;
            message["position_quantity"] = quantity;
            message["working_order_count"] = working;
            message["working_entry_count"] = entryWorking;
            message["position_snapshot_complete"] = true;
            message["order_snapshot_complete"] = true;
            message["foreign_activity"] = foreign;
            message["protective_stop_state"] = protectiveOrder == null ? "NONE" : protectiveOrder.OrderState.ToString().ToUpperInvariant();
            if (!String.IsNullOrWhiteSpace(safetyEventId))
                message["safety_event_id"] = safetyEventId;
            if (safetySettlementFinal)
            {
                message["safety_settlement_final"] = true;
                message["safety_settlement_sequence"] = safetySettlementSequence;
            }
            return SendSigned(message);
        }

        private void TryPublishWatchdogSafetyReconciliation()
        {
            string safetyEventId;
            DateTime activatedAt;
            bool eventPublished;
            long sessionGeneration;
            lock (stateLock)
            {
                safetyEventId = pendingWatchdogSafetyEventId;
                activatedAt = pendingWatchdogSafetyActivatedUtc;
                eventPublished = pendingWatchdogSafetyEventPublished;
                sessionGeneration = authenticatedSessionGeneration;
            }
            if (String.IsNullOrWhiteSpace(safetyEventId) || !authenticated || !eventPublished) return;
            bool finalProofReserved = false;
            bool sent = false;
            long settlementSequence = 0;
            try
            {
                bool settlementFinal = DateTime.UtcNow - activatedAt >= TimeSpan.FromSeconds(WatchdogSettlementSeconds);
                lock (stateLock)
                {
                    if (!String.Equals(pendingWatchdogSafetyEventId, safetyEventId, StringComparison.Ordinal)
                        || authenticatedSessionGeneration != sessionGeneration)
                        return;
                    if (settlementFinal)
                    {
                        if (pendingWatchdogSafetyFinalProofAttempts >= MaximumWatchdogFinalProofAttempts
                            || pendingWatchdogSafetyFinalProofInFlight)
                            return;
                        // Reserve the next sequence without consuming the
                        // bounded attempt. SendReconciliation returns false
                        // for nonflat/mixed snapshots and failed writes.
                        settlementSequence = pendingWatchdogSafetySettlementSequence + 1;
                        pendingWatchdogSafetyFinalProofInFlight = true;
                        finalProofReserved = true;
                    }
                }
                // Keep the correlation pending after a clean snapshot. The
                // watchdog loop emits a bounded set of stable proofs, while
                // the runtime rejects a one-shot snapshot that could straddle
                // a late fill/cancel callback. Bounded attempts prevent a
                // locked-out AddOn from creating an unbounded ledger stream.
                sent = SendReconciliation(safetyEventId, true, settlementFinal, settlementSequence);
            }
            catch (Exception error)
            {
                // Keep the id pending.  A later terminal order/position
                // callback will retry the signed exact-flat proof.
                Diagnostic("WATCHDOG_RECONCILIATION_FAILED_" + error.GetType().Name);
            }
            finally
            {
                if (finalProofReserved) lock (stateLock)
                {
                    // An old socket may finish a write after a re-auth or a
                    // newer safety correlation. It must not mutate the new
                    // connection's proof budget or sequence.
                    if (String.Equals(pendingWatchdogSafetyEventId, safetyEventId, StringComparison.Ordinal)
                        && authenticatedSessionGeneration == sessionGeneration)
                    {
                        if (sent)
                        {
                            pendingWatchdogSafetySettlementSequence = settlementSequence;
                            pendingWatchdogSafetyFinalProofAttempts++;
                        }
                        pendingWatchdogSafetyFinalProofInFlight = false;
                    }
                }
            }
        }

        private void BeginWatchdogSafetyCorrelation(string reason)
        {
            string safetyEventId = "l3g-safety-" + Guid.NewGuid().ToString("N");
            lock (stateLock)
            {
                watchdogSafetyDispatchStarted = true;
                pendingWatchdogSafetyEventId = safetyEventId;
                pendingWatchdogSafetyReason = reason;
                pendingWatchdogSafetyActivatedUtc = DateTime.UtcNow;
                pendingWatchdogSafetyEventPublished = false;
                pendingWatchdogSafetySettlementSequence = 0;
                pendingWatchdogSafetyFinalProofAttempts = 0;
                pendingWatchdogSafetyFinalProofInFlight = false;
                watchdogSafetyFlattenSubmitted = flattenInProgress;
                pendingWatchdogSafetyExitRequired = flattenInProgress;
            }
            // Publish the correlation first. TCP ordering ensures a terminal
            // reconciliation emitted by dispatcher callbacks cannot overtake
            // its watchdog safety event at Python.
            PublishWatchdogSafetyEvent(safetyEventId, reason);
        }

        private bool PublishWatchdogSafetyEvent(string safetyEventId, string reason)
        {
            if (String.IsNullOrWhiteSpace(safetyEventId) || !authenticated) return false;
            try
            {
                Dictionary<string, object> incident = SessionMessage("SAFETY_EVENT");
                incident["receipt_id"] = safetyEventId;
                incident["safety_event_id"] = safetyEventId;
                incident["reason_code"] = reason;
                bool sent = SendSigned(incident);
                if (sent)
                {
                    lock (stateLock)
                        if (String.Equals(pendingWatchdogSafetyEventId, safetyEventId, StringComparison.Ordinal))
                            pendingWatchdogSafetyEventPublished = true;
                }
                return sent;
            }
            catch (Exception error)
            {
                // Keep the correlation pending. Later order/position events
                // can still publish the settlement proof if the socket heals.
                Diagnostic("SAFETY_EVENT_SEND_FAILED_" + error.GetType().Name);
                return false;
            }
        }

        private void RepublishPendingWatchdogSafetyCorrelation()
        {
            string safetyEventId;
            string reason;
            lock (stateLock)
            {
                safetyEventId = pendingWatchdogSafetyEventId;
                reason = pendingWatchdogSafetyReason;
            }
            if (!PublishWatchdogSafetyEvent(safetyEventId, reason)) return;
            TryPublishWatchdogSafetyReconciliation();
        }

        private void BeginNativeObservationCallback()
        {
            Interlocked.Increment(ref nativeObservationCallbacksInFlight);
            Interlocked.Increment(ref nativeObservationGeneration);
        }

        private void EndNativeObservationCallback()
        {
            Interlocked.Increment(ref nativeObservationGeneration);
            Interlocked.Decrement(ref nativeObservationCallbacksInFlight);
        }

        private void OnOrderUpdate(object sender, OrderEventArgs e)
        {
            if (e == null || e.Order == null || !ExactBoundAccount(e.Order.Account)) return;
            BeginNativeObservationCallback();
            Order order = e.Order;
            // Order is a mutable NinjaTrader core object and can already be
            // ahead of this callback. Drive transitions and receipts only from
            // the by-value event snapshot.
            OrderState eventState = e.OrderState;
            int eventFilled = e.Filled;
            int eventQuantity = e.Quantity;
            string eventOrderId = e.OrderId ?? String.Empty;
            bool exact = order.Instrument != null && String.Equals(order.Instrument.FullName, ExactInstrumentName, StringComparison.Ordinal);
            OwnedOrder owner = null;
            string safetyReason = null;
            lock (stateLock)
            {
                if (IsOwnedName(order.Name))
                {
                    OwnedOrder claimed = null;
                    bool preSubmitInitialization = exact
                        && eventState == OrderState.Initialized
                        && eventFilled == 0
                        && eventQuantity == MaximumQuantity;
                    if (!exact || !ownedByName.TryGetValue(order.Name, out claimed)
                        || claimed.Order == null
                        || !Object.ReferenceEquals(claimed.Order, order))
                    {
                        // Callback names are not ownership. Only the explicit
                        // account snapshot rehydration path may adopt an order
                        // that predates this AddOn instance, and a same-name
                        // different Order must never replace a pre-owned handle.
                        // NinjaTrader synchronously publishes INITIALIZED from
                        // inside CreateOrder, before CreateOrder returns the
                        // handle that this AddOn can bind. That exact, zero-fill,
                        // one-contract pre-submit state has no native execution
                        // authority yet. Ignore only that state; its first later
                        // transition must match the returned pre-owned handle or
                        // this remains a hard identity collision.
                        if (!preSubmitInitialization)
                        {
                            if (claimed != null) claimed.OutcomeUnknown = true;
                            foreignActivity = true;
                            lockedOut = true;
                            reconciled = false;
                            safetyReason = "FOREIGN_ORDER_IDENTITY_COLLISION";
                        }
                    }
                    else
                    {
                        owner = claimed;
                        owner.Terminal = !Working(eventState);
                    }
                }
                else if (UnresolvedNativeOrderState(eventState) || eventFilled != 0)
                {
                    foreignActivity = true;
                    lockedOut = true;
                    reconciled = false;
                    safetyReason = "FOREIGN_ORDER_ACTIVITY";
                }
                if (!exact && (UnresolvedNativeOrderState(eventState) || eventFilled != 0))
                {
                    foreignActivity = true;
                    lockedOut = true;
                    reconciled = false;
                    if (safetyReason == null) safetyReason = "FOREIGN_INSTRUMENT_ORDER_ACTIVITY";
                }
                if (owner != null && eventState == OrderState.Unknown)
                {
                    owner.OutcomeUnknown = true;
                    reconciled = false;
                    safetyReason = owner.Role == "ENTRY"
                        ? "OWNED_ENTRY_ORDER_OUTCOME_UNKNOWN"
                        : owner.Role == "PROTECTIVE"
                            ? "OWNED_PROTECTIVE_ORDER_OUTCOME_UNKNOWN"
                            : "OWNED_EXIT_ORDER_OUTCOME_UNKNOWN";
                }
                if (owner != null && owner.Role == "ENTRY")
                {
                    if (eventState == OrderState.Filled)
                    {
                        if (eventFilled == MaximumQuantity)
                            owner.EntryOrderFilledTerminalObserved = true;
                        else
                        {
                            owner.OutcomeUnknown = true;
                            safetyReason = "OWNED_ENTRY_FILLED_QUANTITY_INVALID";
                        }
                        TrySettleEntryLifecycleUnderLock(owner);
                    }
                    else if (eventState == OrderState.Cancelled || eventState == OrderState.Rejected)
                    {
                        if (eventFilled == 0)
                        {
                            owner.EntryNoFillTerminalObserved = true;
                            TrySettleEntryLifecycleUnderLock(owner);
                        }
                        else
                        {
                            owner.OutcomeUnknown = true;
                            safetyReason = "OWNED_ENTRY_TERMINAL_WITH_FILL";
                        }
                    }
                }
                if (owner != null && owner.Role == "PROTECTIVE")
                {
                    if (eventState == OrderState.Accepted || eventState == OrderState.Working)
                    {
                        protectiveOrder = order;
                        protectiveDeadlineUtc = DateTime.MaxValue;
                        List<OwnedOrder> pendingEntries = ownedByName.Values.Where(value =>
                            value.Role == "ENTRY" && !value.EntrySettlementComplete
                            && String.Equals(value.CommandId, owner.CommandId, StringComparison.Ordinal)
                        ).ToList();
                        if (pendingEntries.Count == 1)
                        {
                            pendingEntries[0].EntryExposureHandled = true;
                            TrySettleEntryLifecycleUnderLock(pendingEntries[0]);
                        }
                        else if (pendingEntries.Count > 1)
                        {
                            foreignActivity = true;
                            lockedOut = true;
                            reconciled = false;
                            safetyReason = "ENTRY_PROTECTION_OWNERSHIP_AMBIGUOUS";
                        }
                    }
                    bool pendingProtectiveIdentity = flattenInProgress
                        && Object.ReferenceEquals(pendingFlattenProtectiveOrder, order)
                        && String.Equals(pendingFlattenProtectiveName, order.Name, StringComparison.Ordinal)
                        && !String.IsNullOrWhiteSpace(pendingFlattenProtectiveOco)
                        && String.Equals(pendingFlattenProtectiveOco, order.Oco, StringComparison.Ordinal);
                    bool expectedOcoCancellation = eventState == OrderState.Cancelled
                        && eventFilled == 0
                        && pendingFlattenProtectiveCancellationExpected
                        && pendingProtectiveIdentity
                        && activeFlattenOwner != null
                        && activeFlattenOwner.Order != null
                        && String.Equals(pendingFlattenProtectiveOco, activeFlattenOwner.Order.Oco, StringComparison.Ordinal);
                    bool protectiveNoFillTerminal = eventFilled == 0
                        && (eventState == OrderState.Cancelled
                            || eventState == OrderState.Rejected)
                        && pendingFlattenProtectiveCancellationExpected
                        && pendingProtectiveIdentity
                        && activeFlattenOwner != null
                        && activeFlattenOwner.Order != null
                        && String.Equals(
                            pendingFlattenProtectiveOco,
                            activeFlattenOwner.Order.Oco,
                            StringComparison.Ordinal
                        );
                    if (protectiveNoFillTerminal)
                        // Terminal no-fill is not success by itself: the paired
                        // market EXIT may still reject. Retain the exact fence
                        // until the winner is independently proven. Rejected is
                        // still a protection incident below, but cannot strand
                        // an open position by suppressing one-stop recovery.
                        pendingFlattenProtectiveCancellationObserved = true;
                    if (protectiveNoFillTerminal && pendingFlattenExitNoFillTerminalObserved)
                        safetyReason = "FLATTEN_ORDER_NOT_FILLED";
                    if (eventState == OrderState.Filled && pendingProtectiveIdentity)
                    {
                        if (eventFilled == pendingFlattenExpectedQuantity)
                        {
                            pendingFlattenProtectiveFilledTerminalObserved = true;
                            if (pendingFlattenExitFilledTerminalObserved)
                            {
                                owner.OutcomeUnknown = true;
                                if (activeFlattenOwner != null)
                                    activeFlattenOwner.OutcomeUnknown = true;
                                safetyReason = "MULTIPLE_FLATTEN_LEGS_FILLED";
                            }
                            else
                            {
                                pendingFlattenCompletedByProtective = true;
                                pendingFlattenOrderTerminalObserved = true;
                            }
                        }
                        else
                        {
                            owner.OutcomeUnknown = true;
                            safetyReason = "PROTECTIVE_FLATTEN_FILLED_QUANTITY_INVALID";
                        }
                    }
                    if (!Working(eventState) && Object.ReferenceEquals(protectiveOrder, order))
                        protectiveOrder = null;
                    // Only the exact OCO cancellation paired to the active EXIT
                    // is expected. A rejection or unrelated cancellation is a
                    // genuine protection failure.
                    if (eventState == OrderState.Rejected
                        || (eventState == OrderState.Cancelled && !expectedOcoCancellation))
                        safetyReason = "PROTECTIVE_STOP_REJECTED";
                }
                if (owner != null && owner.Role == "EXIT"
                    && String.Equals(owner.CommandId, pendingFlattenCommandId, StringComparison.Ordinal)
                    && !Working(eventState))
                {
                    if (eventState == OrderState.Filled)
                    {
                        if (eventFilled == pendingFlattenExpectedQuantity)
                        {
                            pendingFlattenExitFilledTerminalObserved = true;
                            if (pendingFlattenProtectiveFilledTerminalObserved)
                            {
                                owner.OutcomeUnknown = true;
                                safetyReason = "MULTIPLE_FLATTEN_LEGS_FILLED";
                            }
                            else
                                pendingFlattenOrderTerminalObserved = true;
                        }
                        else
                        {
                            owner.OutcomeUnknown = true;
                            safetyReason = "FLATTEN_FILLED_QUANTITY_INVALID";
                        }
                    }
                    else if (eventState == OrderState.Rejected || eventState == OrderState.Cancelled)
                    {
                        if (eventFilled == 0 && pendingFlattenExitExecutionQuantity == 0)
                        {
                            pendingFlattenExitNoFillTerminalObserved = true;
                            // With an OCO protective still unresolved, this can
                            // be the normal callback-first shape of the stop
                            // winning. Classify failure only once that exact stop
                            // is also proven cancelled, or when no stop existed.
                            if (!pendingFlattenCompletedByProtective
                                && (!pendingFlattenProtectiveCancellationExpected
                                    || pendingFlattenProtectiveCancellationObserved))
                                safetyReason = "FLATTEN_ORDER_NOT_FILLED";
                        }
                        else
                        {
                            owner.OutcomeUnknown = true;
                            safetyReason = "FLATTEN_TERMINAL_FILL_AMBIGUOUS";
                        }
                    }
                }
            }
            if (safetyReason != null)
                LockAndProtect(safetyReason);
            TryRearmProtectionAfterDefinitiveFlattenNoFill();
            if (authenticated)
            {
                Dictionary<string, object> message = SessionMessage("ORDER_EVENT");
                message["receipt_id"] = "l3g-order-" + Guid.NewGuid().ToString("N");
                message["native_order_id"] = eventOrderId;
                message["order_name"] = order.Name ?? String.Empty;
                message["order_role"] = owner == null ? "FOREIGN" : owner.Role;
                message["order_state"] = eventState.ToString().ToUpperInvariant();
                message["quantity"] = eventQuantity;
                message["filled_quantity"] = eventFilled;
                message["command_id"] = owner == null ? null : owner.CommandId;
                // Preserve NinjaTrader's native rejection diagnostics inside the
                // authenticated receipt. Python must never replace a real venue or
                // connection refusal with a generic green/flat projection.
                message["native_error_code"] = e.Error.ToString().ToUpperInvariant();
                message["native_error_comment"] = e.Comment ?? String.Empty;
                SendSigned(message);
            }
            TryPublishWatchdogSafetyReconciliation();
            Position current = CurrentPosition();
            SettleFlattenOwnershipIfPossible(current != null && current.Quantity != 0);
            TryFinalizeRetainedTermination();
            EndNativeObservationCallback();
        }

        private void OnExecutionUpdate(object sender, ExecutionEventArgs e)
        {
            if (e == null || e.Execution == null || e.Execution.Order == null
                || !ExactBoundAccount(e.Execution.Order.Account)) return;
            BeginNativeObservationCallback();
            Order order = e.Execution.Order;
            // Execution is mutable for the same reason as Order. Preserve the
            // callback's immutable execution facts before touching shared state.
            string executionId = e.ExecutionId ?? String.Empty;
            string eventOrderId = e.OrderId ?? String.Empty;
            int eventQuantity = e.Quantity;
            double eventPrice = e.Price;
            bool exact = order.Instrument != null
                && String.Equals(order.Instrument.FullName, ExactInstrumentName, StringComparison.Ordinal);
            OwnedOrder owner = null;
            OwnedOrder claimed = null;
            lock (stateLock)
            {
                ownedByName.TryGetValue(order.Name ?? String.Empty, out claimed);
                if (exact && claimed != null && claimed.Order != null
                    && Object.ReferenceEquals(claimed.Order, order))
                    owner = claimed;
                else
                {
                    if (claimed != null) claimed.OutcomeUnknown = true;
                    foreignActivity = true;
                    lockedOut = true;
                    reconciled = false;
                }
            }
            if (owner == null)
            {
                lock (stateLock) { foreignActivity = true; lockedOut = true; }
                LockAndProtect("FOREIGN_EXECUTION_ACTIVITY");
                TryFinalizeRetainedTermination();
                EndNativeObservationCallback();
                return;
            }
            if (owner.Role == "ENTRY")
            {
                string commandId = owner.CommandId ?? String.Empty;
                string executionFact = String.Join("|", new[] {
                    commandId,
                    eventOrderId,
                    eventQuantity.ToString(CultureInfo.InvariantCulture),
                    eventPrice.ToString("R", CultureInfo.InvariantCulture),
                    order.OrderAction.ToString()
                });
                bool submitProtection = false;
                bool conflictingExecution = false;
                lock (stateLock)
                {
                    string priorFact;
                    string priorExecution;
                    if (String.IsNullOrWhiteSpace(executionId) || String.IsNullOrWhiteSpace(commandId)
                        || eventQuantity != MaximumQuantity || eventPrice <= 0
                        || (order.OrderAction != OrderAction.Buy
                            && order.OrderAction != OrderAction.SellShort))
                        conflictingExecution = true;
                    else if (failedEntryProtectionCommands.Contains(commandId))
                        conflictingExecution = true;
                    else if (protectedEntryExecutionFacts.TryGetValue(executionId, out priorFact))
                        conflictingExecution = !String.Equals(priorFact, executionFact, StringComparison.Ordinal);
                    else if (protectedEntryCommandExecutions.TryGetValue(commandId, out priorExecution))
                        conflictingExecution = !String.Equals(priorExecution, executionId, StringComparison.Ordinal);
                    else
                    {
                        protectedEntryExecutionFacts[executionId] = executionFact;
                        protectedEntryCommandExecutions[commandId] = executionId;
                        owner.EntryExecutionObserved = true;
                        owner.EntryExecutionQuantity = eventQuantity;
                        owner.EntryExecutionPrice = eventPrice;
                        owner.EntryProtectionPending = true;
                        protectiveDeadlineUtc = DateTime.UtcNow.AddSeconds(ProtectiveAcceptanceSeconds);
                        TryEstablishOwnedPositionUnderLock(owner);
                        TrySettleEntryLifecycleUnderLock(owner);
                        submitProtection = true;
                    }
                    // Once an execution for this command is unidentified or
                    // conflicts with its claimed fact, a later callback must
                    // never turn that incident into fresh submission authority.
                    if (conflictingExecution)
                    {
                        owner.OutcomeUnknown = true;
                        if (!String.IsNullOrWhiteSpace(commandId))
                            failedEntryProtectionCommands.Add(commandId);
                    }
                }
                if (conflictingExecution)
                    LockAndProtect("CONFLICTING_OR_UNIDENTIFIED_ENTRY_EXECUTION");
                else if (submitProtection)
                    TrySubmitPendingEntryProtection(owner);
            }
            else if (owner.Role == "EXIT")
            {
                bool invalidExecution = false;
                lock (stateLock)
                {
                    if (!String.Equals(owner.CommandId, pendingFlattenCommandId, StringComparison.Ordinal))
                    {
                        // Retained terminal EXIT callbacks remain owned audit
                        // facts, but they cannot settle a newer flatten.
                    }
                    else if (String.IsNullOrWhiteSpace(executionId) || eventQuantity <= 0
                        || pendingFlattenExitNoFillTerminalObserved)
                    {
                        invalidExecution = true;
                        owner.OutcomeUnknown = true;
                    }
                    else if (pendingFlattenExecutionIds.Add(executionId))
                    {
                        if (pendingFlattenFilledQuantity + eventQuantity > pendingFlattenExpectedQuantity)
                        {
                            invalidExecution = true;
                            owner.OutcomeUnknown = true;
                        }
                        else
                        {
                            pendingFlattenFilledQuantity += eventQuantity;
                            pendingFlattenExitExecutionQuantity += eventQuantity;
                        }
                        if (pendingFlattenFilledQuantity == pendingFlattenExpectedQuantity)
                            pendingFlattenExecutionObserved = true;
                    }
                }
                if (invalidExecution)
                    LockAndProtect("UNIDENTIFIED_FLATTEN_EXECUTION");
            }
            else if (owner.Role == "PROTECTIVE")
            {
                bool invalidProtectiveFlattenExecution = false;
                lock (stateLock)
                {
                    bool pendingProtectiveIdentity = flattenInProgress
                        && Object.ReferenceEquals(pendingFlattenProtectiveOrder, order)
                        && String.Equals(pendingFlattenProtectiveName, order.Name, StringComparison.Ordinal)
                        && !String.IsNullOrWhiteSpace(pendingFlattenProtectiveOco)
                        && String.Equals(pendingFlattenProtectiveOco, order.Oco, StringComparison.Ordinal);
                    if (pendingProtectiveIdentity)
                    {
                        if (String.IsNullOrWhiteSpace(executionId) || eventQuantity <= 0
                            || (pendingFlattenProtectiveCancellationExpected
                                && pendingFlattenProtectiveCancellationObserved))
                        {
                            invalidProtectiveFlattenExecution = true;
                            owner.OutcomeUnknown = true;
                        }
                        else if (pendingFlattenExecutionIds.Add(executionId))
                        {
                            if (pendingFlattenFilledQuantity + eventQuantity > pendingFlattenExpectedQuantity)
                            {
                                invalidProtectiveFlattenExecution = true;
                                owner.OutcomeUnknown = true;
                            }
                            else
                            {
                                pendingFlattenFilledQuantity += eventQuantity;
                                pendingFlattenProtectiveExecutionQuantity += eventQuantity;
                            }
                            if (pendingFlattenFilledQuantity == pendingFlattenExpectedQuantity)
                                pendingFlattenExecutionObserved = true;
                        }
                    }
                }
                if (invalidProtectiveFlattenExecution)
                    LockAndProtect("UNIDENTIFIED_PROTECTIVE_FLATTEN_EXECUTION");
            }
            if (authenticated)
            {
                Dictionary<string, object> message = SessionMessage("EXECUTION_EVENT");
                message["receipt_id"] = "l3g-execution-" + Guid.NewGuid().ToString("N");
                message["native_execution_id"] = executionId;
                message["native_order_id"] = eventOrderId;
                message["order_role"] = owner.Role;
                message["command_id"] = owner.CommandId;
                message["intent_id"] = owner.IntentId;
                message["decision_id"] = owner.DecisionId;
                message["price"] = eventPrice;
                message["quantity"] = eventQuantity;
                message["direction"] = order.OrderAction == OrderAction.Buy || order.OrderAction == OrderAction.BuyToCover ? "LONG" : "SHORT";
                message["strategy_daily_realized_pnl"] = 0;
                SendSigned(message);
            }
            TryPublishWatchdogSafetyReconciliation();
            Position postExecution = CurrentPosition();
            SettleFlattenOwnershipIfPossible(postExecution != null && postExecution.Quantity != 0);
            TryRearmProtectionAfterDefinitiveFlattenNoFill();
            TryFinalizeRetainedTermination();
            EndNativeObservationCallback();
        }

        private void OnPositionUpdate(object sender, PositionEventArgs e)
        {
            if (e == null || e.Position == null || !ExactBoundAccount(e.Position.Account)) return;
            BeginNativeObservationCallback();
            Position position = e.Position;
            bool exact = position.Instrument != null && String.Equals(position.Instrument.FullName, ExactInstrumentName, StringComparison.Ordinal);
            int eventQuantity = e.Quantity;
            MarketPosition eventMarketPosition = e.MarketPosition;
            double eventAveragePrice = e.AveragePrice;
            if (!exact && eventQuantity != 0)
            {
                lock (stateLock) { foreignActivity = true; lockedOut = true; }
                LockAndProtect("FOREIGN_POSITION_ACTIVITY");
                EndNativeObservationCallback();
                return;
            }
            if (!exact)
            {
                EndNativeObservationCallback();
                return;
            }
            int quantity = eventMarketPosition == MarketPosition.Short ? -eventQuantity
                : eventMarketPosition == MarketPosition.Flat ? 0 : eventQuantity;
            string entryOwnershipIncident = null;
            OwnedOrder entryPendingProtection = null;
            bool flatWithOwnedReducingOrder = false;
            bool flatExpectedDuringFlatten = false;
            bool terminating;
            lock (stateLock) terminating = stopping;
            lock (stateLock)
            {
                if (quantity == 0)
                {
                    // Position ownership is scoped to one observed exposure
                    // cycle. A later manual position cannot inherit authority
                    // from an old completed ENTRY retained for audit.
                    ownedPositionEstablished = false;
                    ownedPositionSignedQuantity = 0;
                    // Use callback-owned terminal facts, not the mutable native
                    // OrderState, so a PositionUpdate cannot miss a protective
                    // or EXIT pre-owned immediately before Submit. Expected
                    // flatten/OCO races cancel without poisoning a normal
                    // reversal; every other flat-with-reducing-order shape is
                    // a lockout because that order could reopen exposure.
                    flatWithOwnedReducingOrder = ownedByName.Values.Any(owner =>
                        (owner.Role == "PROTECTIVE" || owner.Role == "EXIT")
                        && owner.Order != null && !owner.Terminal);
                    flatExpectedDuringFlatten = flattenInProgress;
                    positionOwnershipGeneration++;
                    foreach (OwnedOrder entry in ownedByName.Values.Where(owner =>
                        owner.Role == "ENTRY" && !owner.EntrySettlementComplete
                        && owner.EntryPositionObserved))
                        entry.EntryPositionOwnershipInvalidated = true;
                }
                else
                {
                    List<OwnedOrder> pendingEntries = ownedByName.Values.Where(owner =>
                        owner.Role == "ENTRY" && !owner.EntrySettlementComplete).ToList();
                    if (pendingEntries.Count == 1)
                    {
                        OwnedOrder entry = pendingEntries[0];
                        if (entry.EntryPositionOwnershipInvalidated
                            || (entry.EntryPositionObserved
                                && entry.EntryPositionGeneration != positionOwnershipGeneration)
                            || !EntryDirectionMatchesPosition(entry, quantity))
                        {
                            entry.OutcomeUnknown = true;
                            foreignActivity = true;
                            lockedOut = true;
                            reconciled = false;
                            entryOwnershipIncident = "ENTRY_POSITION_DIRECTION_OR_QUANTITY_MISMATCH";
                        }
                        else
                        {
                            entry.EntryPositionObserved = true;
                            entry.EntryPositionSignedQuantity = quantity;
                            entry.EntryPositionGeneration = positionOwnershipGeneration;
                            TryEstablishOwnedPositionUnderLock(entry);
                            TrySettleEntryLifecycleUnderLock(entry);
                            entryPendingProtection = entry;
                        }
                    }
                    else if (pendingEntries.Count > 1)
                    {
                        foreignActivity = true;
                        lockedOut = true;
                        reconciled = false;
                        entryOwnershipIncident = "ENTRY_POSITION_OWNERSHIP_AMBIGUOUS";
                    }
                    else if (!ownedPositionEstablished)
                    {
                        foreignActivity = true;
                        lockedOut = true;
                        reconciled = false;
                        entryOwnershipIncident = "POSITION_OWNERSHIP_UNPROVEN";
                    }
                    else if (ownedPositionSignedQuantity != quantity)
                    {
                        foreignActivity = true;
                        lockedOut = true;
                        reconciled = false;
                        entryOwnershipIncident = Math.Sign(ownedPositionSignedQuantity) == Math.Sign(quantity)
                            ? "POSITION_OWNERSHIP_QUANTITY_CHANGED"
                            : "POSITION_OWNERSHIP_DIRECTION_CHANGED";
                    }
                }
            }
            if (entryOwnershipIncident != null)
                LockAndProtect(entryOwnershipIncident);
            else if (entryPendingProtection != null)
                TrySubmitPendingEntryProtection(entryPendingProtection);
            if (flatWithOwnedReducingOrder)
            {
                if (flatExpectedDuringFlatten)
                    DispatchOwnedOrderCancellationAfterExpectedFlat();
                else
                    LockAndProtect("FLAT_WITH_OWNED_REDUCING_ORDER");
            }
            if (Math.Abs(quantity) > MaximumQuantity)
                LockAndProtect("MAXIMUM_QUANTITY_BREACH");
            else if (terminating && quantity != 0)
                LockAndProtect("ADDON_TERMINATING_LATE_ENTRY_FILL");
            if (authenticated)
            {
                Dictionary<string, object> message = SessionMessage("POSITION_EVENT");
                message["receipt_id"] = "l3g-position-" + Guid.NewGuid().ToString("N");
                message["quantity"] = quantity;
                message["average_price"] = eventAveragePrice;
                message["timestamp"] = UtcNow();
                SendSigned(message);
            }
            TryPublishWatchdogSafetyReconciliation();
            SettleFlattenOwnershipIfPossible(quantity != 0);
            TryRearmProtectionAfterDefinitiveFlattenNoFill();
            TryFinalizeRetainedTermination();
            EndNativeObservationCallback();
        }

        private void OnAccountItemUpdate(object sender, AccountItemEventArgs e)
        {
            // Account-reported values are intentionally not substituted for
            // strategy-owned execution P&L. They are reconciliation-only.
        }

        private void WatchdogLoop()
        {
            while (!stopping)
            {
                Thread.Sleep(250);
                // Do not rely on a post-reload OrderUpdate to discover an
                // already-working BZ-L3G order before deciding whether missed
                // heartbeats require the independent cancel/flatten action.
                RehydrateOwnedWorkingOrders();
                EnsureCurrentPositionOwnershipProven();
                bool heartbeatLost;
                bool protectionFailed;
                bool flattenFailed;
                bool quantityBreach;
                Position position = CurrentPosition();
                bool positionOpen = position != null && position.Quantity != 0;
                bool ownedActivity = HasOwnedActivity();
                lock (stateLock)
                {
                    heartbeatLost = watchdogSafetyAuthorityEstablished && ownedActivity
                        && DateTime.UtcNow - lastHeartbeatUtc > TimeSpan.FromSeconds(5);
                    protectionFailed = protectiveDeadlineUtc != DateTime.MaxValue && DateTime.UtcNow > protectiveDeadlineUtc;
                    flattenFailed = flattenInProgress && flattenDeadlineUtc != DateTime.MaxValue
                        && DateTime.UtcNow > flattenDeadlineUtc
                        && (positionOpen || !pendingFlattenExecutionObserved || !pendingFlattenOrderTerminalObserved);
                    quantityBreach = watchdogSafetyAuthorityEstablished && position != null
                        && position.Quantity > MaximumQuantity;
                }
                if (heartbeatLost) LockAndProtect("HEARTBEAT_WATCHDOG");
                if (protectionFailed) LockAndProtect("PROTECTIVE_STOP_ACCEPTANCE_TIMEOUT");
                if (flattenFailed) LockAndProtect("FLATTEN_ACCEPTANCE_TIMEOUT");
                if (quantityBreach) LockAndProtect("MAXIMUM_QUANTITY_BREACH");
                TryRearmProtectionAfterDefinitiveFlattenNoFill();
                TryPublishWatchdogSafetyReconciliation();
                Position settledPosition = CurrentPosition();
                SettleFlattenOwnershipIfPossible(settledPosition != null && settledPosition.Quantity != 0);
            }
        }

        private void LockAndProtect(string reason)
        {
            bool beginCorrelation;
            bool dispatchSafetyAction;
            lock (stateLock)
            {
                lockedOut = true;
                reconciled = false;
                protectiveDeadlineUtc = DateTime.MaxValue;
                // Generic lockout can already be true because foreign activity
                // was observed. It is not evidence that this AddOn has begun
                // its independently owned cancel/flatten response.
                beginCorrelation = !watchdogSafetyDispatchStarted;
                if (beginCorrelation)
                    watchdogSafetyDispatchStarted = true;
                DateTime now = DateTime.UtcNow;
                dispatchSafetyAction = !watchdogSafetyActionInFlight
                    && now - watchdogSafetyLastActionUtc >= TimeSpan.FromSeconds(1);
                if (dispatchSafetyAction)
                {
                    watchdogSafetyActionInFlight = true;
                    watchdogSafetyLastActionUtc = now;
                }
            }
            if (beginCorrelation)
                BeginWatchdogSafetyCorrelation(reason);
            if (!dispatchSafetyAction) return;
            try
            {
                NinjaTrader.Core.Globals.RandomDispatcher.BeginInvoke(new Action(delegate
                {
                    try
                    {
                        lock (nativeMutationGate)
                        {
                            bool retainedTermination;
                            lock (stateLock)
                                retainedTermination = stopping && terminationCallbacksRetained;
                            if (stopping && !retainedTermination) return;
                            bool foreign;
                            bool ambiguousOwned;
                            bool alreadySubmitted;
                            string commandId;
                            string decisionId;
                            lock (stateLock)
                            {
                                foreign = foreignActivity;
                                ambiguousOwned = HasAmbiguousOwnedOutcomeUnderLock();
                                alreadySubmitted = watchdogSafetyFlattenSubmitted || flattenInProgress;
                                commandId = pendingWatchdogSafetyEventId;
                                decisionId = pendingWatchdogSafetyReason;
                            }
                            Position position = CurrentPosition();
                            // Foreign activity may include an exact-instrument
                            // order/position. Never submit an exit while its
                            // ownership is ambiguous; keep the correlated shutdown
                            // proof unresolved. The uniquely named EXIT is claimed
                            // before SubmitOwnedFlattenOrder touches NinjaTrader.
                            if (ambiguousOwned)
                            {
                                // A native Unknown order may still fill. Only
                                // cancellation of that pre-owned handle is safe;
                                // any new EXIT could double-flatten or reverse.
                                CancelOwnedOrders();
                            }
                            else if (foreign)
                            {
                                // Never flatten an ambiguous aggregate, but no
                                // foreign fact can revoke cancellation authority
                                // over our exact named working orders.
                                CancelOwnedOrders();
                            }
                            else if (position != null && position.Quantity != 0
                                && !alreadySubmitted)
                            {
                                string refusal = SubmitOwnedFlattenOrder(
                                    commandId, "NATIVE_SAFETY", decisionId, true
                                );
                                if (refusal != null)
                                    Diagnostic("SAFETY_FLATTEN_REFUSED_" + refusal);
                            }
                            else if (position == null || position.Quantity == 0)
                                // A still-working EXIT can reverse a flat account.
                                // With no position, every owned role is stale.
                                CancelOwnedOrders();
                            // Nonforeign + open + already submitted retains the
                            // exact EXIT and its OCO stop until settlement.
                            TryPublishWatchdogSafetyReconciliation();
                            TryFinalizeRetainedTermination();
                        }
                    }
                    catch (Exception error) { Diagnostic("SAFETY_ACTION_FAILED_" + error.GetType().Name); }
                    finally
                    {
                        lock (stateLock) watchdogSafetyActionInFlight = false;
                    }
                }));
            }
            catch (Exception error)
            {
                lock (stateLock) watchdogSafetyActionInFlight = false;
                Diagnostic("SAFETY_DISPATCH_FAILED_" + error.GetType().Name);
            }
        }

        private void Acknowledge(Dictionary<string, object> command, string reason, bool duplicate)
        {
            Dictionary<string, object> message = SessionMessage("COMMAND_ACK");
            message["receipt_id"] = "l3g-ack-" + Guid.NewGuid().ToString("N");
            message["command_id"] = Text(command, "command_id");
            message["reason_code"] = reason;
            message["duplicate"] = duplicate;
            SendSigned(message);
        }

        private void Reject(Dictionary<string, object> command, string reason, string commandId)
        {
            if (!authenticated || stream == null)
            {
                Diagnostic("PROTOCOL_REJECTED_" + reason);
                return;
            }
            Dictionary<string, object> message = SessionMessage("COMMAND_REJECTED");
            message["receipt_id"] = "l3g-reject-" + Guid.NewGuid().ToString("N");
            message["command_id"] = commandId;
            message["reason_code"] = reason;
            SendSigned(message);
        }

        private Dictionary<string, object> SessionMessage(string type)
        {
            Dictionary<string, object> message = new Dictionary<string, object>();
            string kind;
            string sessionId;
            string tradeDate;
            string profileHash;
            long generation;
            lock (stateLock)
            {
                kind = paperSessionKind;
                sessionId = paperSessionId;
                tradeDate = paperTradeDate;
                profileHash = paperSessionProfileHash;
                generation = paperSessionGeneration;
            }
            if (String.IsNullOrWhiteSpace(kind) || String.IsNullOrWhiteSpace(sessionId)
                || String.IsNullOrWhiteSpace(tradeDate) || String.IsNullOrWhiteSpace(profileHash))
            {
                DateTime local = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, NewYorkTimezone());
                kind = "OFF_SESSION";
                tradeDate = IsoDate(local);
                sessionId = "MNQU6:OFF_SESSION:" + tradeDate;
                profileHash = OffSessionProfileHash;
                generation = 0;
            }
            message["schema"] = WireSchema;
            message["message_type"] = type;
            message["execution_session_id"] = executionSessionId;
            message["session_kind"] = kind;
            message["session_family"] = kind == "NEW_YORK_RTH" || kind == "NY_AFTER"
                ? "NEW_YORK" : kind == "LONDON" ? "EUROPE" : kind == "ASIA" ? "ASIA" : "OFF_SESSION";
            message["session_id"] = sessionId;
            message["trade_date"] = tradeDate;
            message["session_profile_hash"] = profileHash;
            message["session_generation"] = generation;
            message["timestamp"] = UtcNow();
            return message;
        }

        private bool SendSigned(Dictionary<string, object> message)
        {
            if (message == null) return false;
            lock (sendLock)
            {
                if (signingKey == null) return false;
                message.Remove("signature");
                message["signature"] = Sign(message);
                byte[] bytes = Encoding.UTF8.GetBytes(Canonical(message) + "\n");
                bool sent = false;
                try
                {
                    NetworkStream current = stream;
                    if (current != null)
                    {
                        current.Write(bytes, 0, bytes.Length);
                        sent = true;
                    }
                }
                catch (IOException) { CloseTransport(); }
                catch (ObjectDisposedException) { CloseTransport(); }
                return sent;
            }
        }

        private bool Verify(Dictionary<string, object> message)
        {
            string supplied = Text(message, "signature");
            if (String.IsNullOrWhiteSpace(supplied)) return false;
            string expected;
            lock (sendLock)
            {
                if (signingKey == null) return false;
                expected = Sign(message);
            }
            if (supplied.Length != expected.Length) return false;
            int difference = 0;
            for (int index = 0; index < supplied.Length; index++) difference |= supplied[index] ^ expected[index];
            return difference == 0;
        }

        private string Sign(Dictionary<string, object> message)
        {
            Dictionary<string, object> unsigned = new Dictionary<string, object>(message, StringComparer.Ordinal);
            unsigned.Remove("signature");
            using (HMACSHA256 hmac = new HMACSHA256(signingKey))
            {
                byte[] digest = hmac.ComputeHash(Encoding.UTF8.GetBytes(Canonical(unsigned)));
                return BitConverter.ToString(digest).Replace("-", String.Empty).ToLowerInvariant();
            }
        }

        private static string Canonical(object value)
        {
            IDictionary<string, object> dictionary = value as IDictionary<string, object>;
            if (dictionary != null)
            {
                return "{" + String.Join(",", dictionary.OrderBy(item => item.Key, StringComparer.Ordinal).Select(item => Quote(item.Key) + ":" + Canonical(item.Value))) + "}";
            }
            object[] array = value as object[];
            if (array != null)
                return "[" + String.Join(",", array.Select(Canonical)) + "]";
            System.Collections.IEnumerable enumerable = value as System.Collections.IEnumerable;
            if (enumerable != null && !(value is string))
            {
                List<string> items = new List<string>();
                foreach (object item in enumerable) items.Add(Canonical(item));
                return "[" + String.Join(",", items) + "]";
            }
            if (value == null) return "null";
            if (value is string) return Quote((string)value);
            if (value is bool) return (bool)value ? "true" : "false";
            if (value is double) return ((double)value).ToString("R", CultureInfo.InvariantCulture);
            if (value is float) return ((float)value).ToString("R", CultureInfo.InvariantCulture);
            if (value is decimal) return ((decimal)value).ToString(CultureInfo.InvariantCulture);
            if (value is IFormattable) return ((IFormattable)value).ToString(null, CultureInfo.InvariantCulture);
            throw new InvalidOperationException("Unsupported protocol value.");
        }

        private static string Quote(string value)
        {
            return new JavaScriptSerializer().Serialize(value);
        }

        private static string Text(Dictionary<string, object> value, string name)
        {
            object item;
            return value != null && value.TryGetValue(name, out item) && item is string ? (string)item : null;
        }

        private static long Integer64(Dictionary<string, object> value, string name, long fallback)
        {
            object item;
            if (value == null || !value.TryGetValue(name, out item)) return fallback;
            try { return Convert.ToInt64(item, CultureInfo.InvariantCulture); }
            catch (Exception) { return fallback; }
        }

        private static int Integer32(Dictionary<string, object> value, string name, int fallback)
        {
            long item = Integer64(value, name, fallback);
            return item < Int32.MinValue || item > Int32.MaxValue ? fallback : (int)item;
        }

        private static bool? Boolean(Dictionary<string, object> value, string name)
        {
            object item;
            return value != null && value.TryGetValue(name, out item) && item is bool ? (bool?)item : null;
        }

        private static bool HasDuplicateTopLevelKeys(string text)
        {
            HashSet<string> keys = new HashSet<string>(StringComparer.Ordinal);
            bool quoted = false;
            bool escaped = false;
            int depth = 0;
            int start = -1;
            for (int index = 0; index < text.Length; index++)
            {
                char character = text[index];
                if (quoted)
                {
                    if (escaped) { escaped = false; continue; }
                    if (character == '\\') { escaped = true; continue; }
                    if (character == '"')
                    {
                        quoted = false;
                        if (depth == 1 && start >= 0)
                        {
                            int next = index + 1;
                            while (next < text.Length && Char.IsWhiteSpace(text[next])) next++;
                            if (next < text.Length && text[next] == ':')
                            {
                                string encoded = text.Substring(start, index - start);
                                string key = new JavaScriptSerializer().Deserialize<string>("\"" + encoded + "\"");
                                if (!keys.Add(key)) return true;
                            }
                        }
                    }
                    continue;
                }
                if (character == '"') { quoted = true; start = index + 1; }
                else if (character == '{' || character == '[') depth++;
                else if (character == '}' || character == ']') depth--;
            }
            return false;
        }

        private static bool ValidTime(string value, int ageSeconds, out DateTime timestamp)
        {
            if (!DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out timestamp)) return false;
            timestamp = timestamp.ToUniversalTime();
            DateTime now = DateTime.UtcNow;
            return timestamp <= now.AddSeconds(1) && now - timestamp <= TimeSpan.FromSeconds(ageSeconds);
        }

        private static bool IsOwnedName(string value)
        {
            return !String.IsNullOrEmpty(value) && (value.StartsWith("BZ-L3G-E-", StringComparison.Ordinal) || value.StartsWith("BZ-L3G-S-", StringComparison.Ordinal) || value.StartsWith("BZ-L3G-X-", StringComparison.Ordinal));
        }

        private static string Fragment(string commandId)
        {
            if (String.IsNullOrEmpty(commandId)) return "INVALID";
            string value = commandId.Replace("l3g-pc-", String.Empty);
            return value.Substring(0, Math.Min(16, value.Length));
        }

        private static string UtcNow()
        {
            return DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture);
        }

        private static string AssemblyHash()
        {
            try
            {
                using (SHA256 sha = SHA256.Create())
                    return BitConverter.ToString(sha.ComputeHash(File.ReadAllBytes(Assembly.GetExecutingAssembly().Location))).Replace("-", String.Empty).ToLowerInvariant();
            }
            catch { return new string('0', 64); }
        }

        private static string AssemblyBuildTimestamp()
        {
            try { return File.GetLastWriteTimeUtc(Assembly.GetExecutingAssembly().Location).ToString("o", CultureInfo.InvariantCulture); }
            catch { return "UNKNOWN"; }
        }

        private void CloseTransport()
        {
            try { if (stream != null) stream.Close(); }
            catch (ObjectDisposedException) { }
            try { if (client != null) client.Close(); }
            catch (ObjectDisposedException) { }
            stream = null;
            client = null;
        }

        private static void Diagnostic(string marker)
        {
            System.Diagnostics.Trace.WriteLine("L3G PAPER " + marker);
        }

        private sealed class ProbeNativeSample
        {
            public int PositionQuantity;
            public int WorkingOrderCount;
            public int WorkingEntryCount;
            public bool PositionSnapshotComplete;
            public bool OrderSnapshotComplete;
            public bool ForeignActivity;
            public string ProtectiveStopState;
            public bool NativeMutationPending;
            public string Hash;
        }

        private sealed class OwnedOrder
        {
            public string CommandId;
            public string IntentId;
            public string DecisionId;
            public string Name;
            public string Role;
            public Order Order;
            public DateTime CreatedUtc;
            public bool Terminal;
            public bool OutcomeUnknown;
            public bool EntryNoFillTerminalObserved;
            public bool EntryOrderFilledTerminalObserved;
            public bool EntryExecutionObserved;
            public int EntryExecutionQuantity;
            public double EntryExecutionPrice;
            public bool EntryProtectionPending;
            public bool EntryPositionObserved;
            public int EntryPositionSignedQuantity;
            public long EntryPositionGeneration;
            public bool EntryPositionOwnershipInvalidated;
            public bool EntryExposureHandled;
            public bool EntrySettlementComplete;

            public OwnedOrder(string commandId, string intentId, string decisionId, string name, string role, Order order, DateTime createdUtc)
            {
                CommandId = commandId;
                IntentId = intentId;
                DecisionId = decisionId;
                Name = name;
                Role = role;
                Order = order;
                CreatedUtc = createdUtc;
            }

            public static OwnedOrder FromCommand(Dictionary<string, object> command, string name, string role, Order order)
            {
                return new OwnedOrder(Text(command, "command_id"), Text(command, "intent_id"), Text(command, "decision_id"), name, role, order, DateTime.UtcNow);
            }

            public static OwnedOrder Restored(Order order)
            {
                string role = order.Name.StartsWith("BZ-L3G-E-", StringComparison.Ordinal) ? "ENTRY" : order.Name.StartsWith("BZ-L3G-S-", StringComparison.Ordinal) ? "PROTECTIVE" : "EXIT";
                return new OwnedOrder("RESTORED", "RESTORED", "RESTORED", order.Name, role, order, DateTime.UtcNow);
            }
        }

        private sealed class CommandOutcome
        {
            public readonly string Fingerprint;
            public readonly string Status;
            public readonly string Reason;

            public CommandOutcome(string fingerprint, string status, string reason)
            {
                Fingerprint = fingerprint;
                Status = status;
                Reason = reason;
            }
        }

        private sealed class CommandDispatchTicket
        {
            private readonly object sync = new object();
            private bool started;
            private bool abandoned;
            private bool complete;
            public readonly ManualResetEvent Completed = new ManualResetEvent(false);
            public Exception Failure;

            public bool TryStart()
            {
                lock (sync)
                {
                    if (abandoned || complete) return false;
                    started = true;
                    return true;
                }
            }

            public bool TryAbandon()
            {
                lock (sync)
                {
                    if (started || complete) return false;
                    abandoned = true;
                    complete = true;
                    Completed.Set();
                    return true;
                }
            }

            public void Complete()
            {
                lock (sync)
                {
                    if (complete) return;
                    complete = true;
                    Completed.Set();
                }
            }
        }
    }
}
