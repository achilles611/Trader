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
        private const string AddonSourceFingerprint = "eee706f322b4f44ab82937bd231cc81ccaa484035c507d5c743a3249d1722879";
        private const string ExactAccountName = "Sim101";
        private const string ExactAccountClass = "LOCAL_SIMULATION";
        private const string ExactInstrumentName = "MNQ SEP26";
        private const string ExactCapability = "PAPER_ONLY";
        private const int Port = 48136;
        private const int MaximumFrameBytes = 65536;
        private const int MaximumQuantity = 1;
        private const double ExactTickSize = 0.25;
        private const double ProtectiveStopDistance = 25.0;
        private const int ProtectiveAcceptanceSeconds = 3;
        private const string PaperTimezone = "America/New_York";
        private const string AsiaProfileHash = "55225b35ccdb289d179bb23afd7f3fdb2c5ab193d53aba21603f17ff9f6d43aa";
        private const string NewYorkProfileHash = "8b8560a08ff41963a7a78d09bc977fbc1faf10f4a11ce58d05f47cacd89e0814";
        private const string NyAfterProfileHash = "e0cea9aa679c24ad4491ad929bcd72832cd3dcb49e2e5a7a64226c8abb5a1db2";
        private const string OffSessionProfileHash = "168f289a5847781ccb7a09f2556c4b3aa03e6f767071dc061dc5e3211d3834eb";

        private readonly object stateLock = new object();
        private readonly object sendLock = new object();
        private readonly object queueLock = new object();
        private readonly Queue<Dictionary<string, object>> commandQueue = new Queue<Dictionary<string, object>>();
        private readonly Dictionary<string, OwnedOrder> ownedByName = new Dictionary<string, OwnedOrder>(StringComparer.Ordinal);
        private readonly HashSet<string> processedCommands = new HashSet<string>(StringComparer.Ordinal);
        private Account paperAccount;
        private Instrument paperInstrument;
        private TcpClient client;
        private NetworkStream stream;
        private Thread connectionThread;
        private Thread commandThread;
        private Thread watchdogThread;
        private byte[] signingKey;
        private bool stopping;
        private bool lockedOut;
        private bool authenticated;
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
                stopping = false;
                lockedOut = false;
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
            lock (stateLock)
            {
                stopping = true;
                authenticated = false;
                reconciled = false;
                Monitor.PulseAll(queueLock);
                CloseTransport();
                if (paperAccount != null)
                {
                    paperAccount.OrderUpdate -= OnOrderUpdate;
                    paperAccount.ExecutionUpdate -= OnExecutionUpdate;
                    paperAccount.PositionUpdate -= OnPositionUpdate;
                    paperAccount.AccountItemUpdate -= OnAccountItemUpdate;
                }
            }
            Join(connectionThread);
            Join(commandThread);
            Join(watchdogThread);
            if (signingKey != null)
                Array.Clear(signingKey, 0, signingKey.Length);
            signingKey = null;
            Diagnostic("PAPER_BOUNDARY_TERMINATED");
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
            lock (stateLock)
            {
                executionSessionId = session;
                paperPolicyHash = policy;
                riskProfileHash = risk;
                accountBindingHash = binding;
                authenticated = true;
                reconciled = false;
                lastCommandSequence = 0;
                lastHeartbeatUtc = DateTime.UtcNow;
            }
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

        private void EnqueueCommand(Dictionary<string, object> command)
        {
            string commandId = Text(command, "command_id");
            lock (stateLock)
            {
                if (processedCommands.Contains(commandId))
                {
                    Acknowledge(command, "DUPLICATE_IDEMPOTENT", true);
                    return;
                }
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
                ManualResetEvent completed = new ManualResetEvent(false);
                Exception failure = null;
                NinjaTrader.Core.Globals.RandomDispatcher.BeginInvoke(new Action(delegate
                {
                    try { ExecuteCommand(command); }
                    catch (Exception error) { failure = error; }
                    finally { completed.Set(); }
                }));
                if (!completed.WaitOne(TimeSpan.FromSeconds(5)))
                {
                    LockAndProtect("COMMAND_DISPATCH_TIMEOUT");
                    Reject(command, "COMMAND_DISPATCH_TIMEOUT", Text(command, "command_id"));
                }
                else if (failure != null)
                {
                    LockAndProtect("COMMAND_DISPATCH_FAILURE");
                    Reject(command, "COMMAND_DISPATCH_FAILURE_" + failure.GetType().Name, Text(command, "command_id"));
                }
                completed.Dispose();
            }
        }

        private void ExecuteCommand(Dictionary<string, object> command)
        {
            string commandId;
            string action;
            long sequence;
            string refusal = ValidateCommand(command, out commandId, out action, out sequence);
            if (refusal != null)
            {
                Reject(command, refusal, commandId);
                return;
            }
            lock (stateLock)
            {
                processedCommands.Add(commandId);
                lastCommandSequence = sequence;
                paperSessionKind = Text(command, "session_kind");
                paperSessionId = Text(command, "session_id");
                paperTradeDate = Text(command, "trade_date");
                paperSessionProfileHash = Text(command, "session_profile_hash");
                paperSessionGeneration = Integer64(command, "session_generation", 0);
            }
            if (action == "ENTER_LONG" || action == "ENTER_SHORT")
                SubmitEntry(command, action == "ENTER_LONG");
            else if (action == "EXIT")
                FlattenOwnedInstrument(command, false);
            else if (action == "EMERGENCY_FLATTEN")
                FlattenOwnedInstrument(command, true);
            else if (action == "CANCEL_OWNED_ORDERS")
                CancelOwnedOrders();
            else if (action == "RECONCILE")
                SendReconciliation();
            else if (action != "HEARTBEAT")
            {
                Reject(command, "UNSUPPORTED_ACTION", commandId);
                return;
            }
            Acknowledge(command, "ACCEPTED", false);
        }

        private string ValidateCommand(Dictionary<string, object> command, out string commandId, out string action, out long sequence)
        {
            commandId = Text(command, "command_id");
            action = Text(command, "action");
            sequence = Integer64(command, "command_sequence", -1);
            DateTime timestamp;
            DateTime expiry;
            lock (stateLock)
            {
                if (!authenticated) return "NOT_AUTHENTICATED";
                if (lockedOut && action.StartsWith("ENTER_", StringComparison.Ordinal)) return "LOCKED_OUT";
                if (!String.Equals(Text(command, "execution_session_id"), executionSessionId, StringComparison.Ordinal)) return "WRONG_EXECUTION_SESSION";
                if (String.IsNullOrWhiteSpace(commandId)) return "MISSING_COMMAND_ID";
                if (processedCommands.Contains(commandId)) return "DUPLICATE_COMMAND";
                if (sequence != lastCommandSequence + 1) return "REORDERED_COMMAND";
                if (!reconciled && action != "RECONCILE" && action != "HEARTBEAT") return "RECONCILIATION_REQUIRED";
            }
            if (!ValidTime(Text(command, "created_at"), 5, out timestamp)) return "STALE_OR_FUTURE_COMMAND";
            if (!DateTime.TryParse(Text(command, "expires_at"), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out expiry) || expiry.ToUniversalTime() < DateTime.UtcNow) return "COMMAND_EXPIRED";
            if (!ExactHashes(command)) return "AUTHORITY_HASH_MISMATCH";
            string sessionRefusal = ValidatePaperSessionFence(command, action);
            if (sessionRefusal != null) return sessionRefusal;
            if (!String.Equals(Text(command, "account_name"), ExactAccountName, StringComparison.Ordinal)) return "ACCOUNT_MISMATCH";
            if (!String.Equals(Text(command, "account_class"), ExactAccountClass, StringComparison.Ordinal)) return "ACCOUNT_CLASS_MISMATCH";
            if (!String.Equals(Text(command, "instrument"), ExactInstrumentName, StringComparison.Ordinal)) return "INSTRUMENT_MISMATCH";
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

        private static TimeZoneInfo NewYorkTimezone()
        {
            // NinjaTrader runs on Windows where the registry name is used;
            // .NET installations with IANA data accept America/New_York.
            try { return TimeZoneInfo.FindSystemTimeZoneById(PaperTimezone); }
            catch (TimeZoneNotFoundException) { return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
            catch (InvalidTimeZoneException) { return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
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
            DateTime local = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, NewYorkTimezone());
            DateTime parsedDate;
            if (!DateTime.TryParseExact(tradeDate, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out parsedDate))
                return "TRADE_DATE_MISMATCH";
            TimeSpan clock = local.TimeOfDay;
            string expectedKind;
            string expectedFamily;
            string expectedDate;
            string expectedHash;
            bool insideEntry;
            if ((clock >= new TimeSpan(18, 0, 0) && AsiaStartDay(local))
                || (clock < new TimeSpan(2, 0, 0) && AsiaStartDay(local.AddDays(-1))))
            {
                expectedKind = "ASIA";
                expectedFamily = "ASIA";
                expectedDate = IsoDate(clock >= new TimeSpan(18, 0, 0) ? local.AddDays(1) : local);
                expectedHash = AsiaProfileHash;
                insideEntry = (clock >= new TimeSpan(18, 5, 0)) || (clock < new TimeSpan(1, 30, 0));
            }
            else if (clock >= new TimeSpan(9, 30, 0) && clock < new TimeSpan(16, 0, 0) && NewYorkStartDay(local))
            {
                expectedKind = "NEW_YORK_RTH";
                expectedFamily = "NEW_YORK";
                expectedDate = IsoDate(local);
                expectedHash = NewYorkProfileHash;
                insideEntry = clock >= new TimeSpan(9, 35, 0) && clock < new TimeSpan(15, 30, 0);
            }
            else if (clock >= new TimeSpan(16, 0, 0) && clock < new TimeSpan(18, 0, 0)
                && local.DayOfWeek >= DayOfWeek.Monday && local.DayOfWeek <= DayOfWeek.Thursday)
            {
                expectedKind = "NY_AFTER";
                expectedFamily = "NEW_YORK";
                expectedDate = IsoDate(local);
                expectedHash = NyAfterProfileHash;
                insideEntry = clock >= new TimeSpan(16, 5, 0) && clock < new TimeSpan(17, 30, 0);
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
            string fragment = Fragment(Text(command, "command_id"));
            string name = "BZ-L3G-E-" + fragment;
            OrderAction orderAction = enterLong ? OrderAction.Buy : OrderAction.SellShort;
            Order order = paperAccount.CreateOrder(paperInstrument, orderAction, OrderType.Market, OrderEntry.Automated, TimeInForce.Day, MaximumQuantity, 0, 0, String.Empty, name, NinjaTrader.Core.Globals.MaxDate, null);
            OwnedOrder owner = OwnedOrder.FromCommand(command, name, "ENTRY", order);
            lock (stateLock) ownedByName[name] = owner;
            paperAccount.Submit(new[] { order });
        }

        private void SubmitProtectiveStop(Execution execution, OwnedOrder entry)
        {
            if (execution == null || execution.Quantity != MaximumQuantity || execution.Price <= 0)
            {
                LockAndProtect("INVALID_ENTRY_FILL");
                return;
            }
            bool longPosition = execution.Order.OrderAction == OrderAction.Buy;
            double raw = longPosition ? execution.Price - ProtectiveStopDistance : execution.Price + ProtectiveStopDistance;
            double stopPrice = Math.Round(raw / ExactTickSize, MidpointRounding.AwayFromZero) * ExactTickSize;
            OrderAction stopAction = longPosition ? OrderAction.Sell : OrderAction.BuyToCover;
            string name = "BZ-L3G-S-" + Fragment(entry.CommandId);
            Order stop = paperAccount.CreateOrder(paperInstrument, stopAction, OrderType.StopMarket, OrderEntry.Automated, TimeInForce.Gtc, execution.Quantity, 0, stopPrice, String.Empty, name, NinjaTrader.Core.Globals.MaxDate, null);
            OwnedOrder protection = new OwnedOrder(entry.CommandId, entry.IntentId, entry.DecisionId, name, "PROTECTIVE", stop, DateTime.UtcNow);
            lock (stateLock)
            {
                ownedByName[name] = protection;
                protectiveOrder = stop;
                pendingProtectionCommandId = entry.CommandId;
                protectiveDeadlineUtc = DateTime.UtcNow.AddSeconds(ProtectiveAcceptanceSeconds);
            }
            paperAccount.Submit(new[] { stop });
        }

        private void FlattenOwnedInstrument(Dictionary<string, object> command, bool emergency)
        {
            if (foreignActivity && !emergency)
            {
                Reject(command, "FOREIGN_ACTIVITY_LOCKOUT", Text(command, "command_id"));
                return;
            }
            lock (stateLock)
            {
                flattenInProgress = true;
                flattenDeadlineUtc = DateTime.UtcNow.AddSeconds(ProtectiveAcceptanceSeconds);
                protectiveDeadlineUtc = DateTime.MaxValue;
                pendingFlattenCommandId = Text(command, "command_id");
                pendingFlattenIntentId = Text(command, "intent_id");
                pendingFlattenDecisionId = Text(command, "decision_id");
            }
            CancelOwnedOrders();
            // This exact-instrument Account API call cannot touch any other
            // account or instrument. Preflight refuses foreign MNQ activity.
            paperAccount.Flatten(new[] { paperInstrument });
        }

        private void CancelOwnedOrders()
        {
            List<Order> orders = OwnedWorkingOrders(null);
            if (orders.Count > 0)
                paperAccount.Cancel(orders);
        }

        private List<Order> OwnedWorkingOrders(string role)
        {
            lock (stateLock)
                return ownedByName.Values.Where(value => (role == null || value.Role == role) && value.Order != null && Working(value.Order.OrderState)).Select(value => value.Order).Distinct().ToList();
        }

        private static bool Working(OrderState state)
        {
            return state == OrderState.Initialized || state == OrderState.Submitted || state == OrderState.Accepted
                || state == OrderState.TriggerPending || state == OrderState.Working || state == OrderState.PartFilled
                || state == OrderState.ChangePending || state == OrderState.ChangeSubmitted
                || state == OrderState.CancelPending || state == OrderState.CancelSubmitted;
        }

        private bool ExpectedFlattenOrder(Order order)
        {
            return flattenInProgress && order != null
                && order.Instrument != null && String.Equals(order.Instrument.FullName, ExactInstrumentName, StringComparison.Ordinal)
                && String.Equals(order.Name, "Close", StringComparison.Ordinal);
        }

        private OwnedOrder ExpectedFlattenOwner(Order order)
        {
            return new OwnedOrder(pendingFlattenCommandId, pendingFlattenIntentId, pendingFlattenDecisionId,
                order.Name ?? "Close", "EXIT", order, DateTime.UtcNow);
        }

        private Position CurrentPosition()
        {
            if (paperAccount == null || paperInstrument == null) return null;
            lock (paperAccount.Positions)
                return paperAccount.Positions.FirstOrDefault(position => position.Instrument != null && String.Equals(position.Instrument.FullName, ExactInstrumentName, StringComparison.Ordinal));
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

        private void SendReconciliation()
        {
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
            lock (paperAccount.Orders)
            {
                foreach (Order order in paperAccount.Orders)
                {
                    if (!Working(order.OrderState)) continue;
                    bool exact = order.Instrument != null && String.Equals(order.Instrument.FullName, ExactInstrumentName, StringComparison.Ordinal);
                    OwnedOrder owner;
                    bool expectedFlatten = flattenInProgress && exact && String.Equals(order.Name, "Close", StringComparison.Ordinal);
                    bool owned = ownedByName.TryGetValue(order.Name ?? String.Empty, out owner) || IsOwnedName(order.Name) || expectedFlatten;
                    if (!exact || !owned) foreign = true;
                    if (exact) working++;
                    if (exact && owned && (owner == null || owner.Role == "ENTRY")) entryWorking++;
                }
            }
            lock (paperAccount.Positions)
                if (paperAccount.Positions.Any(item => item.Quantity != 0 && (item.Instrument == null || !String.Equals(item.Instrument.FullName, ExactInstrumentName, StringComparison.Ordinal)))) foreign = true;
            lock (stateLock)
            {
                foreignActivity = foreign;
                reconciled = !foreign && Math.Abs(quantity) <= MaximumQuantity;
                if (foreign) lockedOut = true;
            }
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
            SendSigned(message);
        }

        private void OnOrderUpdate(object sender, OrderEventArgs e)
        {
            if (e == null || e.Order == null || e.Order.Account != paperAccount) return;
            Order order = e.Order;
            bool exact = order.Instrument != null && String.Equals(order.Instrument.FullName, ExactInstrumentName, StringComparison.Ordinal);
            OwnedOrder owner = null;
            lock (stateLock)
            {
                if (IsOwnedName(order.Name))
                {
                    if (!ownedByName.TryGetValue(order.Name, out owner))
                    {
                        owner = OwnedOrder.Restored(order);
                        ownedByName[order.Name] = owner;
                    }
                    owner.Order = order;
                    owner.Terminal = !Working(order.OrderState);
                }
                else if (ExpectedFlattenOrder(order))
                    owner = ExpectedFlattenOwner(order);
                else if (Working(order.OrderState))
                {
                    foreignActivity = true;
                    lockedOut = true;
                }
                if (!exact && Working(order.OrderState))
                {
                    foreignActivity = true;
                    lockedOut = true;
                }
                if (owner != null && owner.Role == "PROTECTIVE")
                {
                    if (order.OrderState == OrderState.Accepted || order.OrderState == OrderState.Working)
                        protectiveDeadlineUtc = DateTime.MaxValue;
                    // Cancellation is expected while an accepted EXIT or
                    // EMERGENCY_FLATTEN is cancelling owned orders before its
                    // exact-instrument flatten.  A timeout still fails closed
                    // if that flatten does not reach flat promptly.
                    if (order.OrderState == OrderState.Rejected || (order.OrderState == OrderState.Cancelled && !flattenInProgress))
                        LockAndProtect("PROTECTIVE_STOP_REJECTED");
                }
            }
            if (!authenticated) return;
            Dictionary<string, object> message = SessionMessage("ORDER_EVENT");
            message["receipt_id"] = "l3g-order-" + Guid.NewGuid().ToString("N");
            message["native_order_id"] = order.OrderId ?? String.Empty;
            message["order_name"] = order.Name ?? String.Empty;
            message["order_role"] = owner == null ? "FOREIGN" : owner.Role;
            message["order_state"] = order.OrderState.ToString().ToUpperInvariant();
            message["quantity"] = order.Quantity;
            message["filled_quantity"] = order.Filled;
            message["command_id"] = owner == null ? null : owner.CommandId;
            SendSigned(message);
        }

        private void OnExecutionUpdate(object sender, ExecutionEventArgs e)
        {
            if (e == null || e.Execution == null || e.Execution.Order == null || e.Execution.Order.Account != paperAccount) return;
            Order order = e.Execution.Order;
            OwnedOrder owner;
            lock (stateLock)
            {
                ownedByName.TryGetValue(order.Name ?? String.Empty, out owner);
                if (owner == null && ExpectedFlattenOrder(order))
                    owner = ExpectedFlattenOwner(order);
            }
            if (owner == null)
            {
                lock (stateLock) { foreignActivity = true; lockedOut = true; }
                return;
            }
            if (owner.Role == "ENTRY")
                SubmitProtectiveStop(e.Execution, owner);
            if (!authenticated) return;
            Dictionary<string, object> message = SessionMessage("EXECUTION_EVENT");
            message["receipt_id"] = "l3g-execution-" + Guid.NewGuid().ToString("N");
            message["native_execution_id"] = e.Execution.ExecutionId ?? String.Empty;
            message["native_order_id"] = order.OrderId ?? String.Empty;
            message["order_role"] = owner.Role;
            message["command_id"] = owner.CommandId;
            message["intent_id"] = owner.IntentId;
            message["decision_id"] = owner.DecisionId;
            message["price"] = e.Execution.Price;
            message["quantity"] = e.Execution.Quantity;
            message["direction"] = order.OrderAction == OrderAction.Buy || order.OrderAction == OrderAction.BuyToCover ? "LONG" : "SHORT";
            message["strategy_daily_realized_pnl"] = 0;
            SendSigned(message);
        }

        private void OnPositionUpdate(object sender, PositionEventArgs e)
        {
            if (e == null || e.Position == null || e.Position.Account != paperAccount) return;
            Position position = e.Position;
            bool exact = position.Instrument != null && String.Equals(position.Instrument.FullName, ExactInstrumentName, StringComparison.Ordinal);
            if (!exact && position.Quantity != 0)
            {
                lock (stateLock) { foreignActivity = true; lockedOut = true; }
                return;
            }
            if (!exact || !authenticated) return;
            int quantity = position.MarketPosition == MarketPosition.Short ? -position.Quantity : position.MarketPosition == MarketPosition.Flat ? 0 : position.Quantity;
            if (quantity == 0)
            {
                lock (stateLock)
                {
                    flattenInProgress = false;
                    flattenDeadlineUtc = DateTime.MaxValue;
                    pendingFlattenCommandId = null;
                    pendingFlattenIntentId = null;
                    pendingFlattenDecisionId = null;
                }
            }
            Dictionary<string, object> message = SessionMessage("POSITION_EVENT");
            message["receipt_id"] = "l3g-position-" + Guid.NewGuid().ToString("N");
            message["quantity"] = quantity;
            message["average_price"] = position.AveragePrice;
            message["timestamp"] = UtcNow();
            SendSigned(message);
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
                bool heartbeatLost;
                bool protectionFailed;
                bool flattenFailed;
                lock (stateLock)
                {
                    Position position = CurrentPosition();
                    bool positionOpen = position != null && position.Quantity != 0;
                    bool ownedActivity = OwnedWorkingOrders(null).Count > 0 || positionOpen;
                    heartbeatLost = authenticated && ownedActivity && DateTime.UtcNow - lastHeartbeatUtc > TimeSpan.FromSeconds(5);
                    protectionFailed = protectiveDeadlineUtc != DateTime.MaxValue && DateTime.UtcNow > protectiveDeadlineUtc;
                    flattenFailed = flattenInProgress && flattenDeadlineUtc != DateTime.MaxValue && DateTime.UtcNow > flattenDeadlineUtc && positionOpen;
                    if (flattenInProgress && !positionOpen)
                    {
                        flattenInProgress = false;
                        flattenDeadlineUtc = DateTime.MaxValue;
                        pendingFlattenCommandId = null;
                        pendingFlattenIntentId = null;
                        pendingFlattenDecisionId = null;
                    }
                    if (flattenFailed)
                        flattenInProgress = false;
                }
                if (heartbeatLost) LockAndProtect("HEARTBEAT_WATCHDOG");
                if (protectionFailed) LockAndProtect("PROTECTIVE_STOP_ACCEPTANCE_TIMEOUT");
                if (flattenFailed) LockAndProtect("FLATTEN_ACCEPTANCE_TIMEOUT");
            }
        }

        private void LockAndProtect(string reason)
        {
            lock (stateLock)
            {
                if (lockedOut && reason == "HEARTBEAT_WATCHDOG") return;
                lockedOut = true;
                reconciled = false;
                protectiveDeadlineUtc = DateTime.MaxValue;
                flattenInProgress = false;
                flattenDeadlineUtc = DateTime.MaxValue;
                pendingFlattenCommandId = null;
                pendingFlattenIntentId = null;
                pendingFlattenDecisionId = null;
            }
            try
            {
                NinjaTrader.Core.Globals.RandomDispatcher.BeginInvoke(new Action(delegate
                {
                    try
                    {
                        CancelOwnedOrders();
                        Position position = CurrentPosition();
                        if (position != null && position.Quantity != 0)
                            paperAccount.Flatten(new[] { paperInstrument });
                    }
                    catch (Exception error) { Diagnostic("SAFETY_ACTION_FAILED_" + error.GetType().Name); }
                }));
            }
            catch (Exception error) { Diagnostic("SAFETY_DISPATCH_FAILED_" + error.GetType().Name); }
            if (authenticated)
            {
                Dictionary<string, object> incident = SessionMessage("SAFETY_EVENT");
                incident["receipt_id"] = "l3g-safety-" + Guid.NewGuid().ToString("N");
                incident["reason_code"] = reason;
                SendSigned(incident);
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
                ? "NEW_YORK" : kind == "ASIA" ? "ASIA" : "OFF_SESSION";
            message["session_id"] = sessionId;
            message["trade_date"] = tradeDate;
            message["session_profile_hash"] = profileHash;
            message["session_generation"] = generation;
            message["timestamp"] = UtcNow();
            return message;
        }

        private void SendSigned(Dictionary<string, object> message)
        {
            if (message == null || signingKey == null) return;
            message.Remove("signature");
            message["signature"] = Sign(message);
            byte[] bytes = Encoding.UTF8.GetBytes(Canonical(message) + "\n");
            lock (sendLock)
            {
                try
                {
                    NetworkStream current = stream;
                    if (current != null) current.Write(bytes, 0, bytes.Length);
                }
                catch (IOException) { CloseTransport(); }
                catch (ObjectDisposedException) { CloseTransport(); }
            }
        }

        private bool Verify(Dictionary<string, object> message)
        {
            string supplied = Text(message, "signature");
            if (String.IsNullOrWhiteSpace(supplied)) return false;
            string expected = Sign(message);
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
    }
}
