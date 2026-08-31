// L3H is a dedicated, disarmed live-capability AddOn.  It intentionally shares
// no source, port, key, wire schema, or client-order prefix with L3G paper.
// NinjaTrader compilation/installation is an operator-visible commissioning
// step; source presence is never treated as installed-artifact proof.
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Web.Script.Serialization;
using System.Windows;
using System.Windows.Controls;
using NinjaTrader.Cbi;
using NinjaTrader.Gui;
using NinjaTrader.Gui.Tools;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.AddOns;

namespace NinjaTrader.NinjaScript.AddOns
{
    public sealed class BeelzebubLiveExecutionAddOn : AddOnBase
    {
        private const string WireSchema = "lane-iii-phase-h-live-execution-v1";
        private const string ProtocolVersion = "l3h-live-addon-protocol-v1";
        private const int Port = 48137;
        private const int MaximumFrameBytes = 65536;
        private const int MaximumQuantity = 1;
        private const double TickSize = 0.25;
        private const double StopDistancePoints = 25.0;
        private const int HeartbeatTimeoutSeconds = 5;
        // Replaced by l3h_deploy_ninjatrader.ps1 before an operator-visible NT8
        // compile. PENDING is deliberately refused by NativeReady().
        private const string SourceFingerprint = "PENDING_L3H_INSTALL_FINGERPRINT";

        private readonly object stateLock = new object();
        private readonly object sendLock = new object();
        private readonly HashSet<string> seenNonces = new HashSet<string>(StringComparer.Ordinal);
        private readonly HashSet<string> processedCommands = new HashSet<string>(StringComparer.Ordinal);
        private readonly Dictionary<string, Order> ownedOrders = new Dictionary<string, Order>(StringComparer.Ordinal);
        private readonly string addonSessionId = Guid.NewGuid().ToString("N");
        private Account account;
        private Instrument instrument;
        private TcpClient client;
        private NetworkStream stream;
        private Thread transportThread;
        private Thread watchdogThread;
        private byte[] signingKey;
        private bool stopping;
        private bool authenticated;
        // Mechanical readiness is not live-capital authority.  A live entry
        // additionally requires an exact, fresh, one-shot signed envelope.
        private bool mechanicallyArmed;
        private bool killLatch;
        private bool unknownState;
        private bool foreignActivity;
        private bool protectionAvailable;
        // Exposure tracking is a risk-reduction latch, never entry authority.
        // It keeps the watchdog active after a one-shot live capability has
        // been consumed even though mechanicallyArmed must remain false.
        private bool exposureGuardActive;
        private string capabilityHash;
        private string capabilityGeneration;
        private string commissioningEpoch;
        private string accountBindingHash;
        private string expectedAccountName;
        private string bindingAccountClass;
        private bool bindingLiveCapital;
        private string expectedNativeAccountFingerprint;
        private string expectedConnectionIdentityHash;
        private string expectedProviderIdentityHash;
        private string authorizationBoundaryVersion;
        private string authorizationSessionId;
        private string gatewaySessionId;
        private readonly HashSet<string> consumedLiveAuthorizations = new HashSet<string>(StringComparer.Ordinal);
        private int liveSendCount;
        private DateTime lastHeartbeatUtc = DateTime.MinValue;
        private Order protectiveOrder;
        private EventWaitHandle outOfBandKillEvent;
        private NTMenuItem controlCenterNewMenu;
        private NTMenuItem killMenuItem;

        protected override void OnStateChange()
        {
            if (State == State.Active) StartDisarmed();
            else if (State == State.Terminated) StopDisarmed();
        }

        private void StartDisarmed()
        {
            lock (stateLock)
            {
                mechanicallyArmed = false; killLatch = false; authenticated = false; foreignActivity = false; unknownState = false;
                protectionAvailable = false; exposureGuardActive = false; capabilityHash = null; capabilityGeneration = null; commissioningEpoch = null;
                authorizationSessionId = null; gatewaySessionId = null; liveSendCount = 0; consumedLiveAuthorizations.Clear();
                if (!LoadLocalBindingAndKey()) { Diagnostic("L3H_BINDING_OR_KEY_UNAVAILABLE"); return; }
                if (SourceFingerprint == "PENDING_L3H_INSTALL_FINGERPRINT") { Diagnostic("L3H_SOURCE_FINGERPRINT_PENDING"); return; }
                instrument = Instrument.GetInstrument("MNQ SEP26");
                if (!ExactInstrument(instrument)) { Diagnostic("L3H_DENY_WRONG_CONTRACT"); return; }
                List<Account> matches;
                lock (Account.All) matches = Account.All.Where(item => String.Equals(item.Name, expectedAccountName, StringComparison.Ordinal)).ToList();
                if (matches.Count != 1) { Diagnostic("L3H_DENY_WRONG_ACCOUNT"); return; }
                account = matches[0];
                if (!NativeAccountIdentityReady()) { account = null; Diagnostic("L3H_BLOCKED_LIVE_ACCOUNT_IDENTITY"); return; }
                protectionAvailable = true;
                try { outOfBandKillEvent = new EventWaitHandle(false, EventResetMode.ManualReset, @"Global\BeelzebubL3HNativeKill"); }
                catch (Exception error) { protectionAvailable = false; Diagnostic("L3H_OUT_OF_BAND_KILL_UNAVAILABLE_" + error.GetType().Name); }
                account.OrderUpdate += OnOrderUpdate;
                account.ExecutionUpdate += OnExecutionUpdate;
                account.PositionUpdate += OnPositionUpdate;
                stopping = false;
                transportThread = NewThread(TransportLoop, "BeelzebubLiveExecutionTransport");
                watchdogThread = NewThread(WatchdogLoop, "BeelzebubLiveExecutionWatchdog");
                transportThread.Start(); watchdogThread.Start();
                Diagnostic("L3H_NATIVE_RISK_GUARD_DISARMED");
            }
        }

        private bool LoadLocalBindingAndKey()
        {
            // The local binding is created only by the bootstrap/attestation
            // flow and contains no broker password. It is independently HMAC
            // protected; this AddOn refuses a missing or malformed binding.
            try
            {
                // This must match l3h_bootstrap.ps1 exactly. Keeping both the
                // key and signed native binding in LocalApplicationData avoids
                // broadening the NinjaTrader Documents ACL for a capability.
                string root = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "Beelzebub", "authority", "l3h");
                signingKey = File.ReadAllBytes(Path.Combine(root, "keys", "l3h.execution.local.key"));
                if (signingKey.Length < 32) return false;
                string text = File.ReadAllText(Path.Combine(root, "l3h.live.binding.json"), Encoding.UTF8);
                JavaScriptSerializer serializer = new JavaScriptSerializer();
                Dictionary<string, object> binding = serializer.DeserializeObject(text) as Dictionary<string, object>;
                if (binding == null || !VerifyBinding(binding)) return false;
                expectedAccountName = Text(binding, "native_account_id");
                accountBindingHash = Text(binding, "account_binding_hash");
                capabilityHash = Text(binding, "capability_hash");
                capabilityGeneration = Text(binding, "capability_generation");
                commissioningEpoch = Text(binding, "commissioning_epoch");
                bindingAccountClass = Text(binding, "account_class");
                // Legacy L3H.2 bindings are accepted only for exact Sim101
                // mechanics.  Missing metadata can never become live capital.
                if (String.IsNullOrWhiteSpace(bindingAccountClass) && String.Equals(expectedAccountName, "Sim101", StringComparison.Ordinal))
                    bindingAccountClass = "LOCAL_SIMULATION";
                bindingLiveCapital = Boolean(binding, "live_capital");
                expectedNativeAccountFingerprint = Text(binding, "native_account_fingerprint");
                expectedConnectionIdentityHash = Text(binding, "connection_identity_hash");
                expectedProviderIdentityHash = Text(binding, "provider_identity_hash");
                authorizationBoundaryVersion = Text(binding, "authorization_boundary_version");
                return !String.IsNullOrWhiteSpace(expectedAccountName) && Hash(accountBindingHash)
                    && Hash(capabilityHash) && !String.IsNullOrWhiteSpace(capabilityGeneration)
                    && !String.IsNullOrWhiteSpace(commissioningEpoch) && BindingClassValid();
            }
            catch (Exception error) { Diagnostic("L3H_BINDING_LOAD_" + error.GetType().Name); return false; }
        }

        private void StopDisarmed()
        {
            lock (stateLock)
            {
                stopping = true; mechanicallyArmed = false; authenticated = false; capabilityHash = null;
                authorizationSessionId = null; gatewaySessionId = null; consumedLiveAuthorizations.Clear();
                CloseTransport();
                if (account != null)
                {
                    account.OrderUpdate -= OnOrderUpdate; account.ExecutionUpdate -= OnExecutionUpdate; account.PositionUpdate -= OnPositionUpdate;
                }
                if (outOfBandKillEvent != null) { outOfBandKillEvent.Close(); outOfBandKillEvent = null; }
            }
            Join(transportThread); Join(watchdogThread);
            if (signingKey != null) Array.Clear(signingKey, 0, signingKey.Length);
            signingKey = null; account = null; instrument = null;
            Diagnostic("L3H_TERMINATED_DISARMED");
        }

        private void TransportLoop()
        {
            while (!stopping)
            {
                try
                {
                    TcpClient next = new TcpClient(); next.Connect(IPAddress.Loopback, Port);
                    lock (stateLock) { if (stopping) { next.Close(); return; } client = next; stream = next.GetStream(); authenticated = false; mechanicallyArmed = false; authorizationSessionId = null; gatewaySessionId = null; }
                    Send("ADDON_HELLO", "l3h-hello-" + addonSessionId, new Dictionary<string, object> {
                        { "addon_session_id", addonSessionId }, { "addon_fingerprint", SourceFingerprint },
                        { "capability_hash", capabilityHash ?? "UNBOUND" }, { "state", "DISARMED" },
                        { "account_class", bindingAccountClass }, { "account_fingerprint", NativeAccountFingerprint() },
                        { "connection_identity_hash", NativeConnectionIdentityHash() },
                        { "provider_identity_hash", NativeProviderIdentityHash() }, { "live_send_count", liveSendCount }
                    });
                    ReadFrames(stream);
                }
                catch (Exception error) { if (!stopping) Diagnostic("L3H_TRANSPORT_" + error.GetType().Name); }
                finally
                {
                    bool unresolved;
                    lock (stateLock)
                    {
                        unresolved = (mechanicallyArmed || exposureGuardActive) && !stopping;
                        authenticated = false; mechanicallyArmed = false; authorizationSessionId = null; gatewaySessionId = null; CloseTransport();
                    }
                    if (unresolved)
                    {
                        unknownState = true;
                        Diagnostic("L3H_UNKNOWN_STATE_TRANSPORT_LOSS");
                        NativeKillFlattenDisarm(null, "TRANSPORT_LOSS_UNKNOWN");
                    }
                }
                if (!stopping) Thread.Sleep(1000);
            }
        }

        private void ReadFrames(NetworkStream current)
        {
            byte[] read = new byte[4096]; List<byte> buffer = new List<byte>();
            while (!stopping)
            {
                int count = current.Read(read, 0, read.Length); if (count <= 0) return;
                for (int index = 0; index < count; index++)
                {
                    if (read[index] == 10)
                    {
                        if (buffer.Count > 0) { ProcessFrame(Encoding.UTF8.GetString(buffer.ToArray())); buffer.Clear(); }
                    }
                    else { buffer.Add(read[index]); if (buffer.Count > MaximumFrameBytes) { Diagnostic("L3H_DENY_OVERSIZED_FRAME"); return; } }
                }
            }
        }

        private void ProcessFrame(string text)
        {
            Dictionary<string, object> message;
            try
            {
                JavaScriptSerializer serializer = new JavaScriptSerializer(); serializer.MaxJsonLength = MaximumFrameBytes;
                message = serializer.DeserializeObject(text) as Dictionary<string, object>;
                if (message == null) throw new InvalidDataException();
            }
            catch (Exception) { Diagnostic("L3H_DENY_MALFORMED_FRAME"); return; }
            string denial;
            if (!VerifyFrame(message, out denial)) { Reject(message, denial); return; }
            string type = Text(message, "message_type");
            if (type == "GATEWAY_HELLO")
            {
                Dictionary<string, object> hello = message["payload"] as Dictionary<string, object>;
                string nextAuthorizationSession = Text(hello, "authorization_session_id");
                string nextGatewaySession = Text(hello, "gateway_session_id");
                if (!nextAuthorizationSession.StartsWith("l3h3-auth-session-", StringComparison.Ordinal)
                    || !nextGatewaySession.StartsWith("l3h3-gateway-session-", StringComparison.Ordinal))
                { Reject(message, "DENY_AUTHORIZATION_SESSION"); return; }
                authorizationSessionId = nextAuthorizationSession; gatewaySessionId = nextGatewaySession;
                authenticated = true; mechanicallyArmed = false; SendReconciliation("GATEWAY_HELLO"); return;
            }
            if (!authenticated) { Reject(message, "DENY_NOT_AUTHENTICATED"); return; }
            if (type == "HEARTBEAT") { lastHeartbeatUtc = DateTime.UtcNow; return; }
            if (type != "COMMAND") { Reject(message, "DENY_UNSUPPORTED_MESSAGE"); return; }
            ExecuteCommand(message, Text(message, "request_id"));
        }

        private void ExecuteCommand(Dictionary<string, object> message, string requestId)
        {
            Dictionary<string, object> command = message["payload"] as Dictionary<string, object>;
            string commandId = command == null ? null : Text(command, "command_id");
            if (command == null || !ValidCommandIdentity(command)) { Reject(message, "DENY_COMMAND_IDENTITY"); return; }
            lock (stateLock)
            {
                if (!processedCommands.Add(commandId)) { Ack(requestId, commandId, "ACK", "DUPLICATE_COMMAND_NOOP"); return; }
            }
            string action = Text(command, "action");
            if (action == "CAPABILITY_BIND") { BindCapability(command, requestId); return; }
            if (action == "ARM") { Arm(command, requestId); return; }
            if (action == "KILL_FLATTEN_DISARM") { NativeKillFlattenDisarm(commandId, "COMMAND_KILL"); Ack(requestId, commandId, "ACK", "KILL_LATCHED"); return; }
            if (action == "RECONCILE") { SendReconciliation("COMMAND_RECONCILE"); Ack(requestId, commandId, "ACK", "RECONCILIATION_SENT"); return; }
            if (action == "ENTER_LONG" || action == "ENTER_SHORT") { Enter(command, requestId, action == "ENTER_LONG"); return; }
            Reject(message, "DENY_UNSUPPORTED_COMMAND");
        }

        private void BindCapability(Dictionary<string, object> command, string requestId)
        {
            if (!ExactCapability(command)) { RejectCommand(requestId, Text(command, "command_id"), "DENY_CAPABILITY_BINDING"); return; }
            capabilityHash = Text(command, "capability_hash"); capabilityGeneration = Text(command, "capability_generation");
            commissioningEpoch = Text(command, "commissioning_epoch"); mechanicallyArmed = false;
            Ack(requestId, Text(command, "command_id"), "ACK", "CAPABILITY_BOUND_DISARMED"); SendReconciliation("CAPABILITY_BOUND");
        }

        private void Arm(Dictionary<string, object> command, string requestId)
        {
            if (bindingLiveCapital || bindingAccountClass == "LIVE_CAPITAL")
            { RejectCommand(requestId, Text(command, "command_id"), "DENY_LIVE_REQUIRES_ONE_SHOT_AUTHORIZATION"); return; }
            string reason;
            if (!NativeReady(command, out reason)) { RejectCommand(requestId, Text(command, "command_id"), reason); return; }
            if (CurrentQuantity() != 0 || HasWorkingOrders() || foreignActivity) { RejectCommand(requestId, Text(command, "command_id"), "DENY_UNKNOWN_BROKER_STATE"); return; }
            mechanicallyArmed = true; lastHeartbeatUtc = DateTime.UtcNow;
            Ack(requestId, Text(command, "command_id"), "ACK", "ARMED_FLAT"); SendReconciliation("ARMED_FLAT");
        }

        private void Enter(Dictionary<string, object> command, string requestId, bool longSide)
        {
            string reason;
            if (!NativeReady(command, out reason)) { RejectCommand(requestId, Text(command, "command_id"), reason); return; }
            bool liveEntry = bindingLiveCapital && bindingAccountClass == "LIVE_CAPITAL";
            if (!liveEntry && !mechanicallyArmed) { RejectCommand(requestId, Text(command, "command_id"), "DENY_NOT_ARMED"); return; }
            if (CurrentQuantity() != 0) { RejectCommand(requestId, Text(command, "command_id"), "DENY_POSITION_NONFLAT"); return; }
            if (HasWorkingOrders()) { RejectCommand(requestId, Text(command, "command_id"), "DENY_FOREIGN_ORDER"); return; }
            if (!protectionAvailable) { RejectCommand(requestId, Text(command, "command_id"), "DENY_PROTECTION_UNAVAILABLE"); return; }
            if (liveEntry && !ValidateAndConsumeLiveAuthorization(command, out reason))
            { RejectCommand(requestId, Text(command, "command_id"), reason); return; }
            string id = Text(command, "client_order_id");
            Order order = account.CreateOrder(instrument, longSide ? OrderAction.Buy : OrderAction.SellShort, OrderType.Market,
                OrderEntry.Automated, TimeInForce.Day, 1, 0, 0, String.Empty, id, NinjaTrader.Core.Globals.MaxDate, null);
            lock (stateLock) { ownedOrders[id] = order; if (liveEntry) exposureGuardActive = true; }
            if (liveEntry) Interlocked.Increment(ref liveSendCount);
            account.Submit(new[] { order }); Ack(requestId, Text(command, "command_id"), "ACK", "BROKER_SUBMIT_REQUESTED");
        }

        private bool NativeReady(Dictionary<string, object> command, out string reason)
        {
            reason = "DENY_UNKNOWN_BROKER_STATE";
            if (killLatch) { reason = "DENY_KILL_LATCH"; return false; }
            if (!authenticated) { reason = "DENY_NOT_AUTHENTICATED"; return false; }
            if (Text(command, "native_instrument") != "MNQ SEP26" || Text(command, "canonical_contract") != "MNQU6") { reason = "DENY_WRONG_CONTRACT"; return false; }
            if (!ExactCapability(command)) { reason = "DENY_COMMISSION_EPOCH"; return false; }
            if (!ExactInstrument(instrument) || account == null || !String.Equals(account.Name, expectedAccountName, StringComparison.Ordinal)
                || !NativeAccountIdentityReady()) { reason = "DENY_WRONG_ACCOUNT"; return false; }
            if (bindingLiveCapital)
            {
                if (Text(command, "account_class") != "LIVE_CAPITAL" || !Boolean(command, "live_capital"))
                { reason = "DENY_LIVE_ACCOUNT_CLASS"; return false; }
            }
            else if (Text(command, "account_class") != "LOCAL_SIMULATION" || Boolean(command, "live_capital"))
            { reason = "DENY_SIMULATION_ACCOUNT_CLASS"; return false; }
            if (foreignActivity) { reason = "DENY_FOREIGN_ORDER"; return false; }
            if (DateTime.UtcNow - lastHeartbeatUtc > TimeSpan.FromSeconds(HeartbeatTimeoutSeconds)) { reason = "DENY_SESSION"; return false; }
            if (!Boolean(command, "session_valid")) { reason = "DENY_SESSION"; return false; }
            if (!Boolean(command, "daily_loss_clear")) { reason = "DENY_DAILY_LOSS"; return false; }
            if (command.ContainsKey("quantity") && Integer(command, "quantity") != MaximumQuantity) { reason = "DENY_QTY"; return false; }
            return true;
        }

        private bool ExactCapability(Dictionary<string, object> command)
        {
            return Hash(Text(command, "capability_hash")) && String.Equals(Text(command, "capability_hash"), capabilityHash, StringComparison.Ordinal)
                && String.Equals(Text(command, "capability_generation"), capabilityGeneration, StringComparison.Ordinal)
                && String.Equals(Text(command, "commissioning_epoch"), commissioningEpoch, StringComparison.Ordinal)
                && String.Equals(Text(command, "account_binding_hash"), accountBindingHash, StringComparison.Ordinal)
                && String.Equals(Text(command, "native_instrument"), "MNQ SEP26", StringComparison.Ordinal)
                && String.Equals(Text(command, "canonical_contract"), "MNQU6", StringComparison.Ordinal);
        }

        private void OnExecutionUpdate(object sender, ExecutionEventArgs e)
        {
            if (e == null || e.Execution == null || e.Execution.Order == null || e.Execution.Order.Account != account) return;
            Order entry = e.Execution.Order;
            // NinjaTrader creates the exact-instrument Close order for our
            // native flatten request. That exit is not foreign activity; only
            // an independently created order must quarantine the session.
            if (!IsOwned(entry) && !IsNativeKillFlattenOrder(entry)) { foreignActivity = true; NativeKillFlattenDisarm(null, "FOREIGN_EXECUTION"); return; }
            if (IsNativeKillFlattenOrder(entry)) return;
            if (entry.OrderType == OrderType.StopMarket) return;
            try
            {
                OrderAction action = entry.OrderAction == OrderAction.Buy ? OrderAction.Sell : OrderAction.BuyToCover;
                double raw = entry.OrderAction == OrderAction.Buy ? e.Execution.Price - StopDistancePoints : e.Execution.Price + StopDistancePoints;
                double stopPrice = Math.Round(raw / TickSize, MidpointRounding.AwayFromZero) * TickSize;
                string stopId = "BZ-L3H-S-" + entry.Name.Substring(Math.Max(0, entry.Name.Length - 20));
                protectiveOrder = account.CreateOrder(instrument, action, OrderType.StopMarket, OrderEntry.Automated, TimeInForce.Gtc,
                    e.Execution.Quantity, 0, stopPrice, String.Empty, stopId, NinjaTrader.Core.Globals.MaxDate, null);
                lock (stateLock) ownedOrders[stopId] = protectiveOrder;
                if (bindingLiveCapital) Interlocked.Increment(ref liveSendCount);
                account.Submit(new[] { protectiveOrder }); protectionAvailable = true;
            }
            catch (Exception error) { Diagnostic("L3H_PROTECTION_FAILURE_" + error.GetType().Name); NativeKillFlattenDisarm(null, "PROTECTION_FAILURE"); }
        }

        private void OnOrderUpdate(object sender, OrderEventArgs e)
        {
            if (e == null || e.Order == null || e.Order.Account != account) return;
            if (!IsOwned(e.Order) && !IsNativeKillFlattenOrder(e.Order) && Working(e.Order.OrderState)) { foreignActivity = true; mechanicallyArmed = false; Diagnostic("L3H_FOREIGN_ACTIVITY_QUARANTINE"); }
            if (protectiveOrder != null && String.Equals(e.Order.Name, protectiveOrder.Name, StringComparison.Ordinal)
                && (e.Order.OrderState == OrderState.Accepted || e.Order.OrderState == OrderState.Working))
            {
                // A broker can acknowledge the stop after NativeKill has
                // already observed its owned-order list. Cancel that late
                // acknowledgement rather than leaving a flat account exposed
                // to a stale protective order.
                if (killLatch || CurrentQuantity() == 0)
                {
                    try { account.Cancel(new[] { e.Order }); }
                    catch (Exception error) { Diagnostic("L3H_KILL_LATE_PROTECTIVE_CANCEL_" + error.GetType().Name); }
                }
                else protectionAvailable = true;
            }
            if (protectiveOrder != null && String.Equals(e.Order.Name, protectiveOrder.Name, StringComparison.Ordinal)
                && (e.Order.OrderState == OrderState.Rejected || e.Order.OrderState == OrderState.Cancelled) && CurrentQuantity() != 0)
                NativeKillFlattenDisarm(null, "PROTECTION_REJECTED");
            RefreshExposureGuard();
            SendReconciliation("ORDER_UPDATE");
        }

        private void OnPositionUpdate(object sender, PositionEventArgs e)
        {
            // Execution callbacks can precede NinjaTrader's position update.
            // A foreign fill therefore needs one safe, idempotent retry after
            // the native quantity becomes observable; otherwise Flatten can
            // race a still-zero position and leave exposure behind.
            if (killLatch && CurrentQuantity() != 0) NativeKillFlattenDisarm(null, "KILL_LATCH_POSITION_RETRY");
            RefreshExposureGuard();
            SendReconciliation("POSITION_UPDATE");
        }

        private void WatchdogLoop()
        {
            while (!stopping)
            {
                if ((mechanicallyArmed || exposureGuardActive) && DateTime.UtcNow - lastHeartbeatUtc > TimeSpan.FromSeconds(HeartbeatTimeoutSeconds))
                    NativeKillFlattenDisarm(null, "CONTROL_HEARTBEAT_LOST");
                if (outOfBandKillEvent != null && outOfBandKillEvent.WaitOne(0))
                {
                    NativeKillFlattenDisarm(null, "OUT_OF_BAND_KILL");
                    outOfBandKillEvent.Reset();
                }
                Thread.Sleep(250);
            }
        }

        // Native kill path B. It is idempotent, latches before side effects,
        // cancels only BZ-L3H orders, flattens exactly MNQ on the bound account,
        // and remains disarmed even after a reconnect.
        public void NativeKillFlattenDisarm(string commandId, string reason)
        {
            lock (stateLock) { killLatch = true; mechanicallyArmed = false; exposureGuardActive = false; }
            try { account.Cancel(OwnedWorkingOrders()); } catch (Exception error) { Diagnostic("L3H_KILL_CANCEL_" + error.GetType().Name); }
            try { account.Flatten(new[] { instrument }); } catch (Exception error) { Diagnostic("L3H_KILL_FLATTEN_" + error.GetType().Name); }
            Diagnostic("L3H_NATIVE_KILL_" + reason); SendReconciliation("KILL_" + reason);
        }

        protected override void OnWindowCreated(Window window)
        {
            ControlCenter controlCenter = window as ControlCenter;
            if (controlCenter == null || killMenuItem != null) return;
            controlCenterNewMenu = controlCenter.FindFirst("ControlCenterMenuItemNew") as NTMenuItem;
            if (controlCenterNewMenu == null) return;
            killMenuItem = new NTMenuItem { Header = "BEELZEBUB L3H — KILL / FLATTEN / DISARM", Style = Application.Current.TryFindResource("MainMenuItem") as Style };
            controlCenterNewMenu.Items.Add(killMenuItem); killMenuItem.Click += OnNativeKillClick;
        }

        protected override void OnWindowDestroyed(Window window)
        {
            if (!(window is ControlCenter) || killMenuItem == null) return;
            if (controlCenterNewMenu != null && controlCenterNewMenu.Items.Contains(killMenuItem)) controlCenterNewMenu.Items.Remove(killMenuItem);
            killMenuItem.Click -= OnNativeKillClick; killMenuItem = null; controlCenterNewMenu = null;
        }

        private void OnNativeKillClick(object sender, RoutedEventArgs e) { NativeKillFlattenDisarm(null, "NINJATRADER_MENU_KILL"); }

        private void SendReconciliation(string reason)
        {
            if (!authenticated || stream == null || account == null || instrument == null) return;
            Send("RECONCILIATION", "l3h-recon-" + Guid.NewGuid().ToString("N"), new Dictionary<string, object> {
                { "reason", reason }, { "account", SafeAccountIdentifier() }, { "contract", instrument.FullName },
                { "position", CurrentPositionState() }, { "quantity", CurrentQuantity() }, { "owned_working_orders", OwnedWorkingOrders().Count },
                { "foreign_or_unknown_orders", foreignActivity ? 1 : 0 }, { "armed", mechanicallyArmed },
                { "kill_latch", killLatch }, { "unknown_state", unknownState }, { "protection_available", protectionAvailable },
                { "exposure_guard_active", exposureGuardActive },
                { "account_class", bindingAccountClass }, { "account_fingerprint", NativeAccountFingerprint() },
                { "connection_identity_hash", NativeConnectionIdentityHash() }, { "provider_identity_hash", NativeProviderIdentityHash() },
                { "addon_session_id", addonSessionId }, { "gateway_session_id", gatewaySessionId ?? "DISCONNECTED" },
                { "addon_provenance", SourceFingerprint }, { "live_send_count", liveSendCount },
                { "observed_at", DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture) }
            });
        }

        private void Ack(string requestId, string commandId, string outcome, string reason)
        {
            Send(outcome, requestId, new Dictionary<string, object> { { "command_id", commandId }, { "outcome", outcome }, { "reason", reason } });
        }
        private void Reject(Dictionary<string, object> message, string reason) { RejectCommand(Text(message, "request_id"), null, reason); }
        private void RejectCommand(string requestId, string commandId, string reason) { Ack(requestId, commandId ?? "UNKNOWN", "NACK", reason); Diagnostic("L3H_" + reason); }

        private void Send(string messageType, string requestId, Dictionary<string, object> payload)
        {
            if (stream == null || signingKey == null) return;
            Dictionary<string, object> frame = new Dictionary<string, object> {
                { "schema", WireSchema }, { "protocol_version", ProtocolVersion }, { "message_type", messageType },
                { "request_id", requestId }, { "nonce", "l3h-nt-" + Guid.NewGuid().ToString("N") },
                { "timestamp", DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture) }, { "payload", payload }
            };
            frame["payload_hash"] = Sha256(Canonical(payload));
            frame["signature"] = Hex(new HMACSHA256(signingKey).ComputeHash(Encoding.ASCII.GetBytes(
                Text(frame, "schema") + "|" + Text(frame, "protocol_version") + "|" + Text(frame, "message_type") + "|" +
                Text(frame, "request_id") + "|" + Text(frame, "nonce") + "|" + Text(frame, "timestamp") + "|" + Text(frame, "payload_hash"))));
            string line = new JavaScriptSerializer().Serialize(frame) + "\n";
            lock (sendLock) stream.Write(Encoding.UTF8.GetBytes(line), 0, Encoding.UTF8.GetByteCount(line));
        }

        private bool VerifyFrame(Dictionary<string, object> frame, out string denial)
        {
            denial = "DENY_BAD_SIGNATURE";
            if (frame.Count != 9 || Text(frame, "schema") != WireSchema || Text(frame, "protocol_version") != ProtocolVersion) { denial = "DENY_PROTOCOL_VERSION"; return false; }
            Dictionary<string, object> payload = frame["payload"] as Dictionary<string, object>;
            if (payload == null || !Hash(Text(frame, "payload_hash")) || !String.Equals(Text(frame, "payload_hash"), Sha256(Canonical(payload)), StringComparison.Ordinal)) { denial = "DENY_PAYLOAD_HASH"; return false; }
            DateTime timestamp; if (!DateTime.TryParse(Text(frame, "timestamp"), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal, out timestamp) || Math.Abs((DateTime.UtcNow - timestamp.ToUniversalTime()).TotalSeconds) > 10) { denial = "DENY_STALE_TIMESTAMP"; return false; }
            string material = Text(frame, "schema") + "|" + Text(frame, "protocol_version") + "|" + Text(frame, "message_type") + "|" + Text(frame, "request_id") + "|" + Text(frame, "nonce") + "|" + Text(frame, "timestamp") + "|" + Text(frame, "payload_hash");
            string expected = Hex(new HMACSHA256(signingKey).ComputeHash(Encoding.ASCII.GetBytes(material)));
            if (!Hash(Text(frame, "signature")) || !FixedEquals(expected, Text(frame, "signature"))) return false;
            lock (stateLock) { if (!seenNonces.Add(Text(frame, "nonce"))) { denial = "DENY_REPLAY"; return false; } }
            return true;
        }

        private bool VerifyBinding(Dictionary<string, object> binding)
        {
            string signature = Text(binding, "signature"); if (!Hash(signature)) return false;
            binding.Remove("signature"); string expected = Hex(new HMACSHA256(signingKey).ComputeHash(Encoding.UTF8.GetBytes(Canonical(binding)))); binding["signature"] = signature;
            return FixedEquals(expected, signature);
        }

        private bool BindingClassValid()
        {
            if (bindingAccountClass == "LOCAL_SIMULATION")
                return !bindingLiveCapital && (String.IsNullOrWhiteSpace(authorizationBoundaryVersion) || authorizationBoundaryVersion == "L3H3_NONE");
            if (bindingAccountClass == "LIVE_CAPITAL")
                return bindingLiveCapital && authorizationBoundaryVersion == "L3H3_ONE_SHOT_V1"
                    && Hash(expectedNativeAccountFingerprint) && Hash(expectedConnectionIdentityHash) && Hash(expectedProviderIdentityHash);
            return false;
        }

        private bool NativeAccountIdentityReady()
        {
            if (account == null || account.Connection == null || account.Connection.Options == null) return false;
            if (bindingAccountClass == "LOCAL_SIMULATION")
            {
                bool nativeSimulation = account.Provider == Provider.Simulator || account.Connection.Options.Provider == Provider.Simulator;
                return !bindingLiveCapital && nativeSimulation && String.Equals(expectedAccountName, "Sim101", StringComparison.Ordinal);
            }
            if (bindingAccountClass != "LIVE_CAPITAL" || !bindingLiveCapital || authorizationBoundaryVersion != "L3H3_ONE_SHOT_V1") return false;
            bool liveMetadata = account.Provider != Provider.Simulator && account.Provider != Provider.Unknown
                && account.Connection.Options.Provider != Provider.Simulator && account.Connection.Options.Provider != Provider.Unknown
                && !account.Connection.Options.IsDemo && account.Connection.Options.CanManageOrders
                && String.Equals(account.AccountStatus.ToString(), "Enabled", StringComparison.OrdinalIgnoreCase)
                && String.Equals(account.Connection.Status.ToString(), "Connected", StringComparison.OrdinalIgnoreCase);
            return liveMetadata
                && FixedEquals(NativeAccountFingerprint(), expectedNativeAccountFingerprint)
                && FixedEquals(NativeConnectionIdentityHash(), expectedConnectionIdentityHash)
                && FixedEquals(NativeProviderIdentityHash(), expectedProviderIdentityHash);
        }

        private string NativeAccountFingerprint()
        {
            if (account == null) return "UNKNOWN";
            Dictionary<string, object> facts = new Dictionary<string, object> {
                { "platform", "NINJATRADER" }, { "account_id", account.Id.ToString(CultureInfo.InvariantCulture) },
                { "account_name", account.Name ?? String.Empty }, { "display_name", account.DisplayName ?? String.Empty },
                { "fcm", account.Fcm ?? String.Empty }, { "account_provider", account.Provider.ToString() },
                { "account_status", account.AccountStatus.ToString() }, { "connection_identity_hash", NativeConnectionIdentityHash() }
            };
            return Sha256(Canonical(facts));
        }

        private string NativeConnectionIdentityHash()
        {
            if (account == null || account.Connection == null || account.Connection.Options == null) return "UNKNOWN";
            ConnectOptions options = account.Connection.Options;
            Dictionary<string, object> facts = new Dictionary<string, object> {
                { "name", options.Name ?? String.Empty }, { "provider", options.Provider.ToString() },
                { "brand", options.BrandName ?? String.Empty }, { "type", options.TypeName ?? String.Empty },
                { "mode", options.Mode.ToString() }, { "is_demo", options.IsDemo },
                { "can_manage_orders", options.CanManageOrders }, { "connection_status", account.Connection.Status.ToString() }
            };
            return Sha256(Canonical(facts));
        }

        private string NativeProviderIdentityHash()
        {
            if (account == null || account.Connection == null || account.Connection.Options == null) return "UNKNOWN";
            Dictionary<string, object> facts = new Dictionary<string, object> {
                { "account_provider", account.Provider.ToString() },
                { "connection_provider", account.Connection.Options.Provider.ToString() },
                { "brand", account.Connection.Options.BrandName ?? String.Empty }, { "fcm", account.Fcm ?? String.Empty }
            };
            return Sha256(Canonical(facts));
        }

        private string SafeAccountIdentifier()
        {
            if (bindingAccountClass == "LOCAL_SIMULATION") return "Sim101";
            string fingerprint = NativeAccountFingerprint();
            return Hash(fingerprint) ? "LIVE-" + fingerprint.Substring(0, 12).ToUpperInvariant() : "LIVE-UNVERIFIED";
        }

        private bool ValidateAndConsumeLiveAuthorization(Dictionary<string, object> command, out string reason)
        {
            reason = "DENY_LIVE_AUTHORIZATION";
            object rawEnvelope;
            Dictionary<string, object> envelope = command.TryGetValue("live_authorization", out rawEnvelope) ? rawEnvelope as Dictionary<string, object> : null;
            if (envelope == null) { reason = "DENY_LIVE_AUTHORIZATION_REQUIRED"; return false; }
            string[] required = {
                "schema", "authorization_id", "authorization_session_id", "addon_session_id", "gateway_session_id",
                "preflight_digest", "admission_facts_digest", "account_fingerprint", "account_class",
                "provider_identity_hash", "connection_identity_hash", "native_instrument", "canonical_contract",
                "quantity", "action", "command_id", "request_id", "nonce", "issued_at", "expires_at",
                "beelzebub_build_identity", "addon_provenance", "signature"
            };
            if (envelope.Count != required.Length || required.Any(field => !envelope.ContainsKey(field)))
            { reason = "DENY_LIVE_AUTHORIZATION_FIELDS"; return false; }
            string signature = Text(envelope, "signature");
            if (!Hash(signature)) { reason = "DENY_LIVE_AUTHORIZATION_SIGNATURE"; return false; }
            envelope.Remove("signature");
            string expectedSignature = Hex(new HMACSHA256(signingKey).ComputeHash(Encoding.UTF8.GetBytes(Canonical(envelope))));
            envelope["signature"] = signature;
            if (!FixedEquals(expectedSignature, signature)) { reason = "DENY_LIVE_AUTHORIZATION_SIGNATURE"; return false; }
            if (Text(envelope, "schema") != "lane-iii-phase-h-live-admission-v1"
                || Text(envelope, "authorization_session_id") != authorizationSessionId
                || Text(envelope, "addon_session_id") != addonSessionId
                || Text(envelope, "gateway_session_id") != gatewaySessionId)
            { reason = "DENY_LIVE_AUTHORIZATION_SESSION"; return false; }
            if (!Text(envelope, "authorization_id").StartsWith("l3h3-canary-cap-", StringComparison.Ordinal)
                || !Text(envelope, "nonce").StartsWith("l3h3-admission-nonce-", StringComparison.Ordinal)
                || !Hash(Text(envelope, "preflight_digest")) || !Hash(Text(envelope, "admission_facts_digest")))
            { reason = "DENY_LIVE_AUTHORIZATION_IDENTITY"; return false; }
            if (Text(envelope, "account_class") != "LIVE_CAPITAL"
                || Text(envelope, "account_fingerprint") != NativeAccountFingerprint()
                || Text(envelope, "account_fingerprint") != expectedNativeAccountFingerprint
                || Text(envelope, "provider_identity_hash") != NativeProviderIdentityHash()
                || Text(envelope, "connection_identity_hash") != NativeConnectionIdentityHash()
                || Text(envelope, "native_instrument") != "MNQ SEP26" || Text(envelope, "canonical_contract") != "MNQU6"
                || Integer(envelope, "quantity") != 1)
            { reason = "DENY_LIVE_AUTHORIZATION_ACCOUNT_OR_CONTRACT"; return false; }
            if (Text(envelope, "action") != Text(command, "action") || Text(envelope, "command_id") != Text(command, "command_id")
                || Text(envelope, "request_id") != Text(command, "request_id") || Text(envelope, "addon_provenance") != SourceFingerprint
                || !Hash(Text(envelope, "beelzebub_build_identity")))
            { reason = "DENY_LIVE_AUTHORIZATION_COMMAND"; return false; }
            DateTime issued; DateTime expires;
            if (!DateTime.TryParse(Text(envelope, "issued_at"), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal, out issued)
                || !DateTime.TryParse(Text(envelope, "expires_at"), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal, out expires)
                || DateTime.UtcNow < issued.ToUniversalTime() || DateTime.UtcNow >= expires.ToUniversalTime()
                || expires.ToUniversalTime() - issued.ToUniversalTime() > TimeSpan.FromSeconds(60))
            { reason = "DENY_LIVE_AUTHORIZATION_EXPIRED"; return false; }
            // Recheck the native facts after validating the envelope and
            // immediately before one-shot consumption.  This closes the
            // preflight-to-submit account/order/position race at the final
            // native boundary.
            if (!authenticated || !NativeAccountIdentityReady() || CurrentQuantity() != 0 || HasWorkingOrders()
                || foreignActivity || !protectionAvailable)
            { reason = "DENY_LIVE_ATOMIC_FACTS_CHANGED"; return false; }
            lock (stateLock)
            {
                if (!consumedLiveAuthorizations.Add(Text(envelope, "authorization_id")))
                { reason = "DENY_LIVE_AUTHORIZATION_REPLAY"; return false; }
            }
            return true;
        }

        private void RefreshExposureGuard()
        {
            if (!bindingLiveCapital || CurrentQuantity() != 0 || OwnedWorkingOrders().Count != 0) return;
            lock (stateLock) exposureGuardActive = false;
        }

        private int CurrentQuantity() { lock (account.Positions) { Position position = account.Positions.FirstOrDefault(item => item.Instrument != null && item.Instrument.FullName == "MNQ SEP26"); return position == null ? 0 : position.Quantity; } }
        private string CurrentPositionState()
        {
            lock (account.Positions)
            {
                Position position = account.Positions.FirstOrDefault(item => item.Instrument != null && item.Instrument.FullName == "MNQ SEP26");
                if (position == null || position.Quantity == 0) return "FLAT";
                if (position.MarketPosition == MarketPosition.Long) return "LONG";
                if (position.MarketPosition == MarketPosition.Short) return "SHORT";
                return "UNKNOWN";
            }
        }
        private List<Order> OwnedWorkingOrders() { lock (account.Orders) return account.Orders.Where(item => IsOwned(item) && Working(item.OrderState)).ToList(); }
        private bool HasWorkingOrders() { lock (account.Orders) return account.Orders.Any(item => Working(item.OrderState)); }
        private bool IsOwned(Order order) { return order != null && !String.IsNullOrWhiteSpace(order.Name) && order.Name.StartsWith("BZ-L3H-", StringComparison.Ordinal); }
        private bool IsNativeKillFlattenOrder(Order order) { return killLatch && order != null && String.Equals(order.Name, "Close", StringComparison.Ordinal) && order.Instrument != null && ExactInstrument(order.Instrument); }
        private static bool ExactInstrument(Instrument value) { return value != null && value.FullName == "MNQ SEP26" && value.MasterInstrument != null && value.MasterInstrument.Name == "MNQ" && Math.Abs(value.MasterInstrument.TickSize - TickSize) < 0.0000001; }
        private static bool ValidCommandIdentity(Dictionary<string, object> command) { return Text(command, "command_id").StartsWith("l3h-cmd-", StringComparison.Ordinal) && Text(command, "client_order_id").StartsWith("BZ-L3H-", StringComparison.Ordinal); }
        private static bool Working(OrderState state) { return state == OrderState.Initialized || state == OrderState.Submitted || state == OrderState.Accepted || state == OrderState.TriggerPending || state == OrderState.Working || state == OrderState.PartFilled || state == OrderState.ChangePending || state == OrderState.ChangeSubmitted || state == OrderState.CancelPending || state == OrderState.CancelSubmitted; }
        private static string Text(IDictionary<string, object> value, string key) { object item; return value != null && value.TryGetValue(key, out item) && item != null ? Convert.ToString(item, CultureInfo.InvariantCulture) : String.Empty; }
        private static int Integer(IDictionary<string, object> value, string key) { object item; return value != null && value.TryGetValue(key, out item) ? Convert.ToInt32(item, CultureInfo.InvariantCulture) : 0; }
        private static bool Boolean(IDictionary<string, object> value, string key) { object item; return value != null && value.TryGetValue(key, out item) && item is bool && (bool)item; }
        private static bool Hash(string value) { return value != null && value.Length == 64 && value.All(ch => (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')); }
        private static string Sha256(string value) { using (SHA256 hash = SHA256.Create()) return Hex(hash.ComputeHash(Encoding.UTF8.GetBytes(value))); }
        private static string Hex(byte[] value) { return BitConverter.ToString(value).Replace("-", "").ToLowerInvariant(); }
        private static bool FixedEquals(string left, string right) { if (left == null || right == null || left.Length != right.Length) return false; int result = 0; for (int index = 0; index < left.Length; index++) result |= left[index] ^ right[index]; return result == 0; }
        private static string Canonical(object value) { if (value == null) return "null"; Dictionary<string, object> map = value as Dictionary<string, object>; if (map != null) return "{" + String.Join(",", map.OrderBy(item => item.Key, StringComparer.Ordinal).Select(item => new JavaScriptSerializer().Serialize(item.Key) + ":" + Canonical(item.Value))) + "}"; IEnumerable list = value as IEnumerable; if (list != null && !(value is string)) return "[" + String.Join(",", list.Cast<object>().Select(Canonical)) + "]"; if (value is bool) return (bool)value ? "true" : "false"; if (value is string) return new JavaScriptSerializer().Serialize((string)value); return Convert.ToString(value, CultureInfo.InvariantCulture); }
        private static Thread NewThread(ThreadStart action, string name) { Thread thread = new Thread(action); thread.IsBackground = true; thread.Name = name; return thread; }
        private static void Join(Thread thread) { if (thread != null && thread != Thread.CurrentThread) thread.Join(TimeSpan.FromSeconds(3)); }
        private void CloseTransport() { if (stream != null) { try { stream.Close(); } catch (Exception) { } stream = null; } if (client != null) { try { client.Close(); } catch (Exception) { } client = null; } }
        private static void Diagnostic(string marker) { System.Diagnostics.Trace.WriteLine("L3H " + marker); }
    }
}
