// L3-F2 observation-only NinjaTrader 8 AddOn source.
// Install/compile only through NinjaTrader's NinjaScript editor. This source
// intentionally contains no order, ATM, or inbound bridge command surface.
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Windows;
using NinjaTrader.Cbi;
using NinjaTrader.Gui;
using NinjaTrader.Gui.Tools;
using NinjaTrader.NinjaScript.AddOns;

namespace NinjaTrader.NinjaScript.AddOns
{
    public sealed class BeelzebubReadOnlyAddOn : AddOnBase
    {
        private readonly List<Account> observedAccounts = new List<Account>();
        private NTMenuItem controlCenterNewMenu;
        private NTMenuItem attachMenuItem;

        protected override void OnStateChange()
        {
            if (State == State.Active)
            {
                Account.AccountStatusUpdate += OnAccountStatusUpdate;
                BeelzebubReadOnlyOutbound.Diagnostic("ADDON_ACTIVE");
                AttachKnownAccounts();
            }
            else if (State == State.Terminated)
            {
                Account.AccountStatusUpdate -= OnAccountStatusUpdate;
                DetachKnownAccounts();
                BeelzebubReadOnlyOutbound.Shutdown();
                BeelzebubReadOnlyOutbound.Diagnostic("ADDON_TERMINATED");
            }
        }

        // A visible Control Center entry gives the operator an explicit,
        // read-only way to retry account discovery after changing local config.
        // It neither opens nor controls an order entry surface.
        protected override void OnWindowCreated(Window window)
        {
            ControlCenter controlCenter = window as ControlCenter;
            if (controlCenter == null)
                return;

            controlCenterNewMenu = controlCenter.FindFirst("ControlCenterMenuItemNew") as NTMenuItem;
            if (controlCenterNewMenu == null || attachMenuItem != null)
                return;

            attachMenuItem = new NTMenuItem
            {
                Header = "Beelzebub Read-Only Observer",
                Style = Application.Current.TryFindResource("MainMenuItem") as Style
            };
            controlCenterNewMenu.Items.Add(attachMenuItem);
            attachMenuItem.Click += OnAttachMenuItemClick;
            BeelzebubReadOnlyOutbound.Diagnostic("CONTROL_CENTER_MENU_READY");
        }

        protected override void OnWindowDestroyed(Window window)
        {
            if (attachMenuItem == null || !(window is ControlCenter))
                return;

            if (controlCenterNewMenu != null && controlCenterNewMenu.Items.Contains(attachMenuItem))
                controlCenterNewMenu.Items.Remove(attachMenuItem);
            attachMenuItem.Click -= OnAttachMenuItemClick;
            attachMenuItem = null;
            controlCenterNewMenu = null;
        }

        private void OnAttachMenuItemClick(object sender, RoutedEventArgs e)
        {
            BeelzebubReadOnlyOutbound.Diagnostic("MANUAL_ATTACH_REQUESTED");
            AttachKnownAccounts();
        }

        // Connection/account lifecycle notifications make late account
        // availability discoverable without a polling loop. AttachKnownAccounts
        // is idempotent, so duplicate platform notifications do not duplicate
        // handlers.
        private void OnAccountStatusUpdate(object sender, AccountStatusEventArgs e)
        {
            BeelzebubReadOnlyOutbound.Diagnostic("ACCOUNT_STATUS_UPDATE");
            AttachKnownAccounts();
        }

        private void AttachKnownAccounts()
        {
            string configuredId = ResolveConfiguredAccountId();
            if (String.IsNullOrWhiteSpace(configuredId))
            {
                BeelzebubReadOnlyOutbound.Diagnostic("ACCOUNT_BINDING_UNRESOLVED");
                BeelzebubReadOnlyOutbound.Publish("HEALTH", null, null,
                    "{\"state\":\"ACCOUNT_BINDING_UNRESOLVED\"}");
                return;
            }
            int matched = 0;
            List<Account> newlyAttached = new List<Account>();
            lock (Account.All)
            {
                foreach (Account account in Account.All)
                {
                    bool isLucid = String.Equals(account.Name, configuredId, StringComparison.Ordinal);
                    bool isSim101 = String.Equals(account.Name, "Sim101", StringComparison.Ordinal);
                    if (!isLucid && !isSim101)
                        continue;
                    matched++;
                    if (observedAccounts.Contains(account))
                        continue;
                    observedAccounts.Add(account);
                    account.AccountItemUpdate += OnAccountItemUpdate;
                    account.ExecutionUpdate += OnExecutionUpdate;
                    account.OrderUpdate += OnOrderUpdate;
                    account.PositionUpdate += OnPositionUpdate;
                    newlyAttached.Add(account);
                }
            }
            foreach (Account account in newlyAttached)
            {
                bool isLucid = String.Equals(account.Name, configuredId, StringComparison.Ordinal);
                BeelzebubReadOnlyOutbound.Publish("CONNECTION", Alias(account, isLucid), Class(isLucid),
                    "{\"state\":\"ACCOUNT_BOUND\"}");
                PublishAccountBalanceSnapshot(account, isLucid);
                PublishAccountStateSnapshot(account, isLucid);
            }
            BeelzebubReadOnlyOutbound.Diagnostic("ACCOUNT_DISCOVERY_MATCH_COUNT_" + matched);
        }

        private void DetachKnownAccounts()
        {
            foreach (Account account in observedAccounts)
            {
                account.AccountItemUpdate -= OnAccountItemUpdate;
                account.ExecutionUpdate -= OnExecutionUpdate;
                account.OrderUpdate -= OnOrderUpdate;
                account.PositionUpdate -= OnPositionUpdate;
            }
            observedAccounts.Clear();
        }

        // A completed collection scan is authoritative state at the NinjaTrader
        // account boundary. It is read-only and emits a completion marker even
        // when there are no positions or working orders.
        private void PublishAccountStateSnapshot(Account account, bool isLucid)
        {
            try
            {
                int positionCount = 0;
                lock (account.Positions)
                {
                    foreach (Position position in account.Positions)
                    {
                        positionCount++;
                        string contract = position.Instrument == null ? "" : position.Instrument.FullName;
                        BeelzebubReadOnlyOutbound.Publish("POSITION", Alias(account, isLucid), Class(isLucid),
                            "{\"contract_id\":\"" + Escape(contract) + "\",\"quantity\":" + position.Quantity + ",\"direction\":\"" +
                            Escape(position.MarketPosition.ToString()) + "\",\"average_price\":" + position.AveragePrice.ToString(CultureInfo.InvariantCulture) + "}");
                    }
                }
                BeelzebubReadOnlyOutbound.Publish("SNAPSHOT_COMPLETE", Alias(account, isLucid), Class(isLucid),
                    "{\"scope\":\"POSITION\",\"open_count\":" + positionCount + "}");

                int workingOrderCount = 0;
                lock (account.Orders)
                {
                    foreach (Order order in account.Orders)
                    {
                        if (Order.IsTerminalState(order.OrderState))
                            continue;
                        workingOrderCount++;
                        string contract = order.Instrument == null ? "" : order.Instrument.FullName;
                        BeelzebubReadOnlyOutbound.Publish("ORDER", Alias(account, isLucid), Class(isLucid),
                            "{\"native_order_id\":\"" + Escape(order.OrderId) + "\",\"contract_id\":\"" + Escape(contract) + "\",\"status\":\"" +
                            Escape(order.OrderState.ToString()) + "\",\"quantity\":" + order.Quantity + ",\"filled_quantity\":" + order.Filled + "}");
                    }
                }
                BeelzebubReadOnlyOutbound.Publish("SNAPSHOT_COMPLETE", Alias(account, isLucid), Class(isLucid),
                    "{\"scope\":\"ORDER\",\"working_count\":" + workingOrderCount + "}");
            }
            catch (Exception error)
            {
                BeelzebubReadOnlyOutbound.Diagnostic("ACCOUNT_SNAPSHOT_FAILED_" + error.GetType().Name);
            }
        }

        // AccountItemUpdate is event-driven and may not fire immediately for
        // a quiet account. Emit the current read-only values at attachment so
        // the local console does not display a synthetic paper balance.
        private void PublishAccountBalanceSnapshot(Account account, bool isLucid)
        {
            try
            {
                PublishAccountItem(account, isLucid, AccountItem.CashValue);
                PublishAccountItem(account, isLucid, AccountItem.NetLiquidation);
                PublishAccountItem(account, isLucid, AccountItem.RealizedProfitLoss);
                PublishAccountItem(account, isLucid, AccountItem.UnrealizedProfitLoss);
            }
            catch (Exception error)
            {
                BeelzebubReadOnlyOutbound.Diagnostic("ACCOUNT_BALANCE_SNAPSHOT_FAILED_" + error.GetType().Name);
            }
        }

        private static void PublishAccountItem(Account account, bool isLucid, AccountItem item)
        {
            BeelzebubReadOnlyOutbound.Publish("ACCOUNT", Alias(account, isLucid), Class(isLucid),
                "{\"item\":\"" + Escape(item.ToString()) + "\",\"value\":" +
                account.Get(item, Currency.UsDollar).ToString(CultureInfo.InvariantCulture) + "}");
        }

        private static string Alias(Account account, bool isLucid)
        {
            return isLucid ? "Lucid25kflex01" : "Sim101";
        }

        private static string Class(bool isLucid)
        {
            return isLucid ? "PROVIDER_EVALUATION" : "LOCAL_SIMULATION";
        }

        // Environment is preferred. This local, user-owned fallback exists
        // because NinjaTrader may have started before an environment change.
        // It is outside the repository and its contents never cross the bridge.
        private static string ResolveConfiguredAccountId()
        {
            string value = Environment.GetEnvironmentVariable("L3F_NT_LUCID_ACCOUNT_ID");
            if (!String.IsNullOrWhiteSpace(value))
                return value.Trim();
            string path = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "NinjaTrader 8", "l3f2.local.config");
            try
            {
                foreach (string line in File.ReadAllLines(path))
                {
                    const string prefix = "L3F_NT_LUCID_ACCOUNT_ID=";
                    if (line.StartsWith(prefix, StringComparison.Ordinal))
                        return line.Substring(prefix.Length).Trim();
                }
            }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
            return null;
        }

        private void OnAccountItemUpdate(object sender, AccountItemEventArgs e)
        {
            Account account = sender as Account;
            if (account == null) return;
            bool isLucid = !String.Equals(account.Name, "Sim101", StringComparison.Ordinal);
            BeelzebubReadOnlyOutbound.Publish("ACCOUNT", Alias(account, isLucid), Class(isLucid),
                "{\"item\":\"" + Escape(e.AccountItem.ToString()) + "\",\"value\":" +
                e.Value.ToString(CultureInfo.InvariantCulture) + "}", e.Time);
        }

        private void OnExecutionUpdate(object sender, ExecutionEventArgs e)
        {
            Account account = sender as Account;
            if (account == null) return;
            bool isLucid = !String.Equals(account.Name, "Sim101", StringComparison.Ordinal);
            BeelzebubReadOnlyOutbound.Publish("EXECUTION", Alias(account, isLucid), Class(isLucid),
                "{\"native_execution_id\":\"" + Escape(e.Execution.ExecutionId) + "\",\"price\":" +
                e.Execution.Price.ToString(CultureInfo.InvariantCulture) + ",\"quantity\":" +
                e.Execution.Quantity.ToString(CultureInfo.InvariantCulture) + "}", e.Execution.Time);
        }

        private void OnOrderUpdate(object sender, OrderEventArgs e)
        {
            Account account = sender as Account;
            if (account == null) return;
            bool isLucid = !String.Equals(account.Name, "Sim101", StringComparison.Ordinal);
            Order order = e.Order;
            BeelzebubReadOnlyOutbound.Publish("ORDER", Alias(account, isLucid), Class(isLucid),
                "{\"native_order_id\":\"" + Escape(order.OrderId) + "\",\"status\":\"" +
                Escape(order.OrderState.ToString()) + "\",\"quantity\":" + order.Quantity +
                ",\"filled_quantity\":" + order.Filled + "}", e.Time);
        }

        private void OnPositionUpdate(object sender, PositionEventArgs e)
        {
            Account account = sender as Account;
            if (account == null) return;
            bool isLucid = !String.Equals(account.Name, "Sim101", StringComparison.Ordinal);
            Position position = e.Position;
            BeelzebubReadOnlyOutbound.Publish("POSITION", Alias(account, isLucid), Class(isLucid),
                "{\"quantity\":" + position.Quantity + ",\"direction\":\"" +
                Escape(position.MarketPosition.ToString()) + "\",\"average_price\":" +
                position.AveragePrice.ToString(CultureInfo.InvariantCulture) + "}");
        }

        private static string Escape(string value) { return value.Replace("\\", "\\\\").Replace("\"", "\\\""); }
    }

    // Outbound-only loopback sink. It opens a local client connection and only
    // writes one observation frame. There is intentionally no stream Read.
    public static class BeelzebubReadOnlyOutbound
    {
        private const int Port = 48135;
        private const int MaximumQueuedFrames = 20000;
        private static long sequence = 0;
        private static readonly string sessionId = Guid.NewGuid().ToString("N");
        private static readonly HashSet<string> transportMarkers = new HashSet<string>();
        private static readonly object transportMarkersLock = new object();
        private static readonly Queue<string> outboundFrames = new Queue<string>();
        private static readonly object outboundFramesLock = new object();
        private static Thread senderThread;
        private static bool senderRunning;
        private static bool senderStopping;

        public static string Publish(string type, string alias, string accountClass, string payload, DateTime? providerTime = null)
        {
            TransportDiagnosticOnce("BRIDGE_PUBLISH_ATTEMPT_" + type);
            lock (outboundFramesLock)
            {
                // Sequence assignment and enqueueing must share one lock:
                // market-data callbacks may otherwise receive numbers in one
                // thread order but enter the outbound queue in another.
                long number = ++sequence;
                string observationId = "nt-" + sessionId + "-" + number;
                string account = alias == null ? "null" : "{\"alias\":\"" + alias + "\",\"class\":\"" + accountClass + "\"}";
                string timestamp = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture);
                string providerTimestamp = providerTime.HasValue ? providerTime.Value.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture) : "null";
                string frame = "{\"schema\":\"lane-iii-phase-f2-ninjatrader-observation-v1\",\"observation_id\":\"" + observationId +
                    "\",\"session_id\":\"" + sessionId + "\",\"observation_type\":\"" + type + "\",\"ninja_receipt_time\":\"" + timestamp +
                    "\",\"local_monotonic_sequence\":" + number + ",\"provider_timestamp\":" + (providerTimestamp == "null" ? "null" : "\"" + providerTimestamp + "\"") +
                    ",\"provider_sequence\":null,\"exchange_timestamp\":null,\"account\":" + account + ",\"payload\":" + payload + "}\n";
                EnqueueLocked(frame);
                return observationId;
            }
        }

        // Serialize all writes through one localhost-only sender. Market data
        // callbacks can be concurrent; opening one socket per callback permits
        // frame reordering and can exhaust local ephemeral ports.
        private static void EnqueueLocked(string frame)
        {
            if (outboundFrames.Count >= MaximumQueuedFrames)
            {
                TransportDiagnosticOnce("BRIDGE_QUEUE_CAPACITY_REACHED");
                return;
            }
            outboundFrames.Enqueue(frame);
            senderStopping = false;
            if (!senderRunning)
            {
                senderRunning = true;
                senderThread = new Thread(SenderLoop);
                senderThread.IsBackground = true;
                senderThread.Name = "BeelzebubReadOnlyOutbound";
                senderThread.Start();
            }
            Monitor.Pulse(outboundFramesLock);
        }

        private static void SenderLoop()
        {
            TcpClient client = null;
            NetworkStream stream = null;
            try
            {
                while (true)
                {
                    string frame;
                    lock (outboundFramesLock)
                    {
                        while (!senderStopping && outboundFrames.Count == 0)
                            Monitor.Wait(outboundFramesLock);
                        if (senderStopping)
                            return;
                        frame = outboundFrames.Dequeue();
                    }
                    try
                    {
                        if (client == null)
                        {
                            client = new TcpClient();
                            client.Connect(IPAddress.Loopback, Port);
                            stream = client.GetStream();
                            TransportDiagnosticOnce("BRIDGE_CHANNEL_CONNECTED");
                        }
                        byte[] bytes = Encoding.UTF8.GetBytes(frame);
                        stream.Write(bytes, 0, bytes.Length);
                        TransportDiagnosticOnce("BRIDGE_PUBLISH_DELIVERED");
                    }
                    catch (SocketException error)
                    {
                        TransportDiagnosticOnce("BRIDGE_CONNECT_FAILED_" + error.SocketErrorCode);
                        CloseClient(client);
                        client = null;
                        stream = null;
                    }
                    catch (Exception error)
                    {
                        TransportDiagnosticOnce("BRIDGE_PUBLISH_FAILED_" + error.GetType().Name);
                        CloseClient(client);
                        client = null;
                        stream = null;
                    }
                }
            }
            finally
            {
                CloseClient(client);
                lock (outboundFramesLock)
                {
                    senderRunning = false;
                    senderThread = null;
                }
            }
        }

        public static void Shutdown()
        {
            lock (outboundFramesLock)
            {
                senderStopping = true;
                outboundFrames.Clear();
                Monitor.PulseAll(outboundFramesLock);
            }
        }

        private static void CloseClient(TcpClient client)
        {
            if (client == null)
                return;
            try { client.Close(); }
            catch (ObjectDisposedException) { }
        }

        // Transport diagnostics are deliberately one-shot per observation type.
        // They expose no payload, account identifier, credential, or endpoint.
        private static void TransportDiagnosticOnce(string marker)
        {
            lock (transportMarkersLock)
            {
                if (!transportMarkers.Add(marker))
                    return;
            }
            Diagnostic(marker);
        }

        // Output diagnostics carry no provider identity, credential, token, or
        // account value. They are solely for the NinjaTrader Output window.
        public static void Diagnostic(string marker)
        {
            System.Diagnostics.Trace.WriteLine("L3F2 " + marker);
        }
    }
}
