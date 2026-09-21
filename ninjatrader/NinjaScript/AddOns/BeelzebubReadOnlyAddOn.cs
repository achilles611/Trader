// L3-F2 observation-only NinjaTrader 8 AddOn source.
// Install/compile only through NinjaTrader's NinjaScript editor. This source
// intentionally contains no order, ATM, or inbound bridge command surface.
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Web.Script.Serialization;
using System.Windows;
using NinjaTrader.Cbi;
using NinjaTrader.Data;
using NinjaTrader.Gui;
using NinjaTrader.Gui.Tools;
using NinjaTrader.NinjaScript.AddOns;

namespace NinjaTrader.NinjaScript.AddOns
{
    // BEELZEBUB_MARKET_CALLBACK_GATE_BEGIN
    // This dependency-free gate is also compiled by the deterministic producer
    // interleaving regression. The real callback path uses it below.
    public sealed class BeelzebubMarketCallbackGate
    {
        private readonly object sync = new object();
        private long generation = 1;
        private bool active = true;

        public long Generation
        {
            get { lock (sync) return generation; }
        }

        public bool IsActive
        {
            get { lock (sync) return active; }
        }

        public bool TryRun(long expectedGeneration, Action callback)
        {
            if (callback == null)
                throw new ArgumentNullException("callback");
            lock (sync)
            {
                if (!active || expectedGeneration != generation)
                    return false;
                // The callback captures provider state, assigns its stream
                // sequence, signs, and admits to the bounded memory queue. It
                // must contain no socket, disk, dispatcher, or other wait.
                callback();
                return true;
            }
        }

        public void Retire()
        {
            lock (sync)
            {
                active = false;
                generation++;
            }
        }
    }
    // One authorized publisher across native replacement and chart fallback.
    // Locally assigned counters belong to the publisher session, not instances.
    public static class BeelzebubMarketPublisherLease
    {
        private static readonly object sync = new object();
        private static long generation;
        private static long owner;
        private static long trade;
        private static long quote;
        private static long depth;
        [ThreadStatic] private static long current;
        public static long Claim(bool native)
        {
            lock (sync)
            {
                if (!native && owner != 0) return 0;
                owner = ++generation;
                return owner;
            }
        }
        public static bool TryRun(long token, Action callback)
        {
            lock (sync)
            {
                if (token == 0 || token != owner) return false;
                current = token;
                try { callback(); } finally { current = 0; }
                return true;
            }
        }
        public static void Retire(long token)
        {
            lock (sync) { if (token == owner) owner = 0; }
        }
        public static long CurrentGeneration { get { return current; } }
        public static long Next(string stream)
        {
            if (current == 0) throw new InvalidOperationException("Market publisher lease required");
            if (stream == "TRADE") return ++trade;
            if (stream == "QUOTE") return ++quote;
            if (stream == "DEPTH") return ++depth;
            throw new ArgumentException("Unknown local sequence domain");
        }
    }
    // BEELZEBUB_MARKET_CALLBACK_GATE_END

    public sealed class BeelzebubReadOnlyAddOn : AddOnBase
    {
        private readonly List<Account> observedAccounts = new List<Account>();
        private readonly List<NinjaTrader.Gui.Chart.Chart> observedCharts = new List<NinjaTrader.Gui.Chart.Chart>();
        private NTMenuItem controlCenterNewMenu;
        private NTMenuItem attachMenuItem;
        private BeelzebubAutomaticMarketObserver automaticObserver;
        private bool addonActive;
        private static int automaticObserverActive;
        private const int MaximumObserverRecoveryAttempts = 3;
        private static readonly TimeSpan ObserverRecoveryInterval = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan ObserverCallbackStaleAfter = TimeSpan.FromSeconds(15);
        private Timer observerRecoveryTimer;
        private int observerRecoveryCheckQueued;
        private int observerRecoveryAttempts;
        private bool observerRecoveryExhaustedReported;

        public static bool AutomaticObserverActive
        {
            get { return Volatile.Read(ref automaticObserverActive) == 1; }
        }

        protected override void OnStateChange()
        {
            if (State == State.Active)
            {
                addonActive = true;
                Account.AccountStatusUpdate += OnAccountStatusUpdate;
                BeelzebubReadOnlyOutbound.Diagnostic("ADDON_ACTIVE");
                AttachKnownAccounts();
                EnsureAutomaticObserver();
                StartObserverRecoveryMonitor();
            }
            else if (State == State.Terminated)
            {
                addonActive = false;
                StopObserverRecoveryMonitor();
                Account.AccountStatusUpdate -= OnAccountStatusUpdate;
                DetachAutomaticObserver();
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
            NinjaTrader.Gui.Chart.Chart chart = window as NinjaTrader.Gui.Chart.Chart;
            if (chart != null)
            {
                lock (observedCharts)
                    if (!observedCharts.Contains(chart))
                        observedCharts.Add(chart);
                InspectCharts(true);
                return;
            }

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
            NinjaTrader.Gui.Chart.Chart chart = window as NinjaTrader.Gui.Chart.Chart;
            if (chart != null)
            {
                lock (observedCharts)
                    observedCharts.Remove(chart);
                InspectCharts(false);
                return;
            }
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
            BeelzebubReadOnlyOutbound.Diagnostic("OBSERVER_RECHECK_REQUESTED");
            observerRecoveryAttempts = 0;
            observerRecoveryExhaustedReported = false;
            AttachKnownAccounts();
            // The operator's explicit recheck must recover a stale native
            // subscription.  Merely ensuring the existing observer cannot do
            // that because a previously created observer remains non-null
            // even when its provider callbacks have stopped advancing.
            DetachAutomaticObserver();
            EnsureAutomaticObserver();
            InspectCharts(true);
        }

        // A chart is optional. The observer uses NinjaTrader's supported AddOn
        // MarketData/MarketDepth subscriptions and merely focuses an already
        // open matching chart as a convenience.
        private void InspectCharts(bool focusCorrectChart)
        {
            if (!addonActive)
                return;
            string configured = ResolveConfiguredInstrument();
            if (String.IsNullOrWhiteSpace(configured))
            {
                PublishObserverAttachment("CONFIGURED_INSTRUMENT_UNRESOLVED", null, null, false, false, "NATIVE_ADDON");
                return;
            }
            NinjaTrader.Gui.Chart.Chart correct = null;
            string firstInstrument = null;
            NinjaTrader.Gui.Chart.Chart[] charts;
            lock (observedCharts)
                charts = observedCharts.ToArray();
            foreach (NinjaTrader.Gui.Chart.Chart chart in charts)
            {
                string instrument = ChartInstrument(chart);
                if (String.IsNullOrWhiteSpace(instrument))
                    continue;
                if (firstInstrument == null)
                    firstInstrument = instrument;
                if (String.Equals(instrument, configured, StringComparison.Ordinal))
                {
                    correct = chart;
                    break;
                }
            }
            if (correct == null)
            {
                if (automaticObserver != null)
                    automaticObserver.PublishAttachment(false);
                return;
            }
            if (focusCorrectChart)
                ActivateChart(correct);
            if (automaticObserver != null)
                automaticObserver.PublishAttachment(true);
        }

        private static string ChartInstrument(NinjaTrader.Gui.Chart.Chart chart)
        {
            if (chart == null)
                return null;
            Func<string> resolve = () =>
            {
                if (chart.ActiveChartControl == null || chart.ActiveChartControl.Instrument == null)
                    return null;
                return chart.ActiveChartControl.Instrument.FullName;
            };
            try
            {
                if (chart.Dispatcher.CheckAccess())
                    return resolve();
                return chart.Dispatcher.Invoke(resolve);
            }
            catch (InvalidOperationException)
            {
                return null;
            }
        }

        private static void ActivateChart(NinjaTrader.Gui.Chart.Chart chart)
        {
            if (chart == null)
                return;
            Action activate = () =>
            {
                try { chart.Activate(); }
                catch (InvalidOperationException) { }
            };
            try
            {
                if (chart.Dispatcher.CheckAccess())
                    activate();
                else
                    chart.Dispatcher.BeginInvoke(activate);
            }
            catch (InvalidOperationException) { }
        }

        private void EnsureAutomaticObserver()
        {
            if (!addonActive || automaticObserver != null)
                return;
            string configured = ResolveConfiguredInstrument();
            if (String.IsNullOrWhiteSpace(configured))
            {
                PublishObserverAttachment("CONFIGURED_INSTRUMENT_UNRESOLVED", null, null, false, false, "NATIVE_ADDON");
                return;
            }
            Instrument instrument = Instrument.GetInstrument(configured);
            if (instrument == null)
            {
                PublishObserverAttachment("NATIVE_ADDON_OBSERVER_FAILED", configured, null, false, false, "NATIVE_ADDON");
                return;
            }
            BeelzebubAutomaticMarketObserver candidate =
                new BeelzebubAutomaticMarketObserver(instrument);
            automaticObserver = candidate;
            if (!candidate.Start(() => addonActive && ReferenceEquals(automaticObserver, candidate)))
            {
                if (ReferenceEquals(automaticObserver, candidate))
                    automaticObserver = null;
                candidate.Stop();
                PublishObserverAttachment("NATIVE_ADDON_OBSERVER_FAILED", configured, instrument.FullName, false, false, "NATIVE_ADDON");
            }
        }

        private void StartObserverRecoveryMonitor()
        {
            if (observerRecoveryTimer != null)
                return;
            observerRecoveryTimer = new Timer(
                _ => QueueObserverRecoveryCheck(), null,
                ObserverRecoveryInterval, ObserverRecoveryInterval);
        }

        private void StopObserverRecoveryMonitor()
        {
            Timer current = observerRecoveryTimer;
            observerRecoveryTimer = null;
            if (current != null)
                current.Dispose();
            Interlocked.Exchange(ref observerRecoveryCheckQueued, 0);
        }

        private void QueueObserverRecoveryCheck()
        {
            if (!addonActive || Interlocked.Exchange(ref observerRecoveryCheckQueued, 1) != 0)
                return;
            try
            {
                if (Application.Current == null || Application.Current.Dispatcher.HasShutdownStarted)
                {
                    Interlocked.Exchange(ref observerRecoveryCheckQueued, 0);
                    return;
                }
                Application.Current.Dispatcher.BeginInvoke(new Action(() =>
                {
                    try { CheckObserverRecovery(); }
                    finally { Interlocked.Exchange(ref observerRecoveryCheckQueued, 0); }
                }));
            }
            catch (InvalidOperationException)
            {
                Interlocked.Exchange(ref observerRecoveryCheckQueued, 0);
            }
        }

        private void CheckObserverRecovery()
        {
            if (!addonActive)
                return;
            BeelzebubAutomaticMarketObserver current = automaticObserver;
            if (current != null && !current.NeedsRecovery(ObserverCallbackStaleAfter))
            {
                observerRecoveryAttempts = 0;
                observerRecoveryExhaustedReported = false;
                return;
            }
            if (observerRecoveryAttempts >= MaximumObserverRecoveryAttempts)
            {
                if (!observerRecoveryExhaustedReported)
                {
                    observerRecoveryExhaustedReported = true;
                    BeelzebubReadOnlyOutbound.Diagnostic("NATIVE_ADDON_OBSERVER_RECOVERY_EXHAUSTED");
                    PublishObserverAttachment(
                        "OBSERVER_RECOVERY_EXHAUSTED", ResolveConfiguredInstrument(),
                        null, false, false, "NATIVE_ADDON");
                }
                return;
            }
            observerRecoveryAttempts++;
            BeelzebubReadOnlyOutbound.Diagnostic(
                "NATIVE_ADDON_OBSERVER_RECOVERY_ATTEMPT_" + observerRecoveryAttempts);
            DetachAutomaticObserver();
            EnsureAutomaticObserver();
            InspectCharts(false);
        }

        private void DetachAutomaticObserver()
        {
            BeelzebubAutomaticMarketObserver current = automaticObserver;
            automaticObserver = null;
            if (current != null)
                current.Stop();
            Interlocked.Exchange(ref automaticObserverActive, 0);
        }

        internal static void SetAutomaticObserverActive(bool active)
        {
            Interlocked.Exchange(ref automaticObserverActive, active ? 1 : 0);
        }

        public static void PublishObserverAttachment(
            string state, string configuredInstrument, string instrument,
            bool chartFound, bool observerAttached)
        {
            PublishObserverAttachment(state, configuredInstrument, instrument, chartFound, observerAttached, "CHART_INDICATOR");
        }

        public static void PublishObserverAttachment(
            string state, string configuredInstrument, string instrument,
            bool chartFound, bool observerAttached, string subscriptionMode)
        {
            string configured = configuredInstrument == null ? "null" : "\"" + Escape(configuredInstrument) + "\"";
            string actual = instrument == null ? "null" : "\"" + Escape(instrument) + "\"";
            BeelzebubReadOnlyOutbound.Publish("HEALTH", null, null,
                "{\"component\":\"MARKET_OBSERVER_ATTACHMENT\",\"state\":\"" + Escape(state)
                + "\",\"configured_instrument\":" + configured + ",\"instrument\":" + actual
                + ",\"chart_found\":" + (chartFound ? "true" : "false")
                + ",\"observer_attached\":" + (observerAttached ? "true" : "false")
                + ",\"subscription_mode\":\"" + Escape(subscriptionMode) + "\"}");
        }

        // Connection/account lifecycle notifications make late account
        // availability discoverable without a polling loop. AttachKnownAccounts
        // is idempotent, so duplicate platform notifications do not duplicate
        // handlers.
        private void OnAccountStatusUpdate(object sender, AccountStatusEventArgs e)
        {
            BeelzebubReadOnlyOutbound.Diagnostic("ACCOUNT_STATUS_UPDATE");
            AttachKnownAccounts();
            EnsureAutomaticObserver();
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

        public static string ResolveConfiguredInstrument()
        {
            string value = Environment.GetEnvironmentVariable("L3F_NT_MARKET_INSTRUMENT");
            if (!String.IsNullOrWhiteSpace(value))
                return value.Trim();
            string path = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments),
                "NinjaTrader 8", "beelzebub-observer.local.config");
            try
            {
                foreach (string line in File.ReadAllLines(path))
                {
                    const string prefix = "L3F_NT_MARKET_INSTRUMENT=";
                    if (line.StartsWith(prefix, StringComparison.Ordinal))
                        return line.Substring(prefix.Length).Trim();
                }
            }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
            return null;
        }

        public static string ResolveCanonicalContract(string nativeInstrument)
        {
            if (String.Equals(nativeInstrument, "MNQ SEP26", StringComparison.Ordinal))
                return "MNQU6";
            if (String.Equals(nativeInstrument, "MNQ DEC26", StringComparison.Ordinal))
                return "MNQZ6";
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

    // Supported, chart-independent AddOn subscriptions. This object is
    // observation-only: it has no Account, order, ATM, strategy, or inbound
    // command reference. One instance owns both subscriptions for exactly the
    // configured MNQ contract and unsubscribes on the Instrument dispatcher.
    internal sealed class BeelzebubAutomaticMarketObserver
    {
        private const int MaximumPublishedBookLevelsPerSide = 10;
        private static readonly long MinimumPublicationTicks = Math.Max(1L, System.Diagnostics.Stopwatch.Frequency / 2L);
        private static readonly long MinimumAttachmentPublicationTicks = Math.Max(1L, System.Diagnostics.Stopwatch.Frequency * 5L);
        private const string QuotePublicationPolicy = "BOUNDED_LATEST_STATE_2HZ";
        private const string PairedQuotePublicationPolicy = "EVENT_DRIVEN_SAME_CALLBACK_QUOTE_V1";
        private const string TradePublicationPolicy = "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1";
        private const string DepthPublicationPolicy = "EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1";
        private readonly Instrument instrument;
        private readonly object bookLock = new object();
        private readonly BeelzebubMarketCallbackGate marketDataGate =
            new BeelzebubMarketCallbackGate();
        private readonly long marketDataGeneration = 1;
        private readonly long publisherLease;
        // This is the authentic native positional working cursor, not a
        // last-published display book. Every interpretable callback advances
        // it even when the resulting snapshot is crossed or partial and must
        // be withheld from available publication.
        private readonly SortedDictionary<int, KeyValuePair<double, long>> nativeDepthWorkingBookBids =
            new SortedDictionary<int, KeyValuePair<double, long>>();
        private readonly SortedDictionary<int, KeyValuePair<double, long>> nativeDepthWorkingBookAsks =
            new SortedDictionary<int, KeyValuePair<double, long>>();
        private bool depthContinuityBroken;
        private bool depthPositionDisclosurePending;
        private readonly long startedTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        private long lastMarketCallbackTicks;
        private bool subscribed;
        private volatile bool stopping;
        private bool chartFound;
        private bool reportedLevelOne;
        private bool reportedDepth;
        private bool reportedMarketDataConnected;
        private double bestBid = Double.NaN;
        private double bestAsk = Double.NaN;
        private long bestBidSize;
        private long bestAskSize;
        private DateTime bestBidTime = DateTime.MinValue;
        private DateTime bestAskTime = DateTime.MinValue;
        private long lastQuotePublicationTicks;
        private long lastAttachmentPublicationTicks;
        private long quoteEventSequence;
        private long tradeEventSequence;
        private long depthEventSequence;
        private BarsRequest hourlyBarsRequest;
        private BarsRequest volumeBarsRequest;
        private readonly object hourlyLock = new object();
        private readonly HashSet<string> publishedHourlyEnds = new HashSet<string>(StringComparer.Ordinal);

        public BeelzebubAutomaticMarketObserver(Instrument instrument)
        {
            this.instrument = instrument;
            publisherLease = BeelzebubMarketPublisherLease.Claim(true);
        }

        public bool Start(Func<bool> activationCurrent)
        {
            if (activationCurrent == null || instrument == null
                || instrument.Dispatcher == null || instrument.Dispatcher.HasShutdownStarted)
                return false;
            try
            {
                instrument.Dispatcher.InvokeAsync(() =>
                {
                    if (stopping || subscribed || !activationCurrent())
                        return;
                    try
                    {
                        instrument.MarketData.Update += OnMarketData;
                        instrument.MarketDepth.Update += OnMarketDepth;
                        StartHourlyBarsRequest();
                        StartVolumeReferenceRequest();
                        subscribed = true;
                        BeelzebubReadOnlyAddOn.SetAutomaticObserverActive(true);
                        BeelzebubReadOnlyOutbound.Diagnostic("NATIVE_ADDON_OBSERVER_ACTIVE");
                        PublishAttachment(false, true);
                    }
                    catch (Exception error)
                    {
                        subscribed = false;
                        BeelzebubReadOnlyAddOn.SetAutomaticObserverActive(false);
                        BeelzebubReadOnlyOutbound.Diagnostic("NATIVE_ADDON_OBSERVER_FAILED_" + error.GetType().Name);
                        BeelzebubReadOnlyAddOn.PublishObserverAttachment(
                            "NATIVE_ADDON_OBSERVER_FAILED", BeelzebubReadOnlyAddOn.ResolveConfiguredInstrument(),
                            instrument.FullName, false, false, "NATIVE_ADDON");
                    }
                });
                return true;
            }
            catch (Exception error)
            {
                BeelzebubReadOnlyOutbound.Diagnostic("NATIVE_ADDON_OBSERVER_START_FAILED_" + error.GetType().Name);
                return false;
            }
        }

        public void Stop()
        {
            // Drain the currently admitted callback, invalidate this producer
            // generation, then unsubscribe. A queued callback from this older
            // instance can no longer publish after recovery creates its owner.
            marketDataGate.Retire();
            BeelzebubMarketPublisherLease.Retire(publisherLease);
            stopping = true;
            BeelzebubReadOnlyAddOn.SetAutomaticObserverActive(false);
            if (instrument == null || instrument.Dispatcher == null || instrument.Dispatcher.HasShutdownStarted)
                return;
            try
            {
                instrument.Dispatcher.InvokeAsync(() =>
                {
                    if (!subscribed)
                        return;
                    instrument.MarketData.Update -= OnMarketData;
                    instrument.MarketDepth.Update -= OnMarketDepth;
                    if (hourlyBarsRequest != null)
                    {
                        hourlyBarsRequest.Update -= OnHourlyBarsUpdate;
                        hourlyBarsRequest.Dispose();
                        hourlyBarsRequest = null;
                    }
                    if (volumeBarsRequest != null)
                    {
                        volumeBarsRequest.Dispose();
                        volumeBarsRequest = null;
                    }
                    subscribed = false;
                    BeelzebubReadOnlyAddOn.PublishObserverAttachment(
                        "OBSERVER_TERMINATED", BeelzebubReadOnlyAddOn.ResolveConfiguredInstrument(),
                        instrument.FullName, chartFound, false, "NATIVE_ADDON");
                });
            }
            catch (Exception error)
            {
                BeelzebubReadOnlyOutbound.Diagnostic("NATIVE_ADDON_OBSERVER_STOP_FAILED_" + error.GetType().Name);
            }
        }

        public void PublishAttachment(bool found)
        {
            chartFound = found;
            PublishAttachment(found, true);
        }

        private void PublishAttachment(bool found, bool force)
        {
            if (!subscribed && !force)
                return;
            if (!force && !TryReservePublication(ref lastAttachmentPublicationTicks, MinimumAttachmentPublicationTicks))
                return;
            if (force)
                Interlocked.Exchange(ref lastAttachmentPublicationTicks, System.Diagnostics.Stopwatch.GetTimestamp());
            string configured = BeelzebubReadOnlyAddOn.ResolveConfiguredInstrument();
            bool matches = !String.IsNullOrWhiteSpace(configured)
                && String.Equals(configured, instrument.FullName, StringComparison.Ordinal);
            BeelzebubReadOnlyAddOn.PublishObserverAttachment(
                matches ? "NATIVE_ADDON_OBSERVER_ACTIVE" : "WRONG_CHART_INSTRUMENT",
                configured, instrument.FullName, found, matches, "NATIVE_ADDON");
        }

        private const string HourlyWarmStartPolicyVersion = "pong-hourly-warm-start-v1";
        private const string Sep26HourlyWarmStartPolicyHash = "5a1c78a287d50b69e5dd6e8d9bc584ef5a264efd98845997fd08b7c85668e5c2";
        private const string Dec26HourlyWarmStartPolicyHash = "f26f65d7fee9a386afcf220da80f32d98639440117570c932341b83086920443";

        private string ResolveHourlyWarmStartPolicyHash()
        {
            if (String.Equals(instrument.FullName, "MNQ SEP26", StringComparison.Ordinal))
                return Sep26HourlyWarmStartPolicyHash;
            if (String.Equals(instrument.FullName, "MNQ DEC26", StringComparison.Ordinal))
                return Dec26HourlyWarmStartPolicyHash;
            return null;
        }

        private void StartVolumeReferenceRequest()
        {
            // Separate provider history: no aggressors, depth, or live-hour
            // authority. No contract merging, session interpolation or zeros.
            DateTime requestedAt = DateTime.UtcNow;
            string requestId = Guid.NewGuid().ToString("N");
            try
            {
                BarsRequest request = new BarsRequest(instrument, 2016);
                request.BarsPeriod = new BarsPeriod { BarsPeriodType = BarsPeriodType.Minute, Value = 5 };
                request.TradingHours = TradingHours.Get("Default 24 x 7");
                request.LookupPolicy = LookupPolicies.Provider;
                request.MergePolicy = MergePolicy.DoNotMerge;
                volumeBarsRequest = request;
                request.Request(new Action<BarsRequest, ErrorCode, string>((result, errorCode, errorMessage) =>
                {
                    if (stopping || errorCode != ErrorCode.NoError)
                    {
                        BeelzebubReadOnlyOutbound.Diagnostic("VOLUME_REFERENCE_REQUEST_UNAVAILABLE_" + errorCode);
                        return;
                    }
                    List<string> rows = new List<string>();
                    for (int index = 0; index < result.Bars.Count; index++)
                    {
                        DateTime end;
                        string rejection;
                        if (!BeelzebubProviderTimestamp.TryConvertUtc(result.Bars.GetTime(index),
                            Core.Globals.GeneralOptions.TimeZoneInfo, null, out end, out rejection)
                            || end.Minute % 5 != 0 || end.Second != 0 || end.Millisecond != 0
                            || end > requestedAt)
                            continue;
                        rows.Add("{\"start\":\"" + (end - TimeSpan.FromMinutes(5)).ToString("o", CultureInfo.InvariantCulture)
                            + "\",\"end\":\"" + end.ToString("o", CultureInfo.InvariantCulture)
                            + "\",\"volume\":" + result.Bars.GetVolume(index).ToString(CultureInfo.InvariantCulture) + "}");
                    }
                    if (rows.Count != 2016)
                    {
                        BeelzebubReadOnlyOutbound.Diagnostic("VOLUME_REFERENCE_INCOMPLETE_BAR_COUNT");
                        return;
                    }
                    int chunks = (rows.Count + 127) / 128;
                    for (int index = 0; index < chunks; index++)
                    {
                        string canonicalContract = BeelzebubReadOnlyAddOn.ResolveCanonicalContract(instrument.FullName);
                        if (String.IsNullOrWhiteSpace(canonicalContract))
                        {
                            BeelzebubReadOnlyOutbound.Diagnostic("VOLUME_REFERENCE_UNKNOWN_CONTRACT");
                            return;
                        }
                        string payload = "{\"contract_id\":\"" + instrument.FullName
                            + "\",\"canonical_contract\":\"" + canonicalContract + "\",\"source\":\"NINJATRADER_BARS_REQUEST_PROVIDER\""
                            + ",\"status\":\"COMPLETE\",\"merge_policy\":\"DO_NOT_MERGE\",\"trading_hours\":\"Default 24 x 7\""
                            + ",\"timezone\":\"UTC\",\"bar_period_minutes\":5,\"requested_bars_back\":2016"
                            + ",\"request_id\":\"" + requestId + "\",\"requested_at\":\""
                            + requestedAt.ToString("o", CultureInfo.InvariantCulture) + "\",\"chunk_index\":" + index
                            + ",\"chunk_count\":" + chunks + ",\"total_bars\":" + rows.Count
                            + ",\"bars\":[" + String.Join(",", rows.Skip(index * 128).Take(128))
                            + "],\"publication_policy\":\"PROVIDER_COMPLETED_UTC_5M_VOLUME_V1\"}";
                        if (!marketDataGate.TryRun(marketDataGeneration, delegate {
                            BeelzebubMarketPublisherLease.TryRun(publisherLease, delegate {
                                BeelzebubReadOnlyOutbound.Publish("VOLUME_REFERENCE_HISTORY", null, null, payload);
                            });
                        })) return;
                    }
                }));
            }
            catch (Exception error)
            {
                BeelzebubReadOnlyOutbound.Diagnostic("VOLUME_REFERENCE_REQUEST_FAILED_" + error.GetType().Name);
            }
        }

        private void StartHourlyBarsRequest()
        {
            try
            {
                DateTime requestedAt = DateTime.UtcNow;
                BarsRequest request = new BarsRequest(instrument, 6);
                request.BarsPeriod = new BarsPeriod { BarsPeriodType = BarsPeriodType.Minute, Value = 60 };
                request.TradingHours = TradingHours.Get("Default 24 x 7");
                request.LookupPolicy = LookupPolicies.Provider;
                request.MergePolicy = MergePolicy.DoNotMerge;
                request.Update += OnHourlyBarsUpdate;
                hourlyBarsRequest = request;
                request.Request(new Action<BarsRequest, ErrorCode, string>((result, errorCode, errorMessage) =>
                {
                    if (errorCode != ErrorCode.NoError)
                    {
                        BeelzebubReadOnlyOutbound.Diagnostic("HOURLY_WARM_START_REQUEST_FAILED_" + errorCode);
                        PublishHourlyWarmStartUnavailable(requestedAt, "BARS_REQUEST_FAILED");
                        return;
                    }
                    marketDataGate.TryRun(marketDataGeneration, delegate {
                        BeelzebubMarketPublisherLease.TryRun(publisherLease, delegate {
                            PublishHourlyWarmStart(result.Bars, requestedAt);
                        });
                    });
                }));
            }
            catch (Exception error)
            {
                BeelzebubReadOnlyOutbound.Diagnostic("HOURLY_WARM_START_REQUEST_FAILED_" + error.GetType().Name);
                PublishHourlyWarmStartUnavailable(DateTime.UtcNow, "BARS_REQUEST_UNAVAILABLE");
            }
        }

        private void PublishHourlyWarmStart(Bars bars, DateTime requestedAt)
        {
            DateTime boundary = new DateTime(requestedAt.Year, requestedAt.Month, requestedAt.Day, requestedAt.Hour, 0, 0, DateTimeKind.Utc);
            Dictionary<DateTime, string> matching = new Dictionary<DateTime, string>();
            for (int index = 0; index < bars.Count; index++)
            {
                DateTime end;
                if (!TryBarEndUtc(bars.GetTime(index), out end))
                    continue;
                if (end != boundary - TimeSpan.FromHours(1) && end != boundary)
                    continue;
                DateTime start = end - TimeSpan.FromHours(1);
                matching[end] = HourBarJson(
                    start, end, bars.GetOpen(index), bars.GetHigh(index), bars.GetLow(index),
                    bars.GetClose(index), bars.GetVolume(index));
            }
            List<string> rows = new List<string>();
            List<string> gaps = new List<string>();
            foreach (DateTime expectedEnd in new[] { boundary - TimeSpan.FromHours(1), boundary })
            {
                string row;
                if (matching.TryGetValue(expectedEnd, out row)) rows.Add(row);
                else gaps.Add("\"" + expectedEnd.ToString("o", CultureInfo.InvariantCulture) + "\"");
            }
            string status = rows.Count == 2 && gaps.Count == 0 ? "COMPLETE" : "UNAVAILABLE";
            string payload = "{\"contract_id\":\"" + instrument.FullName + "\",\"status\":\"" + status
                + "\",\"source\":\"NINJATRADER_BARS_REQUEST_PROVIDER\",\"requested_at\":\""
                + requestedAt.ToString("o", CultureInfo.InvariantCulture) + "\",\"earliest_required\":\""
                + (boundary - TimeSpan.FromHours(2)).ToString("o", CultureInfo.InvariantCulture)
                + "\",\"latest_included_timestamp\":\"" + boundary.ToString("o", CultureInfo.InvariantCulture)
                + "\",\"expected_intervals\":2,\"actual_intervals\":" + rows.Count
                + ",\"gaps\":[" + String.Join(",", gaps) + "],\"timezone\":\"UTC\",\"day_anchor\":\""
                + (boundary - TimeSpan.FromHours(2)).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
                + "\",\"policy_version\":\"" + HourlyWarmStartPolicyVersion + "\",\"policy_hash\":\""
                + ResolveHourlyWarmStartPolicyHash() + "\",\"bars\":[" + String.Join(",", rows)
                + "],\"publication_policy\":\"CAUSAL_HISTORICAL_COMPLETED_HOUR_V1\"}";
            string observationId = BeelzebubReadOnlyOutbound.Publish("HISTORICAL_WARM_START", null, null, payload);
            if (observationId != null && status == "COMPLETE")
                lock (hourlyLock)
                    foreach (DateTime end in matching.Keys) publishedHourlyEnds.Add(end.ToString("o", CultureInfo.InvariantCulture));
        }

        private void PublishHourlyWarmStartUnavailable(DateTime requestedAt, string reason)
        {
            DateTime boundary = new DateTime(requestedAt.Year, requestedAt.Month, requestedAt.Day, requestedAt.Hour, 0, 0, DateTimeKind.Utc);
            string payload = "{\"contract_id\":\"" + instrument.FullName + "\",\"status\":\"UNAVAILABLE\",\"source\":\"NINJATRADER_BARS_REQUEST_PROVIDER\",\"requested_at\":\""
                + requestedAt.ToString("o", CultureInfo.InvariantCulture) + "\",\"earliest_required\":\""
                + (boundary - TimeSpan.FromHours(2)).ToString("o", CultureInfo.InvariantCulture)
                + "\",\"latest_included_timestamp\":\"" + boundary.ToString("o", CultureInfo.InvariantCulture)
                + "\",\"expected_intervals\":2,\"actual_intervals\":0,\"gaps\":[\"" + reason
                + "\"],\"timezone\":\"UTC\",\"day_anchor\":\""
                + (boundary - TimeSpan.FromHours(2)).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
                + "\",\"policy_version\":\"" + HourlyWarmStartPolicyVersion + "\",\"policy_hash\":\""
                + ResolveHourlyWarmStartPolicyHash() + "\",\"bars\":[],\"publication_policy\":\"CAUSAL_HISTORICAL_COMPLETED_HOUR_V1\"}";
            BeelzebubReadOnlyOutbound.Publish("HISTORICAL_WARM_START", null, null, payload);
        }

        private void OnHourlyBarsUpdate(object sender, BarsUpdateEventArgs e)
        {
            marketDataGate.TryRun(marketDataGeneration, delegate {
                BeelzebubMarketPublisherLease.TryRun(publisherLease, delegate { PublishHourlyBarsUpdate(e); });
            });
        }

        private void PublishHourlyBarsUpdate(BarsUpdateEventArgs e)
        {
            DateTime now = DateTime.UtcNow;
            for (int index = e.MinIndex; index <= e.MaxIndex; index++)
            {
                DateTime end;
                if (!TryBarEndUtc(e.BarsSeries.GetTime(index), out end)
                    || end > now || now - end > TimeSpan.FromMinutes(5))
                    continue;
                string endIdentity = end.ToString("o", CultureInfo.InvariantCulture);
                lock (hourlyLock)
                    if (publishedHourlyEnds.Contains(endIdentity)) continue;
                string payload = HourBarJson(
                    end - TimeSpan.FromHours(1), end, e.BarsSeries.GetOpen(index), e.BarsSeries.GetHigh(index),
                    e.BarsSeries.GetLow(index), e.BarsSeries.GetClose(index), e.BarsSeries.GetVolume(index));
                payload = payload.Substring(0, payload.Length - 1)
                    + ",\"contract_id\":\"" + instrument.FullName
                    + "\",\"source\":\"NINJATRADER_BARS_REQUEST_PROVIDER\",\"policy_version\":\""
                    + HourlyWarmStartPolicyVersion + "\",\"policy_hash\":\"" + ResolveHourlyWarmStartPolicyHash()
                    + "\",\"publication_policy\":\"CAUSAL_LIVE_COMPLETED_HOUR_V1\"}";
                if (BeelzebubReadOnlyOutbound.Publish("HOURLY_BAR", null, null, payload) != null)
                    lock (hourlyLock) publishedHourlyEnds.Add(endIdentity);
            }
        }

        private static bool TryBarEndUtc(DateTime value, out DateTime end)
        {
            string rejection;
            if (!BeelzebubProviderTimestamp.TryConvertUtc(
                value, Core.Globals.GeneralOptions.TimeZoneInfo, null, out end, out rejection))
                return false;
            return end.Minute == 0 && end.Second == 0 && end.Millisecond == 0;
        }

        private static string HourBarJson(
            DateTime start, DateTime end, double open, double high, double low, double close, long volume)
        {
            return "{\"start\":\"" + start.ToString("o", CultureInfo.InvariantCulture) + "\",\"end\":\""
                + end.ToString("o", CultureInfo.InvariantCulture) + "\",\"open\":" + open.ToString(CultureInfo.InvariantCulture)
                + ",\"high\":" + high.ToString(CultureInfo.InvariantCulture) + ",\"low\":" + low.ToString(CultureInfo.InvariantCulture)
                + ",\"close\":" + close.ToString(CultureInfo.InvariantCulture) + ",\"volume\":" + volume + "}";
        }

        private void OnMarketData(object sender, MarketDataEventArgs e)
        {
            long callbackStarted = System.Diagnostics.Stopwatch.GetTimestamp();
            long callbackCpuStarted = BeelzebubReadOnlyOutbound.CurrentThreadCpuCycles();
            string callbackType = e != null && e.MarketDataType == MarketDataType.Last
                ? "TRADE"
                : e != null && (e.MarketDataType == MarketDataType.Bid || e.MarketDataType == MarketDataType.Ask)
                    ? "QUOTE" : null;
            try
            {
                if (!marketDataGate.TryRun(
                    marketDataGeneration, delegate {
                        BeelzebubMarketPublisherLease.TryRun(publisherLease, delegate { OnMarketDataSerial(e); });
                    }))
                    BeelzebubReadOnlyOutbound.Diagnostic(
                        "NATIVE_ADDON_STALE_MARKET_DATA_CALLBACK_REJECTED");
            }
            finally
            {
                if (callbackType != null)
                    BeelzebubReadOnlyOutbound.RecordNativeCallback(
                        callbackType, System.Diagnostics.Stopwatch.GetTimestamp() - callbackStarted,
                        callbackCpuStarted);
            }
        }

        private void OnMarketDataSerial(MarketDataEventArgs e)
        {
            if (stopping)
                return;
            Interlocked.Exchange(ref lastMarketCallbackTicks, System.Diagnostics.Stopwatch.GetTimestamp());
            PublishAttachment(chartFound, false);
            PublishMarketConnectedOnce();
            if (!reportedLevelOne)
            {
                reportedLevelOne = true;
                BeelzebubReadOnlyOutbound.Diagnostic("NATIVE_ADDON_LEVEL_ONE_RECEIVED");
            }
            string contract = instrument.FullName;
            if (e.MarketDataType == MarketDataType.Last)
            {
                long sourceEventSequence = BeelzebubMarketPublisherLease.Next("TRADE");
                double bidAtTrade = e.Bid;
                double askAtTrade = e.Ask;
                bool completeQuote = FinitePositive(bidAtTrade) && FinitePositive(askAtTrade) && bidAtTrade < askAtTrade
                    && bestBid == bidAtTrade && bestAsk == askAtTrade && bestBidSize > 0 && bestAskSize > 0
                    && e.Time >= bestBidTime && e.Time - bestBidTime <= TimeSpan.FromSeconds(10)
                    && e.Time >= bestAskTime && e.Time - bestAskTime <= TimeSpan.FromSeconds(10);
                string quoteObservationId = null;
                if (completeQuote)
                {
                    Interlocked.Exchange(ref lastQuotePublicationTicks, System.Diagnostics.Stopwatch.GetTimestamp());
                    quoteObservationId = BeelzebubReadOnlyOutbound.Publish("QUOTE", null, null,
                        "{\"contract_id\":\"" + contract + "\",\"bid\":" + bidAtTrade.ToString(CultureInfo.InvariantCulture)
                        + ",\"ask\":" + askAtTrade.ToString(CultureInfo.InvariantCulture) + ",\"bid_size\":" + bestBidSize
                        + ",\"ask_size\":" + bestAskSize + ",\"bid_source_time\":\""
                        + bestBidTime.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture) + "\",\"ask_source_time\":\""
                        + bestAskTime.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture)
                        + "\",\"source_event_sequence\":" + BeelzebubMarketPublisherLease.Next("QUOTE")
                        + ",\"publication_policy\":\"" + PairedQuotePublicationPolicy + "\"}", e.Time);
                }
                string source = quoteObservationId == null ? "UNKNOWN" : "BID_ASK_CLASSIFICATION";
                string bid = completeQuote ? bidAtTrade.ToString(CultureInfo.InvariantCulture) : "null";
                string ask = completeQuote ? askAtTrade.ToString(CultureInfo.InvariantCulture) : "null";
                string quoteReference = quoteObservationId == null ? "null" : "\"" + quoteObservationId + "\"";
                BeelzebubReadOnlyOutbound.Publish("TRADE", null, null,
                    "{\"contract_id\":\"" + contract + "\",\"price\":" + e.Price.ToString(CultureInfo.InvariantCulture)
                    + ",\"size\":" + e.Volume + ",\"aggressor_side\":\"UNKNOWN\",\"aggressor_source\":\"" + source
                    + "\",\"bid_at_trade\":" + bid + ",\"ask_at_trade\":" + ask
                    + ",\"derivation_quote_observation_id\":" + quoteReference
                    + ",\"source_event_sequence\":" + sourceEventSequence
                    + ",\"publication_policy\":\"" + TradePublicationPolicy + "\"}", e.Time);
                return;
            }
            if (e.MarketDataType == MarketDataType.Bid)
            {
                bestBid = e.Price;
                bestBidSize = e.Volume;
                bestBidTime = e.Time;
            }
            else if (e.MarketDataType == MarketDataType.Ask)
            {
                bestAsk = e.Price;
                bestAskSize = e.Volume;
                bestAskTime = e.Time;
            }
            if (FinitePositive(bestBid) && FinitePositive(bestAsk) && bestBid < bestAsk
                && bestBidSize > 0 && bestAskSize > 0
                && TryReservePublication(ref lastQuotePublicationTicks, MinimumPublicationTicks))
                BeelzebubReadOnlyOutbound.Publish("QUOTE", null, null,
                    "{\"contract_id\":\"" + contract + "\",\"bid\":" + bestBid.ToString(CultureInfo.InvariantCulture)
                    + ",\"ask\":" + bestAsk.ToString(CultureInfo.InvariantCulture) + ",\"bid_size\":" + bestBidSize
                    + ",\"ask_size\":" + bestAskSize + ",\"source_event_sequence\":" + BeelzebubMarketPublisherLease.Next("QUOTE")
                    + ",\"publication_policy\":\"" + QuotePublicationPolicy + "\"}", e.Time);
        }

        private void OnMarketDepth(object sender, MarketDepthEventArgs e)
        {
            long callbackStarted = System.Diagnostics.Stopwatch.GetTimestamp();
            long callbackCpuStarted = BeelzebubReadOnlyOutbound.CurrentThreadCpuCycles();
            try
            {
                marketDataGate.TryRun(marketDataGeneration, delegate {
                    BeelzebubMarketPublisherLease.TryRun(publisherLease, delegate { OnMarketDepthSerial(e); });
                });
            }
            finally
            {
                BeelzebubReadOnlyOutbound.RecordNativeCallback(
                    "DEPTH", System.Diagnostics.Stopwatch.GetTimestamp() - callbackStarted,
                    callbackCpuStarted);
            }
        }

        private void OnMarketDepthSerial(MarketDepthEventArgs e)
        {
            if (stopping)
                return;
            Interlocked.Exchange(ref lastMarketCallbackTicks, System.Diagnostics.Stopwatch.GetTimestamp());
            PublishAttachment(chartFound, false);
            PublishMarketConnectedOnce();
            if (!reportedDepth)
            {
                reportedDepth = true;
                BeelzebubReadOnlyOutbound.Diagnostic("NATIVE_ADDON_DEPTH_RECEIVED");
            }
            string payload;
            lock (bookLock)
            {
                // NinjaTrader applies Operation to its zero-based Position
                // before this AddOn callback. Read that native positional
                // snapshot directly. A price-keyed Update leaves the old slot
                // price stale whenever the level moves and caused crossed books.
                long sourceEventSequence = BeelzebubMarketPublisherLease.Next("DEPTH");
                string bidLevels;
                string askLevels;
                string bidPositions;
                string askPositions;
                double bestNativeBid;
                double bestNativeAsk;
                double resolvedMutationPrice;
                bool positionStateDisclosed;
                string snapshotFailure;
                if (!TryApplyDepthMutation(
                    e,
                    out bidLevels, out askLevels, out bidPositions,
                    out askPositions, out bestNativeBid,
                    out bestNativeAsk, out resolvedMutationPrice,
                    out positionStateDisclosed,
                    out snapshotFailure))
                {
                    PublishDepthUnavailable(
                        snapshotFailure, sourceEventSequence, e.Time, e,
                        bidLevels, askLevels, bidPositions, askPositions,
                        resolvedMutationPrice, positionStateDisclosed);
                    return;
                }
                if (FinitePositive(bestNativeBid) && FinitePositive(bestNativeAsk) && bestNativeBid >= bestNativeAsk)
                {
                    PublishDepthUnavailable(
                        "CROSSED_NATIVE_POSITIONAL_SNAPSHOT", sourceEventSequence,
                        e.Time, e, bidLevels, askLevels, bidPositions,
                        askPositions, resolvedMutationPrice,
                        positionStateDisclosed);
                    return;
                }
                payload = "{\"contract_id\":\"" + instrument.FullName + "\",\"bids\":" + bidLevels
                    + ",\"asks\":" + askLevels
                    + (positionStateDisclosed
                        ? ",\"position_bids\":" + bidPositions + ",\"position_asks\":" + askPositions
                        : "")
                    + ",\"operation\":\"" + e.Operation
                    + "\",\"side\":\"" + e.MarketDataType + "\",\"mutation_price\":"
                    + e.Price.ToString(CultureInfo.InvariantCulture) + ",\"mutation_volume\":" + e.Volume
                    + ",\"mutation_position\":" + e.Position
                    + ",\"mutation_resolved_price\":" + (FinitePositive(resolvedMutationPrice)
                        ? resolvedMutationPrice.ToString(CultureInfo.InvariantCulture) : "null")
                    + ",\"is_reset\":" + (e.IsReset ? "true" : "false")
                    + ",\"snapshot_ordering\":\"NATIVE_POSITIONAL_BEST_TO_WORST_V1\""
                    + ",\"source_event_sequence\":" + sourceEventSequence
                    + ",\"publication_policy\":\"" + DepthPublicationPolicy + "\"}";
            }
            BeelzebubReadOnlyOutbound.Publish(
                "DEPTH", null, null, payload, e.Time);
        }

        private void PublishMarketConnectedOnce()
        {
            if (reportedMarketDataConnected)
                return;
            reportedMarketDataConnected = true;
            BeelzebubReadOnlyOutbound.Publish("CONNECTION", null, null,
                "{\"scope\":\"MARKET_DATA\",\"price_status\":\"Connected\"}");
        }

        private static bool FinitePositive(double value)
        {
            return !Double.IsNaN(value) && !Double.IsInfinity(value) && value > 0;
        }

        private bool TryApplyDepthMutation(
            MarketDepthEventArgs update,
            out string bidLevels, out string askLevels,
            out string bidPositions, out string askPositions,
            out double bestNativeBid, out double bestNativeAsk,
            out double resolvedMutationPrice,
            out bool positionStateDisclosed,
            out string failureReason)
        {
            bidLevels = "[]";
            askLevels = "[]";
            bidPositions = "[]";
            askPositions = "[]";
            bestNativeBid = Double.NaN;
            bestNativeAsk = Double.NaN;
            resolvedMutationPrice = Double.NaN;
            positionStateDisclosed = false;
            failureReason = null;
            if (update == null)
            {
                depthContinuityBroken = true;
                failureReason = "NATIVE_DEPTH_MUTATION_MISSING";
                return false;
            }
            if (depthContinuityBroken)
            {
                // An uninterpretable mutation means the native positional
                // cursor is no longer reconstructible. Publish no later book
                // from this observer; NeedsRecovery replaces the subscription
                // and starts a new authoritative cursor.
                failureReason = "NATIVE_DEPTH_CONTINUITY_BROKEN";
                return false;
            }
            SortedDictionary<int, KeyValuePair<double, long>> candidateBids =
                new SortedDictionary<int, KeyValuePair<double, long>>(nativeDepthWorkingBookBids);
            SortedDictionary<int, KeyValuePair<double, long>> candidateAsks =
                new SortedDictionary<int, KeyValuePair<double, long>>(nativeDepthWorkingBookAsks);
            SortedDictionary<int, KeyValuePair<double, long>> side;
            if (update.MarketDataType == MarketDataType.Bid)
                side = candidateBids;
            else if (update.MarketDataType == MarketDataType.Ask)
                side = candidateAsks;
            else
            {
                depthContinuityBroken = true;
                failureReason = "NATIVE_DEPTH_SIDE_INVALID";
                return false;
            }
            if (update.IsReset)
            {
                candidateBids.Clear();
                candidateAsks.Clear();
                side = update.MarketDataType == MarketDataType.Bid ? candidateBids : candidateAsks;
            }
            bool validPrice = FinitePositive(update.Price);
            bool applied = false;
            bool removeIdentityUnresolved = false;
            if (update.Operation == Operation.Add)
            {
                if (validPrice && update.Volume > 0 && update.Position >= 0)
                {
                    ShiftKnownPositions(side, update.Position, true);
                    side[update.Position] = new KeyValuePair<double, long>(update.Price, update.Volume);
                    resolvedMutationPrice = update.Price;
                    applied = true;
                }
            }
            else if (update.Operation == Operation.Update)
            {
                if (validPrice && update.Volume > 0 && update.Position >= 0)
                {
                    side[update.Position] = new KeyValuePair<double, long>(update.Price, update.Volume);
                    resolvedMutationPrice = update.Price;
                    applied = true;
                }
            }
            else if (update.Operation == Operation.Remove)
            {
                if (update.Position >= 0)
                {
                    KeyValuePair<double, long> removed;
                    if (side.TryGetValue(update.Position, out removed)
                        && FinitePositive(removed.Key))
                        resolvedMutationPrice = removed.Key;
                    else
                        removeIdentityUnresolved = true;
                    side.Remove(update.Position);
                    ShiftKnownPositions(side, update.Position, false);
                    applied = true;
                }
            }
            if (!applied)
            {
                depthContinuityBroken = true;
                failureReason = "NATIVE_DEPTH_MUTATION_APPLICATION_FAILED";
                return false;
            }

            // NinjaTrader has already advanced its positional book before
            // invoking this callback. Advance our private native cursor now,
            // before deciding whether this state is safe to publish. Rolling
            // back a crossed candidate would make the next callback's
            // Position apply to a stale pre-cross cursor.
            CommitNativeDepthWorkingState(candidateBids, candidateAsks);
            List<KeyValuePair<double, long>> bidPrefix = DepthPrefix(candidateBids, true);
            List<KeyValuePair<double, long>> askPrefix = DepthPrefix(candidateAsks, false);
            List<string> bidRows = bidPrefix
                .Select(row => "{\"price\":" + row.Key.ToString(CultureInfo.InvariantCulture)
                    + ",\"size\":" + row.Value + "}").ToList();
            List<string> askRows = askPrefix
                .Select(row => "{\"price\":" + row.Key.ToString(CultureInfo.InvariantCulture)
                    + ",\"size\":" + row.Value + "}").ToList();
            bidLevels = "[" + String.Join(",", bidRows) + "]";
            askLevels = "[" + String.Join(",", askRows) + "]";
            if (removeIdentityUnresolved)
            {
                // A zero-price Remove identifies its deleted level only by the
                // pre-mutation positional cursor. Advance all known positions,
                // but never invent the missing removed-price identity or call
                // that callback available.
                positionStateDisclosed = true;
                depthPositionDisclosurePending = true;
                bidPositions = DepthPositions(candidateBids);
                askPositions = DepthPositions(candidateAsks);
                failureReason = "NATIVE_DEPTH_REMOVE_IDENTITY_UNRESOLVED";
                return false;
            }
            if (bidPrefix.Count == 0 || askPrefix.Count == 0)
            {
                positionStateDisclosed = true;
                depthPositionDisclosurePending = true;
                bidPositions = DepthPositions(candidateBids);
                askPositions = DepthPositions(candidateAsks);
                failureReason = "NATIVE_DEPTH_POSITION_ZERO_UNAVAILABLE";
                return false;
            }
            bestNativeBid = bidPrefix[0].Key;
            bestNativeAsk = askPrefix[0].Key;
            if (bestNativeBid >= bestNativeAsk)
            {
                // The working cursor already contains this authentic
                // transition. Withhold it from available publication while
                // retaining it as the base for the next positional callback.
                positionStateDisclosed = true;
                depthPositionDisclosurePending = true;
                bidPositions = DepthPositions(candidateBids);
                askPositions = DepthPositions(candidateAsks);
                failureReason = "CROSSED_NATIVE_POSITIONAL_SNAPSHOT";
                return false;
            }
            if (bidPrefix.Count < MaximumPublishedBookLevelsPerSide
                || askPrefix.Count < MaximumPublishedBookLevelsPerSide)
            {
                positionStateDisclosed = true;
                depthPositionDisclosurePending = true;
                bidPositions = DepthPositions(candidateBids);
                askPositions = DepthPositions(candidateAsks);
                failureReason = "NATIVE_DEPTH_EXACT_10X10_REQUIRED";
                return false;
            }
            else if (depthPositionDisclosurePending)
            {
                positionStateDisclosed = true;
                depthPositionDisclosurePending = false;
                bidPositions = DepthPositions(candidateBids);
                askPositions = DepthPositions(candidateAsks);
            }
            return true;
        }

        private void CommitNativeDepthWorkingState(
            SortedDictionary<int, KeyValuePair<double, long>> candidateBids,
            SortedDictionary<int, KeyValuePair<double, long>> candidateAsks)
        {
            nativeDepthWorkingBookBids.Clear();
            foreach (KeyValuePair<int, KeyValuePair<double, long>> row in candidateBids)
                nativeDepthWorkingBookBids[row.Key] = row.Value;
            nativeDepthWorkingBookAsks.Clear();
            foreach (KeyValuePair<int, KeyValuePair<double, long>> row in candidateAsks)
                nativeDepthWorkingBookAsks[row.Key] = row.Value;
        }

        private static string DepthPositions(
            SortedDictionary<int, KeyValuePair<double, long>> levels)
        {
            List<string> rows = levels.Select(row =>
                "{\"position\":" + row.Key.ToString(CultureInfo.InvariantCulture)
                + ",\"price\":" + row.Value.Key.ToString(CultureInfo.InvariantCulture)
                + ",\"size\":" + row.Value.Value.ToString(CultureInfo.InvariantCulture)
                + "}").ToList();
            return "[" + String.Join(",", rows) + "]";
        }

        private static void ShiftKnownPositions(
            SortedDictionary<int, KeyValuePair<double, long>> levels,
            int position, bool inserting)
        {
            IEnumerable<int> affected = levels.Keys
                .Where(value => inserting ? value >= position : value > position);
            List<int> positions = inserting
                ? affected.OrderByDescending(value => value).ToList()
                : affected.OrderBy(value => value).ToList();
            foreach (int current in positions)
            {
                KeyValuePair<double, long> row = levels[current];
                levels.Remove(current);
                levels[inserting ? current + 1 : current - 1] = row;
            }
        }

        private static List<KeyValuePair<double, long>> DepthPrefix(
            SortedDictionary<int, KeyValuePair<double, long>> levels,
            bool descending)
        {
            List<KeyValuePair<double, long>> prefix = new List<KeyValuePair<double, long>>();
            for (int position = 0; position < MaximumPublishedBookLevelsPerSide; position++)
            {
                KeyValuePair<double, long> row;
                if (!levels.TryGetValue(position, out row)
                    || !FinitePositive(row.Key) || row.Value <= 0)
                    break;
                if (prefix.Count > 0 && (descending
                    ? prefix[prefix.Count - 1].Key <= row.Key
                    : prefix[prefix.Count - 1].Key >= row.Key))
                    break;
                prefix.Add(row);
            }
            return prefix;
        }

        public bool NeedsRecovery(TimeSpan maximumCallbackAge)
        {
            if (stopping || depthContinuityBroken)
                return !stopping;
            long latest = Interlocked.Read(ref lastMarketCallbackTicks);
            long baseline = latest == 0 ? startedTicks : latest;
            double elapsed = (System.Diagnostics.Stopwatch.GetTimestamp() - baseline)
                / (double)System.Diagnostics.Stopwatch.Frequency;
            return elapsed > maximumCallbackAge.TotalSeconds;
        }

        private void PublishDepthUnavailable(
            string reason, long sourceEventSequence, DateTime providerTime,
            MarketDepthEventArgs mutation, string bidLevels, string askLevels,
            string bidPositions, string askPositions,
            double resolvedMutationPrice, bool positionStateDisclosed)
        {
            // This is still one authentic depth mutation. Keep it on the
            // sequenced DEPTH stream so the consumer can distinguish explicit
            // unavailability from a missing callback. Publishing it as HEALTH
            // hid the sequence and flooded the operational-health ledger.
            string mutationPrice = mutation == null
                || Double.IsNaN(mutation.Price) || Double.IsInfinity(mutation.Price)
                ? "null" : mutation.Price.ToString(CultureInfo.InvariantCulture);
            string resolvedPrice = FinitePositive(resolvedMutationPrice)
                ? resolvedMutationPrice.ToString(CultureInfo.InvariantCulture) : "null";
            BeelzebubReadOnlyOutbound.Publish("DEPTH", null, null,
                "{\"status\":\"UNAVAILABLE\",\"reason\":\"" + reason
                + "\",\"contract_id\":\"" + instrument.FullName + "\",\"bids\":"
                + (String.IsNullOrEmpty(bidLevels) ? "[]" : bidLevels) + ",\"asks\":"
                + (String.IsNullOrEmpty(askLevels) ? "[]" : askLevels)
                + (positionStateDisclosed
                    ? ",\"position_bids\":" + (String.IsNullOrEmpty(bidPositions) ? "[]" : bidPositions)
                        + ",\"position_asks\":" + (String.IsNullOrEmpty(askPositions) ? "[]" : askPositions)
                    : "")
                + ",\"mutation_operation\":" + (mutation == null ? "null" : "\"" + mutation.Operation + "\"")
                + ",\"mutation_side\":" + (mutation == null ? "null" : "\"" + mutation.MarketDataType + "\"")
                + ",\"mutation_position\":" + (mutation == null ? "null" : mutation.Position.ToString(CultureInfo.InvariantCulture))
                + ",\"mutation_price\":" + mutationPrice
                + ",\"mutation_resolved_price\":" + resolvedPrice
                + ",\"mutation_volume\":" + (mutation == null ? "null" : mutation.Volume.ToString(CultureInfo.InvariantCulture))
                + ",\"mutation_is_reset\":" + (mutation == null ? "null" : (mutation.IsReset ? "true" : "false"))
                + ",\"snapshot_ordering\":\"NATIVE_POSITIONAL_BEST_TO_WORST_V1\""
                + ",\"source_event_sequence\":" + sourceEventSequence
                + ",\"publication_policy\":\"" + DepthPublicationPolicy + "\"}", providerTime);
        }

        private static bool TryReservePublication(ref long lastPublicationTicks, long minimumTicks)
        {
            long now = System.Diagnostics.Stopwatch.GetTimestamp();
            while (true)
            {
                long prior = Interlocked.Read(ref lastPublicationTicks);
                if (prior != 0 && now - prior < minimumTicks)
                    return false;
                if (Interlocked.CompareExchange(ref lastPublicationTicks, now, prior) == prior)
                    return true;
            }
        }
    }

    // BEELZEBUB_PROVIDER_TIMESTAMP_CONTRACT_BEGIN
    // Callback timestamps with Kind=Unspecified are NinjaTrader application-
    // zone wall times. DateTime does not carry a DST fold bit that callers can
    // supply authoritatively, so ambiguous wall times are rejected unless the
    // provider also supplies one of the zone's exact candidate UTC offsets.
    public static class BeelzebubProviderTimestamp
    {
        public static bool TryConvertUtc(
            DateTime providerTime,
            TimeZoneInfo applicationTimeZone,
            TimeSpan? authoritativeUtcOffset,
            out DateTime utc,
            out string rejectionReason)
        {
            utc = DateTime.MinValue;
            rejectionReason = null;

            if (providerTime.Kind == DateTimeKind.Utc)
            {
                utc = providerTime;
                return true;
            }

            TimeZoneInfo sourceTimeZone;
            if (providerTime.Kind == DateTimeKind.Local)
                sourceTimeZone = TimeZoneInfo.Local;
            else if (providerTime.Kind == DateTimeKind.Unspecified)
            {
                if (applicationTimeZone == null)
                {
                    rejectionReason = "PROVIDER_TIMESTAMP_APPLICATION_TIMEZONE_MISSING";
                    return false;
                }
                sourceTimeZone = applicationTimeZone;
            }
            else
            {
                rejectionReason = "PROVIDER_TIMESTAMP_KIND_UNSUPPORTED";
                return false;
            }

            DateTime wallTime = DateTime.SpecifyKind(providerTime, DateTimeKind.Unspecified);
            try
            {
                if (sourceTimeZone.IsInvalidTime(wallTime))
                {
                    rejectionReason = "PROVIDER_TIMESTAMP_DST_INVALID";
                    return false;
                }

                if (sourceTimeZone.IsAmbiguousTime(wallTime))
                {
                    if (!authoritativeUtcOffset.HasValue)
                    {
                        rejectionReason = "PROVIDER_TIMESTAMP_DST_AMBIGUOUS";
                        return false;
                    }

                    TimeSpan suppliedOffset = authoritativeUtcOffset.Value;
                    if (!sourceTimeZone.GetAmbiguousTimeOffsets(wallTime).Contains(suppliedOffset))
                    {
                        rejectionReason = "PROVIDER_TIMESTAMP_AMBIGUOUS_OFFSET_INVALID";
                        return false;
                    }

                    utc = new DateTime(
                        checked(wallTime.Ticks - suppliedOffset.Ticks),
                        DateTimeKind.Utc);
                    return true;
                }

                if (authoritativeUtcOffset.HasValue)
                {
                    rejectionReason = "PROVIDER_TIMESTAMP_OFFSET_NOT_APPLICABLE";
                    return false;
                }

                utc = TimeZoneInfo.ConvertTimeToUtc(wallTime, sourceTimeZone);
                return true;
            }
            catch (ArgumentException)
            {
                rejectionReason = "PROVIDER_TIMESTAMP_CONVERSION_REJECTED";
                return false;
            }
            catch (InvalidTimeZoneException)
            {
                rejectionReason = "PROVIDER_TIMESTAMP_CONVERSION_REJECTED";
                return false;
            }
            catch (OverflowException)
            {
                rejectionReason = "PROVIDER_TIMESTAMP_CONVERSION_REJECTED";
                return false;
            }
        }
    }
    // BEELZEBUB_PROVIDER_TIMESTAMP_CONTRACT_END

    // Outbound-only loopback sink. It opens a local client connection and only
    // writes one observation frame. There is intentionally no stream Read.
    public static class BeelzebubReadOnlyOutbound
    {
        private sealed class OutboundFrame
        {
            public readonly string Wire;
            public readonly long EnqueuedTicks;

            public OutboundFrame(string wire, long enqueuedTicks)
            {
                Wire = wire;
                EnqueuedTicks = enqueuedTicks;
            }
        }

        private const int Port = 48135;
        private const string AttestedSchema = "lane-iii-phase-f3-ninjatrader-observation-v2";
        private const string AttestationProtocol = "l3f3-ninjatrader-observer-attestation-v1";
        private const string AttestationKeyContext = "BEELZEBUB_NINJATRADER_OBSERVATION_V2";
        private const string ProducerSourceFingerprint = "051808624d0d91ac04e91cae27b7e8765d887774053192b909c7b7434f652a14";
        // Event-driven trades/depth require burst capacity, but memory remains
        // bounded. A full queue drops the signed frame after assigning its
        // sequence; the next source_event_sequence exposes the gap.
        private const int MaximumQueuedFrames = 32768;
        private const int MaximumSenderBatchFrames = 256;
        private const int MaximumSenderBatchCharacters = 524288;
        private static long sequence = 0;
        private static long queueOverflowFrames;
        private static long transportFailedFrames;
        private static readonly long telemetryStartedTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        private static readonly string telemetryStartedUtc = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture);
        private static long nativeDepthCallbacks;
        private static long nativeQuoteCallbacks;
        private static long nativeTradeCallbacks;
        private static long nativeDepthCallbackTicks;
        private static long nativeQuoteCallbackTicks;
        private static long nativeTradeCallbackTicks;
        private static long nativeDepthCallbackMaxTicks;
        private static long nativeQuoteCallbackMaxTicks;
        private static long nativeTradeCallbackMaxTicks;
        private static long nativeDepthCallbackCpuCycles;
        private static long nativeQuoteCallbackCpuCycles;
        private static long nativeTradeCallbackCpuCycles;
        private static long nativeDepthCallbackCpuMaxCycles;
        private static long nativeQuoteCallbackCpuMaxCycles;
        private static long nativeTradeCallbackCpuMaxCycles;
        private static long nativeCallbackCpuSamples;
        private static long nativeCallbackCpuFailures;
        private static long publicationAttempts;
        private static long depthPublicationAttempts;
        private static long quotePublicationAttempts;
        private static long tradePublicationAttempts;
        private static long publicationProcessingTicks;
        private static long publicationProcessingMaxTicks;
        private static long queueEnqueuedFrames;
        private static long queueDequeuedFrames;
        private static long queueHighWaterFrames;
        private static long senderBatches;
        private static long senderBatchFrames;
        private static long senderBatchBytes;
        private static long senderBatchMaxFrames;
        private static long senderBatchMaxBytes;
        private static long senderSocketWrites;
        private static long senderWriteTicks;
        private static long senderWriteMaxTicks;
        private static long senderReconnects;
        private static long senderTransportFailureEvents;
        private static long senderInFlightFrames;
        private static long senderInFlightOldestTicks;
        private static readonly string sessionId = Guid.NewGuid().ToString("N");
        private static readonly HashSet<string> transportMarkers = new HashSet<string>();
        private static readonly object transportMarkersLock = new object();
        private static readonly Queue<OutboundFrame> outboundFrames = new Queue<OutboundFrame>();
        private static readonly object outboundFramesLock = new object();
        private static Thread senderThread;
        private static bool senderRunning;
        private static bool senderStopping;
        private static bool signingKeyLoadAttempted;
        private static byte[] signingKey;
        private static readonly string producerBuildFingerprint = AssemblyHash();
        private static readonly string producerBuildTimestamp = AssemblyBuildTimestamp();

        private static string ProviderTimestampUtc(DateTime? providerTime)
        {
            if (!providerTime.HasValue)
                return "null";

            try
            {
                DateTime utc;
                string rejectionReason;
                if (!BeelzebubProviderTimestamp.TryConvertUtc(
                    providerTime.Value,
                    Core.Globals.GeneralOptions.TimeZoneInfo,
                    null,
                    out utc,
                    out rejectionReason))
                {
                    TransportDiagnosticOnce(rejectionReason);
                    return "null";
                }
                return utc.ToString("o", CultureInfo.InvariantCulture);
            }
            catch (ArgumentException)
            {
                TransportDiagnosticOnce("PROVIDER_TIMESTAMP_CONVERSION_REJECTED");
                return "null";
            }
            catch (InvalidTimeZoneException)
            {
                TransportDiagnosticOnce("PROVIDER_TIMESTAMP_CONVERSION_REJECTED");
                return "null";
            }
        }

        public static string Publish(string type, string alias, string accountClass, string payload, DateTime? providerTime = null)
        {
            long publicationStarted = System.Diagnostics.Stopwatch.GetTimestamp();
            Interlocked.Increment(ref publicationAttempts);
            if (String.Equals(type, "DEPTH", StringComparison.Ordinal))
                Interlocked.Increment(ref depthPublicationAttempts);
            else if (String.Equals(type, "QUOTE", StringComparison.Ordinal))
                Interlocked.Increment(ref quotePublicationAttempts);
            else if (String.Equals(type, "TRADE", StringComparison.Ordinal))
                Interlocked.Increment(ref tradePublicationAttempts);
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
                string providerTimestamp = ProviderTimestampUtc(providerTime);
                if (EnsureSigningKey())
                {
                    JavaScriptSerializer serializer = new JavaScriptSerializer();
                    Dictionary<string, object> payloadValue = serializer.DeserializeObject(payload) as Dictionary<string, object>;
                    if (payloadValue == null)
                    {
                        TransportDiagnosticOnce("ATTESTED_PAYLOAD_PARSE_FAILED");
                        RecordPublicationElapsed(publicationStarted);
                        return null;
                    }
                    Dictionary<string, object> accountValue = null;
                    if (type == "TRADE" || type == "QUOTE" || type == "DEPTH")
                        payloadValue["source_generation"] = BeelzebubMarketPublisherLease.CurrentGeneration;
                    payloadValue["observer_queue_overflow_frames"] = Interlocked.Read(ref queueOverflowFrames);
                    payloadValue["observer_transport_failed_frames"] = Interlocked.Read(ref transportFailedFrames);
                    if (String.Equals(type, "HEALTH", StringComparison.Ordinal))
                        AddAggregateTelemetryLocked(payloadValue);
                    if (alias != null)
                        accountValue = new Dictionary<string, object>(StringComparer.Ordinal) {
                            {"alias", alias}, {"class", accountClass}
                        };
                    string publicationPolicy = payloadValue.ContainsKey("publication_policy")
                        ? Convert.ToString(payloadValue["publication_policy"], CultureInfo.InvariantCulture)
                        : "EVENT_DRIVEN_NATIVE_CALLBACK_V1";
                    Dictionary<string, object> producer = new Dictionary<string, object>(StringComparer.Ordinal) {
                        {"protocol_version", AttestationProtocol},
                        {"source_fingerprint", ProducerSourceFingerprint},
                        {"build_fingerprint", producerBuildFingerprint},
                        {"build_timestamp", producerBuildTimestamp},
                        {"source_stream", SourceStream(type)},
                        {"publication_policy", publicationPolicy},
                    };
                    Dictionary<string, object> frameValue = new Dictionary<string, object>(StringComparer.Ordinal) {
                        {"schema", AttestedSchema}, {"observation_id", observationId},
                        {"session_id", sessionId}, {"observation_type", type},
                        {"ninja_receipt_time", timestamp}, {"local_monotonic_sequence", number},
                        {"provider_timestamp", providerTimestamp == "null" ? null : providerTimestamp},
                        {"provider_sequence", null}, {"exchange_timestamp", null},
                        {"account", accountValue}, {"payload", payloadValue},
                        {"payload_hash", Sha256(Canonical(payloadValue))}, {"producer", producer},
                    };
                    frameValue["signature"] = Sign(frameValue);
                    string attestedFrame = Canonical(frameValue) + "\n";
                    string result = EnqueueLocked(attestedFrame) ? observationId : null;
                    RecordPublicationElapsed(publicationStarted);
                    return result;
                }
                string frame = "{\"schema\":\"lane-iii-phase-f2-ninjatrader-observation-v1\",\"observation_id\":\"" + observationId +
                    "\",\"session_id\":\"" + sessionId + "\",\"observation_type\":\"" + type + "\",\"ninja_receipt_time\":\"" + timestamp +
                    "\",\"local_monotonic_sequence\":" + number + ",\"provider_timestamp\":" + (providerTimestamp == "null" ? "null" : "\"" + providerTimestamp + "\"") +
                    ",\"provider_sequence\":null,\"exchange_timestamp\":null,\"account\":" + account + ",\"payload\":" + payload + "}\n";
                string legacyResult = EnqueueLocked(frame) ? observationId : null;
                RecordPublicationElapsed(publicationStarted);
                return legacyResult;
            }
        }

        public static long CurrentThreadCpuCycles()
        {
            try
            {
                ulong cycles;
                if (QueryThreadCycleTime(GetCurrentThread(), out cycles)
                    && cycles <= Int64.MaxValue)
                    return (long)cycles;
            }
            catch (DllNotFoundException) { }
            catch (EntryPointNotFoundException) { }
            return -1;
        }

        public static void RecordNativeCallback(
            string type, long elapsedTicks, long cpuStartedCycles)
        {
            long cpuEndedCycles = CurrentThreadCpuCycles();
            long cpuCycles = cpuStartedCycles >= 0 && cpuEndedCycles >= cpuStartedCycles
                ? cpuEndedCycles - cpuStartedCycles : -1;
            if (cpuCycles >= 0)
                Interlocked.Increment(ref nativeCallbackCpuSamples);
            else
                Interlocked.Increment(ref nativeCallbackCpuFailures);
            if (String.Equals(type, "DEPTH", StringComparison.Ordinal))
            {
                Interlocked.Increment(ref nativeDepthCallbacks);
                Interlocked.Add(ref nativeDepthCallbackTicks, elapsedTicks);
                UpdateMaximum(ref nativeDepthCallbackMaxTicks, elapsedTicks);
                if (cpuCycles >= 0)
                {
                    Interlocked.Add(ref nativeDepthCallbackCpuCycles, cpuCycles);
                    UpdateMaximum(ref nativeDepthCallbackCpuMaxCycles, cpuCycles);
                }
            }
            else if (String.Equals(type, "QUOTE", StringComparison.Ordinal))
            {
                Interlocked.Increment(ref nativeQuoteCallbacks);
                Interlocked.Add(ref nativeQuoteCallbackTicks, elapsedTicks);
                UpdateMaximum(ref nativeQuoteCallbackMaxTicks, elapsedTicks);
                if (cpuCycles >= 0)
                {
                    Interlocked.Add(ref nativeQuoteCallbackCpuCycles, cpuCycles);
                    UpdateMaximum(ref nativeQuoteCallbackCpuMaxCycles, cpuCycles);
                }
            }
            else if (String.Equals(type, "TRADE", StringComparison.Ordinal))
            {
                Interlocked.Increment(ref nativeTradeCallbacks);
                Interlocked.Add(ref nativeTradeCallbackTicks, elapsedTicks);
                UpdateMaximum(ref nativeTradeCallbackMaxTicks, elapsedTicks);
                if (cpuCycles >= 0)
                {
                    Interlocked.Add(ref nativeTradeCallbackCpuCycles, cpuCycles);
                    UpdateMaximum(ref nativeTradeCallbackCpuMaxCycles, cpuCycles);
                }
            }
        }

        private static void AddAggregateTelemetryLocked(Dictionary<string, object> payloadValue)
        {
            long now = System.Diagnostics.Stopwatch.GetTimestamp();
            double elapsedSeconds = Math.Max(0.000001,
                (now - telemetryStartedTicks) / (double)System.Diagnostics.Stopwatch.Frequency);
            long depthCallbacks = Interlocked.Read(ref nativeDepthCallbacks);
            long quoteCallbacks = Interlocked.Read(ref nativeQuoteCallbacks);
            long tradeCallbacks = Interlocked.Read(ref nativeTradeCallbacks);
            long attempts = Interlocked.Read(ref publicationAttempts);
            long batches = Interlocked.Read(ref senderBatches);
            long oldestTicks = outboundFrames.Count == 0
                ? 0 : Math.Max(0, now - outboundFrames.Peek().EnqueuedTicks);
            long inFlightOldest = Interlocked.Read(ref senderInFlightOldestTicks);
            payloadValue["native_transport_telemetry_schema"] = "native-outbound-aggregate-v1";
            payloadValue["native_telemetry_started_utc"] = telemetryStartedUtc;
            payloadValue["native_telemetry_elapsed_seconds"] = elapsedSeconds;
            payloadValue["native_depth_callbacks_total"] = depthCallbacks;
            payloadValue["native_quote_callbacks_total"] = quoteCallbacks;
            payloadValue["native_trade_callbacks_total"] = tradeCallbacks;
            payloadValue["native_depth_callbacks_per_second"] = depthCallbacks / elapsedSeconds;
            payloadValue["native_quote_callbacks_per_second"] = quoteCallbacks / elapsedSeconds;
            payloadValue["native_trade_callbacks_per_second"] = tradeCallbacks / elapsedSeconds;
            payloadValue["native_callback_cpu_cycles_supported"] = Interlocked.Read(ref nativeCallbackCpuSamples) > 0;
            payloadValue["native_callback_cpu_samples_total"] = Interlocked.Read(ref nativeCallbackCpuSamples);
            payloadValue["native_callback_cpu_failures_total"] = Interlocked.Read(ref nativeCallbackCpuFailures);
            payloadValue["native_depth_callback_cpu_cycles_total"] = Interlocked.Read(ref nativeDepthCallbackCpuCycles);
            payloadValue["native_quote_callback_cpu_cycles_total"] = Interlocked.Read(ref nativeQuoteCallbackCpuCycles);
            payloadValue["native_trade_callback_cpu_cycles_total"] = Interlocked.Read(ref nativeTradeCallbackCpuCycles);
            payloadValue["native_depth_callback_cpu_cycles_max"] = Interlocked.Read(ref nativeDepthCallbackCpuMaxCycles);
            payloadValue["native_quote_callback_cpu_cycles_max"] = Interlocked.Read(ref nativeQuoteCallbackCpuMaxCycles);
            payloadValue["native_trade_callback_cpu_cycles_max"] = Interlocked.Read(ref nativeTradeCallbackCpuMaxCycles);
            payloadValue["native_depth_callback_wall_ms_total"] = Milliseconds(Interlocked.Read(ref nativeDepthCallbackTicks));
            payloadValue["native_quote_callback_wall_ms_total"] = Milliseconds(Interlocked.Read(ref nativeQuoteCallbackTicks));
            payloadValue["native_trade_callback_wall_ms_total"] = Milliseconds(Interlocked.Read(ref nativeTradeCallbackTicks));
            payloadValue["native_depth_callback_wall_ms_max"] = Milliseconds(Interlocked.Read(ref nativeDepthCallbackMaxTicks));
            payloadValue["native_quote_callback_wall_ms_max"] = Milliseconds(Interlocked.Read(ref nativeQuoteCallbackMaxTicks));
            payloadValue["native_trade_callback_wall_ms_max"] = Milliseconds(Interlocked.Read(ref nativeTradeCallbackMaxTicks));
            payloadValue["native_publication_attempts_total"] = attempts;
            payloadValue["native_depth_publication_attempts_total"] = Interlocked.Read(ref depthPublicationAttempts);
            payloadValue["native_quote_publication_attempts_total"] = Interlocked.Read(ref quotePublicationAttempts);
            payloadValue["native_trade_publication_attempts_total"] = Interlocked.Read(ref tradePublicationAttempts);
            payloadValue["native_publication_attempts_per_second"] = attempts / elapsedSeconds;
            payloadValue["native_publication_processing_ms_total"] = Milliseconds(Interlocked.Read(ref publicationProcessingTicks));
            payloadValue["native_publication_processing_ms_max"] = Milliseconds(Interlocked.Read(ref publicationProcessingMaxTicks));
            payloadValue["native_outbound_queue_capacity"] = MaximumQueuedFrames;
            payloadValue["native_outbound_queue_current"] = outboundFrames.Count;
            payloadValue["native_outbound_queue_high_water"] = Interlocked.Read(ref queueHighWaterFrames);
            payloadValue["native_outbound_queue_enqueued_total"] = Interlocked.Read(ref queueEnqueuedFrames);
            payloadValue["native_outbound_queue_dequeued_total"] = Interlocked.Read(ref queueDequeuedFrames);
            payloadValue["native_outbound_queue_enqueue_per_second"] = Interlocked.Read(ref queueEnqueuedFrames) / elapsedSeconds;
            payloadValue["native_outbound_queue_dequeue_per_second"] = Interlocked.Read(ref queueDequeuedFrames) / elapsedSeconds;
            payloadValue["native_outbound_queue_overflow_total"] = Interlocked.Read(ref queueOverflowFrames);
            payloadValue["native_outbound_queue_oldest_age_ms"] = Milliseconds(oldestTicks);
            payloadValue["native_sender_batches_total"] = batches;
            payloadValue["native_sender_batches_per_second"] = batches / elapsedSeconds;
            payloadValue["native_sender_batch_frames_total"] = Interlocked.Read(ref senderBatchFrames);
            payloadValue["native_sender_batch_bytes_total"] = Interlocked.Read(ref senderBatchBytes);
            payloadValue["native_sender_records_per_second"] = Interlocked.Read(ref senderBatchFrames) / elapsedSeconds;
            payloadValue["native_sender_bytes_per_second"] = Interlocked.Read(ref senderBatchBytes) / elapsedSeconds;
            payloadValue["native_sender_frames_per_batch"] = batches == 0
                ? 0.0 : Interlocked.Read(ref senderBatchFrames) / (double)batches;
            payloadValue["native_sender_batch_frames_max"] = Interlocked.Read(ref senderBatchMaxFrames);
            payloadValue["native_sender_batch_bytes_max"] = Interlocked.Read(ref senderBatchMaxBytes);
            payloadValue["native_sender_socket_writes_total"] = Interlocked.Read(ref senderSocketWrites);
            payloadValue["native_sender_write_ms_total"] = Milliseconds(Interlocked.Read(ref senderWriteTicks));
            payloadValue["native_sender_write_ms_max"] = Milliseconds(Interlocked.Read(ref senderWriteMaxTicks));
            payloadValue["native_sender_reconnects_total"] = Interlocked.Read(ref senderReconnects);
            payloadValue["native_sender_transport_failure_events_total"] = Interlocked.Read(ref senderTransportFailureEvents);
            payloadValue["native_sender_inflight_frames"] = Interlocked.Read(ref senderInFlightFrames);
            payloadValue["native_sender_inflight_oldest_age_ms"] = inFlightOldest == 0
                ? 0.0 : Milliseconds(Math.Max(0, now - inFlightOldest));
        }

        private static double Milliseconds(long ticks)
        {
            return ticks * 1000.0 / System.Diagnostics.Stopwatch.Frequency;
        }

        private static void UpdateMaximum(ref long target, long candidate)
        {
            long prior;
            while (candidate > (prior = Interlocked.Read(ref target))
                && Interlocked.CompareExchange(ref target, candidate, prior) != prior) { }
        }

        private static void RecordWriteElapsed(long startedTicks)
        {
            if (startedTicks == 0)
                return;
            long elapsed = System.Diagnostics.Stopwatch.GetTimestamp() - startedTicks;
            Interlocked.Add(ref senderWriteTicks, elapsed);
            UpdateMaximum(ref senderWriteMaxTicks, elapsed);
        }

        private static void RecordPublicationElapsed(long startedTicks)
        {
            long elapsed = System.Diagnostics.Stopwatch.GetTimestamp() - startedTicks;
            Interlocked.Add(ref publicationProcessingTicks, elapsed);
            UpdateMaximum(ref publicationProcessingMaxTicks, elapsed);
        }

        [DllImport("kernel32.dll")]
        private static extern IntPtr GetCurrentThread();

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool QueryThreadCycleTime(IntPtr threadHandle, out ulong cycleTime);

        // Serialize all writes through one localhost-only sender. Market data
        // callbacks can be concurrent; opening one socket per callback permits
        // frame reordering and can exhaust local ephemeral ports.
        private static bool EnqueueLocked(string frame)
        {
            if (outboundFrames.Count >= MaximumQueuedFrames)
            {
                Interlocked.Increment(ref queueOverflowFrames);
                TransportDiagnosticOnce("BRIDGE_QUEUE_CAPACITY_REACHED");
                return false;
            }
            outboundFrames.Enqueue(new OutboundFrame(
                frame, System.Diagnostics.Stopwatch.GetTimestamp()));
            Interlocked.Increment(ref queueEnqueuedFrames);
            UpdateMaximum(ref queueHighWaterFrames, outboundFrames.Count);
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
            return true;
        }

        private static void SenderLoop()
        {
            TcpClient client = null;
            NetworkStream stream = null;
            bool connectionAttempted = false;
            try
            {
                while (true)
                {
                    List<string> frames = new List<string>();
                    long batchOldestTicks = 0;
                    lock (outboundFramesLock)
                    {
                        while (!senderStopping && outboundFrames.Count == 0)
                            Monitor.Wait(outboundFramesLock);
                        if (senderStopping)
                            return;
                        int characters = 0;
                        while (outboundFrames.Count > 0
                            && frames.Count < MaximumSenderBatchFrames)
                        {
                            OutboundFrame candidate = outboundFrames.Peek();
                            if (frames.Count > 0
                                && characters + candidate.Wire.Length > MaximumSenderBatchCharacters)
                                break;
                            if (frames.Count == 0)
                                batchOldestTicks = candidate.EnqueuedTicks;
                            frames.Add(outboundFrames.Dequeue().Wire);
                            characters += candidate.Wire.Length;
                        }
                        Interlocked.Add(ref queueDequeuedFrames, frames.Count);
                    }
                    byte[] bytes = Encoding.UTF8.GetBytes(String.Concat(frames));
                    Interlocked.Increment(ref senderBatches);
                    Interlocked.Add(ref senderBatchFrames, frames.Count);
                    Interlocked.Add(ref senderBatchBytes, bytes.Length);
                    UpdateMaximum(ref senderBatchMaxFrames, frames.Count);
                    UpdateMaximum(ref senderBatchMaxBytes, bytes.Length);
                    Interlocked.Exchange(ref senderInFlightFrames, frames.Count);
                    Interlocked.Exchange(ref senderInFlightOldestTicks, batchOldestTicks);
                    long writeStarted = 0;
                    try
                    {
                        if (client == null)
                        {
                            if (connectionAttempted)
                                Interlocked.Increment(ref senderReconnects);
                            connectionAttempted = true;
                            client = new TcpClient();
                            client.Connect(IPAddress.Loopback, Port);
                            stream = client.GetStream();
                            TransportDiagnosticOnce("BRIDGE_CHANNEL_CONNECTED");
                        }
                        // Batch ordered newline-delimited frames into one socket
                        // write. Encoding and network backpressure remain outside
                        // the callback/queue lock; FIFO order is unchanged.
                        writeStarted = System.Diagnostics.Stopwatch.GetTimestamp();
                        Interlocked.Increment(ref senderSocketWrites);
                        stream.Write(bytes, 0, bytes.Length);
                        RecordWriteElapsed(writeStarted);
                        writeStarted = 0;
                        TransportDiagnosticOnce("BRIDGE_PUBLISH_DELIVERED");
                    }
                    catch (SocketException error)
                    {
                        RecordWriteElapsed(writeStarted);
                        Interlocked.Increment(ref senderTransportFailureEvents);
                        Interlocked.Add(ref transportFailedFrames, frames.Count);
                        TransportDiagnosticOnce("BRIDGE_CONNECT_FAILED_" + error.SocketErrorCode);
                        CloseClient(client);
                        client = null;
                        stream = null;
                    }
                    catch (Exception error)
                    {
                        RecordWriteElapsed(writeStarted);
                        Interlocked.Increment(ref senderTransportFailureEvents);
                        Interlocked.Add(ref transportFailedFrames, frames.Count);
                        TransportDiagnosticOnce("BRIDGE_PUBLISH_FAILED_" + error.GetType().Name);
                        CloseClient(client);
                        client = null;
                        stream = null;
                    }
                    Interlocked.Exchange(ref senderInFlightFrames, 0);
                    Interlocked.Exchange(ref senderInFlightOldestTicks, 0);
                }
            }
            finally
            {
                Interlocked.Exchange(ref senderInFlightFrames, 0);
                Interlocked.Exchange(ref senderInFlightOldestTicks, 0);
                CloseClient(client);
                lock (outboundFramesLock)
                {
                    senderRunning = false;
                    senderThread = null;
                }
            }
        }

        private static bool EnsureSigningKey()
        {
            if (signingKey != null)
                return true;
            if (signingKeyLoadAttempted)
                return false;
            signingKeyLoadAttempted = true;
            try
            {
                string path = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments),
                    "NinjaTrader 8", "l3g.paper.local.key");
                byte[] rootKey = File.ReadAllBytes(path);
                if (rootKey.Length < 32)
                    throw new InvalidDataException();
                using (HMACSHA256 hmac = new HMACSHA256(rootKey))
                    signingKey = hmac.ComputeHash(Encoding.UTF8.GetBytes(AttestationKeyContext));
                Array.Clear(rootKey, 0, rootKey.Length);
                TransportDiagnosticOnce("OBSERVATION_ATTESTATION_READY");
                return true;
            }
            catch (IOException) { TransportDiagnosticOnce("OBSERVATION_ATTESTATION_KEY_UNAVAILABLE"); }
            catch (UnauthorizedAccessException) { TransportDiagnosticOnce("OBSERVATION_ATTESTATION_KEY_UNAVAILABLE"); }
            catch (InvalidDataException) { TransportDiagnosticOnce("OBSERVATION_ATTESTATION_KEY_INVALID"); }
            return false;
        }

        private static string SourceStream(string type)
        {
            if (String.Equals(type, "TRADE", StringComparison.Ordinal)) return "MARKET_DATA_LAST";
            if (String.Equals(type, "QUOTE", StringComparison.Ordinal)) return "MARKET_DATA_BID_ASK";
            if (String.Equals(type, "DEPTH", StringComparison.Ordinal)) return "MARKET_DEPTH";
            if (String.Equals(type, "HISTORICAL_WARM_START", StringComparison.Ordinal)
                || String.Equals(type, "VOLUME_REFERENCE_HISTORY", StringComparison.Ordinal)
                || String.Equals(type, "HOURLY_BAR", StringComparison.Ordinal)) return "HISTORICAL_BARS";
            return "NINJATRADER_CALLBACK";
        }

        private static string Sign(Dictionary<string, object> frame)
        {
            Dictionary<string, object> unsigned = new Dictionary<string, object>(frame, StringComparer.Ordinal);
            unsigned.Remove("signature");
            using (HMACSHA256 hmac = new HMACSHA256(signingKey))
                return Hex(hmac.ComputeHash(Encoding.UTF8.GetBytes(Canonical(unsigned))));
        }

        private static string Sha256(string value)
        {
            using (SHA256 sha = SHA256.Create())
                return Hex(sha.ComputeHash(Encoding.UTF8.GetBytes(value)));
        }

        private static string Hex(byte[] value)
        {
            return BitConverter.ToString(value).Replace("-", String.Empty).ToLowerInvariant();
        }

        private static string Canonical(object value)
        {
            IDictionary<string, object> dictionary = value as IDictionary<string, object>;
            if (dictionary != null)
                return "{" + String.Join(",", dictionary.OrderBy(item => item.Key, StringComparer.Ordinal)
                    .Select(item => new JavaScriptSerializer().Serialize(item.Key) + ":" + Canonical(item.Value))) + "}";
            IEnumerable enumerable = value as IEnumerable;
            if (enumerable != null && !(value is string))
            {
                List<string> items = new List<string>();
                foreach (object item in enumerable) items.Add(Canonical(item));
                return "[" + String.Join(",", items) + "]";
            }
            if (value == null) return "null";
            if (value is bool) return (bool)value ? "true" : "false";
            if (value is string) return new JavaScriptSerializer().Serialize((string)value);
            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        private static string AssemblyHash()
        {
            try
            {
                using (SHA256 sha = SHA256.Create())
                    return Hex(sha.ComputeHash(File.ReadAllBytes(Assembly.GetExecutingAssembly().Location)));
            }
            catch { return new string('0', 64); }
        }

        private static string AssemblyBuildTimestamp()
        {
            try { return File.GetLastWriteTimeUtc(Assembly.GetExecutingAssembly().Location).ToString("o", CultureInfo.InvariantCulture); }
            catch { return "1970-01-01T00:00:00.0000000Z"; }
        }

        public static void Shutdown()
        {
            lock (outboundFramesLock)
            {
                senderStopping = true;
                outboundFrames.Clear();
                if (signingKey != null)
                    Array.Clear(signingKey, 0, signingKey.Length);
                signingKey = null;
                signingKeyLoadAttempted = false;
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
