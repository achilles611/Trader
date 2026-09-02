// L3-F2 observation-only NinjaTrader 8 AddOn source.
// Install/compile only through NinjaTrader's NinjaScript editor. This source
// intentionally contains no order, ATM, or inbound bridge command surface.
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Windows;
using NinjaTrader.Cbi;
using NinjaTrader.Data;
using NinjaTrader.Gui;
using NinjaTrader.Gui.Tools;
using NinjaTrader.NinjaScript.AddOns;

namespace NinjaTrader.NinjaScript.AddOns
{
    public sealed class BeelzebubReadOnlyAddOn : AddOnBase
    {
        private readonly List<Account> observedAccounts = new List<Account>();
        private readonly List<NinjaTrader.Gui.Chart.Chart> observedCharts = new List<NinjaTrader.Gui.Chart.Chart>();
        private NTMenuItem controlCenterNewMenu;
        private NTMenuItem attachMenuItem;
        private BeelzebubAutomaticMarketObserver automaticObserver;
        private bool addonActive;
        private static int automaticObserverActive;

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
            }
            else if (State == State.Terminated)
            {
                addonActive = false;
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
            AttachKnownAccounts();
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
            foreach (NinjaTrader.Gui.Chart.Chart chart in observedCharts)
            {
                if (chart == null || chart.ActiveChartControl == null || chart.ActiveChartControl.Instrument == null)
                    continue;
                string instrument = chart.ActiveChartControl.Instrument.FullName;
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
            {
                try { correct.Activate(); }
                catch (InvalidOperationException) { }
            }
            if (automaticObserver != null)
                automaticObserver.PublishAttachment(true);
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
            automaticObserver = new BeelzebubAutomaticMarketObserver(instrument);
            if (!automaticObserver.Start())
            {
                automaticObserver = null;
                PublishObserverAttachment("NATIVE_ADDON_OBSERVER_FAILED", configured, instrument.FullName, false, false, "NATIVE_ADDON");
            }
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
        private const string PublicationPolicy = "BOUNDED_LATEST_STATE_2HZ";
        private readonly Instrument instrument;
        private readonly SortedDictionary<double, long> bids = new SortedDictionary<double, long>(Comparer<double>.Create((x, y) => y.CompareTo(x)));
        private readonly SortedDictionary<double, long> asks = new SortedDictionary<double, long>();
        private bool subscribed;
        private bool stopping;
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
        private long lastTradePublicationTicks;
        private long lastDepthPublicationTicks;
        private long lastAttachmentPublicationTicks;

        public BeelzebubAutomaticMarketObserver(Instrument instrument)
        {
            this.instrument = instrument;
        }

        public bool Start()
        {
            if (instrument == null || instrument.Dispatcher == null || instrument.Dispatcher.HasShutdownStarted)
                return false;
            try
            {
                instrument.Dispatcher.InvokeAsync(() =>
                {
                    if (stopping || subscribed)
                        return;
                    try
                    {
                        instrument.MarketData.Update += OnMarketData;
                        instrument.MarketDepth.Update += OnMarketDepth;
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

        private void OnMarketData(object sender, MarketDataEventArgs e)
        {
            if (stopping)
                return;
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
                if (!TryReservePublication(ref lastTradePublicationTicks, MinimumPublicationTicks))
                    return;
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
                        + "\",\"publication_policy\":\"" + PublicationPolicy + "\"}", e.Time);
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
                    + ",\"publication_policy\":\"" + PublicationPolicy + "\"}", e.Time);
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
                    + ",\"ask_size\":" + bestAskSize + ",\"publication_policy\":\"" + PublicationPolicy + "\"}", e.Time);
        }

        private void OnMarketDepth(object sender, MarketDepthEventArgs e)
        {
            if (stopping)
                return;
            PublishAttachment(chartFound, false);
            PublishMarketConnectedOnce();
            if (!reportedDepth)
            {
                reportedDepth = true;
                BeelzebubReadOnlyOutbound.Diagnostic("NATIVE_ADDON_DEPTH_RECEIVED");
            }
            SortedDictionary<double, long> book = e.MarketDataType == MarketDataType.Bid ? bids : asks;
            double mutationPrice = e.Price;
            if (e.Operation == Operation.Remove && !FinitePositive(mutationPrice))
            {
                if (e.Position < 0 || e.Position >= book.Count)
                    return;
                mutationPrice = book.ElementAt(e.Position).Key;
            }
            if (!FinitePositive(mutationPrice))
                return;
            if (e.Operation == Operation.Remove)
                book.Remove(mutationPrice);
            else
                book[mutationPrice] = e.Volume;
            TrimBook(book);
            if (e.Position >= MaximumPublishedBookLevelsPerSide
                || !TryReservePublication(ref lastDepthPublicationTicks, MinimumPublicationTicks))
                return;
            BeelzebubReadOnlyOutbound.Publish("DEPTH", null, null,
                "{\"contract_id\":\"" + instrument.FullName + "\",\"bids\":" + Levels(bids)
                + ",\"asks\":" + Levels(asks) + ",\"operation\":\"" + e.Operation
                + "\",\"side\":\"" + e.MarketDataType + "\",\"mutation_price\":"
                + mutationPrice.ToString(CultureInfo.InvariantCulture) + ",\"mutation_volume\":" + e.Volume
                + ",\"mutation_position\":" + e.Position
                + ",\"is_reset\":false,\"publication_policy\":\"" + PublicationPolicy + "\"}", e.Time);
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

        private static string Levels(SortedDictionary<double, long> book)
        {
            List<string> values = new List<string>();
            foreach (KeyValuePair<double, long> item in book)
                values.Add("{\"price\":" + item.Key.ToString(CultureInfo.InvariantCulture) + ",\"size\":" + item.Value + "}");
            return "[" + String.Join(",", values) + "]";
        }

        private static void TrimBook(SortedDictionary<double, long> book)
        {
            if (book.Count <= MaximumPublishedBookLevelsPerSide)
                return;
            List<double> discarded = new List<double>();
            int index = 0;
            foreach (KeyValuePair<double, long> item in book)
                if (index++ >= MaximumPublishedBookLevelsPerSide)
                    discarded.Add(item.Key);
            foreach (double price in discarded)
                book.Remove(price);
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

    // Outbound-only loopback sink. It opens a local client connection and only
    // writes one observation frame. There is intentionally no stream Read.
    public static class BeelzebubReadOnlyOutbound
    {
        private const int Port = 48135;
        // Bound recovery lag. At the observer's declared 2 Hz-per-stream
        // publication policy this holds well under one freshness epoch. If it
        // fills, sequence gaps remain visible and authority fails closed.
        private const int MaximumQueuedFrames = 8;
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
