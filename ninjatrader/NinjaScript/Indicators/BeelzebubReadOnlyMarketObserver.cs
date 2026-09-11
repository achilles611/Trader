// Attach only to the MNQ contract resolved by Beelzebub's local configuration.
// This indicator publishes market observations through the outbound-only sink.
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using NinjaTrader.Cbi;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.AddOns;

namespace NinjaTrader.NinjaScript.Indicators
{
    public sealed class BeelzebubReadOnlyMarketObserver : Indicator
    {
        // Publish the authentic top ten positions on each side. The local view
        // remains explicitly unverified, but bounding both callback admission
        // and snapshot size prevents the one-way bridge from silently losing
        // frames during sustained MNQ depth bursts.
        private const int MaximumPublishedBookLevelsPerSide = 10;
        // Quotes remain bounded state, but trades and depth retain every native
        // callback so causal coverage can be proved from source sequences.
        private static readonly long MinimumPublicationTicks = Math.Max(1L, System.Diagnostics.Stopwatch.Frequency / 2L);
        private static readonly long MinimumAttachmentPublicationTicks = Math.Max(1L, System.Diagnostics.Stopwatch.Frequency * 5L);
        private const string QuotePublicationPolicy = "BOUNDED_LATEST_STATE_2HZ";
        private const string PairedQuotePublicationPolicy = "EVENT_DRIVEN_SAME_CALLBACK_QUOTE_V1";
        private const string TradePublicationPolicy = "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1";
        private const string DepthPublicationPolicy = "EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1";
        private readonly object bookLock = new object();
        private double bestBid = Double.NaN;
        private double bestAsk = Double.NaN;
        private long bestBidSize = 0;
        private long bestAskSize = 0;
        private DateTime bestBidTime = DateTime.MinValue;
        private DateTime bestAskTime = DateTime.MinValue;
        private bool reportedLevelOne;
        private bool reportedDepth;
        private bool reportedMarketDataConnected;
        private long lastQuotePublicationTicks;
        private long lastAttachmentPublicationTicks;
        private long quoteEventSequence;
        private long tradeEventSequence;
        private long depthEventSequence;

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Name = "BeelzebubReadOnlyMarketObserver";
                // The observer does not calculate or trade. This ensures the
                // host provides each live update to its event callbacks.
                Calculate = Calculate.OnEachTick;
            }
            else if (State == State.Realtime)
            {
                if (BeelzebubReadOnlyAddOn.AutomaticObserverActive)
                    return;
                BeelzebubReadOnlyOutbound.Diagnostic("MARKET_OBSERVER_REALTIME");
                BeelzebubReadOnlyOutbound.Diagnostic("MARKET_OBSERVER_REALTIME_STRICT_SPREAD_V1");
                PublishAttachmentHealth(true);
            }
            else if (State == State.Terminated)
            {
                if (BeelzebubReadOnlyAddOn.AutomaticObserverActive)
                    return;
                string configured = BeelzebubReadOnlyAddOn.ResolveConfiguredInstrument();
                string instrument = Instrument == null ? null : Instrument.FullName;
                BeelzebubReadOnlyAddOn.PublishObserverAttachment(
                    "OBSERVER_TERMINATED", configured, instrument, true, false);
            }
        }

        protected override void OnMarketData(MarketDataEventArgs e)
        {
            if (BeelzebubReadOnlyAddOn.AutomaticObserverActive)
                return;
            PublishAttachmentHealth();
            PublishMarketConnectedOnce();
            if (!reportedLevelOne)
            {
                reportedLevelOne = true;
                BeelzebubReadOnlyOutbound.Diagnostic("MARKET_OBSERVER_LEVEL_ONE_RECEIVED");
            }
            string contract = Instrument.FullName;
            if (e.MarketDataType == MarketDataType.Last)
            {
                long sourceEventSequence = Interlocked.Increment(ref tradeEventSequence);
                // NinjaTrader directly supplies Bid and Ask on the Last event,
                // but not a native aggressor flag.  Emit a same-callback quote
                // first only when its cached sizes match those exact prices.
                // The Python boundary performs the classification and retains
                // QUOTE_DERIVED provenance; all ambiguity remains UNKNOWN.
                double bidAtTrade = e.Bid;
                double askAtTrade = e.Ask;
                bool completeQuote = !Double.IsNaN(bidAtTrade) && !Double.IsInfinity(bidAtTrade) && bidAtTrade > 0
                    && !Double.IsNaN(askAtTrade) && !Double.IsInfinity(askAtTrade) && askAtTrade > 0
                    && bidAtTrade < askAtTrade && bestBid == bidAtTrade && bestAsk == askAtTrade
                    && bestBidSize > 0 && bestAskSize > 0
                    && e.Time >= bestBidTime && e.Time - bestBidTime <= TimeSpan.FromSeconds(10)
                    && e.Time >= bestAskTime && e.Time - bestAskTime <= TimeSpan.FromSeconds(10);
                string quoteObservationId = null;
                if (completeQuote)
                {
                    // A sampled Last event must retain its exact same-callback
                    // quote pair or aggressor classification becomes UNKNOWN.
                    // Reset the independent quote timer so the pair replaces,
                    // rather than adds to, the next bounded quote publication.
                    Interlocked.Exchange(ref lastQuotePublicationTicks, System.Diagnostics.Stopwatch.GetTimestamp());
                    quoteObservationId = BeelzebubReadOnlyOutbound.Publish("QUOTE", null, null,
                        "{\"contract_id\":\"" + contract + "\",\"bid\":" + bidAtTrade.ToString(CultureInfo.InvariantCulture)
                        + ",\"ask\":" + askAtTrade.ToString(CultureInfo.InvariantCulture) + ",\"bid_size\":" + bestBidSize
                        + ",\"ask_size\":" + bestAskSize + ",\"bid_source_time\":\""
                        + bestBidTime.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture) + "\",\"ask_source_time\":\""
                        + bestAskTime.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture)
                        + "\",\"source_event_sequence\":" + Interlocked.Increment(ref quoteEventSequence)
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
            if (e.MarketDataType == MarketDataType.Ask)
            {
                bestAsk = e.Price;
                bestAskSize = e.Volume;
                bestAskTime = e.Time;
            }
            if (!Double.IsNaN(bestBid) && !Double.IsNaN(bestAsk) && bestBid < bestAsk && bestBidSize > 0 && bestAskSize > 0
                && TryReservePublication(ref lastQuotePublicationTicks))
                BeelzebubReadOnlyOutbound.Publish("QUOTE", null, null, "{\"contract_id\":\"" + contract + "\",\"bid\":" + bestBid.ToString(CultureInfo.InvariantCulture) + ",\"ask\":" + bestAsk.ToString(CultureInfo.InvariantCulture) + ",\"bid_size\":" + bestBidSize + ",\"ask_size\":" + bestAskSize + ",\"source_event_sequence\":" + Interlocked.Increment(ref quoteEventSequence) + ",\"publication_policy\":\"" + QuotePublicationPolicy + "\"}", e.Time);
        }

        protected override void OnConnectionStatusUpdate(ConnectionStatusEventArgs e)
        {
            if (BeelzebubReadOnlyAddOn.AutomaticObserverActive)
                return;
            // Any price-feed transition invalidates locally accumulated quote
            // and book state. A reconnect must rebuild from new callbacks.
            ClearMarketState();
            reportedMarketDataConnected = String.Equals(e.PriceStatus.ToString(), "Connected", StringComparison.OrdinalIgnoreCase);
            PublishAttachmentHealth(true);
            BeelzebubReadOnlyOutbound.Publish("CONNECTION", null, null,
                "{\"scope\":\"MARKET_DATA\",\"price_status\":\"" + e.PriceStatus + "\"}");
        }

        protected override void OnMarketDepth(MarketDepthEventArgs e)
        {
            if (BeelzebubReadOnlyAddOn.AutomaticObserverActive)
                return;
            PublishAttachmentHealth();
            PublishMarketConnectedOnce();
            if (!reportedDepth)
            {
                reportedDepth = true;
                BeelzebubReadOnlyOutbound.Diagnostic("MARKET_OBSERVER_DEPTH_RECEIVED");
            }
            // NinjaTrader documents IsReset as a UI-reset notification that is
            // relevant only to columns.  This is an Indicator; its authoritative
            // lifecycle reset is the price connection transition handled by
            // OnConnectionStatusUpdate.  Treat Add/Update/Remove as authentic
            // depth mutations regardless of the column-only flag.
            string payload;
            lock (bookLock)
            {
                long sourceEventSequence = Interlocked.Increment(ref depthEventSequence);
                string bidLevels;
                string askLevels;
                double bestNativeBid;
                double bestNativeAsk;
                if (!TryNativeDepthSnapshot(out bidLevels, out askLevels, out bestNativeBid, out bestNativeAsk))
                {
                    PublishDepthUnavailable("POSITIONAL_SNAPSHOT_INVALID", sourceEventSequence, e.Time);
                    return;
                }
                if (FinitePositive(bestNativeBid) && FinitePositive(bestNativeAsk) && bestNativeBid >= bestNativeAsk)
                {
                    PublishDepthUnavailable("CROSSED_NATIVE_POSITIONAL_SNAPSHOT", sourceEventSequence, e.Time);
                    return;
                }
                payload = "{\"contract_id\":\"" + Instrument.FullName
                    + "\",\"bids\":" + bidLevels + ",\"asks\":" + askLevels + ",\"operation\":\"" + e.Operation
                    + "\",\"side\":\"" + e.MarketDataType + "\",\"mutation_price\":" + e.Price.ToString(CultureInfo.InvariantCulture)
                    + ",\"mutation_volume\":" + e.Volume + ",\"mutation_position\":" + e.Position
                    + ",\"is_reset\":" + (e.IsReset ? "true" : "false")
                    + ",\"source_event_sequence\":" + sourceEventSequence
                    + ",\"publication_policy\":\"" + DepthPublicationPolicy + "\"}";
            }
            BeelzebubReadOnlyOutbound.Publish(
                "DEPTH", null, null, payload, e.Time);
        }

        private void ClearMarketState()
        {
            bestBid = Double.NaN;
            bestAsk = Double.NaN;
            bestBidSize = 0;
            bestAskSize = 0;
            bestBidTime = DateTime.MinValue;
            bestAskTime = DateTime.MinValue;
        }

        private void PublishMarketConnectedOnce()
        {
            if (reportedMarketDataConnected)
                return;
            reportedMarketDataConnected = true;
            // An authentic callback proves that this exact instrument's price
            // stream is currently delivering. It does not assert provider
            // sequencing, book completeness, or continued future health.
            BeelzebubReadOnlyOutbound.Publish("CONNECTION", null, null,
                "{\"scope\":\"MARKET_DATA\",\"price_status\":\"Connected\"}");
        }

        private void PublishAttachmentHealth(bool force = false)
        {
            if (!force && !TryReservePublication(ref lastAttachmentPublicationTicks, MinimumAttachmentPublicationTicks))
                return;
            if (force)
                Interlocked.Exchange(ref lastAttachmentPublicationTicks, System.Diagnostics.Stopwatch.GetTimestamp());
            string configured = BeelzebubReadOnlyAddOn.ResolveConfiguredInstrument();
            string instrument = Instrument == null ? null : Instrument.FullName;
            bool matches = !String.IsNullOrWhiteSpace(configured)
                && String.Equals(configured, instrument, StringComparison.Ordinal);
            BeelzebubReadOnlyAddOn.PublishObserverAttachment(
                matches ? "OBSERVER_ATTACHED" : "WRONG_CHART_INSTRUMENT",
                configured, instrument, true, true);
        }

        private bool TryNativeDepthSnapshot(
            out string bidLevels, out string askLevels,
            out double bestNativeBid, out double bestNativeAsk)
        {
            bidLevels = "[]";
            askLevels = "[]";
            bestNativeBid = Double.NaN;
            bestNativeAsk = Double.NaN;
            if (Instrument == null || Instrument.MarketDepth == null)
                return false;
            List<string> bidRows = new List<string>();
            List<string> askRows = new List<string>();
            double priorBid = Double.PositiveInfinity;
            double priorAsk = Double.NegativeInfinity;
            try
            {
                // The platform can resize its positional book between Count
                // and an indexed read. Our lock cannot lock that native owner,
                // so make the exact churn race a truthful unavailable snapshot
                // instead of allowing it to terminate depth callbacks.
                int bidCount = Math.Min(MaximumPublishedBookLevelsPerSide, Instrument.MarketDepth.Bids.Count);
                int askCount = Math.Min(MaximumPublishedBookLevelsPerSide, Instrument.MarketDepth.Asks.Count);
                for (int index = 0; index < bidCount; index++)
                {
                    double price = Instrument.MarketDepth.Bids[index].Price;
                    long volume = Instrument.MarketDepth.Bids[index].Volume;
                    if (!FinitePositive(price) || volume <= 0 || price >= priorBid) return false;
                    if (index == 0) bestNativeBid = price;
                    priorBid = price;
                    bidRows.Add("{\"price\":" + price.ToString(CultureInfo.InvariantCulture) + ",\"size\":" + volume + "}");
                }
                for (int index = 0; index < askCount; index++)
                {
                    double price = Instrument.MarketDepth.Asks[index].Price;
                    long volume = Instrument.MarketDepth.Asks[index].Volume;
                    if (!FinitePositive(price) || volume <= 0 || price <= priorAsk) return false;
                    if (index == 0) bestNativeAsk = price;
                    priorAsk = price;
                    askRows.Add("{\"price\":" + price.ToString(CultureInfo.InvariantCulture) + ",\"size\":" + volume + "}");
                }
            }
            catch (ArgumentOutOfRangeException)
            {
                return false;
            }
            bidLevels = "[" + String.Join(",", bidRows) + "]";
            askLevels = "[" + String.Join(",", askRows) + "]";
            return true;
        }

        private void PublishDepthUnavailable(string reason, long sourceEventSequence, DateTime providerTime)
        {
            // This is still one authentic depth mutation. Keep it on the
            // sequenced DEPTH stream so the consumer can distinguish explicit
            // unavailability from a missing callback. Publishing it as HEALTH
            // hid the sequence and flooded the operational-health ledger.
            BeelzebubReadOnlyOutbound.Publish("DEPTH", null, null,
                "{\"status\":\"UNAVAILABLE\",\"reason\":\"" + reason
                + "\",\"contract_id\":\"" + Instrument.FullName + "\",\"bids\":[],\"asks\":[]"
                + ",\"source_event_sequence\":" + sourceEventSequence
                + ",\"publication_policy\":\"" + DepthPublicationPolicy + "\"}", providerTime);
        }

        private static bool FinitePositive(double value)
        {
            return !Double.IsNaN(value) && !Double.IsInfinity(value) && value > 0;
        }

        private static bool TryReservePublication(ref long lastPublicationTicks)
        {
            return TryReservePublication(ref lastPublicationTicks, MinimumPublicationTicks);
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
}
