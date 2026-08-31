// Attach only to the resolved MNQ September 2026 chart/instrument.
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
        // The runtime freshness gates are 2s for quotes and 5s for trades/depth.
        // Publish the latest authentic state at 2 Hz per stream instead of
        // attempting to persist every UI callback. The provider sequence is
        // unavailable and book completeness remains UNVERIFIED, so this is an
        // explicit bounded observation policy, never a complete-feed claim.
        private static readonly long MinimumPublicationTicks = Math.Max(1L, System.Diagnostics.Stopwatch.Frequency / 2L);
        private const string PublicationPolicy = "BOUNDED_LATEST_STATE_2HZ";
        private readonly SortedDictionary<double, long> bids = new SortedDictionary<double, long>(Comparer<double>.Create((x, y) => y.CompareTo(x)));
        private readonly SortedDictionary<double, long> asks = new SortedDictionary<double, long>();
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
        private long lastTradePublicationTicks;
        private long lastDepthPublicationTicks;

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
                BeelzebubReadOnlyOutbound.Diagnostic("MARKET_OBSERVER_REALTIME");
                BeelzebubReadOnlyOutbound.Diagnostic("MARKET_OBSERVER_REALTIME_STRICT_SPREAD_V1");
            }
        }

        protected override void OnMarketData(MarketDataEventArgs e)
        {
            PublishMarketConnectedOnce();
            if (!reportedLevelOne)
            {
                reportedLevelOne = true;
                BeelzebubReadOnlyOutbound.Diagnostic("MARKET_OBSERVER_LEVEL_ONE_RECEIVED");
            }
            string contract = Instrument.FullName;
            if (e.MarketDataType == MarketDataType.Last)
            {
                if (!TryReservePublication(ref lastTradePublicationTicks))
                    return;
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
                if (completeQuote && TryReservePublication(ref lastQuotePublicationTicks))
                    quoteObservationId = BeelzebubReadOnlyOutbound.Publish("QUOTE", null, null,
                        "{\"contract_id\":\"" + contract + "\",\"bid\":" + bidAtTrade.ToString(CultureInfo.InvariantCulture)
                        + ",\"ask\":" + askAtTrade.ToString(CultureInfo.InvariantCulture) + ",\"bid_size\":" + bestBidSize
                        + ",\"ask_size\":" + bestAskSize + ",\"bid_source_time\":\""
                        + bestBidTime.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture) + "\",\"ask_source_time\":\""
                        + bestAskTime.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture)
                        + "\",\"publication_policy\":\"" + PublicationPolicy + "\"}", e.Time);
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
            if (e.MarketDataType == MarketDataType.Ask)
            {
                bestAsk = e.Price;
                bestAskSize = e.Volume;
                bestAskTime = e.Time;
            }
            if (!Double.IsNaN(bestBid) && !Double.IsNaN(bestAsk) && bestBid < bestAsk && bestBidSize > 0 && bestAskSize > 0
                && TryReservePublication(ref lastQuotePublicationTicks))
                BeelzebubReadOnlyOutbound.Publish("QUOTE", null, null, "{\"contract_id\":\"" + contract + "\",\"bid\":" + bestBid.ToString(CultureInfo.InvariantCulture) + ",\"ask\":" + bestAsk.ToString(CultureInfo.InvariantCulture) + ",\"bid_size\":" + bestBidSize + ",\"ask_size\":" + bestAskSize + ",\"publication_policy\":\"" + PublicationPolicy + "\"}", e.Time);
        }

        protected override void OnConnectionStatusUpdate(ConnectionStatusEventArgs e)
        {
            // Any price-feed transition invalidates locally accumulated quote
            // and book state. A reconnect must rebuild from new callbacks.
            ClearMarketState();
            reportedMarketDataConnected = String.Equals(e.PriceStatus.ToString(), "Connected", StringComparison.OrdinalIgnoreCase);
            BeelzebubReadOnlyOutbound.Publish("CONNECTION", null, null,
                "{\"scope\":\"MARKET_DATA\",\"price_status\":\"" + e.PriceStatus + "\"}");
        }

        protected override void OnMarketDepth(MarketDepthEventArgs e)
        {
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
            SortedDictionary<double, long> book = e.MarketDataType == MarketDataType.Bid ? bids : asks;
            double mutationPrice = e.Price;
            if (e.Operation == Operation.Remove && (Double.IsNaN(mutationPrice) || Double.IsInfinity(mutationPrice) || mutationPrice <= 0))
            {
                if (e.Position < 0 || e.Position >= book.Count)
                {
                    BeelzebubReadOnlyOutbound.Diagnostic("MARKET_OBSERVER_UNRESOLVED_DEPTH_REMOVE");
                    return;
                }
                mutationPrice = book.ElementAt(e.Position).Key;
            }
            if (Double.IsNaN(mutationPrice) || Double.IsInfinity(mutationPrice) || mutationPrice <= 0)
                return;
            if (e.Operation == Operation.Remove) book.Remove(mutationPrice); else book[mutationPrice] = e.Volume;
            TrimBook(book);
            if (e.Position >= MaximumPublishedBookLevelsPerSide)
                return;
            if (!TryReservePublication(ref lastDepthPublicationTicks))
                return;
            BeelzebubReadOnlyOutbound.Publish("DEPTH", null, null, "{\"contract_id\":\"" + Instrument.FullName
                + "\",\"bids\":" + Levels(bids) + ",\"asks\":" + Levels(asks) + ",\"operation\":\"" + e.Operation
                + "\",\"side\":\"" + e.MarketDataType + "\",\"mutation_price\":" + mutationPrice.ToString(CultureInfo.InvariantCulture)
                + ",\"mutation_volume\":" + e.Volume + ",\"mutation_position\":" + e.Position
                + ",\"is_reset\":false,\"publication_policy\":\"" + PublicationPolicy + "\"}", e.Time);
        }

        private void ClearMarketState()
        {
            bids.Clear();
            asks.Clear();
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
            {
                if (index++ >= MaximumPublishedBookLevelsPerSide)
                    discarded.Add(item.Key);
            }
            foreach (double price in discarded)
                book.Remove(price);
        }

        private static bool TryReservePublication(ref long lastPublicationTicks)
        {
            long now = System.Diagnostics.Stopwatch.GetTimestamp();
            while (true)
            {
                long prior = Interlocked.Read(ref lastPublicationTicks);
                if (prior != 0 && now - prior < MinimumPublicationTicks)
                    return false;
                if (Interlocked.CompareExchange(ref lastPublicationTicks, now, prior) == prior)
                    return true;
            }
        }
    }
}
