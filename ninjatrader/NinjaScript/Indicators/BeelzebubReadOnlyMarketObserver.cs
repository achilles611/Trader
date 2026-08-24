// Attach only to the resolved MNQ September 2026 chart/instrument.
// This indicator publishes market observations through the outbound-only sink.
using System;
using System.Collections.Generic;
using System.Globalization;
using NinjaTrader.Cbi;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.AddOns;

namespace NinjaTrader.NinjaScript.Indicators
{
    public sealed class BeelzebubReadOnlyMarketObserver : Indicator
    {
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
                BeelzebubReadOnlyOutbound.Diagnostic("MARKET_OBSERVER_REALTIME");
        }

        protected override void OnMarketData(MarketDataEventArgs e)
        {
            if (!reportedLevelOne)
            {
                reportedLevelOne = true;
                BeelzebubReadOnlyOutbound.Diagnostic("MARKET_OBSERVER_LEVEL_ONE_RECEIVED");
            }
            string contract = Instrument.FullName;
            if (e.MarketDataType == MarketDataType.Last)
            {
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
                    quoteObservationId = BeelzebubReadOnlyOutbound.Publish("QUOTE", null, null,
                        "{\"contract_id\":\"" + contract + "\",\"bid\":" + bidAtTrade.ToString(CultureInfo.InvariantCulture)
                        + ",\"ask\":" + askAtTrade.ToString(CultureInfo.InvariantCulture) + ",\"bid_size\":" + bestBidSize
                        + ",\"ask_size\":" + bestAskSize + ",\"bid_source_time\":\""
                        + bestBidTime.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture) + "\",\"ask_source_time\":\""
                        + bestAskTime.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture) + "\"}", e.Time);
                string source = quoteObservationId == null ? "UNKNOWN" : "BID_ASK_CLASSIFICATION";
                string bid = completeQuote ? bidAtTrade.ToString(CultureInfo.InvariantCulture) : "null";
                string ask = completeQuote ? askAtTrade.ToString(CultureInfo.InvariantCulture) : "null";
                string quoteReference = quoteObservationId == null ? "null" : "\"" + quoteObservationId + "\"";
                BeelzebubReadOnlyOutbound.Publish("TRADE", null, null,
                    "{\"contract_id\":\"" + contract + "\",\"price\":" + e.Price.ToString(CultureInfo.InvariantCulture)
                    + ",\"size\":" + e.Volume + ",\"aggressor_side\":\"UNKNOWN\",\"aggressor_source\":\"" + source
                    + "\",\"bid_at_trade\":" + bid + ",\"ask_at_trade\":" + ask
                    + ",\"derivation_quote_observation_id\":" + quoteReference + "}", e.Time);
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
            if (!Double.IsNaN(bestBid) && !Double.IsNaN(bestAsk) && bestBid <= bestAsk && bestBidSize > 0 && bestAskSize > 0)
                BeelzebubReadOnlyOutbound.Publish("QUOTE", null, null, "{\"contract_id\":\"" + contract + "\",\"bid\":" + bestBid.ToString(CultureInfo.InvariantCulture) + ",\"ask\":" + bestAsk.ToString(CultureInfo.InvariantCulture) + ",\"bid_size\":" + bestBidSize + ",\"ask_size\":" + bestAskSize + "}", e.Time);
        }

        protected override void OnConnectionStatusUpdate(ConnectionStatusEventArgs e)
        {
            // Any price-feed transition invalidates locally accumulated quote
            // and book state. A reconnect must rebuild from new callbacks.
            ClearMarketState();
            BeelzebubReadOnlyOutbound.Publish("CONNECTION", null, null,
                "{\"scope\":\"MARKET_DATA\",\"price_status\":\"" + e.PriceStatus + "\"}");
        }

        protected override void OnMarketDepth(MarketDepthEventArgs e)
        {
            if (!reportedDepth)
            {
                reportedDepth = true;
                BeelzebubReadOnlyOutbound.Diagnostic("MARKET_OBSERVER_DEPTH_RECEIVED");
            }
            if (e.IsReset)
                ClearMarketState();
            SortedDictionary<double, long> book = e.MarketDataType == MarketDataType.Bid ? bids : asks;
            if (e.Operation == Operation.Remove) book.Remove(e.Price); else book[e.Price] = e.Volume;
            BeelzebubReadOnlyOutbound.Publish("DEPTH", null, null, "{\"contract_id\":\"" + Instrument.FullName
                + "\",\"bids\":" + Levels(bids) + ",\"asks\":" + Levels(asks) + ",\"operation\":\"" + e.Operation
                + "\",\"side\":\"" + e.MarketDataType + "\",\"mutation_price\":" + e.Price.ToString(CultureInfo.InvariantCulture)
                + ",\"mutation_volume\":" + e.Volume + ",\"mutation_position\":" + e.Position
                + ",\"is_reset\":" + e.IsReset.ToString().ToLowerInvariant() + "}", e.Time);
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

        private static string Levels(SortedDictionary<double, long> book)
        {
            List<string> values = new List<string>();
            foreach (KeyValuePair<double, long> item in book)
                values.Add("{\"price\":" + item.Key.ToString(CultureInfo.InvariantCulture) + ",\"size\":" + item.Value + "}");
            return "[" + String.Join(",", values) + "]";
        }
    }
}
