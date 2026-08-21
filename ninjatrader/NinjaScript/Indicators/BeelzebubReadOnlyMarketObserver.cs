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
                BeelzebubReadOnlyOutbound.Publish("TRADE", null, null, "{\"contract_id\":\"" + contract + "\",\"price\":" + e.Price.ToString(CultureInfo.InvariantCulture) + ",\"size\":" + e.Volume + ",\"aggressor_side\":\"UNKNOWN\"}", e.Time);
            if (e.MarketDataType == MarketDataType.Bid)
            {
                bestBid = e.Price;
                bestBidSize = e.Volume;
            }
            if (e.MarketDataType == MarketDataType.Ask)
            {
                bestAsk = e.Price;
                bestAskSize = e.Volume;
            }
            if (!Double.IsNaN(bestBid) && !Double.IsNaN(bestAsk) && bestBid <= bestAsk && bestBidSize > 0 && bestAskSize > 0)
                BeelzebubReadOnlyOutbound.Publish("QUOTE", null, null, "{\"contract_id\":\"" + contract + "\",\"bid\":" + bestBid.ToString(CultureInfo.InvariantCulture) + ",\"ask\":" + bestAsk.ToString(CultureInfo.InvariantCulture) + ",\"bid_size\":" + bestBidSize + ",\"ask_size\":" + bestAskSize + "}", e.Time);
        }

        protected override void OnMarketDepth(MarketDepthEventArgs e)
        {
            if (!reportedDepth)
            {
                reportedDepth = true;
                BeelzebubReadOnlyOutbound.Diagnostic("MARKET_OBSERVER_DEPTH_RECEIVED");
            }
            SortedDictionary<double, long> book = e.MarketDataType == MarketDataType.Bid ? bids : asks;
            if (e.Operation == Operation.Remove) book.Remove(e.Price); else book[e.Price] = e.Volume;
            BeelzebubReadOnlyOutbound.Publish("DEPTH", null, null, "{\"contract_id\":\"" + Instrument.FullName + "\",\"bids\":" + Levels(bids) + ",\"asks\":" + Levels(asks) + ",\"operation\":\"" + e.Operation + "\",\"side\":\"" + e.MarketDataType + "\"}", e.Time);
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
