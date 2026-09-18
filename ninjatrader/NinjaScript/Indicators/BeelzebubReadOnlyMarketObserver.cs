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
        private readonly BeelzebubMarketCallbackGate marketDataGate =
            new BeelzebubMarketCallbackGate();
        private readonly long marketDataGeneration = 1;
        private long publisherLease;
        private readonly SortedDictionary<int, KeyValuePair<double, long>> depthBids =
            new SortedDictionary<int, KeyValuePair<double, long>>();
        private readonly SortedDictionary<int, KeyValuePair<double, long>> depthAsks =
            new SortedDictionary<int, KeyValuePair<double, long>>();
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
                publisherLease = BeelzebubMarketPublisherLease.Claim(false);
                BeelzebubReadOnlyOutbound.Diagnostic("MARKET_OBSERVER_REALTIME");
                BeelzebubReadOnlyOutbound.Diagnostic("MARKET_OBSERVER_REALTIME_STRICT_SPREAD_V1");
                PublishAttachmentHealth(true);
            }
            else if (State == State.Terminated)
            {
                marketDataGate.Retire();
                BeelzebubMarketPublisherLease.Retire(publisherLease);
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
            if (!marketDataGate.TryRun(
                marketDataGeneration, delegate {
                    BeelzebubMarketPublisherLease.TryRun(publisherLease, delegate { OnMarketDataSerial(e); });
                }))
                BeelzebubReadOnlyOutbound.Diagnostic(
                    "INDICATOR_STALE_MARKET_DATA_CALLBACK_REJECTED");
        }

        private void OnMarketDataSerial(MarketDataEventArgs e)
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
                long sourceEventSequence = BeelzebubMarketPublisherLease.Next("TRADE");
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
            if (e.MarketDataType == MarketDataType.Ask)
            {
                bestAsk = e.Price;
                bestAskSize = e.Volume;
                bestAskTime = e.Time;
            }
            if (!Double.IsNaN(bestBid) && !Double.IsNaN(bestAsk) && bestBid < bestAsk && bestBidSize > 0 && bestAskSize > 0
                && TryReservePublication(ref lastQuotePublicationTicks))
                BeelzebubReadOnlyOutbound.Publish("QUOTE", null, null, "{\"contract_id\":\"" + contract + "\",\"bid\":" + bestBid.ToString(CultureInfo.InvariantCulture) + ",\"ask\":" + bestAsk.ToString(CultureInfo.InvariantCulture) + ",\"bid_size\":" + bestBidSize + ",\"ask_size\":" + bestAskSize + ",\"source_event_sequence\":" + BeelzebubMarketPublisherLease.Next("QUOTE") + ",\"publication_policy\":\"" + QuotePublicationPolicy + "\"}", e.Time);
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
            marketDataGate.TryRun(marketDataGeneration, delegate {
                BeelzebubMarketPublisherLease.TryRun(publisherLease, delegate { OnMarketDepthSerial(e); });
            });
        }

        private void OnMarketDepthSerial(MarketDepthEventArgs e)
        {
            if (!marketDataGate.IsActive || BeelzebubReadOnlyAddOn.AutomaticObserverActive)
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
                long sourceEventSequence = BeelzebubMarketPublisherLease.Next("DEPTH");
                string bidLevels;
                string askLevels;
                double bestNativeBid;
                double bestNativeAsk;
                string snapshotFailure;
                if (!TryApplyDepthMutation(
                    e,
                    out bidLevels, out askLevels, out bestNativeBid,
                    out bestNativeAsk, out snapshotFailure))
                {
                    PublishDepthUnavailable(snapshotFailure, sourceEventSequence, e.Time);
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
                    + ",\"snapshot_ordering\":\"NATIVE_POSITIONAL_BEST_TO_WORST_V1\""
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
            lock (bookLock)
            {
                depthBids.Clear();
                depthAsks.Clear();
            }
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

        private bool TryApplyDepthMutation(
            MarketDepthEventArgs update,
            out string bidLevels, out string askLevels,
            out double bestNativeBid, out double bestNativeAsk,
            out string failureReason)
        {
            bidLevels = "[]";
            askLevels = "[]";
            bestNativeBid = Double.NaN;
            bestNativeAsk = Double.NaN;
            failureReason = null;
            if (update == null)
            {
                failureReason = "NATIVE_DEPTH_MUTATION_MISSING";
                return false;
            }
            SortedDictionary<int, KeyValuePair<double, long>> candidateBids =
                new SortedDictionary<int, KeyValuePair<double, long>>(depthBids);
            SortedDictionary<int, KeyValuePair<double, long>> candidateAsks =
                new SortedDictionary<int, KeyValuePair<double, long>>(depthAsks);
            SortedDictionary<int, KeyValuePair<double, long>> side;
            if (update.MarketDataType == MarketDataType.Bid)
                side = candidateBids;
            else if (update.MarketDataType == MarketDataType.Ask)
                side = candidateAsks;
            else
            {
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
            if (update.Operation == Operation.Add)
            {
                if (validPrice && update.Volume > 0 && update.Position >= 0)
                {
                    ShiftKnownPositions(side, update.Position, true);
                    side[update.Position] = new KeyValuePair<double, long>(update.Price, update.Volume);
                    applied = true;
                }
            }
            else if (update.Operation == Operation.Update)
            {
                if (validPrice && update.Volume > 0 && update.Position >= 0)
                {
                    side[update.Position] = new KeyValuePair<double, long>(update.Price, update.Volume);
                    applied = true;
                }
            }
            else if (update.Operation == Operation.Remove)
            {
                if (update.Position >= 0)
                {
                    side.Remove(update.Position);
                    ShiftKnownPositions(side, update.Position, false);
                    applied = true;
                }
            }
            if (!applied)
            {
                failureReason = "NATIVE_DEPTH_MUTATION_APPLICATION_FAILED";
                return false;
            }
            depthBids.Clear();
            foreach (KeyValuePair<int, KeyValuePair<double, long>> row in candidateBids)
                depthBids[row.Key] = row.Value;
            depthAsks.Clear();
            foreach (KeyValuePair<int, KeyValuePair<double, long>> row in candidateAsks)
                depthAsks[row.Key] = row.Value;
            List<KeyValuePair<double, long>> bidPrefix = DepthPrefix(depthBids, true);
            List<KeyValuePair<double, long>> askPrefix = DepthPrefix(depthAsks, false);
            if (bidPrefix.Count == 0 || askPrefix.Count == 0)
            {
                failureReason = "NATIVE_DEPTH_POSITION_ZERO_UNAVAILABLE";
                return false;
            }
            bestNativeBid = bidPrefix[0].Key;
            bestNativeAsk = askPrefix[0].Key;
            if (bestNativeBid >= bestNativeAsk)
            {
                failureReason = "CROSSED_NATIVE_POSITIONAL_SNAPSHOT";
                return false;
            }
            List<string> bidRows = bidPrefix
                .Select(row => "{\"price\":" + row.Key.ToString(CultureInfo.InvariantCulture)
                    + ",\"size\":" + row.Value + "}").ToList();
            List<string> askRows = askPrefix
                .Select(row => "{\"price\":" + row.Key.ToString(CultureInfo.InvariantCulture)
                    + ",\"size\":" + row.Value + "}").ToList();
            bidLevels = "[" + String.Join(",", bidRows) + "]";
            askLevels = "[" + String.Join(",", askRows) + "]";
            return true;
        }

        private static void ShiftKnownPositions(
            SortedDictionary<int, KeyValuePair<double, long>> levels,
            int position, bool inserting)
        {
            List<int> positions = levels.Keys
                .Where(value => inserting ? value >= position : value > position)
                .ToList();
            if (inserting)
                positions.Reverse();
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
