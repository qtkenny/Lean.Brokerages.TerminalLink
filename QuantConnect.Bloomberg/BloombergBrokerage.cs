/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Bloomberglp.Blpapi;
using NodaTime;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.HistoricalData;
using QuantConnect.Logging;
using QuantConnect.Packets;
using HistoryRequest = QuantConnect.Data.HistoryRequest;
using Subscription = QuantConnect.Lean.Engine.DataFeeds.Subscription;

namespace QuantConnect.Bloomberg
{
    /// <summary>
    /// An implementation of <see cref="IDataQueueHandler"/> and <see cref="IHistoryProvider"/> for Bloomberg
    /// </summary>
    public class BloombergBrokerage : SynchronizingHistoryProvider, IDataQueueHandler, IDisposable
    {
        private readonly string _serverHost = Config.Get("bloomberg-server-host", "localhost");
        private readonly int _serverPort = Config.GetInt("bloomberg-server-port", 8194);

        // the emulator only supports async session for market data and sync session for historical requests so we have two sessions for now
        // TODO: use single async session for both with real API
        private readonly Session _sessionAsync;
        private readonly Session _sessionSync;

        private readonly object _locker = new object();
        private readonly List<Tick> _ticks = new List<Tick>();
        private long _nextCorrelationId;
        private readonly ConcurrentDictionary<string, BloombergSubscriptions> _subscriptionsByTopicName = new ConcurrentDictionary<string, BloombergSubscriptions>();
        private readonly ConcurrentDictionary<string, Symbol> _symbolsByTopicName = new ConcurrentDictionary<string, Symbol>();
        private readonly BloombergSymbolMapper _symbolMapper = new BloombergSymbolMapper();

        /// <summary>
        /// Initializes a new instance of the <see cref="BloombergBrokerage"/> class
        /// </summary>
        public BloombergBrokerage()
        {
            var sessionOptions = new SessionOptions
            {
                ServerHost = _serverHost,
                ServerPort = _serverPort
            };

            _sessionAsync = new Session(sessionOptions, OnBloombergEvent);
            _sessionAsync.StartAsync();
            _sessionAsync.OpenService("//blp/mktdata");

            _sessionSync = new Session(sessionOptions);
            _sessionSync.OpenService("//blp/refdata");
        }

        #region IDataQueueHandler implementation

        /// <summary>
        /// Get the next ticks from the live trading data queue
        /// </summary>
        /// <returns>IEnumerable list of ticks since the last update.</returns>
        public IEnumerable<BaseData> GetNextTicks()
        {
            lock (_locker)
            {
                var copy = _ticks.ToArray();
                _ticks.Clear();
                return copy;
            }
        }

        /// <summary>
        /// Adds the specified symbols to the subscription
        /// </summary>
        /// <param name="job">Job we're subscribing for:</param>
        /// <param name="symbols">The symbols to be added keyed by SecurityType</param>
        public void Subscribe(LiveNodePacket job, IEnumerable<Symbol> symbols)
        {
            var subscriptions = CreateNewBloombergSubscriptions(symbols);

            if (subscriptions.Count > 0)
            {
                foreach (var subscription in subscriptions)
                {
                    Log.Trace($"BloombergDataQueueHandler.Subscribe(): {subscription.SubscriptionString}");
                }

                _sessionAsync.Subscribe(subscriptions);
            }
        }

        /// <summary>
        /// Removes the specified symbols to the subscription
        /// </summary>
        /// <param name="job">Job we're processing.</param>
        /// <param name="symbols">The symbols to be removed keyed by SecurityType</param>
        public void Unsubscribe(LiveNodePacket job, IEnumerable<Symbol> symbols)
        {
            var subscriptions = RemoveExistingBloombergSubscriptions(symbols);

            if (subscriptions.Count > 0)
            {
                foreach (var subscription in subscriptions)
                {
                    Log.Trace($"BloombergDataQueueHandler.Unsubscribe(): {subscription.SubscriptionString}");
                }

                _sessionAsync.Unsubscribe(subscriptions);
            }
        }

        #endregion

        #region IHistoryProvider implementation

        /// <summary>
        /// Initializes this history provider to work for the specified job
        /// </summary>
        /// <param name="parameters">The initialization parameters</param>
        public override void Initialize(HistoryProviderInitializeParameters parameters)
        {
        }

        /// <summary>
        /// Gets the history for the requested securities
        /// </summary>
        /// <param name="requests">The historical data requests</param>
        /// <param name="sliceTimeZone">The time zone used when time stamping the slice instances</param>
        /// <returns>An enumerable of the slices of data covering the span specified in each request</returns>
        public override IEnumerable<Slice> GetHistory(IEnumerable<HistoryRequest> requests, DateTimeZone sliceTimeZone)
        {
            // create subscription objects from the configs
            var subscriptions = new List<Subscription>();
            foreach (var request in requests)
            {
                var history = GetHistory(request);
                var subscription = CreateSubscription(request, history);

                subscription.MoveNext(); // prime pump
                subscriptions.Add(subscription);
            }

            return CreateSliceEnumerableFromSubscriptions(subscriptions, sliceTimeZone);
        }

        private IEnumerable<BaseData> GetHistory(HistoryRequest historyRequest)
        {
            switch (historyRequest.Resolution)
            {
                case Resolution.Daily:
                    // historical daily quotes are not supported natively by Bloomberg so we aggregate from hourly data
                    if (historyRequest.TickType == TickType.Quote)
                    {
                        var historyRequestHour = new HistoryRequest(
                            historyRequest.StartTimeUtc,
                            historyRequest.EndTimeUtc,
                            historyRequest.DataType,
                            historyRequest.Symbol,
                            Resolution.Hour,
                            historyRequest.ExchangeHours,
                            historyRequest.DataTimeZone,
                            historyRequest.FillForwardResolution,
                            historyRequest.IncludeExtendedMarketHours,
                            historyRequest.IsCustomData,
                            historyRequest.DataNormalizationMode,
                            historyRequest.TickType);
                        return AggregateQuoteBars(GetIntradayBarData(historyRequestHour), historyRequest.Symbol, Time.OneDay);
                    }
                    else
                    {
                        return GetHistoricalData(historyRequest);
                    }

                case Resolution.Hour:
                case Resolution.Minute:
                    return GetIntradayBarData(historyRequest);

                case Resolution.Second:
                    // second resolution is not supported natively by Bloomberg so we aggregate from tick data
                    return historyRequest.TickType == TickType.Quote
                        ? AggregateTicksToQuoteBars(GetIntradayTickData(historyRequest), historyRequest.Symbol, Time.OneSecond)
                        : AggregateTicksToTradeBars(GetIntradayTickData(historyRequest), historyRequest.Symbol, Time.OneSecond);

                case Resolution.Tick:
                    return GetIntradayTickData(historyRequest);

                default:
                    Log.Error($"Unsupported resolution: {historyRequest.Resolution}");
                    return Enumerable.Empty<BaseData>();
            }
        }

        private static IEnumerable<BaseData> AggregateQuoteBars(IEnumerable<BaseData> bars, Symbol symbol, TimeSpan period)
        {
            return
            (from b in bars
                let bar = (QuoteBar)b
                group bar by bar.Time.RoundDown(period)
                into g
                select (BaseData)new QuoteBar
                {
                    Symbol = symbol,
                    Time = g.Key,
                    Bid = new Bar
                    {
                        Open = g.First().Bid.Open,
                        High = g.Max(b => b.Bid.High),
                        Low = g.Min(b => b.Bid.Low),
                        Close = g.Last().Bid.Close
                    },
                    LastBidSize = g.Last().LastBidSize,
                    Ask = new Bar
                    {
                        Open = g.First().Ask.Open,
                        High = g.Max(b => b.Ask.High),
                        Low = g.Min(b => b.Ask.Low),
                        Close = g.Last().Ask.Close
                    },
                    LastAskSize = g.Last().LastAskSize,
                    Value = g.Last().Close,
                    Period = period
                });
        }

        private IEnumerable<BaseData> GetIntradayBarData(HistoryRequest historyRequest)
        {
            // Bloomberg intraday bar requests only allow a single event type:
            // for TickType.Trade - single request for "TRADE"
            // for TickType.Quote - two requests for "BID" and "ASK" with result sets joined
            if (historyRequest.TickType == TickType.Quote)
            {
                var historyBid = GetIntradayBarData(historyRequest, "BID");
                var historyAsk = GetIntradayBarData(historyRequest, "ASK");

                return historyBid.Join(historyAsk,
                    bid => bid.Time,
                    ask => ask.Time,
                    (bid, ask) => new QuoteBar(
                        bid.Time,
                        bid.Symbol,
                        new Bar(bid.Open, bid.High, bid.Low, bid.Close),
                        bid.Volume,
                        new Bar(ask.Open, ask.High, ask.Low, ask.Close),
                        ask.Volume,
                        bid.Period));
            }

            return GetIntradayBarData(historyRequest, "TRADE");
        }

        private static IEnumerable<BaseData> AggregateTicksToTradeBars(IEnumerable<Tick> ticks, Symbol symbol, TimeSpan resolutionTimeSpan)
        {
            return
                from t in ticks
                group t by t.Time.RoundDown(resolutionTimeSpan)
                into g
                select new TradeBar
                {
                    Symbol = symbol,
                    Time = g.Key,
                    Period = resolutionTimeSpan,
                    Open = g.First().LastPrice,
                    High = g.Max(t => t.LastPrice),
                    Low = g.Min(t => t.LastPrice),
                    Close = g.Last().LastPrice
                };
        }

        private static IEnumerable<BaseData> AggregateTicksToQuoteBars(IEnumerable<Tick> ticks, Symbol symbol, TimeSpan resolutionTimeSpan)
        {
            return
                from t in ticks
                group t by t.Time.RoundDown(resolutionTimeSpan)
                into g
                select new QuoteBar
                {
                    Symbol = symbol,
                    Time = g.Key,
                    Period = resolutionTimeSpan,
                    Bid = new Bar
                    {
                        Open = g.First().BidPrice,
                        High = g.Max(b => b.BidPrice),
                        Low = g.Min(b => b.BidPrice),
                        Close = g.Last().BidPrice
                    },
                    Ask = new Bar
                    {
                        Open = g.First().AskPrice,
                        High = g.Max(b => b.AskPrice),
                        Low = g.Min(b => b.AskPrice),
                        Close = g.Last().AskPrice
                    }
                };
        }

        private IEnumerable<BaseData> GetHistoricalData(HistoryRequest historyRequest)
        {
            var request = _sessionSync
                .GetService("//blp/refdata")
                .CreateRequest("HistoricalDataRequest");

            var startDate = historyRequest.StartTimeUtc
                .ConvertFromUtc(historyRequest.ExchangeHours.TimeZone).Date;
            var endDate = historyRequest.EndTimeUtc
                .ConvertFromUtc(historyRequest.ExchangeHours.TimeZone).Date;

            request.Append("securities", _symbolMapper.GetBrokerageSymbol(historyRequest.Symbol));

            var fields = request.GetElement("fields");
            fields.AppendValue("OPEN");
            fields.AppendValue("HIGH");
            fields.AppendValue("LOW");
            fields.AppendValue("CLOSE");
            fields.AppendValue("VOLUME");

            request.Set("periodicitySelection", "DAILY");
            request.Set("startDate", startDate.ToString("yyyyMMdd"));
            request.Set("endDate", endDate.ToString("yyyyMMdd"));

            var correlationId = GetNewCorrelationId();
            _sessionSync.SendRequest(request, correlationId);

            // TODO: with real API - use reset event to wait for async responses received in OnBloombergEvent

            while (true)
            {
                var eventObj = _sessionSync.NextEvent();
                foreach (var msg in eventObj)
                {
                    if (Equals(msg.CorrelationID, correlationId))
                    {
                        var rows = msg.AsElement["securityData"]["fieldData"];

                        for (var i = 0; i < rows.NumValues; i++)
                        {
                            var row = rows.GetValueAsElement(i);
                            var date = row["date"].GetValueAsDate();

                            yield return new TradeBar
                            {
                                Symbol = historyRequest.Symbol,
                                Time = new DateTime(date.Year, date.Month, date.DayOfMonth),
                                Period = Time.OneDay,
                                Open = Convert.ToDecimal(row["OPEN"].GetValueAsFloat64()),
                                High = Convert.ToDecimal(row["HIGH"].GetValueAsFloat64()),
                                Low = Convert.ToDecimal(row["LOW"].GetValueAsFloat64()),
                                Close = Convert.ToDecimal(row["CLOSE"].GetValueAsFloat64()),
                                Volume = Convert.ToDecimal(row["VOLUME"].GetValueAsFloat64())
                            };
                        }
                    }
                }

                if (eventObj.Type == Event.EventType.RESPONSE)
                {
                    yield break;
                }
            }
        }

        private static int GetIntervalMinutes(Resolution resolution)
        {
            switch (resolution)
            {
                case Resolution.Hour:
                    return 60;

                case Resolution.Minute:
                    return 1;

                default:
                    throw new NotSupportedException($"GetIntervalMinutes(): resolution not supported: {resolution}");
            }
        }

        private IEnumerable<TradeBar> GetIntradayBarData(HistoryRequest historyRequest, string eventType)
        {
            var request = _sessionSync
                .GetService("//blp/refdata")
                .CreateRequest("IntradayBarRequest");

            var period = historyRequest.Resolution.ToTimeSpan();

            var startDateTime = historyRequest.StartTimeUtc
                .ConvertFromUtc(historyRequest.ExchangeHours.TimeZone);
            var endDateTime = historyRequest.EndTimeUtc
                .ConvertFromUtc(historyRequest.ExchangeHours.TimeZone);

            request.Set("security", _symbolMapper.GetBrokerageSymbol(historyRequest.Symbol));
            request.Set("eventType", eventType);

            request.Set("interval", GetIntervalMinutes(historyRequest.Resolution));
            request.Set("startDateTime", new Datetime(startDateTime.RoundDown(period)));
            request.Set("endDateTime", new Datetime(endDateTime.RoundDown(period)));

            var correlationId = GetNewCorrelationId();
            _sessionSync.SendRequest(request, correlationId);

            // TODO: with real API - use reset event to wait for async responses received in OnBloombergEvent

            while (true)
            {
                var eventObj = _sessionSync.NextEvent();
                foreach (var msg in eventObj)
                {
                    if (Equals(msg.CorrelationID, correlationId))
                    {
                        var rows = msg["barData"]["barTickData"];

                        for (var i = 0; i < rows.NumValues; i++)
                        {
                            var row = rows.GetValueAsElement(i);
                            var time = row["time"].GetValueAsDatetime();

                            var barTime = new DateTime(time.Year, time.Month, time.DayOfMonth, time.Hour, time.Minute, time.Second)
                                .ConvertFromUtc(historyRequest.ExchangeHours.TimeZone);

                            yield return new TradeBar
                            {
                                Symbol = historyRequest.Symbol,
                                Time = barTime,
                                Period = period,
                                Open = Convert.ToDecimal(row.GetElementAsFloat64("open")),
                                High = Convert.ToDecimal(row.GetElementAsFloat64("high")),
                                Low = Convert.ToDecimal(row.GetElementAsFloat64("low")),
                                Close = Convert.ToDecimal(row.GetElementAsFloat64("close")),
                                Volume = Convert.ToDecimal(row.GetElementAsInt64("volume"))
                            };
                        }
                    }
                }

                if (eventObj.Type == Event.EventType.RESPONSE)
                {
                    yield break;
                }
            }
        }

        private IEnumerable<Tick> GetIntradayTickData(HistoryRequest historyRequest)
        {
            var request = _sessionSync
                .GetService("//blp/refdata")
                .CreateRequest("IntradayTickRequest");

            var startDateTime = historyRequest.StartTimeUtc
                .ConvertFromUtc(historyRequest.ExchangeHours.TimeZone);
            var endDateTime = historyRequest.EndTimeUtc
                .ConvertFromUtc(historyRequest.ExchangeHours.TimeZone);

            request.Set("security", _symbolMapper.GetBrokerageSymbol(historyRequest.Symbol));

            if (historyRequest.TickType == TickType.Trade)
            {
                request.Append("eventTypes", "TRADE");
            }
            else if (historyRequest.TickType == TickType.Quote)
            {
                request.Append("eventTypes", "BID");
                request.Append("eventTypes", "ASK");
            }
            else
            {
                throw new NotSupportedException($"GetIntradayTickData(): unsupported tick type: {historyRequest.TickType}");
            }

            request.Set("startDateTime", new Datetime(startDateTime));
            request.Set("endDateTime", new Datetime(endDateTime));
            request.Set("includeConditionCodes", true);

            var correlationId = GetNewCorrelationId();
            _sessionSync.SendRequest(request, correlationId);

            // TODO: with real API - use reset event to wait for async responses received in OnBloombergEvent

            while (true)
            {
                var eventObj = _sessionSync.NextEvent();
                foreach (var msg in eventObj)
                {
                    if (Equals(msg.CorrelationID, correlationId))
                    {
                        var rows = msg["tickData"]["tickData"];

                        var bidPrice = 0m;
                        var askPrice = 0m;
                        var bidSize = 0m;
                        var askSize = 0m;
                        for (var i = 0; i < rows.NumValues; i++)
                        {
                            var row = rows.GetValueAsElement(i);
                            var time = row["time"].GetValueAsDatetime();

                            var tickTime = new DateTime(time.Year, time.Month, time.DayOfMonth, time.Hour, time.Minute, time.Second)
                                .ConvertFromUtc(historyRequest.ExchangeHours.TimeZone);
                            var type = row.GetElementAsString("type");
                            var value = Convert.ToDecimal(row.GetElementAsFloat64("value"));
                            var size = Convert.ToDecimal(row.GetElementAsFloat64("size"));

                            if (type == "TRADE")
                            {
                                yield return new Tick
                                {
                                    Symbol = historyRequest.Symbol,
                                    Time = tickTime,
                                    TickType = TickType.Trade,
                                    Value = value,
                                    Quantity = size
                                };
                            }
                            else if (type == "BID")
                            {
                                bidPrice = value;
                                bidSize = size;

                                if (bidPrice > 0 && askPrice > 0)
                                {
                                    yield return new Tick
                                    {
                                        Symbol = historyRequest.Symbol,
                                        Time = tickTime,
                                        TickType = TickType.Quote,
                                        BidPrice = bidPrice,
                                        AskPrice = askPrice,
                                        BidSize = bidSize,
                                        AskSize = askSize
                                    };
                                }
                            }
                            else if (type == "ASK")
                            {
                                askPrice = value;
                                askSize = size;

                                if (bidPrice > 0 && askPrice > 0)
                                {
                                    yield return new Tick
                                    {
                                        Symbol = historyRequest.Symbol,
                                        Time = tickTime,
                                        TickType = TickType.Quote,
                                        BidPrice = bidPrice,
                                        AskPrice = askPrice,
                                        BidSize = bidSize,
                                        AskSize = askSize
                                    };
                                }
                            }
                        }
                    }
                }

                if (eventObj.Type == Event.EventType.RESPONSE)
                {
                    yield break;
                }
            }
        }

        #endregion

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            _sessionAsync?.Dispose();
        }

        private List<Bloomberglp.Blpapi.Subscription> CreateNewBloombergSubscriptions(IEnumerable<Symbol> symbols)
        {
            var subscriptions = new List<Bloomberglp.Blpapi.Subscription>();

            foreach (var symbol in symbols)
            {
                if (!CanSubscribe(symbol))
                {
                    continue;
                }

                lock (_locker)
                {
                    var topicName = _symbolMapper.GetBrokerageSymbol(symbol);

                    BloombergSubscriptions symbolSubscriptions;
                    if (!_subscriptionsByTopicName.TryGetValue(topicName, out symbolSubscriptions))
                    {
                        symbolSubscriptions = new BloombergSubscriptions(symbol);
                        _subscriptionsByTopicName.TryAdd(topicName, symbolSubscriptions);
                    }

                    _symbolsByTopicName.AddOrUpdate(topicName, symbol);

                    var tickTypes = SubscriptionManager.DefaultDataTypes()[symbol.SecurityType];
                    foreach (var tickType in tickTypes)
                    {
                        var fields = GetBloombergFieldList(tickType);
                        var correlationId = GetNewCorrelationId();

                        var subscription = new Bloomberglp.Blpapi.Subscription(topicName, fields, correlationId);
                        subscriptions.Add(subscription);

                        symbolSubscriptions.Add(tickType, subscription, correlationId);
                    }
                }
            }

            return subscriptions;
        }

        private List<Bloomberglp.Blpapi.Subscription> RemoveExistingBloombergSubscriptions(IEnumerable<Symbol> symbols)
        {
            var subscriptions = new List<Bloomberglp.Blpapi.Subscription>();

            foreach (var symbol in symbols)
            {
                if (!CanSubscribe(symbol))
                {
                    continue;
                }

                lock (_locker)
                {
                    var topicName = _symbolMapper.GetBrokerageSymbol(symbol);

                    BloombergSubscriptions symbolSubscriptions;
                    if (_subscriptionsByTopicName.TryGetValue(topicName, out symbolSubscriptions))
                    {
                        subscriptions.AddRange(symbolSubscriptions);

                        symbolSubscriptions.Clear();
                    }
                }
            }

            return subscriptions;
        }

        private List<string> GetBloombergFieldList(TickType tickType)
        {
            // TODO: some of these field names are educated guesses and need to be confirmed with the real //blp/apiflds service

            switch (tickType)
            {
                case TickType.Quote:
                    return new List<string> { "BID", "ASK", "BID_SIZE", "ASK_SIZE" };

                case TickType.Trade:
                    return new List<string> { "LAST_PRICE", "LAST_TRADE_SIZE" };

                case TickType.OpenInterest:
                    return new List<string> { "OPEN_INTEREST" };

                default:
                    throw new NotSupportedException($"Unsupported tick type: {tickType}");
            }
        }

        private bool CanSubscribe(Symbol symbol)
        {
            var market = symbol.ID.Market;
            var securityType = symbol.ID.SecurityType;

            if (symbol.Value.IndexOfInvariant("universe", true) != -1) return false;

            return
                (securityType == SecurityType.Equity && market == Market.USA) ||
                (securityType == SecurityType.Forex && market == Market.FXCM) ||
                (securityType == SecurityType.Option && market == Market.USA) ||
                (securityType == SecurityType.Future);
        }

        private CorrelationID GetNewCorrelationId()
        {
            return new CorrelationID(Interlocked.Increment(ref _nextCorrelationId));
        }

        private void OnBloombergEvent(Event eventObj, Session session)
        {
            Log.Trace($"BloombergDataQueueHandler.OnBloombergEvent(): Type: {eventObj.Type} [{(int)eventObj.Type}]");

            switch (eventObj.Type)
            {
                case Event.EventType.SUBSCRIPTION_STATUS:
                    foreach (var message in eventObj.GetMessages())
                    {
                        if (message.MessageType.ToString() == "SubscriptionStarted")
                        {
                            Log.Trace($"BloombergDataQueueHandler.OnBloombergEvent(): [{message.TopicName}] subscription started");
                        }
                        else if (message.MessageType.ToString() == "SubscriptionTerminated")
                        {
                            Log.Trace($"BloombergDataQueueHandler.OnBloombergEvent(): [{message.TopicName}] subscription terminated");
                        }
                        else
                        {
                            Log.Trace(message.ToString());
                        }
                    }

                    break;

                case Event.EventType.SUBSCRIPTION_DATA:
                    foreach (var message in eventObj.GetMessages())
                    {
                        //Log.Trace($"BloombergDataQueueHandler.OnBloombergEvent(): {message}");

                        BloombergSubscriptions subscriptions;
                        if (_subscriptionsByTopicName.TryGetValue(message.TopicName, out subscriptions))
                        {
                            var tickType = subscriptions.GetTickType(message.CorrelationID);

                            switch (tickType)
                            {
                                case TickType.Trade:
                                    EmitTradeTick(subscriptions.Symbol, message);
                                    break;
                                case TickType.Quote:
                                    EmitQuoteTick(subscriptions.Symbol, message);
                                    break;
                                case TickType.OpenInterest:
                                    EmitOpenInterestTick(subscriptions.Symbol, message);
                                    break;
                            }
                        }
                        else
                        {
                            Log.Error($"BloombergDataQueueHandler.OnBloombergEvent(): TopicName not found: {message.TopicName}");
                        }
                    }

                    break;

                default:
                    foreach (var message in eventObj.GetMessages())
                    {
                        Log.Trace($"BloombergDataQueueHandler.OnBloombergEvent(): {message}");
                    }

                    break;
            }
        }

        private T GetBloombergFieldValue<T>(Message message, string field) where T : new()
        {
            if (!message.HasElement(field, true))
            {
                return default(T);
            }

            var element = message[field];

            var value = (T)element.GetValue().ConvertInvariant(typeof(T));

            return value;
        }

        private void EmitTradeTick(Symbol symbol, Message message)
        {
            var price = GetBloombergFieldValue<decimal>(message, "LAST_PRICE");
            var quantity = GetBloombergFieldValue<decimal>(message, "LAST_TRADE_SIZE");

            lock (_locker)
            {
                _ticks.Add(new Tick
                {
                    Symbol = symbol,
                    Time = DateTime.UtcNow.ConvertFromUtc(TimeZones.NewYork),
                    TickType = TickType.Trade,
                    Value = price,
                    Quantity = quantity
                });
            }
        }

        private void EmitQuoteTick(Symbol symbol, Message message)
        {
            var bidPrice = GetBloombergFieldValue<decimal>(message, "BID");
            var askPrice = GetBloombergFieldValue<decimal>(message, "ASK");
            var bidSize = GetBloombergFieldValue<decimal>(message, "BID_SIZE");
            var askSize = GetBloombergFieldValue<decimal>(message, "ASK_SIZE");

            lock (_locker)
            {
                _ticks.Add(new Tick
                {
                    Symbol = symbol,
                    Time = DateTime.UtcNow.ConvertFromUtc(TimeZones.NewYork),
                    TickType = TickType.Quote,
                    BidPrice = bidPrice,
                    AskPrice = askPrice,
                    BidSize = bidSize,
                    AskSize = askSize
                });
            }
        }

        private void EmitOpenInterestTick(Symbol symbol, Message message)
        {
            var openInterest = GetBloombergFieldValue<decimal>(message, "OPEN_INTEREST");

            lock (_locker)
            {
                _ticks.Add(new Tick
                {
                    Symbol = symbol,
                    Time = DateTime.UtcNow.ConvertFromUtc(TimeZones.NewYork),
                    TickType = TickType.OpenInterest,
                    Value = openInterest
                });
            }
        }
    }
}