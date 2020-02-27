/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Bloomberglp.Blpapi;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Packets;

namespace QuantConnect.Bloomberg
{
    /// <summary>
    /// An implementation of <see cref="IDataQueueHandler"/> for Bloomberg
    /// </summary>
    public partial class BloombergBrokerage
    {
        private readonly object _locker = new object();
        private readonly List<Tick> _ticks = new List<Tick>();
        private readonly ConcurrentDictionary<string, Subscription> _subscriptionsByTopicName = new ConcurrentDictionary<string, Subscription>();
        private readonly ConcurrentDictionary<CorrelationID, BloombergSubscriptionData> _subscriptionDataByCorrelationId =
            new ConcurrentDictionary<CorrelationID, BloombergSubscriptionData>();

        private readonly List<string> _fieldList = new List<string>
        {
            // Quotes
            BloombergFieldNames.Bid, BloombergFieldNames.Ask, BloombergFieldNames.BidSize, BloombergFieldNames.AskSize,

            // Trades
            BloombergFieldNames.LastPrice, BloombergFieldNames.LastTradeSize,

            // OpenInterest
            BloombergFieldNames.OpenInterest
        };

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
                    Log.Trace($"BloombergBrokerage.Subscribe(): {subscription.SubscriptionString}");
                }

                _sessionMarketData.Subscribe(subscriptions);
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
                    Log.Trace($"BloombergBrokerage.Unsubscribe(): {subscription.SubscriptionString}");
                }

                _sessionMarketData.Unsubscribe(subscriptions);
            }
        }

        #endregion

        private List<Subscription> CreateNewBloombergSubscriptions(IEnumerable<Symbol> symbols)
        {
            var subscriptions = new List<Subscription>();

            foreach (var symbol in symbols)
            {
                if (!CanSubscribe(symbol))
                {
                    continue;
                }

                lock (_locker)
                {
                    var subscribeSymbol = symbol;

                    if (symbol.SecurityType == SecurityType.Future && symbol.IsCanonical())
                    {
                        // future canonical symbol - ignore
                        continue;
                    }

                    if (symbol.SecurityType == SecurityType.Option && symbol.IsCanonical())
                    {
                        // option canonical symbol - subscribe to the underlying
                        subscribeSymbol = symbol.Underlying;
                    }

                    var topicName = _symbolMapper.GetBrokerageSymbol(subscribeSymbol);

                    if (_subscriptionsByTopicName.ContainsKey(topicName))
                    {
                        // already subscribed
                        continue;
                    }

                    var correlationId = GetNewCorrelationId();

                    var subscription = new Subscription(topicName, _fieldList, correlationId);
                    subscriptions.Add(subscription);

                    _subscriptionsByTopicName.TryAdd(topicName, subscription);

                    var exchangeHours = _marketHoursDatabase.GetExchangeHours(subscribeSymbol.ID.Market, subscribeSymbol, subscribeSymbol.SecurityType);

                    if (!_subscriptionDataByCorrelationId.TryAdd(correlationId, new BloombergSubscriptionData(correlationId, subscribeSymbol, exchangeHours.TimeZone)))
                    {
                        throw new Exception("Duplicate correlation id: " + correlationId);
                    }
                }
            }

            return subscriptions;
        }

        private List<Subscription> RemoveExistingBloombergSubscriptions(IEnumerable<Symbol> symbols)
        {
            var subscriptions = new List<Subscription>();

            foreach (var symbol in symbols)
            {
                if (!CanSubscribe(symbol))
                {
                    continue;
                }

                lock (_locker)
                {
                    var topicName = _symbolMapper.GetBrokerageSymbol(symbol);

                    if (_subscriptionsByTopicName.TryGetValue(topicName, out var subscription))
                    {
                        subscriptions.Add(subscription);
                    }
                }
            }

            return subscriptions;
        }

        private static bool CanSubscribe(Symbol symbol)
        {
            var market = symbol.ID.Market;
            var securityType = symbol.ID.SecurityType;

            if (symbol.Value.IndexOfInvariant("universe", true) != -1)
            {
                return false;
            }

            return
                securityType == SecurityType.Equity && market == Market.USA ||
                securityType == SecurityType.Forex && market == Market.FXCM ||
                securityType == SecurityType.Option && market == Market.USA ||
                securityType == SecurityType.Future;
        }

        private CorrelationID GetNewCorrelationId()
        {
            return new CorrelationID(Interlocked.Increment(ref _nextCorrelationId));
        }

        private void OnBloombergMarketDataEvent(Event eventObj, Session session)
        {
            //Log.Trace($"BloombergBrokerage.OnBloombergMarketDataEvent(): Type: {eventObj.Type} [{(int)eventObj.Type}]");

            switch (eventObj.Type)
            {
                case Event.EventType.SUBSCRIPTION_STATUS:
                    foreach (var message in eventObj.GetMessages())
                    {
                        var prefix = $"BloombergBrokerage.OnBloombergMarketDataEvent(): [{message.TopicName}] ";
                        switch (message.MessageType.ToString())
                        {
                            case "SubscriptionStarted":
                                Log.Trace(prefix + "subscription started");
                                break;
                            case "SubscriptionStreamsActivated":
                                Log.Trace(prefix + "subscription activated");
                                break;
                            case "SubscriptionTerminated":
                                Log.Trace(prefix + "subscription terminated");
                                break;
                            case "SubscriptionFailure":
                                Log.Error($"{prefix}subscription failed: {DescribeCorrelationIds(message.CorrelationIDs)}{message}");
                                break;
                            default: throw new Exception($"Message type is not yet handled: {message.MessageType} [message:{message}]");
                        }
                    }

                    break;

                case Event.EventType.SUBSCRIPTION_DATA:
                    foreach (var message in eventObj.GetMessages())
                    {
                        //Log.Trace($"BloombergBrokerage.OnBloombergMarketDataEvent(): {message}");

                        var eventType = message.GetElement(BloombergNames.MktdataEventType).GetValueAsName();
                        var eventSubtype = message.GetElement(BloombergNames.MktdataEventSubtype).GetValueAsName();

                        foreach (var correlationId in message.CorrelationIDs)
                        {
                            if (_subscriptionDataByCorrelationId.TryGetValue(correlationId, out var data))
                            {
                                //Log.Trace($"BloombergBrokerage.OnBloombergMarketDataEvent(): " +
                                //          $"subscription data: {DescribeCorrelationId(correlationId, data)} " +
                                //          $"MktdataEventType: {eventType} " +
                                //          $"MktdataEventSubType: {eventSubtype}");

                                if (Equals(eventType, BloombergNames.Quote) ||
                                    Equals(eventType, BloombergNames.Summary) && Equals(eventSubtype, BloombergNames.InitPaint))
                                {
                                    if (Equals(eventSubtype, BloombergNames.Bid) || Equals(eventSubtype, BloombergNames.Ask))
                                    {
                                        EmitQuoteTick(message, data);
                                    }
                                }
                                else if (Equals(eventType, BloombergNames.Trade) && Equals(eventSubtype, BloombergNames.New))
                                {
                                    EmitTradeTick(message, data);
                                }

                                if (message.HasElement(BloombergFieldNames.OpenInterest))
                                {
                                    EmitOpenInterestTick(message, data);
                                }
                            }
                            else
                            {
                                Log.Error($"BloombergBrokerage.OnBloombergMarketDataEvent(): Correlation Id not found: {correlationId} [message topic:{message.TopicName}]");
                            }
                        }
                    }

                    break;
                default: 
                    Log.Error("Unhandled event type: {0}, event:{1}", eventObj.Type, eventObj);
                    break;
            }
        }

        private void EmitQuoteTick(Message message, BloombergSubscriptionData data)
        {
            if (message.HasElement(BloombergFieldNames.Bid, true))
            {
                data.BidPrice = GetBloombergFieldValue<decimal>(message, BloombergFieldNames.Bid);
            }

            if (message.HasElement(BloombergFieldNames.BidSize, true))
            {
                data.BidSize = GetBloombergFieldValue<decimal>(message, BloombergFieldNames.BidSize);
            }

            if (message.HasElement(BloombergFieldNames.Ask, true))
            {
                data.AskPrice = GetBloombergFieldValue<decimal>(message, BloombergFieldNames.Ask);
            }

            if (message.HasElement(BloombergFieldNames.AskSize, true))
            {
                data.AskSize = GetBloombergFieldValue<decimal>(message, BloombergFieldNames.AskSize);
            }

            if (data.IsQuoteValid())
            {
                var time = GetTickTime(message, data);

                lock (_locker)
                {
                    _ticks.Add(new Tick
                    {
                        Symbol = data.Symbol,
                        Time = time,
                        TickType = TickType.Quote,
                        BidPrice = data.BidPrice,
                        AskPrice = data.AskPrice,
                        BidSize = data.BidSize,
                        AskSize = data.AskSize,
                        Value = (data.BidPrice + data.AskPrice) / 2
                    });
                }
            }
        }

        private void EmitTradeTick(Message message, BloombergSubscriptionData data)
        {
            if (data.Symbol.SecurityType == SecurityType.Forex)
            {
                return;
            }

            var price = GetBloombergFieldValue<decimal>(message, BloombergFieldNames.TradePrice);
            var quantity = GetBloombergFieldValue<decimal>(message, BloombergFieldNames.TradeSize);
            var time = GetTickTime(message, data);

            lock (_locker)
            {
                _ticks.Add(new Tick
                {
                    Symbol = data.Symbol,
                    Time = time,
                    TickType = TickType.Trade,
                    Value = price,
                    Quantity = quantity
                });
            }
        }

        private void EmitOpenInterestTick(Message message, BloombergSubscriptionData data)
        {
            var openInterest = GetBloombergFieldValue<decimal>(message, BloombergFieldNames.OpenInterest);
            var time = GetTickTime(message, data);

            lock (_locker)
            {
                _ticks.Add(new Tick
                {
                    Symbol = data.Symbol,
                    Time = time,
                    TickType = TickType.OpenInterest,
                    Value = openInterest
                });
            }
        }

        private string DescribeCorrelationIds(IEnumerable<CorrelationID> correlationIds)
        {
            return correlationIds?.Aggregate(new StringBuilder(), (s, id) =>
                {
                    if (s.Length > 0)
                    {
                        s.Append(',');
                    }

                    _subscriptionDataByCorrelationId.TryGetValue(id, out var key);
                    return s.Append(DescribeCorrelationId(id, key));
                })
                .ToString();
        }

        private string DescribeCorrelationId(CorrelationID id, BloombergSubscriptionData key)
        {
            if (key == null) return "UnknownCorrelationId:" + id.Value;

            var bbgTicker = _symbolMapper.GetBrokerageSymbol(key.Symbol);
            return $"bbg:{bbgTicker}|lean:{key.Symbol.Value}";
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

        private string GetBloombergFieldValue(Message message, string field)
        {
            return message.HasElement(field, true) ? message[field].GetValue().ToString() : string.Empty;
        }

        private DateTime GetTickTime(Message message, BloombergSubscriptionData data)
        {
            // TODO: get trade date in user time zone (TradeDate field is empty)
            //var time = TimeSpan.Parse(GetBloombergFieldValue(message, BloombergFieldNames.TradeTime), CultureInfo.InvariantCulture);
            //var date = GetBloombergFieldValue(message, BloombergFieldNames.TradeDate);
            //return date.Add(time);

            var utcTime = DateTime.UtcNow;

            return utcTime.ConvertFromUtc(data.ExchangeTimeZone);
        }

    }
}
