/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
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
                    var topicName = _symbolMapper.GetBrokerageSymbol(symbol);

                    var symbolSubscriptions = new BloombergSubscriptions(symbol);
                    if (!_subscriptionsByTopicName.TryAdd(topicName, symbolSubscriptions)) continue;

                    var tickTypes = SubscriptionManager.DefaultDataTypes()[symbol.SecurityType];
                    foreach (var tickType in tickTypes)
                    {
                        var fields = GetBloombergFieldList(tickType);
                        var correlationId = GetNewCorrelationId();

                        var subscription = new Subscription(topicName, fields, correlationId);
                        subscriptions.Add(subscription);

                        symbolSubscriptions.Add(tickType, subscription, correlationId);
                        if (!_subscriptionKeysByCorrelationId.TryAdd(correlationId, new BloombergSubscriptionKey(correlationId, symbol, tickType)))
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
                    return new List<string> { BloombergFieldNames.Bid, BloombergFieldNames.Ask, BloombergFieldNames.BidSize, BloombergFieldNames.AskSize };

                case TickType.Trade:
                    return new List<string> { BloombergFieldNames.LastPrice, BloombergFieldNames.LastTradeSize };

                case TickType.OpenInterest:
                    return new List<string> { BloombergFieldNames.OpenInterest };

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

        private void OnBloombergMarketDataEvent(Event eventObj, Session session)
        {
            Log.Trace($"BloombergBrokerage.OnBloombergMarketDataEvent(): Type: {eventObj.Type} [{(int)eventObj.Type}]");

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
                            case "SubscriptionTerminated":
                                Log.Trace(prefix + "subscription terminated");
                                break;
                            case "SubscriptionFailure":
                                Log.Error($"{prefix}subscription failed: {DescribeCorrelationIds(message.CorrelationIDs)}");
                                break;
                            default:
                                Log.Trace(message.ToString());
                                break;
                        }
                    }

                    break;

                case Event.EventType.SUBSCRIPTION_DATA:
                    foreach (var message in eventObj.GetMessages())
                    {
                        //Log.Trace($"BloombergBrokerage.OnBloombergMarketDataEvent(): {message}");

                        foreach (var correlationId in message.CorrelationIDs)
                        {
                            if (_subscriptionKeysByCorrelationId.TryGetValue(correlationId, out var key))
                            {
                                Log.Trace("BloombergBrokerage.OnBloombergMarketDataEvent(): subscription data: " + DescribeCorrelationId(correlationId, key));
                                switch (key.TickType)
                                {
                                    case TickType.Trade:
                                        EmitTradeTick(key.Symbol, message);
                                        break;
                                    case TickType.Quote:
                                        EmitQuoteTick(key.Symbol, message);
                                        break;
                                    case TickType.OpenInterest:
                                        EmitOpenInterestTick(key.Symbol, message);
                                        break;
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
                    foreach (var message in eventObj.GetMessages())
                    {
                        Log.Trace($"BloombergBrokerage.OnBloombergMarketDataEvent(): {message}");
                    }

                    break;
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

                    _subscriptionKeysByCorrelationId.TryGetValue(id, out var key);
                    return s.Append(DescribeCorrelationId(id, key));
                })
                .ToString();
        }

        private string DescribeCorrelationId(CorrelationID id, BloombergSubscriptionKey key)
        {
            if (key == null) return "UnknownCorrelationId:" + id.Value;

            var bbgTicker = _symbolMapper.GetBrokerageSymbol(key.Symbol);
            return $"bbg:{bbgTicker}|lean:{key.Symbol.Value}|tick:{key.TickType}";
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
            var price = GetBloombergFieldValue<decimal>(message, BloombergFieldNames.LastPrice);
            var quantity = GetBloombergFieldValue<decimal>(message, BloombergFieldNames.LastTradeSize);

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
            var bidPrice = GetBloombergFieldValue<decimal>(message, BloombergFieldNames.Bid);
            var askPrice = GetBloombergFieldValue<decimal>(message, BloombergFieldNames.Ask);
            var bidSize = GetBloombergFieldValue<decimal>(message, BloombergFieldNames.BidSize);
            var askSize = GetBloombergFieldValue<decimal>(message, BloombergFieldNames.AskSize);

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
            var openInterest = GetBloombergFieldValue<decimal>(message, BloombergFieldNames.OpenInterest);

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
