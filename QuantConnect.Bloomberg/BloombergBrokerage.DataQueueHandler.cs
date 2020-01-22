/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Generic;
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

                        var subscription = new Subscription(topicName, fields, correlationId);
                        subscriptions.Add(subscription);

                        symbolSubscriptions.Add(tickType, subscription, correlationId);
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

        private void OnBloombergMarketDataEvent(Event eventObj, Session session)
        {
            Log.Trace($"BloombergBrokerage.OnBloombergMarketDataEvent(): Type: {eventObj.Type} [{(int)eventObj.Type}]");

            switch (eventObj.Type)
            {
                case Event.EventType.SUBSCRIPTION_STATUS:
                    foreach (var message in eventObj.GetMessages())
                    {
                        if (message.MessageType.ToString() == "SubscriptionStarted")
                        {
                            Log.Trace($"BloombergBrokerage.OnBloombergMarketDataEvent(): [{message.TopicName}] subscription started");
                        }
                        else if (message.MessageType.ToString() == "SubscriptionTerminated")
                        {
                            Log.Trace($"BloombergBrokerage.OnBloombergMarketDataEvent(): [{message.TopicName}] subscription terminated");
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
                        //Log.Trace($"BloombergBrokerage.OnBloombergMarketDataEvent(): {message}");

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
                            Log.Error($"BloombergBrokerage.OnBloombergMarketDataEvent(): TopicName not found: {message.TopicName}");
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
