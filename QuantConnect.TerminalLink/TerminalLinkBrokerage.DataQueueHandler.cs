/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Linq;
using System.Text;
using System.Threading;
using QuantConnect.Data;
using Bloomberglp.Blpapi;
using System.Globalization;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.Interfaces;
using QuantConnect.Brokerages;
using QuantConnect.Data.Market;
using System.Collections.Generic;
using System.Collections.Concurrent;
using QuantConnect.Util;
using QuantConnect.Configuration;

namespace QuantConnect.TerminalLink
{
    /// <summary>
    /// An implementation of <see cref="IDataQueueHandler"/> for TerminalLink
    /// </summary>
    public partial class TerminalLinkBrokerage
    {
        private readonly object _locker = new object();
        private IDataAggregator _dataAggregator;
        private EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager;
        private readonly ConcurrentDictionary<string, Subscription> _subscriptionsByTopicName = new ConcurrentDictionary<string, Subscription>();
        private readonly ConcurrentDictionary<CorrelationID, TerminalLinkSubscriptionData> _subscriptionDataByCorrelationId =
            new ConcurrentDictionary<CorrelationID, TerminalLinkSubscriptionData>();

        private readonly List<string> _fieldList = new List<string>
        {
            // Quotes
            TerminalLinkFieldNames.Bid, TerminalLinkFieldNames.Ask, TerminalLinkFieldNames.BidSize, TerminalLinkFieldNames.AskSize,

            // Trades
            TerminalLinkFieldNames.LastPrice, TerminalLinkFieldNames.LastTradeSize,

            // OpenInterest
            TerminalLinkFieldNames.OpenInterest
        };

        #region IDataQueueHandler implementation

        /// <summary>
        /// Subscribe to the specified configuration
        /// </summary>
        /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
        /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
        /// <returns>The new enumerator for this subscription request</returns>
        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, System.EventHandler newDataAvailableHandler)
        {
            if (!CanSubscribe(dataConfig.Symbol))
            {
                return null;
            }

            var enumerator = _dataAggregator.Add(dataConfig, newDataAvailableHandler);
            _subscriptionManager.Subscribe(dataConfig);

            return enumerator;
        }

        /// <summary>
        /// Removes the specified configuration
        /// </summary>
        /// <param name="dataConfig">Subscription config to be removed</param>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            _subscriptionManager.Unsubscribe(dataConfig);
            _dataAggregator.Remove(dataConfig);
        }

        public void SetJob(LiveNodePacket job)
        {
            // read values from the brokerage data
            Enum.TryParse(job.BrokerageData["terminal-link-api-type"], out ApiType apiType);
            Enum.TryParse(job.BrokerageData["terminal-link-environment"], out Environment environment);
            int.TryParse(job.BrokerageData["terminal-link-server-port"], out int serverPort);
            var serverHost = job.BrokerageData["terminal-link-server-host"];
            var symbolMapFile = job.BrokerageData["terminal-link-symbol-map-file"];

            var symbolMapper = Composer.Instance.GetExportedValues<ITerminalLinkSymbolMapper>().FirstOrDefault();
            if (symbolMapper == null)
            {
                symbolMapper = new TerminalLinkSymbolMapper(symbolMapFile);
                Composer.Instance.AddPart<ISymbolMapper>(symbolMapper);
            }

            var aggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(
                Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager"));

            Initialize(apiType, environment, serverHost, serverPort, symbolMapper, aggregator);

            if (!IsConnected)
            {
                Connect();
            }
        }

        /// <summary>
        /// Adds the specified symbols to the subscription
        /// </summary>
        /// <param name="symbols">The symbols to be added keyed by SecurityType</param>
        private bool Subscribe(IEnumerable<Symbol> symbols)
        {
            var subscriptions = CreateNewTerminalLinkSubscriptions(symbols);

            if (subscriptions.Count > 0)
            {
                foreach (var subscription in subscriptions)
                {
                    Log.Trace($"TerminalLinkBrokerage.Subscribe(): {subscription.SubscriptionString}");
                }

                _sessionMarketData.Subscribe(subscriptions);
                return true;
            }
            return false;
        }

        /// <summary>
        /// Removes the specified symbols to the subscription
        /// </summary>
        /// <param name="symbols">The symbols to be removed keyed by SecurityType</param>
        private bool Unsubscribe(IEnumerable<Symbol> symbols)
        {
            var subscriptions = RemoveExistingTerminalLinkSubscriptions(symbols);

            if (subscriptions.Count == 0)
            {
                return false;
            }

            Log.Trace($"TerminalLinkBrokerage.Unsubscribe(): Count={subscriptions.Count}: {string.Join(",", subscriptions.Select(x => x.SubscriptionString))}");
            try
            {
                _sessionMarketData.Unsubscribe(subscriptions);
                return true;
            }
            catch (Exception e)
            {
                Log.Error(e,
                    $"Failed to unsubscribe from market data cleanly [{subscriptions.Count} subscriptions: {string.Join(",", subscriptions.Select(x => x.SubscriptionString))}]");
            }
            return false;
        }

        #endregion

        private List<Subscription> CreateNewTerminalLinkSubscriptions(IEnumerable<Symbol> symbols)
        {
            var subscriptions = new List<Subscription>();

            foreach (var symbol in symbols)
            {
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

                    if (!_subscriptionDataByCorrelationId.TryAdd(correlationId, new TerminalLinkSubscriptionData(correlationId, subscribeSymbol, exchangeHours.TimeZone)))
                    {
                        throw new Exception("Duplicate correlation id: " + correlationId);
                    }
                }
            }

            return subscriptions;
        }

        private List<Subscription> RemoveExistingTerminalLinkSubscriptions(IEnumerable<Symbol> symbols)
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
            var securityType = symbol.ID.SecurityType;

            if (symbol.Value.IndexOfInvariant("universe", true) != -1)
            {
                return false;
            }

            return securityType == SecurityType.Equity || securityType == SecurityType.Forex || securityType == SecurityType.Option || securityType == SecurityType.Future;
        }

        internal static CorrelationID GetNewCorrelationId()
        {
            return new CorrelationID(Interlocked.Increment(ref _nextCorrelationId));
        }

        private void OnTerminalLinkMarketDataEvent(Event eventObj, Session session)
        {
            foreach (var message in eventObj.GetMessages())
            {
                switch (eventObj.Type)
                {
                    case Event.EventType.SUBSCRIPTION_STATUS:
                        var prefix = $"TerminalLinkBrokerage.OnTerminalLinkMarketDataEvent(): [{message.TopicName}] ";
                        switch (message.MessageType.ToString())
                        {
                            case "SubscriptionStarted":
                                Log.Trace(prefix + "subscription started");
                                break;
                            case "SubscriptionStreamsActivated":
                                Log.Trace(prefix + "subscription activated");
                                break;
                            case "SubscriptionTerminated":
                                if (message.HasElement("reason") &&
                                    message.GetElement("reason").HasElement("category") &&
                                    message.GetElement("reason").GetElementAsString("category") == "CANCELED")
                                {
                                    BrokerMessage(BrokerageMessageType.Information, message);
                                    Log.Trace(prefix + "subscription canceled");
                                }
                                else
                                {
                                    BrokerMessage(BrokerageMessageType.Disconnect, message);
                                    Log.Error(prefix + "subscription terminated");
                                }

                                break;
                            case "SubscriptionFailure":
                                Log.Error($"{prefix}subscription failed: {DescribeCorrelationIds(message.CorrelationIDs)}{message}");
                                BrokerMessage(BrokerageMessageType.Error, message);
                                break;
                            default:
                                Log.Error($"Message type is not yet handled: {message.MessageType} [message:{message}]");
                                BrokerMessage(BrokerageMessageType.Error, message);
                                break;
                        }

                        break;

                    case Event.EventType.SUBSCRIPTION_DATA:
                        var eventType = message.GetElement(TerminalLinkNames.MktdataEventType).GetValueAsName();
                        var eventSubtype = message.GetElement(TerminalLinkNames.MktdataEventSubtype).GetValueAsName();

                        foreach (var correlationId in message.CorrelationIDs)
                        {
                            if (_subscriptionDataByCorrelationId.TryGetValue(correlationId, out var data))
                            {
                                if (Equals(eventType, TerminalLinkNames.Summary))
                                {
                                    if (Equals(eventSubtype, TerminalLinkNames.InitPaint))
                                    {
                                        EmitQuoteTick(message, data, eventSubtype);
                                    }
                                }
                                else if (Equals(eventType, TerminalLinkNames.Quote))
                                {
                                    if (Equals(eventSubtype, TerminalLinkNames.Bid) || Equals(eventSubtype, TerminalLinkNames.Ask))
                                    {
                                        EmitQuoteTick(message, data, eventSubtype);
                                    }
                                }
                                else if (Equals(eventType, TerminalLinkNames.Trade) && Equals(eventSubtype, TerminalLinkNames.New))
                                {
                                    EmitTradeTick(message, data);
                                }

                                if (message.HasElement(TerminalLinkFieldNames.OpenInterest))
                                {
                                    EmitOpenInterestTick(message, data);
                                }
                            }
                            else
                            {
                                Log.Error($"TerminalLinkBrokerage.OnTerminalLinkMarketDataEvent(): Correlation Id not found: {correlationId} [message topic:{message.TopicName}]");
                                BrokerMessage(BrokerageMessageType.Error, message);
                            }
                        }

                        break;
                    case Event.EventType.SESSION_STATUS:
                        Log.Trace("TerminalLinkBrokerage.OnTerminalLinkMarketDataEvent(): Session Status: {0}", message);
                        switch (message.MessageType.ToString())
                        {
                            case "SessionConnectionDown":
                                BrokerMessage(BrokerageMessageType.Error, message);
                                break;
                            case "SessionConnectionUp":
                            default:
                                BrokerMessage(BrokerageMessageType.Information, message);
                                break;
                        }
                        break;
                    case Event.EventType.SERVICE_STATUS:
                        Log.Trace("TerminalLinkBrokerage.OnTerminalLinkMarketDataEvent(): Service Status: {0}", message);
                        break;
                    default:
                        Log.Trace("TerminalLinkBrokerage.OnTerminalLinkMarketDataEvent(): Unhandled event type: {0}, message:{1}", eventObj.Type, message);
                        break;
                }
            }
        }

        private void EmitQuoteTick(Message message, TerminalLinkSubscriptionData data, Name eventSubtype)
        {
            if (message.HasElement(TerminalLinkFieldNames.Bid, true))
            {
                data.BidPrice = GetTerminalLinkFieldValue<decimal>(message, TerminalLinkFieldNames.Bid);
            }

            if (message.HasElement(TerminalLinkFieldNames.BidSize, true))
            {
                data.BidSize = GetTerminalLinkFieldValue<decimal>(message, TerminalLinkFieldNames.BidSize);
            }

            if (message.HasElement(TerminalLinkFieldNames.Ask, true))
            {
                data.AskPrice = GetTerminalLinkFieldValue<decimal>(message, TerminalLinkFieldNames.Ask);
            }

            if (message.HasElement(TerminalLinkFieldNames.AskSize, true))
            {
                data.AskSize = GetTerminalLinkFieldValue<decimal>(message, TerminalLinkFieldNames.AskSize);
            }

            if (data.IsQuoteValid())
            {
                var localTickTime = GetLocalTickTime(message, data, TickType.Quote, eventSubtype);

                var tick = new Tick
                {
                    Symbol = data.Symbol,
                    Time =  localTickTime,
                    TickType = TickType.Quote,
                    BidPrice = data.BidPrice,
                    AskPrice = data.AskPrice,
                    BidSize = data.BidSize,
                    AskSize = data.AskSize,
                    Value = (data.BidPrice + data.AskPrice) / 2
                };

                lock (_dataAggregator)
                {
                    _dataAggregator.Update(tick);
                }

                if (Log.DebuggingEnabled)
                {
                    Name terminalLinkName;
                    if (Equals(eventSubtype, TerminalLinkNames.Bid))
                    {
                        terminalLinkName = TerminalLinkNames.BidUpdateStamp;
                    }
                    else if (Equals(eventSubtype, TerminalLinkNames.Ask))
                    {
                        terminalLinkName = TerminalLinkNames.AskUpdateStamp;
                    }
                    else
                    {
                        terminalLinkName = TerminalLinkNames.BidAskTime;
                    }

                    var datestamp = GetTerminalLinkFieldValue(message, TerminalLinkNames.TradingDateTime);
                    var timestamp = GetTerminalLinkFieldValue(message, terminalLinkName);

                    Log.Debug("[Quote] " +
                              $"Datestamp: {datestamp}, " +
                              $"Timestamp: {timestamp} " +
                              $"LocalTickTime: {localTickTime:O}, " +
                              $"Symbol: {data.Symbol}, " +
                              $"BidPrice: {data.BidPrice}, " +
                              $"BidSize: {data.BidSize}, " +
                              $"AskPrice: {data.AskPrice}, " +
                              $"AskSize: {data.AskSize}");
                }
            }
        }

        private void EmitTradeTick(Message message, TerminalLinkSubscriptionData data)
        {
            if (data.Symbol.SecurityType == SecurityType.Forex)
            {
                return;
            }

            var price = GetTerminalLinkFieldValue<decimal>(message, TerminalLinkFieldNames.TradePrice);
            var quantity = GetTerminalLinkFieldValue<decimal>(message, TerminalLinkFieldNames.TradeSize);
            var localTickTime = GetLocalTickTime(message, data, TickType.Trade);

            var tick = new Tick
            {
                Symbol = data.Symbol,
                Time =  localTickTime,
                TickType = TickType.Trade,
                Value = price,
                Quantity = quantity
            };

            lock (_dataAggregator)
            {
                _dataAggregator.Update(tick);
            }

            if (Log.DebuggingEnabled)
            {
                var tradeUpdateStamp = GetTerminalLinkFieldValue(message, TerminalLinkNames.TradeUpdateStamp);
                var eventTradeTimeRealTime = GetTerminalLinkFieldValue(message, TerminalLinkNames.EventTradeTimeRealTime);

                Log.Debug("[Trade] " +
                          $"TradeUpdateStamp: {tradeUpdateStamp}, " +
                          $"EventTradeTimeRealTime: {eventTradeTimeRealTime} " +
                          $"LocalTickTime: {localTickTime:O}, " +
                          $"Symbol: {data.Symbol}, " +
                          $"Price: {price}, " +
                          $"Quantity: {quantity}");
            }
        }

        private void EmitOpenInterestTick(Message message, TerminalLinkSubscriptionData data)
        {
            var openInterest = GetTerminalLinkFieldValue<decimal>(message, TerminalLinkFieldNames.OpenInterest);
            if (openInterest == 0)
            {
                return;
            }

            var localTickTime = GetLocalTickTime(message, data, TickType.OpenInterest);

            var tick = new Tick
            {
                Symbol = data.Symbol,
                Time =  localTickTime,
                TickType = TickType.OpenInterest,
                Value = openInterest
            };

            lock (_dataAggregator)
            {
                _dataAggregator.Update(tick);
            }

            if (Log.DebuggingEnabled)
            {
                var openInterestDate = GetTerminalLinkFieldValue(message, TerminalLinkNames.OpenInterestDate);

                Log.Debug("[OpenInterest] " +
                          $"OpenInterestDate: {openInterestDate}, " +
                          $"LocalTickTime: {localTickTime:O}, " +
                          $"Symbol: {data.Symbol}, " +
                          $"Value: {openInterest}");
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

        private string DescribeCorrelationId(CorrelationID id, TerminalLinkSubscriptionData key)
        {
            if (key == null) return "UnknownCorrelationId:" + id.Value;

            var bbgTicker = _symbolMapper.GetBrokerageSymbol(key.Symbol);
            return $"bbg:{bbgTicker}|lean:{key.Symbol.Value}";
        }

        private static T GetTerminalLinkFieldValue<T>(Message message, string field) where T : new()
        {
            if (!message.HasElement(field, true))
            {
                return default(T);
            }

            var element = message[field];

            var value = (T)element.GetValue().ConvertInvariant(typeof(T));

            return value;
        }

        private static string GetTerminalLinkFieldValue(Message message, Name field)
        {
            return message.HasElement(field, true) ? message[field.ToString()].GetValue().ToString() : string.Empty;
        }

        private static DateTime GetLocalTickTime(Message message, TerminalLinkSubscriptionData data, TickType tickType, Name eventSubtype = null)
        {
            var utcTime = default(DateTime);
            switch (tickType)
            {
                case TickType.Quote:
                {
                    Name terminalLinkName = null;
                    if (Equals(eventSubtype, TerminalLinkNames.Bid))
                    {
                        terminalLinkName = TerminalLinkNames.BidUpdateStamp;
                    }
                    else if (Equals(eventSubtype, TerminalLinkNames.Ask))
                    {
                        terminalLinkName = TerminalLinkNames.AskUpdateStamp;
                    }
                    else if (Equals(eventSubtype, TerminalLinkNames.InitPaint))
                    {
                        terminalLinkName = TerminalLinkNames.BidAskTime;
                    }

                    if (terminalLinkName == null)
                    {
                        throw new ArgumentException($"Invalid eventSubType: {eventSubtype}");
                    }

                    var timestamp = GetTerminalLinkFieldValue(message, terminalLinkName);
                    var datestamp = GetTerminalLinkFieldValue(message, TerminalLinkNames.TradingDateTime);
                    utcTime = DateTime.Parse($"{datestamp} {timestamp}", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
                    break;
                }

                case TickType.Trade:
                {
                    // "2020-02-27T23:42:31.153+00:00"
                    var timestamp = GetTerminalLinkFieldValue(message, TerminalLinkNames.TradeUpdateStamp);
                    if (string.IsNullOrWhiteSpace(timestamp))
                    {
                        timestamp = GetTerminalLinkFieldValue(message, TerminalLinkNames.EventTradeTimeRealTime);
                    }
                    utcTime = DateTime.Parse(timestamp, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
                    break;
                }

                case TickType.OpenInterest:
                {
                    var timestamp = GetTerminalLinkFieldValue(message, TerminalLinkNames.OpenInterestDate);
                    utcTime = DateTime.Parse(timestamp, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
                    break;
                }
            }

            return utcTime.ConvertFromUtc(data.ExchangeTimeZone);
        }
    }
}
