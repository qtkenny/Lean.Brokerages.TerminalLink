/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Bloomberglp.Blpapi;
using NodaTime;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;

namespace QuantConnect.Bloomberg
{
    /// <summary>
    /// An implementation of <see cref="IBrokerage"/> for Bloomberg
    /// </summary>
    public partial class BloombergBrokerage : Brokerage, IDataQueueHandler
    {
        private readonly string _serverHost;
        private readonly int _serverPort;
        private readonly string _broker;
        private readonly DateTimeZone _userTimeZone;
        private readonly string _account;
        private readonly string _strategy;
        private readonly string _notes;
        private readonly string _handlingInstruction;
        private readonly bool _execution;
        private readonly Session _sessionMarketData;
        private readonly Session _sessionReferenceData;
        private readonly Session _sessionEms;

        private Service _serviceEms;
        private Service _serviceReferenceData;

        private long _nextCorrelationId;
        private readonly ConcurrentDictionary<string, BloombergSubscriptions> _subscriptionsByTopicName = new ConcurrentDictionary<string, BloombergSubscriptions>();
        private readonly ConcurrentDictionary<CorrelationID, BloombergSubscriptionKey> _subscriptionKeysByCorrelationId =
            new ConcurrentDictionary<CorrelationID, BloombergSubscriptionKey>();
        private readonly IBloombergSymbolMapper _symbolMapper;

        private readonly SchemaFieldDefinitions _orderFieldDefinitions = new SchemaFieldDefinitions();
        private readonly SchemaFieldDefinitions _routeFieldDefinitions = new SchemaFieldDefinitions();

        private readonly ConcurrentDictionary<CorrelationID, IMessageHandler> _requestMessageHandlers = new ConcurrentDictionary<CorrelationID, IMessageHandler>();
        private readonly ConcurrentDictionary<CorrelationID, IMessageHandler> _subscriptionMessageHandlers = new ConcurrentDictionary<CorrelationID, IMessageHandler>();

        // map request CorrelationId to LEAN OrderId
        private readonly ConcurrentDictionary<CorrelationID, int> _orderMap = new ConcurrentDictionary<CorrelationID, int>();

        private readonly IOrderProvider _orderProvider;
        private readonly ManualResetEvent _blotterInitializedEvent = new ManualResetEvent(false);
        private OrderSubscriptionHandler _orderSubscriptionHandler;
        private bool _isConnected;

        /// <summary>
        /// Initializes a new instance of the <see cref="BloombergBrokerage"/> class
        /// </summary>
        public BloombergBrokerage(IOrderProvider orderProvider, ApiType apiType, Environment environment, 
            IBloombergSymbolMapper symbolMapper, string serverHost, int serverPort)
            : base("Bloomberg brokerage")
        {
            _orderProvider = orderProvider;
            _symbolMapper = symbolMapper;

            ApiType = apiType;
            Environment = environment;
            _serverHost = serverHost;
            _serverPort = serverPort;

            _userTimeZone = DateTimeZoneProviders.Tzdb[Config.GetValue("bloomberg-emsx-user-time-zone", TimeZones.Utc.Id)];
            _broker = Config.GetValue<string>("bloomberg-emsx-broker") ?? throw new Exception("EMSX requries a broker");
            _account = Config.GetValue<string>("bloomberg-emsx-account");
            _strategy = Config.GetValue<string>("bloomberg-emsx-strategy");
            _notes = Config.GetValue<string>("bloomberg-emsx-notes");
            _handlingInstruction = Config.GetValue<string>("bloomberg-emsx-handling");
            _execution = Config.GetBool("bloomberg-execution");

            if (apiType != ApiType.Desktop)
            {
                throw new NotSupportedException("Only the Desktop API is supported for now.");
            }

            var sessionOptions = new SessionOptions
            {
                ServerHost = serverHost,
                ServerPort = serverPort
            };

            _sessionEms = new Session(sessionOptions, OnBloombergEvent);
            _sessionMarketData = new Session(sessionOptions, OnBloombergMarketDataEvent);
            _sessionReferenceData = new Session(sessionOptions);
        }

        internal BloombergOrders Orders { get; private set; }

        /// <summary>
        /// The API type (Desktop, Server or BPIPE)
        /// </summary>
        public ApiType ApiType { get; }

        /// <summary>
        /// The Bloomberg environment (Production or Beta)
        /// </summary>
        public Environment Environment { get; }

        #region IBrokerage implementation

        /// <summary>
        /// Returns true if we're currently connected to the broker
        /// </summary>
        public override bool IsConnected => _isConnected;

        /// <summary>
        /// Connects the client to the broker's remote servers
        /// </summary>
        public override void Connect()
        {
            if (IsConnected)
            {
                return;
            }

            Log.Trace($"BloombergBrokerage.Connect(): Starting EMS session: {_serverHost}:{_serverPort}:{Environment}.");
            if (!_sessionEms.Start())
            {
                throw new Exception("Unable to start EMS session.");
            }

            Log.Trace("BloombergBrokerage.Connect(): Opening EMS service.");
            var emsServiceName = GetServiceName(ServiceType.Ems);
            if (!_sessionEms.OpenService(emsServiceName))
            {
                _sessionEms.Stop();
                throw new Exception("Unable to open EMS service.");
            }
            _serviceEms = _sessionEms.GetService(emsServiceName);

            // Initialize Market Data
            Log.Trace($"BloombergBrokerage(): Starting market data session: {_serverHost}:{_serverPort}:{Environment}.");
            if (!_sessionMarketData.Start())
            {
                throw new Exception("Unable to start market data session.");
            }

            Log.Trace("BloombergBrokerage(): Opening market data service.");
            var marketDataServiceName = GetServiceName(ServiceType.MarketData);
            if (!_sessionMarketData.OpenService(marketDataServiceName))
            {
                throw new Exception("Unable to open market data service.");
            }
            // Initialize reference data
            Log.Trace($"BloombergBrokerage(): Starting reference data session: {_serverHost}:{_serverPort}:{Environment}.");
            if (!_sessionReferenceData.Start())
            {
                throw new Exception("Unable to start reference data session.");
            }

            Log.Trace("BloombergBrokerage(): Opening reference data service.");
            var referenceDataServiceName = GetServiceName(ServiceType.ReferenceData);
            if (!_sessionReferenceData.OpenService(referenceDataServiceName))
            {
                throw new Exception("Unable to open reference data service.");
            }
            _serviceReferenceData = _sessionReferenceData.GetService(referenceDataServiceName);

            InitializeFieldData();

            Orders = new BloombergOrders(_orderFieldDefinitions);
            _orderSubscriptionHandler = new OrderSubscriptionHandler(this, _orderProvider, Orders);
            if (_execution)
            {
                SubscribeOrderEvents();
            }
            else
            {
                Log.Debug("Not subscribing to order events - execution is disabled.");
            }
            _isConnected = true;
        }

        /// <summary>
        /// Disconnects the client from the broker's remote servers
        /// </summary>
        public override void Disconnect()
        {
            Log.Trace("BloombergBrokerage.Disconnect(): Stopping EMS session.");
            _sessionEms?.Stop();
        }

        /// <summary>
        /// Gets all open orders on the account.
        /// NOTE: The order objects returned do not have QC order IDs.
        /// </summary>
        /// <returns>The open orders returned from Bloomberg</returns>
        public override List<Order> GetOpenOrders()
        {
            return Orders
                    .Where(x => !ConvertOrderStatus(x.Status).IsClosed() && x.Amount != 0)
                    .Select(ConvertOrder)
                    .ToList();
        }

        /// <summary>
        /// Gets all holdings for the account
        /// </summary>
        /// <returns>The current holdings from the account</returns>
        public override List<Holding> GetAccountHoldings()
        {
            // Bloomberg is not a portfolio management system, we'll need to fetch this information elsewhere
            return new List<Holding>();
        }

        /// <summary>
        /// Gets the current cash balance for each currency held in the brokerage account
        /// </summary>
        /// <returns>The current cash balance for each currency available for trading</returns>
        public override List<CashAmount> GetCashBalance()
        {
            // TODO: When the below portfolio management system is implemented, this should be replaced with a solution that also works for the UAT / beta environment.
            if (Environment == Environment.Beta)
            {
                return new List<CashAmount> {new CashAmount(100_000, Currencies.USD)};
            }

            // Bloomberg is not a portfolio management system, we'll need to fetch this information elsewhere
            return new List<CashAmount>();
        }

        /// <summary>
        /// Places a new order and assigns a new broker ID to the order
        /// </summary>
        /// <param name="order">The order to be placed</param>
        /// <returns>True if the request for a new order has been placed, false otherwise</returns>
        public override bool PlaceOrder(Order order)
        {
            var request = _serviceEms.CreateRequest(BloombergNames.CreateOrderAndRouteEx.ToString());
            request.Set(BloombergNames.EMSXTicker, _symbolMapper.GetBrokerageSymbol(order.Symbol));
            request.Set(BloombergNames.EMSXSide, ConvertOrderDirection(order.Direction));
            if (!string.IsNullOrWhiteSpace(_broker))
            {
                request.Set(BloombergNames.EMSXBroker, _broker);
            }
            
            if (!string.IsNullOrWhiteSpace(_strategy))
            {
                var element = request["EMSX_STRATEGY_PARAMS"];
                element.SetElement("EMSX_STRATEGY_NAME", _strategy);
            }

            request.Set(BloombergNames.EMSXHandInstruction, _handlingInstruction);
            // Set fields that map back to internal order ids
            request.Set(BloombergNames.EMSXReferenceOrderIdRequest, order.Id);
            PopulateRequest(request, order);
            SendOrderRequest(request, order.Id);
            return true;
        }

        private void PopulateRequest(Request request, Order order)
        { 
            request.Set(BloombergNames.EMSXAmount, Convert.ToInt32(order.AbsoluteQuantity));
            request.Set(BloombergNames.EMSXOrderType, ConvertOrderType(order.Type));
            request.Set(BloombergNames.EMSXTif, ConvertTimeInForce(order.TimeInForce));
            if (!string.IsNullOrWhiteSpace(_account))
            {
                request.Set(BloombergNames.EMSXAccount, _account);
            }

            if (!string.IsNullOrWhiteSpace(_notes))
            {
                request.Set(BloombergNames.EMSXNotes, _notes);
            }

            switch (order.Type)
            {
                case OrderType.Limit:
                    request.Set(BloombergNames.EMSXLimitPrice, Convert.ToDouble(((LimitOrder)order).LimitPrice));
                    break;

                case OrderType.StopMarket:
                    request.Set(BloombergNames.EMSXStopPrice, Convert.ToDouble(((StopMarketOrder)order).StopPrice));
                    break;

                case OrderType.StopLimit:
                    request.Set(BloombergNames.EMSXStopPrice, Convert.ToDouble(((StopLimitOrder)order).StopPrice));
                    request.Set(BloombergNames.EMSXLimitPrice, Convert.ToDouble(((StopLimitOrder)order).LimitPrice));
                    break;
            }
        }

        private static string ConvertOrderDirection(OrderDirection direction)
        {
            if (direction == OrderDirection.Hold) throw new ArgumentException("Invalid direction: Hold", nameof(direction));

            return direction.ToString().ToUpperInvariant();
        }

        /// <summary>
        /// Updates the order with the same id
        /// </summary>
        /// <param name="order">The new order information</param>
        /// <returns>True if the request was made for the order to be updated, false otherwise</returns>
        public override bool UpdateOrder(Order order)
        {
            // TODO: Should this use the broker id?  At the moment broker id isn't able to be persisted back into the order transaction handler.
            if (!_orderSubscriptionHandler.TryGetSequenceId(order.Id, out var sequence))
            {
                Log.Error("Unable to update - cannot find a sequence for order id: " + order.Id);
                return false;
            }
            
            Log.Trace($"Updating order {order.Id}, sequence:{sequence}");
            var request = _serviceEms.CreateRequest(BloombergNames.ModifyOrderEx.ToString());
            request.Set(BloombergNames.EMSXSequence, sequence);
            PopulateRequest(request, order);
            SendOrderRequest(request, order.Id);

            return true;
        }

        /// <summary>
        /// Cancels the order with the specified ID
        /// </summary>
        /// <param name="order">The order to cancel</param>
        /// <returns>True if the request was made for the order to be canceled, false otherwise</returns>
        public override bool CancelOrder(Order order)
        {
            // TODO: Should this use the broker id?  At the moment broker id isn't able to be persisted back into the order transaction handler.
            if (!_orderSubscriptionHandler.TryGetSequenceId(order.Id, out var sequence))
            {
                Log.Error("Unable to cancel - cannot find a sequence for order id: " + order.Id);
                return false;
            }

            Log.Trace($"Cancelling order {order.Id}, sequence:{sequence}");
            var request = _serviceEms.CreateRequest(BloombergNames.CancelOrderEx.ToString());
            request.GetElement(BloombergNames.EMSXSequence).AppendValue(sequence);
            SendOrderRequest(request, order.Id);

            return true;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
            _sessionMarketData?.Stop();
            _sessionReferenceData?.Stop();
            _sessionEms?.Stop();
        }

        #endregion

        public string GetServiceName(ServiceType serviceType)
        {
            switch (serviceType)
            {
                case ServiceType.MarketData:
                    return "//blp/mktdata";

                case ServiceType.ReferenceData:
                    return "//blp/refdata";

                case ServiceType.Ems:
                    switch (Environment)
                    {
                        case Environment.Production:
                            return "//blp/emapisvc";

                        case Environment.Beta:
                            return "//blp/emapisvc_beta";

                        default:
                            throw new Exception($"BloombergBrokerage.GetServiceName(): Invalid environment: {Environment}.");
                    }

                default:
                    throw new Exception($"BloombergBrokerage.GetServiceName(): Invalid service type: {serviceType}.");
            }
        }

        private void InitializeFieldData()
        {
            _orderFieldDefinitions.Clear();
            _routeFieldDefinitions.Clear();

            var orderRouteFields = _serviceEms.GetEventDefinition("OrderRouteFields");
            var typeDef = orderRouteFields.TypeDefinition;

            for (var i = 0; i < typeDef.NumElementDefinitions; i++)
            {
                var e = typeDef.GetElementDefinition(i);

                var f = new SchemaFieldDefinition(e);

                if (f.IsOrderField())
                {
                    _orderFieldDefinitions.Add(f);
                }

                if (f.IsRouteField())
                {
                    _routeFieldDefinitions.Add(f);
                }
            }
        }

        private void OnBloombergEvent(Event @event, Session session)
        {
            switch (@event.Type)
            {
                case Event.EventType.ADMIN:
                    ProcessAdminEvent(@event, session);
                    break;

                case Event.EventType.SESSION_STATUS:
                    ProcessSessionEvent(@event, session);
                    break;

                case Event.EventType.SERVICE_STATUS:
                    ProcessServiceEvent(@event, session);
                    break;

                case Event.EventType.SUBSCRIPTION_DATA:
                    ProcessSubscriptionDataEvent(@event, session);
                    break;

                case Event.EventType.SUBSCRIPTION_STATUS:
                    ProcessSubscriptionStatusEvent(@event, session);
                    break;

                case Event.EventType.RESPONSE:
                    ProcessResponse(@event, session);
                    break;

                default:
                    ProcessOtherEvents(@event, session);
                    break;
            }
        }

        private static void ProcessAdminEvent(Event @event, Session session)
        {
            foreach (var message in @event)
            {
                if (message.MessageType.Equals(BloombergNames.SlowConsumerWarning))
                {
                    Log.Trace("BloombergBrokerage.ProcessAdminEvent(): Slow Consumer Warning.");
                }
                else if (message.MessageType.Equals(BloombergNames.SlowConsumerWarningCleared))
                {
                    Log.Trace("BloombergBrokerage.ProcessAdminEvent(): Slow Consumer Warning cleared.");
                }
                else
                {
                    Log.Trace($"BloombergBrokerage.ProcessAdminEvent(): Unknown message type '{message.MessageType}': {message}");
                }
            }
        }

        private static void ProcessSessionEvent(Event @event, Session session)
        {
            foreach (var message in @event)
            {
                if (message.MessageType.Equals(BloombergNames.SessionStarted))
                {
                    Log.Trace("BloombergBrokerage.ProcessSessionEvent(): Session started.");
                }
                else if (message.MessageType.Equals(BloombergNames.SessionStartupFailure))
                {
                    Log.Trace("BloombergBrokerage.ProcessSessionEvent(): Session startup failure.");
                }
                else if (message.MessageType.Equals(BloombergNames.SessionTerminated))
                {
                    Log.Trace("BloombergBrokerage.ProcessSessionEvent(): Session terminated.");
                }
                else if (message.MessageType.Equals(BloombergNames.SessionConnectionUp))
                {
                    Log.Trace("BloombergBrokerage.ProcessSessionEvent(): Session connection up.");
                }
                else if (message.MessageType.Equals(BloombergNames.SessionConnectionDown))
                {
                    Log.Trace("BloombergBrokerage.ProcessSessionEvent(): Session connection down.");
                }
                else
                {
                    Log.Trace($"BloombergBrokerage.ProcessSessionEvent(): Unknown message type: '{message.MessageType}': {message}");
                }
            }
        }

        private static void ProcessServiceEvent(Event @event, Session session)
        {
            foreach (var message in @event)
            {
                if (message.MessageType.Equals(BloombergNames.ServiceOpened))
                {
                    Log.Trace("BloombergBrokerage.ProcessServiceEvent(): Service opened.");
                }
                else if (message.MessageType.Equals(BloombergNames.ServiceOpenFailure))
                {
                    Log.Trace("BloombergBrokerage.ProcessServiceEvent(): Service open failed.");
                }
                else
                {
                    Log.Trace($"BloombergBrokerage.ProcessServiceEvent(): Unknown message type '{message.MessageType}': {message}");
                }
            }
        }

        private void ProcessSubscriptionDataEvent(Event @event, Session session)
        {
            // TODO: BBG sends heartbeats for the broker connection, resulting in a lot of logging.  Maybe this should be a verbose message?
            //Log.Trace("BloombergBrokerage.ProcessSubscriptionDataEvent(): Processing SUBSCRIPTION_DATA event.");

            foreach (var message in @event)
            {
                foreach (var correlationId in message.CorrelationIDs)
                {
                    IMessageHandler handler;
                    if (!_subscriptionMessageHandlers.TryGetValue(correlationId, out handler))
                    {
                        Log.Error($"BloombergBrokerage.ProcessSubscriptionDataEvent(): Unexpected SUBSCRIPTION_DATA event received (CID={correlationId}): {message}");
                    }
                    else
                    {
                        handler.ProcessMessage(message);
                    }
                }
            }
        }

        private void ProcessSubscriptionStatusEvent(Event @event, Session session)
        {
            Log.Trace("BloombergBrokerage.ProcessSubscriptionStatusEvent(): Processing SUBSCRIPTION_STATUS event.");

            foreach (var message in @event)
            {
                foreach (var correlationId in message.CorrelationIDs)
                {
                    IMessageHandler handler;
                    if (!_subscriptionMessageHandlers.TryGetValue(correlationId, out handler))
                    {
                        Log.Error($"BloombergBrokerage.ProcessSubscriptionStatusEvent(): Unexpected SUBSCRIPTION_STATUS event received (CID={correlationId}): {message}");
                    }
                    else
                    {
                        handler.ProcessMessage(message);
                    }
                }
            }
        }

        private void ProcessResponse(Event @event, Session session)
        {
            Log.Trace("BloombergBrokerage.ProcessResponse(): Processing RESPONSE event.");

            foreach (var message in @event)
            {
                foreach(var correlationId in message.CorrelationIDs)
                {
                    IMessageHandler handler;
                    if (!_requestMessageHandlers.TryGetValue(correlationId, out handler))
                    {
                        Log.Error($"BloombergBrokerage.ProcessResponse(): Unexpected RESPONSE event received (CID={correlationId}): {message}");
                    }
                    else
                    {
                        int orderId;
                        if (!_orderMap.TryGetValue(correlationId, out orderId))
                        {
                            Log.Error($"BloombergBrokerage.ProcessResponse(): OrderId not found (CID={correlationId}):{message}");
                        }
                        else
                        {
                            handler.ProcessMessage(message);

                            _requestMessageHandlers.TryRemove(correlationId, out handler);
                            _orderMap.TryRemove(correlationId, out orderId);

                            Log.Trace($"BloombergBrokerage.ProcessResponse(): MessageHandler removed [CID={correlationId},order:{orderId}]");
                        }
                    }
                }
            }
        }

        private static void ProcessOtherEvents(Event @event, Session session)
        {
            foreach (var message in @event)
            {
                Log.Trace($"BloombergBrokerage.ProcessOtherEvent(): {@event.Type} - {message.MessageType}: {message}");
            }
        }

        private void Subscribe(string topic, IMessageHandler handler)
        {
            var correlationId = GetNewCorrelationId();
            _subscriptionMessageHandlers.AddOrUpdate(correlationId, handler);

            Log.Trace($"Added Subscription message handler: {correlationId}");

            try
            {
                _sessionEms.Subscribe(new List<Subscription>
                {
                    new Subscription(topic, correlationId)
                });
            }
            catch (Exception exception)
            {
                Log.Error(exception);
            }
        }

        private void SendOrderRequest(Request request, int orderId)
        {
            var correlationId = GetNewCorrelationId();
            _requestMessageHandlers.AddOrUpdate(correlationId, _orderSubscriptionHandler);
            _orderMap.AddOrUpdate(correlationId, orderId);

            try
            {
                if (_execution)
                {
                    Log.Trace($"Sending order request: '{request.Operation.Name}' [order:{orderId}, CID={correlationId}]: {request}");
                    _sessionEms.SendRequest(request, correlationId);
                }
                else
                {
                    Log.Trace($"Order was not sent - execution is disabled [{request}] [{correlationId}]");
                }
            }
            catch (Exception e)
            {
                Log.Error(e);
            }
        }

        private void SubscribeOrderEvents()
        {
            var fields = _orderFieldDefinitions.Select(x => x.Name);

            var serviceName = GetServiceName(ServiceType.Ems);
            var topic = $"{serviceName}/order?fields={string.Join(",", fields)}";

            Subscribe(topic, _orderSubscriptionHandler);

            _blotterInitializedEvent.WaitOne();
        }

        public void SetBlotterInitialized()
        {
            _blotterInitializedEvent.Set();
        }

        private Order ConvertOrder(BloombergOrder order)
        {
            var securityType = ConvertSecurityType(order.GetFieldValue(BloombergNames.EMSXAssetClass));

            var symbol = _symbolMapper.GetLeanSymbol(order.GetFieldValue(BloombergNames.EMSXTicker), securityType);
            var quantity = order.GetFieldValueDecimal(BloombergNames.EMSXAmount);
            var orderType = ConvertOrderType(order.GetFieldValue(BloombergNames.EMSXOrderType));
            var orderDirection = order.GetFieldValue(BloombergNames.EMSXSide) == "BUY" ? OrderDirection.Buy : OrderDirection.Sell;
            var timeInForce = ConvertTimeInForce(order.GetFieldValue(BloombergNames.EMSXTif));

            if (orderDirection == OrderDirection.Sell)
            {
                quantity = -quantity;
            }

            var date = DateTime.ParseExact(order.GetFieldValue(BloombergNames.EMSXDate), "yyyyMMdd", CultureInfo.InvariantCulture);
            // the EMSXTimeStampMicrosec field contains a value in seconds with decimals
            var time = order.GetFieldValueDecimal(BloombergNames.EMSXTimeStampMicrosec);
            var orderTime = date.AddSeconds(Convert.ToDouble(time)).ConvertToUtc(_userTimeZone);

            Order newOrder;
            switch (orderType)
            {
                case OrderType.Market:
                    newOrder = new MarketOrder(symbol, quantity, orderTime);
                    break;

                case OrderType.Limit:
                    {
                        var limitPrice = order.GetFieldValueDecimal(BloombergNames.EMSXLimitPrice);
                        newOrder = new LimitOrder(symbol, quantity, limitPrice, orderTime);
                    }
                    break;

                case OrderType.StopMarket:
                    {
                        var stopPrice = order.GetFieldValueDecimal(BloombergNames.EMSXStopPrice);
                        newOrder = new LimitOrder(symbol, quantity, stopPrice, orderTime);
                    }
                    break;

                case OrderType.StopLimit:
                    {
                        var limitPrice = order.GetFieldValueDecimal(BloombergNames.EMSXLimitPrice);
                        var stopPrice = order.GetFieldValueDecimal(BloombergNames.EMSXStopPrice);
                        newOrder = new StopLimitOrder(symbol, quantity, stopPrice, limitPrice, orderTime);
                    }
                    break;

                default:
                    throw new NotSupportedException($"Unsupported order type: {orderType}");
            }

            newOrder.Properties.TimeInForce = timeInForce;

            newOrder.BrokerId.Add(order.Sequence.ToString());

            return newOrder;
        }

        private SecurityType ConvertSecurityType(string assetClass)
        {
            switch (assetClass)
            {
                case "Future":
                    return SecurityType.Future;

                case "Equity":
                    return SecurityType.Equity;

                case "Option":
                    return SecurityType.Option;

                default:
                    throw new Exception($"Unknown asset class: {assetClass}");
            }
        }

        private OrderType ConvertOrderType(string orderType)
        {
            // TODO: check order types, only MKT is used in documentation examples

            switch (orderType)
            {
                case "MKT":
                    return OrderType.Market;

                case "LMT":
                    return OrderType.Limit;

                case "ST":
                    return OrderType.StopMarket;

                case "SL":
                    return OrderType.StopLimit;

                default:
                    throw new NotSupportedException($"Unsupported order type: {orderType}");
            }
        }

        private string ConvertOrderType(OrderType orderType)
        {
            // TODO: check order types, only MKT is used in documentation examples
            // EMSX API - pg. 78
            switch (orderType)
            {
                case OrderType.Market:
                    return "MKT";

                case OrderType.Limit:
                    return "LMT";

                case OrderType.StopMarket:
                    return "ST";

                case OrderType.StopLimit:
                    return "SL";

                default:
                    throw new NotSupportedException($"Unsupported order type: {orderType}");
            }
        }

        private TimeInForce ConvertTimeInForce(string timeInForce)
        {
            // TODO: check time in force values, only DAY is used in documentation examples

            switch (timeInForce)
            {
                case "DAY":
                    return TimeInForce.Day;

                case "IOC":
                    // LEAN does not support IOC yet, we map it to GTC for now
                case "GTC":
                    return TimeInForce.GoodTilCanceled;

                default:
                    throw new NotSupportedException($"Unsupported time in force: {timeInForce}");
            }
        }

        private string ConvertTimeInForce(TimeInForce timeInForce)
        {
            // TODO: check time in force values, only DAY is used in documentation examples

            if (timeInForce == TimeInForce.Day)
            {
                return "DAY";
            }

            if (timeInForce == TimeInForce.GoodTilCanceled)
            {
                return "GTC";
            }

            throw new NotSupportedException($"Unsupported time in force: {timeInForce}");
        }

        public OrderStatus ConvertOrderStatus(string orderStatus)
        {
            switch (orderStatus)
            {
                case "CXL-PEND":
                case "CXL-REQ":
                    return OrderStatus.CancelPending;

                case "ASSIGN":
                case "CANCEL":
                case "EXPIRED":
                    return OrderStatus.Canceled;

                case "SENT":
                case "WORKING":
                    return OrderStatus.Submitted;

                case "COMPLETED":
                case "FILLED":
                    return OrderStatus.Filled;

                case "PARTFILLED":
                    return OrderStatus.PartiallyFilled;

                case "CXLREJ":
                    return OrderStatus.Invalid;

                case "NEW":
                case "ORD-PEND":
                    return OrderStatus.New;

                default:
                    return OrderStatus.None;
            }
        }

        private string ConvertOrderStatus(OrderStatus orderStatus)
        {
            switch (orderStatus)
            {
                case OrderStatus.CancelPending:
                    return "CXL-PEND";

                case OrderStatus.Canceled:
                    return "CANCEL";

                case OrderStatus.Submitted:
                    return "WORKING";

                case OrderStatus.Filled:
                    return "FILLED";

                case OrderStatus.PartiallyFilled:
                    return "PARTFILLED";

                case OrderStatus.Invalid:
                    return "CXLREJ";

                case OrderStatus.None:
                case OrderStatus.New:
                    return "NEW";

                default:
                    return string.Empty;
            }
        }

        internal void FireOrderEvent(OrderEvent orderEvent)
        {
            OnOrderEvent(orderEvent);
        }

        internal void FireBrokerMessage(BrokerageMessageEvent message)
        {
            OnMessage(message);
        }
    }
}