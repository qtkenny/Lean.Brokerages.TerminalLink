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
        private readonly bool _execution;

        private readonly SessionOptions _sessionOptions;
        private readonly Session _sessionMarketData;

        private readonly Session _sessionHistoricalData;
        private readonly Service _serviceHistoricalData;

        private Session _sessionEms;
        private Service _serviceEms;

        private long _nextCorrelationId;
        private readonly ConcurrentDictionary<string, BloombergSubscriptions> _subscriptionsByTopicName = new ConcurrentDictionary<string, BloombergSubscriptions>();
        private readonly ConcurrentDictionary<string, Symbol> _symbolsByTopicName = new ConcurrentDictionary<string, Symbol>();
        private readonly BloombergSymbolMapper _symbolMapper = new BloombergSymbolMapper();

        private readonly SchemaFieldDefinitions _orderFieldDefinitions = new SchemaFieldDefinitions();

        private readonly ConcurrentDictionary<CorrelationID, IMessageHandler> _requestMessageHandlers = new ConcurrentDictionary<CorrelationID, IMessageHandler>();
        private readonly ConcurrentDictionary<CorrelationID, IMessageHandler> _subscriptionMessageHandlers = new ConcurrentDictionary<CorrelationID, IMessageHandler>();

        // map request CorrelationId to LEAN OrderId
        private readonly ConcurrentDictionary<CorrelationID, int> _orderMap = new ConcurrentDictionary<CorrelationID, int>();

        private readonly IOrderProvider _orderProvider;
        private readonly ManualResetEvent _blotterInitializedEvent = new ManualResetEvent(false);
        private IMessageHandler _orderSubscriptionHandler;
        private BloombergOrders _orders;
        private bool _isConnected;

        /// <summary>
        /// Initializes a new instance of the <see cref="BloombergBrokerage"/> class
        /// </summary>
        public BloombergBrokerage(IOrderProvider orderProvider, ApiType apiType, Environment environment, string serverHost, int serverPort)
            : base("Bloomberg brokerage")
        {
            _orderProvider = orderProvider;

            ApiType = apiType;
            Environment = environment;
            _serverHost = serverHost;
            _serverPort = serverPort;
            _execution = Config.GetBool("bloomberg-execution");

            if (apiType != ApiType.Desktop)
            {
                throw new NotSupportedException("Only the Desktop API is supported for now.");
            }

            _sessionOptions = new SessionOptions
            {
                ServerHost = serverHost,
                ServerPort = serverPort
            };

            Log.Trace($"BloombergBrokerage(): Starting market data session: {_serverHost}:{_serverPort}:{Environment}.");
            _sessionMarketData = new Session(_sessionOptions, OnBloombergMarketDataEvent);
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

            Log.Trace($"BloombergBrokerage(): Starting historical data session: {_serverHost}:{_serverPort}:{Environment}.");
            _sessionHistoricalData = new Session(_sessionOptions);
            if (!_sessionHistoricalData.Start())
            {
                throw new Exception("Unable to start historical data session.");
            }

            Log.Trace("BloombergBrokerage(): Opening historical data service.");
            var historicalDataServiceName = GetServiceName(ServiceType.HistoricalData);
            if (!_sessionHistoricalData.OpenService(historicalDataServiceName))
            {
                throw new Exception("Unable to open historical data service.");
            }
            _serviceHistoricalData = _sessionHistoricalData.GetService(historicalDataServiceName);
        }

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
            Log.Trace($"BloombergBrokerage.Connect(): Starting EMS session: {_serverHost}:{_serverPort}:{Environment}.");
            _sessionEms = new Session(_sessionOptions, OnBloombergEvent);
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

            InitializeFieldData();

            _orders = new BloombergOrders(_orderFieldDefinitions);
            _orderSubscriptionHandler = new OrderSubscriptionHandler(this, _orderProvider, _orders);
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
            return _orders.Select(ConvertOrder).ToList();
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
            var request = _serviceEms.CreateRequest("CreateOrder");

            request.Set("EMSX_TICKER", _symbolMapper.GetBrokerageSymbol(order.Symbol));
            request.Set("EMSX_AMOUNT", Convert.ToInt32(order.AbsoluteQuantity));
            request.Set("EMSX_ORDER_TYPE", ConvertOrderType(order.Type));
            request.Set("EMSX_TIF", ConvertTimeInForce(order.TimeInForce));
            request.Set("EMSX_HAND_INSTRUCTION", "ANY");
            request.Set("EMSX_SIDE", order.Direction == OrderDirection.Buy ? "BUY" : "SELL");

            switch (order.Type)
            {
                case OrderType.Limit:
                    request.Set("EMSX_LIMIT_PRICE", Convert.ToDouble(((LimitOrder)order).LimitPrice));
                    break;

                case OrderType.StopMarket:
                    request.Set("EMSX_STOP_PRICE", Convert.ToDouble(((StopMarketOrder)order).StopPrice));
                    break;

                case OrderType.StopLimit:
                    request.Set("EMSX_STOP_PRICE", Convert.ToDouble(((StopLimitOrder)order).StopPrice));
                    request.Set("EMSX_LIMIT_PRICE", Convert.ToDouble(((StopLimitOrder)order).LimitPrice));
                    break;
            }

            SendOrderRequest(request, order.Id);

            return true;
        }

        /// <summary>
        /// Updates the order with the same id
        /// </summary>
        /// <param name="order">The new order information</param>
        /// <returns>True if the request was made for the order to be updated, false otherwise</returns>
        public override bool UpdateOrder(Order order)
        {
            var request = _serviceEms.CreateRequest("ModifyOrderEx");

            request.Set("EMSX_SEQUENCE", Convert.ToInt32(order.BrokerId[0]));
            request.Set("EMSX_TICKER", _symbolMapper.GetBrokerageSymbol(order.Symbol));
            request.Set("EMSX_AMOUNT", Convert.ToInt32(order.AbsoluteQuantity));
            request.Set("EMSX_ORDER_TYPE", ConvertOrderType(order.Type));
            request.Set("EMSX_TIF", ConvertTimeInForce(order.TimeInForce));

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
            var request = _serviceEms.CreateRequest("DeleteOrder");

            request.Set("EMSX_SEQUENCE", Convert.ToInt32(order.BrokerId[0]));

            SendOrderRequest(request, order.Id);

            return true;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
            _sessionMarketData?.Stop();
            _sessionHistoricalData?.Stop();
            _sessionEms?.Stop();
        }

        #endregion

        public string GetServiceName(ServiceType serviceType)
        {
            switch (serviceType)
            {
                case ServiceType.MarketData:
                    return "//blp/mktdata";

                case ServiceType.HistoricalData:
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
            }
        }

        private void ProcessSubscriptionDataEvent(Event @event, Session session)
        {
            Log.Trace("BloombergBrokerage.ProcessSubscriptionDataEvent(): Processing SUBSCRIPTION_DATA event.");

            foreach (var message in @event)
            {
                var correlationId = message.CorrelationID;

                IMessageHandler handler;
                if (!_subscriptionMessageHandlers.TryGetValue(correlationId, out handler))
                {
                    Log.Error($"BloombergBrokerage.ProcessSubscriptionDataEvent(): Unexpected SUBSCRIPTION_DATA event received (CID={correlationId}): {message}");
                }
                else
                {
                    handler.ProcessMessage(message, 0);
                }
            }
        }

        private void ProcessSubscriptionStatusEvent(Event @event, Session session)
        {
            Log.Trace("BloombergBrokerage.ProcessSubscriptionStatusEvent(): Processing SUBSCRIPTION_STATUS event.");

            foreach (var message in @event)
            {
                var correlationId = message.CorrelationID;

                IMessageHandler handler;
                if (!_subscriptionMessageHandlers.TryGetValue(correlationId, out handler))
                {
                    Log.Error($"BloombergBrokerage.ProcessSubscriptionStatusEvent(): Unexpected SUBSCRIPTION_STATUS event received (CID={correlationId}): {message}");
                }
                else
                {
                    handler.ProcessMessage(message, 0);
                }
            }
        }

        private void ProcessResponse(Event @event, Session session)
        {
            Log.Trace("BloombergBrokerage.ProcessResponse(): Processing RESPONSE event.");

            foreach (var message in @event)
            {
                var correlationId = message.CorrelationID;

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
                        Log.Error($"BloombergBrokerage.ProcessResponse(): OrderId not found for CorrelationId: {correlationId}");
                    }

                    handler.ProcessMessage(message, orderId);


                    _requestMessageHandlers.TryRemove(correlationId, out handler);
                    _orderMap.TryRemove(correlationId, out orderId);

                    Log.Trace($"BloombergBrokerage.ProcessResponse(): MessageHandler removed [{correlationId}]");
                }
            }
        }

        private static void ProcessOtherEvents(Event @event, Session session)
        {
            foreach (var message in @event)
            {
                Log.Trace($"BloombergBrokerage.ProcessOtherEvent(): {@event.Type} - {message.MessageType}.");
            }
        }

        private void Subscribe(string topic, IMessageHandler handler)
        {
            var correlationId = new CorrelationID();
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
            var correlationId = new CorrelationID();
            _requestMessageHandlers.AddOrUpdate(correlationId, _orderSubscriptionHandler);
            _orderMap.AddOrUpdate(correlationId, orderId);

            try
            {
                if (_execution)
                {
                    _sessionEms.SendRequest(request, correlationId);
                }
                else
                {
                    Log.Debug($"Order was not sent - execution is disabled [{request}] [{correlationId}]");
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
            var symbol = _symbolMapper.GetLeanSymbol(order.GetFieldValue("EMSX_TICKER"), SecurityType.Equity, Market.USA);
            var quantity = Convert.ToDecimal(order.GetFieldValue("EMSX_AMOUNT"), CultureInfo.InvariantCulture);
            var orderType = ConvertOrderType(order.GetFieldValue("EMSX_ORDER_TYPE"));
            var orderDirection = order.GetFieldValue("EMSX_SIDE") == "BUY" ? OrderDirection.Buy : OrderDirection.Sell;
            var timeInForce = ConvertTimeInForce(order.GetFieldValue("EMSX_TIF"));

            if (orderDirection == OrderDirection.Sell)
            {
                quantity = -quantity;
            }

            Order newOrder;
            switch (orderType)
            {
                case OrderType.Market:
                    newOrder = new MarketOrder(symbol, quantity, DateTime.UtcNow);
                    break;

                case OrderType.Limit:
                    {
                        var limitPrice = Convert.ToDecimal(order.GetFieldValue("EMSX_LIMIT_PRICE"), CultureInfo.InvariantCulture);
                        newOrder = new LimitOrder(symbol, quantity, limitPrice, DateTime.UtcNow);
                    }
                    break;

                case OrderType.StopMarket:
                    {
                        var stopPrice = Convert.ToDecimal(order.GetFieldValue("EMSX_STOP_PRICE"), CultureInfo.InvariantCulture);
                        newOrder = new LimitOrder(symbol, quantity, stopPrice, DateTime.UtcNow);
                    }
                    break;

                case OrderType.StopLimit:
                    {
                        var limitPrice = Convert.ToDecimal(order.GetFieldValue("EMSX_LIMIT_PRICE"), CultureInfo.InvariantCulture);
                        var stopPrice = Convert.ToDecimal(order.GetFieldValue("EMSX_STOP_PRICE"), CultureInfo.InvariantCulture);
                        newOrder = new StopLimitOrder(symbol, quantity, stopPrice, limitPrice, DateTime.UtcNow);
                    }
                    break;

                default:
                    throw new NotSupportedException($"Unsupported order type: {orderType}");
            }

            newOrder.Properties.TimeInForce = timeInForce;

            return newOrder;
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

                case "STP":
                    return OrderType.StopMarket;

                case "SLT":
                    return OrderType.StopLimit;

                default:
                    throw new NotSupportedException($"Unsupported order type: {orderType}");
            }
        }

        private string ConvertOrderType(OrderType orderType)
        {
            // TODO: check order types, only MKT is used in documentation examples

            switch (orderType)
            {
                case OrderType.Market:
                    return "MKT";

                case OrderType.Limit:
                    return "LMT";

                case OrderType.StopMarket:
                    return "STP";

                case OrderType.StopLimit:
                    return "SLT";

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
    }
}