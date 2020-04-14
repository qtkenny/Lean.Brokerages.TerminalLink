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
using NodaTime;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Orders.TimeInForces;
using QuantConnect.Securities;
using QuantConnect.Util;

namespace QuantConnect.Bloomberg
{
    /// <summary>
    /// An implementation of <see cref="IBrokerage"/> for Bloomberg
    /// </summary>
    public partial class BloombergBrokerage : Brokerage, IDataQueueHandler
    {
        private readonly string _serverHost;
        private readonly int _serverPort;
        private readonly string _account;
        private readonly string _strategy;
        private readonly string _notes;
        private readonly string _handlingInstruction;
        private readonly bool _execution;
        private readonly bool _allowModification;
        private readonly Session _sessionMarketData;
        private readonly Session _sessionReferenceData;
        private readonly Session _sessionEms;
        private static long _nextCorrelationId;
        private readonly bool _isBroker;
        private readonly bool _startAtActive;

        private Service _serviceEms;
        private Service _serviceReferenceData;

        private readonly IBloombergSymbolMapper _symbolMapper;
        private readonly SessionOptions _sessionOptions;

        private readonly SchemaFieldDefinitions _orderFieldDefinitions = new SchemaFieldDefinitions();
        private readonly SchemaFieldDefinitions _routeFieldDefinitions = new SchemaFieldDefinitions();

        private readonly MarketHoursDatabase _marketHoursDatabase;

        private readonly IOrderProvider _orderProvider;
        private readonly CountdownEvent _blotterInitializedEvent = new CountdownEvent(2);
        private OrderSubscriptionHandler _orderSubscriptionHandler;
        private bool _isConnected;

        public BloombergBrokerage() : this(Config.GetValue<ApiType>("bloomberg-api-type"), Config.GetValue<Environment>("bloomberg-environment"),
            Config.Get("bloomberg-server-host"), Config.GetInt("bloomberg-server-port"), new BloombergSymbolMapper(Config.Get("bloomberg-symbol-map-file")))
        {
            _isBroker = false;
            Connect();
        }

        private BloombergBrokerage(ApiType apiType, Environment environment, string serverHost, int serverPort, IBloombergSymbolMapper symbolMapper) : base("Bloomberg brokerage")
        {
            _symbolMapper = symbolMapper;
            Composer.Instance.AddPart<ISymbolMapper>(symbolMapper);

            ApiType = apiType;
            Environment = environment;
            _serverHost = serverHost;
            _serverPort = serverPort;

            _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();

            if (apiType != ApiType.Desktop)
            {
                throw new NotSupportedException("Only the Desktop API is supported for now.");
            }

            _sessionOptions = new SessionOptions
            {
                ServerHost = serverHost,
                ServerPort = serverPort,
                AutoRestartOnDisconnection = true,
                // BLPAPI uses int.MaxValue internally to reconnect indefinitely
                NumStartAttempts = int.MaxValue,
                KeepaliveEnabled = true
            };

            _sessionMarketData = new Session(_sessionOptions, OnBloombergMarketDataEvent);
            _sessionReferenceData = new Session(_sessionOptions);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BloombergBrokerage"/> class
        /// </summary>
        public BloombergBrokerage(IOrderProvider orderProvider, ApiType apiType, Environment environment, string serverHost, int serverPort, IBloombergSymbolMapper symbolMapper) :
            this(apiType, environment, serverHost, serverPort, symbolMapper)
        {
            _isBroker = true;
            _orderProvider = orderProvider;
            UserTimeZone = DateTimeZoneProviders.Tzdb[Config.GetValue("bloomberg-emsx-user-time-zone", TimeZones.Utc.Id)];
            Broker = Config.GetValue<string>("bloomberg-emsx-broker") ?? throw new Exception("EMSX requires a broker");
            _account = Config.GetValue<string>("bloomberg-emsx-account");
            _strategy = Config.GetValue<string>("bloomberg-emsx-strategy");
            _notes = Config.GetValue<string>("bloomberg-emsx-notes");
            _startAtActive = Config.GetValue<bool>("bloomberg-futures-start-at-active", true);
            _handlingInstruction = Config.GetValue<string>("bloomberg-emsx-handling");
            _execution = Config.GetBool("bloomberg-execution");
            _allowModification = Config.GetBool("bloomberg-allow-modification");
            _sessionEms = new Session(_sessionOptions, OnBloombergEvent);
        }

        protected BloombergOrders Orders { get; private set; }

        /// <summary>
        /// The API type (Desktop, Server or BPIPE)
        /// </summary>
        public ApiType ApiType { get; }

        
        protected DateTimeZone UserTimeZone { get; }

        /// <summary>
        /// The Bloomberg environment (Production or Beta)
        /// </summary>
        public Environment Environment { get; }

        /// <summary>
        /// The broker to use in EMSX
        /// </summary>
        protected string Broker { get; }

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

            if (_isBroker)
            {
                _blotterInitializedEvent.Reset(2);
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
            }

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

            if (_isBroker)
            {
                InitializeEmsxFieldData();
                Orders = new BloombergOrders();
                _orderSubscriptionHandler = new OrderSubscriptionHandler(this, _orderProvider, Orders);
                if (_execution)
                {
                    SubscribeToEmsx();
                    _blotterInitializedEvent.Wait();
                }
                else
                {
                    Log.Debug("Not subscribing to order events - execution is disabled.");
                }
            }

            _isConnected = true;
        }

        /// <summary>
        /// Disconnects the client from the broker's remote servers
        /// </summary>
        public override void Disconnect()
        {
            Log.Trace("BloombergBrokerage.Disconnect(): Stopping EMS session.");
            try
            {
                _sessionEms?.Stop();
            }
            catch (Exception e)
            {
                Log.Error(e, "Unable to stop the EMS session cleanly.");
            }

            try
            {
                _sessionMarketData?.Stop();
            }
            catch (Exception e)
            {
                Log.Error(e, "Unable to stop the market data session cleanly.");
            }

            try
            {
                _sessionReferenceData?.Stop();
            }
            catch (Exception e)
            {
                Log.Error(e, "Unable to stop the reference data session cleanly.");
            }

            _isConnected = false;
        }

        /// <summary>
        /// Gets all open orders on the account.
        /// NOTE: The order objects returned do not have QC order IDs.
        /// </summary>
        /// <returns>The open orders returned from Bloomberg</returns>
        public override List<Order> GetOpenOrders()
        {
            return Orders
                    .Where(IsValidOrder)
                    .Select(ConvertOrder)
                    .ToList();
        }

        internal bool IsValidOrder(BloombergOrder bbgOrder)
        {
            return bbgOrder.Status.IsOpen() && bbgOrder.Amount != 0 && bbgOrder.IsLeanOrder;
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
            if (!string.IsNullOrWhiteSpace(Broker))
            {
                request.Set(BloombergNames.EMSXBroker, Broker);
            }

            if (!string.IsNullOrWhiteSpace(_strategy))
            {
                var element = request["EMSX_STRATEGY_PARAMS"];
                element.SetElement("EMSX_STRATEGY_NAME", _strategy);
            }

            request.Set(BloombergNames.EMSXHandInstruction, _handlingInstruction);
            // Set fields that map back to internal order ids
            request.Set(BloombergNames.EMSXReferenceOrderIdRequest, order.Id);
            request.Set(BloombergNames.EMSXReferenceRouteId, order.Id);
            PopulateRequest(request, order);
            // Only 1 response should be received.
            var response = _sessionEms.SendRequestSynchronous(request).SingleOrDefault();
            var result = DetermineResult(response);
            if (result)
            {
                var sequence = response.GetSequence();
                order.BrokerId.Add(sequence.ToString());
                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero) {Status = OrderStatus.Submitted});
            }

            return result;
        }

        private void PopulateRequest(Request request, Order order)
        {
            request.Set(BloombergNames.EMSXAmount, Convert.ToInt32(order.AbsoluteQuantity));
            request.Set(BloombergNames.EMSXOrderType, ConvertOrderType(order.Type));
            request.Set(BloombergNames.EMSXTif, ConvertTimeInForce(order));

            var gtdTimeInForce = order.TimeInForce as GoodTilDateTimeInForce;
            if (gtdTimeInForce != null)
            {
                request.Set(BloombergNames.EMSXGTDDate, Convert.ToInt32(gtdTimeInForce.Expiry.ToString("yyyyMMdd")));
            }

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
            var sequence = int.Parse(order.BrokerId[0]);
            Log.Trace($"Updating order {order.Id}, sequence:{sequence}");

            if (!_allowModification)
            {
                Log.Error($"Modification of order is not allowed [id:{order.Id}]");
                return false;
            }

            /*
             WARN: This code is experimental at this point and has not been tested with correct responses.

             EMSX uses a form of state machine to manage the state of an order, it's child route, and the route's status according to the broker.
             This is documented here: https://emsx-api-doc.readthedocs.io/en/latest/programmable/emsxSubscription.html#description-of-the-child-route-status-changes

             Essentially, The state of the order, according to the broker, needs to be managed.
             In testing, the automated broker BMTB was responding with rejections when modifying the order.

              It may be that with a real broker, for an order to be modified:
                - If increasing, the order needs to be increased - followed by the route - so that headroom is available.
                - If decreasing, the route needs to be decreased - followed by the order - so that the headroom can be lowered.
            */
            var orderRequest = _serviceEms.CreateRequest(BloombergNames.ModifyOrderEx.ToString());
            orderRequest.Set(BloombergNames.EMSXSequence, sequence);
            PopulateRequest(orderRequest, order);
            var orderResult = DetermineResult(_sessionEms.SendRequestSynchronous(orderRequest).SingleOrDefault());
            var routeRequest = _serviceEms.CreateRequest(BloombergNames.ModifyRouteEx.ToString());
            routeRequest.Set(BloombergNames.EMSXSequence, sequence);
            routeRequest.Set(BloombergNames.EMSXRouteId, 1);
            PopulateRequest(routeRequest, order);

            return DetermineResult(_sessionEms.SendRequestSynchronous(routeRequest).SingleOrDefault());
        }

        /// <summary>
        /// Cancels the order with the specified ID
        /// </summary>
        /// <param name="order">The order to cancel</param>
        /// <returns>True if the request was made for the order to be canceled, false otherwise</returns>
        public override bool CancelOrder(Order order)
        {
            var sequence = int.Parse(order.BrokerId[0]);
            Log.Trace($"Cancelling order {order.Id}, sequence:{sequence}");

            var cancelOrderRequest = _serviceEms.CreateRequest(BloombergNames.CancelOrderEx.ToString());
            cancelOrderRequest.GetElement(BloombergNames.EMSXSequence).AppendValue(sequence);

            foreach (var response in _sessionEms.SendRequestSynchronous(cancelOrderRequest))
            {
                DetermineResult(response);
            }

            return true;
        }

        private bool DetermineResult(Message message)
        {
            Log.Trace($"Received response: '{message.MessageType}': {message}");

            if (message.IsFailed())
            {
                var requestFailure = new BloombergRequestFailure(message);
                var errorMessage = $"Request Failed: '{message.MessageType}' - {requestFailure}";
                FireBrokerMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, requestFailure.ErrorCode, errorMessage));
                return false;
            }

            if (Equals(message.MessageType, BloombergNames.ErrorInfo))
            {
                var code = message.GetElementAsInt32(BloombergNames.ErrorCode);
                var errorMessage = $"Failed: '{message.MessageType}' - {message.GetElementAsString(BloombergNames.ErrorMessage)}";
                FireBrokerMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, code, errorMessage));
                return false;
            }

            var description = new StringBuilder("Completed: '").Append(message.MessageType).Append('\'');
            if (message.HasElement(BloombergNames.Message))
            {
                description.Append(message.GetElementAsString(BloombergNames.Message));
            }
            else
            {
                description.Append(message);
            }

            FireBrokerMessage(new BrokerageMessageEvent(BrokerageMessageType.Information, 1, description.ToString()));
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
            _blotterInitializedEvent?.Dispose();
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

        private void InitializeEmsxFieldData()
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
                case Event.EventType.SUBSCRIPTION_STATUS:
                    ProcessEmsxEvent(@event);
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

        private void ProcessSessionEvent(Event @event, Session session)
        {
            foreach (var message in @event)
            {
                if (message.MessageType.Equals(BloombergNames.SessionStarted))
                {
                    Log.Trace("BloombergBrokerage.ProcessSessionEvent(): Session started.");
                }
                else if (message.MessageType.Equals(BloombergNames.SessionStartupFailure))
                {
                    Log.Error("BloombergBrokerage.ProcessSessionEvent(): Session startup failure.");
                    BrokerMessage(BrokerageMessageType.Error, message);
                }
                else if (message.MessageType.Equals(BloombergNames.SessionTerminated))
                {
                    Log.Error("BloombergBrokerage.ProcessSessionEvent(): Session terminated.");
                    BrokerMessage(BrokerageMessageType.Disconnect, message);
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

        private void BrokerMessage(BrokerageMessageType type, Message message)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            OnMessage(new BrokerageMessageEvent(type, string.Empty, $"Received '{message.MessageType}' - {message}"));
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

        private void ProcessEmsxEvent(Event @event)
        {
            foreach (var message in @event)
            {
                _orderSubscriptionHandler.ProcessMessage(message);
            }
        }

        private static void ProcessOtherEvents(Event @event, Session session)
        {
            foreach (var message in @event)
            {
                Log.Trace($"BloombergBrokerage.ProcessOtherEvent(): {@event.Type} - {message.MessageType}: {message}");
            }
        }

        private void SubscribeToEmsx()
        {
            Log.Trace("Subscribing to EMSX");

            try
            {
                _sessionEms.Subscribe(new List<Subscription>
                {
                    new Subscription(CreateOrderSubscription(), GetNewCorrelationId()), new Subscription(CreateRouteSubscription(), GetNewCorrelationId())
                });
            }
            catch (Exception exception)
            {
                Log.Error(exception);
            }
        }

        private string CreateOrderSubscription()
        {
            var fields = _orderFieldDefinitions.Select(x => x.Name);

            var serviceName = GetServiceName(ServiceType.Ems);
            return $"{serviceName}/order?fields={string.Join(",", fields)}";
        }

        private string CreateRouteSubscription()
        {
            var fields = _routeFieldDefinitions.Select(x => x.Name);
            var serviceName = GetServiceName(ServiceType.Ems);
            return $"{serviceName}/route?fields={string.Join(",", fields)}";
        }

        public void SignalBlotterInitialised()
        {
            _blotterInitializedEvent.Signal();
        }

        public bool IsInitialized()
        {
            return _blotterInitializedEvent.IsSet;
        }

        protected Order ConvertOrder(BloombergOrder order)
        {
            var securityType = ConvertSecurityType(order.GetString(SubType.Order, BloombergNames.EMSXAssetClass));

            var symbol = _symbolMapper.GetLeanSymbol(order.GetString(SubType.Order, BloombergNames.EMSXTicker), securityType);
            var quantity = order.Amount;
            var orderType = ConvertOrderType(order.GetString(SubType.Order, BloombergNames.EMSXOrderType));
            var orderDirection = order.GetString(SubType.Order, BloombergNames.EMSXSide) == "BUY" ? OrderDirection.Buy : OrderDirection.Sell;

            var tifValue = order.GetString(SubType.Order, BloombergNames.EMSXTif);
            var timeInForce = ConvertTimeInForce(tifValue, tif =>
            {
                switch (tif)
                {
                    case "GTD": return order.GetDate(SubType.Order, BloombergNames.EMSXGTDDate, true);
                    default:
                        throw new ArgumentOutOfRangeException(nameof(tif), tif, $"TIF '{tif}' required a date, but there isn't a field map for it.");
                }
            });

            if (orderDirection == OrderDirection.Sell)
            {
                quantity = -quantity;
            }

            var orderTime = order.GetDateTimeCombo(SubType.Order, BloombergNames.EMSXDate, BloombergNames.EMSXTimeStampMicrosec, false).ConvertToUtc(UserTimeZone);

            Order newOrder;
            switch (orderType)
            {
                case OrderType.Market:
                    newOrder = new MarketOrder(symbol, quantity, orderTime);
                    break;

                case OrderType.Limit:
                    {
                        var limitPrice = order.GetDecimal(SubType.Route, BloombergNames.EMSXLimitPrice, false);
                        newOrder = new LimitOrder(symbol, quantity, limitPrice, orderTime);
                    }
                    break;

                case OrderType.StopMarket:
                    {
                        var stopPrice = order.GetDecimal(SubType.Route, BloombergNames.EMSXStopPrice, false);
                        newOrder = new LimitOrder(symbol, quantity, stopPrice, orderTime);
                    }
                    break;

                case OrderType.StopLimit:
                    {
                        var limitPrice = order.GetDecimal(SubType.Route, BloombergNames.EMSXLimitPrice, false);
                        var stopPrice = order.GetDecimal(SubType.Route, BloombergNames.EMSXStopPrice, false);
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
            switch (orderType)
            {
                case "MKT":
                    return OrderType.Market;

                case "LMT":
                case "CD":
                    return OrderType.Limit;

                case "ST":
                    return OrderType.StopMarket;

                case "SL":
                    return OrderType.StopLimit;

                case "MOC":
                    return OrderType.MarketOnClose;

                case "FUN":
                case "LOC":
                case "OC":
                case "PEG":
                case "MKTL":
                default:
                    throw new NotSupportedException($"Unsupported order type: {orderType}");
            }
        }

        private string ConvertOrderType(OrderType orderType)
        {
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

                case OrderType.MarketOnClose:
                    return "MOC";

                default:
                    throw new NotSupportedException($"Unsupported order type: {orderType}");
            }
        }

        private TimeInForce ConvertTimeInForce(string timeInForce, Func<string, DateTime> expiryDateFunc)
        {
            switch (timeInForce)
            {
                case "DAY":
                    return TimeInForce.Day;

                case "IOC":
                    // LEAN does not support IOC yet, we map it to GTC for now
                case "GTC":
                    return TimeInForce.GoodTilCanceled;

                case "GTD":
                    return TimeInForce.GoodTilDate(expiryDateFunc(timeInForce));

                case "FOK":
                case "GTX":
                case "OPG":
                case "CLO":
                case "AUC":
                case "DAY+":
                default:
                    throw new NotSupportedException($"Unsupported time in force: {timeInForce}");
            }
        }

        protected virtual string ConvertTimeInForce(Order order)
        {
            var timeInForce = order.TimeInForce;
            if (timeInForce == TimeInForce.Day)
            {
                return "DAY";
            }

            if (timeInForce == TimeInForce.GoodTilCanceled)
            {
                return "GTC";
            }

            if (timeInForce is GoodTilDateTimeInForce)
            {
                return "GTD";
            }

            throw new NotSupportedException($"Unsupported time in force: {timeInForce}");
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