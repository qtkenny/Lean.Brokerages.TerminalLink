﻿/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Bloomberglp.Blpapi;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NodaTime;
using QuantConnect.Api;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Orders.TimeInForces;
using QuantConnect.Securities;
using QuantConnect.Util;
using RestSharp;

namespace QuantConnect.TerminalLink
{
    /// <summary>
    /// An implementation of <see cref="IBrokerage"/> for TerminalLink
    /// </summary>
    public partial class TerminalLinkBrokerage : Brokerage, IDataQueueHandler
    {
        private string _serverHost;
        private int _serverPort;
        private readonly string _account;
        private readonly string _strategy;
        private readonly string _notes;
        private readonly string _handlingInstruction;
        private readonly bool _allowModification;
        private Session _sessionMarketData;
        private Session _sessionReferenceData;
        private readonly Session _sessionEms;
        private static long _nextCorrelationId;
        private readonly bool _isBroker;
        private bool _startAtActive;

        private Service _serviceEms;
        private Service _serviceReferenceData;

        private ITerminalLinkSymbolMapper _symbolMapper;
        private SessionOptions _sessionOptions;

        private readonly SchemaFieldDefinitions _orderFieldDefinitions = new SchemaFieldDefinitions();
        private readonly SchemaFieldDefinitions _routeFieldDefinitions = new SchemaFieldDefinitions();

        private MarketHoursDatabase _marketHoursDatabase;

        private readonly IOrderProvider _orderProvider;
        private BrokerageConcurrentMessageHandler<Event> _emsxEventHandler;
        private readonly CountdownEvent _blotterInitializedEvent = new CountdownEvent(2);
        private OrderSubscriptionHandler _orderSubscriptionHandler;
        private bool _isConnected;
        private bool _isInitialized;

        /// <summary>
        /// Initializes a new instance of the <see cref="TerminalLinkBrokerage"/> class
        /// </summary>
        public TerminalLinkBrokerage() : base("TerminalLink brokerage")
        {
            _isBroker = false;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TerminalLinkBrokerage"/> class
        /// </summary>
        public TerminalLinkBrokerage(ApiType apiType, Environment environment, string serverHost, int serverPort, ITerminalLinkSymbolMapper symbolMapper, IDataAggregator aggregator)
            : base("TerminalLink brokerage")
        {
            Initialize(apiType, environment, serverHost, serverPort, symbolMapper, aggregator);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TerminalLinkBrokerage"/> class
        /// </summary>
        public TerminalLinkBrokerage(IOrderProvider orderProvider, ApiType apiType, Environment environment, string serverHost, int serverPort, ITerminalLinkSymbolMapper symbolMapper, IDataAggregator aggregator)
            : this(apiType, environment, serverHost, serverPort, symbolMapper, aggregator)
        {
            _isBroker = true;
            _orderProvider = orderProvider;
            UserTimeZone = DateTimeZoneProviders.Tzdb[Config.GetValue("terminal-link-emsx-user-time-zone", TimeZones.Utc.Id)];
            Broker = Config.GetValue<string>("terminal-link-emsx-broker") ?? throw new Exception("EMSX requires a broker");
            _account = Config.GetValue<string>("terminal-link-emsx-account");
            _strategy = Config.GetValue<string>("terminal-link-emsx-strategy");
            _notes = Config.GetValue<string>("terminal-link-emsx-notes");
            _handlingInstruction = Config.GetValue<string>("terminal-link-emsx-handling");
            _allowModification = Config.GetBool("terminal-link-allow-modification");
            _sessionEms = new Session(_sessionOptions, OnTerminalLinkEvent);
        }

        protected TerminalLinkOrders Orders { get; private set; }

        /// <summary>
        /// The API type (Desktop, Server or BPIPE)
        /// </summary>
        public ApiType ApiType { get; private set; }

        
        protected DateTimeZone UserTimeZone { get; }

        /// <summary>
        /// The TerminalLink environment (Production or Beta)
        /// </summary>
        public Environment Environment { get; private set; }

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
                Log.Trace($"TerminalLinkBrokerage.Connect(): Starting EMS session: {_serverHost}:{_serverPort}:{Environment}.");
                if (!_sessionEms.Start())
                {
                    throw new Exception("Unable to start EMS session.");
                }

                Log.Trace("TerminalLinkBrokerage.Connect(): Opening EMS service.");
                var emsServiceName = GetServiceName(ServiceType.Ems);
                if (!_sessionEms.OpenService(emsServiceName))
                {
                    _sessionEms.Stop();
                    throw new Exception("Unable to open EMS service.");
                }

                _serviceEms = _sessionEms.GetService(emsServiceName);
            }

            // Initialize Market Data
            Log.Trace($"TerminalLinkBrokerage(): Starting market data session: {_serverHost}:{_serverPort}:{Environment}.");
            if (!_sessionMarketData.Start())
            {
                throw new Exception("Unable to start market data session.");
            }

            Log.Trace("TerminalLinkBrokerage(): Opening market data service.");
            var marketDataServiceName = GetServiceName(ServiceType.MarketData);
            if (!_sessionMarketData.OpenService(marketDataServiceName))
            {
                throw new Exception("Unable to open market data service.");
            }
            // Initialize reference data
            Log.Trace($"TerminalLinkBrokerage(): Starting reference data session: {_serverHost}:{_serverPort}:{Environment}.");
            if (!_sessionReferenceData.Start())
            {
                throw new Exception("Unable to start reference data session.");
            }

            Log.Trace("TerminalLinkBrokerage(): Opening reference data service.");
            var referenceDataServiceName = GetServiceName(ServiceType.ReferenceData);
            if (!_sessionReferenceData.OpenService(referenceDataServiceName))
            {
                throw new Exception("Unable to open reference data service.");
            }
            _serviceReferenceData = _sessionReferenceData.GetService(referenceDataServiceName);

            if (_isBroker)
            {
                InitializeEmsxFieldData();
                Orders = new TerminalLinkOrders();
                _orderSubscriptionHandler = new OrderSubscriptionHandler(this, _orderProvider, Orders);
                SubscribeToEmsx();
                _blotterInitializedEvent.Wait();
            }

            _isConnected = true;
        }

        /// <summary>
        /// Disconnects the client from the broker's remote servers
        /// </summary>
        public override void Disconnect()
        {
            Log.Trace("TerminalLinkBrokerage.Disconnect(): Stopping EMS session.");
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
        /// <returns>The open orders returned from TerminalLink</returns>
        public override List<Order> GetOpenOrders()
        {
            return Orders
                    .Where(IsValidOrder)
                    .Select(ConvertOrder)
                    .ToList();
        }

        internal bool IsValidOrder(TerminalLinkOrder bbgOrder)
        {
            return bbgOrder.Status.IsOpen() && bbgOrder.Amount != 0 && bbgOrder.IsLeanOrder;
        }

        /// <summary>
        /// Gets all holdings for the account
        /// </summary>
        /// <returns>The current holdings from the account</returns>
        public override List<Holding> GetAccountHoldings()
        {
            // TerminalLink is not a portfolio management system, we'll need to fetch this information elsewhere
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

            // TerminalLink is not a portfolio management system, we'll need to fetch this information elsewhere
            return new List<CashAmount>();
        }

        /// <summary>
        /// Places a new order and assigns a new broker ID to the order
        /// </summary>
        /// <param name="order">The order to be placed</param>
        /// <returns>True if the request for a new order has been placed, false otherwise</returns>
        public override bool PlaceOrder(Order order)
        {
            var request = _serviceEms.CreateRequest(TerminalLinkNames.CreateOrderAndRouteEx.ToString());
            request.Set(TerminalLinkNames.EMSXTicker, _symbolMapper.GetBrokerageSymbol(order.Symbol));
            request.Set(TerminalLinkNames.EMSXSide, ConvertOrderDirection(order.Direction));
            if (!string.IsNullOrWhiteSpace(Broker))
            {
                request.Set(TerminalLinkNames.EMSXBroker, Broker);
            }

            if (!string.IsNullOrWhiteSpace(_strategy))
            {
                var element = request["EMSX_STRATEGY_PARAMS"];
                element.SetElement("EMSX_STRATEGY_NAME", _strategy);
            }

            request.Set(TerminalLinkNames.EMSXHandInstruction, _handlingInstruction);
            // Set fields that map back to internal order ids
            request.Set(TerminalLinkNames.EMSXReferenceOrderIdRequest, order.Id);
            request.Set(TerminalLinkNames.EMSXReferenceRouteId, order.Id);
            PopulateRequest(request, order);

            var result = false;
            _emsxEventHandler.WithLockedStream(() =>
            {
                // Only 1 response should be received.
                var response = _sessionEms.SendRequestSynchronous(request).SingleOrDefault();
                result = DetermineResult(response);
                if (result)
                {
                    var sequence = response.GetSequence();
                    order.BrokerId.Add(sequence.ToString());

                    _orderSubscriptionHandler?.NewOrderEvent(
                        new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero) { Status = OrderStatus.New }, sequence);
                }
            });

            return result;
        }

        private void PopulateRequest(Request request, Order order)
        {
            request.Set(TerminalLinkNames.EMSXAmount, Convert.ToInt32(order.AbsoluteQuantity));
            request.Set(TerminalLinkNames.EMSXOrderType, ConvertOrderType(order.Type));
            request.Set(TerminalLinkNames.EMSXTif, ConvertTimeInForce(order));

            var gtdTimeInForce = order.TimeInForce as GoodTilDateTimeInForce;
            if (gtdTimeInForce != null)
            {
                request.Set(TerminalLinkNames.EMSXGTDDate, Convert.ToInt32(gtdTimeInForce.Expiry.ToString("yyyyMMdd")));
            }

            if (!string.IsNullOrWhiteSpace(_account))
            {
                request.Set(TerminalLinkNames.EMSXAccount, _account);
            }

            if (!string.IsNullOrWhiteSpace(_notes))
            {
                request.Set(TerminalLinkNames.EMSXNotes, _notes);
            }

            switch (order.Type)
            {
                case OrderType.Limit:
                    request.Set(TerminalLinkNames.EMSXLimitPrice, Convert.ToDouble(((LimitOrder)order).LimitPrice));
                    break;

                case OrderType.StopMarket:
                    request.Set(TerminalLinkNames.EMSXStopPrice, Convert.ToDouble(((StopMarketOrder)order).StopPrice));
                    break;

                case OrderType.StopLimit:
                    request.Set(TerminalLinkNames.EMSXStopPrice, Convert.ToDouble(((StopLimitOrder)order).StopPrice));
                    request.Set(TerminalLinkNames.EMSXLimitPrice, Convert.ToDouble(((StopLimitOrder)order).LimitPrice));
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
            var orderRequest = _serviceEms.CreateRequest(TerminalLinkNames.ModifyOrderEx.ToString());
            orderRequest.Set(TerminalLinkNames.EMSXSequence, sequence);
            PopulateRequest(orderRequest, order);
            var routeRequest = _serviceEms.CreateRequest(TerminalLinkNames.ModifyRouteEx.ToString());
            routeRequest.Set(TerminalLinkNames.EMSXSequence, sequence);
            routeRequest.Set(TerminalLinkNames.EMSXRouteId, 1);
            PopulateRequest(routeRequest, order);

            var orderResult = false;
            var routeResult = false;
            _emsxEventHandler.WithLockedStream(() =>
            {
                orderResult = DetermineResult(_sessionEms.SendRequestSynchronous(orderRequest).SingleOrDefault());
                routeResult = DetermineResult(_sessionEms.SendRequestSynchronous(routeRequest).SingleOrDefault());

                if (orderResult && routeResult)
                {
                    _orderSubscriptionHandler?.NewOrderEvent(
                        new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero) { Status = OrderStatus.UpdateSubmitted }, sequence);
                }
            });

            return orderResult && routeResult;
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

            var cancelOrderRequest = _serviceEms.CreateRequest(TerminalLinkNames.CancelOrderEx.ToString());
            cancelOrderRequest.GetElement(TerminalLinkNames.EMSXSequence).AppendValue(sequence);

            var result = true;
            _emsxEventHandler.WithLockedStream(() =>
            {
                foreach (var response in _sessionEms.SendRequestSynchronous(cancelOrderRequest))
                {
                    result &= DetermineResult(response);
                }

                if (result)
                {
                    _orderSubscriptionHandler?.NewOrderEvent(
                        new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero) { Status = OrderStatus.CancelPending }, sequence);
                }
            });

            return result;
        }

        private bool DetermineResult(Message message)
        {
            Log.Trace($"Received response: '{message.MessageType}': {message}");

            if (message.IsFailed())
            {
                var requestFailure = new TerminalLinkRequestFailure(message);
                var errorMessage = $"Request Failed: '{message.MessageType}' - {requestFailure}";
                FireBrokerMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, requestFailure.ErrorCode, errorMessage));
                return false;
            }

            if (Equals(message.MessageType, TerminalLinkNames.ErrorInfo))
            {
                var code = message.GetElementAsInt32(TerminalLinkNames.ErrorCode);
                var errorMessage = $"Failed: '{message.MessageType}' - {message.GetElementAsString(TerminalLinkNames.ErrorMessage)}";
                FireBrokerMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, code, errorMessage));
                return false;
            }

            var description = new StringBuilder("Completed: '").Append(message.MessageType).Append('\'');
            if (message.HasElement(TerminalLinkNames.Message))
            {
                description.Append(message.GetElementAsString(TerminalLinkNames.Message));
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
            _dataAggregator.DisposeSafely();
            _sessionMarketData?.Stop();
            _sessionReferenceData?.Stop();
            _sessionEms?.Stop();
            _blotterInitializedEvent?.DisposeSafely();
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
                            throw new Exception($"TerminalLinkBrokerage.GetServiceName(): Invalid environment: {Environment}.");
                    }

                default:
                    throw new Exception($"TerminalLinkBrokerage.GetServiceName(): Invalid service type: {serviceType}.");
            }
        }

        private void Initialize(ApiType apiType, Environment environment, string serverHost, int serverPort, ITerminalLinkSymbolMapper symbolMapper, IDataAggregator aggregator)
        {
            if (_isInitialized)
            {
                return;
            }
            _isInitialized = true;
            _dataAggregator = aggregator;
            _symbolMapper = symbolMapper;
            Composer.Instance.AddPart<ISymbolMapper>(symbolMapper);
            _emsxEventHandler = new BrokerageConcurrentMessageHandler<Event>(ProcessEmsxEvent);

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

            _sessionMarketData = new Session(_sessionOptions, OnTerminalLinkMarketDataEvent);
            _sessionReferenceData = new Session(_sessionOptions);
            _startAtActive = Config.GetValue("terminal-link-futures-start-at-active", true);

            _subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();
            _subscriptionManager.SubscribeImpl += (s, t) => Subscribe(s);
            _subscriptionManager.UnsubscribeImpl += (s, t) => Unsubscribe(s);

            // call home
            ValidateSubscription();
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

        private void OnTerminalLinkEvent(Event @event, Session session)
        {
            try
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
                        _emsxEventHandler.HandleNewMessage(@event);
                        break;

                    default:
                        ProcessOtherEvents(@event, session);
                        break;
                }
            }
            catch (Exception e)
            {
                Log.Error(e);
            }
        }

        private static void ProcessAdminEvent(Event @event, Session session)
        {
            foreach (var message in @event)
            {
                if (message.MessageType.Equals(TerminalLinkNames.SlowConsumerWarning))
                {
                    Log.Trace("TerminalLinkBrokerage.ProcessAdminEvent(): Slow Consumer Warning.");
                }
                else if (message.MessageType.Equals(TerminalLinkNames.SlowConsumerWarningCleared))
                {
                    Log.Trace("TerminalLinkBrokerage.ProcessAdminEvent(): Slow Consumer Warning cleared.");
                }
                else
                {
                    Log.Trace($"TerminalLinkBrokerage.ProcessAdminEvent(): Unknown message type '{message.MessageType}': {message}");
                }
            }
        }

        private void ProcessSessionEvent(Event @event, Session session)
        {
            foreach (var message in @event)
            {
                if (message.MessageType.Equals(TerminalLinkNames.SessionStarted))
                {
                    Log.Trace("TerminalLinkBrokerage.ProcessSessionEvent(): Session started.");
                }
                else if (message.MessageType.Equals(TerminalLinkNames.SessionStartupFailure))
                {
                    Log.Error("TerminalLinkBrokerage.ProcessSessionEvent(): Session startup failure.");
                    BrokerMessage(BrokerageMessageType.Error, message);
                }
                else if (message.MessageType.Equals(TerminalLinkNames.SessionTerminated))
                {
                    Log.Error("TerminalLinkBrokerage.ProcessSessionEvent(): Session terminated.");
                    BrokerMessage(BrokerageMessageType.Disconnect, message);
                }
                else if (message.MessageType.Equals(TerminalLinkNames.SessionConnectionUp))
                {
                    Log.Trace("TerminalLinkBrokerage.ProcessSessionEvent(): Session connection up.");
                }
                else if (message.MessageType.Equals(TerminalLinkNames.SessionConnectionDown))
                {
                    Log.Trace("TerminalLinkBrokerage.ProcessSessionEvent(): Session connection down.");
                }
                else
                {
                    Log.Trace($"TerminalLinkBrokerage.ProcessSessionEvent(): Unknown message type: '{message.MessageType}': {message}");
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
                if (message.MessageType.Equals(TerminalLinkNames.ServiceOpened))
                {
                    Log.Trace("TerminalLinkBrokerage.ProcessServiceEvent(): Service opened.");
                }
                else if (message.MessageType.Equals(TerminalLinkNames.ServiceOpenFailure))
                {
                    Log.Trace("TerminalLinkBrokerage.ProcessServiceEvent(): Service open failed.");
                }
                else
                {
                    Log.Trace($"TerminalLinkBrokerage.ProcessServiceEvent(): Unknown message type '{message.MessageType}': {message}");
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
                Log.Trace($"TerminalLinkBrokerage.ProcessOtherEvent(): {@event.Type} - {message.MessageType}: {message}");
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

        public void SignalBlotterInitialized()
        {
            _blotterInitializedEvent.Signal();
        }

        public bool IsInitialized()
        {
            return _blotterInitializedEvent.IsSet;
        }

        protected Order ConvertOrder(TerminalLinkOrder order)
        {
            var securityType = ConvertSecurityType(order.GetString(SubType.Order, TerminalLinkNames.EMSXAssetClass));

            var symbol = _symbolMapper.GetLeanSymbol(order.GetString(SubType.Order, TerminalLinkNames.EMSXTicker), securityType);
            var quantity = order.Amount;
            var orderType = ConvertOrderType(order.GetString(SubType.Order, TerminalLinkNames.EMSXOrderType));
            var orderDirection = order.GetString(SubType.Order, TerminalLinkNames.EMSXSide) == "BUY" ? OrderDirection.Buy : OrderDirection.Sell;

            var tifValue = order.GetString(SubType.Order, TerminalLinkNames.EMSXTif);
            var timeInForce = ConvertTimeInForce(tifValue, tif =>
            {
                switch (tif)
                {
                    case "GTD": return order.GetDate(SubType.Order, TerminalLinkNames.EMSXGTDDate, true);
                    default:
                        throw new ArgumentOutOfRangeException(nameof(tif), tif, $"TIF '{tif}' required a date, but there isn't a field map for it.");
                }
            });

            if (orderDirection == OrderDirection.Sell)
            {
                quantity = -quantity;
            }

            var orderTime = order.GetDateTimeCombo(SubType.Order, TerminalLinkNames.EMSXDate, TerminalLinkNames.EMSXTimeStampMicrosec, false).ConvertToUtc(UserTimeZone);

            Order newOrder;
            switch (orderType)
            {
                case OrderType.Market:
                    newOrder = new MarketOrder(symbol, quantity, orderTime);
                    break;

                case OrderType.Limit:
                    {
                        var limitPrice = order.GetDecimal(SubType.Route, TerminalLinkNames.EMSXLimitPrice, false);
                        newOrder = new LimitOrder(symbol, quantity, limitPrice, orderTime);
                    }
                    break;

                case OrderType.StopMarket:
                    {
                        var stopPrice = order.GetDecimal(SubType.Route, TerminalLinkNames.EMSXStopPrice, false);
                        newOrder = new LimitOrder(symbol, quantity, stopPrice, orderTime);
                    }
                    break;

                case OrderType.StopLimit:
                    {
                        var limitPrice = order.GetDecimal(SubType.Route, TerminalLinkNames.EMSXLimitPrice, false);
                        var stopPrice = order.GetDecimal(SubType.Route, TerminalLinkNames.EMSXStopPrice, false);
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

        private class ModulesReadLicenseRead : Api.RestResponse
        {
            [JsonProperty(PropertyName = "license")]
            public string License;
            [JsonProperty(PropertyName = "organizationId")]
            public string OrganizationId;
        }

        /// <summary>
        /// Validate the user of this project has permission to be using it via our web API.
        /// </summary>
        private static void ValidateSubscription()
        {
            try
            {
                var productId = 44; // TerminalLink Product ID
                var userId = Config.GetInt("job-user-id");
                var token = Config.Get("api-access-token");
                var organizationId = Config.Get("job-organization-id", null);
                // Verify we can authenticate with this user and token
                var api = new ApiConnection(userId, token);
                if (!api.Connected)
                {
                    throw new ArgumentException("Invalid api user id or token, cannot authenticate subscription.");
                }
                // Compile the information we want to send when validating
                var information = new Dictionary<string, object>()
                {
                    {"productId", productId},
                    {"machineName", System.Environment.MachineName},
                    {"userName", System.Environment.UserName},
                    {"domainName", System.Environment.UserDomainName},
                    {"os", System.Environment.OSVersion}
                };
                // IP and Mac Address Information
                try
                {
                    var interfaceDictionary = new List<Dictionary<string, object>>();
                    foreach (var nic in NetworkInterface.GetAllNetworkInterfaces().Where(nic => nic.OperationalStatus == OperationalStatus.Up))
                    {
                        var interfaceInformation = new Dictionary<string, object>();
                        // Get UnicastAddresses
                        var addresses = nic.GetIPProperties().UnicastAddresses
                            .Select(uniAddress => uniAddress.Address)
                            .Where(address => !IPAddress.IsLoopback(address)).Select(x => x.ToString());
                        // If this interface has non-loopback addresses, we will include it
                        if (!addresses.IsNullOrEmpty())
                        {
                            interfaceInformation.Add("unicastAddresses", addresses);
                            // Get MAC address
                            interfaceInformation.Add("MAC", nic.GetPhysicalAddress().ToString());
                            // Add Interface name
                            interfaceInformation.Add("name", nic.Name);
                            // Add these to our dictionary
                            interfaceDictionary.Add(interfaceInformation);
                        }
                    }
                    information.Add("networkInterfaces", interfaceDictionary);
                }
                catch (Exception)
                {
                    // NOP, not necessary to crash if fails to extract and add this information
                }
                // Include our OrganizationId is specified
                if (!string.IsNullOrEmpty(organizationId))
                {
                    information.Add("organizationId", organizationId);
                }
                var request = new RestRequest("modules/license/read", Method.POST) { RequestFormat = DataFormat.Json };
                request.AddParameter("application/json", JsonConvert.SerializeObject(information), ParameterType.RequestBody);
                api.TryRequest(request, out ModulesReadLicenseRead result);
                if (!result.Success)
                {
                    throw new InvalidOperationException($"Request for subscriptions from web failed, Response Errors : {string.Join(',', result.Errors)}");
                }

                var encryptedData = result.License;
                // Decrypt the data we received
                DateTime? expirationDate = null;
                long? stamp = null;
                bool? isValid = null;
                if (encryptedData != null)
                {
                    // Fetch the org id from the response if we are null, we need it to generate our validation key
                    if (string.IsNullOrEmpty(organizationId))
                    {
                        organizationId = result.OrganizationId;
                    }
                    // Create our combination key
                    var password = $"{token}-{organizationId}";
                    var key = SHA256.HashData(Encoding.UTF8.GetBytes(password));
                    // Split the data
                    var info = encryptedData.Split("::");
                    var buffer = Convert.FromBase64String(info[0]);
                    var iv = Convert.FromBase64String(info[1]);
                    // Decrypt our information
                    using var aes = new AesManaged();
                    var decryptor = aes.CreateDecryptor(key, iv);
                    using var memoryStream = new MemoryStream(buffer);
                    using var cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read);
                    using var streamReader = new StreamReader(cryptoStream);
                    var decryptedData = streamReader.ReadToEnd();
                    if (!decryptedData.IsNullOrEmpty())
                    {
                        var jsonInfo = JsonConvert.DeserializeObject<JObject>(decryptedData);
                        expirationDate = jsonInfo["expiration"]?.Value<DateTime>();
                        isValid = jsonInfo["isValid"]?.Value<bool>();
                        stamp = jsonInfo["stamped"]?.Value<int>();
                    }
                }
                // Validate our conditions
                if (!expirationDate.HasValue || !isValid.HasValue || !stamp.HasValue)
                {
                    throw new InvalidOperationException("Failed to validate subscription.");
                }

                var nowUtc = DateTime.UtcNow;
                var timeSpan = nowUtc - Time.UnixTimeStampToDateTime(stamp.Value);
                if (timeSpan > TimeSpan.FromHours(12))
                {
                    throw new InvalidOperationException("Invalid API response.");
                }
                if (!isValid.Value)
                {
                    throw new ArgumentException($"Your subscription is not valid, please check your product subscriptions on our website.");
                }
                if (expirationDate < nowUtc)
                {
                    throw new ArgumentException($"Your subscription expired {expirationDate}, please renew in order to use this product.");
                }
            }
            catch (Exception e)
            {
                Log.Error($"ValidateSubscription(): Failed during validation, shutting down. Error : {e.Message}");
                System.Environment.Exit(1);
            }
        }
    }
}