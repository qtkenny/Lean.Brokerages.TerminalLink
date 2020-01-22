/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Bloomberglp.Blpapi;
using QuantConnect.Brokerages;
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
        private readonly bool _useServerSideApi;
        private readonly string _serverHost;
        private readonly int _serverPort;

        private readonly SessionOptions _sessionOptions;
        private readonly Session _sessionMarketData;

        private readonly Session _sessionHistoricalData;
        private readonly Service _serviceHistoricalData;

        private Session _sessionAuth;
        private Service _serviceAuth;

        private Session _sessionEms;
        private Service _serviceEms;

        private long _nextCorrelationId;
        private readonly ConcurrentDictionary<string, BloombergSubscriptions> _subscriptionsByTopicName = new ConcurrentDictionary<string, BloombergSubscriptions>();
        private readonly ConcurrentDictionary<string, Symbol> _symbolsByTopicName = new ConcurrentDictionary<string, Symbol>();
        private readonly BloombergSymbolMapper _symbolMapper = new BloombergSymbolMapper();

        internal List<SchemaFieldDefinition> OrderFields = new List<SchemaFieldDefinition>();
        internal List<SchemaFieldDefinition> RouteFields = new List<SchemaFieldDefinition>();
        private readonly Dictionary<CorrelationID, IMessageHandler> _requestMessageHandlers = new Dictionary<CorrelationID, IMessageHandler>();
        private readonly Dictionary<CorrelationID, IMessageHandler> _subscriptionMessageHandlers = new Dictionary<CorrelationID, IMessageHandler>();

        private BloombergOrders _orders;

        /// <summary>
        /// Initializes a new instance of the <see cref="BloombergBrokerage"/> class
        /// </summary>
        public BloombergBrokerage(bool useServerSideApi, Environment environment, string serverHost, int serverPort)
            : base("Bloomberg brokerage")
        {
            _useServerSideApi = useServerSideApi;
            Environment = environment;
            _serverHost = serverHost;
            _serverPort = serverPort;

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

            Log.Trace("BloombergBrokerage(): Opening historical data service.");
            var historicalDataServiceName = GetServiceName(ServiceType.HistoricalData);
            if (!_sessionHistoricalData.OpenService(historicalDataServiceName))
            {
                throw new Exception("Unable to open historical data service.");
            }
            _serviceHistoricalData = _sessionAuth.GetService(historicalDataServiceName);
        }

        public Environment Environment { get; }

        #region IBrokerage implementation

        /// <summary>
        /// Returns true if we're currently connected to the broker
        /// </summary>
        public override bool IsConnected { get; }

        /// <summary>
        /// Connects the client to the broker's remote servers
        /// </summary>
        public override void Connect()
        {
            if (_useServerSideApi)
            {
                Log.Trace($"BloombergBrokerage.Connect(): Starting authentication session: {_serverHost}:{_serverPort}:{Environment}.");
                _sessionAuth = new Session(_sessionOptions, OnBloombergEvent);
                if (!_sessionAuth.Start())
                {
                    throw new Exception("Unable to start authentication session.");
                }

                Log.Trace("BloombergBrokerage.Connect(): Opening authentication service.");
                var authServiceName = GetServiceName(ServiceType.Authentication);
                if (!_sessionAuth.OpenService(authServiceName))
                {
                    _sessionAuth.Stop();
                    throw new Exception("Unable to open authentication service.");
                }
                _serviceAuth = _sessionAuth.GetService(authServiceName);

                // TODO: create EMXS after auth

            }
            else
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

                _orders = new BloombergOrders(this);
                _orders.Subscribe();
            }
        }

        /// <summary>
        /// Disconnects the client from the broker's remote servers
        /// </summary>
        public override void Disconnect()
        {
            if (_useServerSideApi)
            {
                Log.Trace("BloombergBrokerage.Disconnect(): Stopping authentication session.");
                _sessionAuth?.Stop();
            }

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
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the current cash balance for each currency held in the brokerage account
        /// </summary>
        /// <returns>The current cash balance for each currency available for trading</returns>
        public override List<CashAmount> GetCashBalance()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Places a new order and assigns a new broker ID to the order
        /// </summary>
        /// <param name="order">The order to be placed</param>
        /// <returns>True if the request for a new order has been placed, false otherwise</returns>
        public override bool PlaceOrder(Order order)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Updates the order with the same id
        /// </summary>
        /// <param name="order">The new order information</param>
        /// <returns>True if the request was made for the order to be updated, false otherwise</returns>
        public override bool UpdateOrder(Order order)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Cancels the order with the specified ID
        /// </summary>
        /// <param name="order">The order to cancel</param>
        /// <returns>True if the request was made for the order to be canceled, false otherwise</returns>
        public override bool CancelOrder(Order order)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
            _sessionMarketData?.Stop();
            _sessionHistoricalData?.Stop();
            _sessionAuth?.Stop();
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

                case ServiceType.Authentication:
                    return "//blp/apiauth";

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
            var orderRouteFields = _serviceEms.GetEventDefinition("OrderRouteFields");
            var typeDef = orderRouteFields.TypeDefinition;

            for (var i = 0; i < typeDef.NumElementDefinitions; i++)
            {
                var e = typeDef.GetElementDefinition(i);

                var f = new SchemaFieldDefinition(e);

                if (f.IsOrderField())
                {
                    OrderFields.Add(f);
                }

                if (f.IsRouteField())
                {
                    RouteFields.Add(f);
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
            foreach (var msg in @event)
            {
                if (msg.MessageType.Equals(BloombergNames.SlowConsumerWarning))
                {
                    Log.Trace("BloombergBrokerage.ProcessAdminEvent(): Slow Consumer Warning.");
                }
                else if (msg.MessageType.Equals(BloombergNames.SlowConsumerWarningCleared))
                {
                    Log.Trace("BloombergBrokerage.ProcessAdminEvent(): Slow Consumer Warning cleared.");
                }
            }
        }

        private static void ProcessSessionEvent(Event @event, Session session)
        {
            foreach (var msg in @event)
            {
                if (msg.MessageType.Equals(BloombergNames.SessionStarted))
                {
                    Log.Trace("BloombergBrokerage.ProcessSessionEvent(): Session started.");
                }
                else if (msg.MessageType.Equals(BloombergNames.SessionStartupFailure))
                {
                    Log.Trace("BloombergBrokerage.ProcessSessionEvent(): Session startup failure.");
                }
                else if (msg.MessageType.Equals(BloombergNames.SessionTerminated))
                {
                    Log.Trace("BloombergBrokerage.ProcessSessionEvent(): Session terminated.");
                }
                else if (msg.MessageType.Equals(BloombergNames.SessionConnectionUp))
                {
                    Log.Trace("BloombergBrokerage.ProcessSessionEvent(): Session connection up.");
                }
                else if (msg.MessageType.Equals(BloombergNames.SessionConnectionDown))
                {
                    Log.Trace("BloombergBrokerage.ProcessSessionEvent(): Session connection down.");
                }
            }
        }

        private static void ProcessServiceEvent(Event @event, Session session)
        {
            foreach (var msg in @event)
            {
                if (msg.MessageType.Equals(BloombergNames.ServiceOpened))
                {
                    Log.Trace("BloombergBrokerage.ProcessServiceEvent(): Service opened.");
                }
                else if (msg.MessageType.Equals(BloombergNames.ServiceOpenFailure))
                {
                    Log.Trace("BloombergBrokerage.ProcessServiceEvent(): Service open failed.");
                }
            }
        }

        private void ProcessSubscriptionDataEvent(Event @event, Session session)
        {
            Log.Trace("BloombergBrokerage.ProcessSubscriptionDataEvent(): Processing SUBSCRIPTION_DATA event.");

            foreach (var msg in @event)
            {
                var correlationId = msg.CorrelationID;
                IMessageHandler mh;
                if (!_subscriptionMessageHandlers.TryGetValue(correlationId, out mh))
                {
                    Log.Trace($"BloombergBrokerage.ProcessSubscriptionDataEvent(): Unexpected SUBSCRIPTION_DATA event received (CID={correlationId}): {msg}");
                }
                else
                {
                    mh.ProcessMessage(msg);
                }
            }
        }

        private void ProcessSubscriptionStatusEvent(Event @event, Session session)
        {
            Log.Trace("BloombergBrokerage.ProcessSubscriptionStatusEvent(): Processing SUBSCRIPTION_STATUS event.");

            foreach (var msg in @event)
            {
                var correlationId = msg.CorrelationID;
                IMessageHandler mh;
                if (!_subscriptionMessageHandlers.TryGetValue(correlationId, out mh))
                {
                    Log.Trace($"BloombergBrokerage.ProcessSubscriptionStatusEvent(): Unexpected SUBSCRIPTION_STATUS event received (CID={correlationId}): {msg}");
                }
                else
                {
                    mh.ProcessMessage(msg);
                }
            }
        }

        private void ProcessResponse(Event evt, Session session)
        {
            Log.Trace("BloombergBrokerage.ProcessResponse(): Processing RESPONSE event.");

            foreach (var msg in evt)
            {
                var correlationId = msg.CorrelationID;
                IMessageHandler mh;
                if (!_subscriptionMessageHandlers.TryGetValue(correlationId, out mh))
                {
                    Log.Trace($"BloombergBrokerage.ProcessResponse(): Unexpected RESPONSE event received (CID={correlationId}): {msg}");
                }
                else
                {
                    mh.ProcessMessage(msg);
                    _requestMessageHandlers.Remove(correlationId);
                    Log.Trace($"BloombergBrokerage.ProcessResponse(): MessageHandler removed [{correlationId}]");
                }
            }
        }

        private static void ProcessOtherEvents(Event @event, Session session)
        {
            foreach (var msg in @event)
            {
                Log.Trace($"BloombergBrokerage.ProcessOtherEvent(): {@event.Type} - {msg.MessageType}.");
            }
        }

        internal void Subscribe(string topic, IMessageHandler handler)
        {
            var correlationId = new CorrelationID();
            _subscriptionMessageHandlers.Add(correlationId, handler);

            Log.Trace($"Added Subscription message handler: {correlationId}");

            try
            {
                _sessionEms.Subscribe(new List<Subscription>
                {
                    new Subscription(topic, correlationId)
                });
            }
            catch (Exception e)
            {
                Log.Error(e);
            }
        }

        private Order ConvertOrder(BloombergOrder order)
        {
            // TODO:
            return new LimitOrder();
        }
    }
}