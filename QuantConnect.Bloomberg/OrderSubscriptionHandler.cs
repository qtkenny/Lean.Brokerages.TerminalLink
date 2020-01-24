/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using Bloomberglp.Blpapi;
using QuantConnect.Logging;

namespace QuantConnect.Bloomberg
{
    public class OrderSubscriptionHandler : IMessageHandler
    {
        private readonly BloombergBrokerage _brokerage;
        private readonly BloombergOrders _orders;

        public OrderSubscriptionHandler(BloombergBrokerage brokerage, BloombergOrders orders)
        {
            _brokerage = brokerage;
            _orders = orders;
        }

        public void ProcessMessage(Message message)
        {
            Log.Trace("OrderSubscriptionHandler: Processing message");
            Log.Trace($"Message: {message}");

            if (message.MessageType.Equals(BloombergNames.SubscriptionStarted))
            {
                Log.Trace("Order subscription started");
                return;
            }

            var eventStatus = (EventStatus)message.GetElementAsInt32("EVENT_STATUS");

            switch (eventStatus)
            {
                case EventStatus.Heartbeat:
                    Log.Trace("OrderSubscriptionHandler: HEARTBEAT received");
                    break;

                case EventStatus.InitialPaint:
                    {
                        Log.Trace("OrderSubscriptionHandler: INIT_PAINT message received");
                        Log.Trace($"Message: {message}");
                        var sequence = message.GetElementAsInt32("EMSX_SEQUENCE");
                        var order = _orders.GetBySequenceNumber(sequence);
                        if (order == null)
                        {
                            // Order not found
                            order = _orders.CreateOrder(sequence);
                        }

                        order.PopulateFields(message, false);
                    }
                    break;

                case EventStatus.New:
                    {
                        // new
                        Log.Trace("OrderSubscriptionHandler: NEW_ORDER_ROUTE message received");
                        Log.Trace($"Message: {message}");

                        var sequence = message.GetElementAsInt32("EMSX_SEQUENCE");
                        var order = _orders.GetBySequenceNumber(sequence);
                        if (order == null)
                        {
                            // Order not found
                            order = _orders.CreateOrder(sequence);
                        }

                        order.PopulateFields(message, false);
                    }
                    break;

                case EventStatus.Update:
                    {
                        // update
                        Log.Trace("OrderSubscriptionHandler: UPD_ORDER_ROUTE message received");
                        Log.Trace($"Message: {message}");

                        // Order should already exist. If it doesn't create it anyway.
                        var sequence = message.GetElementAsInt32("EMSX_SEQUENCE");
                        var order = _orders.GetBySequenceNumber(sequence);
                        if (order == null)
                        {
                            // Order not found
                            Log.Trace("OrderSubscriptionHandler: WARNING > Update received for unknown order");
                            order = _orders.CreateOrder(sequence);
                        }

                        order.PopulateFields(message, true);
                    }
                    break;

                case EventStatus.Delete:
                    {
                        // deleted/expired
                        Log.Trace("OrderSubscriptionHandler: DELETE message received");
                        Log.Trace($"Message: {message}");

                        // Order should already exist. If it doesn't create it anyway.
                        var sequence = message.GetElementAsInt32("EMSX_SEQUENCE");
                        var order = _orders.GetBySequenceNumber(sequence);
                        if (order == null)
                        {
                            // Order not found
                            Log.Trace("OrderSubscriptionHandler: WARNING > Delete received for unknown order");
                            order = _orders.CreateOrder(sequence);
                        }

                        order.PopulateFields(message, false);
                    }
                    break;

                case EventStatus.EndPaint:
                    // End of initial paint messages
                    Log.Trace("OrderSubscriptionHandler: End of Initial Paint");
                    _brokerage.SetBlotterInitialized();
                    break;
            }
        }
    }
}
