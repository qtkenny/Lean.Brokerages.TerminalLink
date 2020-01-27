/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using Bloomberglp.Blpapi;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;

namespace QuantConnect.Bloomberg
{
    public class OrderSubscriptionHandler : IMessageHandler
    {
        private readonly BloombergBrokerage _brokerage;
        private readonly IOrderProvider _orderProvider;
        private readonly BloombergOrders _orders;

        public OrderSubscriptionHandler(BloombergBrokerage brokerage, IOrderProvider orderProvider, BloombergOrders orders)
        {
            _brokerage = brokerage;
            _orderProvider = orderProvider;
            _orders = orders;
        }

        public void ProcessMessage(Message message, int orderId)
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
                        var bbOrder = _orders.GetBySequenceNumber(sequence);
                        if (bbOrder == null)
                        {
                            // Order not found
                            bbOrder = _orders.CreateOrder(sequence);
                        }

                        bbOrder.PopulateFields(message, false);

                        var order = _orderProvider.GetOrderById(orderId);
                        if (order == null)
                        {
                            Log.Error($"OrderSubscriptionHandler: OrderId not found: {orderId}");
                        }
                        else
                        {
                            _brokerage.FireOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, "Bloomberg Order Event")
                            {
                                Status = OrderStatus.Submitted
                            });
                        }
                    }
                    break;

                case EventStatus.Update:
                    {
                        // update
                        Log.Trace("OrderSubscriptionHandler: UPD_ORDER_ROUTE message received");
                        Log.Trace($"Message: {message}");

                        // Order should already exist. If it doesn't create it anyway.
                        var sequence = message.GetElementAsInt32("EMSX_SEQUENCE");
                        var bbOrder = _orders.GetBySequenceNumber(sequence);
                        if (bbOrder == null)
                        {
                            // Order not found
                            Log.Trace("OrderSubscriptionHandler: WARNING > Update received for unknown order");
                            bbOrder = _orders.CreateOrder(sequence);
                        }

                        bbOrder.PopulateFields(message, true);

                        var order = _orderProvider.GetOrderById(orderId);
                        if (order == null)
                        {
                            Log.Error($"OrderSubscriptionHandler: OrderId not found: {orderId}");
                        }
                        else
                        {
                            var orderStatus = _brokerage.ConvertOrderStatus(message.GetElementAsString("EMSX_STATUS"));

                            var orderEvent = new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, "Bloomberg Order Event")
                            {
                                Status = orderStatus
                            };

                            if (orderStatus == OrderStatus.Filled || orderStatus == OrderStatus.PartiallyFilled)
                            {
                                orderEvent.FillPrice = Convert.ToDecimal(message.GetElementAsFloat32("EMSX_FILL_PRICE"));
                                orderEvent.FillQuantity = Convert.ToDecimal(message.GetElementAsInt64("EMSX_FILL_AMOUNT"));
                            }

                            _brokerage.FireOrderEvent(orderEvent);
                        }
                    }
                    break;

                case EventStatus.Delete:
                    {
                        // deleted/expired
                        Log.Trace("OrderSubscriptionHandler: DELETE message received");
                        Log.Trace($"Message: {message}");

                        // Order should already exist. If it doesn't create it anyway.
                        var sequence = message.GetElementAsInt32("EMSX_SEQUENCE");
                        var bbOrder = _orders.GetBySequenceNumber(sequence);
                        if (bbOrder == null)
                        {
                            // Order not found
                            Log.Trace("OrderSubscriptionHandler: WARNING > Delete received for unknown order");
                            bbOrder = _orders.CreateOrder(sequence);
                        }

                        bbOrder.PopulateFields(message, false);

                        var order = _orderProvider.GetOrderById(orderId);
                        if (order == null)
                        {
                            Log.Error($"OrderSubscriptionHandler: OrderId not found: {orderId}");
                        }
                        else
                        {
                            _brokerage.FireOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, "Bloomberg Order Event")
                            {
                                Status = OrderStatus.Canceled
                            });
                        }
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
