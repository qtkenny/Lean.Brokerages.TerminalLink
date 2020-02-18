/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
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
        // TODO: These concurrent dictionaries are not currently being cleaned up after orders are completed.
        private readonly ConcurrentDictionary<int, int> _sequenceToOrderId = new ConcurrentDictionary<int, int>();
        private readonly ConcurrentDictionary<int, int> _orderToSequenceId = new ConcurrentDictionary<int, int>();

        public OrderSubscriptionHandler(BloombergBrokerage brokerage, IOrderProvider orderProvider, BloombergOrders orders)
        {
            _brokerage = brokerage;
            _orderProvider = orderProvider;
            _orders = orders;
        }

        public bool TryGetSequenceId(int orderId, out int sequence)
        {
            return _orderToSequenceId.TryGetValue(orderId, out sequence);
        }

        private static void LogRequestCompletion(Message message)
        {
            Log.Trace($"OrderSubscriptionHandler.LogRequestCompletion(): {message.MessageType} - {GetSequence(message)}");
        }

        private static int GetSequence(Message message)
        {
            return message.GetElementAsInt32(BloombergNames.EMSXSequence);
        }

        private static int? GetRoute(Message message)
        {
            return message.HasElement(BloombergNames.EMSXRouteId) ? message.GetElementAsInt32(BloombergNames.EMSXRouteId) : (int?) null;
        }

        private void OnOrderRouting(Message message)
        {
            var eventStatus = GetEventStatus(message);
            if (eventStatus == EventStatus.Heartbeat)
            {
                return;
            }

            var sequence = GetSequence(message);
            var route = GetRoute(message);
            var subType = message.GetElementAsString(BloombergNames.MessageSubType);
            Log.Trace($"OrderSubscriptionHandler: Message received: '{eventStatus}' [sequence:{sequence},route:{route},sub-type:{subType}]");
            switch (eventStatus)
            {
                case EventStatus.InitialPaint:
                    // Initial order statuses.
                    var order = _orders.GetBySequenceNumber(sequence) ?? _orders.CreateOrder(sequence);
                    order.PopulateFields(message, false);
                    break;
                case EventStatus.EndPaint:
                    // End of the stream of initial orders.
                    Log.Trace("OrderSubscriptionHandler: End of Initial Paint");
                    _brokerage.SetBlotterInitialized();
                    break;
                case EventStatus.New:
                    OnNewOrder(message, sequence);
                    break;
                case EventStatus.Update:
                    OnOrderUpdate(message, sequence);
                    break;
                case EventStatus.Delete:
                    OnOrderDelete(message, sequence);
                    break;
                default:
                    Log.Trace("Order route fields update: " + eventStatus);
                    break;
            }
        }

        private void OnNewOrder(Message message, int sequence)
        {
            var orderId = GetOurOrderId(message);
            _sequenceToOrderId[sequence] = orderId;
            _orderToSequenceId[orderId] = sequence;
            Log.Trace($"OrderSubscriptionHandler.OnNewOrder():{orderId}, EMSX Sequence:{sequence}");

            var bbOrder = _orders.GetBySequenceNumber(sequence) ?? _orders.CreateOrder(sequence);
            bbOrder.PopulateFields(message, false);

            var order = _orderProvider.GetOrderById(orderId);
            if (order == null)
            {
                Log.Error($"OrderSubscriptionHandler.OnNewOrder: OrderId not found: {orderId}, EMSX Sequence: {sequence}");
            }
            else
            {
                // TODO: This is not persisted at the moment.
                order.BrokerId.Add(sequence.ToString());
                _brokerage.FireOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, "Bloomberg Order Event") {Status = OrderStatus.Submitted});
            }
        }

        private static int GetOurOrderId(Message message)
        {
            if (!message.HasElement(BloombergNames.EMSXReferenceOrderIdResponse))
            {
                throw new Exception($"Message does not contain expected field: {BloombergNames.EMSXReferenceOrderIdResponse}, message:{message}");
            }

            var element = message.GetElementAsString(BloombergNames.EMSXReferenceOrderIdResponse);
            if (!int.TryParse(element, out var id))
            {
                throw new Exception("Reference order could not be parsed to an integer, message: " + message);
            }

            return id;
        }

        private void OnOrderUpdate(Message message, int sequence)
        {
            var orderId = GetOurOrderId(message);
            // Order should already exist. If it doesn't create it anyway.
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
                var orderStatus = _brokerage.ConvertOrderStatus(message.GetElementAsString(BloombergNames.EMSXStatus));
                var orderEvent = new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, "Bloomberg Order Event") {Status = orderStatus};
                // TODO: Lean is expecting the fill quantity to be the amount filled in this event. BBG provide the total filled.
                if (orderStatus == OrderStatus.Filled || orderStatus == OrderStatus.PartiallyFilled)
                {
                    orderEvent.FillPrice = Convert.ToDecimal(message.GetElementAsFloat32(BloombergNames.EMSXAvgPrice));
                    orderEvent.FillQuantity = Convert.ToDecimal(message.GetElementAsInt64(BloombergNames.EMSXFilled));
                }

                _brokerage.FireOrderEvent(orderEvent);
            }
        }

        private void OnOrderDelete(Message message, int sequence)
        {
            // Deletion messages from EMSX don't include the reference id
            if (!_sequenceToOrderId.TryGetValue(sequence, out var orderId))
            {
                Log.Error("Unknown order: emsx id:" + sequence);
                return;
            }

            // Order should already exist. If it doesn't create it anyway.
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
                _brokerage.FireOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, "Bloomberg Order Event") {Status = OrderStatus.Canceled});
            }
        }

        private static EventStatus GetEventStatus(Message message)
        {
            return (EventStatus) message.GetElement(BloombergNames.EventStatus).GetValue();
        }

        public void ProcessMessage(Message message)
        {
            var msgType = message.MessageType;
            if (msgType.Equals(BloombergNames.SubscriptionStarted))
            {
                Log.Trace("Order subscription started");
            }
            else if (msgType.Equals(BloombergNames.SubscriptionStreamsActivated))
            {
                Log.Trace("Order subscription streams activated");
            }
            else if (msgType.Equals(BloombergNames.CreateOrderAndRouteEx) || msgType.Equals(BloombergNames.ModifyOrderEx) || msgType.Equals(BloombergNames.DeleteOrder))
            {
                LogRequestCompletion(message);
            }
            else if (msgType.Equals(BloombergNames.OrderRouteFields))
            {
                OnOrderRouting(message);
            }
            else if (msgType.Equals(BloombergNames.OrderErrorInfo))
            {
                Log.Error("Order subscription error: " + message);
            }
            else
            {
                Log.Error($"Unknown message type: {msgType}, message:{message}");
                Debugger.Break();
            }
        }
    }
}