/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using Bloomberglp.Blpapi;
using QuantConnect.Orders;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Orders.Fees;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace QuantConnect.Bloomberg
{
    public class OrderSubscriptionHandler : IMessageHandler
    {
        private readonly BloombergBrokerage _brokerage;
        private readonly IOrderProvider _orderProvider;
        private readonly BloombergOrders _orders;
        private readonly ConcurrentDictionary<int, int> _sequenceToOrderId = new ConcurrentDictionary<int, int>();

        private readonly Dictionary<int, OrderEvent> _lastEvent = new Dictionary<int, OrderEvent>();

        public OrderSubscriptionHandler(BloombergBrokerage brokerage, IOrderProvider orderProvider, BloombergOrders orders)
        {
            _brokerage = brokerage;
            _orderProvider = orderProvider;
            _orders = orders;
        }

        public void ProcessMessage(Message message)
        {
            var msgType = message.MessageType;
            var subTypeStr = message.HasElement(BloombergNames.MessageSubType) ? message.GetElementAsString(BloombergNames.MessageSubType) : null;
            if (string.IsNullOrWhiteSpace(subTypeStr))
            {
                return;
            }

            var subType = subTypeStr.Equals("R", StringComparison.InvariantCultureIgnoreCase) ? SubType.Route : SubType.Order;
            Log.Trace($"OrderSubscriptionHandler.ProcessMessage(type={subType}): Received [{msgType}]: {message}");
            if (msgType.Equals(BloombergNames.SubscriptionStarted))
            {
                Log.Trace($"OrderSubscriptionHandler.ProcessMessage(type={subType}): Subscription started");
            }
            else if (msgType.Equals(BloombergNames.SubscriptionStreamsActivated))
            {
                Log.Trace($"OrderSubscriptionHandler.ProcessMessage(type={subType}): Subscription stream activated");
            }
            else if (msgType.Equals(BloombergNames.OrderRouteFields))
            {
                OnOrderRouting(message, subType);
            }
            else
            {
                Log.Error($"OrderSubscriptionHandler.ProcessMessage(type={subType}): Unknown message type: {msgType}, message:{message}");
            }
        }

        private void OnOrderRouting(Message message, SubType subType)
        {
            var eventStatus = GetEventStatus(message);
            if (eventStatus == EventStatus.Heartbeat)
            {
                return;
            }

            var sequence = message.GetSequence();
            switch (eventStatus)
            {
                case EventStatus.InitialPaint:
                    // Initial order statuses.
                    OnInitialPaint(message, subType, sequence);
                    break;
                case EventStatus.EndPaint:
                    // End of the stream of initial orders.
                    Log.Trace($"OrderSubscriptionHandler.OnOrderRouting(type={subType}): End of Initial Paint");
                    _brokerage.SignalBlotterInitialized();
                    break;
                case EventStatus.New:
                    OnNewOrder(message, subType, sequence);
                    break;
                case EventStatus.Update:
                    OnOrderUpdate(message, subType, sequence);
                    break;
                case EventStatus.Delete:
                    OnOrderDelete(message, subType, sequence);
                    break;
                default: throw new ArgumentOutOfRangeException(nameof(eventStatus), eventStatus, $"Unknown event status: {eventStatus}: {message}");
            }
        }

        private void OnInitialPaint(Message message, SubType subType, int sequence)
        {
            Log.Trace($"OrderSubscriptionHandler.OnInitialPaint(seq={sequence},type={subType}): Initial paint");
            var order = _orders.GetOrCreateOrder(sequence);
            order.PopulateFields(message, subType);
        }

        private void OnNewOrder(Message message, SubType subType, int sequence)
        {
            // If an order is created manually in the terminal, we'll still receive an event.
            if (!TryGetOurOrderId(message, out var orderId))
            {
                Log.Trace($"OrderSubscriptionHandler.OnNewOrder(seq={sequence},type={subType}): Ignoring new order event for a manual trade");
                return;
            }

            _sequenceToOrderId[sequence] = orderId;
            Log.Trace($"OrderSubscriptionHandler.OnNewOrder(ord={orderId},seq={sequence},type={subType}): Received");

            var bbOrder = _orders.GetOrCreateOrder(sequence);
            bbOrder.PopulateFields(message, subType);

            if (TryGetOrder(orderId, subType, sequence, out var order))
            {
                EmitOrderEvent(bbOrder, order, subType);
            }
        }

        private void OnOrderUpdate(Message message, SubType subType, int sequence)
        {
            if (!_sequenceToOrderId.TryGetValue(sequence, out var orderId))
            {
                Log.Trace($"OrderSubscriptionHandler.OnOrderUpdate(seq={sequence},type={subType}): Ignoring order update event for manual order");
                return;
            }

            Log.Trace($"OrderSubscriptionHandler.OnOrderUpdate(ord={orderId},seq={sequence},type={subType}): Received");
            if (TryGetAndUpdateBloombergOrder(sequence, message, subType, orderId, out var bbOrder) && TryGetOrder(orderId, subType, sequence, out var order))
            {
                EmitOrderEvent(bbOrder, order, subType);
            }
        }

        private void OnOrderDelete(Message message, SubType subType, int sequence)
        {
            if (!_sequenceToOrderId.TryGetValue(sequence, out var orderId))
            {
                if (_brokerage.IsInitialized())
                {
                    Log.Error($"OrderSubscriptionHandler.OnOrderDelete(seq={sequence},type={subType}): Deletion received for an unknown sequence: {message}");
                }
                else
                {
                    Log.Trace($"OrderSubscriptionHandler.OnOrderDelete(seq={sequence},type={subType}): Discarding order in a deleted state");
                    return;
                }

                return;
            }

            Log.Trace($"OrderSubscriptionHandler.OnOrderDelete(ord={orderId},seq={sequence},type={subType}): Received");
            if (TryGetAndUpdateBloombergOrder(sequence, message, subType, orderId, out var bbOrder) && TryGetOrder(orderId, subType, sequence, out var order))
            {
                EmitOrderEvent(bbOrder, order, subType);
                _lastEvent.Remove(sequence);
            }
        }

        private static bool TryGetOurOrderId(Message message, out int orderId)
        {
            orderId = -1;
            string element;
            if (message.HasElement(BloombergNames.EMSXReferenceOrderIdResponse))
            {
                element = message.GetElementAsString(BloombergNames.EMSXReferenceOrderIdResponse);
            }
            else
            {
                return false;
            }

            if (int.TryParse(element, out orderId))
            {
                return true;
            }

            if (!string.IsNullOrEmpty(element))
            {
                Log.Error("OrderSubscriptionHandler.TryGetOurOrderId(): Unable to parse order id as an integer: " + element);
            }

            return false;
        }

        private bool TryGetAndUpdateBloombergOrder(int sequence, Message message, SubType subType, int orderId, out BloombergOrder bbOrder, [CallerMemberName] string callerMemberName = null)
        {
            bbOrder = _orders.GetBySequenceNumber(sequence);
            if (bbOrder == null)
            {
                Log.Error($"OrderSubscriptionHandler.TryGetAndUpdateBloombergOrder(ord={orderId},seq={sequence},type={subType}): No existing BB order [caller:{callerMemberName}]");
                return false;
            }

            bbOrder.PopulateFields(message, subType);
            return true;
        }

        private bool TryGetOrder(int orderId, SubType subType, int sequence, out Order order, [CallerMemberName] string callerMemberName = null)
        {
            order = _orderProvider.GetOrderById(orderId);
            if (order != null)
            {
                return true;
            }

            Log.Error($"OrderSubscriptionHandler.TryGetOrder(ord={orderId},seq={sequence},type={subType}): No order found [caller:{callerMemberName}]");
            return false;
        }

        private void EmitOrderEvent(BloombergOrder bbOrder, Order order, SubType subType)
        {
            if (subType == SubType.Order)
            {
                Log.Debug($"OrderSubscriptionHandler.EmitOrderEvent(ord={order.Id},seq={bbOrder.Sequence},type={subType}): dropping event");
                return;
            }

            var newOrderEvent = new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero) {Status = bbOrder.Status, Quantity = bbOrder.Amount};

            var ticket = _orderProvider.GetOrderTicket(order.Id);
            if (ticket != null)
            {
                newOrderEvent.FillPrice = bbOrder.GetDecimal(SubType.Route, BloombergNames.EMSXAvgPrice, false);

                // The Bloomberg API does not return the individual quantity for each partial fill, but the cumulative filled quantity
                var fillQuantity = bbOrder.Filled - Math.Abs(ticket.QuantityFilled);
                newOrderEvent.FillQuantity = fillQuantity * Math.Sign(ticket.Quantity);
            }
            else if (newOrderEvent.Status == OrderStatus.Filled || newOrderEvent.Status == OrderStatus.PartiallyFilled)
            {
                Log.Error($"OrderSubscriptionHandler.EmitOrderEvent(ord={order.Id},seq={bbOrder.Sequence},type={subType}): OrderTicket not found, but we have fills: {bbOrder.Filled}");
            }

            if (!_lastEvent.TryGetValue(bbOrder.Sequence, out var lastEvent)
                // if the order status has changed or we got a new fill (partial fill case)
                || lastEvent.Status != newOrderEvent.Status || newOrderEvent.FillQuantity != 0)
            {
                _lastEvent[bbOrder.Sequence] = newOrderEvent;
                _brokerage.FireOrderEvent(newOrderEvent);
            }
            else
            {
                Log.Error($"OrderSubscriptionHandler.EmitOrderEvent(ord={order.Id},seq={bbOrder.Sequence},type={subType}): Duplicated event dropped");
            }
        }

        private static EventStatus GetEventStatus(Message message)
        {
            return (EventStatus) message.GetElement(BloombergNames.EventStatus).GetValue();
        }
    }
}