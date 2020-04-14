/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
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
            var subType = message.HasElement(BloombergNames.MessageSubType) ? message.GetElementAsString(BloombergNames.MessageSubType) : null;
            if (string.IsNullOrWhiteSpace(subType))
            {
                return;
            }

            Log.Trace($"OrderSubscriptionHandler.ProcessMessage(): Received [{msgType},{subType}]: {message}");
            if (msgType.Equals(BloombergNames.SubscriptionStarted))
            {
                Log.Trace("OrderSubscriptionHandler.ProcessMessage(): Subscription started: " + subType);
            }
            else if (msgType.Equals(BloombergNames.SubscriptionStreamsActivated))
            {
                Log.Trace("OrderSubscriptionHandler.ProcessMessage(): Subscription stream activated: " + subType);
            }
            else if (msgType.Equals(BloombergNames.OrderRouteFields))
            {
                OnOrderRouting(message, subType);
            }
            else
            {
                Log.Error($"OrderSubscriptionHandler.ProcessMessage(): Unknown message type: {msgType}, message:{message}");
            }
        }

        private void OnOrderRouting(Message message, string subType)
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
                    Log.Trace("OrderSubscriptionHandler.OnOrderRouting(): End of Initial Paint ({0})", subType);
                    _brokerage.SignalBlotterInitialised();
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

        private void OnInitialPaint(Message message, string subType, int sequence)
        {
            Log.Trace($"OrderSubscriptionHandler.OnInitialPaint(): Initial paint (sub-type:{subType}, sequence:{sequence})");
            var order = _orders.GetOrCreateOrder(sequence);
            order.PopulateFields(message, subType);
        }

        private void OnNewOrder(Message message, string subType, int sequence)
        {
            // If an order is created manually in the terminal, we'll still receive an event.
            if (!TryGetOurOrderId(message, out var orderId))
            {
                Log.Trace($"OrderSubscriptionHandler.OnNewOrder(): Ignoring new order event for a manual trade (sequence:{sequence})");
                return;
            }

            _sequenceToOrderId[sequence] = orderId;
            Log.Trace($"OrderSubscriptionHandler.OnNewOrder(): Received (orderId={orderId}, sequence={sequence})");

            var bbOrder = _orders.GetOrCreateOrder(sequence);
            bbOrder.PopulateFields(message, subType);

            if (TryGetOrder(orderId, out var order))
            {
                EmitOrderEvent(bbOrder, order);
            }
        }

        private void OnOrderUpdate(Message message, string subType, int sequence)
        {
            if (!_sequenceToOrderId.TryGetValue(sequence, out var orderId))
            {
                Log.Trace($"OrderSubscriptionHandler.OnOrderUpdate(): Ignoring order update event for manual order (sequence:{sequence})");
                return;
            }

            Log.Trace($"OrderSubscriptionHandler.OnOrderUpdate(): Received (orderId={orderId}, sequence={sequence})");
            if (TryGetAndUpdateBloombergOrder(sequence, message, subType, out var bbOrder) && TryGetOrder(orderId, out var order))
            {
                EmitOrderEvent(bbOrder, order);
            }
        }

        private void OnOrderDelete(Message message, string subType, int sequence)
        {
            if (!_sequenceToOrderId.TryGetValue(sequence, out var orderId))
            {
                if (_brokerage.IsInitialized())
                {
                    Log.Error($"OrderSubscriptionHandler.OnOrderDelete(): Deletion received for an unknown sequence '{sequence}': {message}");
                }
                else
                {
                    Log.Trace($"OrderSubscriptionHandler.OnOrderDelete(): Discarding order in a deleted state: '{sequence}'");
                    return;
                }

                return;
            }

            Log.Trace($"OrderSubscriptionHandler.OnOrderDelete(): Received (orderId={orderId}, sequence={sequence})");
            if (TryGetAndUpdateBloombergOrder(sequence, message, subType, out var bbOrder) && TryGetOrder(orderId, out var order))
            {
                EmitOrderEvent(bbOrder, order);
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

        private bool TryGetAndUpdateBloombergOrder(int sequence, Message message, string subType, out BloombergOrder bbOrder, [CallerMemberName] string callerMemberName = null)
        {
            bbOrder = _orders.GetBySequenceNumber(sequence);
            if (bbOrder == null)
            {
                Log.Error($"OrderSubscriptionHandler.TryGetAndUpdateBloombergOrder(): No existing BB order for sequence '{sequence}' [caller:{callerMemberName}]");
                return false;
            }

            bbOrder.PopulateFields(message, subType);
            return true;
        }

        private bool TryGetOrder(int orderId, out Order order, [CallerMemberName] string callerMemberName = null)
        {
            order = _orderProvider.GetOrderById(orderId);
            if (order != null)
            {
                return true;
            }

            Log.Error($"OrderSubscriptionHandler.TryGetOrder(): No order found for '{orderId}' [caller:{callerMemberName}]");
            return false;
        }

        public void EmitOrderEvent(BloombergOrder bbOrder, Order order)
        {
            var orderEvent = new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero) {Status = bbOrder.Status, Quantity = bbOrder.Amount};

            var ticket = _orderProvider.GetOrderTicket(order.Id);
            if (ticket != null)
            {
                orderEvent.FillPrice = bbOrder.GetDecimal(SubType.Route, BloombergNames.EMSXAvgPrice, false);

                // The Bloomberg API does not return the individual quantity for each partial fill, but the cumulative filled quantity
                var fillQuantity = bbOrder.Filled - Math.Abs(ticket.QuantityFilled);
                orderEvent.FillQuantity = fillQuantity * Math.Sign(ticket.Quantity);
            }
            else if (orderEvent.Status == OrderStatus.Filled || orderEvent.Status == OrderStatus.PartiallyFilled)
            {
                Log.Error($"OrderSubscriptionHandler.EmitOrderEvent(): OrderTicket not found for OrderId: {order.Id}, but we have fills: {bbOrder.Filled}");
            }

            if (!_lastEvent.TryGetValue(bbOrder.Sequence, out var evt) || !evt.Equals(orderEvent))
            {
                _lastEvent[bbOrder.Sequence] = evt;
                _brokerage.FireOrderEvent(orderEvent);
            }
        }

        private static EventStatus GetEventStatus(Message message)
        {
            return (EventStatus) message.GetElement(BloombergNames.EventStatus).GetValue();
        }
    }
}