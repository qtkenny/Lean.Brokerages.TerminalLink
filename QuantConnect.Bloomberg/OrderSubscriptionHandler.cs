/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
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

        public OrderSubscriptionHandler(BloombergBrokerage brokerage, IOrderProvider orderProvider, BloombergOrders orders)
        {
            _brokerage = brokerage;
            _orderProvider = orderProvider;
            _orders = orders;
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
                    Log.Trace("OrderSubscriptionHandler: End of Initial Paint ({0})", subType);
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
                case EventStatus.Heartbeat:
                    // No need to log the heartbeat.
                    break;
                default: throw new Exception($"Unknown order fields update: {eventStatus}: {message}");
            }
        }

        private void OnInitialPaint(Message message, string subType, int sequence)
        {
            Log.Trace($"OrderSubscriptionHandler.OnOrderRouting(): Initial paint (sub-type:{subType}, sequence:{sequence})");
            var order = _orders.GetOrCreateOrder(sequence);
            order.PopulateFields(message, subType, false);
        }

        private void OnNewOrder(Message message, string subType, int sequence)
        {
            // Current assumption is that routes match to the quantity (i.e. 1:1 order to route).
            // With that assumption, we only need to process the route creation event.
            if (subType != "R")
            {
                Log.Trace($"OrderSubscriptionHandler.OnNewOrder(): Ignoring message - new orders are handled via the route message stream (sequence: '{sequence}'): {message}");
                return;
            }

            // If an order is created manually in the terminal, we'll still receive an event.
            if (!TryGetOurOrderId(message, out var orderId))
            {
                Log.Trace($"Ignoring new order event for a manual trade (sequence:{sequence})");
                return;
            }

            _sequenceToOrderId[sequence] = orderId;
            Log.Trace($"OrderSubscriptionHandler.OnNewOrder(): Received (orderId={orderId}, sequence={sequence})");

            var bbOrder = _orders.GetOrCreateOrder(sequence);
            bbOrder.PopulateFields(message, subType, false);

            var order = _orderProvider.GetOrderById(orderId);
            if (order == null)
            {
                Log.Error($"OrderSubscriptionHandler.OnNewOrder(): OrderId not found: {orderId} (sequence:{sequence}): {message}");
            }
            else
            {
                var status = OrderStatus.Submitted;
                if (message.HasElement(BloombergNames.EMSXStatus))
                {
                    var value = message.GetElementAsString(BloombergNames.EMSXStatus);
                    status = _brokerage.ConvertOrderStatus(value);
                }

                _brokerage.FireOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, $"{nameof(OnNewOrder)}:{sequence}") {Status = status});
            }
        }

        private static bool TryGetOurOrderId(Message message, out int orderId)
        {
            orderId = -1;
            string element;
            if (message.HasElement(BloombergNames.EMSXReferenceOrderIdResponse))
            {
                element = message.GetElementAsString(BloombergNames.EMSXReferenceOrderIdResponse);
            } else if (message.HasElement(BloombergNames.EMSXReferenceRouteId))
            {
                element = message.GetElementAsString(BloombergNames.EMSXReferenceRouteId);
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
                Log.Error("Unable to parse order id as an integer: " + element);
            }

            return false;
        }

        private void OnOrderUpdate(Message message, string subType, int sequence)
        {
            // Ignore orders that were manually created & have been updated.
            if (subType != "R")
            {
                Log.Trace($"OrderSubscriptionHandler.OnNewOrder(): Ignoring message - new orders are handled via the route message stream (sequence: '{sequence}'): {message}");
                return;
            }

            if (!_sequenceToOrderId.TryGetValue(sequence, out var orderId))
            {
                Log.Trace($"OrderSubscriptionHandler.OnOrderUpdate(): Ignoring order update event for manual order (sequence:{sequence})");
                return;
            }

            Log.Trace($"OrderSubscriptionHandler.OnOrderUpdate(): Received (orderId={orderId}, sequence={sequence})");
            if (!TryCreateOrderEvent(message, subType, sequence, orderId, true, out var orderEvent))
            {
                return;
            }

            if (orderEvent.Status == OrderStatus.Filled || orderEvent.Status == OrderStatus.PartiallyFilled)
            {
                var ticket = _orderProvider.GetOrderTicket(orderId);
                if (ticket == null)
                {
                    Log.Error($"OrderSubscriptionHandler.OnOrderUpdate(): OrderTicket not found for OrderId: {orderId}");
                    return;
                }

                orderEvent.FillPrice = Convert.ToDecimal(message.GetElementAsFloat64(BloombergNames.EMSXAvgPrice));

                // The Bloomberg API does not return the individual quantity for each partial fill, but the cumulative filled quantity
                var fillQuantity = Convert.ToDecimal(message.GetElementAsInt64(BloombergNames.EMSXFilled)) - Math.Abs(ticket.QuantityFilled);
                orderEvent.FillQuantity = fillQuantity * Math.Sign(ticket.Quantity);
            }

            if (orderEvent.Status.IsClosed())
            {
                _sequenceToOrderId.TryRemove(sequence, out orderId);
            }

            _brokerage.FireOrderEvent(orderEvent);
        }

        private void OnOrderDelete(Message message, string subType, int sequence)
        {
            // Deletion messages from EMSX do not include our order id.
            if (subType != "O")
            {
                Log.Trace($"OrderSubscriptionHandler.OnOrderDelete(): Ignoring message - deletions are handled via the order message stream (sequence: '{sequence}'): {message}");
                return;
            }
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
            if (!TryCreateOrderEvent(message, subType, sequence, orderId, false, out var orderEvent))
            {
                return;
            }

            // TODO: This is likely to be incorrect.
            orderEvent.Status = OrderStatus.Canceled;

            if (orderEvent.Status.IsClosed())
            {
                _sequenceToOrderId.TryRemove(sequence, out orderId);
            }

            _brokerage.FireOrderEvent(orderEvent);
        }

        private bool TryCreateOrderEvent(Message message, string subType, int sequence, int orderId, bool dynamicFieldsOnly, out OrderEvent orderEvent, [CallerMemberName] string callerMemberName = null)
        {
            orderEvent = null;

            var bbOrder = _orders.GetBySequenceNumber(sequence);
            if (bbOrder == null)
            {
                Log.Error($"OrderSubscriptionHandler.{callerMemberName}(): No existing BB order for sequence '{sequence}' (order:{orderId}): {message}");
                return false;
            }

            bbOrder.PopulateFields(message, subType, dynamicFieldsOnly);
            var order = _orderProvider.GetOrderById(orderId);
            if (order == null)
            {
                Log.Error($"OrderSubscriptionHandler.{callerMemberName}(): No order found for '{orderId}' (sequence:{sequence}): {message}");
                return false;
            }

            orderEvent = new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, $"{callerMemberName}:{sequence}");
            if (message.HasElement(BloombergNames.EMSXStatus))
            {
                orderEvent.Status = _brokerage.ConvertOrderStatus(message.GetElementAsString(BloombergNames.EMSXStatus));
            }

            return true;
        }

        private static EventStatus GetEventStatus(Message message)
        {
            return (EventStatus) message.GetElement(BloombergNames.EventStatus).GetValue();
        }

        public void ProcessMessage(Message message)
        {
            var msgType = message.MessageType;
            var subType = message.HasElement(BloombergNames.MessageSubType) ? message.GetElementAsString(BloombergNames.MessageSubType) : null;
            if (string.IsNullOrWhiteSpace(subType))
            {
                return;
            }

            Log.Trace($"Received [{msgType},{subType}]: {message}");
            if (msgType.Equals(BloombergNames.SubscriptionStarted))
            {
                Log.Trace("Subscription started: " + subType);
            }
            else if (msgType.Equals(BloombergNames.SubscriptionStreamsActivated))
            {
                Log.Trace("Subscription stream activated: " + subType);
            }
            else if (msgType.Equals(BloombergNames.OrderRouteFields))
            {
                OnOrderRouting(message, subType);
            }
            else
            {
                Log.Error($"Unknown message type: {msgType}, message:{message}");
                Debugger.Break();
            }
        }
    }
}