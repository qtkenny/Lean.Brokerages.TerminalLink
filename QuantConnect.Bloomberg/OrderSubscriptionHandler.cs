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

        private void OnOrderRouting(Message message, string subType)
        {
            var eventStatus = GetEventStatus(message);
            if (eventStatus == EventStatus.Heartbeat)
            {
                return;
            }

            var sequence = message.GetSequence();
            Log.Trace($"OrderSubscriptionHandler.OnOrderRouting(): Message received: '{eventStatus}'(subType:{subType},sequence:{sequence})");
            
            // TODO: Potentially, we should reconcile order quantities with the routes?
            switch (eventStatus)
            {
                case EventStatus.InitialPaint:
                    // Initial order statuses.
                    if (subType == "R")
                    {
                        var order = _orders.GetOrCreateOrder(sequence);
                        order.PopulateFields(message, false);
                    }

                    break;
                case EventStatus.EndPaint:
                    // End of the stream of initial orders.
                    if (subType == "R")
                    {
                        Log.Trace("OrderSubscriptionHandler: End of Initial Paint");
                        _brokerage.SetBlotterInitialized();
                    }

                    break;
                case EventStatus.New:
                    if (subType == "R")
                    {
                        OnNewOrder(message, sequence);
                    }

                    break;
                case EventStatus.Update:
                    if (subType == "R")
                    {
                        OnOrderUpdate(message, sequence);
                    }

                    break;
                case EventStatus.Delete:
                    if (subType == "O")
                    {
                        OnOrderDelete(message, sequence);
                    }

                    break;
                case EventStatus.Heartbeat:
                    // No need to log the heartbeat.
                    break;
                default: throw new Exception($"Unknown order fields update: {eventStatus}: {message}");
            }
        }

        private void OnNewOrder(Message message, int sequence)
        {
            // If an order is created manually in the terminal, we'll still receive an event.
            if (!TryGetOurOrderId(message, out var orderId))
            {
                Log.Trace($"Ignoring new order event for a manual trade (sequence:{sequence})");
                return;
            }

            _sequenceToOrderId[sequence] = orderId;
            _orderToSequenceId[orderId] = sequence;
            Log.Trace($"OrderSubscriptionHandler.OnNewOrder(): Received (orderId={orderId}, sequence={sequence})");

            var bbOrder = _orders.GetOrCreateOrder(sequence);
            bbOrder.PopulateFields(message, false);

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

        private void OnOrderUpdate(Message message, int sequence)
        {
            // Ignore orders that were manually created & have been updated.
            if (!_sequenceToOrderId.TryGetValue(sequence, out var orderId))
            {
                Log.Trace($"OrderSubscriptionHandler.OnOrderUpdate(): Ignoring order update event for manual order (sequence:{sequence})");
                return;
            }

            Log.Trace($"OrderSubscriptionHandler.OnOrderUpdate(): Received (orderId={orderId}, sequence={sequence})");
            if (!TryCreateOrderEvent(message, sequence, orderId, true, out var orderEvent))
            {
                return;
            }

            // TODO: Lean is expecting the fill quantity to be the amount filled in this event. BBG provide the total filled.
            if (orderEvent.Status == OrderStatus.Filled || orderEvent.Status == OrderStatus.PartiallyFilled)
            {
                orderEvent.FillPrice = Convert.ToDecimal(message.GetElementAsFloat64(BloombergNames.EMSXAvgPrice));
                orderEvent.FillQuantity = Convert.ToDecimal(message.GetElementAsInt64(BloombergNames.EMSXFilled));
            }

            _brokerage.FireOrderEvent(orderEvent);
        }

        private void OnOrderDelete(Message message, int sequence)
        {
            // Deletion messages from EMSX do not include our order id.
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
            if (!TryCreateOrderEvent(message, sequence, orderId, false, out var orderEvent))
            {
                return;
            }

            // TODO: This is likely to be incorrect.
            orderEvent.Status = OrderStatus.Canceled;
            _brokerage.FireOrderEvent(orderEvent);
        }

        private bool TryCreateOrderEvent(Message message, int sequence, int orderId, bool dynamicFieldsOnly, out OrderEvent orderEvent, [CallerMemberName] string callerMemberName = null)
        {
            orderEvent = null;

            var bbOrder = _orders.GetBySequenceNumber(sequence);
            if (bbOrder == null)
            {
                Log.Error($"OrderSubscriptionHandler.{callerMemberName}(): No existing BB order for sequence '{sequence}' (order:{orderId}): {message}");
                return false;
            }

            bbOrder.PopulateFields(message, dynamicFieldsOnly);
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