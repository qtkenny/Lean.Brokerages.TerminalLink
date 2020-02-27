/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Bloomberglp.Blpapi;
using QuantConnect.Brokerages;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;

namespace QuantConnect.Bloomberg
{
    public class OrderSubscriptionHandler : IMessageHandler
    {
        private const int NoSequence = -1;
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

        private void LogRequestCompletion(Message message)
        {
            var sequence = GetSequence(message);
            var builder = new StringBuilder("Request completed: '").Append(message.MessageType).Append('\'');
            if (sequence != -NoSequence)
            {
                builder.Append(" (sequence:").Append(sequence).Append(")");
            }

            var description = message.HasElement(BloombergNames.Message) ? message.GetElementAsString(BloombergNames.Message) : message.ToString();
            if (!string.IsNullOrWhiteSpace(description))
            {
                builder.Append(": ").Append(description);
            }

            var output = builder.ToString();
            Log.Trace("OrderSubscriptionHandler.LogRequestCompletion(): " + output);
            _brokerage.FireBrokerMessage(new BrokerageMessageEvent(BrokerageMessageType.Information, 1, output));
        }

        private static int GetSequence(Message message)
        {
            return message.HasElement(BloombergNames.EMSXSequence) ? message.GetElementAsInt32(BloombergNames.EMSXSequence) : NoSequence;
        }

        private void OnOrderRouting(Message message)
        {
            var eventStatus = GetEventStatus(message);
            if (eventStatus == EventStatus.Heartbeat)
            {
                return;
            }

            var sequence = GetSequence(message);
            Log.Trace($"OrderSubscriptionHandler.OnOrderRouting(): Message received: '{eventStatus}' [sequence:{sequence}]");
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
                case EventStatus.Heartbeat:
                    // No need to log the heartbeat.
                    break;
                default: throw new Exception($"Unknown order fields update: {eventStatus} [message:{message}]");
            }
        }

        private void OnNewOrder(Message message, int sequence)
        {
            var orderId = GetOurOrderId(message);
            _sequenceToOrderId[sequence] = orderId;
            _orderToSequenceId[orderId] = sequence;
            Log.Trace($"OrderSubscriptionHandler.OnNewOrder(): Received (orderId={orderId}, sequence={sequence})");

            var bbOrder = _orders.GetBySequenceNumber(sequence) ?? _orders.CreateOrder(sequence);
            bbOrder.PopulateFields(message, false);

            var order = _orderProvider.GetOrderById(orderId);
            if (order == null)
            {
                Log.Error($"OrderSubscriptionHandler.OnNewOrder(): OrderId not found: {orderId} (sequence:{sequence}): {message}");
            }
            else
            {
                // TODO: This is not persisted at the moment.
                order.BrokerId.Add(sequence.ToString());
                var status = OrderStatus.Submitted;
                if (message.HasElement(BloombergNames.EMSXStatus))
                {
                    var value = message.GetElementAsString(BloombergNames.EMSXStatus);
                    status = _brokerage.ConvertOrderStatus(value);
                }

                _brokerage.FireOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero, $"{nameof(OnNewOrder)}:{sequence}") {Status = status});
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

            // Order should already exist. If it doesn't create it anyway.
            var bbOrder = _orders.GetBySequenceNumber(sequence);
            if (bbOrder == null)
            {
                // TODO: Do we need to create an order for an unknown order?
                Log.Error($"OrderSubscriptionHandler.{callerMemberName}(): No existing BB order for sequence '{sequence}' (order:{orderId}): {message}");
                bbOrder = _orders.CreateOrder(sequence);
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
            if (msgType.Equals(BloombergNames.SubscriptionStarted))
            {
                Log.Trace("Order subscription started");
            }
            else if (msgType.Equals(BloombergNames.SubscriptionStreamsActivated))
            {
                Log.Trace("Order subscription streams activated");
            }
            else if (msgType.Equals(BloombergNames.CreateOrderAndRouteEx) || msgType.Equals(BloombergNames.ModifyOrderEx) || msgType.Equals(BloombergNames.CancelOrderEx))
            {
                LogRequestCompletion(message);
            }
            else if (msgType.Equals(BloombergNames.OrderRouteFields))
            {
                OnOrderRouting(message);
            }
            else if (msgType.Equals(BloombergNames.ErrorInfo))
            {
                // Log the error first, then fire a broker event - in case we can't parse the BBG response.
                var code = message.GetElementAsInt32(BloombergNames.ErrorCode);
                var text = message.GetElementAsString(BloombergNames.ErrorMessage);
                _brokerage.FireBrokerMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, code, text));
            }
            else
            {
                Log.Error($"Unknown message type: {msgType}, message:{message}");
                Debugger.Break();
            }
        }
    }
}