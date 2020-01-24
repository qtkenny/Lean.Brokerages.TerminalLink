/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace QuantConnect.Bloomberg
{
    public class BloombergOrders : IEnumerable<BloombergOrder>
    {
        private readonly Dictionary<int, BloombergOrder> _orders = new Dictionary<int, BloombergOrder>();
        private readonly BloombergBrokerage _brokerage;
        private readonly ManualResetEvent _blotterInitializedEvent = new ManualResetEvent(false);

        public IMessageHandler OrderSubscriptionHandler { get; }

        public BloombergOrders(BloombergBrokerage brokerage)
        {
            _brokerage = brokerage;
            OrderSubscriptionHandler = new OrderSubscriptionHandler(this);
        }

        public void SubscribeOrderEvents()
        {
            var fields = _brokerage.OrderFieldDefinitions.Select(x => x.Name);

            var serviceName = _brokerage.GetServiceName(ServiceType.Ems);
            var topic = $"{serviceName}/order?fields={string.Join(",", fields)}";

            _brokerage.Subscribe(topic, OrderSubscriptionHandler);

            _blotterInitializedEvent.WaitOne();
        }

        public BloombergOrder CreateOrder(int sequence)
        {
            var order = new BloombergOrder(_brokerage, sequence);

            _orders.Add(sequence, order);

            return order;
        }

        public BloombergOrder GetBySequenceNumber(int sequence)
        {
            BloombergOrder order;
            return _orders.TryGetValue(sequence, out order) ? order : null;
        }

        public void SetBlotterInitialized()
        {
            _blotterInitializedEvent.Set();
        }

        public IEnumerator<BloombergOrder> GetEnumerator()
        {
            return _orders.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
