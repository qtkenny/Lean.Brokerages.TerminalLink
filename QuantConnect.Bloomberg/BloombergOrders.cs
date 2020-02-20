/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections;
using System.Collections.Generic;

namespace QuantConnect.Bloomberg
{
    public class BloombergOrders : IEnumerable<BloombergOrder>
    {
        private readonly Dictionary<int, BloombergOrder> _orders = new Dictionary<int, BloombergOrder>();
        private readonly SchemaFieldDefinitions _orderFieldDefinitions;

        public BloombergOrders(SchemaFieldDefinitions orderFieldDefinitions)
        {
            _orderFieldDefinitions = orderFieldDefinitions;
        }

        public BloombergOrder CreateOrder(int sequence)
        {
            var order = new BloombergOrder(_orderFieldDefinitions, sequence);

            _orders.Add(sequence, order);
            OrderCreated?.Invoke(this, order);
            return order;
        }

        public event EventHandler<BloombergOrder> OrderCreated;

        public BloombergOrder GetBySequenceNumber(int sequence)
        {
            BloombergOrder order;
            return _orders.TryGetValue(sequence, out order) ? order : null;
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
