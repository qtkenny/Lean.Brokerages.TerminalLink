/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace QuantConnect.Bloomberg
{
    public class BloombergOrders : IEnumerable<BloombergOrder>
    {
        private readonly Dictionary<int, BloombergOrder> _orders = new Dictionary<int, BloombergOrder>();
        private readonly SchemaFieldDefinitions _orderFieldDefinitions;
        private readonly object _lock = new object();

        public BloombergOrders(SchemaFieldDefinitions orderFieldDefinitions)
        {
            _orderFieldDefinitions = orderFieldDefinitions;
        }

        public BloombergOrder CreateOrder(int sequence)
        {
            var order = new BloombergOrder(_orderFieldDefinitions, sequence);
            lock (_lock)
            {
                if (_orders.TryGetValue(sequence, out var existingOrder))
                {
                    throw new Exception($"An order already existed for sequence id '{sequence}': {existingOrder.Describe()}");
                }

                _orders.Add(sequence, order);
            }

            return order;
        }

        public BloombergOrder GetBySequenceNumber(int sequence)
        {
            lock (_lock)
            {
                return _orders.TryGetValue(sequence, out var order) ? order : null;
            }
        }

        public IEnumerator<BloombergOrder> GetEnumerator()
        {
            lock (_lock)
            {
                return _orders.Values.ToList().GetEnumerator();
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
