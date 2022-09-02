/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace QuantConnect.TerminalLink
{
    public class TerminalLinkOrders : IEnumerable<TerminalLinkOrder>
    {
        private readonly Dictionary<int, TerminalLinkOrder> _orders = new Dictionary<int, TerminalLinkOrder>();
        private readonly object _lock = new object();

        public TerminalLinkOrder GetOrCreateOrder(int sequence)
        {
            var order = new TerminalLinkOrder(sequence);
            lock (_lock)
            {
                if (_orders.TryGetValue(sequence, out var existingOrder))
                {
                    return existingOrder;
                }

                _orders.Add(sequence, order);
            }

            return order;
        }

        public TerminalLinkOrder GetBySequenceNumber(int sequence)
        {
            lock (_lock)
            {
                return _orders.TryGetValue(sequence, out var order) ? order : null;
            }
        }

        public IEnumerator<TerminalLinkOrder> GetEnumerator()
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
