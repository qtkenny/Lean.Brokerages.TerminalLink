/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections;
using System.Collections.Generic;
using Bloomberglp.Blpapi;

namespace QuantConnect.Bloomberg
{
    /// <summary>
    /// Represents a group of subscriptions for the same symbol
    /// </summary>
    public class BloombergSubscriptions : IEnumerable<Subscription>
    {
        private readonly Dictionary<TickType, Subscription> _subscriptionsByTickType = new Dictionary<TickType, Subscription>();
        private readonly Dictionary<CorrelationID, TickType> _tickTypesByCorrelationId = new Dictionary<CorrelationID, TickType>();

        /// <summary>
        /// The symbol
        /// </summary>
        public Symbol Symbol { get; }

        /// <summary>
        /// Creates a new instance of the <see cref="BloombergSubscriptions"/> class
        /// </summary>
        /// <param name="symbol">The symbol</param>
        public BloombergSubscriptions(Symbol symbol)
        {
            Symbol = symbol;
        }

        /// <summary>
        /// Adds a new subscription
        /// </summary>
        public void Add(TickType tickType, Subscription subscription, CorrelationID correlationId)
        {
            _subscriptionsByTickType.Add(tickType, subscription);
            _tickTypesByCorrelationId.Add(correlationId, tickType);
        }

        /// <summary>
        /// Removes all the subscriptions
        /// </summary>
        public void Clear()
        {
            _subscriptionsByTickType.Clear();
            _tickTypesByCorrelationId.Clear();
        }

        /// <summary>
        /// Gets the tick type for the given correlation id
        /// </summary>
        public TickType GetTickType(CorrelationID correlationId)
        {
            TickType tickType;
            if (!_tickTypesByCorrelationId.TryGetValue(correlationId, out tickType))
            {
                throw new Exception($"CorrelationID not found: {correlationId}");
            }

            return tickType;
        }

        public IEnumerator<Subscription> GetEnumerator()
        {
            return _subscriptionsByTickType.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
