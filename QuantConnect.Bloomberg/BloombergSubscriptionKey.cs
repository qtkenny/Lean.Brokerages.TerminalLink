using System;
using Bloomberglp.Blpapi;

namespace QuantConnect.Bloomberg
{
    public class BloombergSubscriptionKey
    {
        public CorrelationID CorrelationId { get; }

        public Symbol Symbol { get; }

        public TickType TickType { get; }

        public BloombergSubscriptionKey(CorrelationID correlationId, Symbol symbolName, TickType tickType)
        {
            CorrelationId = correlationId ?? throw new ArgumentNullException(nameof(correlationId));
            Symbol = symbolName ?? throw new ArgumentNullException(nameof(symbolName));
            TickType = tickType;
        }
    }
}