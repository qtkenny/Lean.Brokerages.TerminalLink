/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using Bloomberglp.Blpapi;
using NodaTime;

namespace QuantConnect.Bloomberg
{
    public class BloombergSubscriptionData
    {
        public CorrelationID CorrelationId { get; }

        public Symbol Symbol { get; }

        public DateTimeZone ExchangeTimeZone { get; }

        public decimal BidPrice { get; set; }
        public decimal BidSize { get; set; }
        public decimal AskPrice { get; set; }
        public decimal AskSize { get; set; }

        public BloombergSubscriptionData(CorrelationID correlationId, Symbol symbolName, DateTimeZone exchangeTimeZone)
        {
            CorrelationId = correlationId ?? throw new ArgumentNullException(nameof(correlationId));
            Symbol = symbolName ?? throw new ArgumentNullException(nameof(symbolName));
            ExchangeTimeZone = exchangeTimeZone ?? throw new ArgumentNullException(nameof(exchangeTimeZone));
        }

        public bool IsQuoteValid()
        {
            if (Symbol.SecurityType == SecurityType.Forex)
            {
                return BidPrice > 0 && AskPrice > 0;
            }

            // TODO: This isn't necessary true for Futures, particularly spreads which can have a price of 0.
            return BidPrice > 0 && BidSize > 0 && AskPrice > 0 && AskSize > 0;
        }
    }
}