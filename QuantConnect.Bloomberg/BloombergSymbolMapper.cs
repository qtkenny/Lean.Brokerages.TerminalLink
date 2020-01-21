/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using QuantConnect.Brokerages;
using static QuantConnect.StringExtensions;

namespace QuantConnect.Bloomberg
{
    /// <summary>
    /// Provides the mapping between Lean symbols and Bloomberg symbols.
    /// </summary>
    public class BloombergSymbolMapper : ISymbolMapper
    {
        /// <summary>
        /// Converts a Lean symbol instance to a Bloomberg symbol
        /// </summary>
        /// <param name="symbol">A Lean symbol instance</param>
        /// <returns>The Bloomberg symbol</returns>
        public string GetBrokerageSymbol(Symbol symbol)
        {
            return GetBloombergTopicName(symbol);
        }

        /// <summary>
        /// Converts a Bloomberg symbol to a Lean symbol instance
        /// </summary>
        /// <param name="brokerageSymbol">The Bloomberg symbol</param>
        /// <param name="securityType">The security type</param>
        /// <param name="market">The market</param>
        /// <param name="expirationDate">Expiration date of the security (if applicable)</param>
        /// <param name="strike">The strike of the security (if applicable)</param>
        /// <param name="optionRight">The option right of the security (if applicable)</param>
        /// <returns>A new Lean Symbol instance</returns>
        public Symbol GetLeanSymbol(
            string brokerageSymbol,
            SecurityType securityType,
            string market,
            DateTime expirationDate = new DateTime(),
            decimal strike = 0,
            OptionRight optionRight = OptionRight.Call)
        {
            return Symbol.Empty;
        }

        private string GetBloombergTopicName(Symbol symbol)
        {
            var topicName = GetBloombergSymbol(symbol);

            var bloombergMarket = GetBloombergMarket(symbol.ID.Market, symbol.SecurityType);
            if (bloombergMarket.Length > 0)
            {
                topicName += $" {bloombergMarket}";
            }

            topicName += $" {GetBloombergSecurityType(symbol.SecurityType)}";

            return topicName;
        }

        private string GetBloombergSymbol(Symbol symbol)
        {
            if (symbol.SecurityType == SecurityType.Forex)
            {
                // TODO: documentation does not mention non-USD fx pairs, needs to be tested

                if (symbol.Value.EndsWith("USD"))
                {
                    return symbol.Value.Substring(0, 3);
                }
                if (symbol.Value.StartsWith("USD"))
                {
                    return symbol.Value.Substring(3);
                }

                throw new NotSupportedException($"Unsupported Forex symbol: {symbol.Value}");
            }

            if (symbol.SecurityType == SecurityType.Option)
            {
                // Equity options: Root Ticker x Exchange Code x Expiry MM/DD/YY (or Expiry M/Y only) x C or P x Strike Price
                return Invariant($"{symbol.Underlying.Value} UO {symbol.ID.Date:MM/dd/yy} {(symbol.ID.OptionRight == OptionRight.Call ? "C" : "P")} {symbol.ID.StrikePrice:F2}");
            }

            return symbol.Value;
        }

        private string GetBloombergMarket(string market, SecurityType securityType)
        {
            if (securityType == SecurityType.Forex)
            {
                return "BVAL";
            }

            if (securityType == SecurityType.Future)
            {
                return "COMB";
            }

            if (securityType == SecurityType.Option)
            {
                // exchange/market is already included in the ticker
                return "";
            }

            switch (market)
            {
                case Market.USA:
                    return "US";

                default:
                    throw new NotSupportedException($"Unsupported market: {market} for security type: {securityType}");
            }
        }

        private string GetBloombergSecurityType(SecurityType securityType)
        {
            switch (securityType)
            {
                case SecurityType.Equity:
                    return "Equity";

                case SecurityType.Forex:
                    return "Curncy";

                case SecurityType.Future:
                    // TODO: depends on the underlying which we don't have for now
                    // possible values: Comdty, Index, Curncy, Equity
                    return "Comdty";

                case SecurityType.Option:
                    // only equity options for now
                    return "Equity";

                default:
                    throw new NotSupportedException($"Unsupported security type: {securityType}");
            }
        }
    }
}
