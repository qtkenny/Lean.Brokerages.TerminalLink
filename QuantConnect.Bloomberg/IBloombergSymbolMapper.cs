/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using QuantConnect.Brokerages;

namespace QuantConnect.Bloomberg
{
    public interface IBloombergSymbolMapper : ISymbolMapper
    {
        Symbol GetLeanSymbol(string brokerageSymbol, SecurityType? securityType = null);

        /// <summary>
        ///     Retrieves the members of a futures / options chain than has been manually specified.
        /// </summary>
        /// <param name="symbol">Lean <see cref="Symbol" /> to lookup.</param>
        /// <returns>Chain members, or null.</returns>
        string[] GetManualChain(Symbol symbol);

        /// <summary>
        ///     Provides the market, given a canonical symbol value.
        /// </summary>
        /// <param name="canonicalSymbol">Value to look-up</param>
        /// <returns>Market</returns>
        string GetMarket(string canonicalSymbol);
    }
}