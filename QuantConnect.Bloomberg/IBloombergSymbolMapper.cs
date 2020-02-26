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
    }
}