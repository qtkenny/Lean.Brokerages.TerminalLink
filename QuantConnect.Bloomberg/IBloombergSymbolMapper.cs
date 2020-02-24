using System.Collections.Generic;
using QuantConnect.Brokerages;

namespace QuantConnect.Bloomberg
{
    public interface IBloombergSymbolMapper : ISymbolMapper
    {
        Symbol GetLeanSymbol(string brokerageSymbol);

        IReadOnlyDictionary<Symbol, string> ManuallyMappedSymbols { get; }
    }
}