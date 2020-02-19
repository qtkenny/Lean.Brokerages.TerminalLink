using QuantConnect.Brokerages;

namespace QuantConnect.Bloomberg
{
    public interface IBloombergSymbolMapper : ISymbolMapper
    {
        Symbol GetLeanSymbol(string brokerageSymbol);
    }
}