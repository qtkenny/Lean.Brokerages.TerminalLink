using System;
using System.Collections.Generic;
using QuantConnect.Interfaces;

namespace QuantConnect.Bloomberg
{
    public partial class BloombergBrokerage : IDataQueueUniverseProvider
    {
        public IEnumerable<Symbol> LookupSymbols(string lookupName, SecurityType securityType, string securityCurrency = null, string securityExchange = null)
        {
            if (lookupName == null)
            {
                throw new NotImplementedException($"Only {nameof(LookupSymbols)} providing '{nameof(lookupName)}' are supported.");
            }

            return new[] {_symbolMapper.GetLeanSymbol(lookupName)};
        }

        public bool CanAdvanceTime(SecurityType securityType)
        {
            return true;
        }
    }
}