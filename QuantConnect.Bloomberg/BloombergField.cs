/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System.Diagnostics;

namespace QuantConnect.Bloomberg
{
    [DebuggerDisplay("{Name}={CurrentValue}")]
    public class BloombergField
    {
        public string Name { get; }
        public string CurrentValue { get; private set; }

        public BloombergField(string name, string value)
        {
            Name = name;
            CurrentValue = value;
        }

        public void SetCurrentValue(string value)
        {
            CurrentValue = value;
        }

        public override string ToString()
        {
            return $"{Name}={CurrentValue}";
        }
    }
}
