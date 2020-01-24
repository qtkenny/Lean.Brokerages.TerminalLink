/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

namespace QuantConnect.Bloomberg
{
    public class BloombergField
    {
        public string Name { get; }
        public string CurrentValue { get; private set; }

        public BloombergField()
        {
        }

        public BloombergField(string name, string value)
        {
            Name = name;
            CurrentValue = value;
        }

        public void SetCurrentValue(string value)
        {
            CurrentValue = value;
        }
    }
}
