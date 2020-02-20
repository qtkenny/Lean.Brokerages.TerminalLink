/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Diagnostics;

namespace QuantConnect.Bloomberg
{
    [DebuggerDisplay("{Name}={CurrentValue}")]
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
            if (Equals(CurrentValue, value))
            {
                return;
            }

            CurrentValue = value;
            Updated?.Invoke(this, EventArgs.Empty);
        }

        public override string ToString()
        {
            return $"{Name}={CurrentValue}";
        }

        public event EventHandler<EventArgs> Updated;
    }
}
