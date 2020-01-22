/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

namespace QuantConnect.Bloomberg
{
    public class BloombergOrder
    {
        public int Sequence { get; }

        public BloombergOrder(int sequence)
        {
            Sequence = sequence;
        }
    }
}
