/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using Bloomberglp.Blpapi;

namespace QuantConnect.Bloomberg
{
    public class BloombergOrder
    {
        private readonly BloombergFields _fields;

        public int Sequence { get; }

        public BloombergOrder(BloombergBrokerage brokerage, int sequence)
        {
            Sequence = sequence;
            _fields = new BloombergFields(brokerage.OrderFieldDefinitions);
        }

        public string GetFieldValue(string name)
        {
            return GetField(name)?.CurrentValue;
        }

        public BloombergField GetField(string name)
        {
            return _fields.GetField(name);
        }

        public void PopulateFields(Message message, bool dynamicFieldsOnly)
        {
            _fields.PopulateFields(message, dynamicFieldsOnly);
        }
    }
}
