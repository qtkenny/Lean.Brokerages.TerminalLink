/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Globalization;
using Bloomberglp.Blpapi;

namespace QuantConnect.Bloomberg
{
    public class BloombergOrder
    {
        private readonly BloombergFields _fields;

        public int Sequence { get; }

        public string Status => GetFieldValue(BloombergNames.EMSXStatus);
        public decimal Amount => GetFieldValueDecimal(BloombergNames.EMSXAmount);

        public BloombergOrder(SchemaFieldDefinitions orderFieldDefinitions, int sequence)
        {
            _fields = new BloombergFields(orderFieldDefinitions);
            Sequence = sequence;
        }

        public string GetFieldValue(string name)
        {
            return GetField(name)?.CurrentValue;
        }

        public string GetFieldValue(Name name)
        {
            return GetFieldValue(name.ToString());
        }

        public decimal GetFieldValueDecimal(string name)
        {
            return Convert.ToDecimal(GetFieldValue(name), CultureInfo.InvariantCulture);
        }

        public decimal GetFieldValueDecimal(Name name)
        {
            return GetFieldValueDecimal(name.ToString());
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
