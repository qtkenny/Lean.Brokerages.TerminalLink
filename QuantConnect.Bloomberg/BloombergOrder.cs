/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using Bloomberglp.Blpapi;

namespace QuantConnect.Bloomberg
{
    public class BloombergOrder
    {
        private readonly BloombergFields _orderFields;
        private readonly BloombergFields _routeFields;

        public int Sequence { get; }

        public bool IsLeanOrder =>
            !string.IsNullOrWhiteSpace(GetFieldValue(SubType.Order, BloombergNames.EMSXReferenceOrderIdResponse)) &&
            !string.IsNullOrWhiteSpace(GetFieldValue(SubType.Route, BloombergNames.EMSXReferenceRouteId));

        // Use the route for the status, as the parent can report back being partially filled, but the route is cancelled.
        public string Status => GetFieldValue(SubType.Route, BloombergNames.EMSXStatus);

        public decimal Amount => GetFieldValueDecimal(SubType.Route, BloombergNames.EMSXAmount);

        public decimal Filled => GetFieldValueDecimal(SubType.Route, BloombergNames.EMSXFilled);

        public BloombergOrder(SchemaFieldDefinitions orderFieldDefinitions, SchemaFieldDefinitions routeFieldDefinitions, int sequence)
        {
            _orderFields = new BloombergFields(orderFieldDefinitions);
            _routeFields = new BloombergFields(routeFieldDefinitions);
            Sequence = sequence;
        }

        public string GetFieldValue(SubType subType, string name)
        {
            return GetField(subType, name)?.CurrentValue;
        }

        public string GetFieldValue(SubType subType, Name name)
        {
            return GetFieldValue(subType, name.ToString());
        }

        public decimal GetFieldValueDecimal(SubType subType, string name)
        {
            var value = GetFieldValue(subType, name);
            if (value == null)
            {
                throw new ArgumentException($"Expected numeric field was null (field:{name})");
            }

            if (string.IsNullOrWhiteSpace(value))
            {
                return 0;
            }

            return decimal.TryParse(value, out var result) ? result : throw new ArgumentException($"Unable to parse numeric field '{name}', value:{value}");
        }

        public decimal GetFieldValueDecimal(SubType subType, Name name)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            return GetFieldValueDecimal(subType, name.ToString());
        }

        public BloombergField GetField(SubType subType, string name)
        {
            switch (subType)
            {
                case SubType.Order:
                    return _orderFields.GetField(name);
                case SubType.Route:
                    return _routeFields.GetField(name);
                default: throw new ArgumentOutOfRangeException(nameof(subType), subType, "Unknown subtype: " + subType);
            }
        }

        public void PopulateFields(Message message, string subType, bool dynamicFieldsOnly)
        {
            switch (subType)
            {
                case "O":
                    _orderFields.PopulateFields(message, dynamicFieldsOnly);
                    break;
                case "R":
                    _routeFields.PopulateFields(message, dynamicFieldsOnly);
                    break;
                default: throw new Exception("Unknown sub-type received: " + subType);
            }
        }
    }

    public enum SubType
    {
        Order,
        Route
    }
}
