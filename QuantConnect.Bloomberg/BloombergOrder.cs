/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Bloomberglp.Blpapi;
using QuantConnect.Logging;
using QuantConnect.Orders;

namespace QuantConnect.Bloomberg
{
    public class BloombergOrder
    {
        private readonly Dictionary<Name, Element> _orders = new Dictionary<Name, Element>();
        private readonly Dictionary<Name, Element> _routes = new Dictionary<Name, Element>();

        public BloombergOrder(int sequence)
        {
            Sequence = sequence;
        }

        public int Sequence { get; }

        public bool IsLeanOrder =>
            HasValue(SubType.Order, BloombergNames.EMSXReferenceOrderIdResponse) && HasValue(SubType.Route, BloombergNames.EMSXReferenceRouteId);

        // Use the route for the status, as the parent can report back being partially filled, but the route is cancelled.
        public OrderStatus Status => ConvertOrderStatus(GetString(SubType.Route, BloombergNames.EMSXStatus));

        public int Amount => GetInt(SubType.Route, BloombergNames.EMSXAmount, false);

        public int Filled => GetInt(SubType.Route, BloombergNames.EMSXFilled, false);

        public string GetString(SubType subType, Name name)
        {
            var element = GetElement(subType, name);
            if (element == null)
            {
                return null;
            }

            VerifyType(element, Schema.Datatype.STRING);

            return element.GetValueAsString();
        }

        public DateTime GetDate(SubType subType, Name name, bool allowDefault)
        {
            var element = GetElement(subType, name);
            if (element == null)
            {
                return GetDefault<DateTime>(subType, name, allowDefault);
            }

            var actual = VerifyType(element, Schema.Datatype.INT32, Schema.Datatype.DATE, Schema.Datatype.DATETIME);
            try
            {
                switch (actual)
                {
                    case Schema.Datatype.INT32:
                        var value = element.GetValueAsInt32();
                        if (value == 0)
                        {
                            return DateTime.MinValue;
                        }

                        return DateTime.TryParseExact(value.ToString(), "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var result)
                            ? result
                            : throw new Exception($"Unable to parse int value '{value}' to date");
                    case Schema.Datatype.DATE: return element.GetValueAsDate().ToSystemDateTime();
                    case Schema.Datatype.DATETIME: return element.GetValueAsDatetime().ToSystemDateTime().Date;
                    default: throw new ArgumentOutOfRangeException(nameof(actual), actual, "Data type passed type verification, but hasn't been configured: " + actual);
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException($"Unable to obtain date from '{element.Name}': {element}", e);
            }
        }

        private TimeSpan GetTime(SubType subType, Name name, bool allowDefault)
        {
            var element = GetElement(subType, name);
            if (element == null)
            {
                return GetDefault<TimeSpan>(subType, name, allowDefault);
            }

            var actual = VerifyType(element, Schema.Datatype.FLOAT64, Schema.Datatype.TIME, Schema.Datatype.DATETIME);
            try
            {
                switch (actual)
                {
                    case Schema.Datatype.FLOAT64:
                        var value = element.GetValueAsFloat64();
                        return TimeSpan.FromSeconds(value);
                    case Schema.Datatype.TIME: return element.GetValueAsTime().ToSystemDateTime().TimeOfDay;
                    case Schema.Datatype.DATETIME: return element.GetValueAsDatetime().ToSystemDateTime().TimeOfDay;
                    default: throw new ArgumentOutOfRangeException(nameof(actual), actual, "Data type passed type verification, but hasn't been configured: " + actual);
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException($"Unable to obtain date from '{element.Name}': {element}", e);
            }
        }

        public DateTime GetDateTimeCombo(SubType subType, Name dateName, Name timeName, bool allowDefault)
        {
            var dateElement = GetDate(subType, dateName, allowDefault);
            var timeElement = GetTime(subType, timeName, allowDefault);
            return dateElement.Add(timeElement);
        }

        private static Schema.Datatype VerifyType(Element element, params Schema.Datatype[] dataTypes)
        {
            if (element == null)
            {
                // Element should have been verified prior to entering this method.
                throw new ArgumentNullException(nameof(element));
            }

            if (dataTypes == null || dataTypes.Length == 0)
            {
                throw new ArgumentException("Parameter was empty", nameof(dataTypes));
            }

            var actual = element.Datatype;
            if (dataTypes.All(validType => actual != validType))
            {
                throw new ArgumentException($"Element '{element.Name}' is not the expected data type.  Expected:{string.Join(",", dataTypes)}, Actual:{actual}");
            }

            return actual;
        }

        private bool HasValue(SubType subType, Name name)
        {
            return !string.IsNullOrWhiteSpace(GetElement(subType, name)?.GetValueAsString());
        }

        public decimal GetDecimal(SubType subType, Name name, bool allowDefault)
        {
            var element = GetElement(subType, name);
            if (element == null)
            {
                return GetDefault<decimal>(subType, name, allowDefault);
            }

            VerifyType(element, Schema.Datatype.FLOAT64);
            try
            {
                var result = element.GetValueAsFloat64();
                return new decimal(result);
            }
            catch (Exception e)
            {
                throw new ArgumentException($"Unable to obtain decimal from '{element.Name}': {element}", e);
            }
        }

        private int GetInt(SubType subType, Name name, bool allowDefault)
        {
            var element = GetElement(subType, name);
            if (element == null)
            {
                return GetDefault<int>(subType, name, allowDefault);
            }

            VerifyType(element, Schema.Datatype.INT32);
            try
            {
                return element.GetValueAsInt32();
            }
            catch (Exception e)
            {
                throw new ArgumentException($"Unable to obtain decimal from '{element.Name}': {element}", e);
            }
        }

        private Element GetElement(SubType subType, Name name)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            var source = GetCollection(subType);
            return source.TryGetValue(name, out var element) ? element : null;
        }

        public void PopulateFields(Message message, SubType subType)
        {
            var target = GetCollection(subType);
            foreach (var element in message.Elements)
            {
                target[element.Name] = element;
            }
        }

        private Dictionary<Name, Element> GetCollection(SubType subType)
        {
            switch (subType)
            {
                case SubType.Order:
                    return _orders;
                case SubType.Route:
                    return _routes;
                default: throw new Exception("Unknown sub-type received: " + subType);
            }
        }

        private T GetDefault<T>(SubType subType, Name name, bool allowDefault)
        {
            if (allowDefault)
            {
                return default;
            }

            if (Log.DebuggingEnabled)
            {
                // we are going to throw an unexpected exception, let's log all the information we have
                var order = GetCollection(SubType.Order).Select(x => $"{x.Key}={x.Value}").ToArray();
                var route = GetCollection(SubType.Route).Select(x => $"{x.Key}={x.Value}").ToArray();

                Log.Debug($"BloombergOrder.GetDefault(): Route [{string.Join(",", route)}] -------- Order [{string.Join(",", order)}]");
            }

            throw new ArgumentException($"Required {subType} element '{name}' was not found.");
        }

        private static OrderStatus ConvertOrderStatus(string orderStatus)
        {
            switch (orderStatus)
            {
                case "CXL-PEND":
                case "CXL-REQ":
                    return OrderStatus.CancelPending;

                case "ASSIGN":
                case "CANCEL":
                case "EXPIRED":
                    return OrderStatus.Canceled;

                case "SENT":
                case "WORKING":
                    return OrderStatus.Submitted;

                case "COMPLETED":
                case "PARTFILLED":  // PARTFILLED means the order was cancelled before it was completely filled.
                case "FILLED":
                    return OrderStatus.Filled;

                case "PARTFILL":
                    return OrderStatus.PartiallyFilled;

                case "CXLREJ":
                    return OrderStatus.Invalid;

                case "NEW":
                case "ORD-PEND":
                    return OrderStatus.New;

                default:
                    return OrderStatus.None;
            }
        }
    }

    public enum SubType
    {
        Order,
        Route
    }
}