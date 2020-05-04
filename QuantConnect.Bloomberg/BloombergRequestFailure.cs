/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using Bloomberglp.Blpapi;

namespace QuantConnect.Bloomberg
{
    public class BloombergRequestFailure
    {
        public string Source { get; }
        public int ErrorCode { get; }
        public string Category { get; }
        public string SubCategory { get; }
        public string Description { get; }

        public BloombergRequestFailure(Message msg)
        {
            if (msg.HasElement(BloombergNames.RequestFailure))
            {
                var failure = msg.AsElement[BloombergNames.RequestFailure];

                if (failure.HasElement(BloombergNames.Reason))
                {
                    var reason = failure.GetElement(BloombergNames.Reason);

                    if (reason.HasElement(BloombergNames.Source))
                    {
                        Source = reason.GetElementAsString(BloombergNames.Source);
                    }

                    if (reason.HasElement(BloombergNames.ErrorCodeFailure))
                    {
                        ErrorCode = reason.GetElementAsInt32(BloombergNames.ErrorCodeFailure);
                    }

                    if (reason.HasElement(BloombergNames.Category))
                    {
                        Category = reason.GetElementAsString(BloombergNames.Category);
                    }

                    if (reason.HasElement(BloombergNames.SubCategory))
                    {
                        SubCategory = reason.GetElementAsString(BloombergNames.SubCategory);
                    }

                    if (reason.HasElement(BloombergNames.Description))
                    {
                        Description = reason.GetElementAsString(BloombergNames.Description);
                    }
                }
            }
        }

        public override string ToString()
        {
            return $"Source:{Source}, Category:{Category}, SubCategory:{SubCategory}, Description:{Description}";
        }
    }
}
