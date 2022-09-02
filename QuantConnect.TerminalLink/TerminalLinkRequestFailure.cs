/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using Bloomberglp.Blpapi;

namespace QuantConnect.TerminalLink
{
    public class TerminalLinkRequestFailure
    {
        public string Source { get; }
        public int ErrorCode { get; }
        public string Category { get; }
        public string SubCategory { get; }
        public string Description { get; }

        public TerminalLinkRequestFailure(Message msg)
        {
            if (msg.HasElement(TerminalLinkNames.RequestFailure))
            {
                var failure = msg.AsElement[TerminalLinkNames.RequestFailure];

                if (failure.HasElement(TerminalLinkNames.Reason))
                {
                    var reason = failure.GetElement(TerminalLinkNames.Reason);

                    if (reason.HasElement(TerminalLinkNames.Source))
                    {
                        Source = reason.GetElementAsString(TerminalLinkNames.Source);
                    }

                    if (reason.HasElement(TerminalLinkNames.ErrorCodeFailure))
                    {
                        ErrorCode = reason.GetElementAsInt32(TerminalLinkNames.ErrorCodeFailure);
                    }

                    if (reason.HasElement(TerminalLinkNames.Category))
                    {
                        Category = reason.GetElementAsString(TerminalLinkNames.Category);
                    }

                    if (reason.HasElement(TerminalLinkNames.SubCategory))
                    {
                        SubCategory = reason.GetElementAsString(TerminalLinkNames.SubCategory);
                    }

                    if (reason.HasElement(TerminalLinkNames.Description))
                    {
                        Description = reason.GetElementAsString(TerminalLinkNames.Description);
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
