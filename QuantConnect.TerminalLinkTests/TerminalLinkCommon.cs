/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using QuantConnect.TerminalLink;
using QuantConnect.Securities;
using QuantConnect.Configuration;
using QuantConnect.Lean.Engine.DataFeeds;

namespace QuantConnect.TerminalLinkTests
{
    public static class TerminalLinkCommon
    {
        public static TerminalLinkBrokerage CreateBrokerage(IOrderProvider orderProvider = null)
        {
            var apiType = Config.Get("terminal-link-api-type", ApiType.Desktop.ToString()).ConvertTo<ApiType>();
            var environment = Config.Get("terminal-link-environment", Environment.Beta.ToString()).ConvertTo<Environment>();
            var serverHost = Config.Get("terminal-link-server-host", "localhost");
            var serverPort = Config.GetInt("terminal-link-server-port", 8194);

            var symbolMapper = new TerminalLinkSymbolMapper(Config.Get("terminal-link-symbol-map-file", "terminal-link-symbol-map.json"));
            return new TerminalLinkBrokerage(orderProvider, apiType, environment, serverHost, serverPort, symbolMapper, new AggregationManager());
        }
    }
}
