/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using QuantConnect.Bloomberg;
using QuantConnect.Securities;
using QuantConnect.Configuration;
using QuantConnect.Lean.Engine.DataFeeds;

namespace QuantConnect.BloombergTests
{
    public static class BloombergCommon
    {
        public static BloombergBrokerage CreateBrokerage(IOrderProvider orderProvider = null)
        {
            var apiType = Config.Get("bloomberg-api-type", ApiType.Desktop.ToString()).ConvertTo<ApiType>();
            var environment = Config.Get("bloomberg-environment", Environment.Beta.ToString()).ConvertTo<Environment>();
            var serverHost = Config.Get("bloomberg-server-host", "localhost");
            var serverPort = Config.GetInt("bloomberg-server-port", 8194);

            var symbolMapper = new BloombergSymbolMapper(Config.Get("bloomberg-symbol-map-file", "bloomberg-symbol-map.json"));
            return new BloombergBrokerage(orderProvider, apiType, environment, serverHost, serverPort, symbolMapper, new AggregationManager());
        }
    }
}
