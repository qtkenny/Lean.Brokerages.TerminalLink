/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using Moq;
using QuantConnect.Bloomberg;
using QuantConnect.Configuration;
using QuantConnect.Securities;

namespace QuantConnect.BloombergTests
{
    public static class BloombergCommon
    {
        public static BloombergBrokerage CreateBrokerage(IOrderProvider orderProvider = null, ISecurityProvider securityProvider = null)
        {
            var apiType = Config.Get("bloomberg-api-type", ApiType.Desktop.ToString()).ConvertTo<ApiType>();
            var environment = Config.Get("bloomberg-environment", Environment.Beta.ToString()).ConvertTo<Environment>();
            var serverHost = Config.Get("bloomberg-server-host", "localhost");
            var serverPort = Config.GetInt("bloomberg-server-host", 8194);

            return new BloombergBrokerage(orderProvider, apiType, environment, Mock.Of<IBloombergSymbolMapper>(), serverHost, serverPort);
        }
    }
}
