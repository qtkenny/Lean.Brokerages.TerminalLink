/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System.Linq;
using NUnit.Framework;
using QuantConnect.Configuration;
using QuantConnect.Logging;

namespace QuantConnect.BloombergTests
{
    [TestFixture, Ignore("These tests require a local Bloomberg terminal.")]
    public class BloombergDataQueueUniverseProviderTests
    {
        [SetUp]
        public void SetUp()
        {
            Log.LogHandler = new ConsoleLogHandler();
            Config.SetConfigurationFile("integration-config.json");
            Config.Reset();
        }

        [Test]
        public void FetchesFutureChain()
        {
            using (var brokerage = BloombergCommon.CreateBrokerage())
            {
                brokerage.Connect();
                var symbols = brokerage.LookupSymbols("ES", SecurityType.Future).ToList();

                Log.Trace($"Future contracts found: {symbols.Count}");
                Assert.That(symbols.Count > 0);
            }
        }

        [Test]
        public void FetchesOptionChain()
        {
            using (var brokerage = BloombergCommon.CreateBrokerage())
            {
                brokerage.Connect();
                var symbols = brokerage.LookupSymbols("SPY", SecurityType.Option).ToList();

                Log.Trace($"Option contracts found: {symbols.Count}");
                Assert.That(symbols.Count > 0);
            }
        }
    }
}