/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System.Linq;
using NUnit.Framework;
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
        }

        [Test]
        public void FetchesFutureChain()
        {
            using (var brokerage = BloombergCommon.CreateBrokerage())
            {
                var symbols = brokerage.LookupSymbols("ES", SecurityType.Future).ToList();

                Assert.That(symbols.Count > 0);
            }
        }

        [Test]
        public void FetchesOptionChain()
        {
            using (var brokerage = BloombergCommon.CreateBrokerage())
            {
                var symbols = brokerage.LookupSymbols("SPY", SecurityType.Option).ToList();

                Assert.That(symbols.Count > 0);
            }
        }
    }
}