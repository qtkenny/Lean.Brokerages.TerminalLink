/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System.Linq;
using NUnit.Framework;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Configuration;

namespace QuantConnect.TerminalLinkTests
{
    [TestFixture, Ignore("These tests require a local Bloomberg terminal.")]
    public class TerminalLinkDataQueueUniverseProviderTests
    {
        [SetUp]
        public void SetUp()
        {
            Log.LogHandler = new ConsoleLogHandler();
            Config.SetConfigurationFile("../../integration-config.json");
            Config.Reset();
        }

        [Test]
        public void FetchesFutureChain()
        {
            using (var brokerage = TerminalLinkCommon.CreateBrokerage())
            {
                brokerage.Connect();
                var canonicalSymbol = Symbol.Create(Futures.Indices.SP500EMini, SecurityType.Future, Market.CME);
                var symbols = brokerage.LookupSymbols(canonicalSymbol, false).ToList();

                Log.Trace($"Future contracts found: {symbols.Count}");
                Assert.That(symbols.Count > 0);
            }
        }

        [Test]
        public void FetchesOptionChain()
        {
            using (var brokerage = TerminalLinkCommon.CreateBrokerage())
            {
                brokerage.Connect();
                var canonicalSymbol = Symbol.Create("SPY", SecurityType.Option, Market.USA);
                var symbols = brokerage.LookupSymbols(canonicalSymbol, false).ToList();

                Log.Trace($"Option contracts found: {symbols.Count}");
                Assert.That(symbols.Count > 0);
            }
        }
    }
}