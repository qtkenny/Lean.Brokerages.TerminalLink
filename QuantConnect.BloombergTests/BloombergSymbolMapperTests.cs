/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.IO;
using NUnit.Framework;
using QuantConnect.Bloomberg;
using QuantConnect.Securities;

namespace QuantConnect.BloombergTests
{
    [TestFixture]
    public class BloombergSymbolMapperTests
    {
        private const string TestFileName = "bloomberg-symbol-map-tests.json";

        [Test]
        public void Can_Provide_Mapping_File()
        {
            if (!File.Exists(TestFileName))
            {
                throw new FileNotFoundException("Unable to find test file", TestFileName);
            }

            var underTest = new BloombergSymbolMapper("bloomberg-symbol-map-tests.json");

            const string equityTicker = "Test Equity";
            var equity = underTest.GetLeanSymbol(equityTicker);
            Assert.AreEqual(SecurityType.Equity, equity.ID.SecurityType);
            Assert.AreEqual(Market.USA, equity.ID.Market);
            Assert.AreEqual("EQU", equity.Value);
            Assert.AreEqual(equityTicker, underTest.GetBrokerageSymbol(equity));

            const string futureTicker = "Test Future";
            var future = underTest.GetLeanSymbol(futureTicker);
            Assert.AreEqual(SecurityType.Future, future.ID.SecurityType);
            Assert.AreEqual(Market.USA, future.ID.Market);
            Assert.AreEqual(Futures.Currencies.AUD + "13H0", future.Value);
            Assert.AreEqual(futureTicker, underTest.GetBrokerageSymbol(future));
        }

        [Test]
        public void ThrowsOnNullOrEmptySymbol()
        {
            var mapper = new BloombergSymbolMapper();

            Assert.Throws<ArgumentException>(() => mapper.GetLeanSymbol(null, SecurityType.Forex, Market.FXCM));

            Assert.Throws<ArgumentException>(() => mapper.GetLeanSymbol("", SecurityType.Forex, Market.FXCM));

            var symbol = Symbol.Empty;
            Assert.Throws<ArgumentException>(() => mapper.GetBrokerageSymbol(symbol));

            symbol = null;
            Assert.Throws<ArgumentException>(() => mapper.GetBrokerageSymbol(symbol));

            symbol = Symbol.Create("", SecurityType.Forex, Market.FXCM);
            Assert.Throws<ArgumentException>(() => mapper.GetBrokerageSymbol(symbol));
        }

        [Test]
        public void ReturnsCorrectBrokerageSymbol()
        {
            var mapper = new BloombergSymbolMapper();

            var symbol = Symbol.Create("SPY", SecurityType.Equity, Market.USA);
            var brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("SPY US Equity", brokerageSymbol);

            symbol = Symbol.Create("EURUSD", SecurityType.Forex, Market.Oanda);
            brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("EUR BVAL Curncy", brokerageSymbol);

            symbol = Symbol.CreateFuture("ES", Market.USA, new DateTime(2019, 12, 20));
            brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("ES20Z19 COMB Comdty", brokerageSymbol);

            symbol = Symbol.CreateOption("SPY", Market.USA, OptionStyle.American, OptionRight.Call, 200, new DateTime(2019, 12, 31));
            brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("SPY UO 12/31/19 C 200.00 Equity", brokerageSymbol);
        }

        [Test]
        public void ReturnsCorrectLeanSymbol()
        {
            var mapper = new BloombergSymbolMapper();

            var symbol = mapper.GetLeanSymbol("SPY US Equity");
            Assert.AreEqual("SPY", symbol.Value);
            Assert.AreEqual(SecurityType.Equity, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("EUR BVAL Curncy");
            Assert.AreEqual("EURUSD", symbol.Value);
            Assert.AreEqual(SecurityType.Forex, symbol.ID.SecurityType);
            Assert.AreEqual(Market.FXCM, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("ES19Z20 COMB Comdty");
            Assert.AreEqual("ES19Z20", symbol.Value);
            Assert.AreEqual("ES", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("SPY UO 12/31/19 C 200.00 Equity");
            Assert.AreEqual("SPY", symbol.Underlying.Value);
            Assert.AreEqual(OptionRight.Call, symbol.ID.OptionRight);
            Assert.AreEqual(200m, symbol.ID.StrikePrice);
            Assert.AreEqual(SecurityType.Option, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);
        }
    }
}
