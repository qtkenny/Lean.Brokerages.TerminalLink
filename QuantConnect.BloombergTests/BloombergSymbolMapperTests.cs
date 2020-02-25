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
            var mapper = new BloombergSymbolMapper(TestFileName);

            var symbol = Symbol.Create("SPY", SecurityType.Equity, Market.USA);
            var brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("SPY US Equity", brokerageSymbol);

            symbol = Symbol.Create("EURUSD", SecurityType.Forex, Market.Oanda);
            brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("EUR BVAL Curncy", brokerageSymbol);

            // canonical symbol for future chain
            symbol = Symbol.Create("ZL", SecurityType.Future, Market.USA);
            brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("BO1 COMB Comdty", brokerageSymbol);

            symbol = Symbol.CreateOption("SPY", Market.USA, OptionStyle.American, OptionRight.Call, 200, new DateTime(2019, 12, 31));
            brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("SPY UO 12/31/19 C 200.00 Equity", brokerageSymbol);
        }

        [Test]
        public void ReturnsCorrectLeanSymbol()
        {
            var mapper = new BloombergSymbolMapper(TestFileName);

            var symbol = mapper.GetLeanSymbol("SPY US Equity");
            Assert.AreEqual("SPY", symbol.Value);
            Assert.AreEqual(SecurityType.Equity, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("EUR BVAL Curncy");
            Assert.AreEqual("EURUSD", symbol.Value);
            Assert.AreEqual(SecurityType.Forex, symbol.ID.SecurityType);
            Assert.AreEqual(Market.FXCM, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("BO1 COMB Comdty", SecurityType.Future);
            Assert.AreEqual("/ZL", symbol.Value);
            Assert.AreEqual("ZL", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("BO1 Comdty", SecurityType.Future);
            Assert.AreEqual("/ZL", symbol.Value);
            Assert.AreEqual("ZL", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("BOH0 COMB Comdty", SecurityType.Future);
            Assert.AreEqual("ZL13H20", symbol.Value);
            Assert.AreEqual("ZL", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("BOH0 Comdty", SecurityType.Future);
            Assert.AreEqual("ZL13H20", symbol.Value);
            Assert.AreEqual("ZL", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("C H0 COMB Comdty", SecurityType.Future);
            Assert.AreEqual("ZC13H20", symbol.Value);
            Assert.AreEqual("ZC", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("C H0 Comdty", SecurityType.Future);
            Assert.AreEqual("ZC13H20", symbol.Value);
            Assert.AreEqual("ZC", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("CLH0 COMB Comdty", SecurityType.Future);
            Assert.AreEqual("CL20H20", symbol.Value);
            Assert.AreEqual("CL", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("CLH0 Comdty", SecurityType.Future);
            Assert.AreEqual("CL20H20", symbol.Value);
            Assert.AreEqual("CL", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("CLH28 COMB Comdty", SecurityType.Future);
            Assert.AreEqual("CL22H28", symbol.Value);
            Assert.AreEqual("CL", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("CLH28 Comdty", SecurityType.Future);
            Assert.AreEqual("CL22H28", symbol.Value);
            Assert.AreEqual("CL", symbol.ID.Symbol);
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
