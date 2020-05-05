/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using NUnit.Framework;
using QuantConnect.Bloomberg;

namespace QuantConnect.BloombergTests
{
    [TestFixture]
    public class BloombergSymbolMapperTests
    {
        private const string TestFileName = "bloomberg-symbol-map-tests.json";

        [Test]
        public void ThrowsOnNullOrEmptySymbol()
        {
            var mapper = new BloombergSymbolMapper(TestFileName);

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
            Assert.AreEqual("EURUSD Curncy", brokerageSymbol);

            // canonical symbol for future chain -- commodity
            symbol = Symbol.Create("ZL", SecurityType.Future, Market.USA, "/ZL");
            brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("BO1 COMB Comdty", brokerageSymbol);

            // canonical symbol for future chain -- currency
            symbol = Symbol.Create("6A", SecurityType.Future, Market.USA, "/6A");
            brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("AD1 COMB Curncy", brokerageSymbol);

            // canonical symbol for future chain -- index
            symbol = Symbol.Create("ES", SecurityType.Future, Market.USA, "/ES");
            brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("ES1 COMB Index", brokerageSymbol);

            // future contract
            symbol = Symbol.CreateFuture("ES", Market.USA, new DateTime(2020, 3, 20));
            brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("ESH0 COMB Index", brokerageSymbol);

            // option contract
            symbol = Symbol.CreateOption("SPY", Market.USA, OptionStyle.American, OptionRight.Call, 200, new DateTime(2019, 12, 31));
            brokerageSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.AreEqual("SPY UO 12/31/19 C200.00 Equity", brokerageSymbol);
        }

        [Test]
        public void ReturnsCorrectLeanSymbol()
        {
            var mapper = new BloombergSymbolMapper(TestFileName);

            var symbol = mapper.GetLeanSymbol("SPY US Equity", SecurityType.Equity);
            Assert.AreEqual("SPY", symbol.Value);
            Assert.AreEqual(SecurityType.Equity, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("EURUSD Curncy", SecurityType.Forex);
            Assert.AreEqual("EURUSD", symbol.Value);
            Assert.AreEqual(SecurityType.Forex, symbol.ID.SecurityType);
            Assert.AreEqual(Market.FXCM, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("AD1 COMB Curncy", SecurityType.Future);
            Assert.AreEqual("/6A", symbol.Value);
            Assert.AreEqual("6A", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("AD1 Curncy", SecurityType.Future);
            Assert.AreEqual("/6A", symbol.Value);
            Assert.AreEqual("6A", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("ADH0 COMB Curncy", SecurityType.Future);
            Assert.AreEqual("6A16H20", symbol.Value);
            Assert.AreEqual("6A", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("ADH0 Curncy", SecurityType.Future);
            Assert.AreEqual("6A16H20", symbol.Value);
            Assert.AreEqual("6A", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("ES1 COMB Index", SecurityType.Future);
            Assert.AreEqual("/ES", symbol.Value);
            Assert.AreEqual("ES", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("ES1 Index", SecurityType.Future);
            Assert.AreEqual("/ES", symbol.Value);
            Assert.AreEqual("ES", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("ESH0 COMB Index", SecurityType.Future);
            Assert.AreEqual("ES20H20", symbol.Value);
            Assert.AreEqual("ES", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

            symbol = mapper.GetLeanSymbol("ESH0 Index", SecurityType.Future);
            Assert.AreEqual("ES20H20", symbol.Value);
            Assert.AreEqual("ES", symbol.ID.Symbol);
            Assert.AreEqual(SecurityType.Future, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);

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

            symbol = mapper.GetLeanSymbol("SPY UO 12/31/19 C200.00 Equity", SecurityType.Option);
            Assert.AreEqual("SPY", symbol.Underlying.Value);
            Assert.AreEqual(OptionRight.Call, symbol.ID.OptionRight);
            Assert.AreEqual(200m, symbol.ID.StrikePrice);
            Assert.AreEqual(SecurityType.Option, symbol.ID.SecurityType);
            Assert.AreEqual(Market.USA, symbol.ID.Market);
        }
    }
}
