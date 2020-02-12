/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Securities;
using QuantConnect.Tests.Brokerages;

namespace QuantConnect.BloombergTests
{
    [TestFixture, Ignore("These tests require a local Bloomberg terminal.")]
    public class BloombergBrokerageTests : BrokerageTests
    {
        /// <summary>
        /// Creates the brokerage under test and connects it
        /// </summary>
        /// <returns>A connected brokerage instance</returns>
        protected override IBrokerage CreateBrokerage(IOrderProvider orderProvider, ISecurityProvider securityProvider)
        {
            return BloombergCommon.CreateBrokerage(orderProvider, securityProvider);
        }

        /// <summary>
        /// Disposes of the brokerage and any external resources started in order to create it
        /// </summary>
        /// <param name="brokerage">The brokerage instance to be disposed of</param>
        protected override void DisposeBrokerage(IBrokerage brokerage)
        {
            brokerage.Disconnect();
            brokerage.Dispose();
        }

        /// <summary>
        /// Provides the data required to test each order type in various cases
        /// </summary>
        public override TestCaseData[] OrderParameters => new[]
        {
            new TestCaseData(new MarketOrderTestParameters(Symbol)).SetName("MarketOrder"),
            new TestCaseData(new NonUpdateableLimitOrderTestParameters(Symbol, HighPrice, LowPrice)).SetName("LimitOrder"),
            new TestCaseData(new NonUpdateableStopMarketOrderTestParameters(Symbol, HighPrice, LowPrice)).SetName("StopMarketOrder")
        };

        /// <summary>
        /// Gets the symbol to be traded, must be shortable
        /// </summary>
        protected override Symbol Symbol { get; } = Symbol.Create("SPY", SecurityType.Equity, Market.USA);

        /// <summary>
        /// Gets the security type associated with the <see cref="BrokerageTests.Symbol" />
        /// </summary>
        protected override SecurityType SecurityType => Symbol.SecurityType;

        /// <summary>
        /// Gets a high price for the specified symbol so a limit sell won't fill
        /// </summary>
        protected override decimal HighPrice => 1000m;

        /// <summary>
        /// Gets a low price for the specified symbol so a limit buy won't fill
        /// </summary>
        protected override decimal LowPrice => 0.1m;

        /// <summary>
        /// Returns whether or not the brokers order methods implementation are async
        /// </summary>
        protected override bool IsAsync()
        {
            return false;
        }

        /// <summary>
        /// Returns whether or not the brokers order cancel method implementation is async
        /// </summary>
        protected override bool IsCancelAsync()
        {
            return true;
        }

        /// <summary>
        /// Gets the current market price of the specified security
        /// </summary>
        protected override decimal GetAskPrice(Symbol symbol)
        {
            var request = new HistoryRequest(
                DateTime.UtcNow.Subtract(Time.OneDay),
                DateTime.UtcNow,
                typeof(TradeBar),
                symbol,
                Resolution.Minute,
                MarketHoursDatabase.FromDataFolder().GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType),
                TimeZones.NewYork,
                Resolution.Minute,
                false,
                false,
                DataNormalizationMode.Adjusted,
                TickType.Trade
            );

            var brokerage = Brokerage;
            var bar = brokerage.GetHistory(request).LastOrDefault();
            if (bar == null)
            {
                throw new Exception($"Unable to fetch the market price for {symbol.Value}");
            }

            return bar.Value;
        }
    }
}
