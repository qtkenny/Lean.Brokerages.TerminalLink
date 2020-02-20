/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Moq;
using NodaTime;
using NUnit.Framework;
using QuantConnect.Bloomberg;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Auxiliary;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.HistoricalData;
using QuantConnect.Logging;
using QuantConnect.Securities;

namespace QuantConnect.BloombergTests
{
    [TestFixture, Ignore("These tests have only been tested with the API emulator")]
    public class BloombergTests
    {
        [TestFixtureSetUp]
        public void SetupFixture()
        {
            const string dataDirectory = "../../../../Lean/Data";
            Config.Set("data-folder", dataDirectory);
            Globals.Reset();
        }

        [SetUp]
        public void SetUp()
        {
            Log.LogHandler = new ConsoleLogHandler();
        }

        [Test]
        public void ClientConnects()
        {
            using (BloombergCommon.CreateBrokerage()) { }
        }

        [Test]
        public void SubscribesToMultipleSymbols()
        {
            using (var bb = BloombergCommon.CreateBrokerage())
            {
                var symbols = new List<Symbol>
                {
                    Symbol.Create("AAPL", SecurityType.Equity, Market.USA),
                    Symbol.Create("EURUSD", SecurityType.Forex, Market.FXCM),
                    Symbol.CreateFuture("ES", Market.USA, new DateTime(2019, 12, 20)),
                    Symbol.CreateOption("SPY", Market.USA, OptionStyle.American, OptionRight.Call, 200, new DateTime(2019, 12, 31))
                };

                bb.Subscribe(null, symbols);

                Thread.Sleep(5000);

                bb.Unsubscribe(null, symbols);

                foreach (var tick in bb.GetNextTicks())
                {
                    Log.Trace(tick.ToString());
                }
            }
        }

        [Test, TestCaseSource(nameof(GetHistoryTestData))]
        public void GetsHistory(HistoryTestParameters parameters)
        {
            var mapFileProvider = new LocalDiskMapFileProvider();
            var factorFileProvider = new LocalDiskFactorFileProvider(mapFileProvider);

            using (var brokerage = BloombergCommon.CreateBrokerage())
            {
                var historyProvider = new BrokerageHistoryProvider();

                historyProvider.SetBrokerage(brokerage);
                historyProvider.Initialize(new HistoryProviderInitializeParameters(null, null, null, null, mapFileProvider, factorFileProvider, null, false));

                var historyRequests = new List<HistoryRequest>
                {
                    new HistoryRequest(DateTime.UtcNow.Subtract(parameters.TimeSpan),
                        DateTime.UtcNow,
                        parameters.DataType,
                        parameters.Symbol,
                        parameters.Resolution,
                        MarketHoursDatabase.FromDataFolder().GetExchangeHours(parameters.Symbol.ID.Market, parameters.Symbol, parameters.Symbol.SecurityType),
                        parameters.DataTimeZone,
                        parameters.Resolution == Resolution.Tick ? (Resolution?)null : parameters.Resolution,
                        false,
                        false,
                        DataNormalizationMode.Adjusted,
                        parameters.TickType
                    )
                };

                var history = historyProvider.GetHistory(historyRequests, TimeZones.NewYork).ToList();

                var dataPointCount = 0;
                foreach (var slice in history)
                {
                    if (parameters.Resolution == Resolution.Tick)
                    {
                        dataPointCount += slice.Ticks.Values.Sum(x => x.Count);
                    }
                    else if (parameters.TickType == TickType.Quote)
                    {
                        dataPointCount += slice.QuoteBars.Values.Count;
                    }
                    else if (parameters.TickType == TickType.Trade)
                    {
                        dataPointCount += slice.Bars.Values.Count;
                    }
                }

                Log.Trace($"Total data points: {dataPointCount}");

                Assert.IsTrue(dataPointCount > 0);
            }
        }

        public class HistoryTestParameters
        {
            public Symbol Symbol { get; set; }
            public Type DataType { get; set; }
            public Resolution Resolution { get; set; }
            public DateTimeZone DataTimeZone { get; set; }
            public TickType TickType { get; set; }

            public string Name => $"{Symbol.SecurityType}/{Symbol.ID.Market}/{Resolution}/{DataType.Name}/{TickType}";

            public TimeSpan TimeSpan
            {
                get
                {
                    switch (Resolution)
                    {
                        case Resolution.Daily:
                            return TimeSpan.FromDays(10);

                        case Resolution.Hour:
                            return TimeSpan.FromDays(5);

                        case Resolution.Minute:
                            return TimeSpan.FromDays(2);

                        default:
                            return TimeSpan.FromDays(1);
                    }
                }
            }
        }

        private static TestCaseData[] GetHistoryTestData()
        {
            var equitySymbol = Symbol.Create("SPY", SecurityType.Equity, Market.USA);
            var forexSymbol = Symbol.Create("EURUSD", SecurityType.Forex, Market.FXCM);
            var futureSymbol = Symbol.CreateFuture("ES", Market.USA, new DateTime(2019, 12, 20));
            var optionSymbol = Symbol.CreateOption("SPY", Market.USA, OptionStyle.American, OptionRight.Call, 200, new DateTime(2019, 12, 31));

            return new List<HistoryTestParameters>
            {
                // equity trades (daily)
                new HistoryTestParameters
                {
                    Symbol = equitySymbol,
                    DataType = typeof(TradeBar),
                    Resolution = Resolution.Daily,
                    DataTimeZone = TimeZones.NewYork,
                    TickType = TickType.Trade
                },

                // equity trades (hour)
                new HistoryTestParameters
                {
                    Symbol = equitySymbol,
                    DataType = typeof(TradeBar),
                    Resolution = Resolution.Hour,
                    DataTimeZone = TimeZones.NewYork,
                    TickType = TickType.Trade
                },

                // equity trades (minute)
                new HistoryTestParameters
                {
                    Symbol = equitySymbol,
                    DataType = typeof(TradeBar),
                    Resolution = Resolution.Minute,
                    DataTimeZone = TimeZones.NewYork,
                    TickType = TickType.Trade
                },

                // equity trades (second)
                new HistoryTestParameters
                {
                    Symbol = equitySymbol,
                    DataType = typeof(TradeBar),
                    Resolution = Resolution.Second,
                    DataTimeZone = TimeZones.NewYork,
                    TickType = TickType.Trade
                },

                // equity trades (tick)
                new HistoryTestParameters
                {
                    Symbol = equitySymbol,
                    DataType = typeof(Tick),
                    Resolution = Resolution.Tick,
                    DataTimeZone = TimeZones.NewYork,
                    TickType = TickType.Trade
                },

                // Forex/FXCM quotes (daily)
                new HistoryTestParameters
                {
                    Symbol = forexSymbol,
                    DataType = typeof(QuoteBar),
                    Resolution = Resolution.Daily,
                    DataTimeZone = DateTimeZone.ForOffset(Offset.FromHours(-5)),
                    TickType = TickType.Quote
                },

                // Forex/FXCM quotes (hour)
                new HistoryTestParameters
                {
                    Symbol = forexSymbol,
                    DataType = typeof(QuoteBar),
                    Resolution = Resolution.Hour,
                    DataTimeZone = DateTimeZone.ForOffset(Offset.FromHours(-5)),
                    TickType = TickType.Quote
                },

                // Forex/FXCM quotes (minute)
                new HistoryTestParameters
                {
                    Symbol = forexSymbol,
                    DataType = typeof(QuoteBar),
                    Resolution = Resolution.Minute,
                    DataTimeZone = DateTimeZone.ForOffset(Offset.FromHours(-5)),
                    TickType = TickType.Quote
                },

                // Forex/FXCM quotes (second)
                new HistoryTestParameters
                {
                    Symbol = forexSymbol,
                    DataType = typeof(QuoteBar),
                    Resolution = Resolution.Second,
                    DataTimeZone = DateTimeZone.ForOffset(Offset.FromHours(-5)),
                    TickType = TickType.Quote
                },

                // Forex/FXCM quotes (tick)
                new HistoryTestParameters
                {
                    Symbol = forexSymbol,
                    DataType = typeof(Tick),
                    Resolution = Resolution.Tick,
                    DataTimeZone = DateTimeZone.ForOffset(Offset.FromHours(-5)),
                    TickType = TickType.Quote
                },

                // Future quotes (daily)
                new HistoryTestParameters
                {
                    Symbol = futureSymbol,
                    DataType = typeof(QuoteBar),
                    Resolution = Resolution.Daily,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Quote
                },

                // Future quotes (hour)
                new HistoryTestParameters
                {
                    Symbol = futureSymbol,
                    DataType = typeof(QuoteBar),
                    Resolution = Resolution.Hour,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Quote
                },

                // Future quotes (minute)
                new HistoryTestParameters
                {
                    Symbol = futureSymbol,
                    DataType = typeof(QuoteBar),
                    Resolution = Resolution.Minute,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Quote
                },

                // Future quotes (second)
                new HistoryTestParameters
                {
                    Symbol = futureSymbol,
                    DataType = typeof(QuoteBar),
                    Resolution = Resolution.Second,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Quote
                },

                // Future quotes (tick)
                new HistoryTestParameters
                {
                    Symbol = futureSymbol,
                    DataType = typeof(Tick),
                    Resolution = Resolution.Tick,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Quote
                },

                // Future trades (daily)
                new HistoryTestParameters
                {
                    Symbol = futureSymbol,
                    DataType = typeof(TradeBar),
                    Resolution = Resolution.Daily,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Trade
                },

                // Future trades (hour)
                new HistoryTestParameters
                {
                    Symbol = futureSymbol,
                    DataType = typeof(TradeBar),
                    Resolution = Resolution.Hour,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Trade
                },

                // Future trades (minute)
                new HistoryTestParameters
                {
                    Symbol = futureSymbol,
                    DataType = typeof(TradeBar),
                    Resolution = Resolution.Minute,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Trade
                },

                // Future trades (second)
                new HistoryTestParameters
                {
                    Symbol = futureSymbol,
                    DataType = typeof(TradeBar),
                    Resolution = Resolution.Second,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Trade
                },

                // Future trades (tick)
                new HistoryTestParameters
                {
                    Symbol = futureSymbol,
                    DataType = typeof(Tick),
                    Resolution = Resolution.Tick,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Trade
                },

                // Option quotes (daily)
                new HistoryTestParameters
                {
                    Symbol = optionSymbol,
                    DataType = typeof(QuoteBar),
                    Resolution = Resolution.Daily,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Quote
                },

                // Option quotes (hour)
                new HistoryTestParameters
                {
                    Symbol = optionSymbol,
                    DataType = typeof(QuoteBar),
                    Resolution = Resolution.Hour,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Quote
                },

                // Option quotes (minute)
                new HistoryTestParameters
                {
                    Symbol = optionSymbol,
                    DataType = typeof(QuoteBar),
                    Resolution = Resolution.Minute,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Quote
                },

                // Option quotes (second)
                new HistoryTestParameters
                {
                    Symbol = optionSymbol,
                    DataType = typeof(QuoteBar),
                    Resolution = Resolution.Second,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Quote
                },

                // Option quotes (tick)
                new HistoryTestParameters
                {
                    Symbol = optionSymbol,
                    DataType = typeof(Tick),
                    Resolution = Resolution.Tick,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Quote
                },

                // Option trades (daily)
                new HistoryTestParameters
                {
                    Symbol = optionSymbol,
                    DataType = typeof(TradeBar),
                    Resolution = Resolution.Daily,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Trade
                },

                // Option trades (hour)
                new HistoryTestParameters
                {
                    Symbol = optionSymbol,
                    DataType = typeof(TradeBar),
                    Resolution = Resolution.Hour,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Trade
                },

                // Option trades (minute)
                new HistoryTestParameters
                {
                    Symbol = optionSymbol,
                    DataType = typeof(TradeBar),
                    Resolution = Resolution.Minute,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Trade
                },

                // Option trades (second)
                new HistoryTestParameters
                {
                    Symbol = optionSymbol,
                    DataType = typeof(TradeBar),
                    Resolution = Resolution.Second,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Trade
                },

                // Option trades (tick)
                new HistoryTestParameters
                {
                    Symbol = optionSymbol,
                    DataType = typeof(Tick),
                    Resolution = Resolution.Tick,
                    DataTimeZone = TimeZones.Utc,
                    TickType = TickType.Trade
                },

            }.Select(x => new TestCaseData(x).SetName(x.Name)).ToArray();
        }
    }
}
